"""Per-grid-load telemetry: the Buckaroo-server perf-span push (buckaroo#943).

Three layers, mirroring test_buckaroo.py:
- storage: ``record_span`` / ``read_spans`` round-trip, filtering, ordering,
  and the best-effort guards (oversized / unserializable / never-raises).
- endpoint: POST one span then GET it back via the companion TestClient,
  including the per-trace filter the Log UI joins on.
- wiring: ``BuckarooManager.load_session`` puts a per-project ``telemetry_url``
  on the ``/load_expr`` body when (and only when) it knows the companion's
  address, and the buckaroo grid-load activity event carries the ``session_id``
  that keys those spans back to it.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_core import entry_dir
from tallyman_core.telemetry import read_spans, record_span

# A representative server span: the summary-stats phase carrying the #944 cache
# signal — the one datum the companion can't observe any other way.
_SPAN = {
    "trace": "sess-abc",
    "source": "server",
    "name": "firstpull.summary_stats",
    "t_start_ms": 1_700_000_000_000.0,
    "t_end_ms": 1_700_000_000_186.4,
    "attrs": {"cache_status": "hit", "cache_hits": 12, "cache_misses": 0, "cache_secs": 0.03},
}


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------


def test_record_span_round_trip_stamps_received_at(project: str):
    stored = record_span(project, _SPAN)
    assert stored is not None
    assert "received_at" in stored  # server wall clock, added on ingest
    assert stored["name"] == "firstpull.summary_stats"

    spans = read_spans(project)
    assert len(spans) == 1
    assert spans[0]["trace"] == "sess-abc"
    assert spans[0]["attrs"]["cache_status"] == "hit"


def test_read_spans_filters_by_trace(project: str):
    record_span(project, {**_SPAN, "trace": "sess-a", "name": "firstpull.load_expr"})
    record_span(project, {**_SPAN, "trace": "sess-b", "name": "firstpull.load_expr"})
    record_span(project, {**_SPAN, "trace": "sess-a", "name": "firstpull.summary_stats"})

    a = read_spans(project, trace="sess-a")
    assert {s["name"] for s in a} == {"firstpull.load_expr", "firstpull.summary_stats"}
    assert all(s["trace"] == "sess-a" for s in a)

    b = read_spans(project, trace="sess-b")
    assert len(b) == 1


def test_read_spans_newest_first_and_limit(project: str):
    for i in range(5):
        record_span(project, {**_SPAN, "name": f"span-{i}"})
    spans = read_spans(project, limit=3)
    assert len(spans) == 3
    # Newest-first by received_at: the last appended sorts first. received_at is
    # monotonic across these synchronous appends, so the order is well-defined.
    received = [s["received_at"] for s in spans]
    assert received == sorted(received, reverse=True)


def test_record_span_rejects_oversized(project: str):
    huge = {**_SPAN, "attrs": {"blob": "x" * (64 * 1024 + 1)}}
    assert record_span(project, huge) is None
    assert read_spans(project) == []


def test_record_span_rejects_unserializable_without_raising(project: str):
    # A set is not JSON-serializable; record_span must swallow it and return None
    # rather than letting a bad POST surface as an error to the buckaroo server.
    bad = {**_SPAN, "attrs": {"weird": {1, 2, 3}}}
    assert record_span(project, bad) is None
    assert read_spans(project) == []


def test_read_spans_tolerates_partial_trailing_line(project: str):
    record_span(project, _SPAN)
    from tallyman_core.paths import artifacts_dir

    p = artifacts_dir(project) / "telemetry.jsonl"
    with p.open("a") as fh:
        fh.write('{"trace": "half-written"')  # no newline, unterminated JSON
    spans = read_spans(project)
    assert len(spans) == 1  # the good line survives, the partial one is skipped


# ---------------------------------------------------------------------------
# endpoint
# ---------------------------------------------------------------------------


def test_endpoint_ingest_then_read(project: str, fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/telemetry", json=_SPAN)
    assert r.status_code == 200
    assert r.json() == {"ok": True}

    got = c.get(f"/{project}/api/telemetry?trace=sess-abc").json()
    assert got["trace"] == "sess-abc"
    assert len(got["spans"]) == 1
    assert got["spans"][0]["attrs"]["cache_status"] == "hit"


def test_endpoint_ingest_oversized_is_ok_false_not_500(project: str, fresh_companion_app):
    # A rejected record still returns 200 {ok: false}: telemetry is best-effort
    # and must never read as a server error on buckaroo's fire-and-forget POST.
    c = TestClient(fresh_companion_app)
    huge = {**_SPAN, "attrs": {"blob": "x" * (64 * 1024 + 1)}}
    r = c.post(f"/{project}/api/telemetry", json=huge)
    assert r.status_code == 200
    assert r.json() == {"ok": False}


def test_endpoint_read_empty_trace(project: str, fresh_companion_app):
    c = TestClient(fresh_companion_app)
    got = c.get(f"/{project}/api/telemetry?trace=nope").json()
    assert got["spans"] == []


def test_ingest_does_not_checkpoint_the_catalog(project: str, orders_parquet, fresh_companion_app):
    """A telemetry POST must be exempt from the dispatch-boundary catalog
    checkpoint. The middleware checkpoints every non-GET project route by
    default (so new authored-state routes can't silently skip it), but spans
    fire many times per grid load and carry no authored state — a checkpoint
    per span would flood the catalog history and stall each load on git."""
    from tallyman_core.catalog_state import current_step

    # Build one entry so the project has a catalog with a real step counter.
    from tallyman_xorq import build_and_persist

    build_and_persist(project, _code_for(project))

    c = TestClient(fresh_companion_app)
    before = current_step(project)
    for _ in range(3):
        c.post(f"/{project}/api/telemetry", json=_SPAN)
    assert current_step(project) == before  # no new commits


# ---------------------------------------------------------------------------
# wiring: load_session telemetry_url + the session_id on the grid-load event
# ---------------------------------------------------------------------------


def _running_manager(monkeypatch, **kwargs) -> tuple[BuckarooManager, dict]:
    """A BuckarooManager that looks running, with /load_expr stubbed to capture
    the posted body. Returns (manager, captured)."""
    mgr = BuckarooManager(**kwargs)
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()
    captured: dict[str, Any] = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "sess-xyz"}

    def fake_post(url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr(mgr._client, "post", fake_post)
    return mgr, captured


def test_load_session_includes_telemetry_url_when_companion_known(project, orders_parquet, monkeypatch):
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project, _code_for(project))
    mgr, captured = _running_manager(monkeypatch, companion_base_url="http://127.0.0.1:7860/")
    mgr.load_session(res.content_hash, project)
    # Trailing slash on the base url is normalized; the path is per-project.
    assert captured["json"]["telemetry_url"] == f"http://127.0.0.1:7860/{project}/api/telemetry"


def test_load_session_omits_telemetry_url_when_companion_unknown(project, orders_parquet, monkeypatch):
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project, _code_for(project))
    mgr, captured = _running_manager(monkeypatch)  # no companion_base_url
    mgr.load_session(res.content_hash, project)
    assert "telemetry_url" not in captured["json"]


def test_grid_load_event_records_session_id_for_join(project, orders_parquet, monkeypatch):
    """The buckaroo activity event must carry the session_id so the Log UI can
    join it to the firstpull.* spans (keyed by that id as their `trace`)."""
    from tallyman_companion import create_app
    from tallyman_core.events import read_events
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project, _code_for(project))

    class _StubBuckaroo:
        bound_port = 8700
        is_running = True
        ws_base_url = "ws://127.0.0.1:8700"

        def load_session(self, content_hash, project, column_config_overrides=None):
            return {"status": "ok", "session_id": "sess-join", "detail": "", "load_expr_ms": 5.0}

    app = create_app(project, buckaroo=_StubBuckaroo())  # type: ignore[arg-type]
    c = TestClient(app)
    c.get(f"/{project}/api/session/{res.content_hash}")

    evs = [e for e in read_events(project) if e.get("kind") == "buckaroo"]
    assert evs and evs[0]["session_id"] == "sess-join"


def _code_for(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


# ---------------------------------------------------------------------------
# integration: a real buckaroo subprocess actually POSTs spans to telemetry_url
# ---------------------------------------------------------------------------


class _CaptureHandler(BaseHTTPRequestHandler):
    """Minimal telemetry receiver: append each POSTed JSON span to a shared list."""

    captured: list[dict] = []

    def do_POST(self):  # noqa: N802 (http.server's required name)
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        try:
            type(self).captured.append(json.loads(body))
        except ValueError:
            pass
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, *a):  # silence the default stderr access log
        pass


@pytest.mark.integration
def test_integration_buckaroo_posts_firstpull_spans(project: str, orders_parquet: Path):
    """End-to-end: a real buckaroo 0.15.3 server, told a ``telemetry_url`` on
    ``/load_expr``, fire-and-forget POSTs ``firstpull.*`` perf spans back in the
    OTel-shaped record this module reads (``{trace, source, name, t_start_ms,
    t_end_ms, attrs}``). Drives the POST directly (no browser), so only the
    server-side spans that fire during load fire here — the WS-pull spans
    (summary_stats, ws_first_payload) need a widget and aren't asserted."""
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project, _code_for(project))

    _CaptureHandler.captured = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CaptureHandler)
    recv_port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    mgr = BuckarooManager(port=0, startup_timeout=15.0)
    try:
        mgr.start()
        expanded = entry_dir(project, res.content_hash) / ".xorq_build_expanded"
        mgr.ensure_session(res.content_hash, project)  # materialise the expanded build
        resp = httpx.post(
            f"{mgr.base_url}/load_expr",
            json={
                "build_dir": str(expanded),
                "no_browser": True,
                "telemetry_url": f"http://127.0.0.1:{recv_port}/{project}/api/telemetry",
            },
            timeout=15.0,
        )
        resp.raise_for_status()

        # Spans are POSTed on the server's IOLoop after the response returns, so
        # poll briefly for them to arrive rather than asserting synchronously.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not _CaptureHandler.captured:
            time.sleep(0.05)
    finally:
        mgr.stop()
        server.shutdown()

    names = {r.get("name") for r in _CaptureHandler.captured}
    assert "firstpull.load_expr" in names, _CaptureHandler.captured
    # Every record is the OTel-shaped envelope read_spans/the UI rely on.
    one = next(r for r in _CaptureHandler.captured if r["name"] == "firstpull.load_expr")
    assert set(one) >= {"trace", "source", "name", "t_start_ms", "t_end_ms", "attrs"}
    assert one["t_end_ms"] >= one["t_start_ms"]
