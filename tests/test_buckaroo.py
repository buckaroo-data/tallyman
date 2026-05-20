"""Tests for the Buckaroo lifecycle.

Three layers:
- `test_buckaroo_unit_*`: BuckarooManager session bookkeeping with no
  subprocess (we manually poke its state).
- `test_buckaroo_integration_*`: real subprocess + real /load round-trip.
  Marked slow; run with `pytest -m integration`.
- companion-level: entry_detail with a stub manager that returns a
  fixed session id, verifying the iframe lands in the response.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from pydata_companion.buckaroo_lifecycle import BuckarooManager
from pydata_xorq import build_and_persist


def _code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


# ---------------------------------------------------------------------------
# unit: session map only
# ---------------------------------------------------------------------------

def test_unit_session_file_round_trip(project: str):
    mgr = BuckarooManager(project)
    mgr._sessions["abc"] = "sess-1"
    mgr._sessions["def"] = "sess-2"
    mgr._buckaroo_started_at = 123.456
    mgr._persist_sessions()

    # Fresh instance picks up persisted sessions and the start-time anchor.
    mgr2 = BuckarooManager(project)
    assert mgr2._sessions == {"abc": "sess-1", "def": "sess-2"}
    assert mgr2._buckaroo_started_at == 123.456


def test_unit_legacy_session_file_still_loads(project: str):
    """If we read a `buckaroo_sessions.json` written by V0.5 (no envelope),
    the manager picks up the bare `{hash: session_id}` map."""
    from pydata_core.paths import catalog_dir as _cd
    p = _cd(project) / "buckaroo_sessions.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"abc": "sess-1"}))
    mgr = BuckarooManager(project)
    assert mgr._sessions == {"abc": "sess-1"}
    assert mgr._buckaroo_started_at is None


def test_unit_ensure_session_short_circuits_when_not_running(project: str):
    mgr = BuckarooManager(project)
    # is_running is False because we never called start().
    assert mgr.ensure_session("anyhash") is None


def test_unit_ensure_session_restarts_dead_subprocess(
    project: str, orders_parquet: Path, monkeypatch
):
    """If buckaroo's subprocess died since the last call (mid-session crash,
    OOM, signal), ensure_session attempts one restart instead of silently
    falling back forever. Without this, a one-time death turns the rest of
    the session into pandas-only with no log line the user would notice."""
    res = build_and_persist(project, _code(project))

    mgr = BuckarooManager(project)
    # Simulate a previously-running buckaroo that has now exited.
    class _DeadProc:
        returncode = 1
        def poll(self):
            return 1
    mgr.proc = _DeadProc()  # type: ignore[assignment]
    mgr.bound_port = None

    # Stub start() so the test doesn't actually spawn a subprocess —
    # it just flips state to "running" as a real start would.
    restart_calls = {"n": 0}
    class _LiveProc:
        returncode = None
        def poll(self):
            return None
    def fake_start(self):
        restart_calls["n"] += 1
        self.proc = _LiveProc()
        self.bound_port = 8700
    monkeypatch.setattr(BuckarooManager, "start", fake_start)

    # Stub the /load POST so we don't need a real server.
    class _Resp:
        def raise_for_status(self): pass
        def json(self): return {"session": "restored-session"}
    monkeypatch.setattr(mgr._client, "post", lambda *a, **kw: _Resp())

    sid = mgr.ensure_session(res.content_hash)
    assert sid == "restored-session"
    assert restart_calls["n"] == 1


def test_unit_ensure_session_restart_throttled(
    project: str, orders_parquet: Path, monkeypatch
):
    """If buckaroo restart fails, don't retry on every page hit — that
    would hammer the system with subprocess spawns. Throttle to one
    attempt per cooldown."""
    build_and_persist(project, _code(project))

    mgr = BuckarooManager(project)
    class _DeadProc:
        returncode = 1
        def poll(self):
            return 1
    mgr.proc = _DeadProc()  # type: ignore[assignment]
    mgr.bound_port = None

    from pydata_companion.buckaroo_lifecycle import BuckarooUnavailable
    restart_calls = {"n": 0}
    def failing_start(self):
        restart_calls["n"] += 1
        raise BuckarooUnavailable("simulated startup failure")
    monkeypatch.setattr(BuckarooManager, "start", failing_start)

    # First call attempts a restart.
    assert mgr.ensure_session("anyhash") is None
    assert restart_calls["n"] == 1

    # Second call within the cooldown window does NOT attempt another.
    assert mgr.ensure_session("anyhash") is None
    assert restart_calls["n"] == 1


def test_unit_status_shape(project: str):
    mgr = BuckarooManager(project)
    s = mgr.status()
    assert s["running"] is False
    assert s["port"] is None
    assert s["session_count"] == 0


def test_unit_default_mode_is_buckaroo(project: str):
    """We want the BuckarooInfiniteWidget pipeline, not the lighter DfViewer.
    The server picks the pipeline based on the `mode` field in POST /load."""
    mgr = BuckarooManager(project)
    assert mgr.mode == "buckaroo"


def test_unit_mode_override(project: str):
    mgr = BuckarooManager(project, mode="viewer")
    assert mgr.mode == "viewer"


def test_unit_ensure_session_sends_mode_in_request(
    project: str, orders_parquet: Path, monkeypatch
):
    """Pin the wire shape: POST /load body must include mode='buckaroo'
    so the server builds the BuckarooInfiniteWidget pipeline."""
    from pydata_xorq import build_and_persist

    res = build_and_persist(project, _code(project))

    mgr = BuckarooManager(project)
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    captured: dict[str, Any] = {}

    class FakeResponse:
        status_code = 200
        def raise_for_status(self):
            pass
        def json(self):
            return {"session": "abc123def456"}

    def fake_post(url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr(mgr._client, "post", fake_post)
    session = mgr.ensure_session(res.content_hash)
    assert session == "abc123def456"
    assert captured["url"].endswith("/load")
    assert captured["json"]["mode"] == "buckaroo"
    assert captured["json"]["no_browser"] is True
    assert captured["json"]["path"].endswith("/result.parquet")


# ---------------------------------------------------------------------------
# integration: real subprocess + real /load
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_integration_spawn_and_load(project: str, orders_parquet: Path):
    # Build at least one catalog entry so we have a result.parquet to /load.
    res = build_and_persist(project, _code(project))

    # Use port=0 to dodge collisions with anything else on :8700.
    mgr = BuckarooManager(project, port=0, startup_timeout=15.0)
    try:
        mgr.start()
        assert mgr.is_running
        assert mgr.bound_port is not None and mgr.bound_port > 0
        session = mgr.ensure_session(res.content_hash)
        assert session is not None and len(session) >= 16
        # Cached lookup returns the same session id.
        assert mgr.ensure_session(res.content_hash) == session
        # Persisted on disk under the new envelope shape.
        sessions_file = json.loads(
            (mgr._session_file_path()).read_text()
        )
        assert sessions_file["sessions"][res.content_hash] == session
        assert sessions_file["buckaroo_started_at"] is not None
    finally:
        mgr.stop()
    assert not mgr.is_running


@pytest.mark.integration
def test_integration_stop_cleans_up(project: str, orders_parquet: Path):
    mgr = BuckarooManager(project, port=0, startup_timeout=15.0)
    mgr.start()
    assert mgr.proc is not None
    pid = mgr.proc.pid
    mgr.stop()
    # The proc reference is cleared after stop.
    assert mgr.proc is None
    # The pid is no longer alive (give it a moment).
    import time
    time.sleep(0.2)
    import os
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


# ---------------------------------------------------------------------------
# companion: entry_detail picks up the iframe when a session is available
# ---------------------------------------------------------------------------

class _StubBuckaroo:
    """A BuckarooManager-shaped stub that returns a predetermined session id."""

    def __init__(self, *, session: str, port: int = 8700):
        self.session = session
        self.bound_port = port
        self.is_running = True

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.bound_port}"

    @property
    def ws_base_url(self) -> str:
        return f"ws://127.0.0.1:{self.bound_port}"

    def ensure_session(self, content_hash: str) -> str | None:
        return self.session


def test_entry_detail_embeds_react_widget_when_buckaroo_present(
    project: str, orders_parquet: Path
):
    from pydata_companion import create_app

    res = build_and_persist(project, _code(project))
    bk: Any = _StubBuckaroo(session="abc123", port=8700)
    app = create_app(project, buckaroo=bk)
    c = TestClient(app)
    r = c.get(f"/catalog/{res.content_hash}")
    assert r.status_code == 200
    assert 'class="buckaroo-embed"' in r.text
    assert 'data-ws-url="ws://127.0.0.1:8700/ws/abc123"' in r.text
    # Pandas preview is still in the page but tucked behind a <details>.
    assert "<details" in r.text
    assert "data-table" in r.text


_EMBED_TAG = '<div\n        class="buckaroo-embed"'


def test_entry_detail_falls_back_when_buckaroo_absent(
    fresh_companion_app, project: str, orders_parquet: Path
):
    res = build_and_persist(project, _code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/catalog/{res.content_hash}")
    assert r.status_code == 200
    # The class name may appear inside build_metadata.json's captured git
    # diff (HTML-escaped); check for the actual mount-div tag instead.
    assert _EMBED_TAG not in r.text


def test_entry_detail_falls_back_when_session_unavailable(
    project: str, orders_parquet: Path
):
    """When ensure_session returns None (Buckaroo died or /load failed) the
    page still renders — just falls back to the pandas preview."""
    from pydata_companion import create_app

    res = build_and_persist(project, _code(project))

    class _DownBuckaroo:
        bound_port = None
        is_running = False
        base_url = "http://127.0.0.1:8700"

        def ensure_session(self, content_hash):
            return None

    app = create_app(project, buckaroo=_DownBuckaroo())  # type: ignore[arg-type]
    c = TestClient(app)
    r = c.get(f"/catalog/{res.content_hash}")
    assert r.status_code == 200
    assert _EMBED_TAG not in r.text
    assert "data-table" in r.text
