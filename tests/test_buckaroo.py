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


def test_unit_status_shape(project: str):
    mgr = BuckarooManager(project)
    s = mgr.status()
    assert s["running"] is False
    assert s["port"] is None
    assert s["session_count"] == 0


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

    def ensure_session(self, content_hash: str) -> str | None:
        return self.session


def test_entry_detail_embeds_iframe_when_buckaroo_present(
    project: str, orders_parquet: Path
):
    from pydata_companion import create_app

    res = build_and_persist(project, _code(project))
    bk: Any = _StubBuckaroo(session="abc123", port=8700)
    app = create_app(project, buckaroo=bk)
    c = TestClient(app)
    r = c.get(f"/catalog/{res.content_hash}")
    assert r.status_code == 200
    assert 'class="buckaroo-frame"' in r.text
    assert "http://127.0.0.1:8700/s/abc123" in r.text
    # Pandas preview is still in the page but tucked behind a <details>.
    assert "<details" in r.text
    assert "data-table" in r.text


def test_entry_detail_falls_back_when_buckaroo_absent(
    fresh_companion_app, project: str, orders_parquet: Path
):
    res = build_and_persist(project, _code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/catalog/{res.content_hash}")
    assert r.status_code == 200
    assert "buckaroo-frame" not in r.text


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
    assert "buckaroo-frame" not in r.text
    assert "data-table" in r.text
