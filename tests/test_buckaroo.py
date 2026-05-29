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
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import httpx
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


def test_unit_session_file_round_trip(project: str, orders_parquet: Path):
    # Build one entry so the project/hash on disk satisfies the prune check
    # that runs on _load_session_file.
    res = build_and_persist(project, _code(project))
    h = res.content_hash

    mgr = BuckarooManager()
    mgr._sessions[h] = {"session_id": "sess-1", "project": project}
    mgr._buckaroo_started_at = 123.456
    mgr._persist_sessions()

    # Fresh instance picks up persisted sessions and the start-time anchor.
    mgr2 = BuckarooManager()
    assert mgr2._sessions == {h: {"session_id": "sess-1", "project": project}}
    assert mgr2._buckaroo_started_at == 123.456


def test_unit_ensure_session_short_circuits_when_not_running(project: str):
    mgr = BuckarooManager()
    # is_running is False because we never called start().
    assert mgr.ensure_session("anyhash", project) is None


def test_unit_ensure_session_restarts_dead_subprocess(project: str, orders_parquet: Path, monkeypatch):
    """If buckaroo's subprocess died since the last call (mid-session crash,
    OOM, signal), ensure_session attempts one restart instead of silently
    falling back forever. Without this, a one-time death turns the rest of
    the session into pandas-only with no log line the user would notice."""
    res = build_and_persist(project, _code(project))

    mgr = BuckarooManager()

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
        pid = 99999

        def poll(self):
            return None

    def fake_start(self):
        restart_calls["n"] += 1
        self.proc = _LiveProc()
        self.bound_port = 8700

    monkeypatch.setattr(BuckarooManager, "start", fake_start)

    # Stub the /load POST so we don't need a real server.
    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "restored-session"}

    monkeypatch.setattr(mgr._client, "post", lambda *a, **kw: _Resp())

    sid = mgr.ensure_session(res.content_hash, project)
    assert sid == "restored-session"
    assert restart_calls["n"] == 1


def test_unit_ensure_session_restart_throttled(project: str, orders_parquet: Path, monkeypatch):
    """If buckaroo restart fails, don't retry on every page hit — that
    would hammer the system with subprocess spawns. Throttle to one
    attempt per cooldown."""
    build_and_persist(project, _code(project))

    mgr = BuckarooManager()

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
    assert mgr.ensure_session("anyhash", project) is None
    assert restart_calls["n"] == 1

    # Second call within the cooldown window does NOT attempt another.
    assert mgr.ensure_session("anyhash", project) is None
    assert restart_calls["n"] == 1


def test_unit_forget_project_sessions_evicts_matching(project: str):
    mgr = BuckarooManager()
    mgr._sessions = {
        "hash_a": {"session_id": "s1", "project": project},
        "hash_b": {"session_id": "s2", "project": project},
        "hash_c": {"session_id": "s3", "project": "other_project"},
    }
    dropped = mgr.forget_project_sessions(project)
    assert dropped == 2
    assert set(mgr._sessions.keys()) == {"hash_c"}


def test_unit_forget_project_sessions_returns_zero_when_none_match(project: str):
    mgr = BuckarooManager()
    mgr._sessions = {
        "hash_a": {"session_id": "s1", "project": "other_project"},
    }
    dropped = mgr.forget_project_sessions(project)
    assert dropped == 0
    assert len(mgr._sessions) == 1


def test_unit_reload_project_sessions_calls_reload_expr(project: str, monkeypatch):
    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()
    mgr._sessions = {
        "hash_a": {"session_id": "sess-aaa", "project": project},
        "hash_b": {"session_id": "sess-bbb", "project": project},
        "hash_c": {"session_id": "sess-ccc", "project": "other_project"},
    }

    posted_urls: list[str] = []

    class FakeResponse:
        def raise_for_status(self):
            pass

    monkeypatch.setattr(mgr._client, "post", lambda url, **kw: (posted_urls.append(url), FakeResponse())[1])

    reloaded = mgr.reload_project_sessions(project)
    assert reloaded == 2
    assert sorted(posted_urls) == sorted([
        f"{mgr.base_url}/reload_expr/sess-aaa",
        f"{mgr.base_url}/reload_expr/sess-bbb",
    ])
    # Sessions are NOT evicted — they remain cached for fast reconnect.
    assert len(mgr._sessions) == 3


def test_unit_reload_project_sessions_returns_zero_when_not_running(project: str):
    mgr = BuckarooManager()
    mgr._sessions = {"hash_a": {"session_id": "s1", "project": project}}
    assert mgr.reload_project_sessions(project) == 0


def test_unit_status_shape(project: str):
    mgr = BuckarooManager()
    s = mgr.status()
    assert s["running"] is False
    assert s["port"] is None
    assert s["session_count"] == 0


def test_unit_ensure_session_uses_load_expr_with_xorq_build_dir(project: str, orders_parquet: Path, monkeypatch):
    """When an entry has an xorq_build/ dir (every catalog entry does),
    ensure_session posts to /load_expr so Buckaroo serves it via the
    xorq backend with push-down sort/search rather than paging over a
    materialised parquet."""
    from pydata_xorq import build_and_persist

    res = build_and_persist(project, _code(project))

    mgr = BuckarooManager()
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
    session = mgr.ensure_session(res.content_hash, project)
    assert session == "abc123def456"
    assert captured["url"].endswith("/load_expr"), captured["url"]
    assert captured["json"]["no_browser"] is True
    # The build_dir we POST must already have ${PYDATA_PROJECT_ROOT}
    # expanded — buckaroo's xorq_loading.load_expr_build_dir calls
    # xorq.api.load_expr directly and does no placeholder handling, so
    # an unexpanded build_dir produces a session whose paged reads return
    # zero rows (the bug we hit on 2026-05-20).
    posted_dir = Path(captured["json"]["build_dir"])
    assert posted_dir.is_dir(), posted_dir
    expr_yaml = (posted_dir / "expr.yaml").read_text()
    assert "${PYDATA_PROJECT_ROOT}" not in expr_yaml


# ---------------------------------------------------------------------------
# stress: tmp-dir lifecycle, concurrency
# ---------------------------------------------------------------------------


def test_unit_stop_cleans_expanded_dirs(project: str, orders_parquet: Path, monkeypatch):
    """The tmp dir created by ensure_session must be deleted by stop().

    Without this, a long-lived companion accumulates per-content-hash
    tmp dirs in /var/folders/.../pydata_load_*.
    """
    res = build_and_persist(project, _code(project))

    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "s"}

    monkeypatch.setattr(mgr._client, "post", lambda *a, **kw: FakeResponse())

    mgr.ensure_session(res.content_hash, project)
    tmp = mgr._expanded_dirs[res.content_hash]
    assert tmp.is_dir()

    # Simulate already-exited subprocess so stop()'s early-return path runs;
    # _cleanup_expanded_dirs must fire on that path too.
    mgr.proc = None
    mgr.stop()
    assert not tmp.exists()
    assert mgr._expanded_dirs == {}


def test_unit_stop_cleans_many_expanded_dirs(project: str):
    """Leak guard: stop() must clean every tracked tmp dir, not just one.

    Constructs N synthetic tmp dirs directly (bypassing ensure_session
    for speed — we only care about the cleanup contract here).
    """
    N = 25
    mgr = BuckarooManager()
    tmp_paths = []
    for i in range(N):
        d = Path(tempfile.mkdtemp(prefix="pydata_load_test_"))
        mgr._expanded_dirs[f"hash_{i:02d}"] = d
        tmp_paths.append(d)

    assert all(p.exists() for p in tmp_paths)
    mgr.stop()  # subprocess never started; only tmp-dir cleanup runs

    assert mgr._expanded_dirs == {}
    for p in tmp_paths:
        assert not p.exists(), p


def test_unit_concurrent_same_hash_loads_once(project: str, orders_parquet: Path, monkeypatch):
    """N threads racing on the same content_hash must produce exactly one
    /load_expr POST and one tmp dir — the session_lock serialises."""
    res = build_and_persist(project, _code(project))

    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    post_urls: list[str] = []

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "shared"}

    def fake_post(url, json=None, timeout=None):
        post_urls.append(url)
        # Small sleep so threads have a chance to contend on the lock.
        time.sleep(0.02)
        return FakeResponse()

    monkeypatch.setattr(mgr._client, "post", fake_post)

    results: list[str | None] = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(mgr.ensure_session(res.content_hash, project))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results == ["shared"] * 8
    assert len(post_urls) == 1, post_urls
    assert len(mgr._expanded_dirs) == 1


def test_unit_concurrent_different_hashes_each_load_once(project: str, orders_parquet: Path, monkeypatch):
    """Threads racing on N distinct content_hashes each get their own
    /load_expr POST + tmp dir; no cross-talk through the session_lock."""
    hashes = [
        build_and_persist(project, _code(project) + f"\nexpr = expr.mutate(_v={i})").content_hash for i in range(5)
    ]

    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    post_count = {"n": 0}
    posted_build_dirs: list[str] = []
    posted_lock = threading.Lock()

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "any"}

    def fake_post(url, json=None, timeout=None):
        with posted_lock:
            post_count["n"] += 1
            posted_build_dirs.append(json["build_dir"])
        time.sleep(0.01)
        return FakeResponse()

    monkeypatch.setattr(mgr._client, "post", fake_post)

    barrier = threading.Barrier(len(hashes))

    def worker(h):
        barrier.wait()
        mgr.ensure_session(h, project)

    threads = [threading.Thread(target=worker, args=(h,)) for h in hashes]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert post_count["n"] == len(hashes)
    assert len(set(posted_build_dirs)) == len(hashes)  # one tmp dir per hash
    assert len(mgr._expanded_dirs) == len(hashes)


# ---------------------------------------------------------------------------
# integration: real subprocess + real /load
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_integration_spawn_and_load(project: str, orders_parquet: Path):
    # Build at least one catalog entry so we have a result.parquet to /load.
    res = build_and_persist(project, _code(project))

    # Use port=0 to dodge collisions with anything else on :8700.
    mgr = BuckarooManager(port=0, startup_timeout=15.0)
    try:
        mgr.start()
        assert mgr.is_running
        assert mgr.bound_port is not None and mgr.bound_port > 0
        session = mgr.ensure_session(res.content_hash, project)
        assert session is not None and len(session) >= 16
        # Cached lookup returns the same session id.
        assert mgr.ensure_session(res.content_hash, project) == session
        # Persisted on disk under the new envelope shape.
        sessions_file = json.loads((mgr._session_file_path()).read_text())
        assert sessions_file["sessions"][res.content_hash]["session_id"] == session
        assert sessions_file["sessions"][res.content_hash]["project"] == project
        assert sessions_file["buckaroo_started_at"] is not None
    finally:
        mgr.stop()
    assert not mgr.is_running


@pytest.mark.integration
def test_integration_load_expr_returns_nonzero_rows(project: str, orders_parquet: Path):
    """Smoking-gun integration test for the placeholder-expansion fix.

    Without expansion, /load_expr loads the schema fine (it reads YAML
    metadata) but its `rows` field is 0 — buckaroo's xorq.api.load_expr
    call can't resolve ``${PYDATA_PROJECT_ROOT}`` paths so the underlying
    parquet read returns empty. The original
    ``test_integration_spawn_and_load`` test passed even with that bug
    in place because it only asserted a session id came back. This
    test asserts the actual contract callers care about: data flows.
    """
    res = build_and_persist(project, _code(project))

    mgr = BuckarooManager(port=0, startup_timeout=15.0)
    try:
        mgr.start()
        session_id = mgr.ensure_session(res.content_hash, project)
        assert session_id is not None

        # The tmp dir lives for the manager's lifetime; expr.yaml must be
        # placeholder-free so xorq can read the upstream parquet.
        tmp = mgr._expanded_dirs[res.content_hash]
        expr_yaml = (tmp / "expr.yaml").read_text()
        assert "${PYDATA_PROJECT_ROOT}" not in expr_yaml

        # /load_expr's response surfaces rows — POST a probe directly to
        # inspect it. With unexpanded paths this comes back rows=0.
        resp = httpx.post(
            f"{mgr.base_url}/load_expr",
            json={"build_dir": str(tmp), "no_browser": True},
            timeout=10.0,
        )
        resp.raise_for_status()
        body = resp.json()
        assert body["rows"] > 0, body
        # Same schema buckaroo will report over WS to the embed.
        assert [c["name"] for c in body["columns"]] == ["region", "n"]
    finally:
        mgr.stop()


@pytest.mark.integration
def test_integration_stop_cleans_up(project: str, orders_parquet: Path):
    mgr = BuckarooManager(port=0, startup_timeout=15.0)
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

    def ensure_session(self, content_hash: str, project: str) -> str | None:
        return self.session


def test_entry_detail_embeds_react_widget_when_buckaroo_present(project: str, orders_parquet: Path):
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


def test_entry_detail_falls_back_when_buckaroo_absent(fresh_companion_app, project: str, orders_parquet: Path):
    res = build_and_persist(project, _code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/catalog/{res.content_hash}")
    assert r.status_code == 200
    # The class name may appear inside build_metadata.json's captured git
    # diff (HTML-escaped); check for the actual mount-div tag instead.
    assert _EMBED_TAG not in r.text


def test_entry_detail_falls_back_when_session_unavailable(project: str, orders_parquet: Path):
    """When ensure_session returns None (Buckaroo died or /load failed) the
    page still renders — just falls back to the pandas preview."""
    from pydata_companion import create_app

    res = build_and_persist(project, _code(project))

    class _DownBuckaroo:
        bound_port = None
        is_running = False
        base_url = "http://127.0.0.1:8700"

        def ensure_session(self, content_hash, project):
            return None

    app = create_app(project, buckaroo=_DownBuckaroo())  # type: ignore[arg-type]
    c = TestClient(app)
    r = c.get(f"/catalog/{res.content_hash}")
    assert r.status_code == 200
    assert _EMBED_TAG not in r.text
    assert "data-table" in r.text
