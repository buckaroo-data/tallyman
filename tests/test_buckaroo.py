"""Tests for the Buckaroo lifecycle.

Three layers:
- `test_unit_*`: BuckarooManager with no subprocess (its HTTP client is stubbed). Tallyman keeps no record of Buckaroo's
  sessions (plans/ADR-007-tallyman-owned-materialization.md D6): a session id is derived from the project and the
  content hash, and every open posts. tests/test_buckaroo_handoff.py covers the hand-off itself (forgotten sessions,
  the view build, forced reload, the klass reload of an open grid).
- `test_integration_*`: real subprocess + real /load_expr round-trip.
  Marked slow; run with `pytest -m integration`.
- companion-level: entry_detail with a stub manager that returns a
  fixed session id, verifying the iframe lands in the response.
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_core import entry_dir
from tallyman_core.paths import entry_stat_cache_dir, entry_view_build_dir
from tallyman_xorq import build_and_persist


def _code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def _cheap_code(project: str) -> str:  # a filter over one file: a cheap entry, re-run on every read
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.filter(t.qty > 1)
"""


def _chain_parent_code(project: str) -> str:  # Aggregate → expensive → snapshot
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _chain_child_code(parent: str) -> str:  # cheap child chaining off an expensive parent
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.mutate(total2=t.total * 2)
"""


# ---------------------------------------------------------------------------
# unit: the manager, with its HTTP client stubbed
# ---------------------------------------------------------------------------


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

    from tallyman_companion.buckaroo_lifecycle import BuckarooUnavailable

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


def _running_manager() -> BuckarooManager:
    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()
    return mgr


def test_unit_reload_project_sessions_posts_one_reload_per_entry_of_the_project(
    project: str, orders_parquet: Path, monkeypatch
):
    """Klass hot-reload posts ``/reload_expr/<derived id>`` for every entry of THE project (ADR-007 D6): tallyman keeps
    no record of open sessions, so it asks Buckaroo about each entry. Another project's entries are never touched."""
    from tallyman_cli.fixtures import write_shoe_orders
    from tallyman_core import data_dir, ensure_project

    a = build_and_persist(project, _code(project)).content_hash
    b = build_and_persist(project, _cheap_code(project)).content_hash
    ensure_project("other")
    write_shoe_orders(data_dir("other") / "orders.parquet", n_rows=50, seed=1)
    build_and_persist("other", _code("other"))

    mgr = _running_manager()
    posted_urls: list[str] = []

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

    monkeypatch.setattr(mgr._client, "post", lambda url, **kw: (posted_urls.append(url), FakeResponse())[1])

    reloaded = mgr.reload_project_sessions(project)
    assert reloaded == 2
    assert sorted(posted_urls) == sorted(f"{mgr.base_url}/reload_expr/{mgr.session_id_for(project, h)}" for h in (a, b))


def test_unit_reload_project_sessions_returns_zero_when_not_running(project: str, orders_parquet: Path):
    build_and_persist(project, _code(project))
    mgr = BuckarooManager()
    # is_running is False because we never called start(): nothing is posted, nothing is reloaded.
    assert mgr.reload_project_sessions(project) == 0


def test_unit_reload_project_sessions_skips_sessions_buckaroo_does_not_hold(
    project: str, orders_parquet: Path, monkeypatch
):
    """A 404 (never opened, or idle-evicted) or a 400 (no longer an xorq session) from /reload_expr means "not open"
    (ADR-007 D6): skipped and not counted, and the entry's stat cache is left alone. Only a grid that reloaded has its
    stat cache cleared, so its next request recomputes the stats with the new klass."""
    live = build_and_persist(project, _code(project)).content_hash
    unopened = build_and_persist(project, _cheap_code(project)).content_hash
    not_xorq = build_and_persist(project, _code(project) + "\nexpr = expr.mutate(_v=1)").content_hash
    for h in (live, unopened, not_xorq):
        sentinel = entry_stat_cache_dir(project, h) / "parquet" / "stats.parquet"
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text("stats")

    mgr = _running_manager()
    status_for = {
        mgr.session_id_for(project, live): 200,
        mgr.session_id_for(project, unopened): 404,
        mgr.session_id_for(project, not_xorq): 400,
    }

    class _Response:
        def __init__(self, status_code: int):
            self.status_code = status_code

        def raise_for_status(self):
            pass

    monkeypatch.setattr(mgr._client, "post", lambda url, **kw: _Response(status_for[url.rsplit("/", 1)[1]]))

    assert mgr.reload_project_sessions(project) == 1  # only the live grid reloaded
    assert not (entry_stat_cache_dir(project, live) / "parquet").exists()  # its stale stats are gone
    assert (entry_stat_cache_dir(project, unopened) / "parquet" / "stats.parquet").exists()
    assert (entry_stat_cache_dir(project, not_xorq) / "parquet" / "stats.parquet").exists()


def test_unit_status_shape(project: str):
    mgr = BuckarooManager()
    s = mgr.status()
    assert s["running"] is False
    assert s["port"] is None
    assert "session_count" not in s  # there is no session record to count (ADR-007 D6)


def test_unit_ensure_session_uses_load_expr_with_xorq_build_dir(project: str, orders_parquet: Path, monkeypatch):
    """When an entry has an xorq_build/ dir (every catalog entry does),
    ensure_session posts to /load_expr so Buckaroo serves it via the
    xorq backend with push-down sort/search rather than paging over a
    materialised parquet."""
    from tallyman_xorq import build_and_persist

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
    # The build_dir we POST must already have ${TALLYMAN_PROJECT_ROOT}
    # expanded — buckaroo's xorq_loading.load_expr_build_dir calls
    # xorq.api.load_expr directly and does no placeholder handling, so
    # an unexpanded build_dir produces a session whose paged reads return
    # zero rows (the bug we hit on 2026-05-20).
    posted_dir = Path(captured["json"]["build_dir"])
    assert posted_dir.is_dir(), posted_dir
    expr_yaml = (posted_dir / "expr.yaml").read_text()
    assert "${TALLYMAN_PROJECT_ROOT}" not in expr_yaml


def test_unit_ensure_session_worthy_entry_is_served_from_its_snapshot_no_result_parquet(
    project: str, orders_parquet: Path, monkeypatch
):
    """A worthy entry's grid is handed a build that only READS its snapshot (ADR-007 D6): Buckaroo is never given the
    aggregate to re-run, and no per-entry result.parquet is materialised for the viewer."""
    from tallyman_xorq.materialize import snapshot_path
    from tallyman_xorq.result_cache import cache_worthy

    res = build_and_persist(project, _code(project))  # group_by.aggregate → worthy
    assert cache_worthy(project, res.content_hash) is True

    mgr = _running_manager()
    captured: dict[str, Any] = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "s"}

    def fake_post(url, json=None, timeout=None):
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr(mgr._client, "post", fake_post)
    mgr.ensure_session(res.content_hash, project)

    posted = Path(captured["json"]["build_dir"])
    assert posted.is_dir(), posted
    posted_yaml = (posted / "expr.yaml").read_text()
    assert "op: Aggregate" not in posted_yaml  # nothing for Buckaroo to compute
    assert str(snapshot_path(project, res.content_hash)) in posted_yaml  # the one file it reads
    # No on-demand result.parquet was materialised for the viewer.
    assert not (entry_dir(project, res.content_hash) / "result.parquet").exists()


def test_unit_ensure_session_posts_a_build_over_files_that_exist_on_a_cold_cache(project, orders_parquet, monkeypatch):
    """Everything the posted build reads exists by the time Buckaroo is called, on a cold compute cache.

    Historically the build ensure_session posted had to feed a cold grid with no pre-heal: under ADR-006 D4 the child's
    build carried the parent's cache node, so Buckaroo's replay regenerated the evicted snapshot itself. ADR-007
    retires that (a child's build holds a bare read of the parent's snapshot, and Buckaroo never heals): tallyman makes
    every file exist first (``ensure_materialized``, D5), so the POST goes out over files that are there, and replaying
    the posted build exactly as Buckaroo does (``load_expr`` with no cache directory) reads them and writes nothing.
    """
    import shutil

    from xorq.ibis_yaml.compiler import load_expr

    from tallyman_core.paths import compute_cache_dir
    from tallyman_mcp.server import catalog_create
    from tallyman_xorq.materialize import snapshot_path
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    parent_h = catalog_create("agg", _chain_parent_code(project))["hash"]  # expensive parent (Aggregate)
    child_h = catalog_create("chain", _chain_child_code("agg"))["hash"]  # cheap child chained off it

    # Cold start, as a fresh clone / not-yet-warmed entry has: evict every file under compute_cache (the parent's
    # snapshot and the ordered copy of the source under it) and clear the plan memo the in-process build warmed.
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()

    mgr = _running_manager()
    posted: dict = {}
    parent_existed_at_post: list[bool] = []

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "sess-cold-heal"}

    def _capture_post(url, json=None, timeout=None):
        parent_existed_at_post.append(snapshot_path(project, parent_h).is_file())
        posted.update(json or {})
        return _Resp()

    monkeypatch.setattr(mgr._client, "post", _capture_post)

    mgr.ensure_session(child_h, project)

    assert posted.get("build_dir"), "no /load_expr POST captured"
    assert parent_existed_at_post == [True], "the parent's snapshot must be made again before Buckaroo is called"

    # Replay the posted build exactly as Buckaroo does: load it with no cache directory and execute. It reads files
    # that exist and writes nothing under compute_cache.
    before = {str(p) for p in compute_cache_dir(project).rglob("*") if p.is_file()}
    replayed = load_expr(posted["build_dir"])
    assert int(replayed.count().execute()) > 0
    assert {str(p) for p in compute_cache_dir(project).rglob("*") if p.is_file()} == before


# ---------------------------------------------------------------------------
# stress: tmp-dir lifecycle, concurrency
# ---------------------------------------------------------------------------


def test_unit_ensure_session_writes_stable_expanded_dir(project: str, orders_parquet: Path, monkeypatch):
    """A cheap entry is handed its own build, expanded into a stable per-entry .xorq_build_expanded dir.

    The expansion used to go to a random mkdtemp tracked in _expanded_dirs and
    deleted by stop(); it now lives under the (immutable, content-addressed)
    entry dir so the path is identical across restarts — Buckaroo's stat-cache keys include the build directory's path,
    so a stable path is what lets them hit. It must therefore survive stop(). (A worthy entry's stable directory is its
    view build, under .xorq_view_build: tests/test_buckaroo_handoff.py.)
    """
    res = build_and_persist(project, _cheap_code(project))

    mgr = _running_manager()
    posted: list[str] = []

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "s"}

    def fake_post(url, json=None, timeout=None):
        posted.append(json["build_dir"])
        return FakeResponse()

    monkeypatch.setattr(mgr._client, "post", fake_post)

    mgr.ensure_session(res.content_hash, project)
    expanded = entry_dir(project, res.content_hash) / ".xorq_build_expanded"
    assert expanded.is_dir()
    assert posted == [str(expanded)]

    # Simulate already-exited subprocess so stop()'s early-return path runs.
    mgr.proc = None
    mgr.stop()
    # The expansion is reused across restarts — stop() must NOT delete it.
    assert expanded.is_dir()


def test_unit_ensure_session_heals_partial_expansion(project: str, orders_parquet: Path, monkeypatch):
    """A crash mid-expansion must not poison the stable expanded dir forever.

    The expansion writes files into .xorq_build_expanded; a kill/OOM partway
    leaves a half-written dir. The old ``if not expanded.exists()`` guard then
    treated that truncated dir as complete and fed buckaroo a build_dir missing
    files (zero rows), or — if a subdir landed half-copied — raised
    FileExistsError out of the lock on every later load. ensure_session must key
    off a completion marker written last, and re-expand when it's absent.
    """
    res = build_and_persist(project, _cheap_code(project))

    mgr = _running_manager()

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "s"}

    monkeypatch.setattr(mgr._client, "post", lambda *a, **kw: FakeResponse())

    # Simulate an interrupted expansion: a dir with stray contents, no marker.
    expanded = entry_dir(project, res.content_hash) / ".xorq_build_expanded"
    marker = entry_dir(project, res.content_hash) / ".xorq_build_expanded.complete"
    expanded.mkdir(parents=True)
    (expanded / "stale_partial.txt").write_text("interrupted")
    assert not marker.exists()

    mgr.ensure_session(res.content_hash, project)

    # Healed: the stray file is gone, the marker exists, and every file from the
    # source build_dir was expanded in.
    assert marker.exists()
    assert not (expanded / "stale_partial.txt").exists()
    build_dir = entry_dir(project, res.content_hash) / "xorq_build"
    for item in build_dir.iterdir():
        assert (expanded / item.name).exists(), item.name


def test_unit_diff_sessions_invalidated_on_buckaroo_restart(project: str):
    """A buckaroo restart must drop loaded diff-compare sessions.

    Buckaroo's /load_expr sessions live only in the subprocess's RAM, so a
    restart (fresh ``started_at`` from /health) invalidates every session_id.
    Entry sessions need no record on tallyman's side (their ids are derived and every open
    posts, ADR-007 D6), but the live diff still keeps one: the bookkeeping used to be a module global that nothing
    reset, so a post-restart diff view skipped the re-POST and handed the client a session the new process never
    loaded.
    """
    mgr = BuckarooManager()
    mgr._buckaroo_started_at = 1000.0
    mgr.mark_diff_session_loaded("diff-aaaa-bbbb")
    assert mgr.diff_session_is_loaded("diff-aaaa-bbbb")

    # Same started_at → not a restart → bookkeeping preserved.
    mgr._reset_session_bookkeeping_if_restarted(1000.0)
    assert mgr.diff_session_is_loaded("diff-aaaa-bbbb")

    # Fresh started_at → restart → the diff sessions are dropped.
    mgr._reset_session_bookkeeping_if_restarted(2000.0)
    assert not mgr.diff_session_is_loaded("diff-aaaa-bbbb")


def test_unit_stop_cleans_many_expanded_dirs(project: str):
    """Leak guard: stop() must clean every tracked tmp dir, not just one.

    Constructs N synthetic tmp dirs directly (bypassing ensure_session
    for speed — we only care about the cleanup contract here).
    """
    N = 25
    mgr = BuckarooManager()
    tmp_paths = []
    for i in range(N):
        d = Path(tempfile.mkdtemp(prefix="tallyman_load_test_"))
        mgr._expanded_dirs[f"hash_{i:02d}"] = d
        tmp_paths.append(d)

    assert all(p.exists() for p in tmp_paths)
    mgr.stop()  # subprocess never started; only tmp-dir cleanup runs

    assert mgr._expanded_dirs == {}
    for p in tmp_paths:
        assert not p.exists(), p


@pytest.mark.parametrize("recipe", [_code, _cheap_code], ids=["worthy", "cheap"])
def test_unit_concurrent_opens_of_one_entry_share_one_intact_build_dir(
    project: str, orders_parquet: Path, monkeypatch, recipe
):
    """N threads opening the same entry at once all get its derived session id and are all handed the SAME build
    directory, complete (ADR-007 D6).

    Tallyman keeps no session map, so every open posts (Buckaroo's own short-circuit makes a repeat post cheap) and
    there is no session lock to serialise on. What must hold is that racing writers of the stable build dir (the view
    build of a worthy entry, the expanded build of a cheap one) never hand Buckaroo a half-written one.
    """
    res = build_and_persist(project, recipe(project))

    mgr = _running_manager()
    posted: list[dict] = []

    class FakeResponse:
        def __init__(self, session: str):
            self._session = session

        def raise_for_status(self):
            pass

        def json(self):
            return {"session": self._session}

    def fake_post(url, json=None, timeout=None):
        posted.append(json)
        # Small sleep so threads have a chance to contend.
        time.sleep(0.02)
        return FakeResponse(json["session"])

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

    assert results == [mgr.session_id_for(project, res.content_hash)] * 8
    assert len(posted) == 8  # every open posts: tallyman remembers nothing
    build_dirs = {body["build_dir"] for body in posted}
    assert len(build_dirs) == 1, build_dirs
    (build_dir,) = build_dirs
    assert (Path(build_dir) / "expr.yaml").is_file()


def test_unit_concurrent_different_hashes_each_load_once(project: str, orders_parquet: Path, monkeypatch):
    """Threads racing on N distinct content_hashes each get their own
    /load_expr POST, session id and build dir; no cross-talk.
    """
    hashes = [
        build_and_persist(project, _code(project) + f"\nexpr = expr.mutate(_v={i})").content_hash
        for i in range(5)
    ]

    mgr = _running_manager()

    posted: list[dict] = []
    posted_lock = threading.Lock()

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "any"}

    def fake_post(url, json=None, timeout=None):
        with posted_lock:
            posted.append(json)
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

    assert len(posted) == len(hashes)
    assert {body["session"] for body in posted} == {mgr.session_id_for(project, h) for h in hashes}
    assert len({body["build_dir"] for body in posted}) == len(hashes)  # one build dir per hash
    assert all(entry_view_build_dir(project, h).is_dir() for h in hashes)  # these are worthy: a view build each


# ---------------------------------------------------------------------------
# integration: real subprocess + real /load
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_integration_spawn_and_load(project: str, orders_parquet: Path, isolated_home: Path):
    # Build at least one catalog entry so there is a build to /load_expr.
    res = build_and_persist(project, _code(project))

    # Use port=0 to dodge collisions with anything else on :8700.
    mgr = BuckarooManager(port=0, startup_timeout=15.0)
    try:
        mgr.start()
        assert mgr.is_running
        assert mgr.bound_port is not None and mgr.bound_port > 0
        session = mgr.ensure_session(res.content_hash, project)
        # The id is derived from the project and the hash (ADR-007 D6), and Buckaroo honours the id it is given.
        assert session == mgr.session_id_for(project, res.content_hash)
        # A repeat open posts again and gets the same id: Buckaroo skips the work while it holds the session.
        assert mgr.ensure_session(res.content_hash, project) == session
        # Tallyman keeps no record of the session anywhere.
        assert not list(isolated_home.rglob("buckaroo_sessions.json"))
    finally:
        mgr.stop()
    assert not mgr.is_running


@pytest.mark.integration
def test_integration_load_expr_returns_nonzero_rows(project: str, orders_parquet: Path):
    """Smoking-gun integration test for the placeholder-expansion fix.

    Without expansion, /load_expr loads the schema fine (it reads YAML
    metadata) but its `rows` field is 0 — buckaroo's xorq.api.load_expr
    call can't resolve ``${TALLYMAN_PROJECT_ROOT}`` paths so the underlying
    parquet read returns empty. The original
    ``test_integration_spawn_and_load`` test passed even with that bug
    in place because it only asserted a session id came back. This
    test asserts the actual contract callers care about: data flows, for the
    build tallyman hands Buckaroo (a worthy entry's view build of its snapshot;
    a cheap entry's own expanded build).
    """
    worthy = build_and_persist(project, _code(project))
    cheap = build_and_persist(project, _cheap_code(project))

    mgr = BuckarooManager(port=0, startup_timeout=15.0)
    try:
        mgr.start()
        for res in (worthy, cheap):
            assert mgr.ensure_session(res.content_hash, project) is not None

        # A worthy entry: Buckaroo reads the snapshot through the view build, whose columns are the snapshot's, and
        # __row_order is the last of them.
        view = entry_view_build_dir(project, worthy.content_hash)
        assert "${TALLYMAN_PROJECT_ROOT}" not in (view / "expr.yaml").read_text()
        # /load_expr's response surfaces rows — POST a probe directly to inspect it. With unexpanded paths this comes
        # back rows=0.
        resp = httpx.post(f"{mgr.base_url}/load_expr", json={"build_dir": str(view), "no_browser": True}, timeout=10.0)
        resp.raise_for_status()
        body = resp.json()
        assert body["rows"] > 0, body
        # Same schema buckaroo will report over WS to the embed.
        assert [c["name"] for c in body["columns"]] == ["region", "n", "__row_order"]

        # A cheap entry: the expansion lives in the entry's stable .xorq_build_expanded dir; expr.yaml must be
        # placeholder-free so xorq can read the upstream ordered copy.
        expanded = entry_dir(project, cheap.content_hash) / ".xorq_build_expanded"
        assert "${TALLYMAN_PROJECT_ROOT}" not in (expanded / "expr.yaml").read_text()
        resp = httpx.post(
            f"{mgr.base_url}/load_expr", json={"build_dir": str(expanded), "no_browser": True}, timeout=10.0
        )
        resp.raise_for_status()
        body = resp.json()
        assert body["rows"] > 0, body
        assert [c["name"] for c in body["columns"]][-1] == "__row_order"
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

    def ensure_session(self, content_hash: str, project: str, column_config_overrides=None) -> str | None:
        return self.session

    def load_session(self, content_hash: str, project: str, column_config_overrides=None) -> dict:
        return {"status": "ok", "session_id": self.session, "detail": ""}


def test_entry_detail_decoupled_grid_session_via_session_endpoint(project: str, orders_parquet: Path):
    """The detail payload is metadata-only; the grid session loads lazily via
    /api/session so the detail request never blocks on Buckaroo (#133)."""
    from tallyman_companion import create_app

    res = build_and_persist(project, _code(project))
    bk: Any = _StubBuckaroo(session="abc123", port=8700)
    app = create_app(project, buckaroo=bk)
    c = TestClient(app)

    # Detail no longer carries a session — it's decoupled.
    body = c.get(f"/{project}/api/entry/{res.content_hash}").json()
    assert body["buckaroo_session"] is None
    assert body["buckaroo_ws_base"] == "ws://127.0.0.1:8700"

    # The session endpoint provides the widget on demand, with a typed status.
    sr = c.get(f"/{project}/api/session/{res.content_hash}").json()
    assert sr["status"] == "ok"
    assert sr["ws_url"] == "ws://127.0.0.1:8700/ws/abc123"


def test_entry_detail_falls_back_when_session_unavailable(project: str, orders_parquet: Path):
    """When ensure_session returns None, API returns null for buckaroo fields."""
    from tallyman_companion import create_app

    res = build_and_persist(project, _code(project))

    class _DownBuckaroo:
        bound_port = None
        is_running = False
        base_url = "http://127.0.0.1:8700"

        def ensure_session(self, content_hash, project, column_config_overrides=None):
            return None

    app = create_app(project, buckaroo=_DownBuckaroo())  # type: ignore[arg-type]
    c = TestClient(app)
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
    body = r.json()
    assert body["buckaroo_session"] is None
    assert body["buckaroo_ws_base"] is None
