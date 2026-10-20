"""The companion's blocking-action tracker, the lifecycle sites that use it, and the timing endpoint.

A blocking action is announced before it runs (an ``action_start`` event, so the UI can say what the companion is
about to wait on) and again when it ends (``action_end``, carrying the name and duration, so the UI can show the last
action). ``GET /{project}/api/timing`` returns the companion's actions beside the spans Buckaroo posted back, on one
clock, so a request that sat in Buckaroo's queue shows up as an action that overlaps another session's stats run.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from contextlib import contextmanager

import httpx
import pytest
from fastapi.testclient import TestClient

from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_core import get_alias
from tallyman_core.telemetry import read_spans, record_span
from tallyman_mcp.server import catalog_create


@pytest.fixture
def tracker():
    from tallyman_core.activity import tracker as global_tracker

    global_tracker.reset()
    yield global_tracker
    global_tracker.reset()


def _fresh(keep=200):
    from tallyman_core.activity import ActivityTracker

    return ActivityTracker(keep=keep)


# ---------------------------------------------------------------------------
# the tracker
# ---------------------------------------------------------------------------


def test_running_while_the_body_runs_then_recent_with_a_duration():
    t = _fresh()
    with t.track("prepare entry", "abc123"):
        snap = t.snapshot()
        assert [a["name"] for a in snap["running"]] == ["prepare entry"]
        assert snap["running"][0]["ended_ms"] is None
        assert snap["running"][0]["status"] == "running"
        time.sleep(0.01)
    snap = t.snapshot()
    assert snap["running"] == []
    done = snap["recent"][0]
    assert (done["name"], done["detail"], done["status"]) == ("prepare entry", "abc123", "ok")
    assert done["ended_ms"] >= done["started_ms"] + 10


def test_start_is_published_before_the_body_runs_and_end_after():
    t = _fresh()
    seen: list[str] = []
    t.add_listener(lambda ev: seen.append(ev["kind"]))
    with t.track("x"):
        assert seen == ["action_start"]
    assert seen == ["action_start", "action_end"]


def test_end_event_names_the_action_and_carries_its_duration():
    t = _fresh()
    events: list[dict] = []
    t.add_listener(events.append)
    with t.track("buckaroo load_expr", "abc123"):
        time.sleep(0.01)
    start, end = events
    assert start["action"]["id"] == end["action"]["id"]
    assert end["action"]["name"] == "buckaroo load_expr"
    assert end["action"]["ended_ms"] - end["action"]["started_ms"] >= 10


def test_an_exception_propagates_and_is_recorded_as_an_error():
    t = _fresh()
    with pytest.raises(ValueError, match="boom"):
        with t.track("x"):
            raise ValueError("boom")
    done = t.snapshot()["recent"][0]
    assert done["status"] == "error"
    assert "boom" in done["error"]
    assert t.snapshot()["running"] == []


def test_recent_is_newest_first_and_bounded():
    t = _fresh(keep=3)
    for i in range(5):
        with t.track(f"a{i}"):
            pass
    assert [a["name"] for a in t.snapshot()["recent"]] == ["a4", "a3", "a2"]


def test_concurrent_actions_are_all_running():
    t = _fresh()
    with t.track("one"):
        with t.track("two"):
            assert sorted(a["name"] for a in t.snapshot()["running"]) == ["one", "two"]
    assert t.snapshot()["running"] == []


def test_a_listener_that_raises_does_not_break_the_action():
    t = _fresh()

    def bad(_ev):
        raise RuntimeError("listener bug")

    t.add_listener(bad)
    with t.track("x"):
        pass
    assert t.snapshot()["recent"][0]["status"] == "ok"


def test_a_removed_listener_hears_nothing():
    t = _fresh()
    seen: list[dict] = []
    t.add_listener(seen.append)
    t.remove_listener(seen.append)
    with t.track("x"):
        pass
    assert seen == []


# ---------------------------------------------------------------------------
# the sites that announce their blocking work
# ---------------------------------------------------------------------------


class _AliveProc:
    def poll(self):
        return None


class _RaisingClient:
    def __init__(self, exc):
        self._exc = exc

    def post(self, *a, **k):
        raise self._exc


class _OkResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {"session": "sess-1"}


class _OkClient:
    def post(self, *a, **k):
        return _OkResponse()


def _entry(project):
    catalog_create(
        "shoe_sales",
        f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
""",
    )
    return get_alias(project, "shoe_sales")


def _bk(monkeypatch, client):
    bk = BuckarooManager()
    bk.proc = _AliveProc()
    bk.bound_port = 8799
    monkeypatch.setattr(bk, "_maybe_restart", lambda: None)
    bk._client = client
    return bk


def test_load_session_announces_preparing_the_entry_then_the_post(project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _entry(project)
    started: list[str] = []
    tracker.add_listener(lambda ev: started.append(ev["action"]["name"]) if ev["kind"] == "action_start" else None)
    assert _bk(monkeypatch, _OkClient()).load_session(h, project)["status"] == "ok"
    assert started == ["prepare entry", "buckaroo load_expr"]
    last = tracker.snapshot()["recent"][0]
    assert last["name"] == "buckaroo load_expr"
    assert last["detail"] == h[:12]
    assert last["status"] == "ok"


def test_a_timed_out_post_is_recorded_as_an_error_action(project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _entry(project)
    bk = _bk(monkeypatch, _RaisingClient(httpx.TimeoutException("timed out")))
    assert bk.load_session(h, project)["status"] == "timeout"
    last = tracker.snapshot()["recent"][0]
    assert (last["name"], last["status"]) == ("buckaroo load_expr", "error")


def test_the_staleness_scan_is_announced(fresh_companion_app, project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _entry(project)
    TestClient(fresh_companion_app).get(f"/{project}/api/staleness")
    assert "scan staleness" in [a["name"] for a in tracker.snapshot()["recent"]]


# ---------------------------------------------------------------------------
# GET /{project}/api/timing
# ---------------------------------------------------------------------------


def _timing(app, project, **params):
    r = TestClient(app).get(f"/{project}/api/timing", params=params)
    assert r.status_code == 200
    return r.json()


def test_timing_lists_running_and_finished_actions(fresh_companion_app, project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    with tracker.track("done one"):
        pass
    with tracker.track("still running"):
        body = _timing(fresh_companion_app, project)
    by_name = {a["name"]: a for a in body["actions"]}
    assert by_name["done one"]["status"] == "ok"
    assert by_name["still running"]["status"] == "running"
    assert by_name["still running"]["ended_ms"] is None
    assert abs(body["now_ms"] - time.time() * 1000) < 5000


def test_timing_returns_buckaroo_spans_in_the_window(fresh_companion_app, project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    now = time.time() * 1000
    span = {"trace": "entry-test-aaa", "source": "server", "attrs": {"stats_gen": 1}}
    record_span(project, {**span, "name": "stats.complete", "t_start_ms": now - 5000, "t_end_ms": now - 1000})
    record_span(project, {**span, "name": "stats.complete", "t_start_ms": now - 3_600_000, "t_end_ms": now - 3_599_000})
    body = _timing(fresh_companion_app, project, window_s=600)
    assert [(s["name"], s["trace"]) for s in body["spans"]] == [("stats.complete", "entry-test-aaa")]
    assert body["spans"][0]["t_end_ms"] - body["spans"][0]["t_start_ms"] == 4000


def test_timing_window_also_bounds_the_actions(fresh_companion_app, project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core import activity

    real_now = activity._now_ms
    monkeypatch.setattr(activity, "_now_ms", lambda: real_now() - 3_600_000)  # an action an hour ago
    with tracker.track("old"):
        pass
    monkeypatch.setattr(activity, "_now_ms", real_now)
    with tracker.track("fresh"):
        pass
    names = [a["name"] for a in _timing(fresh_companion_app, project, window_s=600)["actions"]]
    assert "fresh" in names and "old" not in names


def test_timing_skips_a_span_record_whose_times_are_not_numbers(
    fresh_companion_app, project, orders_src, monkeypatch, tracker
):
    """POST /api/telemetry stores any dict, so one malformed record must not make every later read fail."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    now = time.time() * 1000
    span = {"trace": "entry-test-aaa", "source": "server", "name": "stats.complete"}
    record_span(project, {**span, "t_start_ms": now - 2000, "t_end_ms": None})
    record_span(project, {**span, "t_start_ms": now - 2000, "t_end_ms": "soon"})
    record_span(project, {**span, "t_end_ms": now - 1000})
    record_span(project, {**span, "t_start_ms": now - 5000, "t_end_ms": now - 1000})
    body = _timing(fresh_companion_app, project, window_s=600)
    assert [(s["t_start_ms"], s["t_end_ms"]) for s in body["spans"]] == [(now - 5000, now - 1000)]


# ---------------------------------------------------------------------------
# reading only the recent end of telemetry.jsonl
# ---------------------------------------------------------------------------


def test_reading_recent_spans_parses_only_the_tail_of_the_file(project, monkeypatch):
    from tallyman_core.telemetry import _telemetry_path

    now = time.time()
    old = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(now - 86_400))
    path = _telemetry_path(project)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps({"received_at": old, "name": "stats.complete", "n": i}) + "\n" for i in range(3000))
    )
    record_span(project, {"name": "stats.complete", "n": "recent-1"})
    record_span(project, {"name": "stats.complete", "n": "recent-2"})

    parsed = []
    real_loads = json.loads
    monkeypatch.setattr(json, "loads", lambda *a, **k: parsed.append(1) or real_loads(*a, **k))
    spans = read_spans(project, received_since_ms=(now - 600) * 1000)
    monkeypatch.undo()

    assert sorted(s["n"] for s in spans) == ["recent-1", "recent-2"]
    assert len(parsed) < 100  # not the 3002 lines in the file


# ---------------------------------------------------------------------------
# a refused request is not an action
# ---------------------------------------------------------------------------


def test_a_request_refused_before_any_work_is_not_announced(project, orders_src, monkeypatch, tracker):
    from tallyman_companion import create_app

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    started: list[dict] = []
    listener = lambda ev: started.append(ev) if ev["kind"] == "action_start" else None  # noqa: E731
    tracker.add_listener(listener)
    try:
        read_only = TestClient(create_app(project, read_only=True))
        assert read_only.put(f"/{project}/api/code/shoe_sales", json={"code": "x"}).status_code == 403
        assert read_only.post(f"/{project}/api/recalc", json={}).status_code == 403
        assert TestClient(create_app(project)).get("/no_such_project/api/staleness").status_code == 404
    finally:
        tracker.remove_listener(listener)
    assert started == []
    assert tracker.snapshot()["recent"] == []


# ---------------------------------------------------------------------------
# the other POSTs that wait in Buckaroo's queue
# ---------------------------------------------------------------------------


class _StatusResponse(_OkResponse):
    status_code = 200


class _StatusClient:
    def post(self, *a, **k):
        return _StatusResponse()


def test_a_forced_reload_is_announced_like_a_load(project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _entry(project)
    assert _bk(monkeypatch, _OkClient()).force_reload_session(project, h) is True
    last = tracker.snapshot()["recent"][0]
    assert (last["name"], last["detail"], last["status"]) == ("buckaroo load_expr", h[:12], "ok")


def test_reloading_the_klasses_of_open_sessions_is_announced(project, orders_src, monkeypatch, tracker):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _entry(project)
    _bk(monkeypatch, _StatusClient()).reload_project_sessions(project)
    last = tracker.snapshot()["recent"][0]
    assert (last["name"], last["project"], last["status"]) == ("buckaroo reload_expr", project, "ok")


def test_the_diff_view_load_is_announced_with_its_session_id(project, monkeypatch, tracker):
    from pathlib import Path

    import tallyman_companion.app as companion_app

    view = type(
        "View",
        (),
        {"identity": "c" * 16, "build_dir": Path("/nowhere"), "column_config_overrides": {}},
    )()
    monkeypatch.setattr(companion_app, "open_diff_view", lambda *a, **k: view)

    class _Buckaroo:
        base_url = "http://buckaroo.invalid"
        _client = _StatusClient()
        loaded: list[str] = []

        def diff_session_is_loaded(self, sid):
            return False

        def mark_diff_session_loaded(self, sid):
            self.loaded.append(sid)

    out = companion_app._load_compare_view(_Buckaroo(), project, "a" * 16, "b" * 16, ["id"], None, None)
    assert out is not None
    last = tracker.snapshot()["recent"][0]
    assert (last["name"], last["detail"]) == ("buckaroo load_expr", out[1])


# ---------------------------------------------------------------------------
# an announced route must not hold the event loop through its work
# ---------------------------------------------------------------------------


def _ticks_while_requesting(app, method, url, **kw):
    """(status, seconds the request took, ticks of a 10 ms heartbeat on the app's loop while it ran)."""

    async def go():
        ticks: list[float] = []
        done = asyncio.Event()

        async def beat():
            while not done.is_set():
                ticks.append(time.monotonic())
                await asyncio.sleep(0.01)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://companion") as client:
            heartbeat = asyncio.create_task(beat())
            await asyncio.sleep(0.05)
            t0 = time.monotonic()
            resp = await client.request(method, url, **kw)
            t1 = time.monotonic()
            done.set()
            await heartbeat
        return resp.status_code, t1 - t0, sum(1 for t in ticks if t0 <= t <= t1)

    return asyncio.run(go())


SLOW = 0.4


class _SlowBuckaroo:
    """Stands in for BuckarooManager where a route reloads the open sessions."""

    is_running = False

    def reload_project_sessions(self, project):
        time.sleep(SLOW)
        return 0


def _slow_save(monkeypatch, project):
    import tallyman_xorq
    from tallyman_xorq.build import BuildError

    def slow_build(*a, **k):
        time.sleep(SLOW)
        raise BuildError("stop here")

    monkeypatch.setattr(tallyman_xorq, "build_and_persist", slow_build)
    return "PUT", f"/{project}/api/code/shoe_sales", {"json": {"code": "expr = 1"}}, 400


def _slow_promote(monkeypatch, project):
    import tallyman_companion.app as companion_app

    monkeypatch.setattr(companion_app, "history_for", lambda p, alias: ["a" * 16, "b" * 16])
    monkeypatch.setattr(companion_app, "diff_keys", lambda *a, **k: time.sleep(SLOW) or [])
    return "POST", f"/{project}/api/promote_diff/shoe_sales/1/2", {}, 400


def _slow_reset(monkeypatch, project):
    from tallyman_core import catalog_state

    monkeypatch.setattr(catalog_state, "reset_to", lambda *a, **k: None)
    return "POST", f"/{project}/api/reset", {"json": {"ref": 1}}, 200


def _slow_notify(monkeypatch, project):
    from tallyman_core.paths import tallyman_home

    return "POST", "/internal/notify", {"json": {"kind": "post_processing_changed", "home": str(tallyman_home())}}, 200


@pytest.mark.parametrize("scenario", [_slow_save, _slow_promote, _slow_reset, _slow_notify])
def test_the_loop_keeps_running_while_an_announced_route_does_its_blocking_work(
    scenario, project, orders_src, isolated_home, monkeypatch, tracker
):
    from tallyman_companion import create_app

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _entry(project)
    method, url, kw, expected = scenario(monkeypatch, project)
    app = create_app(project, buckaroo=_SlowBuckaroo())
    status, seconds, ticks = _ticks_while_requesting(app, method, url, **kw)
    assert status == expected
    assert seconds >= SLOW * 0.9, "the slow step did not run"
    # 10 ms heartbeat over >= 0.4 s: a loop held by the work sees one or two of them, a free loop about forty.
    assert ticks >= 10


# ---------------------------------------------------------------------------
# over a real SSE stream
# ---------------------------------------------------------------------------


@contextmanager
def _live_companion(app):
    import uvicorn

    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while not server.started:
            assert time.monotonic() < deadline, "the companion did not start"
            time.sleep(0.02)
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        server.should_exit = True
        thread.join(timeout=15)
        sock.close()


def _sse_events(lines):
    """(event name, parsed data) for each SSE event on the stream that is not a keep-alive ping."""
    name = None
    for line in lines:
        if line.startswith("event:"):
            name = line.split(":", 1)[1].strip()
        elif line.startswith("data:") and name is not None:
            if name != "ping":
                yield name, json.loads(line.split(":", 1)[1])
            name = None


def test_hello_carries_this_projects_running_and_finished_actions(fresh_companion_app, project, tracker):
    """The first event on a stream is the snapshot a reconnecting tab replaces its header state with, so an
    action_end missed while disconnected cannot leave a stale 'now:' behind."""
    with tracker.track("done one", "", project):
        pass
    with tracker.track("in another project", "", "elsewhere"):
        pass
    with _live_companion(fresh_companion_app) as base:
        with tracker.track("still running", "", project):
            with httpx.Client(timeout=30.0) as client, client.stream("GET", f"{base}/{project}/api/sse") as stream:
                name, data = next(_sse_events(stream.iter_lines()))
    assert name == "hello"
    activity = data["activity"]
    assert [a["name"] for a in activity["running"]] == ["still running"]
    assert [a["name"] for a in activity["recent"]] == ["done one"]


def test_a_slow_save_is_announced_while_it_runs(fresh_companion_app, project, orders_src, monkeypatch, tracker):
    """The 'now:' pill needs the action_start to reach the browser before the work finishes, not alongside the end."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _entry(project)
    method, url, kw, expected = _slow_save(monkeypatch, project)
    with _live_companion(fresh_companion_app) as base:
        with httpx.Client(timeout=30.0) as client, client.stream("GET", f"{base}/{project}/api/sse") as stream:
            events = _sse_events(stream.iter_lines())
            assert next(events)[0] == "hello"
            finished: dict = {}

            def save():
                with httpx.Client(timeout=30.0) as saver:
                    finished["status"] = saver.request(method, base + url, **kw).status_code
                finished["at"] = time.monotonic()

            worker = threading.Thread(target=save)
            worker.start()
            name, data = next(events)
            heard_at = time.monotonic()
            worker.join(timeout=30)
    assert name == "action_start", f"the first event after hello was {name!r}: the start was held behind the work"
    assert data["action"]["name"] == "save code"
    assert finished["status"] == expected
    assert finished["at"] - heard_at >= SLOW * 0.5, "the start was held until the work was done"
