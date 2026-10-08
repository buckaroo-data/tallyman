"""The companion's blocking-action tracker, the lifecycle sites that use it, and the timing endpoint.

A blocking action is announced before it runs (an ``action_start`` event, so the UI can say what the companion is
about to wait on) and again when it ends (``action_end``, carrying the name and duration, so the UI can show the last
action). ``GET /{project}/api/timing`` returns the companion's actions beside the spans Buckaroo posted back, on one
clock, so a request that sat in Buckaroo's queue shows up as an action that overlaps another session's stats run.
"""

from __future__ import annotations

import time

import httpx
import pytest
from fastapi.testclient import TestClient

from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_core import get_alias
from tallyman_core.telemetry import record_span
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
