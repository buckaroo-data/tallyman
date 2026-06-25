from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_core.events import read_events, record_event
from tallyman_mcp.server import catalog_create, catalog_revise, catalog_run

# The project activity log (events.jsonl) is a linear, cross-session feed: MCP
# build runs (success/failure with a full traceback), alias creations, and
# Buckaroo grid loads — each stamped with the MCP process's session id so
# multiple Claude sessions writing one project stay distinguishable. Read by
# GET /{project}/api/log for the UI's filterable, expandable log. Rendering is
# the SPA's job; these pin the event/JSON contract.

_BAD = "x = 5  # never binds `expr` → BuildError"


def _ok_code(project: str, cols: str = '"region", "price"') -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select({cols})
"""


def test_build_error_records_event_with_traceback(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_run(_BAD)
    assert "error" in out

    err = [e for e in read_events(project, categories=["mcp"]) if e["kind"] == "build_error"]
    assert err, "a build_error event was recorded"
    e = err[0]
    assert e["category"] == "mcp"
    assert e["session"]  # stamped with the MCP session id
    assert e["traceback"]  # full stacktrace, not just the one-line message
    assert e["error_id"]
    assert e["tool"] == "catalog_run"
    assert e["code"] == _BAD


def test_alias_create_and_revise_record_alias_events(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoes", _ok_code(project))
    catalog_revise("shoes", _ok_code(project, cols='"region"'))  # different projection → V2

    aliases = [e for e in read_events(project, categories=["alias"]) if e["alias"] == "shoes"]
    assert len(aliases) >= 2
    assert {a["tool"] for a in aliases} >= {"catalog_create", "catalog_revise"}
    assert all(a["session"] for a in aliases)
    assert min(a["version"] for a in aliases) == 1


def test_api_log_filters_by_category_and_lists_sessions(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_run(_BAD)  # mcp build_error
    catalog_create("good", _ok_code(project))  # alias_set
    record_event(project, "buckaroo", origin="companion", status="ok", hash="deadbeef", load_ms=12.3)

    c = TestClient(fresh_companion_app)
    body = c.get(f"/{project}/api/log").json()
    kinds = {e["kind"] for e in body["events"]}
    assert {"build_error", "alias_set", "buckaroo"} <= kinds
    assert body["sessions"] and body["sessions"][0]["session"]

    only_alias = c.get(f"/{project}/api/log?categories=alias").json()["events"]
    assert only_alias and all(e["category"] == "alias" for e in only_alias)

    # Newest-first ordering.
    ts = [e["ts"] for e in body["events"]]
    assert ts == sorted(ts, reverse=True)


def test_events_are_session_filterable(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    record_event(project, "build_ok", session="aaa", tool="catalog_run", hash="h1")
    record_event(project, "build_ok", session="bbb", tool="catalog_run", hash="h2")
    got = read_events(project, sessions=["aaa"])
    assert got and all(e["session"] == "aaa" for e in got)
