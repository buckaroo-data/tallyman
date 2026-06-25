from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_core.errors import record_error
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


def test_api_log_backfills_errors_from_errors_jsonl(fresh_companion_app, project, orders_parquet, monkeypatch):
    # The Log tab must show build failures even when no event was recorded for
    # them — e.g. an older MCP that wrote errors.jsonl but not events.jsonl.
    # /api/log backfills build_error from errors.jsonl (#log), marked history.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    rec = record_error(project, code="expr = undefined_zzz", message="boom", tool="catalog_run", prompt="p")

    c = TestClient(fresh_companion_app)
    body = c.get(f"/{project}/api/log").json()
    be = [e for e in body["events"] if e["kind"] == "build_error" and e.get("error_id") == rec["id"]]
    assert be, "errors.jsonl failure backfilled into the log"
    assert be[0]["category"] == "mcp"
    assert be[0]["message"] == "boom"
    assert be[0]["origin"] == "history"
    # also filterable under the mcp category
    only_mcp = c.get(f"/{project}/api/log?categories=mcp").json()["events"]
    assert any(e.get("error_id") == rec["id"] for e in only_mcp)


def test_api_log_dedups_error_present_in_both_stores(fresh_companion_app, project, orders_parquet, monkeypatch):
    # When the new MCP recorded a build_error event AND errors.jsonl has the
    # same id, it must appear once — the rich event wins over the backfill.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    rec = record_error(project, code="x", message="boom", tool="catalog_run")
    record_event(project, "build_error", session="s1", error_id=rec["id"], tool="catalog_run", message="boom")

    c = TestClient(fresh_companion_app)
    body = c.get(f"/{project}/api/log").json()
    matches = [e for e in body["events"] if e.get("error_id") == rec["id"]]
    assert len(matches) == 1, f"error should appear once, got {len(matches)}"
    assert matches[0]["session"] == "s1"  # the rich events.jsonl event, not the backfill
