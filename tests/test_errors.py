from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_core import clear_errors, get_error, list_errors, record_error
from tallyman_mcp.server import catalog_run


def test_record_error_writes_jsonl(project: str):
    rec = record_error(project, code="raise X", message="boom", prompt="bad prompt")
    assert rec["id"]
    rows = list_errors(project)
    assert len(rows) == 1
    assert rows[0]["id"] == rec["id"]
    assert rows[0]["message"] == "boom"
    assert rows[0]["prompt"] == "bad prompt"


def test_list_errors_most_recent_first(project: str):
    a = record_error(project, code="a", message="m1")
    b = record_error(project, code="b", message="m2")
    rows = list_errors(project)
    assert [r["id"] for r in rows] == [b["id"], a["id"]]


def test_get_error_by_id(project: str):
    a = record_error(project, code="x", message="m")
    record_error(project, code="y", message="n")
    got = get_error(project, a["id"])
    assert got["message"] == "m"
    assert get_error(project, "missing") is None


def test_catalog_run_failure_records_error(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_run("expr = nope", prompt="broken thing")
    assert "error" in out
    assert "error_id" in out
    rows = list_errors(project)
    assert len(rows) == 1
    assert rows[0]["id"] == out["error_id"]
    assert rows[0]["prompt"] == "broken thing"


def test_companion_catalog_renders_errors(fresh_companion_app, project: str):
    record_error(project, code="x", message="boom please notice me", prompt="p")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/errors")
    assert r.status_code == 200
    body = r.json()
    messages = [e["message"] for e in body["errors"]]
    assert any("boom please notice me" in m for m in messages)


def test_companion_error_detail_route(fresh_companion_app, project: str):
    rec = record_error(project, code="x = 1\nexpr = nope", message="full traceback here")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/error/{rec['id']}")
    assert r.status_code == 200
    body = r.json()
    assert "full traceback here" in body["error"]["message"]
    assert "expr = nope" in body["error"]["code"]


def test_companion_error_detail_404(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/error/missing").status_code == 404


def test_companion_api_errors(fresh_companion_app, project: str):
    record_error(project, code="x", message="m1")
    record_error(project, code="y", message="m2")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/errors")
    assert r.status_code == 200
    body = r.json()
    assert body["project"] == project
    assert len(body["errors"]) == 2


def test_record_error_persists_tool_field(project: str):
    rec = record_error(project, code="x", message="m", tool="catalog_revise")
    assert rec["tool"] == "catalog_revise"
    rows = list_errors(project)
    assert rows[0]["tool"] == "catalog_revise"


def test_catalog_run_records_tool_name(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_run("expr = nope", prompt="broken")
    assert "error" in out
    rec = list_errors(project)[0]
    assert rec["tool"] == "catalog_run"


def test_catalog_create_failure_records_tool_name(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_mcp.server import catalog_create

    out = catalog_create("x", "expr = oops", prompt="bad")
    assert "error" in out
    rec = list_errors(project)[0]
    assert rec["tool"] == "catalog_create"


def test_catalog_banner_shows_tool_pill(fresh_companion_app, project: str):
    record_error(project, code="x", message="boom", tool="catalog_revise")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/errors")
    assert r.status_code == 200
    tools = [e["tool"] for e in r.json()["errors"]]
    assert "catalog_revise" in tools


def test_error_detail_shows_tool_pill(fresh_companion_app, project: str):
    rec = record_error(project, code="x", message="boom", tool="catalog_run")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/error/{rec['id']}")
    assert r.status_code == 200
    assert r.json()["error"]["tool"] == "catalog_run"


def test_error_detail_sidebar_has_full_catalog_list(fresh_companion_app, project: str, orders_parquet, monkeypatch):
    """Entries and error APIs return independent data for the React sidebar."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_mcp.server import catalog_create

    catalog_create(
        "shoe_sales",
        f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
""",
    )
    rec = record_error(project, code="x", message="boom", tool="catalog_run")
    c = TestClient(fresh_companion_app)

    # Entries endpoint has the named entry.
    entries_r = c.get(f"/{project}/api/entries")
    assert entries_r.status_code == 200
    aliases = [e["alias"] for e in entries_r.json()["entries"] if e["alias"]]
    assert "shoe_sales" in aliases

    # Error endpoint has the error.
    error_r = c.get(f"/{project}/api/error/{rec['id']}")
    assert error_r.status_code == 200
    assert error_r.json()["error"]["tool"] == "catalog_run"


def test_clear_errors_removes_all(project: str):
    record_error(project, code="a", message="m1")
    record_error(project, code="b", message="m2")
    cleared = clear_errors(project)
    assert cleared == 2
    assert list_errors(project) == []


def test_clear_errors_empty_is_noop(project: str):
    assert clear_errors(project) == 0


def test_companion_clear_errors_route(fresh_companion_app, project: str):
    record_error(project, code="x", message="boom")
    record_error(project, code="y", message="bang")
    c = TestClient(fresh_companion_app)
    r = c.delete(f"/{project}/api/errors")
    assert r.status_code == 200
    assert r.json()["cleared"] == 2
    # Errors stay gone across subsequent reads (the bug being fixed: a
    # client-only dismiss reappeared on every page visit).
    assert c.get(f"/{project}/api/errors").json()["errors"] == []


def test_companion_clear_errors_serve_mode_403(project: str):
    record_error(project, code="x", message="boom")
    from tallyman_companion import create_app

    app = create_app(project, read_only=True)
    c = TestClient(app)
    assert c.delete(f"/{project}/api/errors").status_code == 403
