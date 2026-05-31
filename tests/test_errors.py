from __future__ import annotations

from fastapi.testclient import TestClient

from pydata_core import get_error, list_errors, record_error
from pydata_mcp.server import catalog_run


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
    monkeypatch.setenv("PYDATA_PROJECT", project)
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
    r = c.get(f"/{project}/catalog")
    assert r.status_code == 200
    assert "build failures" in r.text
    assert "boom please notice me" in r.text


def test_companion_error_detail_route(fresh_companion_app, project: str):
    rec = record_error(project, code="x = 1\nexpr = nope", message="full traceback here")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/errors/{rec['id']}")
    assert r.status_code == 200
    assert "full traceback here" in r.text
    assert "expr = nope" in r.text


def test_companion_error_detail_404(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/errors/missing").status_code == 404


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
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_run("expr = nope", prompt="broken")
    assert "error" in out
    rec = list_errors(project)[0]
    assert rec["tool"] == "catalog_run"


def test_catalog_create_failure_records_tool_name(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create

    out = catalog_create("x", "expr = oops", prompt="bad")
    assert "error" in out
    rec = list_errors(project)[0]
    assert rec["tool"] == "catalog_create"


def test_catalog_banner_shows_tool_pill(fresh_companion_app, project: str):
    record_error(project, code="x", message="boom", tool="catalog_revise")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/catalog")
    assert r.status_code == 200
    assert "catalog_revise" in r.text


def test_error_detail_shows_tool_pill(fresh_companion_app, project: str):
    rec = record_error(project, code="x", message="boom", tool="catalog_run")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/errors/{rec['id']}")
    assert "tool: catalog_run" in r.text


def test_error_detail_sidebar_has_full_catalog_list(fresh_companion_app, project: str, orders_parquet, monkeypatch):
    """The error-detail sidebar also gets the full catalog list, with no
    current-highlight (errors aren't catalog entries)."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create

    catalog_create(
        "shoe_sales",
        f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
""",
    )
    rec = record_error(project, code="x", message="boom", tool="catalog_run")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/errors/{rec['id']}")
    assert "named (" in r.text
    assert "shoe_sales" in r.text
    # No item should be highlighted on an error-detail page — `aria-current`
    # is the cleanest signal; the `.current` CSS rule lives in base.html so
    # it always matches and isn't a useful check.
    assert 'aria-current="page"' not in r.text
    assert "← back to catalog" not in r.text
