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
    r = c.get("/catalog")
    assert r.status_code == 200
    assert "build failures" in r.text
    assert "boom please notice me" in r.text


def test_companion_error_detail_route(fresh_companion_app, project: str):
    rec = record_error(project, code="x = 1\nexpr = nope", message="full traceback here")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/errors/{rec['id']}")
    assert r.status_code == 200
    assert "full traceback here" in r.text
    assert "expr = nope" in r.text


def test_companion_error_detail_404(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    assert c.get("/errors/missing").status_code == 404


def test_companion_api_errors(fresh_companion_app, project: str):
    record_error(project, code="x", message="m1")
    record_error(project, code="y", message="m2")
    c = TestClient(fresh_companion_app)
    r = c.get("/api/errors")
    assert r.status_code == 200
    body = r.json()
    assert body["project"] == project
    assert len(body["errors"]) == 2
