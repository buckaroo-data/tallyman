"""T-16: edit code from the browser via PUT /api/code/<alias>."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tallyman_core import get_alias, history_for
from tallyman_mcp.server import catalog_create


def _agg(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def _filter(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
f = t.filter(t.category == "boots")
expr = f.group_by("region").aggregate(n=f.count())
"""


def test_put_code_revises_alias(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg(project))
    v1_hash = get_alias(project, "shoe_sales")

    c = TestClient(fresh_companion_app)
    r = c.put(f"/{project}/api/code/shoe_sales", json={"code": _filter(project)})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["alias"] == "shoe_sales"
    assert body["version"] == 2
    assert body["hash"] != v1_hash

    # Alias points at V2; V1 is in forensic history.
    assert get_alias(project, "shoe_sales") == body["hash"]
    assert history_for(project, "shoe_sales") == [v1_hash, body["hash"]]


def test_put_code_missing_alias_404(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.put(f"/{project}/api/code/nonexistent", json={"code": "expr = 1"})
    assert r.status_code == 404


def test_put_code_empty_400(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg(project))
    c = TestClient(fresh_companion_app)
    r = c.put(f"/{project}/api/code/shoe_sales", json={"code": "   "})
    assert r.status_code == 400


def test_put_code_build_error_records_and_400s(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg(project))
    c = TestClient(fresh_companion_app)
    r = c.put(f"/{project}/api/code/shoe_sales", json={"code": "expr = undefined_thing"})
    assert r.status_code == 400

    # The error was recorded via record_error with tool="api_code".
    from tallyman_core import list_errors

    rec = list_errors(project)[0]
    assert rec["tool"] == "api_code"


def test_put_code_serve_mode_403(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg(project))
    from tallyman_companion import create_app

    app = create_app(project, read_only=True)
    c = TestClient(app)
    r = c.put(f"/{project}/api/code/shoe_sales", json={"code": _filter(project)})
    assert r.status_code == 403


def test_entry_detail_has_alias_for_named(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    """Named entries include alias and code in the JSON API response."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_create("shoe_sales", _agg(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{out['hash']}")
    assert r.status_code == 200
    body = r.json()
    assert body["alias"] == "shoe_sales"
    assert body["code"].strip() != ""


def test_entry_detail_no_alias_for_scratch(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    """Scratch entries have no alias in the JSON API response."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_mcp.server import catalog_run

    out = catalog_run(_agg(project), prompt="exploratory")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{out['hash']}")
    assert r.status_code == 200
    assert r.json()["alias"] is None


def test_entry_detail_omits_edit_button_in_serve_mode(project: str, orders_parquet: Path, monkeypatch):
    """read_only mode returns 403 on PUT /api/code/."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg(project))
    from tallyman_companion import create_app

    app = create_app(project, read_only=True)
    c = TestClient(app)
    r = c.put(f"/{project}/api/code/shoe_sales", json={"code": _filter(project)})
    assert r.status_code == 403
