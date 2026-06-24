"""T-16: edit code from the browser via PUT /api/code/<alias>."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tallyman_core import get_alias, history_for
from tallyman_mcp.server import catalog_create


def _agg(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def _filter(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
f = t.filter(t.category == "boots")
expr = f.group_by("region").aggregate(n=f.count())
"""


def _nondeterministic(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
import xorq.vendor.ibis as ibis
t = read_project_file("orders.parquet", project={project!r})
expr = t.mutate(built_at=ibis.now())
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


def test_put_code_carries_chart_and_display(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    """A code-edit revise (PUT /api/code) seeds the new version's per-entry
    chart + display config from the previous one, same as catalog_revise."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core import get_chart, set_chart
    from tallyman_core.display_configs import get_display_config, set_display_config

    catalog_create("shoe_sales", _agg(project))
    v1 = get_alias(project, "shoe_sales")
    chart = {"mark": "bar", "encoding": {"x": {"field": "region"}, "y": {"field": "n"}}}
    display = {"column_config_overrides": {"n": {"color_map_config": "BLUE_TO_RED"}}}
    set_chart(project, v1, chart)
    set_display_config(project, v1, display)

    c = TestClient(fresh_companion_app)
    r = c.put(f"/{project}/api/code/shoe_sales", json={"code": _filter(project)})
    assert r.status_code == 200, r.text
    v2 = r.json()["hash"]
    assert v2 != v1
    assert get_chart(project, v2) == chart, "chart did not carry over via put_code"
    assert get_display_config(project, v2) == display, "display config did not carry over via put_code"
    assert set(r.json().get("carried_over", [])) == {"chart", "display_config"}


def test_put_code_surfaces_nondeterminism_lint(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    # An execution-nondeterministic recipe (now()) builds fine, but the PUT
    # /api/code reply must carry the advisory lint so the SPA can show it (#88).
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg(project))
    c = TestClient(fresh_companion_app)
    r = c.put(f"/{project}/api/code/shoe_sales", json={"code": _nondeterministic(project)})
    assert r.status_code == 200, r.text
    body = r.json()
    assert "lint_warnings" in body
    assert any("now()" in w for w in body["lint_warnings"])


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
