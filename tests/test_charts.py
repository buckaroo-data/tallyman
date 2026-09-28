from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from tallyman_core import ChartSpecError, get_chart, list_charts, list_errors, remove_chart, set_chart
from tallyman_mcp.server import catalog_chart, catalog_chart_errors, catalog_create
from tallyman_xorq import build_and_persist

SAMPLE_SPEC = {
    "mark": "bar",
    "encoding": {
        "x": {"field": "region", "type": "nominal"},
        "y": {"field": "n", "type": "quantitative"},
    },
}


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


# ---------------------------------------------------------------------------
# storage layer
# ---------------------------------------------------------------------------


def test_set_get_chart_round_trip(project: str):
    set_chart(project, "abc", SAMPLE_SPEC)
    got = get_chart(project, "abc")
    assert got["mark"] == "bar"


def test_set_chart_accepts_json_string(project: str):
    set_chart(project, "abc", json.dumps(SAMPLE_SPEC))
    assert get_chart(project, "abc")["mark"] == "bar"


def test_set_chart_rejects_invalid_json(project: str):
    with pytest.raises(ChartSpecError):
        set_chart(project, "abc", "{not json}")


def test_set_chart_rejects_non_dict(project: str):
    with pytest.raises(ChartSpecError):
        set_chart(project, "abc", 42)  # type: ignore[arg-type]


def test_get_chart_returns_none_for_missing(project: str):
    assert get_chart(project, "missing") is None


def test_list_charts(project: str):
    set_chart(project, "a", SAMPLE_SPEC)
    set_chart(project, "b", SAMPLE_SPEC)
    assert list_charts(project) == ["a", "b"]


def test_remove_chart(project: str):
    set_chart(project, "a", SAMPLE_SPEC)
    assert remove_chart(project, "a") is True
    assert remove_chart(project, "a") is False
    assert get_chart(project, "a") is None


# ---------------------------------------------------------------------------
# catalog_chart MCP tool
# ---------------------------------------------------------------------------


def test_catalog_chart_by_hash(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _agg_code(project))
    out = catalog_chart(res.content_hash, SAMPLE_SPEC)
    assert "error" not in out
    assert out["hash"] == res.content_hash
    assert get_chart(project, res.content_hash) is not None


def test_catalog_chart_by_alias(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_chart("shoe_sales", SAMPLE_SPEC)
    assert "error" not in out
    # The chart is stored against the latest hash for this alias.
    from tallyman_core import get_alias

    assert get_chart(project, get_alias(project, "shoe_sales")) is not None


def test_catalog_chart_rejects_missing(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_chart("nonexistent_alias", SAMPLE_SPEC)
    assert "error" in out


def test_catalog_chart_invalid_spec(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _agg_code(project))
    out = catalog_chart(res.content_hash, "{not json}")
    assert "error" in out


# ---------------------------------------------------------------------------
# companion rendering + data endpoint
# ---------------------------------------------------------------------------


def test_entry_detail_renders_chart_when_present(fresh_companion_app, project: str, orders_src: str):
    """Entry API includes chart_spec when a chart has been attached."""
    res = build_and_persist(project, _agg_code(project))
    set_chart(project, res.content_hash, SAMPLE_SPEC)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
    body = r.json()
    assert body["chart_spec"] is not None
    assert body["chart_spec"]["mark"] == "bar"


def test_entry_detail_omits_chart_when_absent(fresh_companion_app, project: str, orders_src: str):
    """Entry API returns null chart_spec when no chart is attached."""
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
    assert r.json()["chart_spec"] is None


def test_api_data_endpoint(fresh_companion_app, project: str, orders_src: str):
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{res.content_hash}")
    assert r.status_code == 200
    body = r.json()
    assert "data" in body
    assert body["total"] == 4
    assert body["offset"] == 0
    assert {row["region"] for row in body["data"]} == {"NE", "MW", "S", "W"}


def test_api_data_404_on_missing(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/data/deadbeef").status_code == 404


def test_api_data_limit_truncates(fresh_companion_app, project: str, orders_src: str):
    """Sanity-check that limit is plumbed through."""
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{res.content_hash}?limit=2")
    body = r.json()
    assert len(body["data"]) == 2
    assert body["total"] == 4
    assert body["limit"] == 2


def test_api_data_offset_paginates(fresh_companion_app, project: str, orders_src: str):
    """T-20: offset+limit cursor pagination."""
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    a = c.get(f"/{project}/api/data/{res.content_hash}?offset=0&limit=2").json()
    b = c.get(f"/{project}/api/data/{res.content_hash}?offset=2&limit=2").json()
    # All 4 regions across the two pages, no overlap.
    seen = {row["region"] for row in a["data"]} | {row["region"] for row in b["data"]}
    assert seen == {"NE", "MW", "S", "W"}
    assert a["offset"] == 0 and b["offset"] == 2


def test_api_data_rejects_negative_limit(fresh_companion_app, project: str, orders_src: str):
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{res.content_hash}?limit=-1")
    assert r.status_code == 400


def test_api_data_default_limit_is_200(fresh_companion_app, project: str, orders_src: str, monkeypatch):
    """Defaults are demo-safe: aggregate fits, raw 2k-row loads cap at 200."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    # Use a passthrough expression (raw orders, 200 rows in the fixture).
    code = """
from tallyman_xorq.io import tracked_expr_from_alias
expr = tracked_expr_from_alias("orders_src")
"""
    res = build_and_persist(project, code)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{res.content_hash}")  # no offset/limit
    body = r.json()
    assert body["total"] == 200  # the test fixture
    assert len(body["data"]) == 200  # the test fixture is the same size as default cap
    assert body["limit"] == 200


# ---------------------------------------------------------------------------
# browser-reported render failures (chart rendering is client-side, so a spec
# that stores fine can still fail silently in vega-embed once real data hits
# it — the page reports that back here)
# ---------------------------------------------------------------------------


def test_api_chart_error_records_error(fresh_companion_app, project: str, orders_src: str):
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.post(
        f"/{project}/api/chart_error",
        json={"hash": res.content_hash, "message": "clamp() choked on a null field"},
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True
    errors = list_errors(project)
    assert any(
        e["tool"] == "chart_render"
        and e["hash"] == res.content_hash
        and e["message"] == "clamp() choked on a null field"
        for e in errors
    )


def test_api_chart_error_rejects_malformed_hash(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/chart_error", json={"hash": "not-hex!", "message": "boom"})
    assert r.status_code == 400


def test_catalog_chart_errors_empty_when_none_reported(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _agg_code(project))
    assert catalog_chart_errors(res.content_hash)["items"] == []


def test_catalog_chart_errors_filters_by_hash_and_tool(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _agg_code(project))
    from tallyman_core import record_error

    record_error(project, code="", message="render blew up", tool="chart_render", hash=res.content_hash)
    # A build failure on the same hash from a different tool must not show up here.
    record_error(project, code="bad code", message="unrelated build failure", tool="api_code", hash=res.content_hash)

    out = catalog_chart_errors(res.content_hash)["items"]
    assert len(out) == 1
    assert out[0]["message"] == "render blew up"


def test_catalog_chart_errors_resolves_alias(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    from tallyman_core import get_alias, record_error

    target_hash = get_alias(project, "shoe_sales")
    record_error(project, code="", message="render blew up", tool="chart_render", hash=target_hash)
    out = catalog_chart_errors("shoe_sales")["items"]
    assert [e["message"] for e in out] == ["render blew up"]


def test_catalog_chart_errors_rejects_missing(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_chart_errors("nonexistent_alias")["items"]
    assert out and "error" in out[0]
