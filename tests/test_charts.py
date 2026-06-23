from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tallyman_core import ChartSpecError, get_chart, list_charts, remove_chart, set_chart
from tallyman_mcp.server import catalog_chart, catalog_create
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
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
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


def test_catalog_chart_by_hash(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _agg_code(project))
    out = catalog_chart(res.content_hash, SAMPLE_SPEC)
    assert "error" not in out
    assert out["hash"] == res.content_hash
    assert get_chart(project, res.content_hash) is not None


def test_catalog_chart_by_alias(project: str, orders_parquet: Path, monkeypatch):
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


def test_catalog_chart_invalid_spec(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _agg_code(project))
    out = catalog_chart(res.content_hash, "{not json}")
    assert "error" in out


# ---------------------------------------------------------------------------
# companion rendering + data endpoint
# ---------------------------------------------------------------------------


def test_entry_detail_renders_chart_when_present(fresh_companion_app, project: str, orders_parquet: Path):
    """Entry API includes chart_spec when a chart has been attached."""
    res = build_and_persist(project, _agg_code(project))
    set_chart(project, res.content_hash, SAMPLE_SPEC)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
    body = r.json()
    assert body["chart_spec"] is not None
    assert body["chart_spec"]["mark"] == "bar"


def test_entry_detail_omits_chart_when_absent(fresh_companion_app, project: str, orders_parquet: Path):
    """Entry API returns null chart_spec when no chart is attached."""
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
    assert r.json()["chart_spec"] is None


def test_api_data_endpoint(fresh_companion_app, project: str, orders_parquet: Path):
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


def test_api_data_limit_truncates(fresh_companion_app, project: str, orders_parquet: Path):
    """Sanity-check that limit is plumbed through."""
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{res.content_hash}?limit=2")
    body = r.json()
    assert len(body["data"]) == 2
    assert body["total"] == 4
    assert body["limit"] == 2


def test_api_data_offset_paginates(fresh_companion_app, project: str, orders_parquet: Path):
    """T-20: offset+limit cursor pagination."""
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    a = c.get(f"/{project}/api/data/{res.content_hash}?offset=0&limit=2").json()
    b = c.get(f"/{project}/api/data/{res.content_hash}?offset=2&limit=2").json()
    # All 4 regions across the two pages, no overlap.
    seen = {row["region"] for row in a["data"]} | {row["region"] for row in b["data"]}
    assert seen == {"NE", "MW", "S", "W"}
    assert a["offset"] == 0 and b["offset"] == 2


def test_api_data_rejects_negative_limit(fresh_companion_app, project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{res.content_hash}?limit=-1")
    assert r.status_code == 400


def test_api_data_default_limit_is_200(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    """Defaults are demo-safe: aggregate fits, raw 2k-row loads cap at 200."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    # Use a passthrough expression (raw orders, 200 rows in the fixture).
    code = """
from tallyman_xorq.io import read_project_file
expr = read_project_file("orders.parquet")
"""
    res = build_and_persist(project, code)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{res.content_hash}")  # no offset/limit
    body = r.json()
    assert body["total"] == 200  # the test fixture
    assert len(body["data"]) == 200  # the test fixture is the same size as default cap
    assert body["limit"] == 200
