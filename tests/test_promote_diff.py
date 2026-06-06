"""Tests for the promote-diff feature.

Covers:
  - build_compare_expr columns and overrides (pydata_companion.diff)
  - build_diff_expr round-trip (active-project path)
  - catalog_promote_diff MCP tool
  - POST /api/promote_diff HTTP endpoint
  - marimo export inlines source expressions for diff entries
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from pydata_mcp.server import catalog_create, catalog_promote_diff, catalog_revise
from pydata_xorq import build_and_persist
from pydata_xorq.result_cache import cached_result_expr


def _agg_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _filter_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(total=filtered.price.sum(), n=filtered.count())
"""


# ---------------------------------------------------------------------------
# build_compare_expr — columns and overrides
# ---------------------------------------------------------------------------


def test_build_compare_expr_has_sentinel_columns(project: str, orders_parquet: Path):
    from pydata_companion.diff import build_compare_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)

    expr, overrides = build_compare_expr(a_expr, b_expr, ["region"])
    schema = expr.schema()
    assert "membership" in schema
    assert "total_v2" in schema
    assert "total_eq" in schema
    assert "total_pct_delta" in schema
    assert "n_v2" in schema
    assert "n_eq" in schema
    assert "n_pct_delta" in schema


def test_build_compare_expr_overrides_hide_raw_cols(project: str, orders_parquet: Path):
    from pydata_companion.diff import build_compare_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)

    _, overrides = build_compare_expr(a_expr, b_expr, ["region"])
    assert overrides["membership"]["merge_rule"] == "hidden"
    assert overrides["total"]["merge_rule"] == "hidden"
    assert overrides["total_pct_delta"]["merge_rule"] == "hidden"


def test_build_compare_expr_numeric_col_uses_diverging_colormap(project: str, orders_parquet: Path):
    from pydata_companion.diff import build_compare_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)

    _, overrides = build_compare_expr(a_expr, b_expr, ["region"])
    v2_cfg = overrides["total_v2"]
    assert v2_cfg["color_map_config"]["color_rule"] == "color_map"
    assert v2_cfg["color_map_config"]["map_name"] == "DIVERGING_RED_WHITE_BLUE"
    assert v2_cfg["color_map_config"]["val_column"] == "total_pct_delta"


def test_build_compare_expr_key_col_uses_categorical_colormap(project: str, orders_parquet: Path):
    from pydata_companion.diff import build_compare_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)

    _, overrides = build_compare_expr(a_expr, b_expr, ["region"])
    assert overrides["region"]["color_map_config"]["color_rule"] == "color_categorical"
    assert overrides["region"]["color_map_config"]["val_column"] == "membership"


# ---------------------------------------------------------------------------
# build_diff_expr — project-aware rebuild path
# ---------------------------------------------------------------------------


def test_build_diff_expr_returns_ibis_expr(project: str, orders_parquet: Path):
    from pydata_companion.diff import build_diff_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    expr = build_diff_expr(a.content_hash, b.content_hash, keys=["region"])
    assert "membership" in expr.schema()


# ---------------------------------------------------------------------------
# catalog_promote_diff MCP tool
# ---------------------------------------------------------------------------


def test_catalog_promote_diff_creates_entry(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales")
    assert "error" not in out
    assert out["hash"]
    assert out["source_alias"] == "shoe_sales"
    assert out["va"] == 1 and out["vb"] == 2
    assert out["row_count"] > 0


def test_catalog_promote_diff_auto_alias(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales")
    assert out["alias"] == "diff_shoe_sales_v1_v2"


def test_catalog_promote_diff_custom_alias(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales", alias="my_diff")
    assert out["alias"] == "my_diff"


def test_catalog_promote_diff_stores_display_config(project: str, orders_parquet: Path, monkeypatch):
    from pydata_core.display_configs import get_display_config

    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales")
    cfg = get_display_config(project, out["hash"])
    assert cfg is not None
    assert "column_config_overrides" in cfg
    assert "diff_provenance" in cfg
    assert cfg["diff_provenance"]["source_alias"] == "shoe_sales"
    assert cfg["diff_provenance"]["va"] == 1
    assert cfg["diff_provenance"]["vb"] == 2
    assert cfg["column_config_overrides"]["membership"]["merge_rule"] == "hidden"


def test_catalog_promote_diff_no_history_returns_error(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_promote_diff("nonexistent")
    assert "error" in out


def test_catalog_promote_diff_out_of_range_returns_error(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_promote_diff("shoe_sales", va=1, vb=5)
    assert "error" in out


def test_catalog_promote_diff_negative_indices(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales", va=-2, vb=-1)
    assert "error" not in out
    assert out["va"] == 1 and out["vb"] == 2


# ---------------------------------------------------------------------------
# POST /api/promote_diff HTTP endpoint
# ---------------------------------------------------------------------------


def test_http_promote_diff_creates_entry(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))

    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/promote_diff/shoe_sales/1/2")
    assert r.status_code == 200
    body = r.json()
    assert body["alias"] == "diff_shoe_sales_v1_v2"
    assert body["hash"]
    assert body["source_alias"] == "shoe_sales"
    assert body["va"] == 1 and body["vb"] == 2
    assert body["row_count"] > 0


def test_http_promote_diff_sets_display_config(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    from pydata_core.display_configs import get_display_config

    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))

    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/promote_diff/shoe_sales/1/2")
    assert r.status_code == 200
    content_hash = r.json()["hash"]
    cfg = get_display_config(project, content_hash)
    assert cfg is not None
    assert "diff_provenance" in cfg


def test_http_promote_diff_unknown_alias_404(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/promote_diff/nonexistent/1/2")
    assert r.status_code == 404


def test_http_promote_diff_out_of_range_400(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))

    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/promote_diff/shoe_sales/1/5")
    assert r.status_code == 400


def test_http_entry_detail_diff_has_display_config(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))

    c = TestClient(fresh_companion_app)
    promote_r = c.post(f"/{project}/api/promote_diff/shoe_sales/1/2")
    assert promote_r.status_code == 200
    content_hash = promote_r.json()["hash"]

    detail_r = c.get(f"/{project}/api/entry/{content_hash}")
    assert detail_r.status_code == 200
    body = detail_r.json()
    assert body["display_config"] is not None
    assert body["display_config"]["diff_provenance"]["source_alias"] == "shoe_sales"


# ---------------------------------------------------------------------------
# marimo export — diff entries inline source expressions
# ---------------------------------------------------------------------------


def test_marimo_export_diff_entry_inlines_source_code(project: str, orders_parquet: Path, monkeypatch):
    from pydata_core import notebook as nb_mod
    from pydata_core.display_configs import set_display_config
    from pydata_core.marimo_export import notebook_to_marimo

    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales")
    diff_hash = out["hash"]

    set_display_config(
        project,
        diff_hash,
        {
            "column_config_overrides": {"membership": {"merge_rule": "hidden"}},
            "diff_provenance": {
                "source_alias": "shoe_sales",
                "va": 1,
                "vb": 2,
                "a_hash": out["a_hash"],
                "b_hash": out["b_hash"],
                "keys": out["keys"],
            },
        },
    )

    nb_mod.append(project, "diff_shoe_sales_v1_v2")
    nb_source = notebook_to_marimo(project)
    assert "build_compare_expr" in nb_source
    assert "XorqBuckarooInfiniteWidget" in nb_source
    assert out["a_hash"] in nb_source
    assert out["b_hash"] in nb_source
    assert "a_expr = expr" in nb_source
    assert "b_expr = expr" in nb_source


def test_marimo_export_diff_entry_includes_overrides(project: str, orders_parquet: Path, monkeypatch):
    from pydata_core import notebook as nb_mod
    from pydata_core.display_configs import set_display_config
    from pydata_core.marimo_export import notebook_to_marimo

    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales")
    diff_hash = out["hash"]

    set_display_config(
        project,
        diff_hash,
        {
            "column_config_overrides": {
                "membership": {"merge_rule": "hidden"},
                "region": {"color_map_config": {"color_rule": "color_categorical"}},
            },
            "diff_provenance": {
                "source_alias": "shoe_sales",
                "va": 1,
                "vb": 2,
                "a_hash": out["a_hash"],
                "b_hash": out["b_hash"],
                "keys": out["keys"],
            },
        },
    )

    nb_mod.append(project, "diff_shoe_sales_v1_v2")
    nb_source = notebook_to_marimo(project)
    assert "_overrides = " in nb_source
    assert "membership" in nb_source
