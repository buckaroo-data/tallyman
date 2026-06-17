"""Tests for the promote-diff feature.

Covers:
  - build_compare_expr columns and overrides (tallyman_companion.diff)
  - build_diff_expr round-trip (active-project path)
  - catalog_promote_diff MCP tool
  - POST /api/promote_diff HTTP endpoint
  - marimo export inlines source expressions for diff entries
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tallyman_mcp.server import catalog_create, catalog_promote_diff, catalog_revise
from tallyman_xorq import build_and_persist
from tallyman_xorq.result_cache import cached_result_expr


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _filter_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(total=filtered.price.sum(), n=filtered.count())
"""


# ---------------------------------------------------------------------------
# build_compare_expr — columns and overrides
# ---------------------------------------------------------------------------


def test_build_compare_expr_has_sentinel_columns(project: str, orders_parquet: Path):
    from tallyman_companion.diff import build_compare_expr

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
    assert "total_abs_delta" in schema
    assert "n_v2" in schema
    assert "n_eq" in schema
    assert "n_pct_delta" in schema
    assert "n_abs_delta" in schema


def test_build_compare_expr_delta_columns_follow_v2(project: str, orders_parquet: Path):
    from tallyman_companion.diff import build_compare_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)

    expr, _ = build_compare_expr(a_expr, b_expr, ["region"])
    cols = list(expr.schema().keys())
    # _pct_delta and _abs_delta must appear right after their _v2 column
    for prefix in ("total", "n"):
        v2_idx = cols.index(f"{prefix}_v2")
        assert cols[v2_idx + 1] == f"{prefix}_pct_delta"
        assert cols[v2_idx + 2] == f"{prefix}_abs_delta"


def test_build_compare_expr_overrides_hide_raw_cols(project: str, orders_parquet: Path):
    from tallyman_companion.diff import build_compare_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)

    _, overrides = build_compare_expr(a_expr, b_expr, ["region"])
    assert overrides["membership"]["merge_rule"] == "hidden"
    assert overrides["total"]["merge_rule"] == "hidden"
    # _pct_delta and _abs_delta are not hidden; DiffMainStyling filters them
    assert "total_pct_delta" not in overrides
    assert "total_abs_delta" not in overrides


def test_build_compare_expr_numeric_col_uses_diverging_colormap(project: str, orders_parquet: Path):
    # The base overrides (used by promoted diff entries, which render without
    # the diff display klasses) color numeric value columns by pct_delta. The
    # live /diff route strips this via strip_live_diff_color so the per-view
    # klass coloring wins — see test_strip_live_diff_color below.
    from tallyman_companion.diff import build_compare_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)

    _, overrides = build_compare_expr(a_expr, b_expr, ["region"])
    v2_cfg = overrides["total_v2"]
    assert v2_cfg["color_map_config"]["color_rule"] == "color_map"
    assert v2_cfg["color_map_config"]["map_name"] == "DIVERGING_RED_WHITE_BLUE"
    assert v2_cfg["color_map_config"]["val_column"] == "total_pct_delta"


def test_strip_live_diff_color_drops_numeric_keeps_categorical():
    # The live /diff route strips numeric (color_map) value-column coloring so
    # the display klasses own it, but keeps categorical key/equality colors.
    from tallyman_companion.diff import strip_live_diff_color

    overrides = {
        "total_v2": {
            "header_name": "total",
            "tooltip_config": {"tooltip_type": "simple", "val_column": "total"},
            "color_map_config": {
                "color_rule": "color_map",
                "map_name": "DIVERGING_RED_WHITE_BLUE",
                "val_column": "total_pct_delta",
            },
        },
        "region_v2": {
            "header_name": "region",
            "color_map_config": {"color_rule": "color_categorical", "val_column": "region_eq"},
        },
        "region": {"color_map_config": {"color_rule": "color_categorical", "val_column": "membership"}},
    }
    stripped = strip_live_diff_color(overrides)
    # numeric color dropped, rename + tooltip kept
    assert "color_map_config" not in stripped["total_v2"]
    assert stripped["total_v2"]["header_name"] == "total"
    assert stripped["total_v2"]["tooltip_config"]["val_column"] == "total"
    # categorical colors preserved
    assert stripped["region_v2"]["color_map_config"]["color_rule"] == "color_categorical"
    assert stripped["region"]["color_map_config"]["color_rule"] == "color_categorical"


def test_build_compare_expr_key_col_uses_categorical_colormap(project: str, orders_parquet: Path):
    from tallyman_companion.diff import build_compare_expr

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
    from tallyman_companion.diff import build_diff_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    expr = build_diff_expr(a.content_hash, b.content_hash, keys=["region"])
    assert "membership" in expr.schema()


def test_build_diff_expr_writes_no_result_parquet(project: str, orders_parquet: Path, monkeypatch):
    """build_diff_expr composes the two entries' cache-resolving expressions
    directly. It must not materialise an on-demand ``result.parquet`` for
    either side — those parquets are the layer #73 began retiring and are never
    read on this path.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_companion.diff import build_diff_expr
    from tallyman_core import entry_dir

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    expr = build_diff_expr(a.content_hash, b.content_hash, keys=["region"])
    assert "membership" in expr.schema()
    assert not (entry_dir(project, a.content_hash) / "result.parquet").exists()
    assert not (entry_dir(project, b.content_hash) / "result.parquet").exists()


# ---------------------------------------------------------------------------
# catalog_promote_diff MCP tool
# ---------------------------------------------------------------------------


def test_catalog_promote_diff_creates_entry(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales")
    assert "error" not in out
    assert out["hash"]
    assert out["source_alias"] == "shoe_sales"
    assert out["va"] == 1 and out["vb"] == 2
    assert out["row_count"] > 0


def test_catalog_promote_diff_auto_alias(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales")
    assert out["alias"] == "diff_shoe_sales_v1_v2"


def test_catalog_promote_diff_custom_alias(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales", alias="my_diff")
    assert out["alias"] == "my_diff"


def test_catalog_promote_diff_stores_display_config(project: str, orders_parquet: Path, monkeypatch):
    from tallyman_core.display_configs import get_display_config

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_promote_diff("nonexistent")
    assert "error" in out


def test_catalog_promote_diff_out_of_range_returns_error(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_promote_diff("shoe_sales", va=1, vb=5)
    assert "error" in out


def test_catalog_promote_diff_negative_indices(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_promote_diff("shoe_sales", va=-2, vb=-1)
    assert "error" not in out
    assert out["va"] == 1 and out["vb"] == 2


# ---------------------------------------------------------------------------
# POST /api/promote_diff HTTP endpoint
# ---------------------------------------------------------------------------


def test_http_promote_diff_creates_entry(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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
    from tallyman_core.display_configs import get_display_config

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))

    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/promote_diff/shoe_sales/1/5")
    assert r.status_code == 400


def test_http_entry_detail_diff_has_display_config(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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
    from tallyman_core import notebook as nb_mod
    from tallyman_core.display_configs import set_display_config
    from tallyman_core.marimo_export import notebook_to_marimo

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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
    from tallyman_core import notebook as nb_mod
    from tallyman_core.display_configs import set_display_config
    from tallyman_core.marimo_export import notebook_to_marimo

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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


def test_marimo_export_loads_in_marimo_without_collisions(
    project: str, orders_parquet: Path, monkeypatch, tmp_path: Path
):
    """The exported notebook must load in marimo without a MultipleDefinitionError.

    Each code cell inlines an ``expr.py`` that binds ``expr`` and imports
    ``os`` / ``xo`` / ``from_project``. With more than one entry those names
    used to leak into marimo's global dataflow graph and collide; the closure
    wrapper in ``marimo_export`` keeps them cell-local. String assertions
    alone never caught this — we have to actually build the marimo graph.
    """
    import importlib.util
    import sys

    from marimo._ast.app import InternalApp

    from tallyman_core.marimo_export import notebook_to_marimo

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_create("shoe_boots", _filter_code(project))

    src = notebook_to_marimo(project)

    # marimo's @app.cell decorator reads source via inspect.getsourcelines,
    # so the module must exist as a real file on disk, not exec'd in memory.
    nb_path = tmp_path / "nb_marimo_regression.py"
    nb_path.write_text(src)
    spec = importlib.util.spec_from_file_location("nb_marimo_regression", nb_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["nb_marimo_regression"] = mod
    try:
        spec.loader.exec_module(mod)
        # Building the graph raises MultipleDefinitionError if any
        # non-underscore name is defined by more than one cell.
        graph = InternalApp(mod.app).graph
    finally:
        sys.modules.pop("nb_marimo_regression", None)

    # bootstrap + title + (markdown + code) per entry
    assert len(graph.cells) >= 6
