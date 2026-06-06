from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from pydata_mcp.server import catalog_create, catalog_diff, catalog_revise
from pydata_xorq import (
    code_diff,
    full_diff,
    head_diff,
    head_diff_polars,
    head_diff_xorq,
    key_diff,
    key_diff_polars,
    key_diff_xorq,
    schema_diff,
    stats_diff,
    stats_diff_polars,
    stats_diff_xorq,
)


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
# pure-function diffs (no xorq build needed)
# ---------------------------------------------------------------------------


def test_code_diff_html_has_pygments_highlight():
    out = code_diff("a = 1\nb = 2\n", "a = 1\nb = 3\n")
    assert "highlight" in out  # Pygments wraps in <div class="highlight">
    assert "+b = 3" in out
    assert "-b = 2" in out


def test_code_diff_identical_returns_sentinel():
    out = code_diff("a = 1\n", "a = 1\n")
    assert "identical" in out.lower()


def test_schema_diff_added_removed_changed():
    before = {"fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}], "row_count": 10}
    after = {"fields": [{"name": "x", "type": "string"}, {"name": "z", "type": "int"}], "row_count": 12}
    d = schema_diff(before, after)
    assert d["added"] == ["z"]
    assert d["removed"] == ["y"]
    assert d["changed_type"] == [{"name": "x", "before": "int", "after": "string"}]
    assert d["row_count"] == {"before": 10, "after": 12}


def test_head_diff_renders_html():
    a = pd.DataFrame({"x": [1, 2, 3]})
    b = pd.DataFrame({"x": [1, 2, 4]})
    d = head_diff(a, b, n=2)
    assert "<table" in d["before"]
    assert "<table" in d["after"]
    assert d["a_total"] == 3 and d["b_total"] == 3


def test_stats_diff_basic():
    a = pd.DataFrame({"region": ["NE", "NE", "S"], "v": [1, 2, 3]})
    b = pd.DataFrame({"region": ["NE", "S", "W"], "v": [10, 20, 30]})
    s = stats_diff(a, b)
    by_col = {row["name"]: row for row in s}
    assert by_col["region"]["before"]["distinct"] == 2
    assert by_col["region"]["after"]["distinct"] == 3
    assert by_col["v"]["before"]["numeric"]["sum"] == 6.0
    assert by_col["v"]["after"]["numeric"]["sum"] == 60.0


def test_key_diff_outer_join():
    a = pd.DataFrame({"region": ["NE", "MW", "S"], "n": [10, 20, 30]})
    b = pd.DataFrame({"region": ["NE", "S", "W"], "n": [15, 25, 35]})
    d = key_diff(a, b)
    assert d is not None
    assert d["keys"] == ["region"]
    assert d["matched"] == 2  # NE + S
    assert d["only_before"] == 1  # MW
    assert d["only_after"] == 1  # W


def test_key_diff_detects_unique_numeric_key():
    # buckaroo 0.14.10's approximate-PK detection keys on any unique column,
    # numeric included (the pre-release heuristic skipped numeric columns).
    a = pd.DataFrame({"x": [1, 2, 3]})
    b = pd.DataFrame({"x": [1, 2, 4]})
    d = key_diff(a, b)
    assert d["keys"] == ["x"]
    assert d["matched"] == 2  # 1, 2
    assert d["only_before"] == 1  # 3
    assert d["only_after"] == 1  # 4


# ---------------------------------------------------------------------------
# full_diff against real xorq builds
# ---------------------------------------------------------------------------


def test_full_diff_against_two_builds(project: str, orders_parquet: Path):
    from pydata_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    from pydata_core import entry_dir

    diff = full_diff(entry_dir(project, a.content_hash), entry_dir(project, b.content_hash))
    assert "highlight" in diff["code"]
    assert diff["schema"]["row_count"]["before"] == 4
    assert "stats" in diff
    assert diff["keyed"] is not None  # region is a shared key


# ---------------------------------------------------------------------------
# catalog_diff MCP tool
# ---------------------------------------------------------------------------


def test_catalog_diff_default_compares_n_minus_one_to_n(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_diff("shoe_sales")
    assert "error" not in out
    assert out["before"]["version"] == 1
    assert out["after"]["version"] == 2


def test_catalog_diff_explicit_versions(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_diff("shoe_sales", 1, 2)
    assert "error" not in out


def test_catalog_diff_no_history(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_diff("nonexistent")
    assert "error" in out


def test_catalog_diff_out_of_range(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_diff("shoe_sales", 1, 5)
    assert "error" in out


# ---------------------------------------------------------------------------
# /diff/<alias>/<va>/<vb> route
# ---------------------------------------------------------------------------


def test_diff_route_default(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/shoe_sales/1/2")
    assert r.status_code == 200
    body = r.json()
    assert body["alias"] == "shoe_sales"
    assert body["va"] == 1 and body["vb"] == 2
    assert "stats" in body["diff"]
    assert "schema" in body["diff"]
    assert "head" in body["diff"]


def test_diff_route_explicit(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/shoe_sales/1/2")
    assert r.status_code == 200
    body = r.json()
    assert body["va"] == 1 and body["vb"] == 2


def test_diff_route_single_version_400(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    # Only 1 version: requesting vb=2 is out of range → 404.
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/shoe_sales/1/2")
    assert r.status_code == 404


def test_diff_route_no_alias_404(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/diff_data/missing/1/2").status_code == 404


def test_diff_route_same_version_400(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    # Diffing a version against itself is rejected.
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/shoe_sales/2/2")
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# polars backend
# ---------------------------------------------------------------------------

_A_PL = pl.DataFrame({"region": ["NE", "MW", "S"], "n": [10, 20, 30]})
_B_PL = pl.DataFrame({"region": ["NE", "S", "W"], "n": [15, 25, 35]})


def test_stats_diff_polars_matches_pandas():
    a_pd = _A_PL.to_pandas()
    b_pd = _B_PL.to_pandas()
    pd_result = {r["name"]: r for r in stats_diff(a_pd, b_pd)}
    pl_result = {r["name"]: r for r in stats_diff_polars(_A_PL, _B_PL)}
    assert set(pd_result) == set(pl_result)
    for col in pd_result:
        assert pd_result[col]["before"]["distinct"] == pl_result[col]["before"]["distinct"]
        assert pd_result[col]["before"]["nulls"] == pl_result[col]["before"]["nulls"]
        if pd_result[col]["before"]["numeric"]:
            assert abs(pd_result[col]["before"]["numeric"]["sum"] - pl_result[col]["before"]["numeric"]["sum"]) < 1e-6


def test_stats_diff_polars_numeric_summary():
    df = pl.DataFrame({"x": [1.0, 2.0, 3.0], "label": ["a", "b", "c"]})
    result = {r["name"]: r for r in stats_diff_polars(df, df)}
    assert result["x"]["before"]["numeric"]["mean"] == 2.0
    assert result["x"]["before"]["numeric"]["sum"] == 6.0
    assert result["label"]["before"]["numeric"] is None


def test_key_diff_polars_membership():
    d = key_diff_polars(_A_PL, _B_PL)
    assert d is not None
    assert d["keys"] == ["region"]
    assert d["matched"] == 2
    assert d["only_before"] == 1
    assert d["only_after"] == 1


def test_key_diff_polars_detects_unique_numeric_key():
    # See test_key_diff_detects_unique_numeric_key: a unique column is a key
    # regardless of dtype under 0.14.10's approximate-PK detection.
    a = pl.DataFrame({"x": [1, 2, 3]})
    b = pl.DataFrame({"x": [4, 5, 6]})
    d = key_diff_polars(a, b)
    assert d["keys"] == ["x"]
    assert d["matched"] == 0  # no shared values
    assert d["only_before"] == 3
    assert d["only_after"] == 3


def test_head_diff_polars_reads_n_rows(project: str, orders_parquet: Path):
    from pydata_xorq import build_and_persist
    res = build_and_persist(project, _agg_code(project))
    from pydata_core import entry_dir
    pq_path = entry_dir(project, res.content_hash) / "result.parquet"
    d = head_diff_polars(pq_path, pq_path, n=2)
    assert d["n"] == 2
    assert d["a_total"] == d["b_total"]
    assert "<table" in d["before"]


def test_full_diff_polars_backend(project: str, orders_parquet: Path):
    from pydata_core import entry_dir
    from pydata_xorq import build_and_persist
    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    diff = full_diff(entry_dir(project, a.content_hash), entry_dir(project, b.content_hash), backend="polars")
    assert diff["keyed"] is not None
    assert "stats" in diff


# ---------------------------------------------------------------------------
# xorq backend
# ---------------------------------------------------------------------------


def test_stats_diff_xorq_matches_pandas(project: str, orders_parquet: Path):
    from pydata_core import entry_dir
    from pydata_xorq import build_and_persist
    res = build_and_persist(project, _agg_code(project))
    pq_path = entry_dir(project, res.content_hash) / "result.parquet"
    pd_result = {r["name"]: r for r in stats_diff(
        pq.read_table(pq_path).to_pandas(),
        pq.read_table(pq_path).to_pandas(),
    )}
    xq_result = {r["name"]: r for r in stats_diff_xorq(pq_path, pq_path)}
    assert set(pd_result) == set(xq_result)
    for col in pd_result:
        assert pd_result[col]["before"]["distinct"] == xq_result[col]["before"]["distinct"]
        assert pd_result[col]["before"]["nulls"] == xq_result[col]["before"]["nulls"]
        if pd_result[col]["before"]["numeric"] and xq_result[col]["before"]["numeric"]:
            assert abs(
                pd_result[col]["before"]["numeric"]["sum"] - xq_result[col]["before"]["numeric"]["sum"]
            ) < 1e-6


def test_head_diff_xorq_reads_n_rows(project: str, orders_parquet: Path):
    from pydata_core import entry_dir
    from pydata_xorq import build_and_persist
    res = build_and_persist(project, _agg_code(project))
    pq_path = entry_dir(project, res.content_hash) / "result.parquet"
    d = head_diff_xorq(pq_path, pq_path, n=3)
    assert d["n"] == 3
    assert "<table" in d["before"]


def test_key_diff_xorq_membership(project: str, orders_parquet: Path):
    from pydata_core import entry_dir
    from pydata_xorq import build_and_persist
    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_pq = entry_dir(project, a.content_hash) / "result.parquet"
    b_pq = entry_dir(project, b.content_hash) / "result.parquet"
    d = key_diff_xorq(a_pq, b_pq)
    assert d is not None
    assert "region" in d["keys"]
    # All 4 regions have boots so both sides have the same keys; the diff
    # surfaces value changes (total, n), not missing rows.
    assert d["matched"] == 4
    assert d["only_before"] == 0
    assert d["only_after"] == 0
    assert "<table" in d["table_html"]


def test_full_diff_xorq_backend(project: str, orders_parquet: Path):
    from pydata_core import entry_dir
    from pydata_xorq import build_and_persist
    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    diff = full_diff(entry_dir(project, a.content_hash), entry_dir(project, b.content_hash), backend="xorq")
    assert diff["keyed"] is not None
    assert diff["keyed"]["matched"] == 4
    assert "stats" in diff


def test_build_compare_expr_reuses_session_build_dir(project: str, orders_parquet: Path):
    # The Buckaroo comparison embed builds an outer-join expr to a temp dir.
    # Re-rendering the same diff must reuse one session-scoped build dir, not
    # spawn a fresh mkdtemp per page view (which leaks /tmp across a demo).
    from pydata_companion.app import _build_compare_expr
    from pydata_xorq import build_and_persist
    from pydata_xorq.result_cache import cached_result_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)
    p1, _ = _build_compare_expr(a_expr, b_expr, ["region"])
    p2, _ = _build_compare_expr(a_expr, b_expr, ["region"])
    assert p1 == p2


def test_build_compare_expr_magnitude_coloring(project: str, orders_parquet: Path):
    # Numeric shared columns use DIVERGING_BLUE_WHITE_RED keyed on {col}_pct_delta;
    # non-numeric columns keep the categorical membership palette.
    from pydata_companion.app import _build_compare_expr
    from pydata_xorq import build_and_persist
    from pydata_xorq.result_cache import cached_result_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_expr = cached_result_expr(project, a.content_hash)
    b_expr = cached_result_expr(project, b.content_hash)
    _, overrides = _build_compare_expr(a_expr, b_expr, ["region"])

    # Numeric shared cols: new value displayed, old value via tooltip, pct coloring.
    for col in ("total", "n"):
        assert overrides[col]["merge_rule"] == "hidden"
        v2 = overrides[f"{col}_v2"]
        assert v2["header_name"] == col
        assert v2["tooltip_config"]["val_column"] == col
        assert v2["color_map_config"]["color_rule"] == "color_map"
        assert isinstance(v2["color_map_config"]["map_name"], list)  # inline DIVERGING_BLUE_WHITE_RED array
        assert v2["color_map_config"]["val_column"] == f"{col}_pct_delta"
        # _pct_delta hiding is handled by DiffMainStyling display klass, not global overrides
        assert f"{col}_pct_delta" not in overrides

    # "region" is the join key — must use categorical purple
    assert overrides["region"]["color_map_config"]["color_rule"] == "color_categorical"
