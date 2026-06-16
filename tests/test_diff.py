from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from tallyman_mcp.server import catalog_create, catalog_diff, catalog_revise
from tallyman_xorq import (
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
    from tallyman_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    from tallyman_core import entry_dir

    diff = full_diff(entry_dir(project, a.content_hash), entry_dir(project, b.content_hash))
    assert "highlight" in diff["code"]
    assert diff["schema"]["row_count"]["before"] == 4
    assert "stats" in diff
    assert diff["keyed"] is not None  # region is a shared key


# ---------------------------------------------------------------------------
# catalog_diff MCP tool
# ---------------------------------------------------------------------------


def test_catalog_diff_default_compares_n_minus_one_to_n(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_diff("shoe_sales")
    assert "error" not in out
    assert out["before"]["version"] == 1
    assert out["after"]["version"] == 2


def test_catalog_diff_explicit_versions(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_diff("shoe_sales", 1, 2)
    assert "error" not in out


def test_catalog_diff_no_history(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_diff("nonexistent")
    assert "error" in out


def test_catalog_diff_out_of_range(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_diff("shoe_sales", 1, 5)
    assert "error" in out


# ---------------------------------------------------------------------------
# /diff/<alias>/<va>/<vb> route
# ---------------------------------------------------------------------------


def test_diff_route_default(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/shoe_sales/1/2")
    assert r.status_code == 200
    body = r.json()
    assert body["va"] == 1 and body["vb"] == 2


def test_diff_route_single_version_400(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    # Only 1 version: requesting vb=2 is out of range → 404.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/shoe_sales/1/2")
    assert r.status_code == 404


def test_diff_route_no_alias_404(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/diff_data/missing/1/2").status_code == 404


def test_diff_route_same_version_400(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    # Diffing a version against itself is rejected.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
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
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project, _agg_code(project))
    from tallyman_xorq.result_cache import ensure_result

    pq_path = ensure_result(project, res.content_hash)
    d = head_diff_polars(pq_path, pq_path, n=2)
    assert d["n"] == 2
    assert d["a_total"] == d["b_total"]
    assert "<table" in d["before"]


def test_full_diff_polars_backend(project: str, orders_parquet: Path):
    from tallyman_core import entry_dir
    from tallyman_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    diff = full_diff(entry_dir(project, a.content_hash), entry_dir(project, b.content_hash), backend="polars")
    assert diff["keyed"] is not None
    assert "stats" in diff


# ---------------------------------------------------------------------------
# xorq backend
# ---------------------------------------------------------------------------


def test_stats_diff_xorq_matches_pandas(project: str, orders_parquet: Path):
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project, _agg_code(project))
    from tallyman_xorq.result_cache import ensure_result

    pq_path = ensure_result(project, res.content_hash)
    pd_result = {
        r["name"]: r
        for r in stats_diff(
            pq.read_table(pq_path).to_pandas(),
            pq.read_table(pq_path).to_pandas(),
        )
    }
    xq_result = {r["name"]: r for r in stats_diff_xorq(pq_path, pq_path)}
    assert set(pd_result) == set(xq_result)
    for col in pd_result:
        assert pd_result[col]["before"]["distinct"] == xq_result[col]["before"]["distinct"]
        assert pd_result[col]["before"]["nulls"] == xq_result[col]["before"]["nulls"]
        if pd_result[col]["before"]["numeric"] and xq_result[col]["before"]["numeric"]:
            assert abs(pd_result[col]["before"]["numeric"]["sum"] - xq_result[col]["before"]["numeric"]["sum"]) < 1e-6


def test_head_diff_xorq_reads_n_rows(project: str, orders_parquet: Path):
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project, _agg_code(project))
    from tallyman_xorq.result_cache import ensure_result

    pq_path = ensure_result(project, res.content_hash)
    d = head_diff_xorq(pq_path, pq_path, n=3)
    assert d["n"] == 3
    assert "<table" in d["before"]


def test_key_diff_xorq_membership(project: str, orders_parquet: Path):
    from tallyman_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    from tallyman_xorq.result_cache import ensure_result

    a_pq = ensure_result(project, a.content_hash)
    b_pq = ensure_result(project, b.content_hash)
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
    from tallyman_core import entry_dir
    from tallyman_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    diff = full_diff(entry_dir(project, a.content_hash), entry_dir(project, b.content_hash), backend="xorq")
    assert diff["keyed"] is not None
    assert diff["keyed"]["matched"] == 4
    assert "stats" in diff


def test_full_diff_xorq_with_exprs_writes_no_result_parquet(project: str, orders_parquet: Path):
    # #73: the wired diff path passes both entries' cache-resolving expressions,
    # which compose at the expression level. full_diff must NOT eagerly
    # materialise a result.parquet for either entry — those parquets are never
    # read on this path and re-introduce exactly the build-time materialisation
    # #73 removed.
    from tallyman_core import entry_dir
    from tallyman_xorq import build_and_persist
    from tallyman_xorq.result_cache import cached_result_expr

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    a_dir, b_dir = entry_dir(project, a.content_hash), entry_dir(project, b.content_hash)
    diff = full_diff(
        a_dir,
        b_dir,
        backend="xorq",
        a_expr=cached_result_expr(project, a.content_hash),
        b_expr=cached_result_expr(project, b.content_hash),
    )
    assert diff["keyed"] is not None
    assert not (a_dir / "result.parquet").exists()
    assert not (b_dir / "result.parquet").exists()


def test_build_compare_expr_reuses_session_build_dir(project: str, orders_parquet: Path):
    # The Buckaroo comparison embed builds an outer-join expr to a temp dir.
    # Re-rendering the same diff must reuse one session-scoped build dir, not
    # spawn a fresh mkdtemp per page view (which leaks /tmp across a demo).
    from tallyman_companion.app import _build_compare_expr
    from tallyman_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    p1, _ = _build_compare_expr(project, a.content_hash, b.content_hash, ("region",))
    p2, _ = _build_compare_expr(project, a.content_hash, b.content_hash, ("region",))
    assert p1 == p2


def test_build_compare_expr_magnitude_coloring(project: str, orders_parquet: Path):
    # Numeric value-column coloring is applied per-view by the diff display
    # klasses (main/detailed_pct color by pct_delta, detailed_absolute by
    # abs_delta), NOT by the shared global override — which would clobber the
    # per-view color via merge_column_config. The global override only renames
    # the _v2 column, attaches the old-value tooltip, and hides the a-side.
    # Non-numeric columns keep the categorical membership palette.
    from tallyman_companion.app import _build_compare_expr
    from tallyman_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    _, overrides = _build_compare_expr(project, a.content_hash, b.content_hash, ("region",))

    # Numeric shared cols: new value displayed, old value via tooltip, a-side
    # hidden. No color_map_config here — coloring is per-view in the klasses.
    for col in ("total", "n"):
        assert overrides[col]["merge_rule"] == "hidden"
        v2 = overrides[f"{col}_v2"]
        assert v2["header_name"] == col
        assert v2["tooltip_config"]["val_column"] == col
        assert "color_map_config" not in v2

    # "region" is the join key — must use categorical purple
    assert overrides["region"]["color_map_config"]["color_rule"] == "color_categorical"


def test_compute_column_config_overrides_type_changed_falls_back(project: str):
    # Issue #68: a non-key column that keeps its name but changes dtype across
    # revisions (string→date) is not comparable. The override must not reference
    # a {col}_eq term (none is emitted) and must leave the a-side visible so the
    # column renders side-by-side, while unchanged columns keep their coloring.
    import xorq.vendor.ibis as ibis

    from tallyman_companion.diff import compute_column_config_overrides

    a = ibis.schema({"id": "int64", "issue_date": "string", "amt": "int64", "label": "string"})
    b = ibis.schema({"id": "int64", "issue_date": "date", "amt": "int64", "label": "string"})
    ov = compute_column_config_overrides(a, b, ["id"])

    # type-changed col: side-by-side, no _eq override, a-side not hidden, no coloring
    assert "issue_date_eq" not in ov
    assert ov.get("issue_date", {}).get("merge_rule") != "hidden"
    assert "color_map_config" not in ov["issue_date_v2"]

    # unchanged numeric col: a-side hidden, value colored by pct_delta
    assert ov["amt"]["merge_rule"] == "hidden"
    assert ov["amt_v2"]["color_map_config"]["val_column"] == "amt_pct_delta"

    # unchanged string col: a-side hidden, value colored categorically by _eq
    assert ov["label"]["merge_rule"] == "hidden"
    assert ov["label_v2"]["color_map_config"]["val_column"] == "label_eq"


def test_build_compare_expr_type_changed_shared_column(tmp_path):
    # Issue #68: building the comparison expr over a string→date column must not
    # emit a raw {col} == {col}_v2 equality term, which xorq rejects with
    # XorqTypeError. The column falls back to side-by-side display: both sides
    # present, no {col}_eq column.
    import datetime as dt

    import pyarrow as pa
    import xorq.api as xo

    from tallyman_companion.diff import build_compare_expr

    a_tbl = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "issue_date": pa.array(["2024-01-01", "2024-02-01"], pa.string()),
        }
    )
    b_tbl = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "issue_date": pa.array([dt.date(2024, 1, 1), dt.date(2024, 3, 1)], pa.date32()),
        }
    )
    a_path = tmp_path / "a.parquet"
    b_path = tmp_path / "b.parquet"
    pq.write_table(a_tbl, a_path)
    pq.write_table(b_tbl, b_path)

    a_expr = xo.deferred_read_parquet(str(a_path))
    b_expr = xo.deferred_read_parquet(str(b_path))

    expr, overrides = build_compare_expr(a_expr, b_expr, ["id"])
    cols = set(expr.schema().names)
    assert "issue_date" in cols  # a-side present
    assert "issue_date_v2" in cols  # b-side present
    assert "issue_date_eq" not in cols  # no ill-typed equality term
    assert "issue_date_eq" not in overrides


def test_build_compare_expr_type_changed_key_column(tmp_path):
    # Issue #68 (reopened after #69): #69 gated the non-key {col}_eq term, but
    # the same string→date failure still aborts the diff through the *join key*.
    # build_compare_expr joins on raw key equality (a_expr[k] == b_renamed[k])
    # and coalesces the key — both type-sensitive. When the key keeps its name
    # but changes dtype across revisions, xorq raises XorqTypeError and the whole
    # diff pipeline aborts. Both sides of such a key are cast to a common type
    # (string) for the join and coalesce so the comparison is well-typed.
    import datetime as dt

    import pyarrow as pa
    import xorq.api as xo

    from tallyman_companion.diff import build_compare_expr

    a_tbl = pa.table(
        {
            "issue_date": pa.array(["2024-01-01", "2024-02-01"], pa.string()),
            "amt": pa.array([10, 20], pa.int64()),
        }
    )
    b_tbl = pa.table(
        {
            "issue_date": pa.array([dt.date(2024, 1, 1), dt.date(2024, 3, 1)], pa.date32()),
            "amt": pa.array([15, 25], pa.int64()),
        }
    )
    a_path = tmp_path / "a.parquet"
    b_path = tmp_path / "b.parquet"
    pq.write_table(a_tbl, a_path)
    pq.write_table(b_tbl, b_path)

    a_expr = xo.deferred_read_parquet(str(a_path))
    b_expr = xo.deferred_read_parquet(str(b_path))

    # Building and executing must not raise XorqTypeError on the type-changed key.
    expr, _ = build_compare_expr(a_expr, b_expr, ["issue_date"])
    assert "issue_date" in expr.schema().names
    df = expr.execute()
    # The string-cast date matches the string side: 2024-01-01 is shared,
    # 2024-02-01 is a-only, 2024-03-01 is b-only — a well-formed outer join.
    assert {"2024-01-01", "2024-02-01", "2024-03-01"} == {str(d) for d in df["issue_date"]}


def test_diff_display_klasses_per_view():
    """The three diff display klasses produce the right per-view columns,
    ordering, and coloring from raw (pre-override) column configs.

    style_columns runs before column_config_overrides are merged, so value
    columns still carry their raw ``{col}_v2`` headers and deltas their
    ``{col}_pct_delta`` / ``{col}_abs_delta`` headers; col_name is the short
    alias the JS color map references. A delta column whose min and max both
    round to 0.000 at display precision (|v| < 5e-4) is dropped — covering both
    exact zeros and floating-point noise (e.g. 1e-6 lat/lng deltas).
    """
    from buckaroo.server.xorq_loading import load_project_display_klasses

    import tallyman_companion

    root = str(Path(tallyman_companion.__file__).parent / "diff_extras")
    klasses = {k.df_display_name: k for k in load_project_display_klasses(root)}
    assert {"main", "detailed_pct", "detailed_absolute"} <= set(klasses)

    cols = [
        "station",
        "price_v2",
        "price_pct_delta",
        "price_abs_delta",
        "vol_v2",
        "vol_pct_delta",
        "vol_abs_delta",
        "lat_v2",
        "lat_pct_delta",
        "lat_abs_delta",
    ]
    df = pd.DataFrame({c: [0, 0] for c in cols})

    def meta(t, orig, mn, mx):
        return {"_type": t, "orig_col_name": orig, "min": mn, "max": mx}

    # 'price' changed (non-zero deltas); 'vol' unchanged (exactly-zero deltas);
    # 'lat' has only floating-point noise (~1e-6) that displays as 0.000 and
    # must also be dropped.
    sd = {
        "a": meta("string", "station", 0, 0),
        "b": meta("float", "price_v2", 1.0, 9.0),
        "c": meta("float", "price_pct_delta", -0.5, 0.5),
        "d": meta("float", "price_abs_delta", -3.0, 3.0),
        "e": meta("float", "vol_v2", 1.0, 9.0),
        "f": meta("float", "vol_pct_delta", 0.0, 0.0),
        "g": meta("float", "vol_abs_delta", 0.0, 0.0),
        "h": meta("float", "lat_v2", 40.0, 41.0),
        "i": meta("float", "lat_pct_delta", 0.0, 2.4e-08),
        "j": meta("float", "lat_abs_delta", -1.0e-06, 1.0e-06),
    }

    def headers(cfgs):
        return [c["header_name"] for c in cfgs]

    def color(cfgs, header):
        for c in cfgs:
            if c["header_name"] == header:
                cmc = c.get("color_map_config")
                return cmc["val_column"] if cmc else None
        return None

    # main: no delta columns; value column colored by its pct_delta (alias c).
    main = klasses["main"].style_columns(sd, df)
    assert not any(h.endswith(("_pct_delta", "_abs_delta")) for h in headers(main))
    assert color(main, "price_v2") == "c"

    # detailed_pct: pct_delta adjacent (after) its value column; abs_delta and
    # all-zero/noise pct_delta dropped; both colored by pct_delta.
    pct = klasses["detailed_pct"].style_columns(sd, df)
    hp = headers(pct)
    assert not any(h.endswith("_abs_delta") for h in hp)
    assert "vol_pct_delta" not in hp  # exactly-zero dropped
    assert "lat_pct_delta" not in hp  # ~1e-8 noise dropped
    assert hp.index("price_pct_delta") == hp.index("price_v2") + 1
    assert color(pct, "price_v2") == "c"
    assert color(pct, "price_pct_delta") == "c"

    # detailed_absolute: abs_delta adjacent; pct_delta and all-zero/noise
    # abs_delta dropped; both colored by abs_delta (alias d), NOT pct_delta.
    ab = klasses["detailed_absolute"].style_columns(sd, df)
    ha = headers(ab)
    assert not any(h.endswith("_pct_delta") for h in ha)
    assert "vol_abs_delta" not in ha  # exactly-zero dropped
    assert "lat_abs_delta" not in ha  # ~1e-6 noise dropped
    assert ha.index("price_abs_delta") == ha.index("price_v2") + 1
    assert color(ab, "price_v2") == "d"
    assert color(ab, "price_abs_delta") == "d"
