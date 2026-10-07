"""The live diff tells Buckaroo not to compute summary stats for the compare columns that no display shows a stat of.

Buckaroo runs a batch aggregate and one histogram query per column of the grid it is handed. The compare grid is three
to four times as wide as the entries it compares, and a third to a half of its columns are hidden: the before-value of
each column, its equality sentinel, and ``membership``. Nothing reads a stat of those, so they are sent as
``skip_stat_columns`` (``plans/diff-performance-proposals.md``, B1).
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xorq.api as xo

from tests.test_diff_snapshot import _cheap_pair, _diff_files, _open_diff, _worthy_pair

# What a diff of ``shoe_sales`` V1 to V2 (``region``, ``total``, ``n``) hides: ``membership``, the before-value and
# the equality sentinel of each value column, and the file's ``__row_order``.
SHOE_SALES_HIDDEN = {"membership", "total", "total_eq", "n", "n_eq", "__row_order"}


def _parquet_expr(tmp_path: Path, name: str, table: pa.Table):
    path = tmp_path / f"{name}.parquet"
    pq.write_table(table, path)
    return xo.deferred_read_parquet(str(path))


def _hidden(overrides: dict) -> set[str]:
    return {c for c, cfg in overrides.items() if cfg.get("merge_rule") == "hidden"}


def _body(project: str, alias: str) -> dict:
    """The one ``/load_expr`` body the diff route posts for V1 to V2 of *alias*."""
    posted: list[dict] = []
    _open_diff(project, alias, posted)
    assert len(posted) == 1
    return posted[0]


def _compare(tmp_path: Path, keys: list[str], a_cols: dict, b_cols: dict):
    from tallyman_companion.diff import build_compare_expr, strip_live_diff_color

    a = _parquet_expr(tmp_path, "a", pa.table(a_cols))
    b = _parquet_expr(tmp_path, "b", pa.table(b_cols))
    expr, overrides = build_compare_expr(a, b, keys)
    return expr, strip_live_diff_color(overrides)


# ---------------------------------------------------------------------------
# which columns
# ---------------------------------------------------------------------------


def test_skip_list_is_exactly_the_columns_the_overrides_hide(tmp_path: Path):
    """The before-value and the equality sentinel of each comparable column, and ``membership``. A column whose dtype
    changed to an incomparable one (#68) is shown side by side, so its before-value is visible and not skipped."""
    from tallyman_companion.diff import diff_skip_stat_columns

    expr, overrides = _compare(
        tmp_path,
        ["id"],
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "amt": pa.array([10, 20, 30], pa.int32()),
            "price": pa.array([1.5, 2.5, 3.5], pa.float64()),
            "name": pa.array(["a", "b", "c"], pa.string()),
            "issue_date": pa.array(["2024-01-01", "2024-02-01", "2024-03-01"], pa.string()),
            "removed": pa.array([1, 1, 1], pa.int64()),
        },
        {
            "id": pa.array([2, 3, 4], pa.int64()),
            "amt": pa.array([21, 30, 40], pa.int64()),
            "price": pa.array([2.5, 3.0, 4.5], pa.float64()),
            "name": pa.array(["b", "x", "d"], pa.string()),
            "issue_date": pa.array([dt.date(2024, 2, 1), dt.date(2024, 3, 2), dt.date(2024, 4, 1)], pa.date32()),
        },
    )

    skip = diff_skip_stat_columns(overrides)

    assert isinstance(skip, list)
    assert skip == sorted(set(skip)), "sorted and without repeats, so the posted body is stable"
    assert set(skip) == {"membership", "amt", "amt_eq", "price", "price_eq", "name", "name_eq"}
    visible = set(expr.columns) - set(skip)
    assert {
        "id",
        "amt_v2",
        "amt_pct_delta",
        "amt_abs_delta",
        "price_v2",
        "price_pct_delta",
        "price_abs_delta",
    } <= visible
    assert {"name_v2", "issue_date", "issue_date_v2", "removed"} <= visible
    assert set(skip) <= set(expr.columns), "a skipped name that the grid does not have would be a silent no-op"


def test_a_key_that_changed_dtype_is_never_skipped(tmp_path: Path):
    """#68: both sides of such a key are cast to string for the join, and the key stays the grid's colored column."""
    from tallyman_companion.diff import diff_skip_stat_columns

    expr, overrides = _compare(
        tmp_path,
        ["issue_date"],
        {"issue_date": pa.array(["2024-01-01", "2024-02-01"], pa.string()), "amt": pa.array([10, 20], pa.int64())},
        {
            "issue_date": pa.array([dt.date(2024, 1, 1), dt.date(2024, 3, 1)], pa.date32()),
            "amt": pa.array([15, 25], pa.int64()),
        },
    )

    skip = diff_skip_stat_columns(overrides)

    assert set(skip) == {"membership", "amt", "amt_eq"}
    assert "issue_date" in expr.columns and "issue_date" not in skip


def test_no_overrides_hide_nothing():
    from tallyman_companion.diff import diff_skip_stat_columns

    assert diff_skip_stat_columns({}) == []
    assert diff_skip_stat_columns({"a": {"header_name": "A"}, "b": {}}) == []


# ---------------------------------------------------------------------------
# what the route sends
# ---------------------------------------------------------------------------


def test_diff_load_posts_the_skip_list_as_a_list(project: str, orders_src: str, monkeypatch):
    """Buckaroo turns the field into a set, so a bare string would become a set of letters and skip nothing."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)

    skip = _body(project, "shoe_sales")["skip_stat_columns"]

    assert isinstance(skip, list)
    assert set(skip) == SHOE_SALES_HIDDEN
    assert len(skip) == len(set(skip))


@pytest.mark.parametrize("pair", [_worthy_pair, _cheap_pair], ids=["worthy", "cheap"])
def test_the_posted_skip_list_is_the_hidden_columns_of_the_posted_overrides_and_no_other(
    project: str, orders_src: str, monkeypatch, pair
):
    """Over every column of the file Buckaroo is handed: hidden ones are skipped, and no visible one is."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    pair(project)
    alias = "shoe_sales" if pair is _worthy_pair else "rows"

    body = _body(project, alias)

    [path] = _diff_files(project)
    columns = set(pq.read_schema(path).names)
    hidden = _hidden(body["column_config_overrides"]) & columns
    assert hidden, "the compare grid always hides membership"
    assert set(body["skip_stat_columns"]) == hidden
    assert columns - hidden, "something stays visible"
    assert not (columns - hidden) & set(body["skip_stat_columns"])


def test_the_other_fields_of_the_load_are_what_they_were(project: str, orders_src: str, monkeypatch):
    """The skip list is added to the body; the session, the stat cache, the data id, the stats delivery, the klasses
    and the overrides stay."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)

    body = _body(project, "shoe_sales")

    assert set(body) == {
        "session",
        "build_dir",
        "no_browser",
        "column_config_overrides",
        "skip_stat_columns",
        "cache_storage_path",
        "data_id",
        "stats_delivery",
        "extra_grid_config",
        "project_root",
    }
    assert body["column_config_overrides"]["membership"] == {"merge_rule": "hidden"}


# ---------------------------------------------------------------------------
# what Buckaroo does with it, over the file the route hands it
# ---------------------------------------------------------------------------


def _dataflow(body: dict, skip, cache_dir: Path):
    """The dataflow Buckaroo's ``LoadExprHandler`` builds for *body*, with *skip* as its ``skip_stat_columns``."""
    from buckaroo.server import xorq_loading

    expr = xorq_loading.load_expr_build_dir(body["build_dir"])
    klasses = xorq_loading.load_project_display_klasses(body["project_root"])
    return xorq_loading.XorqServerDataflow(
        expr,
        skip_main_serial=True,
        extra_klasses=klasses,
        cache_storage_path=str(cache_dir),
        column_config_overrides=body["column_config_overrides"],
        skip_stat_columns=skip,
    )


def _column_defs(display_arg: dict) -> list[dict]:
    return display_arg["df_viewer_config"]["column_config"]


@pytest.fixture
def shoe_sales_body(project: str, orders_src: str, monkeypatch) -> dict:
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)
    return _body(project, "shoe_sales")


def test_no_view_shows_a_skipped_column_or_reads_its_stats(shoe_sales_body: dict, tmp_path: Path):
    """The guard for what the skip list may not take away: no view draws a skipped column, and every ``color_map``
    rule reads the histogram of a column that keeps its stats, since the JS reads ``histogram_stats[val_column]``."""
    skip = set(shoe_sales_body["skip_stat_columns"])
    assert skip
    df = _dataflow(shoe_sales_body, shoe_sales_body["skip_stat_columns"], tmp_path / "skip")
    alias_to_name = {alias: stats["orig_col_name"] for alias, stats in df.merged_sd.items()}

    color_maps = 0
    assert {"main", "detailed_pct", "detailed_absolute", "summary"} <= set(df.df_display_args)
    for view, arg in df.df_display_args.items():
        for col in _column_defs(arg):
            if col.get("col_name") in alias_to_name:
                assert alias_to_name[col["col_name"]] not in skip, f"{view} draws a skipped column"
            cmc = col.get("color_map_config") or {}
            if cmc.get("color_rule") == "color_map":
                color_maps += 1
                assert alias_to_name[cmc["val_column"]] not in skip, f"{view} colors by a skipped column's histogram"
    assert color_maps, "no view colored by a histogram, so this guard checked nothing"


def test_display_config_is_the_same_with_and_without_the_skip_list(shoe_sales_body: dict, tmp_path: Path):
    """What the grid draws, in every view, and the stats it draws it from for each column that is not skipped."""
    skip = shoe_sales_body["skip_stat_columns"]
    assert skip
    with_skip = _dataflow(shoe_sales_body, skip, tmp_path / "with")
    without = _dataflow(shoe_sales_body, [], tmp_path / "without")

    assert with_skip.df_display_args == without.df_display_args

    def stats(df):
        return {
            stats["orig_col_name"]: {k: v for k, v in stats.items() if k != "histogram"}
            for stats in df.merged_sd.values()
            if stats["orig_col_name"] not in skip
        }

    assert stats(with_skip) == stats(without)


def test_skipped_columns_get_no_stat_queries_and_the_rest_keep_their_histograms(
    shoe_sales_body: dict, tmp_path: Path, monkeypatch
):
    """A cold stat cache both times. Also guards a Buckaroo that stops honoring the field."""
    from buckaroo.pluggable_analysis_framework.xorq_stat_pipeline import XorqStatPipeline

    queries: list[int] = []
    execute = XorqStatPipeline._execute

    def counting(self, query):
        queries.append(1)
        return execute(self, query)

    monkeypatch.setattr(XorqStatPipeline, "_execute", counting)
    skip = shoe_sales_body["skip_stat_columns"]

    _dataflow(shoe_sales_body, [], tmp_path / "without")
    n_without, queries[:] = len(queries), []
    with_skip = _dataflow(shoe_sales_body, skip, tmp_path / "with")
    n_with = len(queries)

    assert n_with < n_without
    by_name = {stats["orig_col_name"]: stats for stats in with_skip.merged_sd.values()}
    assert all("histogram" not in by_name[c] for c in skip)
    numeric_visible = ["total_v2", "total_pct_delta", "n_v2", "n_abs_delta"]
    assert all("histogram" in by_name[c] for c in numeric_visible)
    assert {c for c in by_name if "histogram" in by_name[c]} >= set(numeric_visible)
    assert all(by_name[c]["dtype"] and by_name[c]["length"] for c in skip), "name, dtype and length are kept"
