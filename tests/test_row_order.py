"""ADR-008 (plans/ADR-008-row-order-of-reads.md): every file tallyman writes carries ``__row_order``.

Red tests for what a build produces: the column itself (ADR-008 D2), the build error for a cheap entry that drops it
(ADR-008 D3), the test that decides which entries are cheap (ADR-008 D4), the reserved name and joins (ADR-008 D6),
CSV roots (ADR-008 D7) and raw parquet reads (ADR-008 D12). Pages (ADR-008 D5) are in
``tests/test_row_order_pages.py``; sort grafting and hoisting (ADR-008 D10 and D11) in
``tests/test_row_order_sorts.py``.

The new API these tests touch (``tallyman_xorq.materialize.snapshot_path`` and
``tallyman_xorq.worthiness.classify_expr``) is imported inside helpers, not at module top: a top-level import of a
module that does not exist yet makes ruff mis-group the block, the CI lint job fails, and the tests never run.
"""

from __future__ import annotations

import datetime
import os
import re
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xorq.api as xo
import xorq.vendor.ibis as ibis
from polars.exceptions import PanicException

from tallyman_companion.diff import build_compare_expr, build_diff_expr
from tallyman_core import data_dir, read_manifest, set_alias
from tallyman_core.paths import compute_cache_dir, entry_build_dir, entry_dir
from tallyman_xorq.build import BuildError, build_and_persist
from tallyman_xorq.io import read_project_file
from tallyman_xorq.primary_key import resolve_primary_key
from tallyman_xorq.result_cache import cache_worthy, cached_result_expr

ROW_ORDER = "__row_order"
ROW_ORDER_RIGHT = "__row_order_right"

_PRELUDE = """
import xorq.api as xo
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import pinned_expr_from_alias, read_project_file, tallyman_read_csv, tracked_expr_from_alias
"""

# A select of these columns is written the way the ADR-008 D3 error shows it: t.select("g", "n", "__row_order").
_CORRECTED_SELECT = re.compile(r"""select\(\s*["']region["'],\s*["']price["'],\s*["']__row_order["']\s*\)""")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _over(project: str, source: str, body: str) -> str:
    """A recipe over one source file: ``t`` is ``read_project_file(source)`` and ``expr`` is *body*."""
    return f"{_PRELUDE}t = read_project_file({source!r}, project={project!r})\nexpr = {body}\n"


def _orders(project: str, body: str) -> str:
    """A recipe over the shoe-orders source: ``t`` is a read of orders.parquet and ``expr`` is *body*."""
    return _over(project, "orders.parquet", body)


def _chained(alias: str, body: str) -> str:
    """A recipe that builds on the entry named *alias*: ``t`` is that entry and ``expr`` is *body*."""
    return f"{_PRELUDE}t = tracked_expr_from_alias({alias!r})\nexpr = {body}\n"


def _create(project: str, alias: str, code: str) -> str:
    """Build *code* and name the entry, so a later recipe can chain off it; returns the content hash."""
    content_hash = build_and_persist(project, code).content_hash
    set_alias(project, alias, content_hash, expect_exists=False)
    return content_hash


def _write(project: str, name: str, columns: dict) -> Path:
    """A small parquet source under the project's data dir."""
    path = data_dir(project) / name
    pq.write_table(pa.table(columns), path)
    return path


def _names(res) -> list[str]:
    """The column names a build recorded in its schema."""
    return [f["name"] for f in res.schema["fields"]]


def _snapshot_path(project: str, content_hash: str) -> Path:
    from tallyman_xorq.materialize import snapshot_path

    return snapshot_path(project, content_hash)


def _classify(expr):
    from tallyman_xorq.worthiness import classify_expr

    return classify_expr(expr)


def _ordered_copies(project: str) -> list[Path]:
    """Ordered copies of sources: ADR-007 D13 puts them under the project's compute cache."""
    return sorted((compute_cache_dir(project) / "ordered_sources").glob("*.parquet"))


def _csv_recipe(csv: Path) -> str:
    return (
        "import xorq.vendor.ibis as ibis\n"
        "from tallyman_xorq.io import tallyman_read_csv\n"
        f"expr = tallyman_read_csv({str(csv)!r}, schema=ibis.schema({{'k': 'int64', 'v': 'int64'}}))\n"
    )


# --------------------------------------------------------------------------- #
# ADR-008 D2: every file tallyman reads carries __row_order
# --------------------------------------------------------------------------- #
def test_editing_a_csv_forks_the_content_hash(project):
    """ADR-008 D2 (ordered copy built from the content-addressed clone), #168: an edit is a new entry.

    Today the intermediate is keyed by the CSV's path and overwritten in place, so the recipe hashes to the same
    entry before and after the edit and the new rows are never seen.
    """
    csv = data_dir(project) / "edited.csv"
    csv.write_text("k,v\n1,10\n2,20\n3,30\n")
    first = build_and_persist(project, _csv_recipe(csv)).content_hash

    csv.write_text("k,v\n1,10\n2,999\n3,30\n4,40\n")
    st = csv.stat()
    os.utime(csv, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000_000))  # newer than any filesystem clock granularity
    second = build_and_persist(project, _csv_recipe(csv)).content_hash

    assert second != first, "editing the CSV and re-running the same recipe must create a new entry"
    cached_result_expr.cache_clear()
    assert cached_result_expr(project, first).execute()["v"].tolist() == [10, 20, 30]
    assert cached_result_expr(project, second).execute()["v"].tolist() == [10, 999, 30, 40]


def test_a_worthy_snapshot_ends_in_row_order(project, orders_parquet):
    """ADR-008 D2: a snapshot ends in an int64 ``__row_order`` holding ``0..N-1`` in the file's row order."""
    res = build_and_persist(project, _orders(project, "t.order_by(t.price.desc())"))
    assert _names(res)[-1] == ROW_ORDER, f"the entry's schema must end in __row_order, got {_names(res)}"

    table = pq.read_table(_snapshot_path(project, res.content_hash))
    assert table.column_names[-1] == ROW_ORDER
    assert table.schema.field(ROW_ORDER).type == pa.int64()
    assert table[ROW_ORDER].to_pylist() == list(range(table.num_rows))
    prices = table["price"].to_pylist()
    assert prices == sorted(prices, reverse=True), "numbered in the order the file is written in"


def test_the_ordered_copy_of_a_parquet_source_ends_in_row_order(project, orders_parquet):
    """ADR-008 D2: a parquet source enters tallyman through an ordered copy with ``__row_order`` last."""
    build_and_persist(project, _orders(project, "t"))

    copies = _ordered_copies(project)
    assert len(copies) == 1, f"expected one ordered copy under compute_cache/ordered_sources, found {copies}"
    copy, source = pq.read_table(copies[0]), pq.read_table(orders_parquet)
    assert copy.column_names == [*source.column_names, ROW_ORDER]
    assert copy.schema.field(ROW_ORDER).type == pa.int64()
    assert copy[ROW_ORDER].to_pylist() == list(range(source.num_rows))
    for name in source.column_names:
        assert copy[name].to_pylist() == source[name].to_pylist(), f"{name} must be copied in file order"


def test_the_ordered_copy_of_a_csv_source_ends_in_row_order(project):
    """ADR-008 D2: a CSV source enters tallyman through an ordered copy with ``__row_order`` last."""
    csv = data_dir(project) / "ordered.csv"
    csv.write_text("k,v\n3,30\n1,10\n2,20\n")
    build_and_persist(project, _csv_recipe(csv))

    copies = _ordered_copies(project)
    assert len(copies) == 1, f"expected one ordered copy under compute_cache/ordered_sources, found {copies}"
    copy = pq.read_table(copies[0])
    assert copy.column_names == ["k", "v", ROW_ORDER]
    assert copy["k"].to_pylist() == [3, 1, 2]  # file order, not sorted order
    assert copy[ROW_ORDER].to_pylist() == [0, 1, 2]


def test_a_parquet_source_with_its_own_row_order_column_has_it_overwritten(project):
    """ADR-008 D2: a source that already has ``__row_order`` (a file tallyman exported) has it overwritten."""
    _write(project, "exported.parquet", {"k": [10, 20, 30, 40], ROW_ORDER: [5, 3, 9, 1]})
    code = f"{_PRELUDE}expr = read_project_file('exported.parquet', project={project!r})\n"
    res = build_and_persist(project, code)

    df = cached_result_expr(project, res.content_hash).execute()
    assert list(df.columns) == ["k", ROW_ORDER]
    assert df["k"].tolist() == [10, 20, 30, 40]
    assert df[ROW_ORDER].tolist() == [0, 1, 2, 3], "the source's own values must be overwritten with 0..N-1"


def _arrow_fields(schema: pa.Schema) -> list[tuple[str, str]]:
    return [(f.name, str(f.type)) for f in schema]


def _ibis_fields(schema) -> list[tuple[str, str]]:
    return [(name, str(dtype)) for name, dtype in schema.items()]


def test_the_ordered_copy_of_a_parquet_source_keeps_the_sources_types(project):
    """#197: the copy's schema is the source's plus ``__row_order``, so a recipe sees the types a direct read gives.

    polars used to write the copy, and changed types on the way: a ``date64`` came back as a timestamp, a map as a list
    of structs, and a ``time32`` or ``time64`` as ``time64[ns]``. ``fixed_size_binary`` is left out: xorq cannot read it
    from the source either.
    """
    source = _write(
        project,
        "typed.parquet",
        {
            "day": pa.array([datetime.date(2026, 9, 22), None], pa.date64()),
            "tags": pa.array([[("a", 1)], [("b", 2), ("c", 3)]], pa.map_(pa.string(), pa.int64())),
            "clock_s": pa.array([datetime.time(1, 2, 3), None], pa.time32("s")),
            "clock_us": pa.array([datetime.time(1, 2, 3, 4), None], pa.time64("us")),
        },
    )
    seen = read_project_file("typed.parquet", project=project).schema()

    [copy] = _ordered_copies(project)
    assert _arrow_fields(pq.read_schema(copy)) == [*_arrow_fields(pq.read_schema(source)), (ROW_ORDER, "int64")]
    direct = xo.deferred_read_parquet(str(source)).schema()
    assert _ibis_fields(seen) == [*_ibis_fields(direct), (ROW_ORDER, "int64")]
    assert pq.read_table(copy).drop_columns([ROW_ORDER]).equals(pq.read_table(source))


def test_a_decimal256_source_builds_and_keeps_its_type(project):
    """#197: polars holds at most 38 decimal digits, and writing the copy of a ``decimal256(40, 2)`` column panicked.

    The panic is a ``PanicException``, a ``BaseException``, so no ``except Exception`` on the build or MCP path caught
    it. Caught here so the test fails instead of the panic escaping it.
    """
    amounts = [Decimal("1.25"), Decimal("12345678901234567890123456789012345678.90")]
    _write(project, "wide.parquet", {"amount": pa.array(amounts, pa.decimal256(40, 2))})
    try:
        res = build_and_persist(project, _over(project, "wide.parquet", "t"))
    except PanicException as exc:
        pytest.fail(f"building over a decimal256 source raised a PanicException, not a BuildError: {exc}")

    [copy] = _ordered_copies(project)
    assert pq.read_schema(copy).field("amount").type == pa.decimal256(40, 2)
    assert cached_result_expr(project, res.content_hash).execute()["amount"].tolist() == amounts


def test_a_worthy_entry_that_keeps_its_parents_rows_renumbers_them(project, orders_parquet):
    """ADR-008 D2: ``materialize`` replaces an inherited ``__row_order`` with positions in its own file.

    A filter leaves gaps in the parent's positions. The entry is worthy because of the window function, and its file
    numbers its own rows ``0..M-1`` with no gaps.
    """
    _create(project, "orders", _orders(project, "t"))
    child = build_and_persist(project, _chained("orders", "t.filter(t.qty > 2).mutate(rn=ibis.row_number())"))
    assert _names(child)[-1] == ROW_ORDER, f"the entry's schema must end in __row_order, got {_names(child)}"

    table = pq.read_table(_snapshot_path(project, child.content_hash))
    assert 0 < table.num_rows < 200
    assert table.column_names[-1] == ROW_ORDER
    assert table[ROW_ORDER].to_pylist() == list(range(table.num_rows))


# --------------------------------------------------------------------------- #
# ADR-008 D3: a cheap entry that drops __row_order is a build error
# --------------------------------------------------------------------------- #
def test_a_cheap_select_that_omits_row_order_names_its_parent_and_the_fix(project, orders_parquet):
    """ADR-008 D3: the error names the parent entry and shows the corrected select."""
    parent = _create(project, "orders", _orders(project, "t"))
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, _chained("orders", "t.select('region', 'price')"))
    msg = str(exc.value)
    assert ROW_ORDER in msg, msg
    assert _CORRECTED_SELECT.search(msg), f"the message must show the corrected select: {msg}"
    assert parent in msg or parent[:12] in msg or "orders" in msg, f"the message must name the parent entry: {msg}"


def test_a_cheap_select_over_a_source_read_omits_row_order_with_the_fix(project, orders_parquet):
    """ADR-008 D3: the same error when the cheap entry reads a source file directly."""
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, _orders(project, "t.select('region', 'price')"))
    msg = str(exc.value)
    assert ROW_ORDER in msg, msg
    assert _CORRECTED_SELECT.search(msg), f"the message must show the corrected select: {msg}"


def test_the_same_select_over_a_worthy_recipe_builds_and_is_numbered(project, orders_parquet):
    """ADR-008 D3: a worthy entry is exempt, because the writer numbers its rows."""
    res = build_and_persist(
        project, _orders(project, "t.group_by('region').aggregate(n=t.count()).select('region', 'n')")
    )
    assert _names(res) == ["region", "n", ROW_ORDER]


def test_a_computed_column_added_after_row_order_leaves_it_last(project, orders_parquet):
    """ADR-008 D3: tallyman moves ``__row_order`` to the last position, at the top of the expression only."""
    res = build_and_persist(project, _orders(project, "t.mutate(double=t.price * 2)"))
    names = _names(res)
    assert "double" in names
    assert names.count(ROW_ORDER) == 1 and names[-1] == ROW_ORDER, names
    assert cache_worthy(project, res.content_hash) is False


# --------------------------------------------------------------------------- #
# ADR-008 D3 / D6: asking for an order renumbers, assigning is an error
# --------------------------------------------------------------------------- #
def test_asking_for_an_order_numbers_the_file_in_that_order(project):
    """ADR-008 D3: an ``order_by`` makes the entry worthy and the writer numbers the rows in the requested order."""
    _write(project, "amounts.parquet", {"name": list("abcdef"), "amount": [40, 10, 60, 20, 50, 30]})
    res = build_and_persist(project, _over(project, "amounts.parquet", "t.order_by(t.amount.desc())"))

    df = cached_result_expr(project, res.content_hash).execute()
    assert ROW_ORDER in df.columns, f"the entry must carry __row_order, got {list(df.columns)}"
    assert df["amount"].tolist() == [60, 50, 40, 30, 20, 10]
    assert df[ROW_ORDER].tolist() == [0, 1, 2, 3, 4, 5]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("t.mutate(__row_order=t.order_id * 2)", id="mutate"),
        pytest.param("t.select('region', __row_order=t.order_id)", id="select"),
        pytest.param("t.mutate(__row_order=t.order_id).order_by('region')", id="worthy entry"),
    ],
)
def test_assigning_to_row_order_is_a_build_error(project, orders_parquet, body):
    """ADR-008 D6: arbitrary values could contain ties or gaps, so a recipe may not assign to the column."""
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, _orders(project, body))
    assert ROW_ORDER in str(exc.value)


def test_a_debugging_copy_of_row_order_survives_materialization(project, orders_parquet):
    """ADR-008 D6: ``__row_order_v1`` is ordinary data: it keeps the parent's positions after the child is written."""
    parent = _create(project, "foo_v1", _orders(project, "t.order_by(t.price.desc())"))
    child_code = (
        f"{_PRELUDE}t = tracked_expr_from_alias('foo_v1')\n"
        "c = t.mutate(__row_order_v1=t['__row_order'])\n"
        "expr = c.order_by(c.qty)\n"
    )
    child = build_and_persist(project, child_code)

    parent_df = pq.read_table(_snapshot_path(project, parent)).to_pandas()
    kid = pq.read_table(_snapshot_path(project, child.content_hash)).to_pandas()
    assert "__row_order_v1" in kid.columns and list(kid.columns)[-1] == ROW_ORDER
    assert kid[ROW_ORDER].tolist() == list(range(len(kid))), "the child's own positions"
    joined = kid.merge(parent_df[["order_id", ROW_ORDER]], on="order_id", suffixes=("", "_parent"))
    assert len(joined) == len(kid)
    assert (joined["__row_order_v1"] == joined[f"{ROW_ORDER}_parent"]).all(), "the copy keeps the parent's positions"
    assert not (joined[ROW_ORDER] == joined[f"{ROW_ORDER}_parent"]).all(), "the sort by qty moved the rows"


# --------------------------------------------------------------------------- #
# ADR-008 D4: cheap means row-preserving over one file
# --------------------------------------------------------------------------- #
def _live_tables(tmp_path: Path):
    """Two files that already carry ``__row_order``, read as live expressions (nothing is built)."""
    n = 6
    tags = [["x", "y"], ["z"], [], ["x"], ["y", "z", "w"], ["q"]]
    pq.write_table(
        pa.table({"k": list(range(n)), "v": [1.5, 2.5, None, 4.5, 5.5, 6.5], "tags": tags, ROW_ORDER: list(range(n))}),
        tmp_path / "t.parquet",
    )
    pq.write_table(pa.table({"k": [1, 3, 5], ROW_ORDER: [0, 1, 2]}), tmp_path / "u.parquet")
    return xo.deferred_read_parquet(str(tmp_path / "t.parquet")), xo.deferred_read_parquet(str(tmp_path / "u.parquet"))


_WORTHY_SHAPES = {
    "union": lambda t, u: t.union(t),
    "distinct": lambda t, u: t.select("k", ROW_ORDER).distinct(),
    "relation-level unnest": lambda t, u: t.unnest("tags"),
    "an operation the allow-list has never seen": lambda t, u: t.sample(0.5),
    "limit": lambda t, u: t.limit(3),
    "sort": lambda t, u: t.order_by("k"),
    "aggregate": lambda t, u: t.group_by("k").aggregate(n=t.count()),
    "join": lambda t, u: t.join(u, "k"),
    "value-level unnest inside a select": lambda t, u: t.select("k", ROW_ORDER, tag=t.tags.unnest()),
    "row_number() in a mutate": lambda t, u: t.mutate(rn=ibis.row_number()),
    "lag() in a mutate": lambda t, u: t.mutate(prev=t.v.lag()),
    "random() in a mutate": lambda t, u: t.mutate(r=ibis.random()),
    "now() in a mutate": lambda t, u: t.mutate(ts=ibis.now()),
    "a filter against a second file": lambda t, u: t.filter(t.k.isin(u.k)),
}
_CHEAP_SHAPES = {
    "a bare read": lambda t, u: t,
    "filter and a computed column": lambda t, u: t.filter(t.k > 0).mutate(w=t.v * 2),
    "rename, cast and drop": lambda t, u: t.rename(key="k").mutate(v=t.v.cast("float32")).drop("tags"),
    "drop_null": lambda t, u: t.drop_null(["v"]),
    "fill_null": lambda t, u: t.fill_null({"v": 0.0}),
}


@pytest.mark.parametrize("label", list(_WORTHY_SHAPES))
def test_the_cheap_test_classifies_these_shapes_as_worthy(tmp_path, label):
    """ADR-008 D4: an unknown operation, a second file, and value operations that multiply rows or read the order."""
    t, u = _live_tables(tmp_path)
    verdict = _classify(_WORTHY_SHAPES[label](t, u))
    assert verdict.worthy is True, f"{label} must be worthy: {verdict}"


@pytest.mark.parametrize("label", list(_CHEAP_SHAPES))
def test_the_cheap_test_classifies_these_shapes_as_cheap(tmp_path, label):
    """ADR-008 D4: a file read, filter, selection, computed column, rename, cast, drop, drop_null, fill_null."""
    t, u = _live_tables(tmp_path)
    verdict = _classify(_CHEAP_SHAPES[label](t, u))
    assert verdict.worthy is False, f"{label} must be cheap: {verdict}"


def test_the_verdict_is_read_from_the_manifest_with_no_expr_yaml_parsed(project, orders_parquet, monkeypatch):
    """ADR-008 D4: computed once at build and recorded; ``classify_build`` and its regex over expr.yaml are retired."""
    cheap = build_and_persist(project, _orders(project, "t.filter(t.qty > 1).mutate(double=t.price * 2)")).content_hash
    worthy = build_and_persist(
        project, _orders(project, "t.group_by('region').aggregate(total=t.price.sum())")
    ).content_hash
    assert read_manifest(entry_dir(project, cheap)).cache_worthy is False
    assert read_manifest(entry_dir(project, worthy)).cache_worthy is True

    real_read_text = Path.read_text

    def guarded(self, *args, **kwargs):
        if self.suffix in {".yaml", ".yml"}:
            raise AssertionError(f"the worthiness verdict parsed {self}")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded)
    assert cache_worthy(project, cheap) is False
    assert cache_worthy(project, worthy) is True


def test_a_value_level_unnest_makes_the_entry_worthy(project):
    """ADR-008 D4: an ``unnest`` inside a select multiplies rows, which a list of relation operations cannot see."""
    _write(project, "lists.parquet", {"k": [1, 2, 3], "tags": [["x", "y"], ["z"], ["x", "y", "z"]]})
    res = build_and_persist(project, _over(project, "lists.parquet", "t.select('k', tag=t.tags.unnest())"))
    assert res.cache_worthy is True
    assert read_manifest(entry_dir(project, res.content_hash)).cache_worthy is True


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("t.union(t)", id="union"),
        pytest.param("t.select('region', 'category').distinct()", id="distinct"),
    ],
)
def test_a_union_and_a_distinct_are_materialized(project, orders_parquet, body):
    """ADR-008 D4: neither can carry one parent's row order, and today's deny-list classes both as cheap."""
    res = build_and_persist(project, _orders(project, body))
    assert res.cache_worthy is True
    assert _names(res)[-1] == ROW_ORDER


# --------------------------------------------------------------------------- #
# ADR-008 D6: the reserved name
# --------------------------------------------------------------------------- #
def test_the_primary_key_search_never_returns_row_order(project, orders_parquet):
    """ADR-008 D6: ``__row_order`` is unique in every table, so it would win the search for any table without a key."""
    h = build_and_persist(project, _orders(project, "t.select('region', 'category').order_by('region')")).content_hash
    assert ROW_ORDER in cached_result_expr(project, h).columns, "the entry must carry __row_order for this test"

    key = resolve_primary_key(project, h)
    assert ROW_ORDER not in key
    assert key == [], "region and category alone are not unique, and __row_order does not count"


def test_a_diff_carries_no_row_order_column_from_either_side(project, orders_parquet):
    """ADR-008 D6: ``build_compare_expr`` and ``build_diff_expr`` drop it from both sides before joining."""
    a = _create(project, "orders", _orders(project, "t"))
    b = build_and_persist(project, _chained("orders", "t.filter(t.qty > 1)")).content_hash
    a_expr, b_expr = cached_result_expr(project, a), cached_result_expr(project, b)
    assert ROW_ORDER in a_expr.columns and ROW_ORDER in b_expr.columns, "both inputs must carry __row_order"

    compared, _overrides = build_compare_expr(a_expr, b_expr, ["order_id"])
    assert not [c for c in compared.columns if c.startswith(ROW_ORDER)], list(compared.columns)
    promoted = build_diff_expr(a, b, ["order_id"])
    assert not [c for c in promoted.columns if c.startswith(ROW_ORDER)], list(promoted.columns)


# --------------------------------------------------------------------------- #
# ADR-008 D6: joins
# --------------------------------------------------------------------------- #
def _keyed_sources(project: str) -> None:
    for name in ("a", "b", "c"):
        _write(project, f"{name}.parquet", {"k": list(range(5)), name: [f"{name}{i}" for i in range(5)]})


def _three_way(project: str, right_side: str = "{}") -> str:
    """Three sources joined in one recipe; *right_side* is a template for how the right-hand inputs are written."""
    return (
        f"{_PRELUDE}"
        f"a = read_project_file('a.parquet', project={project!r})\n"
        f"b = read_project_file('b.parquet', project={project!r})\n"
        f"c = read_project_file('c.parquet', project={project!r})\n"
        f"expr = a.join({right_side.format('b')}, 'k').join({right_side.format('c')}, 'k')\n"
    )


def test_a_join_entrys_file_has_no_right_hand_row_order(project):
    """ADR-008 D6: ibis renames the right side's copy to ``__row_order_right``; the writer drops it."""
    _keyed_sources(project)
    code = (
        f"{_PRELUDE}a = read_project_file('a.parquet', project={project!r})\n"
        f"b = read_project_file('b.parquet', project={project!r})\nexpr = a.join(b, 'k')\n"
    )
    res = build_and_persist(project, code)
    assert _names(res)[-1] == ROW_ORDER, _names(res)

    names = pq.read_table(_snapshot_path(project, res.content_hash)).column_names
    assert ROW_ORDER_RIGHT not in names
    assert names[-1] == ROW_ORDER
    assert sorted(names) == sorted(["k", "a", "b", ROW_ORDER])


def test_a_join_entry_can_be_joined_to_a_third_entry(project):
    """ADR-008 D6: with the right-hand copy dropped from its file, the join entry joins to a third entry."""
    _keyed_sources(project)
    ab = (
        f"{_PRELUDE}a = read_project_file('a.parquet', project={project!r})\n"
        f"b = read_project_file('b.parquet', project={project!r})\nexpr = a.join(b, 'k')\n"
    )
    _create(project, "ab", ab)
    third = (
        f"{_PRELUDE}ab = tracked_expr_from_alias('ab')\n"
        f"c = read_project_file('c.parquet', project={project!r})\nexpr = ab.join(c, 'k')\n"
    )
    res = build_and_persist(project, third)

    names = pq.read_table(_snapshot_path(project, res.content_hash)).column_names
    assert ROW_ORDER_RIGHT not in names
    assert names[-1] == ROW_ORDER
    assert sorted(names) == sorted(["k", "a", "b", "c", ROW_ORDER])


def test_a_three_way_join_in_one_recipe_tells_the_author_to_drop_the_right_hand_copies(project):
    """ADR-008 D6: ibis fails on ``__row_order_right``, a name the author never wrote, so the build says what to do."""
    _keyed_sources(project)
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, _three_way(project))
    msg = str(exc.value)
    assert re.search(r"""drop\(\s*["']__row_order["']\s*\)""", msg), f"the message must say to drop it: {msg}"
    assert "right" in msg.lower(), msg


def test_a_three_way_join_builds_when_the_right_hand_inputs_drop_row_order(project):
    """ADR-008 D6: the instruction the error gives works."""
    _keyed_sources(project)
    res = build_and_persist(project, _three_way(project, "{}.drop('__row_order')"))
    names = pq.read_table(_snapshot_path(project, res.content_hash)).column_names
    assert ROW_ORDER_RIGHT not in names
    assert names[-1] == ROW_ORDER
    assert sorted(names) == sorted(["k", "a", "b", "c", ROW_ORDER])


# --------------------------------------------------------------------------- #
# ADR-008 D7: CSV roots
# --------------------------------------------------------------------------- #
def test_a_csv_root_is_a_cheap_read_with_exactly_one_row_order_column(project):
    """ADR-008 D7: ``tallyman_read_csv`` loses its trailing ``order_by`` and its column becomes ``__row_order``."""
    csv = data_dir(project) / "root.csv"
    csv.write_text("k,v\n3,30\n1,10\n2,20\n")
    res = build_and_persist(project, _csv_recipe(csv))

    yaml_text = (entry_build_dir(project, res.content_hash) / "expr.yaml").read_text()
    assert not re.search(r"op:\s*Sort\b", yaml_text), "a CSV root is a plain read of its ordered copy: no Sort"
    assert cache_worthy(project, res.content_hash) is False
    assert read_manifest(entry_dir(project, res.content_hash)).cache_worthy is False

    names = _names(res)
    assert names == ["k", "v", ROW_ORDER], names
    assert "original_row_order" not in names


# --------------------------------------------------------------------------- #
# ADR-008 D12: a raw parquet read is a build error
# --------------------------------------------------------------------------- #
def test_a_raw_parquet_read_of_a_source_file_is_a_build_error(project, orders_parquet):
    """ADR-008 D12: such a read has no digest, no clone and no ordered copy, so no ``__row_order``."""
    code = f"import xorq.api as xo\nexpr = xo.deferred_read_parquet({str(orders_parquet)!r})\n"
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, code)
    assert "read_project_file" in str(exc.value)


def test_a_raw_parquet_read_of_a_file_outside_the_project_is_a_build_error(project, tmp_path):
    """ADR-008 D12: only tallyman's own files (snapshots and ordered copies under compute_cache) may be read raw."""
    outside = tmp_path / "elsewhere.parquet"
    pq.write_table(pa.table({"k": [1, 2, 3]}), outside)
    code = f"import xorq.api as xo\nt = xo.deferred_read_parquet({str(outside)!r})\nexpr = t.filter(t.k > 1)\n"
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, code)
    assert "read_project_file" in str(exc.value)
