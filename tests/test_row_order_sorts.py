"""ADR-008 D10 and D11 (plans/ADR-008-row-order-of-reads.md): where a unique sort has to be imposed.

Paddy's rule: when a supplied sort is not deterministic, the natural order (``__row_order``) is imposed into each
``order_by``. Today ``_canonical_sorted`` extends an author's sort only when it is the top node of the expression, and
wraps everything else in a sort that leads with the inherited row order, which is unique, so an author's sort that
is followed by another step has no effect on what is written.

- ADR-008 D10: every ``Sort`` node gets ``__row_order`` and then the remaining sortable columns as its last keys, so a
  sort that feeds a ``limit`` decides the same rows on any connection.
- ADR-008 D11: a sort that is not the recipe's last step is hoisted: the top-level sort leads with its keys. A key that
  did not survive (dropped, overwritten, or an expression) is a build error that names it.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tallyman_core import data_dir
from tallyman_xorq.build import BuildError, build_and_persist
from tallyman_xorq.result_cache import cached_result_expr
from tallyman_xorq.source_import import update_and_depend
from tests.big_parquet import write_big_parquet

ROW_ORDER = "__row_order"
AMOUNTS_SRC = "amounts_src"
BIG_SRC = "big_src"

_PRELUDE = "import xorq.vendor.ibis as ibis\nfrom tallyman_xorq.io import tracked_expr_from_alias\n"

# The parent's rows are in this order, so an entry that ignores the author's sort is written 40, 10, 60, ...
AMOUNTS = [40, 10, 60, 20, 50, 30]


@pytest.fixture
def amounts(project: str) -> str:
    """Six rows out of amount order, imported as a source alias (ADR-011 D1); the alias name a recipe reads."""
    path = data_dir(project) / "amounts.parquet"
    pq.write_table(pa.table({"name": list("abcdef"), "amount": AMOUNTS}), path)
    update_and_depend(path, AMOUNTS_SRC, project=project)
    return AMOUNTS_SRC


@pytest.fixture(scope="module")
def big_source(tmp_path_factory) -> Path:
    return write_big_parquet(tmp_path_factory.mktemp("big_source") / "big.parquet")


def _sorted_then(project: str, body: str, source: str = AMOUNTS_SRC) -> str:
    """A recipe whose ``by`` is the source ordered by amount, descending, followed by *body*."""
    return (
        f"{_PRELUDE}t = tracked_expr_from_alias({source!r}, project={project!r})\n"
        "by = t.order_by(t.amount.desc())\n"
        f"expr = {body}\n"
    )


def _sort_key_names(project: str, content_hash: str) -> list[list[str]]:
    """The column names in the keys of every ``Sort`` node of the entry's frozen build."""
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.common.utils.graph_utils import walk_nodes

    from tallyman_xorq.result_cache import load_entry_expr

    loaded = load_entry_expr(project, content_hash)
    return [[k.expr.name for k in sort.keys if isinstance(k.expr, ops.Field)] for sort in walk_nodes(ops.Sort, loaded)]


# --------------------------------------------------------------------------- #
# ADR-008 D10: the natural order is imposed on every order_by
# --------------------------------------------------------------------------- #
def test_a_sort_that_feeds_a_limit_keeps_the_rows_the_natural_order_picks(project, big_source):
    """ADR-008 D10: ``order_by(g).limit(1000)`` holds exactly the rows that ``(g, __row_order)`` picks.

    About 7,500 rows tie on the smallest ``g`` and the limit cuts through them, so which 1,000 the entry holds is
    decided by the tie-break of the sort that feeds the limit, not by the sort above it. ``id`` is the file position,
    so ``(g, id)`` is ``(g, __row_order)``.
    """
    update_and_depend(big_source, BIG_SRC, project=project)
    code = (
        f"{_PRELUDE}t = tracked_expr_from_alias({BIG_SRC!r}, project={project!r})\n"
        "expr = t.order_by('g').limit(1000)\n"
    )
    res = build_and_persist(project, code)

    frame = pq.read_table(big_source).to_pandas()
    expected = set(frame.sort_values(["g", "id"]).head(1000)["id"])
    got = set(cached_result_expr(project, res.content_hash).execute()["id"])
    assert len(got) == 1000
    assert got == expected, f"{len(got ^ expected)} rows differ from the 1,000 that (g, __row_order) picks"


def test_every_sort_in_the_graph_carries_the_row_order_tie_break(project, amounts):
    """ADR-008 D10: the keys the author wrote, then ``__row_order``, at every ``Sort`` node and not only the top one."""
    res = build_and_persist(project, _sorted_then(project, "by.limit(3)"))

    sorts = _sort_key_names(project, res.content_hash)
    assert sorts, "the entry's build must contain a Sort"
    for keys in sorts:
        assert ROW_ORDER in keys, f"a Sort without the natural-order tie-break: keys {keys}"
        assert keys.index("amount") < keys.index(ROW_ORDER), f"the author's key comes before the tie-break: {keys}"


# --------------------------------------------------------------------------- #
# ADR-008 D11: a sort that is not the last step is hoisted, or the build fails
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("body", "column", "expected"),
    [
        pytest.param("by.limit(3)", "amount", [60, 50, 40], id="a top-three entry is written in rank order"),
        pytest.param("by.mutate(double=by.amount * 2)", "amount", [60, 50, 40, 30, 20, 10], id="order_by then mutate"),
        pytest.param("by.filter(by.amount > 15)", "amount", [60, 50, 40, 30, 20], id="order_by then filter"),
        pytest.param("by.select(by.name, by.amount)", "amount", [60, 50, 40, 30, 20, 10], id="order_by then select"),
        pytest.param("by.select(by.name, cost=by.amount)", "cost", [60, 50, 40, 30, 20, 10], id="a rename is followed"),
    ],
)
def test_a_sort_that_is_not_the_last_step_decides_the_written_order(project, amounts, body, column, expected):
    """ADR-008 D11: the author asked for amount descending, and it is kept; the file is numbered in that order."""
    res = build_and_persist(project, _sorted_then(project, body))

    df = cached_result_expr(project, res.content_hash).execute()
    assert df[column].tolist() == expected
    assert df[ROW_ORDER].tolist() == list(range(len(expected)))
    assert list(df.columns)[-1] == ROW_ORDER


def _assert_names_the_sort_key(msg: str) -> None:
    """The message names the key and talks about the sort, and comes from the build, not from executing the plan.

    Overwriting the key of a sort already fails today, but only when the plan is executed, with DataFusion's
    "Schema contains qualified field name t0.amount and unqualified field name amount which would be ambiguous". That
    message names ``amount`` too, so naming the key is not enough to tell the two apart.
    """
    assert "amount" in msg, msg
    assert "execution failed" not in msg, f"the build must refuse the recipe before it executes it: {msg[:300]}"
    mentions_a_sort = "sort" in msg.lower() or "order_by" in msg.lower()
    assert mentions_a_sort, f"the message must say that a sort is involved: {msg[:300]}"


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("by.select(by.name)", id="the key is dropped"),
        pytest.param("by.mutate(amount=by.amount * -1)", id="the key is overwritten by a mutate"),
        pytest.param("by.select(by.name, amount=by.amount.cast('float64'))", id="the key is redefined in a select"),
    ],
)
def test_a_sort_key_that_did_not_survive_is_a_build_error_naming_it(project, amounts, body):
    """ADR-008 D11: report what the author can fix in one line: keep the column, or sort as the last step."""
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, _sorted_then(project, body))
    _assert_names_the_sort_key(str(exc.value))


def test_a_sort_key_that_was_an_expression_is_a_build_error_when_a_step_follows(project, amounts):
    """ADR-008 D11: an expression is not an output column, so there is nothing to lead the top-level sort with."""
    code = (
        f"{_PRELUDE}t = tracked_expr_from_alias({AMOUNTS_SRC!r}, project={project!r})\n"
        "by = t.order_by(t.amount + 1)\n"
        "expr = by.mutate(double=by.amount * 2)\n"
    )
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, code)
    _assert_names_the_sort_key(str(exc.value))
