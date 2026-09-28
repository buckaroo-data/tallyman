"""``__row_order``: the natural order of a file, and the sorts that keep every query deterministic (ADR-008).

Every file tallyman writes ends in an int64 column named ``__row_order`` holding ``0..N-1`` in the file's physical row
order. A page of any entry is ``ORDER BY __row_order`` (or the user's keys, then ``__row_order``), so it is the same
page in any process and any cache state. This module holds the rules that keep that true while recipes are written by
someone else:

- a cheap entry has no file of its own, so it inherits the column and must keep it (D3);
- a recipe may read the column and copy it under another name, but not assign to it (D6);
- every sort in a recipe gets the natural order, then the remaining columns, as its last keys, so the sort is total
  wherever the recipe put it (D10), and a sort that is not the last step decides the order that is written (D11).
"""

from __future__ import annotations

ROW_ORDER = "__row_order"
# ibis's name for the right side's copy of a column both sides of a join carry.
ROW_ORDER_RIGHT = "__row_order_right"
# The two names that are tallyman's and not the author's (ADR-008 D6).
RESERVED = (ROW_ORDER, ROW_ORDER_RIGHT)


class RowOrderError(RuntimeError):
    """A recipe breaks the row-order contract. ``build_and_persist`` reports it as a ``BuildError``."""


def _sortable(dtype) -> bool:
    """Whether a column can serve as a sort key.

    Nested / geospatial types can't be sort keys in datafusion; leaving them out of the tie-breakers is safe, since
    any remaining ties are between rows identical on every sortable column, and identical rows write identical bytes.
    """
    for pred in ("is_array", "is_map", "is_struct", "is_json", "is_geospatial"):
        if getattr(dtype, pred, None) and getattr(dtype, pred)():
            return False
    return True


def _constant_names(rel) -> set[str]:
    """Columns the relation defines as scalars (projected literals/constants).

    A constant column adds no ordering information (every row ties on it), and ibis dereferences a sort key straight
    through to the defining value, so ``ORDER BY <literal>`` reaches datafusion's planner, which rejects a bare
    literal there (it reads a numeric literal as a column ordinal). Skipping them is both necessary and free.
    """
    values = getattr(rel, "values", None)
    if not values:
        return set()
    out = set()
    for name, v in values.items():
        shape = getattr(v, "shape", None)
        if shape is not None and shape.is_scalar():
            out.add(name)
    return out


def tie_break_order(names, keyed: set[str], schema, rel) -> list[str]:
    """The columns to append to a sort: ``__row_order`` first, then every other sortable, non-constant column."""
    skip = keyed | _constant_names(rel)
    remaining = [n for n in names if n not in skip and _sortable(schema[n])]
    remaining.sort(key=lambda n: n != ROW_ORDER)  # stable: the natural order first, schema order after
    return remaining


def _keyed_names(keys) -> set[str]:
    import xorq.vendor.ibis.expr.operations as ops

    return {k.expr.name for k in keys if isinstance(k.expr, ops.Field)}


def _extend_every_sort(expr):
    """Append the tie-break to every ``Sort`` in the graph (ADR-008 D10)."""
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.common.utils.graph_utils import replace_nodes, walk_nodes

    if not walk_nodes(ops.Sort, expr):
        return expr

    def replacer(node, kwargs):
        node = node.__recreate__(kwargs) if kwargs else node
        if not isinstance(node, ops.Sort):
            return node
        parent = node.parent
        remaining = tie_break_order(parent.schema.names, _keyed_names(node.keys), parent.schema, parent)
        if not remaining:
            return node
        extra = tuple(ops.SortKey(ops.Field(parent, n)) for n in remaining)
        return ops.Sort(parent, tuple(node.keys) + extra)

    return replace_nodes(replacer, expr).to_expr()


def _map_key_through(step, name: str) -> str:
    """Follow one sort key (a column name) up through one order-keeping step, or say why it did not survive."""
    import xorq.vendor.ibis.expr.operations as ops

    if isinstance(step, ops.Project):
        outputs = [
            out
            for out, v in step.values.items()
            if isinstance(v, ops.Field) and v.name == name and v.rel == step.parent
        ]
        if name in outputs:
            return name
        if outputs:
            return outputs[0]  # a rename is followed
        raise RowOrderError(
            f"the sort key {name!r} was "
            + ("overwritten" if name in step.values else "dropped")
            + " by a later step"
        )
    if isinstance(step, ops.DropColumns):
        if name in step.columns_to_drop:
            raise RowOrderError(f"the sort key {name!r} was dropped by a later step")
        return name
    if isinstance(step, ops.FillNull):
        replacements = step.replacements
        if not isinstance(replacements, dict) or name in replacements:
            raise RowOrderError(f"the sort key {name!r} was overwritten by a later fill_null")
        return name
    return name  # Filter, Limit, DropNull: the columns are the parent's


def _hoisted_keys(expr):
    """The keys the author wrote in the nearest sort reachable from the top through order-keeping steps, as
    ``(output column, ascending, nulls_first)`` for the top-level sort.

    None when no sort is reachable (an aggregate or a join sits in the way, and the order of rows is gone there).
    Raises ``RowOrderError`` when a key did not survive as an unchanged output column (ADR-008 D11). Only the keys
    the author wrote count: the tie-break appended to a sort (``__row_order`` and the other columns) is added again at
    the top, and a later select is free to drop those columns.
    """
    import xorq.vendor.ibis.expr.operations as ops

    top = expr.op()
    chain = []
    node = top
    while not isinstance(node, ops.Sort):
        if not isinstance(node, (ops.Filter, ops.Limit, ops.DropNull, ops.Project, ops.DropColumns, ops.FillNull)):
            return None
        chain.append(node)
        node = node.parent
    hoisted = []
    for key in node.keys:
        if not isinstance(key.expr, ops.Field):
            raise RowOrderError(
                "an order_by that is not the last step must sort by plain columns so its order can be kept, and "
                f"this one sorts by an expression ({key.expr.name}). Sort as the last step, or add the expression as "
                "a column first"
            )
        name = key.expr.name
        try:
            for step in reversed(chain):
                name = _map_key_through(step, name)
        except RowOrderError as exc:
            raise RowOrderError(
                f"{exc}, so the order you asked for cannot be kept. Keep the column in every later select, or "
                "sort as the last step of the recipe"
            ) from exc
        hoisted.append((name, key.ascending, key.nulls_first))
    return hoisted


def canonical_sorted(expr):
    """Impose a deterministic total order on a worthy entry before it is materialized.

    Key priority: the author's own sort keys stay primary (the served row order is part of what they asked for), then
    ``__row_order``, then the remaining sortable columns in schema order. Every ``Sort`` in the recipe is extended
    that way in place (D10). When the recipe's last step is not a sort, the nearest sort below it that the order of
    rows survives to is hoisted, so the top-level sort leads with its keys (D11); with none, the top-level sort is the
    tie-break alone.
    """
    import xorq.vendor.ibis.expr.operations as ops

    lead = _hoisted_keys(expr) if not isinstance(expr.op(), ops.Sort) else None
    expr = _extend_every_sort(expr)
    node = expr.op()
    if isinstance(node, ops.Sort):
        return expr
    lead_keys = [ops.SortKey(ops.Field(node, name), asc, nulls) for name, asc, nulls in (lead or [])]
    schema = expr.schema()
    names = tie_break_order(schema.names, {name for name, _, _ in (lead or [])}, schema, node)
    keys = tuple(lead_keys) + tuple(ops.SortKey(ops.Field(node, n)) for n in names)
    if not keys:
        return expr  # nothing sortable: the digest stays best-effort for this entry
    return ops.Sort(node, keys).to_expr()


def assert_not_assigned(expr) -> None:
    """A recipe may read ``__row_order`` and copy it under another name, but not assign to it (ADR-008 D6).

    Arbitrary values could hold ties or gaps, and the contract depends on ``0..N-1`` with neither. Passing the column
    through unchanged is not an assignment.
    """
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.vendor.ibis.expr.operations.core import Node

    def _is_passthrough(value) -> bool:
        return isinstance(value, ops.Field) and value.name == ROW_ORDER

    for node in walk_nodes((Node,), expr):
        assigned = False
        if isinstance(node, ops.Project):
            value = node.values.get(ROW_ORDER)
            assigned = value is not None and not _is_passthrough(value)
        elif isinstance(node, ops.Aggregate):
            value = node.groups.get(ROW_ORDER)
            assigned = (value is not None and not _is_passthrough(value)) or ROW_ORDER in node.metrics
        if assigned:
            raise RowOrderError(
                f"the recipe assigns to {ROW_ORDER!r}, which is reserved: it holds each row's position in the file "
                "(0..N-1) and paging depends on that. To change the order of rows, sort them (order_by) and the "
                f"column is renumbered to match. To keep a copy, give it another name, e.g. "
                f"t.mutate({ROW_ORDER}_v1=t[{ROW_ORDER!r}])"
            )


def require_on_cheap(expr, *, reading: str) -> None:
    """A cheap entry has no file of its own, so it must carry ``__row_order`` from what it reads (ADR-008 D3)."""
    columns = list(expr.columns)
    if ROW_ORDER in columns:
        return
    fix = ", ".join(repr(c) for c in [*columns, ROW_ORDER])
    raise RowOrderError(
        f"this entry reads {reading} and keeps its rows, so it must keep the {ROW_ORDER!r} column: pages of the "
        f"entry are ordered by it, which is what keeps paging repeatable. Add it to the select, e.g. "
        f"t.select({fix})"
    )


def move_last(expr):
    """Put ``__row_order`` in the last position, at the top of the expression only.

    A computed column added after it would otherwise push it into the middle of the table.
    """
    columns = list(expr.columns)
    if columns[-1] == ROW_ORDER:
        return expr
    return expr.select(*[c for c in columns if c != ROW_ORDER], ROW_ORDER)


def without_row_order(expr):
    """*expr* without ``__row_order`` or ibis's join copy of it, for comparing one entry with another (ADR-008 D6).

    A row's position in its entry's file is not data, and a diff is keyed: one row inserted near the front of a file
    moves every later row's position, so comparing positions shows every later row as changed (#200). A copy under a
    name the author chose, such as ``__row_order_v1``, is ordinary data and stays.
    """
    reserved = [c for c in RESERVED if c in expr.columns]
    return expr.drop(*reserved) if reserved else expr


def assert_joinable(expr) -> None:
    """A join chain whose first side and two or more right-hand sides all carry ``__row_order`` collides (ADR-008 D6).

    Every entry carries the column, so a join of two entries leaves the right side's copy behind under ibis's
    collision name, and the second join in the same chain needs that name too. Whether ibis then fails while building
    the expression, while compiling it, or not at all depends on what is stacked on top, so the check is explicit and
    the author is told what to write.
    """
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.common.utils.graph_utils import walk_nodes

    for chain in walk_nodes(ops.JoinChain, expr):
        with_column = [link for link in chain.rest if ROW_ORDER in link.table.schema.names]
        if ROW_ORDER in chain.first.schema.names and len(with_column) >= 2:
            raise _collision_error()


def _collision_error() -> RowOrderError:
    return RowOrderError(
        f"joining more than two entries in one recipe: every entry carries {ROW_ORDER!r}, so the second join collides "
        f"on {ROW_ORDER_RIGHT!r}. Drop the column from the right-hand inputs, e.g. "
        f"a.join(b.drop({ROW_ORDER!r}), key).join(c.drop({ROW_ORDER!r}), key)"
    )


def translate_collision(exc: Exception) -> RowOrderError | None:
    """The instruction for ibis's raw name-collision error on ``__row_order_right``, or None for any other error.

    A join of two entries leaves the right side's copy behind under ibis's collision name, and a second join in the
    same recipe collides with it. The author never wrote that name, so the raw message would mean nothing to them.
    """
    if ROW_ORDER_RIGHT not in str(exc):
        return None
    return _collision_error()


def page(expr, *, offset: int, limit: int, sort=()):
    """One page of an entry: ``ORDER BY <the user's keys>, __row_order`` then ``LIMIT/OFFSET`` (ADR-008 D5).

    With no user sort the page is ``ORDER BY __row_order``. With one, the user's keys come first and ``__row_order``
    is the last key, which breaks every tie, so the same request returns the same rows in any process and any cache
    state. An expression that does not carry the column (a diff, which drops it) is paged as given.
    """
    keys = [*sort, ROW_ORDER] if ROW_ORDER in expr.columns else list(sort)
    if keys:
        expr = expr.order_by(keys)
    return expr.limit(limit, offset=offset)
