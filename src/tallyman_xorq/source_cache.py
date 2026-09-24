"""The rewrite step between a recipe and its build (ADR-007 D1, ADR-008).

Before a build, tallyman looks at the submitted expression, rejects what cannot become a sound entry, and adds the one
thing a materialized entry needs. No xorq cache node is created or kept: tallyman writes its own result files
(``tallyman_xorq.materialize``), so nothing in a build points at xorq's cache, and xorq stays the expression, build,
load, hashing and execution layer.

The rewrite rejects:

- **In-memory reads** (``read_in_memory`` or an ``ibis.memtable``). They signal that the author dropped to pandas
  instead of importing the file and reading the source alias; the bytes would not round-trip through the portable
  build.
- **A recipe that already contains a cache node.** A node with default storage would write under ``~/.cache/xorq``.
- **A recipe that assigns to ``__row_order``**, and **a cheap entry that drops it** (ADR-008 D3, D6).

And it adds:

- for a **worthy** entry, the canonical sort (ADR-006 D5, ADR-008 D10 and D11): a deterministic total order, so the
  snapshot is the same on every rebuild and the writer numbers the rows in the order the author asked for;
- for a **cheap** entry, nothing but moving ``__row_order`` to the last column.

Whether an entry is cheap or worthy is decided once, by ``worthiness.classify_expr`` on the expression the author
wrote, and recorded in the manifest. This module never re-derives it from the serialized build.
"""

from __future__ import annotations

from tallyman_xorq.row_order import (
    canonical_sorted as _canonical_sorted,  # noqa: F401 (the evidence scripts import it here)
)


class InMemoryReadError(RuntimeError):
    """The expression reads in-memory data instead of a deferred file read."""


class CacheNodeError(RuntimeError):
    """The recipe contains a xorq cache node."""


_IN_MEMORY_MSG = (
    "expression reads in-memory data (read_in_memory / ibis.memtable). This "
    "usually means the source was loaded into pandas (e.g. pd.read_csv) and "
    "handed to xorq in memory, instead of entering the catalog as data. Import "
    "the file once, catalog_import_source('<abs path>', '<alias>', schema=...), "
    "and read it with tracked_expr_from_alias('<alias>'), so the rows are held by "
    "tallyman, round-trip through the portable build, and carry a stable __row_order."
)

_CACHE_NODE_MSG = (
    "the recipe calls .cache(), which puts a xorq cache node in the build. Tallyman writes its own result files "
    "(entries that do expensive work are materialized when they are created), and a cache node with default storage "
    "would write under ~/.cache/xorq. Remove the .cache() call."
)


def rewrite_for_build(expr, project: str, *, verdict=None, reading: str | None = None):
    """Rewrite the submitted expression before build.

    In order:

    1. **Reject in-memory reads** — raise :class:`InMemoryReadError`.
    2. **Reject cache nodes** — raise :class:`CacheNodeError` (ADR-007 D1).
    3. **Reject assignment to ``__row_order``**, and, for a cheap entry, a select that drops it — raise
       :class:`tallyman_xorq.row_order.RowOrderError` (ADR-008 D3, D6). ``reading`` names what the entry reads, for
       the message.
    4. A worthy entry gets the **canonical sort**; a cheap entry gets ``__row_order`` moved last.

    ``verdict`` is the entry's ``worthiness.Verdict`` (computed here when the caller has not already).
    """
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.expr.relations import CachedNode, Read

    from tallyman_xorq import row_order
    from tallyman_xorq.worthiness import classify_expr

    reads = walk_nodes(Read, expr)
    if walk_nodes(ops.InMemoryTable, expr) or any(r.method_name == "read_in_memory" for r in reads):
        raise InMemoryReadError(_IN_MEMORY_MSG)
    if walk_nodes(CachedNode, expr):
        raise CacheNodeError(_CACHE_NODE_MSG)

    verdict = verdict or classify_expr(expr)
    row_order.assert_not_assigned(expr)
    row_order.assert_joinable(expr)
    if verdict.worthy:
        try:
            return row_order.canonical_sorted(expr)
        except row_order.RowOrderError:
            raise
        except Exception as exc:
            translated = row_order.translate_collision(exc)
            if translated is not None:
                raise translated from exc
            raise
    row_order.require_on_cheap(expr, reading=reading or "a file")
    return row_order.move_last(expr)
