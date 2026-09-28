"""Cheap or worthy: the one place that decides (ADR-008 D4).

A **cheap** entry has no file of its own: its small plan re-runs on every read, and it pages by the ``__row_order``
of the one file it reads. So cheap has to mean something strong, that the plan is row-preserving over exactly one file
and that ``__row_order`` is still present and unique at the top of it. Everything else is **worthy**, which means it
is materialized: a snapshot is written when the entry is created and pages read that file.

The test is an allow-list, so an operation nobody has thought about yet costs a copy (safe) instead of unstable
paging (unsafe). It has three parts, all decided on the live expression by the class of each node:

- every relation operation is on ``ROW_PRESERVING_RELATIONS``;
- the plan reads exactly one file;
- no value operation multiplies rows (``Unnest``), depends on the order rows arrive in (``WindowFunction``, which also
  covers ``row_number`` and ``lag``) or is not pure (``Impure``: ``random()`` and ``uuid()``; ``now()`` and
  ``today()``, which xorq's ibis classes as constants, by name; and any UDF, matched by its base class).

The verdict is computed once, when the entry is built, and recorded in the manifest. Nothing reads ``expr.yaml`` to
work it out again: a regex over that file cannot hold an allow-list, since ``op:`` in it also matches types and
literals that are not operations.
"""

from __future__ import annotations

from typing import NamedTuple


class Verdict(NamedTuple):
    worthy: bool
    why: str


# Value operations that break the row-order contract of a cheap entry, by base class.
_NEVER_CHEAP_VALUES = ("Unnest", "WindowFunction", "Impure")

# xorq's ibis classes these two as constants, not as impure, so they are matched by name.
_NEVER_CHEAP_BY_NAME = frozenset({"TimestampNow", "DateNow"})


def _cheap_relation_types():
    """The relation operations that keep each output row tied to exactly one input row."""
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.expr.relations import Read

    return (Read, ops.Filter, ops.Project, ops.DropColumns, ops.DropNull, ops.FillNull)


def classify_expr(expr) -> Verdict:
    """Cheap or worthy, from the live author expression, with the reason in a short string.

    ``why`` names what made the entry worthy (``ops:Aggregate,Join``, ``reads:2``, ``values:Unnest``,
    ``udf:plusone``), or says it is cheap. It is recorded in the manifest as ``cache_worthy_why``.
    """
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.expr.relations import Read
    from xorq.vendor.ibis.expr.operations.core import Node

    cheap_relations = _cheap_relation_types()
    never_cheap_values = tuple(getattr(ops, name) for name in _NEVER_CHEAP_VALUES)

    nodes = list(walk_nodes((Node,), expr))
    relations = [n for n in nodes if isinstance(n, ops.Relation)]
    bits: list[str] = []

    other = sorted({type(n).__name__ for n in relations if not isinstance(n, cheap_relations)})
    if other:
        bits.append("ops:" + ",".join(other))
    reads = {n for n in relations if isinstance(n, Read)}
    if len(reads) != 1:
        bits.append(f"reads:{len(reads)}")
    values = sorted(
        {
            type(n).__name__
            for n in nodes
            if isinstance(n, never_cheap_values) or type(n).__name__ in _NEVER_CHEAP_BY_NAME
        }
    )
    if values:
        bits.append("values:" + ",".join(values))
    udfs = sorted({type(n).__name__ for n in nodes if any("UDF" in base.__name__ for base in type(n).__mro__)})
    if udfs:
        bits.append("udf:" + ",".join(udfs))

    if bits:
        return Verdict(True, "; ".join(bits))
    return Verdict(False, "cheap (row-preserving over one file)")
