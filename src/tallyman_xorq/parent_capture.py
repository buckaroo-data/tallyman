"""Side-channel for recording cross-entry parent edges during a build.

Mirrors ``source_identity``'s source-digest collector. Since #73/#74,
``tracked_expr_from_alias`` composes the parent's *expression* into the child instead of
reading the parent's ``result.parquet``, so the child's ``expr.yaml`` carries no
path naming the parent entry. The edge isn't lost, only implicit:
``tracked_expr_from_alias`` resolves the parent's content hash at build time. This collector
captures that resolution so ``build_and_persist`` persists it into
``manifest.parents`` — durable provenance kept for a future lineage feature
(#84; the inter-entry DAG view that read it was removed pending a proper rebuild).

Each entry records ``{hash, ref, follow}``:

- ``hash`` — the resolved build-time parent content hash (the DAG edge).
- ``ref`` — the original ``tracked_expr_from_alias`` argument (an alias name or a literal
  hash), preserved as read-intent.
- ``follow`` — ``True`` when the argument was an alias (the child should follow
  the alias head and go stale as it advances), ``False`` when it was a literal
  hash (the child pins that exact revision).

Only *direct* parents are collected. ``tracked_expr_from_alias`` reconstructs a parent's
recipe to compose it, which re-runs the parent's own ``tracked_expr_from_alias`` calls; the
caller gates those transitive resolutions out so a grandparent is not recorded
as a direct parent (see ``tallyman_xorq.io.tracked_expr_from_alias``).

A second collector records the files behind each result tallyman hands the
recipe (``note_reads``, called by ``result_cache.cached_result_expr``), so the
build can tell those reads apart from a ``deferred_read_parquet`` the recipe
wrote itself (#228).
"""

from __future__ import annotations

import contextvars
from pathlib import Path

_collector: contextvars.ContextVar[list[dict] | None] = contextvars.ContextVar(
    "tallyman_parent_collector", default=None
)


def begin_collect() -> contextvars.Token:
    return _collector.set([])


def note_parent(content_hash: str, ref: str, follow: bool) -> None:
    bag = _collector.get()
    if bag is None:
        return
    # De-dup repeated reads of the same (hash, ref, follow) within one build.
    edge = {"hash": content_hash, "ref": ref, "follow": follow}
    if edge not in bag:
        bag.append(edge)


def end_collect(token: contextvars.Token) -> list[dict]:
    bag = _collector.get() or []
    _collector.reset(token)
    return list(bag)


# The files behind every result tallyman handed the recipe while it ran: a worthy parent's snapshot, or the files a
# cheap parent's plan reads (its own parents' snapshots). ``build._raw_parquet_read_check`` allows a
# ``deferred_read_parquet`` of exactly these, so a recipe cannot name another entry's snapshot by its path (#228).
_reads: contextvars.ContextVar[set[Path] | None] = contextvars.ContextVar("tallyman_handed_out_reads", default=None)


def begin_reads() -> contextvars.Token:
    return _reads.set(set())


def note_reads(paths) -> None:
    bag = _reads.get()
    if bag is None:
        return
    bag.update(Path(p) for p in paths)


def end_reads(token: contextvars.Token) -> frozenset[Path]:
    bag = _reads.get() or set()
    _reads.reset(token)
    return frozenset(bag)
