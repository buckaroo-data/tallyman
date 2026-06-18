"""Side-channel for recording cross-entry parent edges during a build.

Mirrors ``source_identity``'s source-digest collector. Since #73/#74,
``from_catalog`` composes the parent's *expression* into the child instead of
reading the parent's ``result.parquet``, so the child's ``expr.yaml`` carries no
path naming the parent entry. The edge isn't lost, only implicit:
``from_catalog`` resolves the parent's content hash at build time. This collector
captures that resolution so ``build_and_persist`` persists it into
``manifest.parents`` — durable provenance kept for a future lineage feature
(#84; the inter-entry DAG view that read it was removed pending a proper rebuild).

Each entry records ``{hash, ref, follow}``:

- ``hash`` — the resolved build-time parent content hash (the DAG edge).
- ``ref`` — the original ``from_catalog`` argument (an alias name or a literal
  hash), preserved as read-intent.
- ``follow`` — ``True`` when the argument was an alias (the child should follow
  the alias head and go stale as it advances), ``False`` when it was a literal
  hash (the child pins that exact revision).

Only *direct* parents are collected. ``from_catalog`` reconstructs a parent's
recipe to compose it, which re-runs the parent's own ``from_catalog`` calls; the
caller gates those transitive resolutions out so a grandparent is not recorded
as a direct parent (see ``tallyman_xorq.io.from_catalog``).
"""

from __future__ import annotations

import contextvars

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
