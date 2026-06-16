"""xorq-backed result cache for catalog entries, gated by a structural rubric.

The entry's *expression* (its portable ``xorq_build``) is the source of truth.
Whether we keep a materialised copy of its result depends on how expensive the
computation is — caching only pays off when recompute costs far more than
reading a cached parquet:

  * Expensive entries — the expression contains an Aggregate / Join / Sort /
    window / UDF — are materialised in xorq's ``ParquetSnapshotCache``.  Reads
    hit the cache; a deleted cache file self-heals by recomputing from the
    build.
  * Cheap entries — a source read plus projections / renames / row-wise scalar
    math (the citibike column-reorder/derive case) — materialise nothing (#73).
    Recompute ≈ re-reading a columnar source (pushdown), so a stored copy would
    burn work and storage for ~no savings; a non-parquet source's parse is
    already cached at the read (see ``tallyman_xorq.source_cache``).  Reads of a
    cheap entry recompute the expression directly. ``ensure_result`` still
    materialises a ``result.parquet`` on demand for the few callers that need a
    parquet *path* (downloads, promote-diff, post-processing).

Why ``ParquetSnapshotCache`` over the other xorq caches: parquet storage is
what the DuckDB keyed diff and the Buckaroo viewer stream from and is
inspectable on disk; the *snapshot* strategy keys purely on expression
structure (a catalog entry is an immutable, content-addressed artifact, so its
result must not invalidate just because an upstream file's mtime drifts); and
there's no TTL because entries are permanent history, not expiring scratch.

(Buckaroo summary stats are cached separately, in each entry's
``.buckaroo_stat_cache`` — always on, orthogonal to this decision.)
"""

from __future__ import annotations

import functools
import re
from pathlib import Path

# Ops whose presence makes an expression worth caching: they require a
# shuffle / sort / full materialisation rather than a streaming row-wise pass.
_EXPENSIVE_OPS = {
    "Aggregate",
    "Join",
    "JoinChain",
    "JoinLink",
    "JoinReference",
    "Sort",
    "SortKey",
    "WindowFunction",
    "RowNumber",
}


def classify_build(build_dir: Path) -> dict:
    """Decide whether an entry's result is worth caching, from its build.

    Reads the serialized expression (no execution) and looks for expensive ops
    or UDFs.  Returns ``{"worthy": bool, "why": str}``.

    A non-parquet *source* read (CSV / JSON) is **not** itself worthy (#73):
    the parse is cached at the read by the injected source-cache node (see
    ``tallyman_xorq.source_cache``), so an entry earns a ``result_cache``
    snapshot only when it does expensive work — a shuffle / sort / window / UDF
    / full materialisation — on top of its source.

    Two implementations of one predicate: this reads the serialized build,
    ``source_cache._is_worthy_expr`` walks the live expression to make the same
    bake decision at build time. They share ``_EXPENSIVE_OPS`` and must stay in
    lockstep — change one, change both.
    """
    ops: set[str] = set()
    for y in Path(build_dir).glob("*.yaml"):
        text = y.read_text()
        ops |= set(re.findall(r"op:\s*([A-Za-z_]+)", text))

    expensive = ops & _EXPENSIVE_OPS
    udfs = {o for o in ops if "UDF" in o}

    worthy = bool(expensive or udfs)
    why_bits = []
    if expensive:
        why_bits.append("ops:" + ",".join(sorted(expensive)))
    if udfs:
        why_bits.append("udf:" + ",".join(sorted(udfs)))
    return {
        "worthy": worthy,
        "why": "; ".join(why_bits) or "cheap (no Aggregate/Join/Sort/window/UDF)",
    }


def cache_worthy(project: str, content_hash: str) -> bool:
    from tallyman_core.paths import entry_build_dir

    return classify_build(entry_build_dir(project, content_hash))["worthy"]


@functools.lru_cache(maxsize=256)
def cached_result_expr(project: str, content_hash: str):
    """The entry's result as an expression that resolves its own materialisation.

    Bounded so the cache can't grow without limit across a long-lived process;
    entries are content-addressed, so an evicted hash rebuilds an identical
    expression on the next access.

    As of #73 this is simply the loaded build for every entry. An expensive
    entry's build *carries a baked result-cache node* (see
    ``tallyman_xorq.source_cache.rewrite_for_build``), so loading it yields an
    expression that reads its own cached result — a hit reads the cached
    parquet, a miss recomputes and stores. A cheap entry's build has no result
    cache, so it recomputes on read; pushdown makes that ~free and its source
    parse is itself cached at the read. The caller composes ``expr1`` ⋈
    ``expr2`` and never depends on a pre-existing ``result.parquet``.

    salt source-identity mode skips baked caches (path-only snapshot keys would
    collide across salted entries — see ``rewrite_for_build``), so reads route
    through the content-honest in-place ``result.parquet`` instead.
    """
    import xorq.api as xo

    from tallyman_xorq import source_identity as si
    from tallyman_xorq.build import load_entry

    if si.mode() == "salt":
        return xo.deferred_read_parquet(str(ensure_result(project, content_hash)))
    return load_entry(project, content_hash)


def ensure_result(project: str, content_hash: str) -> Path:
    """Return a guaranteed-present ``result.parquet`` path for the entry's result.

    On-demand materialisation for the callers that need a parquet *path* rather
    than an expression — downloads, promote-diff, post-processing. Loads the
    entry and writes its result in place if absent (#73): an expensive entry's
    build carries a baked result cache, so this reads through that cache (a hit
    reuses the materialised result, a miss computes once); a cheap entry
    recomputes. Either way the caller never depends on a pre-existing
    ``result.parquet``, and a deleted one self-heals on the next call.
    """
    from tallyman_core.paths import entry_result_path
    from tallyman_xorq.build import load_entry

    result_path = entry_result_path(project, content_hash)
    if not result_path.exists():
        load_entry(project, content_hash).to_parquet(str(result_path))
    return result_path
