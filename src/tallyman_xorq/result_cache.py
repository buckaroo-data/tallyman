"""xorq-backed result cache for catalog entries, gated by a structural rubric.

The entry's *expression* (its portable ``xorq_build``) is the source of truth.
Whether we keep a materialised copy of its result depends on how expensive the
computation is — caching only pays off when recompute costs far more than
reading a cached parquet:

  * Expensive entries — the expression contains an Aggregate / Join / Sort /
    window / UDF, or reads a non-parquet source (e.g. CSV) — are materialised
    in xorq's ``ParquetSnapshotCache``.  Reads hit the cache; a deleted cache
    file self-heals by recomputing from the build.
  * Cheap entries — a parquet read plus projections / renames / row-wise
    scalar math (the citibike column-reorder/derive case) — get *no* extra
    cache.  Recompute ≈ re-reading the source, so an extra parquet copy would
    burn storage for ~no savings.  Their ``result.parquet`` is regenerated in
    place from the build if missing.

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
# Read methods cheap enough that re-reading the source beats keeping a copy.
_CHEAP_READS = {"read_parquet", "read_in_memory", "read_delta"}


def classify_build(build_dir: Path) -> dict:
    """Decide whether an entry's result is worth caching, from its build.

    Reads the serialized expression (no execution) and looks for expensive ops,
    UDFs, or a non-parquet source.  Returns ``{"worthy": bool, "why": str}``.
    """
    ops: set[str] = set()
    reads: set[str] = set()
    for y in Path(build_dir).glob("*.yaml"):
        text = y.read_text()
        ops |= set(re.findall(r"op:\s*([A-Za-z_]+)", text))
        reads |= set(re.findall(r"method_name:\s*([A-Za-z_]+)", text))

    expensive = ops & _EXPENSIVE_OPS
    udfs = {o for o in ops if "UDF" in o}
    nonparquet = {r for r in reads if r not in _CHEAP_READS}

    worthy = bool(expensive or udfs or nonparquet)
    why_bits = []
    if expensive:
        why_bits.append("ops:" + ",".join(sorted(expensive)))
    if udfs:
        why_bits.append("udf:" + ",".join(sorted(udfs)))
    if nonparquet:
        why_bits.append("source:" + ",".join(sorted(nonparquet)))
    return {
        "worthy": worthy,
        "why": "; ".join(why_bits) or "cheap (parquet read + projection/scalar only)",
    }


def cache_worthy(project: str, content_hash: str) -> bool:
    from tallyman_core.paths import entry_dir

    return classify_build(entry_dir(project, content_hash) / "xorq_build")["worthy"]


@functools.lru_cache(maxsize=None)
def result_cache(project: str):
    """A ParquetSnapshotCache rooted under the project's result_cache dir.

    One connection per project per process — lru_cache ensures xo.connect() is
    called once rather than on every cached_result_expr / ensure_result call.
    """
    import xorq.api as xo

    from tallyman_core.paths import result_cache_dir

    base = result_cache_dir(project)
    base.mkdir(parents=True, exist_ok=True)
    return xo.ParquetSnapshotCache.from_kwargs(source=xo.connect(), base_path=base)


@functools.lru_cache(maxsize=None)
def cached_result_expr(project: str, content_hash: str):
    """The entry's result as an expression that resolves its own materialisation.

    * Expensive entry → its expression wrapped in the snapshot cache: a hit
      reads the cached parquet, a miss recomputes from the build and stores.
    * Cheap entry → a read of its in-place ``result.parquet`` (the
      materialisation xorq already wrote at build time), regenerated from the
      build if it was evicted.  Reading the existing parquet keeps the live
      viewer fast and lazy; recomputing a cheap pipeline twice per diff plus a
      cross-backend transport is far slower and buys nothing.

    Either way the caller composes ``expr1`` ⋈ ``expr2`` and never depends on a
    pre-existing ``result.parquet``.
    """
    import xorq.api as xo

    from tallyman_xorq.build import load_entry

    if cache_worthy(project, content_hash):
        return load_entry(project, content_hash).cache(cache=result_cache(project))
    return xo.deferred_read_parquet(str(ensure_result(project, content_hash)))


def ensure_result(project: str, content_hash: str) -> Path:
    """Return a guaranteed-present parquet path for the entry's result.

    Expensive entries are materialised in the snapshot cache (recomputed on a
    miss).  Cheap entries use their in-place ``result.parquet``, regenerated
    from the build if it was deleted.  Either way the caller never depends on a
    pre-existing ``result.parquet``.
    """
    from tallyman_core.paths import entry_dir
    from tallyman_xorq.build import load_entry

    if cache_worthy(project, content_hash):
        cache = result_cache(project)
        expr = load_entry(project, content_hash)
        if not cache.exists(expr):
            # Executing the cached expression forces the cache node to write
            # its input; a count is enough and avoids pulling rows to Python.
            expr.cache(cache=cache).count().execute()
        return Path(cache.storage.get_path(cache.calc_key(expr)))

    result_path = entry_dir(project, content_hash) / "result.parquet"
    if not result_path.exists():
        load_entry(project, content_hash).to_parquet(str(result_path))
    return result_path
