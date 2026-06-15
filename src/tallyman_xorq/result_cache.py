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
import shutil
import tempfile
import threading
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
    from tallyman_core.paths import entry_build_dir

    return classify_build(entry_build_dir(project, content_hash))["worthy"]


@functools.lru_cache(maxsize=16)
def result_cache(project: str):
    """A ParquetSnapshotCache rooted under the project's result_cache dir.

    One connection per project per process — lru_cache ensures xo.connect() is
    called once rather than on every cached_result_expr / ensure_result call.
    Bounded (a process touches only a handful of projects); an evicted project
    just reopens its connection on next use.
    """
    import xorq.api as xo

    from tallyman_core.paths import result_cache_dir

    base = result_cache_dir(project)
    base.mkdir(parents=True, exist_ok=True)
    return xo.ParquetSnapshotCache.from_kwargs(source=xo.connect(), base_path=base)


@functools.lru_cache(maxsize=256)
def cached_result_expr(project: str, content_hash: str):
    """The entry's result as an expression that resolves its own materialisation.

    Bounded so the cache can't grow without limit across a long-lived process;
    entries are content-addressed, so an evicted hash rebuilds an identical
    expression on the next access.

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

    from tallyman_xorq import source_identity as si
    from tallyman_xorq.build import load_entry

    # salt identity mode: the snapshot cache keys on path-only expression
    # structure, so two salted entries with identical code+paths but
    # different source CONTENT collide on the same cache key and one serves
    # the other's stale parquet (demonstrated in the cache lab's
    # append_invalidates scenario). Route every read through the entry-dir
    # result.parquet, which is keyed by the salted content_hash.
    if si.mode() == "salt":
        return xo.deferred_read_parquet(str(ensure_result(project, content_hash)))
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
    from tallyman_core.paths import entry_result_path
    from tallyman_xorq import source_identity as si
    from tallyman_xorq.build import load_entry

    # salt identity mode bypasses the snapshot cache (path-only keys collide
    # across salted entries — see cached_result_expr) and always uses the
    # in-place result.parquet under the salted-hash entry dir.
    if si.mode() != "salt" and cache_worthy(project, content_hash):
        cache = result_cache(project)
        expr = load_entry(project, content_hash)
        if not cache.exists(expr):
            # result_cache() is memoized, so its storage's directory is only
            # created at construction; if the result_cache dir was deleted
            # since (self-heal covers a deleted *directory*, not just a
            # deleted file), the write below would land in a missing path.
            cache.storage.path.mkdir(parents=True, exist_ok=True)
            # Executing the cached expression forces the cache node to write
            # its input; a count is enough and avoids pulling rows to Python.
            expr.cache(cache=cache).count().execute()
        return Path(cache.storage.get_path(cache.calc_key(expr)))

    result_path = entry_result_path(project, content_hash)
    if not result_path.exists():
        load_entry(project, content_hash).to_parquet(str(result_path))
    return result_path


# Per-target locks so two concurrent loads of the same entry don't race on the
# rmtree/build/marker sequence below. Mirrors portable._expand_lock; a bounded
# number of entries are touched per process, so this never grows large.
_result_build_locks_guard = threading.Lock()
_result_build_locks: dict[str, threading.Lock] = {}


def _result_build_lock(key: str) -> threading.Lock:
    with _result_build_locks_guard:
        return _result_build_locks.setdefault(key, threading.Lock())


def ensure_result_build(project: str, content_hash: str) -> Path:
    """Portable xorq build whose expression reads the entry's materialised result.

    For a cache-worthy entry the recipe is an Aggregate/Join/Sort that re-runs
    the full DAG on every ``count()`` and every paged ``LIMIT`` window. This
    persists a tiny build whose expression is a ``deferred_read_parquet`` of the
    cached result (``ensure_result`` guarantees the parquet is present first), so
    the viewer pages over the materialised parquet instead of recomputing (#71).
    Push-down still holds: a parquet scan pushes filters/limit/sort down too.

    The build is portable (``${TALLYMAN_PROJECT_ROOT}`` placeholders), so the
    caller expands it with ``ensure_expanded_build`` exactly like the recipe.

    A sibling ``.complete`` marker, written last, gates reuse: a build
    interrupted by a crash/OOM leaves a partial dir with no marker and is redone,
    never served truncated. Entries are immutable and content-addressed, so the
    build is always correct once written. The path is stable across restarts,
    which keeps xorq's build-dir-keyed stat-cache lookups hitting.
    """
    import xorq.api as xo
    from xorq.ibis_yaml.compiler import build_expr

    from tallyman_core.paths import entry_result_build_dir, project_dir
    from tallyman_xorq._git_state_guard import install_git_state_guard
    from tallyman_xorq.portable import make_portable_inplace

    target = entry_result_build_dir(project, content_hash)
    marker = target.with_name(target.name + ".complete")
    if marker.exists():
        return target
    with _result_build_lock(str(target)):
        # Re-check under the lock: another thread may have finished building.
        if marker.exists():
            return target
        # ensure_result must run before build_expr — the deferred read points at
        # the parquet path, and the file has to exist when the viewer executes.
        result_path = ensure_result(project, content_hash)
        # git-provenance capture in xorq's compiler can SIGSEGV when forked from
        # the long-lived server; make it best-effort, as build_and_persist does.
        install_git_state_guard()
        expr = xo.deferred_read_parquet(str(result_path))
        if target.exists():
            shutil.rmtree(target)
        with tempfile.TemporaryDirectory(prefix="tallyman_result_build_") as tmp:
            build_path = Path(build_expr(expr, builds_dir=Path(tmp)))
            target.mkdir(parents=True)
            for item in build_path.rglob("*"):
                dest = target / item.relative_to(build_path)
                if item.is_dir():
                    dest.mkdir(parents=True, exist_ok=True)
                else:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_bytes(item.read_bytes())
        # Rewrite project-root substrings to the portable placeholder so the
        # caller can expand it on this host exactly like the recipe build.
        make_portable_inplace(target, project_dir(project))
        marker.write_text("")
    return target
