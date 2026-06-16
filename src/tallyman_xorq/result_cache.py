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


# The recipe variable name build_and_persist binds and persists into expr.py.
_RECIPE_VAR = "expr"


def _recipe_expr(project: str, content_hash: str):
    """Re-import the entry's persisted recipe (``expr.py``) as a live expression.

    Symmetric with the original build's ``_import_script`` — same source code,
    same source-identity handling — so the reconstructed expression is
    structurally identical to what was built. The recipe's ``from_project`` /
    deferred readers bind to the in-process *default* backend, so the returned
    expression roots there and composes with ``from_project`` and other
    ``from_catalog`` results as a single backend (#75).
    """
    from tallyman_core.paths import entry_dir, project_dir
    from tallyman_xorq.build import BuildError, _import_script
    from tallyman_xorq.portable import PLACEHOLDER

    code = (entry_dir(project, content_hash) / "expr.py").read_text().replace(PLACEHOLDER, str(project_dir(project)))
    module, tmp = _import_script(code)
    try:
        expr = getattr(module, _RECIPE_VAR, None)
        if expr is None:
            raise BuildError(f"recipe for {content_hash} in {project!r} binds no {_RECIPE_VAR!r}")
        return expr
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


@functools.lru_cache(maxsize=256)
def cached_result_expr(project: str, content_hash: str):
    """The entry's result as a single-backend expression on the default backend.

    Reconstituting a catalog entry for chaining (``from_catalog``) or diffing
    must yield an expression rooted on the *one* in-process backend, so two
    entries combined in a ``union`` / ``join`` resolve to a single backend
    instead of raising "Multiple backends found" (#75). #73's predecessor loaded
    each entry's build into its own backend, so any composition spanned two.
    Both paths below root on the default backend and materialise no entry
    ``result.parquet``:

      * Cheap entry — re-import its recipe (``expr.py``) and hand back the live
        expression. Recompute on read is ~free (pushdown over a columnar source;
        a non-parquet parse is cached at the read), and the recipe binds to the
        default backend.
      * Expensive entry — read its baked ``result_cache`` snapshot directly as a
        single ``deferred_read_parquet`` on the default backend. The snapshot was
        materialised when the entry built (``rewrite_for_build``'s top-level
        result cache); reading it skips re-running the DAG *and* keeps the cache
        node's own backend out of the caller's expression (a baked
        ``CachedNode`` carries its own storage connection, which is the second
        backend #73 tripped over). Self-heals: an evicted snapshot is recomputed
        once via the cache node, then read.

    Bounded LRU so the cache can't grow without limit; entries are
    content-addressed, so an evicted hash rebuilds an identical expression.

    salt source-identity mode skips baked caches (path-only snapshot keys would
    collide across salted entries — see ``rewrite_for_build``), so reads route
    through the content-honest in-place ``result.parquet`` instead.
    """
    import xorq.api as xo

    from tallyman_xorq import source_identity as si

    if si.mode() == "salt":
        return xo.deferred_read_parquet(str(ensure_result(project, content_hash)))

    raw = _recipe_expr(project, content_hash)
    if not cache_worthy(project, content_hash):
        return raw

    # Expensive: locate the baked snapshot via the same rewrite the build used,
    # then read it as a bare parquet. rewrite_for_build wraps the expression in a
    # ParquetSnapshotCache whose structural key matches the file baked at build
    # time, so storage/calc_key give its on-disk path without re-executing.
    from tallyman_xorq.source_cache import rewrite_for_build

    baked = rewrite_for_build(raw, project)
    node = baked.op()
    if type(node).__name__ != "CachedNode":
        # classify_build (serialized) and _is_worthy_expr (live) disagreed — fall
        # back to recompute rather than dereference a cache that wasn't baked.
        return raw
    path = Path(node.cache.storage.get_path(node.cache.calc_key(node.parent)))
    if not path.exists():
        baked.count().execute()  # evicted snapshot: repopulate once, then read
    return xo.deferred_read_parquet(str(path))


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
