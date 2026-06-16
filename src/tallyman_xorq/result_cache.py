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

import contextvars
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


# Entries currently being reconstructed by cached_result_expr, on this call
# stack. ``from_catalog`` consults it to step a self-referential recipe (a
# revise-in-place that chains off its own alias) back to the build-time parent
# before it recurses without bound — the #74 ~50GB RSS spike that locked the box.
# A set suffices: resolution walks alias history, not this stack's order.
_RECONSTRUCTING: contextvars.ContextVar[frozenset] = contextvars.ContextVar("_reconstructing", default=frozenset())

# Hard ceiling on reconstruction nesting. Cycle resolution already guarantees
# termination; this only fires on a cycle the resolver fails to break, turning a
# machine-locking memory blowup into a fast, clear error instead.
_MAX_RECON_DEPTH = 64


def _resolve_noncyclic_hash(project: str, requested: str, content_hash: str) -> str:
    """The hash ``from_catalog`` should load, stepped out of any reconstruction cycle.

    ``from_catalog(alias)`` resolves an alias to its *live* head. When an entry's
    recipe reads ``from_catalog`` of the alias it is itself the head of (the
    chaining pattern ``catalog_revise`` documents), reconstructing it re-resolves
    to itself and recurses forever (#74). At build time that ``from_catalog``
    meant the *previous* revision — the alias is repointed only after the build —
    so we recover the build-time binding by walking back through alias history to
    the nearest revision not already under reconstruction on this stack.
    """
    from tallyman_core.aliases import previous_version

    active = _RECONSTRUCTING.get()
    while (project, content_hash) in active:
        parent = previous_version(project, content_hash)
        if parent is None:
            from tallyman_xorq.build import BuildError

            raise BuildError(
                f"from_catalog({requested!r}) in {project!r} resolves to an entry that "
                "reconstructs itself with no prior revision to fall back to"
            )
        content_hash = parent
    return content_hash


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

    active = _RECONSTRUCTING.get()
    if len(active) >= _MAX_RECON_DEPTH:
        raise BuildError(
            f"from_catalog reconstruction nested past {_MAX_RECON_DEPTH} levels "
            f"(at {content_hash} in {project!r}) — aborting a runaway recipe cycle"
        )

    code = (entry_dir(project, content_hash) / "expr.py").read_text().replace(PLACEHOLDER, str(project_dir(project)))
    # Mark this entry in-flight for the duration of the recipe exec, so any
    # from_catalog the recipe issues can step back out of a self-reference (#74).
    token = _RECONSTRUCTING.set(active | {(project, content_hash)})
    try:
        module, tmp = _import_script(code)
    finally:
        _RECONSTRUCTING.reset(token)
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
    than an expression — downloads, promote-diff, post-processing. Writes the
    result in place if absent (#73), and a deleted one self-heals on the next call.

    Reconstructs through ``cached_result_expr`` (the recipe path), not
    ``load_entry`` (the serialized build). An entry that ``from_catalog``s an
    expensive parent serialises a *bare* ``deferred_read_parquet`` of the
    parent's baked snapshot — #75 strips the parent's ``CachedNode`` so the
    composed expression stays on one backend — with no recipe or source embedded
    to recompute from. On a cold compute cache (fresh clone / write-isolated
    overlay) ``load_entry`` resolves that read to zero files and raises
    ``ValueError: At least one path is required`` (#73/#74). Re-running the recipe
    re-resolves ``from_catalog`` and recomputes the parent, so an evicted snapshot
    anywhere in the chain self-heals (``cached_result_expr`` repopulates it).

    salt source-identity mode is the exception: there ``cached_result_expr``
    routes its reads back through ``ensure_result`` (path-only snapshot keys would
    collide across salted entries — see ``rewrite_for_build``), so we load the
    serialized build here instead, to avoid recursing into ourselves.
    """
    from tallyman_core.paths import entry_result_path
    from tallyman_xorq import source_identity as si

    result_path = entry_result_path(project, content_hash)
    if not result_path.exists():
        if si.mode() == "salt":
            from tallyman_xorq.build import load_entry

            load_entry(project, content_hash).to_parquet(str(result_path))
        else:
            cached_result_expr(project, content_hash).to_parquet(str(result_path))
    return result_path
