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
    already cached (see ``tallyman_xorq.source_cache``).  Reads of a cheap entry
    recompute the expression directly — no per-entry ``result.parquet`` is ever
    written, on demand or otherwise. Every consumer (the viewer, paginated
    reads, diffs, post-processing) reads ``cached_result_expr``.

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
import logging
import re
import sys
import threading
import time
from pathlib import Path

# Perf instrumentation rides a dedicated child namespace so it can be dialed up
# independently of the rest of tallyman's logging (#60), via TALLYMAN_LOG_LEVEL.
perf_log = logging.getLogger("tallyman.perf")

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
# stack. ``tracked_expr_from_alias`` consults it to step a self-referential recipe (a
# revise-in-place that chains off its own alias) back to the build-time parent
# before it recurses without bound — the #74 ~50GB RSS spike that locked the box.
# A set suffices: resolution walks alias history, not this stack's order.
_RECONSTRUCTING: contextvars.ContextVar[frozenset] = contextvars.ContextVar("_reconstructing", default=frozenset())

# Hard ceiling on reconstruction nesting. Cycle resolution already guarantees
# termination; this only fires on a cycle the resolver fails to break, turning a
# machine-locking memory blowup into a fast, clear error instead.
_MAX_RECON_DEPTH = 64

# The source digests ({rel_path: digest} from manifest.sources) the entry being
# reconstructed on this stack was BUILT from, threaded into read_project_file so a cold
# read resolves each source to the frozen data/.cas/<digest> clone it was built from —
# not a re-digest of the (possibly edited-in-place) live file (#115). Set per-entry in
# _recipe_expr alongside _RECONSTRUCTING and OVERWRITTEN (then restored) by each nested
# reconstruction, so a grandparent's read_project_file sees the grandparent's recorded
# sources. Carries the project so a read_project_file resolving a *different* project falls
# through to the live path. None outside reconstruction (the build path); an entry that
# recorded no sources (mode=off / pre-#86) yields an empty map, so read_project_file also
# falls through.
_RECON_SOURCES: contextvars.ContextVar[tuple[str, dict] | None] = contextvars.ContextVar(
    "_recon_sources", default=None
)


def _recorded_sources(project: str, content_hash: str) -> dict:
    """The entry's recorded ``manifest.sources`` ({rel_path: digest}), or ``{}``.

    Empty when the manifest is unreadable or the entry recorded no sources (mode=off,
    or a build predating #86); read_project_file then falls through to its live-file path.
    """
    from tallyman_core import read_manifest
    from tallyman_core.paths import entry_dir

    try:
        return read_manifest(entry_dir(project, content_hash)).sources or {}
    except (OSError, ValueError):
        return {}


def _resolve_noncyclic_hash(project: str, requested: str, content_hash: str) -> str:
    """The hash ``tracked_expr_from_alias`` should load, stepped out of any reconstruction cycle.

    ``tracked_expr_from_alias(alias)`` resolves an alias to its *live* head. When an entry's
    recipe reads ``tracked_expr_from_alias`` of the alias it is itself the head of (the
    chaining pattern ``catalog_revise`` documents), reconstructing it re-resolves
    to itself and recurses forever (#74). At build time that ``tracked_expr_from_alias``
    meant the *previous* revision — the alias is repointed only after the build —
    so we recover the build-time binding by walking back through alias history to
    the nearest revision not already under reconstruction on this stack.
    """
    from tallyman_core.aliases import get_alias, previous_version

    # `requested` is the caller's argument: an alias (from tracked_expr_from_alias) or a
    # hash (from pinned_expr_from_alias). Treat it as a lineage hint only when it names
    # an alias, so a hash shared across histories steps back through the alias the caller
    # asked for rather than whichever sorts first (#85). A hash has no lineage to follow,
    # so it stays None and resolution keeps the dict-order fallback.
    alias_hint = requested if get_alias(project, requested) is not None else None
    active = _RECONSTRUCTING.get()
    while (project, content_hash) in active:
        parent = previous_version(project, content_hash, alias=alias_hint)
        if parent is None:
            from tallyman_xorq.build import BuildError

            raise BuildError(
                f"tracked_expr_from_alias({requested!r}) in {project!r} resolves to an entry that "
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
    structurally identical to what was built. The recipe's ``read_project_file`` /
    deferred readers bind to the in-process *default* backend, so the returned
    expression roots there and composes with ``read_project_file`` and other
    ``tracked_expr_from_alias`` results as a single backend (#75).
    """
    from tallyman_core.paths import entry_dir, project_dir
    from tallyman_xorq.build import BuildError, _import_script
    from tallyman_xorq.portable import PLACEHOLDER

    active = _RECONSTRUCTING.get()
    if len(active) >= _MAX_RECON_DEPTH:
        raise BuildError(
            f"tracked_expr_from_alias reconstruction nested past {_MAX_RECON_DEPTH} levels "
            f"(at {content_hash} in {project!r}) — aborting a runaway recipe cycle"
        )

    code = (entry_dir(project, content_hash) / "expr.py").read_text().replace(PLACEHOLDER, str(project_dir(project)))
    # Mark this entry in-flight for the duration of the recipe exec, so any
    # tracked_expr_from_alias the recipe issues can step back out of a self-reference (#74).
    # In the SAME window, pin this entry's recorded sources so any read_project_file the
    # recipe issues resolves to the frozen .cas clone it was built from, not a
    # re-digest of the edited-in-place live file (#115). Both contextvars must wrap
    # _import_script — that is where the recipe's tracked_expr_from_alias / read_project_file run —
    # and reset in this finally, NOT the sys.modules-cleanup finally below.
    token = _RECONSTRUCTING.set(active | {(project, content_hash)})
    recon_token = _RECON_SOURCES.set((project, _recorded_sources(project, content_hash)))
    try:
        module, tmp = _import_script(code)
    finally:
        _RECON_SOURCES.reset(recon_token)
        _RECONSTRUCTING.reset(token)
    try:
        expr = getattr(module, _RECIPE_VAR, None)
        if expr is None:
            raise BuildError(f"recipe for {content_hash} in {project!r} binds no {_RECIPE_VAR!r}")
        return expr
    finally:
        # _import_script registers the recipe module in sys.modules (needed
        # during exec, e.g. for dataclass annotation resolution). Reconstruction
        # runs on every cold read since #73, so drop that registration to avoid
        # an unbounded sys.modules leak of recipe modules and the expression
        # graphs they pin. The returned expr still holds the module object (and
        # any UDF __globals__) while in use; only the name->module mapping goes.
        sys.modules.pop(getattr(module, "__name__", "") or "", None)
        try:
            tmp.unlink()
        except OSError:
            pass


def _cached_node_path(baked) -> Path | None:
    """On-disk path of the top-level baked result-cache snapshot, or None.

    ``baked`` is ``rewrite_for_build``'s output (the expression carrying the
    build's cache nodes). Returns the snapshot path when its top node is the
    baked result ``CachedNode``; None when the top isn't a cache — a cheap
    expression bakes nothing, or ``classify_build`` (serialized) and
    ``_is_worthy_expr`` (live) disagreed.
    """
    node = baked.op()
    if type(node).__name__ != "CachedNode":
        return None
    return Path(node.cache.storage.get_path(node.cache.calc_key(node.parent)))


def baked_snapshot_path(project: str, content_hash: str) -> Path | None:
    """Path of the entry's baked result-cache snapshot, or None if it bakes none.

    Mirrors ``cached_result_expr``'s derivation — re-import the recipe, rewrite
    it for build, read the top-level ``CachedNode``'s storage path — so it names
    the very file a cold read returns. None for a cheap entry (bakes nothing), a
    worthiness disagreement, or salt mode (bakes no cache — see
    ``rewrite_for_build``). The build uses it to size the snapshot (#87, the
    value-per-byte denominator); other callers use it to find the snapshot
    without reconstructing the read expression.
    """
    from tallyman_xorq import source_identity as si

    if si.mode() == "salt" or not cache_worthy(project, content_hash):
        return None

    from tallyman_xorq.source_cache import rewrite_for_build

    return _cached_node_path(rewrite_for_build(_recipe_expr(project, content_hash), project))


def snapshot_file_digest(path: Path) -> str:
    """SHA-256 of a snapshot parquet file's raw bytes.

    The new ``result_digest`` for worthy entries (ADR
    ``adr-result-digest-canonical-ordering.md``): the bake sorts by
    ``original_row_order`` before materialising, so the file bytes are
    deterministic run-to-run and the file hash is a sound multiset identity.
    ~0.3s for a 400 MB file — far cheaper than the retired per-row
    ``repr()``+sha256 loop.  Cheap entries record no digest (their
    recompute is live; there is no snapshot to hash).
    """
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def stream_row_count(expr) -> int:
    """Stream the result once, counting rows, without hashing.

    The cheap-entry build path uses this to force row-level evaluation (catch a
    failing cast / arithmetic at build time) and get the exact row count — the
    only obligation the build has for a cheap entry.  No digest is recorded for
    cheap entries; their result has no snapshot to hash.
    """
    n = 0
    for batch in expr.to_pyarrow_batches():
        n += batch.num_rows
    return n


def _recorded_result_digest(project: str, content_hash: str) -> str | None:
    from tallyman_core import read_manifest
    from tallyman_core.paths import entry_dir

    try:
        return read_manifest(entry_dir(project, content_hash)).result_digest
    except (OSError, ValueError):
        return None


def verify_result_faithful(project: str, content_hash: str) -> bool | None:
    """Whether the entry's baked snapshot matches its build-time digest.

    Only meaningful for worthy (snapshot-baking) entries: returns True when the
    snapshot file's SHA-256 matches the recorded ``result_digest``, False on
    drift, and None when no digest was recorded (cheap entry, or built before
    the canonical-order ADR — ``adr-result-digest-canonical-ordering.md``).
    Cheap entries record no digest, so this always returns None for them.
    """
    recorded = _recorded_result_digest(project, content_hash)
    if not recorded:
        return None
    snap = baked_snapshot_path(project, content_hash)
    if snap is None or not snap.exists():
        return None
    return snapshot_file_digest(snap) == recorded


def _reconstructed_hash(project: str, content_hash: str) -> str | None:
    """Best-effort structural ``content_hash`` of the entry's current reconstructed
    recipe — re-import ``expr.py``, rewrite for build, tokenize. Returns None if
    any step fails (a warning enrichment must never break the read). Tokenize only,
    no execute.
    """
    import tempfile

    try:
        from xorq.ibis_yaml.compiler import build_expr

        from tallyman_xorq.source_cache import rewrite_for_build

        rewritten = rewrite_for_build(_recipe_expr(project, content_hash), project)
        with tempfile.TemporaryDirectory(prefix="tallyman_rehash_") as d:
            return Path(build_expr(rewritten, builds_dir=Path(d))).name
    except Exception:
        return None


def recipe_is_structurally_nondeterministic(project: str, content_hash: str) -> bool:
    """Whether two reconstructions of the recipe yield different graph hashes (#88).

    A recipe that bakes a Python-level nondeterministic literal (``random.random()``,
    ``pd.Timestamp.now()``) at author time re-derives a different graph each
    ``expr.py`` import, so its content_hash moves between reconstructions — the
    structural case the op lint can't see (the literal is not an ibis op, so
    ``build._nondeterminism_warnings`` is blind to it). A stable graph whose only
    nondeterminism is at execute (an ibis nondeterministic op, an impure UDF, or
    source drift under ``off`` identity) hashes the same both times; that drift is
    execution-level (#83). Comparing two reconstructions to each other — not to the
    stored hash — keeps the predicate robust to any build-vs-reconstruct path skew.
    Best-effort: an unresolvable hash returns False (treated as not-structural).

    UDF entries are excluded: xorq's ``make_pandas_udf`` mints a fresh class per
    ``expr.py`` import, so even a *deterministic* UDF recipe reconstructs to a
    different graph hash each time — graph-hash instability from the class mint,
    not an author-time baked literal. The two-reconstruction discriminator can't
    tell those apart, and an impure UDF's nondeterminism is execution-level anyway
    (#83), so a UDF entry's drift is attributed execution, never structural.
    """
    from tallyman_core.paths import entry_build_dir

    # classify_build's why carries "udf:<names>" when the serialized build holds a
    # UDF (the same detection cache_worthy uses, #81). Reuse it rather than re-walk.
    # Best-effort, like _reconstructed_hash below: classify_build does unguarded
    # Path.glob + read_text, so a missing or unreadable build dir raises. This
    # predicate runs on the self-heal warning path OUTSIDE its try/except (and, per
    # #125, would run at build time too), so a fault here must degrade to "not
    # structural" — the conservative execution (#83) attribution — never break the
    # read it only annotates. The docstring's "an unresolvable hash returns False"
    # contract owns this for every caller.
    try:
        why = classify_build(entry_build_dir(project, content_hash))["why"]
    except Exception:
        return False
    if "udf:" in why:
        return False

    a = _reconstructed_hash(project, content_hash)
    b = _reconstructed_hash(project, content_hash)
    return a is not None and b is not None and a != b


def _warn_if_self_heal_unfaithful(project: str, content_hash: str, path: Path) -> None:
    """Compare a just-repopulated snapshot to the build-time digest, warn on drift.

    Eviction's load-bearing assumption is that an evicted snapshot recomputes to
    what was evicted (the result-cache + source-identity ADRs both call this out).
    For an execution-nondeterministic entry that is false: the snapshot self-heals
    to *different* bytes, silently. This fires the soundness signal — a
    ``tallyman.perf`` warning — without ever breaking the read (best-effort),
    attributing the drift as structural (#88: the recipe's graph hash itself moves)
    vs execution (#83: a fixed graph that runs differently).

    The digest is now the SHA-256 of the snapshot file bytes (ADR
    ``adr-result-digest-canonical-ordering.md``), so we hash the newly-landed
    file directly — no extra expression execution.
    """
    recorded = _recorded_result_digest(project, content_hash)
    if not recorded:
        return
    try:
        actual = snapshot_file_digest(path)
    except Exception:
        return
    if actual != recorded:
        kind = (
            "structural (#88) — the recipe bakes a nondeterministic literal that re-derives a different graph hash"
            if recipe_is_structurally_nondeterministic(project, content_hash)
            else "execution (#83) — a fixed graph that runs differently each execute, or source drift under off"
        )
        perf_log.warning(
            "cached_result_expr self-heal %s: UNFAITHFUL recompute [%s] — result "
            "digest %s != recorded %s; eviction self-healed it to different bytes "
            "than were built",
            content_hash,
            kind,
            actual,
            recorded,
        )


@functools.lru_cache(maxsize=256)
def _resolve_result_plan(project: str, content_hash: str):
    """The expensive, memoisable half of ``cached_result_expr``: reconstruct the
    recipe and decide how the entry's result is read. Returns either
    ``("recompute", expr)`` for a cheap/fallback entry, or ``("baked", baked, path)``
    for an expensive entry whose snapshot ``cached_result_expr`` re-checks on every
    call (so a snapshot evicted *after* a warm read still self-heals rather than
    returning a dangling ``deferred_read_parquet``).
    """
    t0 = time.monotonic()

    def _cold(tag):
        perf_log.debug(
            "cached_result_expr cold read %s: path=%s wall_ms=%.1f",
            content_hash,
            tag,
            (time.monotonic() - t0) * 1000,
        )

    raw = _recipe_expr(project, content_hash)
    if not cache_worthy(project, content_hash):
        _cold("cheap-recompute")
        return ("recompute", raw)

    # Expensive: locate the baked snapshot via the same rewrite the build used.
    # rewrite_for_build wraps the expression in a ParquetSnapshotCache whose
    # structural key matches the file baked at build time, so storage/calc_key
    # give its on-disk path without re-executing.
    from tallyman_xorq.source_cache import rewrite_for_build

    baked = rewrite_for_build(raw, project)
    path = _cached_node_path(baked)
    if path is None:
        # classify_build (serialized) and _is_worthy_expr (live) disagreed — fall
        # back to recompute rather than dereference a cache that wasn't baked.
        _cold("worthy-recompute-fallback")
        return ("recompute", raw)
    _cold("baked-read")
    return ("baked", baked, path)


# Per-(project, content_hash) locks serialise the snapshot heal in cached_result_expr
# so two concurrent cold reads of the same entry don't both execute the shared baked op
# (#79). Keyed locks under a master lock; the registry grows with distinct entries read
# (bounded by catalog size) and stale locks are harmless.
_HEAL_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_HEAL_LOCKS_GUARD = threading.Lock()


def _heal_lock(project: str, content_hash: str) -> threading.Lock:
    key = (project, content_hash)
    with _HEAL_LOCKS_GUARD:
        lock = _HEAL_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _HEAL_LOCKS[key] = lock
        return lock


def cached_result_expr(project: str, content_hash: str):
    """The entry's result as a single-backend expression on the default backend.

    Reconstituting a catalog entry for chaining (``tracked_expr_from_alias``) or diffing
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

    salt source-identity mode bakes no cache (path-only snapshot keys would
    collide across salted entries — see ``rewrite_for_build``), so a salted entry
    falls through to recompute: a cheap one via ``cheap-recompute``, an expensive
    one via ``worthy-recompute-fallback`` (its ``rewrite_for_build`` returns the
    expression uncached under salt, so there is no snapshot to dereference). Both
    are single-backend recompute expressions — content-honest, no parquet.

    The expensive reconstruction is memoised in ``_resolve_result_plan``; the
    snapshot's on-disk presence is re-checked HERE on every call, so an expensive
    entry whose snapshot is evicted *after* a warm read self-heals on the next
    read instead of handing back a stale ``deferred_read_parquet`` (which would
    ``ValueError: At least one path is required`` at execute time). The
    perf-namespace cold-read tag (#87) is emitted from the memoised half, so it
    still fires once per genuine reconstruct.
    """
    from xorq.expr.api import deferred_read_parquet

    plan = _resolve_result_plan(project, content_hash)
    if plan[0] == "recompute":
        return plan[1]
    _, baked, path = plan
    if not path.exists():
        # Single-flight the heal per (project, content_hash). _resolve_result_plan is
        # lru-cached, so concurrent cold readers share one `baked` op; without this lock
        # both miss the evicted snapshot and both execute it, and the loser raises —
        # DataFusion "Already borrowed" on the shared in-process op, or FileNotFoundError
        # on xorq ParquetStorage's fixed <key>.parquet.tmp across processes — surfacing
        # as a 500 from api_data and, swallowed by ensure_session, an empty grid (#79).
        with _heal_lock(project, content_hash):
            if not path.exists():  # a peer thread may have healed it while we waited
                try:
                    baked.count().execute()  # evicted snapshot: repopulate once, then read
                except (FileNotFoundError, ValueError):
                    # A peer *process* (MCP build vs companion heal sharing compute_cache)
                    # can win the shared-tmp rename out from under us. If the snapshot
                    # landed, read it; otherwise the failure is real, so re-raise.
                    if not path.exists():
                        raise
                perf_log.debug("cached_result_expr self-heal %s: evicted-self-heal", content_hash)
                # The repopulated snapshot must match the digest recorded at build; a
                # mismatch means this entry's recompute is execution-nondeterministic and
                # eviction just served different bytes than were built (#83).
                _warn_if_self_heal_unfaithful(project, content_hash, path)
    return deferred_read_parquet(str(path))


# Existing callers clear the result-expr memo via cached_result_expr.cache_clear()
# (companion app, conftest, cache tests); the memo now lives on the plan resolver,
# so re-expose its cache controls on the public name.
cached_result_expr.cache_clear = _resolve_result_plan.cache_clear
cached_result_expr.cache_info = _resolve_result_plan.cache_info
