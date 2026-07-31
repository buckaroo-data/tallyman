"""xorq-backed result cache for catalog entries, gated by a structural rubric.

The entry's frozen build (``xorq_build/``) is the source of truth, and **every
read loads it** — ``ensure_expanded_build`` → ``load_expr(cache_dir=…)`` → the
deep cache-dir rewrite (``portable.rewrite_cache_dirs``). This is the canonical
read of ``docs/system-contract.md`` (#163): a content hash names the fixed
result its build freezes, so reads never re-import ``expr.py`` — recipe
re-execution happens only at minting (build / revise / recalc) and in the
structural-nondeterminism diagnostic below. A missing or unloadable build is a
hard error (ADR D6), never a fallback.

Whether we keep a materialised copy of the result depends on how expensive the
computation is — caching only pays off when recompute costs far more than
reading a cached parquet:

  * Expensive entries — the expression contains an Aggregate / Join / Sort /
    window / UDF — are materialised in xorq's ``ParquetSnapshotCache``.  Reads
    hit the cache; a deleted cache file self-heals by re-executing the frozen
    build, and the healed bytes are verified against the recorded
    ``result_digest`` before they are served (ADR D7/D10).
  * Cheap entries — a source read plus projections / renames / row-wise scalar
    math (the citibike column-reorder/derive case) — materialise nothing (#73).
    Recompute ≈ re-reading a columnar source (pushdown), so a stored copy would
    burn work and storage for ~no savings; a non-parquet source's parse is
    already cached (see ``tallyman_xorq.source_cache``).  Reads of a cheap entry
    execute the loaded build directly — no per-entry ``result.parquet`` is ever
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
from typing import NamedTuple

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


def load_entry_expr(project: str, content_hash: str):
    """Load the entry's frozen build as a live expression — the canonical read (#163).

    ``ensure_expanded_build`` → ``load_expr(expanded, cache_dir=compute_cache)``
    → ``rewrite_cache_dirs`` (the deep rewrite xorq's shallow load-time one
    misses for nested cache nodes, ADR D4). The build binds by value — parents
    inlined, sources as content-pinned paths — so the returned expression is the
    entry's fixed computation regardless of where alias heads sit today.

    A missing or unloadable build raises ``BuildError`` naming the entry and the
    remedy (ADR D6). There is no recipe fallback: ``expr.py`` binds by name and
    re-executing it is how #163's lineage drift happened.
    """
    from xorq.ibis_yaml.compiler import load_expr

    from tallyman_core.paths import (
        compute_cache_dir,
        entry_build_dir,
        entry_expanded_build_dir,
        project_dir,
    )
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.portable import ensure_expanded_build, rewrite_cache_dirs

    build_dir = entry_build_dir(project, content_hash)
    if not (build_dir / "expr.yaml").is_file():
        raise BuildError(
            f"entry {content_hash} in {project!r} has no loadable xorq_build/ "
            f"(expected {build_dir / 'expr.yaml'}). The frozen build is the read "
            "path's only source of truth — rebuild the entry (catalog_revise / "
            "catalog_recalc) to restore it; reads never fall back to expr.py."
        )
    cache_dir = compute_cache_dir(project)
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        expanded = ensure_expanded_build(
            build_dir, project_dir(project), entry_expanded_build_dir(project, content_hash)
        )
        loaded = load_expr(expanded, cache_dir=cache_dir)
        return rewrite_cache_dirs(loaded, cache_dir)
    except Exception as exc:
        raise BuildError(
            f"entry {content_hash} in {project!r}: loading its frozen build failed "
            f"({type(exc).__name__}: {exc}) — rebuild the entry (catalog_revise / catalog_recalc)"
        ) from exc


def _profile_content_token(backend) -> str:
    """A backend profile's content identity: its tokenized dict minus ``idx``.

    ``Profile.idx`` is a process-local counter (creation-order noise), so two
    backends are the same *kind* of connection exactly when their idx-stripped
    profiles tokenize identically — the equality ``Profile.__eq__`` itself uses.
    """
    import toolz
    from xorq.common.utils.dasher import tokenize

    return tokenize(toolz.dissoc(backend._profile.as_dict(), "idx"))


def _rebind_to_default_backend(expr):
    """Collapse every backend in a loaded build onto the process default backend.

    ``load_expr`` mints fresh backend objects per profile, so two loaded builds —
    or a loaded build and a recipe's ``read_project_file`` — span distinct
    backend objects and composition raises "Multiple backends found". The
    contract allows the collapse because every profile in a tallyman build is
    content-identical no-arg ``xorq_datafusion``; that assumption is enforced
    here (ADR D3): more than one distinct content profile fails loudly rather
    than misbinding a node onto the wrong kind of connection. A raw
    ``DatabaseTable`` (bundled data that would need a copy) also fails loudly —
    ``replace_sources`` raises unless told to transfer, and tallyman builds must
    never contain one (in-memory reads are rejected at build).
    """
    from xorq.common.utils.graph_utils import find_all_sources, replace_sources
    from xorq.config import default_backend

    target = default_backend()
    others = [s for s in find_all_sources(expr) if s is not target]
    if not others:
        return expr
    tokens = {_profile_content_token(s) for s in (*others, target)}
    if len(tokens) > 1:
        from tallyman_xorq.build import BuildError

        raise BuildError(
            f"loaded build spans {len(tokens)} distinct backend content profiles; "
            "rebinding onto the default backend would misbind — every profile in a "
            "tallyman build must be content-identical no-arg xorq_datafusion (ADR D3)"
        )
    return replace_sources({id(s): target for s in others}, expr)


def _assert_recorded_snapshot_key(project: str, content_hash: str, path: Path) -> None:
    """ADR D8 tripwire: the read's snapshot derivation must match the build's.

    The build records the baked snapshot's filename (``manifest.snapshot_key``);
    the canonical read asserts its own derivation lands on the same file. With
    both sides sharing one derivation route the original disagreement mechanism
    is gone — a mismatch means an xorq tokenization change or a rewrite drift,
    and reading on would serve the wrong file silently. Manifests without the
    field (mid-rebuild corpus) are skipped.
    """
    from tallyman_core import read_manifest
    from tallyman_core.paths import entry_dir

    try:
        recorded = read_manifest(entry_dir(project, content_hash)).snapshot_key
    except (OSError, ValueError):
        return
    if recorded is None or path.name == recorded:
        return
    from tallyman_xorq.build import BuildError

    raise BuildError(
        f"entry {content_hash} in {project!r}: the read derives snapshot key "
        f"{path.name!r} but the build recorded {recorded!r} — the two derivations "
        "have diverged (an xorq upgrade or a rewrite drift); rebuild the entry "
        "rather than read the wrong snapshot"
    )


def baked_snapshot_path(project: str, content_hash: str) -> Path | None:
    """Path of the entry's baked result-cache snapshot, or None if it bakes none.

    Derived from the entry's *loaded build* — the same derivation
    ``cached_result_expr`` serves and the build recorded — so it names the very
    file a cold read returns. None for a cheap entry (bakes nothing), a
    worthiness disagreement, or a build written under salt mode (no cache node —
    see ``rewrite_for_build``). Raises like ``load_entry_expr`` when the build is
    missing (ADR D6).
    """
    return _resolve_result_plan(project, content_hash).path


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
    """Whether executing this entry's frozen build reproduces its recorded digest.

    Re-anchored by ADR D7: the snapshot is located through the entry's *own
    loaded build* — never today's alias state — so the verdict is about this
    entry's bytes, not a sibling's. Returns True when the snapshot file's
    SHA-256 matches the recorded ``result_digest``, False on drift, and None
    when there is nothing to check (no recorded digest — a cheap entry — or no
    snapshot on disk yet; a cold entry heals on its next read, which verifies).
    """
    recorded = _recorded_result_digest(project, content_hash)
    if not recorded:
        return None
    snap = _resolve_result_plan(project, content_hash).path
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


# Called (project, content_hash) after an UNFAITHFUL self-heal, best-effort.
# The companion registers a hook that evicts the entry's Buckaroo session and
# pushes the SSE badge event (ADR D7/D10); processes without an SSE bus (the
# MCP server) still get the durable errors.jsonl record written below.
UNFAITHFUL_HEAL_HOOKS: list = []


def _verify_self_heal(project: str, content_hash: str, path: Path) -> None:
    """Verify a just-repopulated snapshot against the build-time digest (ADR D7).

    Eviction's load-bearing assumption is that an evicted snapshot recomputes to
    what was evicted. A faithful heal is byte-identical (the canonical sort makes
    the bake deterministic); a mismatch means this entry's recompute is genuinely
    nondeterministic and the heal just manufactured different bytes under the
    entry's recorded hash. The read is still served — the bytes are the honest
    output of the frozen build — but never silently:

      * a ``tallyman.perf`` UNFAITHFUL warning, attributing the drift as
        structural (#88: the recipe's graph hash itself moves) vs execution
        (#83: a fixed graph that runs differently);
      * a durable ``errors.jsonl`` record (``code="unfaithful_heal"``) — the UI
        badge's source, and the ADR D12 pin marking (the entry's bytes are not
        regenerable, so eviction machinery must treat its snapshot as retained,
        not reclaimable);
      * the entry's ``.buckaroo_stat_cache`` is wiped (ADR D10): Buckaroo's
        summary stats key on expression structure and stable paths
        (buckaroo#955), so stale stats would render beside the fresh rows;
      * registered hooks fire (companion: session eviction + SSE).
    """
    recorded = _recorded_result_digest(project, content_hash)
    if not recorded:
        return
    try:
        actual = snapshot_file_digest(path)
    except Exception:
        return
    if actual == recorded:
        return
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
    import shutil

    from tallyman_core.paths import entry_stat_cache_dir

    shutil.rmtree(entry_stat_cache_dir(project, content_hash), ignore_errors=True)
    try:
        from tallyman_core.errors import record_error

        record_error(
            project,
            code="unfaithful_heal",
            message=(
                f"self-heal produced different bytes than were built [{kind}]: "
                f"digest {actual} != recorded {recorded}"
            ),
            hash=content_hash,
        )
    except Exception:
        perf_log.debug("unfaithful-heal error record failed for %s", content_hash, exc_info=True)
    for hook in list(UNFAITHFUL_HEAL_HOOKS):
        try:
            hook(project, content_hash)
        except Exception:
            perf_log.debug("unfaithful-heal hook failed for %s", content_hash, exc_info=True)


class _ResultPlan(NamedTuple):
    """How an entry's result is read, resolved once from its frozen build.

    ``loaded`` is the build as ``load_entry_expr`` returned it — per-load backend
    objects, exactly the expression shape the build executed, so a heal re-runs
    the build's own computation. ``graph`` is the same graph rebound onto the
    process default backend (ADR D3) — the composable form chaining and cheap
    reads serve. ``path`` is the baked snapshot's location, None when the build
    bakes none (cheap entry, worthiness disagreement, or a salt-mode build).
    """

    kind: str  # "baked" | "recompute"
    loaded: object
    graph: object
    path: Path | None


@functools.lru_cache(maxsize=256)
def _resolve_result_plan(project: str, content_hash: str) -> _ResultPlan:
    """The expensive, memoisable half of ``cached_result_expr``: load the entry's
    frozen build and decide how its result is read. The LRU is sound because its
    key finally determines its value — the build is immutable and the plan is a
    pure function of it. ``cached_result_expr`` re-checks the snapshot's on-disk
    presence on every call, so a snapshot evicted *after* a warm read still
    self-heals rather than returning a dangling ``deferred_read_parquet``.
    """
    t0 = time.monotonic()

    def _cold(tag):
        perf_log.debug(
            "cached_result_expr cold read %s: path=%s wall_ms=%.1f",
            content_hash,
            tag,
            (time.monotonic() - t0) * 1000,
        )

    loaded = load_entry_expr(project, content_hash)
    graph = _rebind_to_default_backend(loaded)
    path = _cached_node_path(loaded)
    if path is None:
        # No baked result cache in the build: a cheap entry, a
        # classify_build/_is_worthy_expr disagreement, or a salt-mode build
        # (rewrite_for_build bakes no cache under salt). Execute the frozen
        # graph directly.
        _cold("cheap-recompute")
        return _ResultPlan("recompute", loaded, graph, None)
    _assert_recorded_snapshot_key(project, content_hash, path)
    _cold("baked-read")
    return _ResultPlan("baked", loaded, graph, path)


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

    Both paths below serve the entry's *frozen build* (``load_entry_expr``),
    root on the default backend, and materialise no entry ``result.parquet``:

      * Cheap entry — the loaded build's graph, rebound onto the default backend
        (ADR D3). Recompute on read is ~free (pushdown over content-pinned
        columnar sources; a non-parquet parse is cached at the read), and the
        graph is frozen, so the bytes are the entry's recorded result — never
        today's alias heads.
      * Expensive entry — read its baked ``result_cache`` snapshot directly as a
        single ``deferred_read_parquet`` on the default backend. The snapshot was
        materialised when the entry built (``rewrite_for_build``'s top-level
        result cache); reading it skips re-running the DAG and keeps the cache
        node's own storage connection out of the caller's expression. Self-heals:
        an evicted snapshot is re-executed once *from the frozen build*, verified
        against the recorded digest (ADR D7), then read.

    For composition that must stay self-healing — chaining a child recipe off
    this entry — use ``entry_graph_expr`` instead: it returns the graph with the
    cache node still in place, so the child's build carries its parents' healing
    knowledge (ADR D4).

    Bounded LRU so the cache can't grow without limit; entries are
    content-addressed and builds immutable, so an evicted hash reloads an
    identical expression.

    A salt-mode build carries no cache nodes (path-only snapshot keys would
    collide across salted entries — see ``rewrite_for_build``), so a salted entry
    falls through to the recompute path — single-backend, content-honest, no
    parquet.

    The build load is memoised in ``_resolve_result_plan``; the snapshot's
    on-disk presence is re-checked HERE on every call, so an expensive entry
    whose snapshot is evicted *after* a warm read self-heals on the next read
    instead of handing back a stale ``deferred_read_parquet`` (which would
    ``ValueError: At least one path is required`` at execute time). The
    perf-namespace cold-read tag (#87) is emitted from the memoised half, so it
    still fires once per genuine load.
    """
    from xorq.expr.api import deferred_read_parquet

    plan = _resolve_result_plan(project, content_hash)
    if plan.kind == "recompute":
        return plan.graph
    path = plan.path
    if not path.exists():
        # Single-flight the heal per (project, content_hash). _resolve_result_plan is
        # lru-cached, so concurrent cold readers share one loaded op; without this lock
        # both miss the evicted snapshot and both execute it, and the loser raises —
        # DataFusion "Already borrowed" on the shared in-process op, or FileNotFoundError
        # on xorq ParquetStorage's fixed <key>.parquet.tmp across processes — surfacing
        # as a 500 from api_data and, swallowed by ensure_session, an empty grid (#79).
        with _heal_lock(project, content_hash):
            if not path.exists():  # a peer thread may have healed it while we waited
                try:
                    # Evicted snapshot: re-execute the frozen build once, then read.
                    # plan.loaded is the exact expression shape the build executed,
                    # so a faithful heal lands byte-identical parquet.
                    plan.loaded.count().execute()
                except (FileNotFoundError, ValueError):
                    # A peer *process* (MCP build vs companion heal sharing compute_cache)
                    # can win the shared-tmp rename out from under us. If the snapshot
                    # landed, read it; otherwise the failure is real, so re-raise.
                    if not path.exists():
                        raise
                perf_log.debug("cached_result_expr self-heal %s: evicted-self-heal", content_hash)
                _verify_self_heal(project, content_hash, path)
    return deferred_read_parquet(str(path))


def entry_graph_expr(project: str, content_hash: str):
    """The entry's frozen graph — cache nodes included — on the default backend.

    The chaining primitive behind ``tracked_expr_from_alias`` /
    ``pinned_expr_from_alias`` (ADR D4). Where ``cached_result_expr`` serves a
    worthy entry as a bare read of its snapshot file (fast, but a file reference
    with no regeneration knowledge), this returns the loaded build itself: the
    entry's ``CachedNode`` stays in the graph, so a child recipe built over it
    freezes its parents' healing knowledge into its own build — a cold load of
    the child regenerates ancestor snapshots through ordinary cache mechanics,
    with no pre-heal choreography. Rebound onto the process default backend so
    it composes with ``read_project_file`` and other entries as one backend.
    """
    return _resolve_result_plan(project, content_hash).graph


# Existing callers clear the result-expr memo via cached_result_expr.cache_clear()
# (companion app, conftest, cache tests); the memo now lives on the plan resolver,
# so re-expose its cache controls on the public name.
cached_result_expr.cache_clear = _resolve_result_plan.cache_clear
cached_result_expr.cache_info = _resolve_result_plan.cache_info
