"""Reading an entry's result: the canonical read (#163), over tallyman's own materialization.

The entry's frozen build (``xorq_build/``) is the source of truth, and **every read loads it** —
``ensure_expanded_build`` → ``load_expr``. This is the canonical read of ``docs/system-contract.md``: a content hash
names the fixed result its build freezes, so reads never re-import ``expr.py`` — recipe re-execution happens only at
minting (build / revise / recalc) and in the structural-nondeterminism diagnostic below. A missing or unloadable build
is a hard error (ADR-006 D6), never a fallback.

There is no xorq cache node in any build (ADR-007 D1). Whether an entry has a file of its own is one recorded fact,
``manifest.cache_worthy``, decided once at build by ``worthiness.classify_expr`` (ADR-008 D4):

  * **Worthy** entries do work that is expensive or that cannot inherit a row order (an aggregate, join, sort, window
    function, UDF, union). Tallyman materializes them: ``materialize`` writes
    ``compute_cache/result_cache/<hash>.parquet`` when the entry is created, and every read is a bare read of that
    file. A file that is missing is made again and checked against the recorded digest by ``ensure_materialized``
    before anything reads it (ADR-007 D5).
  * **Cheap** entries are row-preserving over one file (a filter, a selection, a computed column). They write nothing;
    reading one runs its small frozen plan over files that exist.

Every consumer (the viewer, paginated reads, diffs, post-processing, chaining a child recipe) reads
``cached_result_expr``. (Buckaroo summary stats are cached separately, in each entry's ``.buckaroo_stat_cache``: always
on, orthogonal to this decision.)
"""

from __future__ import annotations

import contextvars
import functools
import logging
import sys
import time
from pathlib import Path
from typing import NamedTuple

# Perf instrumentation rides a dedicated child namespace so it can be dialed up
# independently of the rest of tallyman's logging (#60), via TALLYMAN_LOG_LEVEL.
perf_log = logging.getLogger("tallyman.perf")


def cache_worthy(project: str, content_hash: str) -> bool:
    """Whether the entry is materialized, read from its manifest: the verdict recorded at build (ADR-008 D4).

    Nothing re-derives it: the manifest is the record, and ``expr.yaml`` is never parsed to work it out.
    """
    from tallyman_core import read_manifest
    from tallyman_core.paths import entry_dir

    return bool(read_manifest(entry_dir(project, content_hash)).cache_worthy)


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


def load_entry_expr(project: str, content_hash: str):
    """Load the entry's frozen build as a live expression — the canonical read (#163).

    ``ensure_expanded_build`` → ``load_expr``. No cache directory is supplied and none is needed (ADR-007 D7): a build
    holds no cache node, so nothing in it resolves through one. The build binds by value — sources as content-pinned
    ordered copies, a worthy parent as a bare read of its snapshot, a cheap parent's graph inlined — so the returned
    expression is the entry's fixed computation regardless of where alias heads sit today.

    A missing or unloadable build raises ``BuildError`` naming the entry and the remedy (ADR-006 D6). There is no
    recipe fallback: ``expr.py`` binds by name and re-executing it is how #163's lineage drift happened.
    """
    from xorq.ibis_yaml.compiler import load_expr

    from tallyman_core.paths import entry_build_dir, entry_expanded_build_dir, project_dir
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.portable import ensure_expanded_build

    build_dir = entry_build_dir(project, content_hash)
    if not (build_dir / "expr.yaml").is_file():
        raise BuildError(
            f"entry {content_hash} in {project!r} has no loadable xorq_build/ "
            f"(expected {build_dir / 'expr.yaml'}). The frozen build is the read "
            "path's only source of truth — rebuild the entry (catalog_revise / "
            "catalog_recalc) to restore it; reads never fall back to expr.py."
        )
    try:
        expanded = ensure_expanded_build(
            build_dir, project_dir(project), entry_expanded_build_dir(project, content_hash)
        )
        return load_expr(expanded)
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


def rebind_onto(expr, target):
    """Collapse every backend in a loaded build onto *target*.

    ``load_expr`` mints fresh backend objects per profile, so two loaded builds — or a loaded build and a recipe's
    ``read_project_file`` — span distinct backend objects and composition raises "Multiple backends found". The
    contract allows the collapse because every profile in a tallyman build is content-identical no-arg
    ``xorq_datafusion``; that assumption is enforced here (ADR-006 D3): more than one distinct content profile fails
    loudly rather than misbinding a node onto the wrong kind of connection. A raw ``DatabaseTable`` (bundled data that
    would need a copy) also fails loudly — ``replace_sources`` raises unless told to transfer, and tallyman builds must
    never contain one (in-memory reads are rejected at build).

    The *target* is the process default backend for reads and composition, and the single-partition connection when an
    entry is materialized (ADR-009 D1): a loaded build ignores a connection it was never bound to.
    """
    from xorq.common.utils.graph_utils import find_all_sources, replace_sources

    others = [s for s in find_all_sources(expr) if s is not target]
    if not others:
        return expr
    tokens = {_profile_content_token(s) for s in (*others, target)}
    if len(tokens) > 1:
        from tallyman_xorq.build import BuildError

        raise BuildError(
            f"loaded build spans {len(tokens)} distinct backend content profiles; "
            "rebinding onto one backend would misbind — every profile in a "
            "tallyman build must be content-identical no-arg xorq_datafusion (ADR-006 D3)"
        )
    return replace_sources({id(s): target for s in others}, expr)


def _rebind_to_default_backend(expr):
    """Collapse every backend in a loaded build onto the process default backend (ADR-006 D3)."""
    from xorq.config import default_backend

    return rebind_onto(expr, default_backend())


def baked_snapshot_path(project: str, content_hash: str) -> Path | None:
    """Path of the entry's snapshot, or None for a cheap entry, which has none.

    A function of the content hash and the manifest's ``cache_worthy`` (ADR-007 D2), so it loads nothing.
    """
    from tallyman_xorq.materialize import snapshot_path

    return snapshot_path(project, content_hash) if cache_worthy(project, content_hash) else None


def snapshot_file_digest(path: Path) -> str:
    """The content digest of a snapshot parquet file: ``arrow-sha256:<hex>`` (ADR-009 D2).

    A SHA-256 over the file's ordered Arrow data, read back, so it does not depend on the row-group size, the codec,
    the writer's version or how the rows were batched. Cheap entries record no digest (they have no snapshot).
    """
    from tallyman_xorq.digest import content_digest

    return content_digest(Path(path))


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
    """Whether the entry's snapshot on disk still has its recorded ``result_digest``.

    Returns True when the file's content digest matches, False on drift, and None when there is nothing to check (no
    recorded digest, which a cheap entry has, or no snapshot on disk). It reads and never writes (ADR-007 D12): a
    snapshot that is missing is checked at the moment it is next made, since every file ``ensure_materialized`` writes
    is verified before it is served.
    """
    from tallyman_xorq.materialize import snapshot_path

    recorded = _recorded_result_digest(project, content_hash)
    if not recorded:
        return None
    snap = snapshot_path(project, content_hash)
    if not snap.exists():
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
    # The manifest's worthiness reason carries "udf:<names>" when the graph holds a UDF (#81). Best-effort, like
    # _reconstructed_hash below: this predicate runs on the self-heal warning path, so a fault here must degrade to
    # "not structural" — the conservative execution (#83) attribution — never break the read it only annotates.
    try:
        from tallyman_core import read_manifest
        from tallyman_core.paths import entry_dir

        why = read_manifest(entry_dir(project, content_hash)).cache_worthy_why or ""
    except Exception:
        return False
    if "udf:" in why:
        return False

    a = _reconstructed_hash(project, content_hash)
    b = _reconstructed_hash(project, content_hash)
    return a is not None and b is not None and a != b


# Called (project, content_hash) after an UNFAITHFUL self-heal, best-effort.
# The companion registers a hook that forces Buckaroo to reload the entry's grid and pushes the SSE badge event
# (ADR-007 D6); processes without an SSE bus (the MCP server) still get the durable errors.jsonl record written below.
UNFAITHFUL_HEAL_HOOKS: list = []


def _engine_change(project: str, content_hash: str) -> str | None:
    """A sentence naming what changed when the engine versions differ from the ones recorded at build (ADR-009 D4)."""
    from tallyman_core import read_manifest
    from tallyman_core.paths import entry_dir
    from tallyman_xorq.materialize import SNAPSHOT_FORMAT_VERSION, engine_versions

    try:
        manifest = read_manifest(entry_dir(project, content_hash))
    except (OSError, ValueError):
        return None
    changes = []
    recorded = manifest.engine_versions or {}
    for name, now in engine_versions().items():
        was = recorded.get(name)
        if was is not None and was != now:
            changes.append(f"{name} {was} -> {now}")
    if manifest.snapshot_format is not None and manifest.snapshot_format != SNAPSHOT_FORMAT_VERSION:
        changes.append(f"snapshot format {manifest.snapshot_format} -> {SNAPSHOT_FORMAT_VERSION}")
    return ", ".join(changes) or None


def _verify_self_heal(project: str, content_hash: str, actual: str) -> None:
    """Verify a just-repopulated snapshot's content digest against the build-time digest (ADR-007 D5, ADR-006 D7).

    Eviction's load-bearing assumption is that an evicted snapshot recomputes to what was evicted. A faithful heal
    has the recorded digest; a mismatch means the entry's recompute changed and the heal just manufactured different
    rows under the entry's recorded hash. The read is still served — the bytes are the honest output of the frozen
    build — but never silently:

      * a ``tallyman.perf`` UNFAITHFUL warning, attributing the change (ADR-009 D4): the engine (a version recorded
        at build differs from today's), the recipe's graph moving (#88), or a fixed graph that runs differently (#83);
      * a durable ``errors.jsonl`` record (``code="unfaithful_heal"``) — the UI badge's source, and the pin (the entry's
        bytes are not regenerable, so the Cache page's delete leaves its file alone);
      * the entry's ``.buckaroo_stat_cache`` is wiped (ADR-006 D10): Buckaroo's summary stats key on expression
        structure and stable paths (buckaroo#955), so stale stats would render beside the fresh rows;
      * registered hooks fire (companion: a forced reload of the open grid, and the SSE event).
    """
    recorded = _recorded_result_digest(project, content_hash)
    if not recorded or actual == recorded:
        return
    engine = _engine_change(project, content_hash)
    if engine:
        kind = (
            f"the engine changed since the entry was built ({engine}), so its result may differ. "
            "Rebuild the entry; the recipe is not implicated"
        )
    elif recipe_is_structurally_nondeterministic(project, content_hash):
        kind = "structural (#88) — the recipe bakes a nondeterministic literal that re-derives a different graph hash"
    else:
        kind = "execution (#83) — a fixed graph that runs differently each execute, or source drift under off"
    perf_log.warning(
        "ensure_materialized self-heal %s: UNFAITHFUL recompute [%s] — result "
        "digest %s != recorded %s; eviction self-healed it to different rows "
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
                f"self-heal produced different rows than were built [{kind}]: "
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

    ``loaded`` is the build as ``load_entry_expr`` returned it — per-load backend objects. ``graph`` is the same graph
    rebound onto the process default backend (ADR-006 D3) — the composable form a cheap read serves and chaining
    inlines. ``reads`` is every file the plan's ``Read`` nodes point at, which ``ensure_materialized`` checks exist
    before anything executes (ADR-007 D5).
    """

    loaded: object
    graph: object
    reads: tuple[Path, ...]


def _read_paths(expr) -> tuple[Path, ...]:
    """Every file the expression's ``Read`` nodes point at, in graph order and without repeats."""
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.expr.relations import Read

    seen: dict[Path, None] = {}
    for node in walk_nodes(Read, expr):
        path = dict(node.read_kwargs).get("hash_path")
        if path:
            seen[Path(str(path))] = None
    return tuple(seen)


@functools.lru_cache(maxsize=256)
def _resolve_result_plan(project: str, content_hash: str) -> _ResultPlan:
    """The expensive, memoisable half of ``cached_result_expr``: load the entry's frozen build and collect what it
    reads. The LRU is sound because its key finally determines its value — the build is immutable and the plan is a
    pure function of it. Whether each file exists is checked on every call by ``ensure_materialized``, since existence
    is the one input that remains mutable.
    """
    t0 = time.monotonic()
    loaded = load_entry_expr(project, content_hash)
    plan = _ResultPlan(loaded, _rebind_to_default_backend(loaded), _read_paths(loaded))
    perf_log.debug(
        "cached_result_expr cold read %s: reads=%d wall_ms=%.1f",
        content_hash,
        len(plan.reads),
        (time.monotonic() - t0) * 1000,
    )
    return plan


@functools.lru_cache(maxsize=1024)
def _snapshot_read(project: str, content_hash: str):
    """One bare read of the entry's snapshot, memoised for the life of the process (ADR-007 D2).

    A read has one table name, so repeated reads stop piling up tables in the shared backend. xorq still registers a
    deferred read's table on every execute, so the footer is opened once per query, which the snapshot format keeps
    small.
    """
    from xorq.expr.api import deferred_read_parquet

    from tallyman_xorq.materialize import snapshot_path

    return deferred_read_parquet(str(snapshot_path(project, content_hash)))


def cached_result_expr(project: str, content_hash: str):
    """The entry's result as a single-backend expression on the default backend.

    Every file the read needs is made to exist first (``ensure_materialized``, ADR-007 D5), so nothing executes over a
    missing file.

      * Worthy entry: ONE bare read of its snapshot (``_snapshot_read``, memoised), served without loading the entry's
        build when the file exists. Composing it into a child recipe makes the child's identity a function of the
        parent's, since the path carries the parent's content hash (ADR-007 D3).
      * Cheap entry: the loaded build's graph, rebound onto the default backend (ADR-006 D3). Its small plan re-runs on
        every read over files that exist, and the graph is frozen, so the rows are the entry's recorded result and
        never today's alias heads.
    """
    from tallyman_xorq.materialize import _ensure

    if _ensure(project, content_hash):
        return _snapshot_read(project, content_hash)
    return _resolve_result_plan(project, content_hash).graph


def preload_plan(project: str, content_hash: str) -> None:
    """Load an entry's frozen build into the in-process memo. Writes nothing (ADR-007 D12).

    The startup warm-up uses it, so the first page request of a cheap entry does not pay for the load. It does not call
    ``ensure_materialized``, and it needs no file to exist: loading a build does not open the files it reads.
    """
    _resolve_result_plan(project, content_hash)


def _clear_read_memos() -> None:
    _resolve_result_plan.cache_clear()
    _snapshot_read.cache_clear()


# Existing callers clear the result memo via cached_result_expr.cache_clear() (companion app, conftest, reset, recalc,
# tests); the memos now live on the plan resolver and the snapshot read, so re-expose their cache controls on the public
# name.
cached_result_expr.cache_clear = _clear_read_memos
cached_result_expr.cache_info = _resolve_result_plan.cache_info
