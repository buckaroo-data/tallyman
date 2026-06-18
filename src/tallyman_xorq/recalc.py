"""Recompute stale catalog entries and their dependents (#89, Stage 2).

``tallyman_xorq.staleness`` is the read-only half of the reactive consumer — it
decides an entry is stale relative to its recorded inputs but never acts. This is
the half that acts: given a set of roots, walk their descendant cone in
dependency order and replay each entry's recipe through ``build_and_persist``
(the same primitive ``catalog_revise`` uses), so a recomputed entry gets a fresh
``content_hash``, fresh ``manifest.parents`` (now resolving the *advanced* alias
head), and a fresh ``result_digest``. A followed alias is re-pointed to the new
hash; a hash-pinned child re-pins the same parent and is a no-op.

Why a plain topological replay is correct *and* idempotent:

- A recipe references a followed parent by its *alias name*
  (``from_catalog("A")``), which ``io.from_catalog`` resolves to the alias's
  **current** head at build time (``get_alias``). Re-pointing alias ``A`` to its
  recomputed hash *before* replaying ``A``'s dependents means each dependent's
  replay reads the advanced parent. Dependency order (``descendant_cone``)
  guarantees a parent is re-pointed before its children replay.
- An entry whose inputs did not move replays to the **same** ``content_hash``,
  and ``build_and_persist`` early-returns on an already-present entry dir
  (``build.py``). So recomputing the whole cone is safe: an unchanged node — and
  a hash-pinned child, whose literal-hash recipe re-resolves to the same parent —
  short-circuits to a no-op and its alias is left untouched.

Cascade-failure policy is **stop-and-report** (the plan's default, ADR Q "Cascade
failure"): a build that raises halts the walk, leaves the already-recomputed
prefix committed (recoverable via ``reset_to``), and returns a report flagging the
failure. It does **not** raise — so the one checkpoint this function takes still
commits the prefix, and an MCP/companion caller sees a structured report rather
than a 500.

``result_digest`` is *not* a recompute trigger here: an entry that recomputes to
the same ``content_hash`` but a different ``result_digest`` is *nondeterministic*,
not stale, and recompute cannot make it fresh — that is the #83/#121 path. The
report surfaces it (``action="noop"`` with the verdict) but does not loop on it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tallyman_core import aliases
from tallyman_core.entry_config import carry_forward_entry_config
from tallyman_core.paths import entry_dir, entry_manifest_path, project_dir
from tallyman_xorq.build import BuildError, build_and_persist
from tallyman_xorq.dependents import DependencyCycleError, build_dag, descendant_cone
from tallyman_xorq.portable import PLACEHOLDER
from tallyman_xorq.staleness import StaleReason, scan


@dataclass
class RecalcEntry:
    """One entry's outcome in a recalc walk.

    ``action`` vocabulary (disjoint between preview and real runs):

    - dry-run: ``"rebuild"`` (directly stale — own inputs moved),
      ``"cascade"`` (a followed parent inside the cone will advance),
      ``"unchanged"`` (pinned, or a root that isn't stale).
    - real run: ``"rebuilt"`` (replayed to a *new* hash), ``"noop"`` (replayed to
      the same hash), ``"failed"`` (the build raised; the walk stopped here),
      ``"skipped"`` (a later cone entry never reached because the walk stopped).
    """

    content_hash: str  # the entry processed (its pre-recalc hash)
    action: str
    stale: bool  # directly stale at scan time
    transitively_stale: bool
    new_hash: str | None = None  # the fresh hash when rebuilt
    alias: str | None = None  # the primary alias re-pointed (if any)
    reasons: list[StaleReason] = field(default_factory=list)
    error: str | None = None  # build error message when action == "failed"


@dataclass
class RecalcReport:
    project: str
    roots: list[str]
    cone: list[str]  # topologically ordered (parents before children)
    entries: list[RecalcEntry]
    dry_run: bool
    status: str  # "ok" | "failed" | "cycle"
    remap: dict[str, str] = field(default_factory=dict)  # old_hash -> new_hash, changed only
    checkpoint_step: int | None = None  # the step the recalc committed (real runs that changed)
    error: str | None = None  # set for status == "cycle"

    @property
    def rebuilt(self) -> list[str]:
        return [e.content_hash for e in self.entries if e.action == "rebuilt"]


def _recipe_code(project: str, content_hash: str) -> str:
    """The entry's persisted recipe (``expr.py``), with the portable placeholder
    expanded back to this machine's project root — replay-ready source, the same
    expansion ``result_cache._recipe_expr`` does for reconstruction."""
    code = (entry_dir(project, content_hash) / "expr.py").read_text()
    return code.replace(PLACEHOLDER, str(project_dir(project)))


def _aliases_heading(project: str, content_hash: str) -> list[str]:
    """Every alias whose *current* head is ``content_hash`` (usually one, never
    more for a freshly-built entry; a doubly-aliased head returns both)."""
    return sorted(name for name, latest in aliases.load_aliases(project).items() if latest == content_hash)


def _preview_action(content_hash: str, verdict, cone: set[str], dag: dict) -> str:
    """Classify a cone entry for a dry-run preview without building it."""
    if verdict.stale:
        return "rebuild"  # its own recorded inputs moved
    followed_in_cone = [p for p in dag.get(content_hash, []) if p.follow and p.hash in cone]
    if followed_in_cone:
        return "cascade"  # a followed ancestor inside the cone will advance
    return "unchanged"  # pinned to an out-of-cone parent, or a non-stale root


def recalc(project: str, roots: list[str], *, dry_run: bool = False) -> RecalcReport:
    """Recompute *roots* and their dependents, in dependency order.

    ``dry_run=True`` returns the cone with each entry's staleness verdict and the
    planned action, building nothing — the safe preview the surfaces call first.
    A real run replays each recipe, re-points followed aliases, carries entry
    config forward, and takes **one** checkpoint for the whole walk (only when a
    change actually landed), so the recalc is a single git transaction that
    ``reset_to`` undoes atomically. Stops at the first build failure (the prefix
    stays committed); never raises on a build error.
    """
    # Drop roots that name no live entry (a caller mistake, or a crashed
    # mid-build leftover dir) so the walk never indexes a missing verdict; an
    # all-bogus root set yields an empty report. Gate on the manifest, not the
    # directory: `verdicts` (and the cone) are keyed by manifest-bearing entries
    # (scan/list_entries), so a dir-without-manifest root would otherwise enter
    # the cone and KeyError the walk at verdicts[hash].
    roots = [r for r in roots if entry_manifest_path(project, r).exists()]
    try:
        cone = descendant_cone(project, roots)
    except DependencyCycleError as exc:
        return RecalcReport(project, roots, [], [], dry_run, "cycle", error=str(exc))

    # Reuse the Stage-1 scan for the direct/transitive verdicts (one pass over the
    # project), then index into it for the cone. Keeps the staleness semantics in
    # one tested place rather than re-deriving them here.
    verdicts = scan(project)
    cone_set = set(cone)

    if dry_run:
        dag = build_dag(project)
        entries = [
            RecalcEntry(
                content_hash=h,
                action=_preview_action(h, verdicts[h], cone_set, dag),
                stale=verdicts[h].stale,
                transitively_stale=verdicts[h].transitively_stale,
                alias=(_aliases_heading(project, h) or [None])[0],
                reasons=verdicts[h].reasons,
            )
            for h in cone
        ]
        return RecalcReport(project, roots, cone, entries, True, "ok")

    remap: dict[str, str] = {}
    entries: list[RecalcEntry] = []
    status = "ok"
    halted = False
    for old_hash in cone:
        verdict = verdicts[old_hash]
        if halted:
            entries.append(_entry(old_hash, "skipped", verdict, project))
            continue
        # Resolve the heading alias(es) BEFORE re-pointing anything for this entry,
        # so a yet-to-be-processed downstream alias is read at its pre-recalc head.
        heading = _aliases_heading(project, old_hash)
        try:
            result = build_and_persist(project, _recipe_code(project, old_hash))
        except BuildError as exc:
            entries.append(
                _entry(old_hash, "failed", verdict, project, alias=heading[0] if heading else None, error=str(exc))
            )
            status = "failed"
            halted = True  # stop-and-report: leave the prefix committed, stop the walk
            continue
        new_hash = result.content_hash
        if new_hash != old_hash:
            remap[old_hash] = new_hash
            for alias in heading:
                aliases.set_alias(project, alias, new_hash, expect_exists=True)
            carry_forward_entry_config(project, old_hash, new_hash)
            # Fresh hash means no stale plan is cached against it, but drop the
            # memo anyway so a downstream replay that reads this entry can't serve
            # a plan resolved against the pre-recalc head (#80/#96).
            _clear_result_plan_cache()
            entries.append(
                _entry(old_hash, "rebuilt", verdict, project, new_hash=new_hash, alias=heading[0] if heading else None)
            )
        else:
            entries.append(_entry(old_hash, "noop", verdict, project, alias=heading[0] if heading else None))

    step = _checkpoint(project, remap) if remap else None
    return RecalcReport(project, roots, cone, entries, False, status, remap=remap, checkpoint_step=step)


def _entry(content_hash, action, verdict, project, *, new_hash=None, alias=None, error=None) -> RecalcEntry:
    return RecalcEntry(
        content_hash=content_hash,
        action=action,
        stale=verdict.stale,
        transitively_stale=verdict.transitively_stale,
        new_hash=new_hash,
        alias=alias,
        reasons=verdict.reasons,
        error=error,
    )


def _clear_result_plan_cache() -> None:
    from tallyman_xorq.result_cache import cached_result_expr  # noqa: PLC0415

    cached_result_expr.cache_clear()


def _checkpoint(project: str, remap: dict[str, str]) -> int | None:
    """One checkpoint for the whole walk, so the recalc is a single git
    transaction. Both surfaces exempt recalc from their generic per-op
    checkpoint, so this is the sole writer for the operation."""
    from tallyman_core.catalog_state import checkpoint_catalog  # noqa: PLC0415

    return checkpoint_catalog(project, f"tallyman: catalog_recalc ({len(remap)} rebuilt)")
