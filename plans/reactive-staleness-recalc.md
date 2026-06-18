# Plan: Reactive consumer — staleness detection & recalc

- **Status:** Plan (2026-06-18). Implements item #2 of the reactive gap analysis
  against `plans/adr-reactive-catalog-recalc.md` (#51): the *consumer* side.
  Everything the consumer reads is already captured; nothing reads it yet.
- **Scope:** Detect that an entry is stale relative to its inputs, surface the
  stale cone, and recompute it (and its dependents) on demand. Excludes the
  trigger-policy and cascade-semantics *decisions* — those are flagged as
  decision gates below, not coded blind.
- **Affected code:** `src/tallyman_xorq/parent_capture.py` /
  `src/tallyman_core/manifest.py` (`ParentRef`, `Manifest.parents`,
  `Manifest.sources`, `Manifest.result_digest` — the captured inputs),
  `src/tallyman_core/catalog_state.py` (`read_tallyman_state`, entry
  enumeration), `src/tallyman_core/aliases.py` (`get_alias` head resolution),
  `src/tallyman_xorq/source_identity.py` (source digesting, cas clones),
  `src/tallyman_xorq/build.py` (`build_and_persist` — the recompute primitive),
  new `src/tallyman_xorq/dependents.py` and `src/tallyman_xorq/staleness.py`,
  `src/tallyman_mcp/server.py` + `src/tallyman_companion/app.py` (surfaces).

## What remains — sequenced (verified against the tree 2026-06-18)

The substrate is essentially complete. Every reactive prerequisite in epic #89
is closed except the two open PRs below, and the worst substrate holes the
consumer would have inherited — #75 (multi-parent backends), #76 (by-path
snapshot pin), #97 (0-row parent), #79 (concurrent materialise race), #81
(scalar-UDF worthiness), #83 (deferred-nondeterminism faithfulness), #85
(alias-history disambiguation) — are all **closed**, as is #82 (deep-chain cost,
the stage-3 measurement gate, landed in PR #122). What is left is small and
ordered:

0. **Land the two open prerequisite PRs.** Both are green and rebase clean on
   current main.
   - **#124** (#80/#96) — invalidates `_resolve_result_plan` and the companion's
     `_build_compare_expr` LRUs on reset. *Load-bearing for this plan:* Stage 2
     step 4 needs the `cache_clear`-on-reset hook, and `_build_compare_expr` has
     no per-call self-heal, so a recalc that prunes a snapshot then reads would
     otherwise execute over a deleted parquet (the #73/#74 symptom). Merge first.
   - **#121** (#88 part 2) — runtime structural-vs-execution drift attribution,
     so recipe nondeterminism is not misread as staleness. Completes the
     determinism prerequisite.

1. **Close #115 first — the one genuine hard blocker.** The #86 cas flip made
   *build* identity content-aware but did **not** make *cold reconstruction*
   content-faithful: `from_project` (`io.py:55-60`) re-digests the **live** file
   on every cold read and ignores both the recorded `manifest.sources` digest and
   the frozen `.cas/<digest>` clone, even while `_RECONSTRUCTING` is set. So a
   cold read of a not-yet-recalced downstream entry serves edited bytes under its
   *original* `content_hash` — the "this hash ⇒ these bytes" premise this whole
   consumer rests on is false the moment a source is edited in place. Both
   staleness axes and the recomputed result are untrustworthy until this lands.
   Fix: thread the reconstructing entry's recorded `sources` into a contextvar and
   resolve `from_project` to `.cas/<digest>` (not `digest_for(live)`) during
   reconstruction; pin with the assertion #86 omitted (cold read after an
   in-place edit returns the *original* rows).

2. **Build the consumer (this plan), Stages 0→3** — with the corrections under
   "Corrections found during verification" below applied.

3. **Answer the decision gates** (see "Open decisions") — confirm the recommended
   defaults, don't default silently.

**Land-with quality items** (fold each into the stage that touches the same code
path; none blocks correctness): #111 (`set_alias` is repointed before
`carry_forward_entry_config` — recalc's re-point adds a third caller of that
sequence), #117 (scalar-UDF snapshot key is non-deterministic — recalc re-bakes
on replay so it is noise on the determinism axis, not a recompute blocker), #118
(concurrent `.execute()` on the shared default backend raises "Already borrowed"
— the serial recalc walk doesn't self-collide, but a recalc execute racing a
user read needs a process-wide execute lock before the in-companion Stage-3
surfaces are reliable).

**Deferred / out of scope:** #125 (escalate #88 to a build-time `BuildError` —
optional; this plan's separate `nondeterministic` flag already segregates those
entries), #77 (viewer empty grid on a cross-machine clone), and the
archive/delete-cone/multi-parent-unbind semantics (ADR Q3/Q4 — deletion work,
not the revise/source-edit staleness this plan handles). #98 appears obsolete
(the `xorq_catalog.py` yaml round-trip it depends on was removed in the native
recipe-store cut) — verify and close.

**Corrections found during verification (apply when implementing):**

- **Entry enumeration:** `read_tallyman_state(project)["entry_hashes"]` lists only
  **checkpointed** entries (from `entries.jsonl`); a freshly built,
  not-yet-checkpointed entry is on disk but absent. Enumerate via
  `build.list_entries(project)` (reads every `manifest.json`) for a complete scan.
- **Source axis:** `source_identity.digest_for` is **stat-memoized** (keyed on
  mtime_ns/size/inode in `artifacts/source_digests.json`); a same-stat content
  swap returns the stale digest. Set `TALLYMAN_SOURCE_REHASH=1` for a sound
  staleness check, or the source-drift axis can false-negative.
- **Cache invalidation:** the public clear is `cached_result_expr.cache_clear()`
  (the alias of `_resolve_result_plan.cache_clear`, `result_cache.py:479`), not a
  module-level `result_cache.cache_clear`.
- **Recompute primitive:** `build_and_persist(project, code, ...)` takes recipe
  **source text**, not an expr or a hash, and does **not** set the alias or
  checkpoint — the recalc caller reads the entry's `expr.py`, replays it, then
  re-points the alias and checkpoints. Replay yields *advanced* parents only when
  the recipe's `from_catalog` argument is the alias **name** (follow=True); a
  literal-hash recipe re-pins the same parent (consistent with pinned-child
  handling).
- **Stale code refs to clean up while here:** `io.py:83` (the `from_catalog`
  docstring) and `parent_capture.py:9` still name `lineage.catalog_parents`,
  deleted in #76; `source_identity.py:16` docstring still says `off (default)`
  though the code default is `cas`.

## Why this is the biggest piece

The substrate shipped (#73/#74/#103/#104/#84/#83/#88) and source-identity now
defaults to `cas` (`source_identity.py:53`). Parent edges are *recorded*
(`manifest.parents`), source digests are *recorded* (`manifest.sources`), and the
executed-bytes digest is *recorded* (`manifest.result_digest`). But **nothing
consumes any of it**: no code compares a recorded parent hash against the current
alias head, nothing compares a recorded source digest against the file on disk,
and `from_catalog`'s docstring (`io.py:83`) still points at a
`lineage.catalog_parents` reader that #76 deleted. The reactive promise — "edit a
source or revise an upstream entry and dependents recompute instead of going
silently stale" — is entirely unbuilt on the read side. This plan builds it.

## Prerequisite (item #1): a manifest-driven dependents reader

#76 deleted `lineage.py` (`catalog_parents` / `catalog_dag`) because its only
consumer was the removed lineage views. The edges survive in `manifest.parents`;
the *reader* does not. Rebuild a thin one — this is not the old column-lineage
feature, just the DAG walk the consumer needs.

New `src/tallyman_xorq/dependents.py`:

- `parents_of(project, content_hash) -> list[ParentRef]` — read
  `manifest.parents` for one entry (None → `[]`).
- `build_dag(project) -> {child_hash: list[ParentRef]}` — enumerate every live
  entry hash via `build.list_entries(project)` (reads every `manifest.json`) and
  read each manifest's `parents`. Forward edges. (Do **not** enumerate from
  `read_tallyman_state(project)["entry_hashes"]` — that lists only *checkpointed*
  entries and misses a freshly built, not-yet-checkpointed one.)
- `dependents_index(project) -> {parent_hash: set[child_hash]}` — invert
  `build_dag`. This is the reverse index the recalc walk needs; it does not exist
  anywhere today.
- `descendant_cone(project, root_hashes) -> ordered list[hash]` — BFS over the
  reverse index, topologically ordered (parents before children) so a recalc
  rebuilds in dependency order. Detect and report cycles rather than looping
  (a `from_catalog`-of-own-alias recipe is already guarded at build, `io.py:100`,
  but the reader must not assume acyclicity).

Tests: a 3-level chain (A→B→C), a diamond (A→B, A→C, B+C→D), a pinned edge
(`follow=False`) and a followed edge (`follow=True`) both appearing in the cone,
and a cycle surfacing as an error not a hang.

## Stage 1 — staleness predicates (read-only, no recompute)

New `src/tallyman_xorq/staleness.py`. An entry is **stale** when any input it was
built against no longer matches the current world. Two independent axes, both
computable from already-recorded data:

1. **Upstream advanced (followed alias).** For each `ParentRef` with
   `follow=True`: resolve the alias head now via `aliases.get_alias(project,
   ref)`. Stale iff `head != parent.hash`. A `follow=False` (hash-pinned) parent
   is *never* stale on this axis by definition (`parent_capture.py:16-19`).
2. **Source drifted.** For each `(rel_path, md5)` in `manifest.sources`:
   recompute the current source digest with the same helper `source_identity`
   uses at build (`digest_for`, via `io.project_path(rel_path, project)`), compare
   to the recorded `md5`. Stale iff they differ. Set `TALLYMAN_SOURCE_REHASH=1`
   first — `digest_for` is stat-memoized, so a same-stat in-place content swap
   otherwise returns the cached digest and the axis false-negatives. Under `cas`
   the recorded digest is the frozen-clone digest, so this is
   a faithful "the file the user pointed at changed" signal; under `off`
   (`manifest.sources is None`) this axis is unavailable — report `unknown`, not
   `fresh` (see Open decision: off-mode).

`entry_staleness(project, hash) -> StaleVerdict` returns
`{stale: bool, reasons: [{axis, ref|path, was, now}], unknown_axes: [...]}`.

`scan(project) -> {hash: StaleVerdict}` runs it over every live entry. A
**directly** stale entry (its own inputs moved) is distinguished from a
**transitively** stale entry (a stale ancestor in `dependents.descendant_cone`);
the scan marks both but tags which. This is the "scan-on-load that flags
staleness" the ADR's trigger discussion leans toward.

`result_digest` is *not* a staleness input — it is the soundness check #83
already wires at self-heal. The scan should surface a separate
`nondeterministic` flag (recorded `result_digest` present, recompute differs)
distinct from `stale`, because that entry can't be made fresh by recompute and
needs the #83/#30 path, not this one.

Tests (failing-first, per the TDD rule): build A, alias it, build B
`from_catalog("A")`, re-revise A so the alias advances → `entry_staleness(B)`
reports stale on the followed-alias axis with `was=oldhash now=newhash`. Build
with a literal hash → not stale after A advances. cas mode: mutate the underlying
source file → stale on the source axis; off mode: same mutation → `unknown`.

## Stage 2 — recalc action (the consumer that acts)

`recalc(project, roots, *, dry_run) -> RecalcReport`:

1. `cone = descendant_cone(project, roots)` — topologically ordered.
2. `dry_run=True` returns the cone with each entry's `StaleVerdict` and the
   recompute plan (what would rebuild, in order) **without building**. This is
   the "preview the cone" the ADR Q4 calls for; it is also the safe default the
   surfaces should call first.
3. For a real run, walk the cone in order and rebuild each entry by replaying its
   recipe through `build.build_and_persist` — the same primitive `catalog_revise`
   uses — so a recomputed entry gets a fresh `content_hash`, fresh
   `manifest.parents` (now resolving the *advanced* alias head), and a fresh
   `result_digest`. Re-point the followed alias to the new hash via
   `aliases.set_alias`. A hash-pinned child is left on its pin (it asked for that
   revision).
4. **Invalidate the in-process cache** before/after each rebuild: call
   `cached_result_expr.cache_clear()` (the alias of
   `_resolve_result_plan.cache_clear`, `result_cache.py:479`). PR #124 already
   wires this into `reset_to`, so a checkpoint-wrapped recalc inherits it; the
   explicit call covers a rebuild that reads before the wrapping checkpoint
   commits. This is the coupling to #80/#96 — see Dependencies.
5. Wrap the whole walk in one checkpoint (`catalog_state.checkpoint_catalog`) so a
   recalc is one git transaction and `reset_to` undoes it atomically.

The cascade-failure behavior (what to do when entry k of the cone fails to
rebuild) is a **decision gate** — do not pick silently. Default the
implementation to **stop-and-report** (leave already-rebuilt entries committed,
halt at the failure, return the report) because it is the least surprising and is
trivially recoverable via `reset_to`; flag the all-or-nothing and
skip-and-continue alternatives in the report for the decision.

## Stage 3 — surfaces

- **MCP** (`tallyman_mcp/server.py`): `catalog_scan_staleness(project)` →
  the scan map; `catalog_recalc(project, roots, dry_run)` → the report. dry_run
  defaults true.
- **Companion** (`tallyman_companion/app.py`): a staleness badge per entry in the
  catalog sidebar driven by the scan, and a "recalc" affordance that previews the
  cone (dry run) then commits. Scan-on-load wiring fires the scan when a project
  is switched in / loaded.
- Keep the surfaces thin: all logic lives in `staleness.py` / `dependents.py` so
  both MCP and companion call the same code.

## Open decisions (gate before coding the stage they touch)

These are genuinely undecided in #51 and must be answered, not defaulted:

- **Trigger model** — button vs scan-on-load vs auto-cascade-on-revise. Plan
  assumes *scan-on-load (passive flag) + explicit recalc button*; auto-cascade is
  out unless chosen. Gates Stage 3.
- **Cascade failure** — stop / skip-and-continue / all-or-nothing. Plan defaults
  to stop-and-report. Gates Stage 2 step 4.
- **off-mode source axis** — report `unknown` (plan's choice) or force a one-time
  digest backfill so the axis becomes available. Now that `cas` is the default,
  off is opt-out; `unknown` is probably fine. Gates Stage 1 axis 2.
- **Pinned-child-of-advanced-alias** — confirm a `follow=False` child stays put on
  recalc (plan assumes yes; it asked for that revision). Gates Stage 2 step 3.
- **Multi-parent / archive semantics** (ADR Q3/Q4 — unbind, archive overlay,
  delete cone) — *out of scope here*; this plan handles revise/source-edit
  staleness, not deletion. Cross-link when that work starts.

## Dependencies on open issues

- **#115 (OPEN — hard blocker, land before Stage 1).** Cold reconstruction is not
  content-faithful: `from_project` re-digests the live file and ignores the
  recorded `manifest.sources` digest / `.cas` clone during reconstruction, so an
  edited-in-place source is served under the entry's original `content_hash`. The
  staleness scan's "hash ⇒ bytes" premise and a recompute that reads upstream
  bytes are both unsound until this is fixed. See "What remains" step 1.
- **#80 / #96 (FIXED by PR #124, pending merge).** `reset_to` and a compute-cache
  prune did not invalidate the `_resolve_result_plan` / `_build_compare_expr`
  LRUs, so a recalc that rebuilds then reads could serve a pruned snapshot path.
  Stage 2 step 4 *needs* the `cache_clear` hook; PR #124 wires it into `reset_to`
  and the companion reset paths. Merge before the recalc action lands.
- **#82 (CLOSED — PR #122).** Deep-chain build/reconstruct cost was measured
  (the stage-3 gate). Recalc replays recipes through the cone; the measurement
  bounds how interactive a large cone's recalc is and whether more baking
  boundaries are warranted, but it no longer gates the design.

## Staging / TDD order (per global rules)

0. Merge PR #124 (#80/#96) and PR #121 (#88); then fix #115 (digest-pinned
   reconstruction) — failing test (cold read after in-place edit returns original
   rows) then fix. These precede the consumer.
1. Dependents reader + staleness predicates with **failing** tests committed and
   seen red on CI (alias-advance, source-drift, pinned-stays, off=unknown, cycle).
2. Then the reader + `staleness.py` implementation; watch CI go green.
3. `recalc` (dry-run first, then real) + report — failing test then fix.
4. MCP + companion surfaces last, once the trigger/cascade decisions are made,
   and after the #118 execute-lock if the recalc runs in the companion process.

Each test bundle is one commit, separate from its fix, never bundled with it.
