# ADR: Reactive catalog — recalc & staleness over the dependency DAG

- **Status:** Research snapshot (2026-06-14), reconciled with PR #74 (merged
  2026-06-16) and PRs #103 / #104 (merged 2026-06-17) — see the two
  "Reconciliation with PR …" sections below. #74 replaced the chaining/caching
  substrate this ADR planned on top of: it dropped the blanket per-entry
  `result.parquet` and materializes only at expensive boundaries (the model the
  author intended all along), which supersedes the tagged-read spine (findings
  #6/#7) and takes cross-entry lineage dark. #104 then removed the last
  *on-demand* `result.parquet` layer (`ensure_result` and its consumers), so the
  baked `.cache()` snapshot is the only materialized copy and `cached_result_expr`
  is the sole read path. #103 dropped xorq's catalog package for a native
  tallyman store, dissolving the #48/#52 bug cluster by construction and
  decomposing `catalog.yaml` into per-concern tracked files. Separately, the
  manifest `parents` edge this ADR called for as a prerequisite already shipped in
  #84 (merged via PR #91, before #103): the cross-entry DAG is captured and read
  today. The reactive intent and the still-open design questions
  (cascade/transaction semantics, archive overlay, trigger model) stand; findings
  are annotated inline where #74, #84, #103, or #104 overtook them.
- **Context:** A tallyman project has a few raw sources and many derived
  expressions. Today, edits to a raw source or updates to an upstream entry do
  not reach the dependents — they go silently stale. We want an on-demand
  "recalc" (and/or a scan-on-load that flags staleness) without giving up the
  catalog's reproducible, recoverable, git-backed nature.
- **Affected code:** `src/tallyman_xorq/io.py` (`from_catalog`, io.py:63 —
  composes the parent via `cached_result_expr` and records the edge with
  `parent_capture.note_parent`, io.py:111),
  `src/tallyman_xorq/result_cache.py` (`cached_result_expr` at result_cache.py:273,
  its memoized resolver `_resolve_result_plan` at :232, `baked_snapshot_path` at
  :211, `_resolve_noncyclic_hash` at :117 — `ensure_result` was deleted by #104)
  and `src/tallyman_xorq/source_cache.py` (`rewrite_for_build`, source_cache.py:82)
  — both added by #74 — `src/tallyman_xorq/parent_capture.py` (the #84 parent-edge
  collector), `src/tallyman_core/manifest.py` (`ParentRef` + `Manifest.parents`,
  manifest.py:16/:56), `src/tallyman_xorq/lineage.py` (`catalog_parents` /
  `catalog_dag`, now manifest-driven, lineage.py:112),
  `src/tallyman_xorq/source_identity.py`, `src/tallyman_core/aliases.py`
  (`previous_version` at aliases.py:110, now `aliases.jsonl`-backed),
  `src/tallyman_companion/app.py` (checkpoint middleware).
- **Related:** buckaroo-data/tallyman#48 (catalog-consistency bug, a
  prerequisite for the recovery model — closed by PR #57, then the whole
  constraint eliminated by PR #103, which dropped xorq's catalog package), #73/#74
  (the source-cache + baked-result rewrite that replaced this ADR's chaining
  substrate — see Reconciliation), #101/#104 (removed the on-demand
  `result.parquet` layer), #52/#103 (native `tallyman_core.catalog` store),
  #84 (manifest `parents` edge — restores the cross-entry DAG, merged via PR #91),
  #82 (deep-chain build-cost measurement), #83 (execution-nondeterminism
  soundness), #80 (in-process LRU invalidation on reset),
  `plans/native-catalog-store.md` (the PR #103 design + investigation record),
  `plans/adr-source-identity-content-hash.md`,
  `plans/adr-result-cache-cost-rubric.md`.
- **xorq evidence:** read at xorq 0.3.29 (commit `1f46392e`), under
  `~/xorq/python/xorq`.

## Intent

The author's framing, paraphrased: a notebook is a handful of raw sources and
many dependent expressions. Two properties make derived work go stale without
anyone noticing.

1. **Alias dependencies are by-value.** `from_catalog("A")` resolves alias A to
   its *current* hash at build time and binds that resolved version. When A is
   revised, B keeps reading A's old version. B depends on "the value A had when B
   was built," not on "alias A." *[#74/#104: the binding is no longer an embedded
   `result.parquet` read — `from_catalog` (io.py:63) composes A's expression via
   `cached_result_expr` and records the resolved `{hash, ref, follow}` in
   `manifest.parents` (#84). The by-value staleness diagnosis is unchanged; only
   the mechanism the value rides on moved from a parquet path to a recorded hash.]*

2. **xorq invalidates lazily, with no propagation.** A cache key is recomputed
   only when an expression executes, by walking to its Read nodes; there is no
   "this source changed → invalidate its dependents" mechanism anywhere.
   Combined with tallyman's deliberate choice of `SnapshotStrategy` (path-only
   keys), an in-place source edit is invisible to identity.

The goal is to recompute the dependents of a changed source or an updated entry
on demand, while keeping each entry a reproducible, recoverable artifact.

**Guiding principle (stated 2026-06-14):** use xorq natively wherever possible;
patch in a tallyman-specific solution only when (1) xorq offers no way to do the
thing, or (2) xorq's way is too slow for reasonable interactive use.

## Reconciliation with PR #74 (merged 2026-06-16)

This ADR was written believing the per-entry `result.parquet` was a deliberate
truncation boundary worth preserving. That was a misread of the codebase. The
blanket model materialized *every* entry's result out of band regardless of
cost; the author had not realized that, and only ever wanted materialization at
expensive boundaries. Read correctly, the design goal was never "keep the parquet
read and tag it" — it was "stop materializing cheap steps." PR #74 implements
exactly that, so it supersedes this ADR's chaining proposal rather than refining
it.

**What #74 changed.** `result.parquet` is no longer written at build. A
`rewrite_for_build` step (`source_cache.py`) instead (1) caches each non-parquet
source read (`read_csv`/`read_json`) once, shared across entries, and (2) bakes a
top-level result cache into the durable recipe only for *expensive* entries
(`cache_worthy` — Aggregate/Join/Sort/window/UDF). Cheap entries materialize
nothing. `from_catalog` (`io.py:63`) stopped reading `result.parquet` and now
reconstructs the parent as an expression on the in-process default backend
(`cached_result_expr`, `result_cache.py:273`): an expensive parent resolves to a
bare read of its baked snapshot, a cheap parent re-runs its recipe (pushdown
makes that ~free). At #74's merge `result.parquet` still survived as a
regenerable on-demand artifact (`ensure_result`) for the few callers that needed
a parquet *path* — downloads, promote-diff, post-processing.
*[Superseded by #104: that on-demand layer is gone too. `ensure_result` and the
path helpers (`ENTRY_RESULT_FILENAME`, `entry_result_path`, the manifest
`result_path` field) were deleted; every caller now routes through
`cached_result_expr`. No per-entry `result.parquet` is written at build or on
demand. See "Reconciliation with PR #104" below.]*

The boundary #74 uses — structural `cache_worthy` (contains
Aggregate/Join/Sort/window/UDF) — is a stopgap, not a settled definition of
"expensive." Whether it should become the measured value-per-byte rubric of
`plans/adr-result-cache-cost-rubric.md` (#30) is decided empirically: instrument
both verdicts, watch where they disagree (a Join-descendant computed column is
structurally worthy but cheap to recompute), and flip when the data says so. The
materialization boundary is policy, not a fixed `cache_worthy`.

**What this supersedes here.**
- Findings #6 and #7's tagged read of `result.parquet` — there is no per-entry
  `result.parquet` read left to tag. The verified-findings note that this spine
  "works" still holds in isolation; it was simply not the path taken.
- Recovering cross-entry edges via `walk_nodes(HashingTag)`. #74 took
  `catalog_parents` dark: composing the parent's expression leaves no
  `result.parquet` path in `expr.yaml`, so the catalog DAG was momentarily a
  forest of single-node trees. But going dark was a #74 scope decision, not a
  design stance, and the edge is not lost. The fix this ADR proposed — record the
  resolved parent hash in the entry's manifest (a `parents` field, parallel to
  `sources`) and have `catalog_parents` read the manifest instead of regex-matching
  paths — *shipped in #84 (merged via PR #91, ahead of #103)*: `from_catalog`
  resolves the parent hash at build and `parent_capture.note_parent` records
  `{hash, ref, follow}` (io.py:111), `build_and_persist` persists it into
  `Manifest.parents` (`ParentRef`, manifest.py:16/:56), and `catalog_parents` reads
  it (lineage.py:112) with the `result.parquet` regex retained only as a pre-#74
  fallback (lineage.py:134). So
  the hash-edge DAG and the read-intent are *captured* today. Only the richer
  *follow-the-concept* layer (depend on what a parent computes, not its
  materialised hash — #73) and the reactive *consumer* (walk dependents to
  recompute) remain. See §Open questions (read-intent) and the staged plan's
  stage 3.

**What still stands.** The reactive intent (§Intent) and the staleness diagnosis
(by-value alias deps; xorq's lazy, propagation-free invalidation) are unchanged —
#74 reworked the caching substrate, not reactivity. The open questions move onto
#74's model rather than disappearing: see §Open questions.

## Reconciliation with PR #104 (merged 2026-06-17)

#104 (#101) finished what #74 started: it removed the *on-demand* `result.parquet`
layer that #74 had kept as a regenerable artifact. Where #74 stopped writing
`result.parquet` at build but left `ensure_result` to regenerate it lazily, #104
deleted `ensure_result` outright and repointed its three real consumers
(`post_processing`, `companion.diff.build_diff_expr`, `xorq.diff.full_diff`) at
`cached_result_expr`. It also unwound #102's viewer-side reintroduction
(`ensure_result_build` / `ensure_viewer_expanded_build`): `ensure_session` now
replays the entry's recipe build onto its baked snapshot instead of paging a
materialised parquet. The path plumbing went with it — `ENTRY_RESULT_FILENAME`,
`entry_result_path`, the `.xorq_result_build*` helpers, and the manifest
`result_path` field are all gone (verified absent from the tree).

**Net effect on this ADR.** The baked `.cache()` snapshot is now the *only*
materialized copy of any entry's rows, and `cached_result_expr` (result_cache.py:273)
is the single read path every surface goes through — the viewer, both diffs,
post-processing, and `from_catalog` parent composition. That sharpens, not
weakens, the reactive design: there is exactly one place to reason about staleness
and one cache to invalidate. Three downstream consequences:

- **Salt mode no longer routes through an in-place `result.parquet`.** #74's note
  that salt-mode reads bypass the baked cache and read an in-place parquet is
  obsolete: salt bakes no snapshot and now falls through to plain recompute
  (cheap-recompute / worthy-recompute-fallback, result_cache.py:300).
- **Recompute-on-cold-read is now the default for every non-baked entry.** With no
  `result.parquet` anywhere, a cheap or salt entry reconstructs its recipe on each
  cold read. This *widens* the source-identity drift hazard (§Open questions,
  source-identity) and the execution-nondeterminism soundness hole (#83) to a
  larger surface, reinforcing both as prerequisites — it does not change their
  verdicts.
- **The cache-admin surfaces are correct now.** The disk-usage pill counts
  `compute_cache` and dropped the empty `results` bucket (app.py:129); the
  result-cache inspector and delete drive off the manifest + the baked snapshot
  path, listing only snapshots that exist on disk (app.py:1007 / :1085). These
  were #74 follow-ups the staged plan listed as open; #104 closed them.

## Reconciliation with PR #103 (merged 2026-06-17)

#103 (#52) replaced xorq's catalog package with a tallyman-native,
content-addressed recipe store (`src/tallyman_core/catalog.py`) and decomposed the
monolithic `catalog.yaml` into per-concern git-tracked files. It deleted
`xorq_catalog.py` (the subprocess that shelled out to `xorq catalog add`) and
added a datafusion `connect()` shim (`backend.py:23`) so the runtime never loads
xorq's 12-module `catalog` package. This ADR's recovery model and several findings
were written against the xorq-catalog substrate; #103 dissolves that substrate.

**The #48/#52 bug cluster is gone by construction, not fixed in place.** Finding
"#48 — tallyman breaks its own xorq catalog" diagnosed the failure as tallyman
committing `aliases.json` / `alias_history.json` into the xorq catalog repo, which
violated xorq's `Catalog.assert_consistency`, so every `xorq catalog add` after
the first silently failed. With the native store there is no `xorq catalog add`
and no xorq tree contract: tallyman owns the format, so committing any decomposed
sidecar is legal by design. `assert_consistency`-on-every-add is gone; the native
`assert_catalog_consistent` (catalog.py:197, a deny-by-default `TRACKED_SURFACE`
path-allowlist with three guards) runs only at `reset_to` and after rebuild, never
per-add, so #48's failure mode cannot recur. The checkpoint is now the sole zip
writer and sole git transaction (`write_recipe_zip` → `zip_pending_entries`,
catalog.py:126/:159), which also closes the out-of-lock-git-write race #48 rode on.

**`catalog.yaml` is gone; authored state is decomposed.** The recovery model
(Q5) said "git tracks the small state (alias pointers, xorq catalog zips); heavy
`entries/<hash>/` dirs are untracked and recovered via the bullpen." Post-#103 the
tracked surface is tallyman's own: `aliases.jsonl`, `notebook.jsonl`,
`chart_specs/*.vl.json`, `display_configs/*.json`, `post_processing/*.py`,
`stats/*.py`, `prompts/<hash>.jsonl`, plus the deterministic `entries/<hash>.zip`
recipes (allowlist `{expr.py, xorq_build/, schema.json, manifest.json}`) and two
untracked-artifact pointer lists (`entries.jsonl`, `compute_cache.jsonl`). A
`git reset --hard` rolls all of it back together; the heavy entry dirs are still
gitignored and bullpen-recovered. The #49/#54 worry — authored state stored as
`catalog.yaml` keys is dropped when xorq rebuilds `catalog.yaml` on a merge — is
moot: there is no `catalog.yaml` and no xorq merge-rebuild.

**The manifest `parents` edge — this ADR's prerequisite — was already shipped (in
#84), and #103 makes it durable.** The capture+read landed earlier in #84 (merged
via PR #91, ahead of #103): `parent_capture.py` collects `{hash, ref, follow}` per
direct parent during a build, `build_and_persist` persists it into
`Manifest.parents`, and `catalog_parents` reads it. #103's contribution is
durability: it put `manifest.json` inside the recipe zip's member set
(`RECIPE_TOP_LEVEL` + `RECIPE_TREE_DIRS` = `{expr.py, schema.json, manifest.json,
xorq_build/}`, catalog.py:35), so the edge now survives `reset_to` where a loose
lineage file would not. So "restore the
cross-entry DAG + read-intent" is no longer a pending prerequisite — capture and
read are done (#84) and durable (#103); only the reactive *consumer* (walk
dependents, recompute) remains.

**What still stands.** The reactive intent and staleness diagnosis are untouched —
#103 reworked the catalog *storage and consistency model*, not reactivity. xorq's
catalog primitives (`HashingTag`, `composed_from`, `Catalog.load`) described under
"What xorq does natively" remain true of xorq-the-library, but tallyman no longer
drives that package at all, so they are no longer candidate mechanisms for
tallyman's lineage (the native-first mapping's `HashingTag` rows — already
superseded by #74 — are now also unreachable through the dropped package). See
`plans/native-catalog-store.md` for the full native-store design and the
decomposition spec.

## What xorq does natively

- **Cache strategies.** `ModificationTimeStrategy` keys a cache on the source's
  `(mtime, size, inode)` (`caching/strategy.py:89`, `common/utils/defer_utils.py:86`),
  so a changed file misses and recomputes. `SnapshotStrategy` keys on the path
  string only (`caching/strategy.py:49`), deliberately ignoring source content.
  tallyman's entry `content_hash` and `result_cache` use Snapshot.
- **Invalidation is pull-based, at `execute()`.** `cache.set_default` recomputes
  the key by walking to the Read nodes; there is no reverse-dependency index,
  watcher, or propagation (confirmed across `caching/`).
- **Provenance tagging.** `HashingTag` (`expr/relations.py:120`) tags a node
  with catalog provenance — `CatalogTag.SOURCE` / `TRANSFORM` plus `entry_name`
  (`catalog/bind.py:9`, `:118`, `:145`) — and is recoverable with `walk_nodes`.
  `composed_from` entry metadata records entry→entry edges (`catalog/catalog.py:1281`).
- **Catalog composition.** `Catalog.load` / `bind` / `compose` compose entries,
  but every path calls eager `load_expr()` and inlines the upstream recipe
  (`catalog/bind.py:115`), wrapping it in `RemoteTable` + `HashingTag`.
- **The catalog is git-backed.** `<project>/artifacts/catalog/` is a git repo;
  `xorq catalog add` zips builds; tallyman's `reset_to` sits on top of it.

## What tallyman did before #74

(Pre-#74 state; the `result.parquet` mechanics in the first two bullets were
replaced — see Reconciliation. Retained for the diagnosis it grounds.)

- `from_catalog` reads the upstream entry's materialized `result.parquet`
  (pre-#74 io.py; the read no longer exists in the current tree) as an *anonymous*
  `deferred_read_parquet`. This keeps each entry's
  graph shallow. The reason on record: long xorq expression graphs are expensive
  just to **build/tokenize**, independent of execution, so the parquet read is a
  deliberate truncation boundary. The cost: it severs xorq's native dependency
  graph — xorq sees a parquet file, not "entry A."
  *[Corrected post-#74: the truncation was real, but the blanket per-entry
  `result.parquet` behind it was not a wanted design — it materialized every step
  out of band regardless of cost, which the author had not realized. #74 keeps
  truncation only at expensive boundaries (baked snapshot) and drops it for cheap
  entries.]*
- Because the edge is invisible to xorq, tallyman re-derived the cross-entry DAG
  by regex-matching `result.parquet` paths inside each `expr.yaml`
  (`catalog_parents`). *[Post-#84: `catalog_parents` (now lineage.py:112)
  reads `manifest.parents` first; the `result.parquet` path-regex is retained only
  as a pre-#74 fallback, lineage.py:134. The old lineage.py:53-64 citation is
  stale — those lines are now unrelated `_strip_cache_nodes` code.]*
- Because Snapshot keys are content-blind, tallyman re-adds source-content
  sensitivity in `source_identity` (modes `off` / `salt` / `cas`), `off` by
  default.

## Questions answered

1. **Dependency semantics — follow-alias, pin-hash.** `from_catalog("alias")`
   should *follow* the alias head (go stale when it advances);
   `from_catalog("<hash>")` stays pinned. Requires recording the read-intent
   (alias vs hash) at build time, which is not stored today (io.py collapses
   both to a path).
2. **Binding through renames — follow.** Track the upstream through renames via
   the alias history chain; no stable alias-id exists (aliases.py keys purely on
   name). Unalias/delete prompts for what to do with dependents.
3. **Multi-parent ("poisoned") children — offer unbind.** A child that reads a
   deleted parent *and* a live one can be **unbound** from the deleted parent:
   inline that parent's contribution as a retained snapshot, keep the live
   edges. The snapshot becomes source-of-truth (retained in `.cas`), not
   evictable cache.
4. **Delete — freeze by default, cascade by option, always notify.** Author's
   instinct is git-style cleanliness: deleting an alias means the concept was
   flawed, so its descendants are suspect; recovery is real because the catalog
   is git-backed + bullpen. Recommended refinements: model deletion as
   **archive** (hide, retain bytes, restorable); **derive** "hidden because an
   ancestor is archived" rather than stamping `archived` onto each dependent;
   **preview the cone** before any cascade.
5. **Recovery model.** The catalog *is* git-backed (corrected mid-discussion —
   an earlier claim that it wasn't was wrong). git tracks the small state (alias
   pointers, recipe zips); the heavy `entries/<hash>/` dirs are untracked and
   recovered via the **bullpen** during `reset_to`. Recompute-from-recipe is the
   recovery story, so recipes must be durable — see #48. *[Post-#103: the tracked
   state is tallyman's own native surface (`aliases.jsonl` + the decomposed
   sidecars + deterministic `entries/<hash>.zip` recipes under a `TRACKED_SURFACE`
   allowlist), not xorq catalog zips; the consistency guard is the native
   `assert_catalog_consistent`. The recovery story is unchanged in shape.]*
6. **Chaining mechanism — a tagged read of the materialized result.** Replace
   the anonymous parquet read with
   `deferred_read_parquet(<entry>/result.parquet).hashing_tag(CatalogTag.SOURCE, entry_name=…)`.
   This records the dependency edge natively (so `catalog_parents` path-matching
   becomes a `walk_nodes(HashingTag)`) while keeping the shallow, cheap-to-build
   graph the parquet boundary was protecting.
   *[Superseded by #74: not adopted. #74 reconstructs the parent expression
   (cheap → re-run recipe; expensive → read baked snapshot) instead of a tagged
   parquet read, and lets `catalog_parents` go dark rather than switching it to
   `walk_nodes(HashingTag)`. See Reconciliation.]*
7. **Native-first mapping (the principle applied):**
   - record entry→entry edge → **xorq native** (`HashingTag`); drop the
     path-matching in lineage.py.
   - within-expression recompute → **xorq native** (`execute()`).
   - reference an entry *as a dependency* → **patch**: xorq's `Catalog.load`
     deep-loads (too slow, criterion 2), so compose xorq's own
     `deferred_read_parquet` + `HashingTag` instead of using `Catalog.load`.
   - cross-entry invalidation propagation → **patch** (criterion 1; xorq has none).
   - source-change detection → **patch** (criterion 1; xorq's only native option,
     `ModificationTimeStrategy`, conflicts with the content-addressed identity
     tallyman relies on).
   *[#74 update: the first and third rows were superseded. #74 does not record
   the edge via `HashingTag` (the edge goes dark, deferred to a
   semantic-dependency mechanism), and it references a parent by reconstructing
   its expression on the default backend (`cached_result_expr`) — composition,
   but not via `HashingTag`. It still avoids `Catalog.load`'s deep-load, so the
   criterion-2 read of `Catalog.load` holds. The invalidation and source-change
   rows are untouched by #74.]*

## Verified findings

- **The tagged-read spine works.**
  `deferred_read_parquet(<entry>/result.parquet).hashing_tag(CatalogTag.SOURCE, entry_name=…)`
  built in 0.019s, stayed shallow (one Read of the result), `head(3).execute()`
  returned real data in 0.20s off a 263MB result, and the edge was recovered via
  `walk_nodes(HashingTag)` as `('catalog-source', 'af917591da4b')`
  (af917591da4b = the `camera_select_columns` entry).
  *[#74: this measurement stands on its own, but the spine was not adopted — #74
  reconstructs the parent expression rather than reading and tagging a
  `result.parquet`. Kept as the record of what was tested. #104 makes the spine
  categorically moot: no `result.parquet` is written for any entry now, on-demand
  or otherwise, so there is no artifact left to read and tag.]*
- **xorq composition deep-loads.** `_resolve_source` for a `CatalogEntry` calls
  eager `load_expr()` then wraps the result in `RemoteTable` (bind.py:115–118);
  `compose` / `bind` inline upstream recipes via `replace_unbound`. So
  referencing an entry through xorq's catalog API rebuilds its full graph — the
  cost the parquet boundary was avoiding. (Slowness *at depth* is structural plus
  author-reported; not yet timed on a deep chain.)
- **#48 — tallyman breaks its own xorq catalog.** tallyman commits
  `aliases.json` / `alias_history.json` into the xorq catalog repo, which
  violates xorq's `Catalog.assert_consistency`. After the first entry, every
  `xorq catalog add` fails with an opaque empty error, so 17 of 18 entry recipes
  never reach git — undermining the recompute-from-recipe recovery model above.
  Filed as buckaroo-data/tallyman#48.
  *[#48 was closed by PR #57, then the whole failure class was eliminated by
  PR #103: xorq's catalog package and its `assert_consistency` are dropped, there
  is no `xorq catalog add` subprocess, and aliases live in a tracked `aliases.jsonl`
  (aliases.py:37) — no `aliases.json`. The original line citations (aliases.py:30-35,
  app.py:545-566, the xorq `catalog.py:768-785`) are stale; the native consistency
  guard is `assert_catalog_consistent` (catalog.py:197), run only at reset/rebuild.]*

## Open questions

- **Source-identity mode — resolved (2026-06-16): cas is required, not optional.**
  cas is the only mode that both detects drift *and* recovers original bytes
  faithfully (each read pinned to a content-addressed clone); salt detects drift
  but recovers against the *current* file (unfaithful); off does neither. #74
  turned this from a tuning question into a prerequisite: `cached_result_expr`
  reconstructs on every cold read (a cheap entry re-runs its recipe; an expensive
  entry self-heals an evicted snapshot), so under `off` a cold read of an entry
  whose source changed in place silently serves the new bytes under the entry's
  old `content_hash`. Only cas (frozen `.cas` clone) keeps "this hash ⇒ these
  bytes" true through reconstruction, which the reactive staleness signal depends
  on. The native alternative, `ModificationTimeStrategy`, is unreachable for
  identity (the hash is computed in a hardwired `SnapshotStrategy` context,
  `provenance_utils.py:24`) and hostile to reset-to (it forks on the mtime/inode
  bump every `reset_to` / checkout causes). So cas is the decision; the only open
  part is the disk-cost mitigation the source-identity ADR already lists (reflink
  on Linux, `.cas` GC). *[#74 special-cased salt by routing its reads around the
  baked cache; #104 then removed the in-place `result.parquet` entirely, so a
  salted entry bakes no snapshot and falls through to plain recompute via
  `cached_result_expr` (cheap-recompute / worthy-recompute-fallback,
  result_cache.py:300). The recompute-on-cold-read default that makes cas
  mandatory now covers every cheap and salt entry, not just expensive evictions —
  widening the hazard, not changing the verdict.]*
- **Read-intent recording — capture shipped (#84, merged via PR #91); consumption
  still open.** `from_catalog("alias")` should follow the alias head;
  `from_catalog("<hash>")` should stay pinned. This is now *recorded* at build:
  `parent_capture.note_parent(content_hash, ref=alias_or_hash, follow=not is_hash)`
  (io.py:111) captures `{hash, ref, follow}` — the resolved edge, the original
  argument, and whether it was an alias — and `build_and_persist` persists it into
  `Manifest.parents` (`ParentRef`, manifest.py:16/:56). What remains is the
  *reactive consumer*: nothing yet reads `follow` to decide a dependent is stale
  when its alias advances. The alias-history walk both this and #74's cycle-breaker
  (`_resolve_noncyclic_hash`, result_cache.py:117) rely on is still ambiguous
  (`version_of_hash` returns the first alias in iteration order, now aliases.py:101
  over `aliases.jsonl`); the recorded `ref` disambiguates it (thread the requested
  alias through `previous_version`, aliases.py:110, rather than scanning). That
  ambiguity is also flagged out-of-scope in `plans/native-catalog-store.md`. A
  stable alias-id (question 2's "no stable alias-id") is deferred until usage shows
  real cross-alias collisions.
- **Cascade failure / transaction semantics.** When a mid-DAG recompute fails:
  stop, skip-and-continue, or all-or-nothing.
- **Archive overlay — derived vs stored** (recommended derived; to confirm).
- **Trigger model.** Button vs scan-on-load vs auto-cascade-on-revise (leaning
  button + scan; explicit over implicit).
- **Determinism — resolved (2026-06-16): detect-and-warn, don't enforce.** The
  original claim ("re-running `expr.py` with any nondeterminism yields a new
  hash") only holds for *structural* nondeterminism — a Python literal baked into
  the graph (`now()`, `random()`), which changes the re-derived hash and is
  detectable by the predicate "reconstructed hash ≠ stored `content_hash` while
  the source digest is unchanged" (cheap; folds into the stage-2 instrumentation).
  *Execution* nondeterminism (`.sample()`, `ibis.now()`, unordered `.limit()`,
  impure UDF) leaves the graph — and the hash — unchanged while the bytes differ,
  so the hash can't see it and cas can't fix it (it is not source drift). v0 is a
  build-time lint on known-nondeterministic ops; the durable fix (it also breaks
  #30's evict-and-recompute faithfulness) is #83. No hard enforcement (see
  `plans/adr-source-identity-content-hash.md`).
- **Deep-chain build cost (now #82).** Originally framed as timing eager
  `load_expr` against the tagged-read reference; #74 took the composition fork, so
  the measurement now targets `cached_result_expr` reconstruction at depth —
  expensive parents truncate (bare snapshot read), cheap parents compose the full
  graph. #104 made `cached_result_expr` the *sole* read path (every viewer, diff,
  and post-processing surface routes through it), so this measurement now covers
  every read, not just parent composition — strengthening #82's relevance. Filed
  as #82; gates stage 3 of the staged plan.
- **In-process cache invalidation on reset (#80).** `cached_result_expr` is backed
  by a module-level LRU that reset-to-revision does not invalidate, so a warmed
  expensive entry can serve a pruned snapshot path after a reset. The #103
  hardening pass moved that memo onto `_resolve_result_plan`
  (`@functools.lru_cache`, result_cache.py:232) with `cache_clear` / `cache_info`
  re-exposed on the public `cached_result_expr` name, and fixed the related
  warm-then-evicted dangling-path bug by re-checking `path.exists()` on every call
  (result_cache.py:321). #80 itself — `reset_to` does not call `cache_clear` — is
  still open; a reactive recalc/staleness layer has to treat this in-process cache
  as one more thing to invalidate. Filed #80.

## Not in scope here

No implementation. #48 was a prerequisite for the recovery model (closed by #57,
then the constraint removed by #103). The per-expression cache-metadata tab and
the catalog-sidebar tweaks are separate work in progress on another branch.
