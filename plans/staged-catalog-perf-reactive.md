# Plan: three-stage path to reactive catalog recalc

Status: **proposed (2026-06-14); reconciled with PR #74 (merged 2026-06-16) and
PRs #103 / #104 (merged 2026-06-17).**
Synthesises the decisions from the planning conversation around the
reactive-catalog ADR (`plans/adr-reactive-catalog-recalc.md`). Splits the work
into three sequenced stages. No implementation here.

**#74 reconciliation.** #74 dropped the blanket per-entry `result.parquet` and
moved to materialization only at expensive boundaries — the model intended all
along; the prior blanket behaviour was a misread of the codebase (see the ADR's
Reconciliation section). Net effect on this plan: stage 1 is unaffected (its
prerequisites closed); stage 2's cache-layer inventory and the build-cost
measurement are updated below (#82); and stage 3's "landable early" tagged-read
spine is removed — #74 took the composition fork instead, so cross-entry lineage
is now a deferred semantic-dependency mechanism, not a `HashingTag` walk.

**#103 / #104 reconciliation.** Two PRs landed after this plan was written and
reshape Stage 1 and parts of Stages 2/3. **#104** (#101) removed the last
*on-demand* `result.parquet` layer (`ensure_result` and consumers deleted; the
baked `.cache()` snapshot is the only materialized copy; `cached_result_expr` is
the sole read path) and closed the #74 disk-usage / cache-inspector follow-ups
that Stage 2 listed as open. **#103** (#52) replaced xorq's catalog package with a
native `tallyman_core.catalog` store and decomposed `catalog.yaml` into
per-concern tracked files — which *dissolves Stage 1's whole bug cluster by
construction* rather than fixing it within xorq, supersedes the PR #57 mechanism
(aliases moved out to `aliases.jsonl`, not folded into `catalog.yaml`), ships a
catalog rebuild script (`scripts/rebuild_native_catalog.py` — a migration
convenience, not on the critical path; see Stage 1 item 4), and — via #84 —
ships the manifest `parents` edge Stage 3 listed as a prerequisite. Per-item
annotations below; full design in `plans/native-catalog-store.md`.

The throughline: don't build the reactive layer on machinery that doesn't yet
hold its own invariants, and don't redesign the caching until it's measured. So
stage 1 makes the existing catalog machinery correct and tested, stage 2 maps and
measures the caching, and stage 3 builds reactivity on top — using stage 2's
numbers to settle the ADR's open design questions.

Operating constraints (stated 2026-06-14): single user, one active parking
catalog, everything `--no-sync`. All perf work runs on this Mac alone; no
distributed or CI perf runs. These narrow the scope in places noted below.

---

## Stage 1 — catalog ↔ xorq coexistence, correct and tested

The existing machinery has a cluster of bugs where tallyman and the xorq catalog
disagree about what is durable. Stage 1 closes that cluster and proves it closed,
then produces a clean parking catalog for stage 2 to measure.

### The bug cluster

> **Superseded by PR #103.** This cluster was framed as "make tallyman and the
> xorq catalog agree about what is durable." #103 dropped xorq's catalog package
> instead: there is no `xorq catalog add` subprocess, no xorq `assert_consistency`,
> and no `catalog.yaml`. Tallyman owns the format end-to-end, so the disagreement
> that produced #48 cannot recur — the cluster is dissolved by construction, not
> reconciled. The descriptions below are the pre-103 diagnosis; per-bug status
> follows in Work. See `plans/native-catalog-store.md`.

- **#48** — tallyman commits `aliases.json` / `alias_history.json` into the xorq
  catalog repo → `assert_consistency` fails → every `xorq catalog add` after the
  first silently no-ops. 17/18 recipes never reached git.
- **#52** — `reset_to` reconciles the untracked tallyman artifacts (entry dirs,
  result/compute cache) against the pointer lists via the bullpen, but the
  git-tracked xorq `.zip` recipes are reconciled separately by `git reset --hard`,
  with no cross-view consistency check. A back→forward reset restores an entry the
  durable catalog cannot reproduce and reports success. Compounded by #48 today,
  but independent: any failed/interrupted `add` (which `checkpoint_catalog` already
  anticipates, `catalog_state.py:277`) triggers it after #48 is fixed.
- **#49 / #54** — authored state (charts, post_processing, stats, notebook, …)
  stored as `catalog.yaml` keys is dropped when xorq rebuilds `catalog.yaml` from
  entries+aliases on a merge. **Resolution direction settled by #54 (and the #57
  precedent):** this state is shareable catalog content and *belongs* in
  `catalog.yaml` so it clones and versions with the catalog — #49's proposed
  relocation to an out-of-repo store would stop that. So the fix is on the
  *durability* side (make the keys survive xorq's merge-rebuild — e.g. per-entry
  state into the entry metadata sidecar, which xorq treats as opaque), not
  relocation. **Deprioritised out of stage 1's critical path:** the drop is latent
  only under multi-user `pull`/sync, which does not happen single-user /
  `--no-sync`. Stays filed (#49, #54); not fixed this stage.
  **[Resolved-by-decomposition in PR #103.** The premise — authored state lives as
  `catalog.yaml` keys that xorq's merge-rebuild can drop — no longer holds. #103
  decomposed `catalog.yaml` out of existence: charts → `chart_specs/<hash>.vl.json`,
  display → `display_configs/<hash>.json`, post-processing/stats → tracked `.py`
  files, notebook → `notebook.jsonl`, aliases → `aliases.jsonl`, per-entry prompts →
  `prompts/<hash>.jsonl`. Each section is its own git-tracked file (the durability
  #49 wanted), achieved by dropping xorq rather than relocating out-of-repo. No
  `catalog.yaml`, no merge-rebuild, nothing to drop.]**

### Work

1. **✅ DONE (PR #57, merged) — #48 fix, alias bookkeeping in `catalog.yaml`.**
   `aliases.py` read/wrote `alias_map` / `alias_history` as `catalog.yaml` keys via
   `catalog_state` (no `aliases.json`, no out-of-repo sidecar — #53's alternative
   was rejected). The tracked file set was back to xorq's expected set,
   `assert_consistency` passed, `xorq catalog add` stopped no-opping. `reset_to`'s
   `git reset` rolled the alias keys back with the rest of `catalog.yaml`. **#48
   closed.** `test_reset_rolls_back_alias_state` shipped with the fix.
   *[Superseded by PR #103: the `catalog.yaml`-keys mechanism is gone. Aliases now
   live in a dedicated tracked `aliases.jsonl` (`aliases.py:37`, `_aliases_file →
   catalog_dir/aliases.jsonl`), which #57 could not do because committing a
   separate file into the (then xorq) catalog repo broke `assert_consistency` —
   the very constraint #103 removed. #48 stays closed; the #57 implementation no
   longer exists.]*

2. **✅ DONE (PR #58, merged) — #52 fix, reset cross-view reconciliation.**
   `reset_to` asserts the git-tracked recipe set (`entries/<hash>.zip`) equals the
   `entry_hashes` pointers after the bullpen reconcile; any pointer with no durable
   recipe (or recipe with no pointer) raises instead of returning a
   silently-divergent catalog. The open recovery-policy question ("re-register vs
   fail loudly") was settled **fail-loud**. **#52 closed.**
   *[#103 moved this check into the native store: `reset_to` now calls
   `tallyman_core.catalog.assert_catalog_consistent` (catalog.py:197) — a
   deny-by-default `TRACKED_SURFACE` allowlist whose pointer/recipe-agreement guard
   subsumes the #58 check — and the `entries/<hash>.zip` are now tallyman's own
   deterministic `write_recipe_zip` output (catalog.py:126), not `xorq catalog add`
   output. The fail-loud invariant is preserved; the mechanism is native.]*

3. **Largely dissolved by PR #103 — re-scope #56.** The silent swallow was a
   property of the xorq-subprocess path: `set_alias` discarded the bool from
   `xorq catalog add-alias`, and `catalog_registered` tracked whether the
   subprocess committed. #103 deleted that subprocess. `catalog_registered` now
   means "the entry dir was persisted" — a local fact that succeeds because the
   dir is written complete before the pointer is added (`build.py`) — and
   `set_alias` no longer wraps a swallowed `add_alias` call. The remaining
   observability gap is different: `plans/native-catalog-store.md` notes that a
   corrupt tracked JSON surfaces as a bare HTTP 500. Re-scope #56 against the
   native store before doing any work; the original swallow it targeted is gone.

4. **Build the stage-2 corpus FRESH in native format — we do NOT migrate old
   catalogs.** Per the project rule (single-user dev repo, no installed base):
   there is no migration/upgrade burden, and **rebuilding the corpus from scratch
   is always the accepted fix**. So the stage-2 corpus is produced by re-authoring
   the entries fresh against the source parquets through the normal build path
   (`build_and_persist`) — the same way the originals were created — **not** by
   converting the pre-#103 on-disk parking catalogs. The source parquets exist
   (`/Users/paddy/code/parking_speeding_data/`, ≈25GB). Truncation is a build-time
   parameter of the fresh authoring (`head(1M)` on the source read; see Truncation
   design under Deliverable 2), not a property of any conversion step. The pre-#103
   on-disk parking catalogs (`parking_ticket_analysis`, `parking_ticket_analysis_1m`)
   are stale and can simply be discarded and rebuilt; their layout is irrelevant.

   `scripts/rebuild_native_catalog.py` (shipped in PR #103) is a *migration*
   convenience that re-execs an old catalog's recipes into the native store. It is
   **not on the critical path** and we don't need it for our own data — its
   build-time alias-revision pinning limit on schema-evolving corpora is a
   migration-only concern and **out of scope** under the no-migration rule. (#98 —
   the old `rebuild_parking_catalog.py` shelling to `xorq catalog add` — is likewise
   moot; #99 — the perf harness reading native `aliases.jsonl` — is already fixed in
   code, `b51216f`, and closeable.) See `plans/native-catalog-store.md`.

### Tests / acceptance

- ✅ PR #50's failing integration tests (Groups A/B/D) landed on `main`
  (`80d2e9b`); #57 made the #48 ones pass and added Group F (alias registration
  durability); #58 added `test_reset_surfaces_pointer_without_durable_recipe` for
  the reset divergence and made it pass. The TDD red-then-green split is satisfied
  for both #48 and #52. *[#103 replaced the xorq-catalog test surface: the
  assertions that pinned `xorq catalog add` behavior were deleted or retargeted,
  and the native store has its own consistency/durability suite
  (`tests/test_catalog_xorq_integration.py` rewritten, plus the recipe-zip /
  tracked-tree / reset tests). The #48/#52 invariants are still pinned, now against
  the native store. See `plans/native-catalog-store.md`.]*
- A broader end-to-end demo-walk invariant test (genesis → creates → revise →
  reset back → forward → back, asserting catalog-opens + recipe-durable +
  alias-correct + authored-state-survives at every step) would still be a nice
  umbrella over the now-passing per-bug tests, but the core invariants are pinned.
  Optional; not blocking stage 2.

---

## Stage 2 — model the expression lifecycle, then measure it

Premise (corrected 2026-06-14): app performance is dictated by **how many
expressions have to be built, loaded, and re-executed, and when** — not by the
cache *key* strategy (Snapshot vs ModificationTime), which experimentation so far
shows is second-order. So stage 2 is reframed around the expression lifecycle, not
the cache design.

The xorq question is pragmatic, not a correctness audit. Ignore "are you using it
right" — the only question that matters is the ADR's guiding principle: where xorq
can do what we need *and is fast enough*, lean on it and write no code; where it
can't, or is too slow for interactive use, build our own. Each capability gets a
native-or-build verdict backed by a number, not an opinion.

### Deliverable 0 — consolidate perf instrumentation, make noise configurable (#60)

Across tallyman and buckaroo, perf/cache-stat logging has been added and then
turned back off in churn (buckaroo `19abae89` "log cache stats", `72bcf2ad`
bk-flash cross-stream tracing + "console cleanup", and similar on/off cycles).
Stop the churn: **leave the instrumentation in permanently and gate its noisiness
the normal way** — log levels and namespaces, not commenting lines in and out.

The plumbing already exists and is partly used correctly: `TALLYMAN_LOG_LEVEL`
(`src/tallyman_mcp/server.py:1388`), namespaced loggers (`tallyman.companion`,
`tallyman.buckaroo`, `tallyman_mcp`), and instrumentation already level-gated —
the warmup timing in `app.py:400-432` (`log.debug` per entry at :419, `log.info`
summary at :427), `execute_seconds` in `build.py:493`. The work:

- Recover the toggled-off perf/cache-stat logs and reinstate them permanently at
  the right level (per-event detail at DEBUG, per-action summaries at INFO).
- Route perf logging through a dedicated child namespace (e.g. `tallyman.perf`,
  and the buckaroo equivalent) so it is independently dialable without raising
  the level of everything else.
- For the buckaroo JS side, gate the bk-flash / cross-stream tracing behind a
  debug flag rather than deleting it in a "console cleanup."
- Log the **admission decision** with both verdicts side by side, so the
  structural-vs-measured tradeoff (#30) is decidable from data — not just
  lifecycle counts. *The recording half of this shipped in #87 (PR #94):* the
  manifest now persists the structural `cache_worthy` / `cache_worthy_why` verdict
  (no longer computed and thrown away) alongside the measured inputs —
  `compile_seconds` (the dominant per-view cost), `cache_bytes` (the
  result/snapshot byte-size) at manifest.py:47-50, plus `execute_seconds` at
  manifest.py:38. #87 also
  added the `compute_cache` byte count to disk accounting (app.py:129), supplying
  the byte denominator and closing the #74 disk-usage follow-up. What remains is
  the side-by-side comparison/flip logic and the per-read path tags: tag each cold
  `cached_result_expr` read with the path it took (`cheap-recompute` /
  `worthy-recompute-fallback` / `baked-read` / `evicted-self-heal`,
  result_cache.py emits these via its `_cold` helper). Counts alone can't show that
  a Join-descendant is structurally worthy but cost-cheap.

This is a prerequisite for the rest of stage 2: the lifecycle counts in
deliverable 1 and the harness in deliverable 2 read these logs as the in-app
source of build/load/execute events. (buckaroo changes are in scope — it's the
companion's own viewer — and follow buckaroo's build/test conventions.)

### Deliverable 1 — the expression-lifecycle cost model (write before any perf test)

> **Update (PR #47, merged):** the *prose* lifecycle model and the refreshed
> cache map shipped as `docs/expression-lifecycle.md` and `docs/caching.md`, both
> already post-#74/#104 accurate (baked snapshot is the only materialized copy,
> baked at build; source-read cache; no `result.parquet`). What remains for this
> deliverable is the *measured* per-action build/load/execute counts, which
> Deliverable 2's harness produces.

For each user-facing action — open the app, view an entry, revise a source, open a
diff, reset — answer: how many expressions get **built** (graph constructed /
tokenized), how many get **loaded** (from a build dir, the `/load_expr` path), and
how many get **re-executed** (`.execute()`), and which of those were avoidable.
Build/load/execute counts are the explanatory variable; wall-clock is the symptom.

- **Build cost** dominates with graph depth and count — long xorq graphs are
  expensive to construct independent of execution. Pre-#74 `from_catalog`
  truncated every parent at a parquet read; #74 truncates only at expensive
  parents (baked snapshot) and composes cheap ones, so "how many entries, how deep
  each chain, and where the expensive boundaries sit" is the first-order lever
  (measured by #82).
- **Load cost** is distinct from build — #34 (the 10s `/load_expr` timeout that
  abandons cold loads) is a load-path problem, not a cache-key problem.
- **Re-execution timing** is where caching actually shows up, but the lever is
  *when* re-execution is triggered, not how the key is computed — #21 (caches warm
  lazily on view, never at build) means re-execution lands at view time and the
  user waits.

The cache layers matter only insofar as they change those counts (a hit skips a
re-execution; a shallow graph skips a build). The catalogue of layers already
exists — merged via PR #47 as `docs/caching.md` ("map every cache in the
tallyman/xorq/buckaroo stack", `xorq` `SnapshotStrategy`, the `.cas` store +
`content_hash`, buckaroo's stat/session caches). **The #74/#104 refresh is already in that merged doc:** the
separate `result_cache/` dir is retired (baked results now co-locate in the
per-project content-addressed `compute_cache`), and a new source-read cache
(snapshot `.cache()` after each `read_csv`/`read_json`, shared across entries,
parquet exempt) was added. #74's two disk-usage / cache-inspector follow-ups are
now closed, across two PRs: #87 (PR #94) added the `compute_cache` byte count to
`_compute_disk_usage` (app.py:129), and #104 dropped the now-empty `results`
bucket and repointed the cache inspector + delete off the manifest `cache_bytes`
and the baked snapshot path, listing only snapshots that exist on disk (app.py:1007
/ :1085) — there is no on-demand `result.parquet` left to reflect. Reuse the map as
the levers section; the prose lifecycle model on top also shipped
(`docs/expression-lifecycle.md`, PR #47), so what is left for this deliverable is
the *measured* per-action build/load/execute counts — produced by Deliverable 2's
harness, not a re-derived cache-key taxonomy.

Existing backlog items are findings against this model: #21 (lazy warm), #22
(checkpoint capture cost per mutating request), #30 (measured cost rubric vs
structural `cache_worthy`), #35 (leftover result dirs). The hypotheses to settle
with numbers: build cost as a function of `from_catalog` chain depth now that #74
truncates only at expensive boundaries and composes cheap parents (#82 — reframes
the ADR's untimed deep-chain question); how many expressions are needlessly
rebuilt or reloaded per app open; does revising a source re-execute dependents at
all today (ADR says no — fast but stale).

### Deliverable 2 — performance integration tests (#59)

> **Update (#59 / #64, both closed):** both tiers shipped — Tier B
> (`tests/test_perf_integration.py`, `perf` marker: execute / page-load / diff,
> cold+warm) and Tier A (`tests/test_perf_tier_a.py`, `perf_tier_a` marker, #64:
> Playwright to `.df-viewer .ag-cell` + backend/total calibration). Three things
> remain: (1) it is **report-only** — promote #45's diff time to a regression gate
> once baselined; (2) the harness still references the OLD
> `scripts/rebuild_parking_catalog.py` for its corpus — repoint it at the
> freshly-built native corpus (Stage 1 item 4; no migration), with truncation done
> at authoring time (`head(1M)` source read); (3) the **#82 deep-chain depth
> measurement does not exist** and gates Stage 3.

Three measurements, each on both corpus variants (full / truncated) and across a
cold/warm cache axis. Local-only on this Mac, marker-gated off CI (same convention
as `cache_lab`).

Every measurement records the **build / load / execute counts** alongside
wall-clock — per deliverable 1, the counts are the explanatory variable and the
time is the symptom. A regression in wall-clock should be attributable to a
changed count (one more expression rebuilt, one more cold load), not left as an
unexplained slowdown.

- **Execution time** — `time expr.execute()`. Trivial; cold and warm.
- **Page load including buckaroo — two tiers.**
  - *Tier A (Playwright, ground truth).* Spin up the catalog + buckaroo server,
    drive `/load` (`mode: 'buckaroo'`), measure to **data-grid-visible**
    (`.df-viewer .ag-cell` first-visible) — that is "loaded." Follow buckaroo's
    server-mode examples: `packages/buckaroo-js-core/pw-tests/server-buckaroo-summary.spec.ts`,
    `playwright.config.server.ts`, `scripts/test_playwright_server.sh`. Produces
    the baseline number.
  - *Tier B (python-only proxy).* Call the same backend the buckaroo `/load`
    (`mode=buckaroo`) handler dispatches to — load the xorq expr from the entry's
    `xorq_build/` dir + compute summary stats — no server, no browser. Fast and
    deterministic; this is the test that runs routinely.
  - *Calibration (one-time, required).* Run both tiers on the same entry, record
    the backend/total ratio, and document what Tier B cannot see (browser render,
    network, first-paint JS). For the big parking datasets the backend dominates,
    so the proxy is faithful — but the ratio must be written down so the proxy
    can't silently drift from reality.
- **Diff performance** — exercise the `catalog_diff` stat path (#45 measured 27s
  synchronous on first open for 18.4M vs 1.0M). Define the revision pairs diffed;
  cold and warm.

**Truncation design.** Truncate at the raw-source read (`head(1M)` on the source
parquet) so the DAG shape and build/tokenize cost are identical to the full
corpus and only execution cost drops. Both variants are built fresh in the native
format from the source parquets (Stage 1 item 4 — no migration of old catalogs);
regenerable, not checked in.

**Gating.** Report-only first (no baselines yet), matching the existing perf-report
suite. Promote a few to regression gates once baselined — #45's diff time is the
obvious first gate.

**Why this is the point.** Once the harness exists, competing approaches stop
being argued and start being measured. Every native-or-build verdict in this
stage, and every design choice in stage 3, is decided by running both options
through the same harness and reading the build/load/execute counts and wall-clock
off the parking corpus. The harness is the comparison instrument, not just a
regression guard.

---

## Stage 3 — reactive recalc & staleness

Design captured in `plans/adr-reactive-catalog-recalc.md`. Two of its open
questions are answered by stage 2's measurements, so stage 3 design is gated on
stage 2:

- deep-chain build cost → #82 (reframed by #74: `cached_result_expr`
  reconstruction at depth, not eager `load_expr` against a tagged read).
- `cas` is a prerequisite, not an open question (resolved in the reactive ADR):
  #74's recompute-on-cold-read means only cas keeps "this hash ⇒ these bytes"
  true. Stage 2 measures only its disk cost, to size the reflink/GC mitigation —
  not whether to adopt it.

**Superseded by #74 — the "landable early" tagged-read spine is dropped.** This
plan previously proposed landing, during the stage-1/2 window, a tagged read
(`deferred_read_parquet(<entry>/result.parquet).hashing_tag(…)`) plus a
`walk_nodes(HashingTag)` rewrite of `lineage.py`. #74 took the other fork:
`from_catalog` reconstructs the parent expression (`cached_result_expr`) instead
of reading a `result.parquet`, and `catalog_parents` is allowed to go dark. There
is no per-entry `result.parquet` read left to tag, so neither item can or should
land. The cross-entry DAG restoration this paragraph proposed — record the
resolved parent hash in the manifest (a `parents` field, parallel to `sources`)
and have `catalog_parents` read it — **shipped in #84 (merged via PR #91, ahead of
#103)**: `parent_capture.note_parent` records `{hash, ref, follow}` at build
(io.py:111), `build_and_persist` persists it into `Manifest.parents`
(manifest.py:16/:56), and `catalog_parents` reads it (lineage.py:112), with the
path-regex retained only as a pre-#74 fallback. #103 then made the edge durable
across reset by putting `manifest.json` in the recipe-zip member set
(`RECIPE_TOP_LEVEL` + `RECIPE_TREE_DIRS`). Only the *follow-the-concept* layer
(depend on what a parent
computes, not its materialised hash — #73) and the reactive *consumer* (walk the
recorded edges to recompute dependents) are the genuinely deferred work.

**Prerequisites — landable now, ahead of the reactive feature** (each filed as
its own issue; the 2026-06-16 design review settled their direction):

- **Restore the cross-entry DAG + read-intent** — ✅ **DONE (#84, merged via
  PR #91):** `Manifest.parents: [{hash, ref, follow}]` is recorded by
  `parent_capture` + `build_and_persist`, and `catalog_parents` reads it
  (lineage.py:112). #103 made the edge durable across reset by putting
  `manifest.json` in the recipe-zip allowlist. What remains is not the capture but
  the reactive **consumer** — walk the recorded edges to find and recompute
  dependents (the feature itself, below).
- **Fix the alias-history ambiguity** — still OPEN. Thread the requested alias
  through `previous_version` / `_resolve_noncyclic_hash` so follow-resolution can't
  step back through the wrong lineage. `version_of_hash` still returns the
  first-match alias in iteration order (aliases.py:101, now over `aliases.jsonl`);
  also flagged out-of-scope in `plans/native-catalog-store.md`.
- **Flip the default to `cas`** (+ reflink on Linux, `.cas` GC) — the identity
  prerequisite from #74's recompute-on-cold-read (widened by #104: recompute is now
  the default for every cheap and salt entry).
- **Admission-decision telemetry** — deliverable 0 above; gates the #30 flip.
  *Recording shipped in #87 (merged via PR #94):* the manifest persists the
  structural verdict (`cache_worthy` / `cache_worthy_why`) alongside
  `compile_seconds` and `cache_bytes` (manifest.py:47-50); the side-by-side
  comparison/flip logic is the remaining work.
- **Determinism lint** — ✅ **SHIPPED (#88):** the build-time hint on
  nondeterministic ops landed (`_NONDETERMINISTIC_OPS`, build.py:261; wired into
  `build_and_persist` at build.py:370; surfaced via the MCP server and companion;
  tests pass). Residual: the runtime structural-case predicate log (reconstructed
  hash ≠ stored `content_hash`); the durable execution-nondeterminism fix is still
  #83 (open).
- **In-process LRU invalidation on reset** — #80 / #96 (still open). The memo
  moved onto `_resolve_result_plan` (`@functools.lru_cache`) in #103's hardening
  pass and `reset_to` still does not call `cache_clear`, so as filed both stay
  open — but the concrete dangling-path failure is now *mitigated*: the
  `path.exists()` re-check + self-heal runs in the public `cached_result_expr`
  body (result_cache.py:321) on every call, warm LRU hits included, so a reset or
  prune no longer serves a deleted snapshot. A reactive layer still has to treat
  this in-process cache as one more thing to invalidate.

**Still genuinely open (the reactive feature's design — tracked as epic #89):**
the reactive *consumer* that walks `manifest.parents` to find and recompute
dependents, plus the trigger model (button vs scan-on-load vs auto-cascade —
leaning button + scan), cascade/transaction semantics, and archive overlay
(derived vs stored). Nothing in the tree walks `manifest.parents` to recompute
today — `catalog_dag` / `catalog_parents` feed only the DAG-visualization
endpoints. The DAG and read-intent *capture* are in place (#84, merged via PR #91;
made durable by #103); these staleness-layer decisions sit on top, once `cas` is
the default and the alias-history ambiguity is fixed.

**Recompute-substrate correctness holes the consumer inherits (filed since the
#103 reconciliation; not yet reflected above).** The consumer recomputes
dependents through the same `cached_result_expr` / `from_catalog` substrate these
bugs sit in, so each effectively gates a *correct* cascade:

- **#75** — an entry combining ≥2 catalog parents hits "Multiple backends found".
  Multi-parent composition (joins/unions across entries) is the normal case a
  cascade must rebuild. Appears handled in-tree (the multi-parent self-heal test
  passes) but the issue is still open — confirm before relying on it.
- **#76 / #77** — an expensive-parent child pins the parent's untracked
  `compute_cache` snapshot *by path*; on a pruned or cross-machine-cloned snapshot
  the child read fails (#76) or the viewer shows an empty grid (#77) instead of
  recomputing from the parent. Recompute can be "correct" via `cached_result_expr`
  while the user-facing grid is still stale/empty.
- **#97** — a 0-row expensive parent bakes no snapshot, so a child's `/load_expr`
  replay reads zero files and the self-heal can't repopulate it. Any cascade that
  produces a 0-row intermediate breaks every downstream dependent.
- **#79** — concurrent on-demand materialisation of the same cold entry races on
  the output path (torn snapshot + 500, no single-flight lock). A cascade or
  scan-on-load that warms many entries introduces exactly this concurrency.
- **#81** — scalar-UDF-only entries are misclassified by `_is_worthy_expr`
  (source_cache.py) vs `classify_build`, so some expensive entries bake nothing and
  recompute on every read. A concrete instance of the #30/#87 worthiness
  disagreement; it skews the recompute-cost model the staleness signal relies on.

None of these are referenced in the plan/ADR today. They don't change the staged
sequencing, but the cascade/transaction design above should treat them as gating
the substrate it runs on.

---

## Cross-stage dependencies

```
#48 ✓ ─┐
#52 ✓ ─┤   native store ✓ ─► build stage-2 corpus FRESH ──► stage 2 corpus
#103 ✓ ─┴─► (no migration of old catalogs — re-author)     │
                                                            │
cache map + lifecycle docs ✓ (PR #47) ──► perf tiers ✓ (#59/#64) ─► numbers
                                          (deep-chain #82 ✗)        │
                                                          ┌─────────┘
substrate (merged: #74 source-cache + baked result,      │
           #104 result.parquet removal, #103 native store)│
                                                          ▼
              prerequisites: parents-DAG + read-intent ✓ (#84/PR #91),
              determinism lint ✓ (#88), admission telemetry recording ✓ (#87/PR #94);
              still open: cas default (#86), previous_version fix (#85),
              #30 worthiness flip, #80/#96 LRU-on-reset (mitigated)
                                                          │
                                                          ▼
              stage 3 reactive feature (epic #89): consume parents-DAG (recompute
              dependents), trigger model, cascade, archive
              — gated also by substrate holes #75/#76/#77/#97/#79/#81
```
