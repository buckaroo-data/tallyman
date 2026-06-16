# Plan: three-stage path to reactive catalog recalc

Status: **proposed (2026-06-14); reconciled with PR #74 (merged 2026-06-16).**
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

- **#48** — tallyman commits `aliases.json` / `alias_history.json` into the xorq
  catalog repo → `assert_consistency` fails → every `xorq catalog add` after the
  first silently no-ops. 17/18 recipes never reached git.
- **#52** — `reset_to` reconciles the untracked tallyman artifacts (entry dirs,
  result/compute cache) against the pointer lists via the bullpen, but the
  git-tracked xorq `.zip` recipes are reconciled separately by `git reset --hard`,
  with no cross-view consistency check. A back→forward reset restores an entry the
  durable catalog cannot reproduce and reports success. Compounded by #48 today,
  but independent: any failed/interrupted `add` (which `checkpoint_catalog` already
  anticipates, `catalog_state.py:388-390`) triggers it after #48 is fixed.
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

### Work

1. **✅ DONE (PR #57, merged) — #48 fix, alias bookkeeping in `catalog.yaml`.**
   `aliases.py` now reads/writes `alias_map` / `alias_history` as `catalog.yaml`
   keys via `catalog_state` (no `aliases.json`, no out-of-repo sidecar — #53's
   alternative was rejected). The tracked file set is back to xorq's expected set,
   `assert_consistency` passes, `xorq catalog add` stops no-opping. `reset_to`'s
   `git reset` rolls the alias keys back with the rest of `catalog.yaml` — no
   separate reconcile path. **#48 closed.** `test_reset_rolls_back_alias_state`
   shipped with the fix and guards the rollback; the non-xorq-fileset audit
   landed too (`_TRACKED` is `catalog.yaml` only).

2. **✅ DONE (PR #58, merged) — #52 fix, reset cross-view reconciliation.**
   `reset_to` now asserts the git-tracked recipe set (`entries/<hash>.zip`) equals
   the `entry_hashes` pointers after the bullpen reconcile; any pointer with no
   durable recipe (or recipe with no pointer) raises `RuntimeError` instead of
   returning a silently-divergent catalog. The open recovery-policy question
   ("re-register vs fail loudly") was settled **fail-loud**. **#52 closed.**

3. **PARTLY DONE — surface the silent swallow.** #57 exposed
   `BuildResult.catalog_registered` and logs the failed entry-`add` at ERROR. The
   remainder is **#56 (open):** `catalog_registered` is computed but dies in
   `_run_and_record` (never propagated to the caller / companion), and `set_alias`
   still discards `add_alias`'s bool, so a failed *alias* add stays swallowed.
   Finish #56 to actually surface both failures.

4. **OPEN — reproducible parking-catalog rebuild script.** No migration/repair —
   rebuild from scratch through the now-fixed machinery. One script drives the
   parking sources through `catalog_create`/`catalog_revise` to produce the
   canonical catalog and emits both corpus variants for stage 2 (full ≈10M+ rows,
   truncated ≈1M). Doubles as the stage-2 corpus generator. Now unblocked on the
   #48 side; still wants #52 fixed first so a back/forward rebuild stays durable.
   **Blocking input:** confirm the parking source parquet(s) still exist / where.

### Tests / acceptance

- ✅ PR #50's failing integration tests (Groups A/B/D) landed on `main`
  (`80d2e9b`); #57 made the #48 ones pass and added Group F (alias registration
  durability); #58 added `test_reset_surfaces_pointer_without_durable_recipe` for
  the reset divergence and made it pass. The TDD red-then-green split is satisfied
  for both #48 and #52.
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
(`src/tallyman_mcp/server.py:1383`), namespaced loggers (`tallyman.companion`,
`tallyman.buckaroo`, `tallyman_mcp`), and instrumentation already level-gated —
the warmup timing in `app.py:411-444` (`log.debug` per entry, `log.info`
summary), `execute_seconds` in `build.py:335-341`. The work:

- Recover the toggled-off perf/cache-stat logs and reinstate them permanently at
  the right level (per-event detail at DEBUG, per-action summaries at INFO).
- Route perf logging through a dedicated child namespace (e.g. `tallyman.perf`,
  and the buckaroo equivalent) so it is independently dialable without raising
  the level of everything else.
- For the buckaroo JS side, gate the bk-flash / cross-stream tracing behind a
  debug flag rather than deleting it in a "console cleanup."
- Log the **admission decision** with both verdicts side by side, so the
  structural-vs-measured tradeoff (#30) is decidable from data — not just
  lifecycle counts. Persist the structural `cache_worthy` verdict (today it is
  computed and thrown away), and record the measured value-per-byte inputs:
  `compile_seconds` (new — the dominant per-view cost), the result/snapshot
  byte-size (new), and `execute_seconds` (already recorded). Tag each cold
  `cached_result_expr` read with the #74 path it took (cheap-recompute /
  baked-read / evicted-self-heal). Counts alone can't show that a Join-descendant
  is structurally worthy but cost-cheap. Fixing disk accounting to count
  `compute_cache` per entry supplies the byte denominator and closes the #74
  disk-usage follow-up at once.

This is a prerequisite for the rest of stage 2: the lifecycle counts in
deliverable 1 and the harness in deliverable 2 read these logs as the in-app
source of build/load/execute events. (buckaroo changes are in scope — it's the
companion's own viewer — and follow buckaroo's build/test conventions.)

### Deliverable 1 — the expression-lifecycle cost model (write before any perf test)

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
exists — the `docs/caching-architecture` branch ("map every cache in the
tallyman/xorq/buckaroo stack", `xorq` `SnapshotStrategy`, the `.cas` store +
`content_hash`, buckaroo's stat/session caches). **Refresh it for #74:** the
separate `result_cache/` dir is retired (baked results now co-locate in the
per-project content-addressed `compute_cache`), and a new source-read cache
(snapshot `.cache()` after each `read_csv`/`read_json`, shared across entries,
parquet exempt) was added. #74's own follow-ups are part of this refresh —
disk-usage reporting still points at the now-empty `result_cache/` and doesn't
count `compute_cache`, and the cache-inspector still reflects on-demand
`result.parquet` rather than the baked compute-cache results. Reuse the map as
the levers section; the deliverable here is the lifecycle model on top, not a
re-derived cache-key taxonomy.

Existing backlog items are findings against this model: #21 (lazy warm), #22
(checkpoint capture cost per mutating request), #30 (measured cost rubric vs
structural `cache_worthy`), #35 (leftover result dirs). The hypotheses to settle
with numbers: build cost as a function of `from_catalog` chain depth now that #74
truncates only at expensive boundaries and composes cheap parents (#82 — reframes
the ADR's untimed deep-chain question); how many expressions are needlessly
rebuilt or reloaded per app open; does revising a source re-execute dependents at
all today (ADR says no — fast but stale).

### Deliverable 2 — performance integration tests (#59)

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
corpus and only execution cost drops. Both variants emitted by the stage-1
rebuild script; regenerable, not checked in.

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
land. But the cross-entry DAG itself is restorable cheaply: `from_catalog` knows
the resolved parent hash at build, so recording it in the manifest (a `parents`
field, parallel to `sources`) brings `catalog_parents` back as a manifest read.
Only the *follow-the-concept* layer (depend on what a parent computes, not its
materialised hash — #73) is the genuinely deferred semantic-dependency work.

**Prerequisites — landable now, ahead of the reactive feature** (each filed as
its own issue; the 2026-06-16 design review settled their direction):

- **Restore the cross-entry DAG + read-intent** — manifest
  `parents: [{hash, ref, follow}]`; `catalog_parents` reads the manifest.
  Foundational: reactive recalc can't find dependents to recompute without it.
- **Fix the alias-history ambiguity** — thread the requested alias through
  `previous_version` / `_resolve_noncyclic_hash` so follow-resolution can't step
  back through the wrong lineage (the #74-review follow-up).
- **Flip the default to `cas`** (+ reflink on Linux, `.cas` GC) — the identity
  prerequisite from #74's recompute-on-cold-read.
- **Admission-decision telemetry** — deliverable 0 above; gates the #30 flip.
- **Determinism lint** — build-time hint on nondeterministic ops; durable fix #83.
- **In-process LRU invalidation on reset** — #80.

**Still genuinely open (the reactive feature's design):** trigger model (button
vs scan-on-load vs auto-cascade — leaning button + scan), cascade/transaction
semantics, archive overlay (derived vs stored). These staleness-layer decisions
sit on top of the prerequisites, once the DAG, cas, and read-intent are in place.

---

## Cross-stage dependencies

```
#48 ✓ ─┐
#52 ✓ ─┴─► rebuild script ─► clean parking catalog ─► stage 2 corpus
                                                        │
cache-architecture map (refresh for #74) ────────────► perf tests ─► numbers
                                                        (incl. #82)      │
                                                          ┌──────────────┘
PR #74 substrate (merged: source-cache + baked result) ─►│
                                                          ▼
              prerequisites: parents-DAG + read-intent, cas default,
              previous_version fix, admission telemetry, determinism lint
                                                          │
                                                          ▼
              stage 3 reactive feature (trigger model, cascade, archive)
```
