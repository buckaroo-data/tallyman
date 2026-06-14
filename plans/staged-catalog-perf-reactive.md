# Plan: three-stage path to reactive catalog recalc

Status: **proposed (2026-06-14).** Synthesises the decisions from the planning
conversation around the reactive-catalog ADR (`plans/adr-reactive-catalog-recalc.md`).
Splits the work into three sequenced stages. No implementation here.

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
- **#49** — authored state (charts, post_processing, …) stored as extra
  `catalog.yaml` keys is dropped when xorq rebuilds `catalog.yaml` from
  entries+aliases on a merge. **Deprioritised out of stage 1's critical path:**
  latent only under multi-user `pull`/sync, which does not happen single-user /
  `--no-sync`. Stays filed; not fixed this stage.

### Work

1. **#48 fix.** Move `aliases.json` / `alias_history.json` (and any other non-xorq
   file the checkpoint commits — audit the full set written under
   `catalog_dir`, not just the two named) out of the xorq catalog repo.
   - **Open decision — where they go.** *Recommendation: a sibling
     `<project>/artifacts/tallyman_state/` git repo, committed in lockstep with
     the xorq repo by the same `checkpoint_catalog` (matching step tags), and
     reset by the same `reset_to`.* This keeps the xorq repo holding only what
     `assert_consistency` expects, while preserving the free `git reset --hard`
     rollback of alias state — so `test_reset_rolls_back_alias_state` stays green
     via git rather than bespoke reconciliation. The alternative (files outside
     any repo + explicit pointer reconciliation in `reset_to`) is simpler to
     write but reintroduces the manual-reconciliation surface #52 is about. Needs
     ratification before coding.
   - The deferred `test_reset_rolls_back_alias_state` guard (PR #50) ships *with*
     this fix — moving the files changes reset's rollback behaviour, so the guard
     must travel with the change per the TDD rule.

2. **#52 fix.** Add an explicit cross-view reconciliation + assertion at the end of
   `reset_to`: compare the tracked `entries/*.zip` set against `entry_hashes`,
   re-register (or fail loudly on) any pointer whose recipe is missing, and assert
   `Catalog.from_kwargs(init=False)` opens. Turns the silent divergence observable.

3. **#48 secondary — surface the silent swallow.** `add_entry` is best-effort and
   discards its result (`xorq_catalog.py:63-68`, `build.py`); expose a
   `BuildResult.catalog_registered`-style signal, log failures at ERROR, and emit a
   one-time companion warning. PR #50's Group D pins this.

4. **Reproducible parking-catalog rebuild script.** No migration/repair of the
   existing broken catalog — rebuild from scratch through the *fixed* machinery.
   One script drives the parking sources through `catalog_create`/`catalog_revise`
   to produce the canonical catalog, and emits both corpus variants for stage 2
   (full ≈10M+ rows, truncated ≈1M). This replaces "repair" on the task list and
   doubles as the stage-2 corpus generator. Depends on #48 + #52 being fixed first
   (otherwise the rebuild is born broken again).

### Tests / acceptance

- PR #50's failing integration tests (Groups A/B/D) go green.
- One end-to-end demo-walk invariant test: genesis → multiple creates → revise →
  reset back → forward → back, asserting at every step that the catalog opens,
  every recipe is durable as a tracked `.zip`, alias state is correct, and (where
  in scope) authored state survives.
- TDD split per house rule: failing tests (PR #50) seen red on CI first, then the
  fixes; the alias-rollback guard ships with the fix commit.

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

### Deliverable 0 — consolidate perf instrumentation, make noise configurable

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
  expensive to construct independent of execution, which is the whole reason
  `from_catalog` truncates at a parquet read (ADR). So "how many entries, how deep
  each graph" is a first-order lever.
- **Load cost** is distinct from build — #34 (the 10s `/load_expr` timeout that
  abandons cold loads) is a load-path problem, not a cache-key problem.
- **Re-execution timing** is where caching actually shows up, but the lever is
  *when* re-execution is triggered, not how the key is computed — #21 (caches warm
  lazily on view, never at build) means re-execution lands at view time and the
  user waits.

The cache layers matter only insofar as they change those counts (a hit skips a
re-execution; a shallow graph skips a build). The catalogue of layers already
exists — the `docs/caching-architecture` branch ("map every cache in the
tallyman/xorq/buckaroo stack", `xorq` `SnapshotStrategy`, tallyman `result_cache`,
the `.cas` store + `content_hash`, buckaroo's stat/session caches). Reuse it as
the levers section; the deliverable here is the lifecycle model on top, not a
re-derived cache-key taxonomy.

Existing backlog items are findings against this model: #21 (lazy warm), #22
(checkpoint capture cost per mutating request), #30 (measured cost rubric vs
structural `cache_worthy`), #35 (leftover result dirs). The hypotheses to settle
with numbers: does the parquet-truncation boundary actually cut build cost at
depth (ADR open question, untimed); how many expressions are needlessly rebuilt or
reloaded per app open; does revising a source re-execute dependents at all today
(ADR says no — fast but stale).

### Deliverable 2 — performance integration tests

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

- deep-chain build cost (ADR open question, explicitly untimed) → stage 2's
  build-cost-at-depth measurement.
- is `cas` affordable as the default for recalc-enabled projects (disk cost) →
  stage 2's disk/cold-warm numbers.

**Landable early (low-risk, ADR-verified, net-neutral).** These can go in during
the stage-1/2 window:

- The tagged-read spine: replace the anonymous `deferred_read_parquet` in
  `io.py` with `deferred_read_parquet(<entry>/result.parquet).hashing_tag(
  CatalogTag.SOURCE, entry_name=…)` (ADR: built in 0.019s, stayed shallow).
- Drop the regex `result.parquet`-path matching in `lineage.py` for
  `walk_nodes(HashingTag)`.

**Still open (settle after stage 2):** read-intent recording (alias-follow vs
hash-pin at build time), trigger model (button vs scan-on-load vs
auto-cascade — leaning button + scan), cascade/transaction semantics, archive
overlay (derived vs stored).

---

## Cross-stage dependencies

```
#48 fix ─┐
#52 fix ─┴─► rebuild script ─► clean parking catalog ─► stage 2 corpus
                                                          │
cache-architecture map ───────────────────────────────► perf tests ─► numbers
                                                                         │
                                                          ┌──────────────┘
tagged-read spine (early) ──────────────────────────────►│
                                                          ▼
                                                  stage 3 reactive design
```
