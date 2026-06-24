# Execution plan: clear #89's remaining determinism prerequisites

- **Status:** Execution plan (2026-06-18). Drives the still-open implementable
  items in the reactive-recalc prerequisites epic (#89) to done, so the staleness
  layer's design questions are the only thing left blocking it.
- **Epic:** buckaroo-data/tallyman#89. Design in
  `.claude-worktrees/docs-reactive-catalog/plans/adr-reactive-catalog-recalc.md`.
- **Branch:** `feat/89-determinism-prereqs` off `main`.

## Why this plan exists

#89's checkboxes are stale. Read against `main`, the prerequisites have mostly
landed: #84 (PR #91), #85 (PR #114), #86 (PR #116), #87 (PR #94), and the
overnight native-store pass (`ee0a90a`) reworked `cached_result_expr` in a way
that already neutralises #80's failure mode. The determinism axis — the one
prerequisite group the reactive staleness signal can't tolerate being wrong about
— is the remaining work:

- **#83 (result-digest faithfulness)** — landed on `main` via **PR #120** while
  this branch was in flight. The durable mechanism is now canonical there:
  `result_digest`, `verify_result_faithful`, `_warn_if_self_heal_unfaithful`,
  the manifest `result_digest` field, the build wiring. This branch was rebased
  onto that, so it no longer carries a duplicate #83 implementation.
- **#88 (determinism lint)** — part 1 (build-time op lint, `build.py:254-305`)
  already on `main`; this branch adds **part 2**, the runtime drift attribution.
- **#80** — confirmed neutralised by `ee0a90a`'s per-call snapshot existence
  re-check; this branch adds the `reset_to`-driven regression guard. Closeable.
- **#82** — report-only deep-chain measurement, in progress on `perf/82`; not
  carried here.

The staleness layer itself (trigger model, cascade/transaction semantics, archive
overlay) is **design-open** and out of scope.

## Why determinism is the load-bearing prerequisite

#74 reconstructs an entry's result on cold read instead of freezing it in
`result.parquet`. So a recipe whose *graph* is identical run-to-run but whose
*execution* is nondeterministic (`sample()`, `now()`, an unordered `limit`, an
impure UDF) — or, under `off` identity, one whose source drifts — now serves
different bytes under one `content_hash` at view/diff time. The structural hash is
a `SnapshotStrategy` tokenization of the graph; it cannot see this. Two
consequences the reactive layer can't absorb:

1. **Eviction is unsound.** The result-cache and source-identity ADRs make
   eviction depend on "an evicted snapshot recomputes to what was evicted." For an
   execution-nondeterministic entry that is false, silently. #83's `result_digest`
   re-check on self-heal is the soundness signal that closes this.
2. **The staleness signal is polluted.** Recompute-vs-recorded comparison
   produces false positives / non-convergence when the recompute is itself
   unstable.

Detection has two regimes:

- **Execution** nondeterminism (a fixed graph that runs differently each execute,
  or source drift under `off`) leaves the hash fixed and only moves the *bytes*.
  Caught by #83's recorded `result_digest`.
- **Structural** nondeterminism (a Python-level literal — `pd.Timestamp.now()`,
  `random.random()` — evaluated at author time and baked into the graph)
  re-derives a *different* graph each `expr.py` import, so the reconstructed
  `content_hash` itself moves. The op lint (#88 part 1) can't see it (the literal
  is not an ibis op). This is #88 part 2.

## Stage 0 — #80: confirm neutralised, add the regression guard, close

`cached_result_expr` was split (`ee0a90a`) into a memoised plan resolver and a
per-call snapshot existence re-check that runs on every call, warm LRU or not. A
snapshot pruned *after* a warm read self-heals on the next read instead of
dangling — exactly #80's failure mode, so cache_clear-on-reset is no longer
load-bearing.

- `test_reset_prune_self_heals_warmed_expensive_entry` (`test_reset_to_revision.py`)
  drives the actual `prune_compute_cache` path `reset_to` invokes: warm an
  expensive entry, prune its snapshot to a warm-set that excludes it, re-read
  without `cache_clear`, assert self-heal. Passes on current `main` → it is a
  regression guard, bundled with the fix increments, not failing-first.
- **Close #80** referencing `ee0a90a` and this guard.

## Stage 1 — #88 part 2: structural-drift runtime attribution

`recipe_is_structurally_nondeterministic(project, content_hash)` reconstructs the
recipe's graph hash twice (`_reconstructed_hash`: re-import `expr.py`, rewrite for
build, tokenize — no execute) and compares the two. A baked-literal recipe hashes
differently each time (structural); a fixed graph hashes the same (execution).
Comparing two reconstructions *to each other* — not to the stored hash — keeps the
predicate robust to any build-vs-reconstruct path skew, and best-effort (an
unresolvable hash returns False).

#83's `_warn_if_self_heal_unfaithful` now carries the attribution: when a
self-healed snapshot's digest differs from the recorded one, the `tallyman.perf`
warning labels the drift `[structural (#88)]` or `[execution (#83)]`. The
re-derivation runs only on a confirmed-drift branch (rare), so it stays off the
hot path — satisfying #88's "cheap" constraint without depending on the #82
stage-2 instrumentation that does not exist yet.

- Tests: a baked `random.random()` recipe is detected structural; a deterministic
  recipe is not; a source-drift self-heal (graph fixed) is attributed execution.
- **Close #88** (lint + structural attribution both landed); #83 is the durable
  mechanism.

## Stage 2 — #82: deep-chain build/reconstruct cost (report-only)

In progress on `perf/82`. Not carried on this branch. Measurement, not a
correctness fix: a synthetic `tracked_expr_from_alias` chain parameterised by depth and a
cheap/expensive mix, recording build/load/execute/reconstruct counts alongside
wall-clock per #59's convention; report-only, marker-gated off CI.

## Commit / TDD sequencing

Per the repo's TDD rule (tests visible failing on CI before the fix lands):

1. **Failing-tests commit** — #88 part-2 tests (`recipe_is_structurally_nondeterministic`
   absent → ImportError red). Push, watch CI go red.
2. **Fix commit** — #88 part-2 implementation + the self-heal attribution + the
   passing additions (framing/order digest test, #80 regression guard). Push,
   watch CI go green.

## Out of scope

- The staleness layer (trigger model, cascade semantics, archive overlay) —
  design-open in the ADR, not implementation.
- #83 itself — landed via PR #120; this branch builds on it.
- #82 — in progress on `perf/82`.
- Migration/back-compat for pre-#83 corpora — the single-user no-migration rule
  applies; rebuilding is the accepted fix.
