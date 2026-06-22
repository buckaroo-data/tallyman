# Auto-recalc on revise (atomic)

- **Status:** Proposed (2026-06-19) on `feat/auto-recalc-on-revise`. This flips one
  decision gate left open in `plans/recalc-mechanism.md` ("Trigger model … No
  auto-cascade-on-revise") to: **revising an alias optionally recomputes its stale
  dependents in the same atomic checkpoint.** The recalc engine already exists; this
  is about *triggering* it from revise, folding it into *one* git transaction, and
  *reporting* what cascaded. No new compute.
- **Builds on:** `recalc.py` (the cone walk + re-point + single checkpoint),
  `catalog_revise` (`tallyman_mcp/server.py`), the companion revise route
  (`tallyman_companion/app.py`), and the normalized cross-process notify/SSE path
  (`_recalc_sse_event` + the `recalc` kind in `/internal/notify`).
- **Branch-state note:** the normalized notify/SSE path lives in commits `44f3519`
  + `f2fd504`, which are on local `feat/89-reactive-recalc` but **not on main**
  (main merged PR #128 at `c212a92`). The viewer-refresh half of this feature
  depends on them. Carrying them onto this branch (or landing them on main first) is
  an open decision — see *Prerequisites*.

## The problem

Today revising an alias and recomputing its dependents is a deliberate two-step:

```
catalog_revise("orders", new_recipe)     # advances alias 'orders'; followers now stale
catalog_scan_staleness()                 # shows by_region / top_regions stale
catalog_recalc(dry_run=False)            # recomputes them, re-points their aliases
```

For a closed, single-user catalog where "I changed this, recompute what depends on
it" is the whole workflow, the scan-and-explicit-recalc dance is friction. We want:

```
catalog_revise("orders", new_recipe)     # advances 'orders' AND cascades to followers, atomically
```

The hard constraint, and the reason this is a plan and not a one-liner, is
**atomic**: the head advance and the whole cascade must be a *single* git
checkpoint, so `reset_to` the step before the revise undoes the revise *and* every
dependent it recomputed, as one unit.

## Why it isn't just "call recalc from revise"

The recalc compute is reusable, but the *public* `recalc()` self-checkpoints, and
calling it from revise would double-commit. The subtlety is which checkpoint owns
the transaction:

- The MCP dispatch decorator (`server.py:146`) and the companion middleware
  (`app.py:377`) already take **exactly one** checkpoint per non-exempt call, *after*
  the handler returns. `checkpoint_catalog` is `git add -A && git commit
  --allow-empty` (`catalog_state.py:277`), so that one checkpoint sweeps the entire
  working tree into a single revision.
- `recalc` is exempt and self-checkpoints **only because of `dry_run`**: a dry run
  must take no checkpoint, and the decorator can't see the flag (it keys on tool
  name), so recalc had to own its checkpoint. Revise has no dry-run — it always
  mutates — so it can lean on the existing per-op checkpoint.

So atomicity is *already there* for revise: whatever revise leaves in the working
tree, the one post-call checkpoint commits as a single revision. The only rule is
that the recalc embedded in revise must **not** take its own checkpoint. That makes
the design a refactor, not new checkpoint plumbing.

## Design: one walk, the existing one checkpoint

### 1. Factor the recalc walk from its checkpoint

Split `recalc.py` so the cone walk is composable into another op's transaction,
with **zero change to `recalc`'s public behavior**:

- `_recalc_walk(project, roots) -> (entries, remap, status, cone)` — drop bogus
  roots, build the cone, replay each recipe, re-point followed aliases, carry config
  forward. **Takes no checkpoint.** This is lines 161–197 of today's `recalc()`,
  lifted out verbatim.
- `recalc(project, roots, *, dry_run=False)` — unchanged signature/return. Real run
  now = `_recalc_walk(...)` then `step = _checkpoint(...) if remap else None`.

The public `recalc` keeps taking exactly one checkpoint; `_recalc_walk` is the
reusable engine.

### 2. Revise embeds the walk; the existing checkpoint captures both

`catalog_revise` (and the companion revise route) stay **non-exempt** — no
`_NO_CHECKPOINT` / `_checkpoint_exempt` change. Revise just runs the checkpoint-free
walk in-line; the dispatch decorator / middleware then takes its single
already-existing checkpoint, which `git add -A` sweeps over the head advance *and*
the cascade together:

```
build_and_persist(revised recipe)               # new head content on disk (uncommitted)
set_alias(name, new_hash, expect_exists=True)    # head advanced (uncommitted)
carry_forward_entry_config(prev_hash, new_hash)

if auto_recalc_enabled(project):
    roots = followers_made_stale_by(name, prev_hash)        # see §3
    entries, remap, status, cone = _recalc_walk(project, roots)   # NO checkpoint
    out["recalc"] = report(entries, remap, status, cone)
# revise returns; the per-op decorator/middleware checkpoint commits the whole
# working tree (head advance + every re-pointed dependent) as ONE revision.
```

`reset_to(step-1)` rolls back the entire cascade atomically because it was one
commit. Critically, revise must call the **checkpoint-free** `_recalc_walk`, never
the public `recalc()` — the latter would add a second, redundant commit before the
decorator's. No checkpoint plumbing changes; the only new state is the
`auto_recalc_enabled` switch (§D1) and the `recalc` sub-report.

Revise is the canonical case, but the same embedding applies to *every* path that
re-points an existing alias — `catalog_promote_diff` and the companion routes — via a
shared `_auto_recalc_after_head_advance(project, name)` helper (§D5). Each is
non-exempt, so each gets its own single per-op checkpoint covering its head advance +
cascade.

### 3. Which roots — the followers of *this* revise

Roots are scoped to the consequences of *this* revise, not the project's whole stale
set. After advancing `name` from `prev_hash`, the directly-stale followers are
exactly the entries whose recorded `from_catalog(name)` (follow=True) head no longer
matches. Reuse `scan` and filter to the alias axis on this name:

```
roots = [h for h, v in scan(project).items()
         if any(r.axis == "alias" and r.ref == name for r in v.reasons)]
```

`_recalc_walk` expands these to the full descendant cone. Pre-existing staleness from
*other* causes (a source edited earlier, a different alias revised-but-not-recalced)
is left untouched — the blast radius of an auto-recalc is precisely what this revise
made stale. (Decision D2 below revisits "scope to this revise" vs "heal everything
stale.")

### 4. Reporting

`catalog_revise` returns its usual `{hash, alias, version, url, carried_over?}` plus,
on the auto path, a `recalc` sub-report: `{cone, entries, remap, status,
checkpoint_step}` — the same shape `catalog_recalc` returns, so a caller (Claude over
MCP, or the SPA) sees exactly what cascaded and whether anything failed. The
companion route returns the same JSON and publishes the `recalc` SSE event via
`_recalc_sse_event(remap, step)`.

### 5. Cross-process + frontend

The MCP auto path reuses the existing `_notify("recalc", {remap, step})` →
`/internal/notify` → `_recalc_sse_event` chain, so a revise driven from Claude
invalidates the companion's caches, reloads buckaroo sessions, and emits the SSE
event with no new wiring. **The frontend still does not consume the `recalc` SSE
event** (the SPA listens for nine kinds, not `recalc`). Auto-recalc does not change
that gap; wiring the listener is the separate Stage C below, required for an open
view to refresh on its own.

### 6. Account for every stale entry (orphan-stale logging + durable recalc failures)

Because auto-recalc recomputes only this revise's followers (§3, D2), it must not
*silently* ignore the rest. The scan it already runs yields the whole directly-stale
set, so partition it for free:

- `followers` — directly-stale with an alias-axis reason whose `ref` is the revised
  name. These are recomputed.
- `orphan_stale` — directly-stale **minus** `followers`. These are entries stale for
  some *other* reason (a source edited on disk, a different alias revised-but-never-
  recalced, or — the alarming case — staleness that leaked in with no explanation).
  Not recomputed; **logged**.

For each `orphan_stale` entry, log at WARNING with its hash, alias, and reasons
(`axis`/`ref`/`was`/`now`), and classify it against the durable error store
(`tallyman_core.errors`, `errors.jsonl`):

- the orphan's hash matches a recorded recalc/build failure → "explained by error
  `<id>`" (a known prior failure left it stale; expected).
- no match → **"UNEXPLAINED stale entry — staleness with no recorded recalc error;
  file a bug."** This is the case to surface loudest: it means an invariant broke
  somewhere we don't yet understand.

`orphan_stale` (with classification) is also returned in the revise / recalc report,
so the caller sees it, not just the log.

For the tie-back to work going forward, **persist recalc failures**, which today are
only in the in-memory report: when `_recalc_walk` marks an entry `failed`,
`record_error(project, code=recipe, message=error, tool=…, hash=old_hash)`. This
needs a small extension — an optional `hash` field on the error record (single-user
repo, no migration concern). It applies to explicit `catalog_recalc` too, so its
failures also become durable and show in the existing `/api/errors` banner.

`errors.jsonl` lives in `artifacts_dir`, **outside** the catalog repo that the
checkpoint's `git add -A` commits, so recording a failure is a side-log that
**survives `reset_to`** — undo the catalog change, keep the forensic record. It is
deliberately not part of the atomic revision.

## Decision gates (for grilling)

Each is localized; the plan states a recommended default and the alternative.

- **D1 — The switch (decided).** A **per-project config file in the catalog repo**
  (`config.json` alongside `aliases.jsonl` in `catalog_dir(project)`), **overridable
  by env var**. It is tracked, so it clones / versions / `reset_to`s with the catalog,
  same as the alias state. Resolution precedence: `TALLYMAN_AUTO_RECALC` env var → the
  config file's `auto_recalc` key → built-in default, which is **ON**. Auto-recalc is
  the intended default workflow; the explicit scan→recalc path stays available by
  setting the flag off (per-project or via env).
- **D2 — Root scope (decided: followers of the revised alias).** Roots = entries this
  revise made stale (§3); the cone expands from there. Blast radius = the consequence
  of this edit; pre-existing staleness from other causes is left alone (and *logged*,
  see §6). Failures in the report are therefore always attributable to this revise.
  (Rejected: recompute the whole directly-stale set — wider, less predictable, and it
  buries unrelated failures in a revise's report.)
- **D3 — Cascade failure (decided: stop-and-report).** The revise always lands. The
  head advance + recomputed prefix commit in the one checkpoint; a failing dependent
  is reported `failed`, the rest `skipped`. A broken *downstream* entry never blocks
  fixing its *upstream*; one failure model shared with `recalc`. `reset_to(step-1)`
  is still available if the partial result is unwanted. (Rejected: all-or-nothing —
  it lets a broken dependent veto an upstream edit and needs an explicit working-tree
  rollback.)
- **D4 — Sync vs async (decided: sync for v1).** Synchronous on both surfaces. MCP is
  naturally synchronous (Claude waits for the report); the companion revise route
  recomputes inline. Known risk: a deep/expensive cone makes the in-browser revise
  HTTP call slow. Async (revise returns, cascade runs in a threadpool with SSE
  progress) is a deliberate follow-up, added only if the latency bites.
- **D5 — Which head-movers trigger (decided: ALL existing-alias head-movers).**
  `catalog_revise` is not the only path that advances an existing alias head, so
  hooking only it would let dependents go silently stale. Trigger auto-recalc from a
  single shared helper `_auto_recalc_after_head_advance(project, name)` called at
  every surface path that re-points an *existing* alias:
  - `catalog_revise` (`server.py:570`, `expect_exists=True`).
  - the companion in-browser revise route (`app.py:1108`).
  - `catalog_promote_diff` (`server.py:845`, the no-flag `set_alias` re-points an
    existing alias) and the companion promote_diff (`app.py:870`).

  The helper hooks at the *surface* level, not at `set_alias` — recalc itself calls
  `set_alias` to re-point dependents, so a lower hook would recurse. Excluded by
  design: `catalog_create` / `catalog_load_parquet` / `catalog_alias` (all
  `expect_exists=False` — a new alias or an error, never a re-point), and `reset_to`
  (moves heads *backward* — must not recompute forward).

  *Bypasses the trigger cannot catch* — `catalog_unalias` + `catalog_create` of the
  same name (sidesteps revise and discards history), `catalog_rename` (followers'
  `ref` resolves to nothing → reported `unknown`, not `stale`; deletion-side, ADR
  Q3), and any future tool or manual edit — are the job of the **§6 orphan-stale
  backstop**: they leave followers stale, which is logged and classified on the next
  auto-recalc. Because that only fires on the next revise, the orphan classification
  also runs inside `catalog_scan_staleness` (the scan-on-load surface), so a project
  load surfaces orphans even when no revise has happened since.

## Staging (TDD)

Per the testing rule: failing tests bundled and seen red on CI first, then the fix.

- **Stage A — MCP backend, behind the flag.**
  - Failing tests (`tests/test_recalc.py` / a new `tests/test_auto_recalc.py`):
    1. *cascade-on-revise*: build `a → b`; `catalog_revise("a", …)` with the flag on
       recomputes `b` and re-points alias `b` **without** any `catalog_recalc` call.
    2. *atomic, one checkpoint*: the revise+cascade advances the catalog step by
       exactly 1; `reset_to(step-1)` restores both `a`'s prior head and `b`'s.
    3. *opt-out*: flag off → revise advances `a`, leaves `b` stale (today's behavior).
    4. *no followers*: revise an alias nothing follows → one checkpoint, empty
       `recalc` report, identical to today.
    5. *stop-and-report*: a broken downstream recipe → revise lands, `recalc.status
       == "failed"`, one checkpoint, prefix committed.
    6. *return shape*: the `recalc` sub-report is present and well-formed.
    7. *orphan-stale logged*: with an unrelated pre-existing source-stale entry, an
       auto-recalc on `a` recomputes only `a`'s followers, leaves the orphan
       untouched, logs it at WARNING, and returns it in `orphan_stale` classified
       UNEXPLAINED (no matching error record).
    8. *failure persisted + tie-back*: a cascade failure writes an `errors.jsonl`
       record carrying the failed entry's `hash`; a later auto-recalc that still sees
       that entry stale classifies it "explained by error `<id>`". The error record
       survives a `reset_to` of the revise.
    9. *config + override*: `auto_recalc` read from catalog-repo `config.json`, with
       `TALLYMAN_AUTO_RECALC` taking precedence; default applies when neither is set.
  - Fix: the config store (`config.json` in the catalog repo + env override), the
    `_recalc_walk` factor (§1), embedding the checkpoint-free walk in revise with **no**
    checkpoint-exemption change (§2), the followers roots filter (§3), the report (§4),
    orphan-stale logging + the `hash` field on `record_error` (§6).
- **Stage B — companion revise route parity.** Failing route tests in
  `tests/test_recalc_surfaces.py`: the in-browser revise cascades, returns the
  `recalc` report, publishes the `recalc` SSE event, is checkpoint-exempt and atomic.
  Then the fix.
- **Stage C — frontend (separate).** SPA `SSEContext` consumes the `recalc` event
  and refetches the remapped entries so an open view refreshes after an auto-recalc.
  Independent of A/B; tracked here only as the dependency that makes the feature
  visible in the viewer.

## Prerequisites

- **The `f2fd504` review fixes — now PR #129.** Stage A's MCP→viewer refresh and
  Stage B's SSE parity assume the normalized `_recalc_sse_event` shape and the
  `recalc` handling in `/internal/notify`, which merged into main at `c212a92` without
  the follow-up fixes. Those fixes (`44f3519` tests + `f2fd504`) are landed as a
  focused PR off main — **#129** (CI green). Once it merges, rebase this branch on
  main. (Stage A's *core* atomic-checkpoint behavior does not need them; only the
  cross-process viewer story does.)
- **`docs/reactive-recalc.md`.** The rewritten doc (currently untracked) describes
  the explicit two-step model and already states "No automatic cascade on revise."
  When this lands, update its *Trigger model* and *Current limits* sections to
  document the opt-in auto path.

## Out of scope

- Auto-recalc on source-file edits (the source axis). This plan triggers on alias
  revision only; a file-watcher that auto-recomputes on disk changes is a different,
  larger feature and runs against the "closed system, no surprise updates" framing.
- Deletion-side semantics (archive / delete-cone), unchanged from
  `plans/recalc-mechanism.md`.
- Alternative cascade policies beyond stop-and-report.
