# Plan: failing integration tests for the catalog ↔ xorq coexistence and reset round-trips

Status: **proposed — awaiting review.** Test-only PR. No production fix in this branch.

## Why

Issue #48 (aliases.json/alias_history.json committed into the xorq catalog repo →
`assert_consistency` fails → every `xorq catalog add` after the first silently
no-ops) shipped through all of V0 unnoticed. The reason it hid: the only
integration test that exercises real `xorq catalog add`
(`test_xorq_catalog_add_registers_after_genesis`) adds **exactly one** entry, so
it never crosses the checkpoint that commits the two bookkeeping files. Every
other reset/catalog test uses synthetic `entries/aaaa` dirs and never runs xorq
at all.

Reproduced both bugs against the real demo flow (`genesis → catalog_create →
catalog_create → reset_to`), driven through the MCP tools so the checkpoint
wrapper fires exactly as in production:

1. **#48** — with two real entries, only entry 1's `entries/<hash>.zip` is
   git-tracked; entry 2's add silently no-ops; `Catalog.from_kwargs(init=False)`
   raises `AssertionError`.
2. **reset ↔ xorq-catalog divergence (the second bug)** — a back→forward reset
   round-trips the *tallyman* `entries/<hash>/` dir through the bullpen, but the
   xorq `.zip` recipe is reconciled by a different mechanism (`git reset`) and is
   never regained. The two views of "what entries exist" drift apart, and
   `reset_to` never re-validates the xorq catalog. "Git-backed, recompute from
   history" is false for any entry after the first.

```
[after both]   dirs=[023fb..,49b47..]  tracked_zips=[49b47..zip]   head=2
[reset->s1]    dirs=[49b47..]          tracked_zips=[49b47..zip]   head=1
[forward->s2]  dirs=[023fb..,49b47..]  tracked_zips=[49b47..zip]   head=2   # entry2 recipe never durable
```

## Scope

- **Test-only.** This branch adds failing integration tests and nothing else. No
  edits to `src/`. The #48 fix is a separate future PR.
- Every test marked `@pytest.mark.integration` so it runs in the CI `integration`
  job (visible failing on CI before any fix lands — satisfies the TDD rule).
- Driven through the real surfaces: `tallyman_mcp.server.catalog_create` /
  `catalog_revise` (which apply the checkpoint wrapper, the actual #48 trigger),
  not synthetic dirs.

## New file

`tests/test_catalog_xorq_integration.py` — a cohesive suite. `test_reset_to_revision.py`
is already 637 lines and its one related integration test
(`test_xorq_catalog_add_registers_after_genesis`) stays where it is; the new
multi-entry/reset-coexistence suite is more discoverable in its own file. Shared
helpers (`_git`, `_open_xorq_catalog`, `_tracked_entry_zips`) live in the new file.

## Helpers

- `_git(project, *args)` — shell to `git -C <catalog_dir>` (the sanctioned test
  exception to the no-bare-git guard, same as the existing reset suite).
- `_tracked_entry_zips(project) -> set[str]` — `git ls-files entries/*.zip`,
  stripped to bare hashes. The durable, content-addressed xorq artifact set.
- `_open_xorq_catalog(project)` — `Catalog.from_kwargs(path=catalog_dir, init=False)`;
  returns the catalog or re-raises so a test can assert it opens.
- `_make_entries(project, n)` — drive `catalog_create` n times with distinct
  aggregations over `orders_parquet`, returning the hashes/steps.

## Tests — all fail today (this PR)

### Group A — multi-entry catalog add (#48 core)

1. `test_every_entry_registers_in_xorq_catalog`
   Create 3 real entries via `catalog_create`. Assert every entry hash is in
   `_tracked_entry_zips`. **Today:** only the first → fails.

2. `test_catalog_openable_after_checkpoint`
   genesis + one `catalog_create` (which checkpoints). Assert
   `_open_xorq_catalog` does not raise. **Today:** `AssertionError` → fails.
   Directly pins the `assert_consistency` root cause.

3. `test_bookkeeping_files_not_tracked_in_xorq_repo`
   Assert `git ls-files` contains neither `aliases.json` nor `alias_history.json`.
   **Today:** both tracked → fails. This is the precise #48 invariant; the fix
   (move them out of `catalog_dir`) makes it pass.

### Group B — reset round-trips keep the xorq catalog consistent (second bug)

4. `test_reset_keeps_xorq_catalog_consistent`
   Build 2 entries, `reset_to` step-1, assert `_open_xorq_catalog` opens.
   **Today:** unopenable → fails.

5. `test_xorq_entry_set_matches_tallyman_pointers`
   At each step, assert `_tracked_entry_zips(project)` == set of
   `entry_hashes` in catalog.yaml. **Today:** diverges (1 zip vs 2 pointers) → fails.

6. `test_back_then_forward_restores_xorq_recipe`
   2 entries → reset back to step-1 → reset forward to step-2. Assert entry 2 is
   durably present in the xorq catalog (tracked `.zip`), not only as a bullpen
   dir copy. **Today:** zip never existed → fails. This is the divergence pinned
   end to end.

### Group D — silent-swallow surfacing (#48 secondary)

7. `test_failed_registration_is_observable`
   Force a failing `xorq catalog add` (run against the already-inconsistent
   catalog) and assert the failure is observable rather than reported as success.
   Proposed assertion: an ERROR-level log record is emitted (`caplog`) — matching
   the issue's "log it at error level" suggested fix. **Today:** only WARNING in
   the best-effort wrapper, and `build_and_persist` discards the return value →
   no ERROR → fails.
   *Open question for review:* keep this loose ("not silently swallowed" — assert
   `build_and_persist` exposes a registration-failed signal) or specific (ERROR
   log + `errors.jsonl` row)? The specific form is more fix-shaped; the loose
   form pins the property without dictating the API. Default: loose, asserting a
   `BuildResult.catalog_registered`-style signal exists and is `False` on failure.

## Deferred to the fix PR (NOT in this branch)

- `test_reset_rolls_back_alias_state` — **passes today** because `aliases.json` is
  git-tracked and `git reset --hard` rolls it back. Once the #48 fix moves the
  bookkeeping files out of the repo, reset stops rolling back alias state unless
  the fix reconciles it explicitly. So this is a regression-guard that must ship
  *with* the fix, per the rule that a test passing today is never bundled into a
  failing-tests commit. Noted here so the fix PR doesn't forget it.

## Branch / PR / CI flow

- Branch `test/catalog-xorq-integration` off `main`.
- Run `uv run pytest -m integration tests/test_catalog_xorq_integration.py`
  locally first; confirm all Group A/B/D tests fail for the expected reasons
  (assertion text, not collection/import errors).
- Commit the failing tests as a single commit (all bugs, one commit — they share
  the "seen failing on CI" axis).
- Push `-u`, open PR, watch the CI `integration` job go red.
- PR body: links #48, states this is the failing-test half of a TDD split, lists
  which assertion pins which bug, and names the deferred alias-rollback guard.

## Cost / risk

- Each test runs real xorq builds (~3–15s each per the integration marker doc).
  Entry counts kept at 2–3; ~7 tests → roughly 1–2 min added to the integration
  job. Acceptable; integration job is already separate and not on the fast path.
- No `src/` changes, so the fast suite and lint are untouched.
