# Tests needed before any concurrency change

The ten proposals in `plans/concurrency-and-responsiveness.md` (P1 to P10; that plan and
`plans/concurrency-investigation-report.md` are local files, not committed) change locking, threading, process
boundaries and what a tool call promises. Today's tests guard the locks themselves. They do not guard most of what those
proposals change. This file lists the tests to land first, in their own PR, so that a regression from a later
proposal shows up as a red test instead of a user report.

This file is the spec. It contains no tests. Source for most of them already exists on the local branches
`investigate/p1..p10` (each has its own `plans/investigations/P<N>-*.md`; none are pushed).

## State of main (4e505b8)

- `uv run pytest`: 1126 passed, 1 failed. The failure is `test_buckaroo_version_lockstep.py::test_installed_js_core_matches_pin`
  (local `node_modules` has buckaroo-js-core 0.15.6, `package.json` pins 0.15.9). Not related to concurrency.
- `tests/test_execution_lock.py` followed by `tests/test_git_state_guard.py` fails in 5 of 8 runs: `git version` exits
  with -11 (SIGSEGV) during `import git`, so two guard tests error at setup (`test_installed_guard_dispatches_git_fork_free`,
  `test_guard_reflects_repo_state_changes`). In a larger run, 16 tests in `test_diff.py` failed the same way
  (`ImportError: Failed to initialize: Cmd('git') failed ... exit code(-11)`). The reverse order, and the full default
  run, passed. The variable is whether a fork happens while a DataFusion (tokio) worker thread is running; which runs
  hit it depends on timing at the fork. `ADR-001-git-subprocess-threading.md` guards xorq's `get_git_state`;
  GitPython's import-time `git version` is a path it does not cover.

## What is guarded today

| Behavior | Test |
|---|---|
| Two threads on the shared backend (`Already borrowed`) | `test_execution_lock.py` |
| `project_lock` across a build, failed concurrent build cannot delete the winner | `test_project_lock.py` |
| Cold heal runs once | `test_result_cache.py::test_concurrent_cold_heal_is_single_flighted` |
| Digest definition | `test_digest.py` |
| One checkpoint per tool call | `test_reset_to_revision.py` |
| Concurrent Buckaroo opens share one build dir | `test_buckaroo.py` (`test_unit_concurrent_*`) |
| Buckaroo load timeout is classified | `test_buckaroo_session.py::test_load_session_classifies_timeout_then_error` |

## What is not guarded

Nothing pins: the companion's event loop staying free (P1, P2); concurrent creates keeping their aliases (P8); a
concurrent or parallel materialize giving the same digest and rows (P7); a diff built ahead equalling one built on
demand (P5); a clean exit with work in flight (P6, P9); a fork after DataFusion threads surviving; a call failing fast
instead of hanging (timeouts); Buckaroo first-click latency (P3). The perf suites that touch latency are local-only
(`perf`, `perf_tier_a` are excluded in `addopts`).

## Tests to write

Order of commits follows the repo's TDD rule: failing tests first, pushed and seen red on CI, then the markers that
make them green. A test that is expected to fail on main until a proposal lands is marked
`@pytest.mark.xfail(strict=True, reason="P<N>: ...")`, so the proposal's PR has to delete the marker when it fixes the
behavior. Everything here runs in the `fast` CI job unless marked.

### 1. Event loop stays free (P1, P2)

`tests/test_event_loop_free.py`, taken from `investigate/p1-event-loop` as is (10 tests, a real uvicorn on an
ephemeral port, stubbed slow work, polls `/api/version` and SSE `hello` while the slow request runs). On main 9 fail,
for the six `async def` routes that block the loop (`put_code`, `api_promote_diff`, `notify`, `api_reset`,
`api_recalc`, `_auto_recalc_after_head_advance`), plus the SSE and thread-pool starvation checks and the AST rule
`test_no_async_def_calls_a_blocking_function`. One passes. Mark the 9 strict xfail with `P1`.

### 2. Contracts that pass on main and must keep passing

New file `tests/test_concurrency_contracts.py`. All of these should pass today; verify that before committing.

1. Concurrent creates keep every alias. 8 threads each call `srv.catalog_create` with a distinct name over one
   source. Afterwards every alias exists and resolves to its own entry, the working tree is clean, and there is one
   checkpoint commit per call. (P8 lost 15 of 100 aliases without extra changes; nothing on main would notice.)
2. The same across processes. 3 subprocesses (`sys.executable`, shared `TALLYMAN_HOME`) each create 4 aliases.
   Reuse `test_alias_and_notebook_writes_from_two_processes_are_not_lost` and
   `test_two_builds_resolving_one_alias_at_once_both_read_the_same_head` from
   `investigate/p8-narrow-lock:tests/test_narrow_lock_exposures.py`, minus anything that imports the new
   `narrow_lock_enabled` API.
3. Concurrent materialize of one entry from 4 threads gives one digest, and it equals the manifest's
   `result_digest` and `file_digests(snapshot)`. `check_reproducible=True` reports reproducible for a deterministic
   query.
4. The two reproducibility passes write the same bytes and the same digest and row order as each other (the
   contract P7a must keep when it runs them in parallel). Adapt
   `test_serial_and_parallel_passes_write_the_same_bytes_and_digest` from `investigate/p7-pool-and-parallel-passes`
   to the serial code that exists on main.
5. A failure in either pass leaves no temp file behind (adapt the `[first-pass]`/`[both]` cases from the same file).
6. A cold heal after the snapshot is deleted, started from a second process while a thread of this one also heals,
   builds it once and verifies against the recorded digest (extends `test_concurrent_cold_heal_is_single_flighted`
   across processes).
7. Diff: two concurrent `catalog_diff` calls of one pair both succeed and equal a serial call, and a second call
   (warm) returns the same result as the first (cold). This is the contract P5 relies on to precompute a diff.
   See `test_diff_precompute.py` on `investigate/p5-prebuild-diff` for the comparison helpers.

### 3. Fork after DataFusion threads

New file `tests/test_fork_safety.py`. Each test runs in a child interpreter so a crash is a return code, not a dead
pytest.

1. A worker thread loops a DataFusion aggregate over a 300k-row parquet while the main thread runs
   `subprocess.run(["git", "version"])` 200 times (bare program name, so `fork_exec`). Assert no negative return code.
   Expected to fail on main. Before choosing the iteration count, measure it: it must fail on main in nearly every run,
   otherwise the test is a coin flip. If it cannot be made reliable, mark it `xfail(strict=False)`. Not `strict=True`:
   an XPASS would fail an unrelated run.
2. The same churn with `os.posix_spawn` (or `Popen(close_fds=False)`, which takes that path) never fails. Passes on
   main; pins the fix the proposals rely on.
3. The same churn through the code that forks in tallyman today: the `cp -c` clone in
   `src/tallyman_xorq/source_identity.py:69` and the `Popen` in
   `src/tallyman_companion/buckaroo_lifecycle.py:208`. Fails on main until those are moved to `posix_spawn`.
4. Suite hygiene: `tests/conftest.py` imports `git` at module level, before any DataFusion thread exists. Acceptance:
   the `test_execution_lock.py` + `test_git_state_guard.py` pair passes 8 of 8 runs (it fails 5 of 8 today).

### 4. Process hygiene (marked `integration`)

New tests next to `tests/test_mcp_stdio.py`, reusing its spawn helper.

1. Close the MCP server's stdin after one tool call. It exits 0 within 10 s and no child process of it remains.
2. A background thread running a long DataFusion query when the process exits does not hang the exit (P9's daemon
   thread hang). Passes on main because there are no jobs; it is the tripwire for P9.
3. Companion shutdown leaves no Buckaroo child (`test_integration_stop_cleans_up` covers this; keep it and extend to
   "even when a load is in flight").

### 5. Buckaroo first click (one `integration` test, loose budget)

Create an entry, open its session through the companion, fetch the first grid window. Assert it arrives within 15 s
(cold is about 2 s today). This is a tripwire for P3 and P4, not a measurement. Everything else about the Buckaroo
handoff already has a `FakeBuckaroo` in `test_buckaroo_handoff.py`; P3's pre-warm test should assert on that fake, not
on a real Buckaroo. P4 changes Buckaroo's own handlers, so its regression tests belong in the Buckaroo repo.

### 6. Timeouts (blocked on a decision)

Nothing to pin until the "Fail at five seconds" questions in section 7 of the plan are answered (question 4: is a
five-second failure acceptable while the work continues, or must the work stop; which calls; what the error looks like). Once decided, write `xfail(strict=True)` tests: a call that
exceeds its budget returns the agreed error within the budget plus a small margin, and leaves no held lock.

## Not in this PR

- The fixes (posix_spawn for the two fork sites, P1 and so on). Tests only.
- Latency budgets tighter than "a convoy of tens of times fails". A CPU-bound Python thread slowed a digest from 1 s
  to 73 s in the probes; a ceiling of a few multiples of the solo time on a 20k-row entry could catch that, but it
  needs measuring on CI hardware first, so it is optional and can go behind the `perf` marker.
- Branch tests that exercise code that does not exist yet (`test_prewarm.py`, `test_workers.py`, `test_mcp_jobs.py`,
  `test_job_progress.py`, `test_load_jobs.py`, `test_narrow_project_lock.py`, `test_execution_pool.py`,
  `test_diff_precompute.py`, `test_buckaroo_threads.py`). They land with their proposals.
