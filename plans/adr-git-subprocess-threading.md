# ADR: Calling git from the multithreaded tallyman server

- **Status:** Accepted (2026-06-03)
- **Context ticket:** buckaroo-data/nokernel-notebooks#25
- **Affected code:** `src/tallyman_xorq/_git_state_guard.py`, the `run_git()` primitive in `tests/git_multithread_reliable.py`, `xorq.common.utils.logging_utils.get_git_state`

## Problem

xorq records git provenance by shelling out to `git rev-parse HEAD`, `git diff`, and `git diff --cached` from `logging_utils.get_git_state`, called inline on the catalog-write/compile path. The tallyman companion server is long-lived and multithreaded, so these calls run while other threads are active.

On macOS, `fork()` from a multithreaded process is unsafe: the child inherits locks (malloc, dyld, CoreFoundation) held by threads that do not exist in the child, and can die with SIGSEGV/SIGABRT before reaching `exec`. `subprocess` reports that as the spawned command dying with a negative returncode (e.g. -11). xorq's `get_git_state` had no error containment, so that propagated out of `build_expr` and failed every catalog write — a best-effort logging detail became a hard blocker.

`subprocess.check_output(["git", ...])` with a bare program name is the exact shape that forks: CPython only takes the fork-free `posix_spawn` path when the executable has a directory component and the `close_fds` requirement is satisfiable. A bare `"git"` always forks.

xorq makes these calls **repeatedly** throughout the run (once per build), not once at startup. That rules out a capture-once strategy.

## Investigation

We tried to reproduce the crash with two probes (`/tmp/crash_probe.py`, `/tmp/crash_probe2.py`) that drive git from many threads under heavy load. Findings:

- The probes use 64 threads in tight allocation loops as background load. A control with the same 64 threads and **no git call at all** hung the interpreter outright (the main thread made zero progress in 25s); 64 *idle/sleeping* threads ran fine. The "deadlock" the probes show is GIL starvation from CPU-bound Python threads, reproduced before any git spawn. It is not the git fork hazard.
- Because of that starvation, the probes' "posix_spawn deadlocks" and the single-call fork-vs-spawn split were artifacts of GIL scheduling during thread ramp-up, not fork-safety properties.
- Under a faithful backdrop (8 threads that initialize and periodically touch CoreFoundation but are I/O-bound, like a real server), both bare-`fork` `subprocess` and `os.posix_spawn` completed 400 git calls with 0 errors. The production SIGSEGV is a rare race, not a deterministic failure.
- CPython issue python/cpython#137109 ("multi-threading + fork warning when threads are stopped before fork") is tangential: it refines the 3.13 fork-with-threads *warning* (fixed in gh-141438, backported to 3.13/3.14). It confirms CPython treats fork-from-threads as hazardous; it does not bless `subprocess` or offer a new escape.

Two conclusions: the parent process never dies (the abort kills the forked child, surfaced as a returncode), so containment in the caller is sufficient to keep the server up; and `posix_spawn` removes the race by construction rather than by luck, because it does not clone the address space or the other threads.

## Decision

1. **Spawn every runtime git call fork-free via `os.posix_spawn`.** Use the `run_git()` pattern from `tests/git_multithread_reliable.py` (dup2 stdout to a temp file, stderr to `/dev/null`, `posix_spawn`, `waitpid`, check `WIFSIGNALED`/`WEXITSTATUS`). Calling `os.posix_spawn` directly avoids depending on CPython's `subprocess` heuristics (an absolute path alone is not enough on this build because `CLOSEFROM` is False; it would also need `close_fds=False`).

2. **Wrap each call in catch-and-degrade.** Any failure — signal death, missing git, timeout, odd repo state — returns placeholder provenance (`{"commit": "unknown", "diff": "", "diff_cached": ""}`) instead of raising. Provenance must never fail a write.

3. **Drop the permanent one-shot cache.** The current `_git_state_guard.py` caches the first capture in a global `_raw_state` forever, which freezes provenance: every build after the first would record stale diffs once the user edits files. Each call should reflect repo state at that build.

4. **Optional short-TTL coalescing only if needed.** If a single compile triggers a burst of calls, a short TTL (1–2s) keyed by `(repo, command)` may coalesce them. Default to no cache; add it only if it shows up in a profile. Correctness (fresh state per build) takes precedence over avoiding the spawn.

The fix stays in-repo (the guard swaps `lu.get_git_state`), not in xorq.

## Alternatives considered

- **Capture once at single-threaded startup + cache forever.** Rejected: xorq calls git per build, so this both freezes provenance and protects only the first call.
- **Separate long-lived git-worker process with IPC.** Rejected as disproportionate. It would give the same safety as `posix_spawn` (git forked from a clean context) but adds IPC, lifecycle, and restart logic for three commands run per build. Kept as a back-pocket escape hatch if `posix_spawn` ever proves insufficient.
- **Absolute path + `close_fds=False` via `subprocess`.** Works but depends on CPython internals that the docstring in `tests/git_multithread_reliable.py` documents as fragile. Calling `os.posix_spawn` directly is more robust.

## Consequences

- The macOS fork-from-multithreaded crash becomes structurally impossible for git provenance (item 1), and harmless even if it somehow occurred (item 2): the server stays up, worst case is "unknown" provenance for one build.
- Provenance is fresh per build again (item 3).
- `posix_spawn` protects against the fork hazard, not GIL starvation. It is not a defense against a future workload with many CPU-bound threads; that is a separate problem (process pool / subinterpreters / free-threaded build) and out of scope here.
- Tests should use a faithful backdrop (idle / CoreFoundation-touching threads), not tight-allocation churn, so they exercise the real spawn path instead of an interpreter-starvation artifact. `tests/repro_git_state_segfault.py` already proves containment deterministically with a fake SIGSEGV-on-invoke git.
