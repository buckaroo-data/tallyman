#!/usr/bin/env python3
"""Run git reliably from a multithreaded Python program (no xorq, raw stdlib).

THE HAZARD
----------
`subprocess.check_output(["git", ...])` with a BARE program name is dispatched via
fork()+exec(), not posix_spawn. CPython only takes the posix_spawn fast path when
`os.path.dirname(executable)` is truthy (see subprocess.Popen._execute_child) — a
bare "git" has no directory, so it forks.

On macOS, fork() from a multithreaded process is unsafe: the child gets only the
forking thread but inherits mutexes other threads were holding (malloc, dyld,
Foundation). The child can crash before it even reaches exec, which `subprocess`
reports as the spawned command "dying" with SIGSEGV/SIGABRT — intermittently,
depending on what the other threads were doing at fork time.

THE FIX
-------
os.posix_spawn() uses a vfork-style spawn that does NOT clone the parent's address
space or threads, so it is fork-safe by construction. `run_git()` below uses it
directly, so it never forks no matter how CPython's heuristics change.

(A subprocess-based alternative also works, but is finicky: CPython only picks
posix_spawn when the executable has a directory component AND
`(not close_fds or _HAVE_POSIX_SPAWN_CLOSEFROM)`. On this build CLOSEFROM is False,
so an absolute path is NOT enough — you also need `close_fds=False`. Calling
os.posix_spawn directly avoids depending on those internals.)

This script:
  1. proves which low-level primitive each approach uses (fork vs posix_spawn),
  2. stress-tests run_git() from many threads under hostile allocator load — 0 errors,
  3. runs the naive approach under the same load in an ISOLATED child process, so a
     crash there is observed (not fatal to us).

Run:  python3 git_multithread_reliable.py   (exit 0 == reliable path verified)
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

GIT = shutil.which("git")


# ---------------------------------------------------------------------------
# THE RELIABLE PRIMITIVE — git via os.posix_spawn (fork-free).
# ---------------------------------------------------------------------------
def run_git(args, cwd=None) -> str:
    """Run `git <args>` without forking. Returns stripped stdout; raises on failure."""
    if GIT is None:
        raise FileNotFoundError("git not found on PATH")
    argv = [GIT, "-C", str(cwd), *args] if cwd else [GIT, *args]
    devnull = os.open(os.devnull, os.O_WRONLY)
    try:
        with tempfile.TemporaryFile() as out:
            file_actions = [
                (os.POSIX_SPAWN_DUP2, out.fileno(), 1),  # stdout -> temp file
                (os.POSIX_SPAWN_DUP2, devnull, 2),  # stderr -> /dev/null
            ]
            pid = os.posix_spawn(GIT, argv, os.environ, file_actions=file_actions)
            _, status = os.waitpid(pid, 0)
            if os.WIFSIGNALED(status):
                raise RuntimeError(f"git {args} killed by signal {os.WTERMSIG(status)}")
            if not (os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0):
                raise RuntimeError(f"git {args} exited non-zero (status={status})")
            out.seek(0)
            return out.read().decode().strip()
    finally:
        os.close(devnull)


def naive_git(args) -> str:
    """The hazardous form: bare program name -> fork()+exec()."""
    return subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL).decode().strip()


# ---------------------------------------------------------------------------
# 1. Mechanism proof. subprocess has exactly two dispatch paths: os.posix_spawn
#    (fork-free) or _posixsubprocess.fork_exec (forks — the hazard). We wrap
#    os.posix_spawn and count it; if a spawning call did NOT use posix_spawn, it
#    necessarily forked. (Patching fork_exec is unreliable — subprocess binds it
#    via a path our reference doesn't reach — so we infer it.)
# ---------------------------------------------------------------------------
_posix_spawn_count = 0
_real_posix_spawn = os.posix_spawn


def _counting_posix_spawn(*a, **k):
    global _posix_spawn_count
    _posix_spawn_count += 1
    return _real_posix_spawn(*a, **k)


def mechanism(label, fn):
    global _posix_spawn_count
    os.posix_spawn = _counting_posix_spawn
    before = _posix_spawn_count
    try:
        fn()
    finally:
        os.posix_spawn = _real_posix_spawn
    used_posix_spawn = _posix_spawn_count > before
    prim = "posix_spawn" if used_posix_spawn else "fork()+exec()"
    note = "fork-free — safe" if used_posix_spawn else "FORKS — unsafe from threads"
    print(f"  {label:<46} -> {prim:<14} ({note})")


# ---------------------------------------------------------------------------
# 2. Hostile multithreaded load + reliability stress for run_git.
# ---------------------------------------------------------------------------
def _allocator_noise(stop: threading.Event):
    """Keep other threads churning the allocator/GIL — the fork-hazard backdrop."""
    while not stop.is_set():
        _ = [bytearray(4096) for _ in range(64)]
        _ = "".join(str(i) for i in range(256))


def stress(fn, label, n_threads=24, iters=40):
    expected = fn(["rev-parse", "HEAD"])  # baseline on the main thread
    errors, oks = [], []
    lock = threading.Lock()
    start = threading.Barrier(n_threads)

    def worker():
        start.wait()
        for _ in range(iters):
            try:
                r = fn(["rev-parse", "HEAD"])
                with lock:
                    oks.append(r == expected)
            except BaseException as e:  # noqa: BLE001
                with lock:
                    errors.append(repr(e))

    stop = threading.Event()
    noise = [threading.Thread(target=_allocator_noise, args=(stop,), daemon=True) for _ in range(6)]
    for t in noise:
        t.start()
    workers = [threading.Thread(target=worker) for _ in range(n_threads)]
    t0 = time.time()
    for t in workers:
        t.start()
    for t in workers:
        t.join()
    stop.set()
    total = n_threads * iters
    print(f"  {label}: {len(oks)}/{total} succeeded, {len(errors)} errors, {sum(oks)} correct, {time.time() - t0:.2f}s")
    if errors:
        print(f"    first error: {errors[0]}")
    return len(errors) == 0 and sum(oks) == total, expected


# ---------------------------------------------------------------------------
# 3. Run the NAIVE (forking) approach under load in an isolated child process,
#    so if it crashes the multithreaded program we observe it instead of dying.
# ---------------------------------------------------------------------------
_NAIVE_CHILD = r"""
import subprocess, threading, sys
def noise(stop):
    while not stop.is_set():
        _ = [bytearray(4096) for _ in range(64)]
        _ = "".join(str(i) for i in range(256))
stop = threading.Event()
for _ in range(8):
    threading.Thread(target=noise, args=(stop,), daemon=True).start()
errs = 0
def work():
    global errs
    for _ in range(60):
        try:
            subprocess.check_output(["git","rev-parse","HEAD"], stderr=subprocess.DEVNULL)
        except Exception:
            errs += 1
ts = [threading.Thread(target=work) for _ in range(24)]
for t in ts: t.start()
for t in ts: t.join()
stop.set()
print(f"naive child finished: {errs} CalledProcessError(s)")
"""


def naive_under_load_isolated() -> str:
    """Returns a human description of how the forking approach fared."""
    p = subprocess.run(
        [sys.executable, "-c", _NAIVE_CHILD],
        capture_output=True,
        text=True,
        cwd=os.getcwd(),
    )
    if p.returncode < 0:
        return f"CRASHED — child killed by signal {-p.returncode} (the fork hazard fired)"
    if p.returncode != 0:
        return f"failed (exit {p.returncode}): {p.stderr.strip()[:200]}"
    tail = (p.stdout.strip().splitlines() or ["<no output>"])[-1]
    return f"survived this run (exit 0): {tail}  [latent race — not guaranteed]"


def main():
    if GIT is None or not os.path.isdir(".git"):
        print("Run this from inside a git repo with git installed.")
        sys.exit(2)

    print("=" * 72)
    print("MECHANISM — which dispatch does each use?  (fork from threads is the hazard)")
    print("=" * 72)
    mechanism("naive: subprocess(['git', ...])", lambda: naive_git(["rev-parse", "HEAD"]))
    mechanism("run_git (os.posix_spawn)", lambda: run_git(["rev-parse", "HEAD"]))
    mechanism(
        "subprocess([ABS_GIT, ...])  (default close_fds)",
        lambda: subprocess.check_output([GIT, "rev-parse", "HEAD"], stderr=subprocess.DEVNULL),
    )
    mechanism(
        "subprocess([ABS_GIT, ...], close_fds=False)",
        lambda: subprocess.check_output([GIT, "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, close_fds=False),
    )

    print("\n" + "=" * 72)
    print("RELIABILITY — run_git from 24 threads x 40 iters under allocator load")
    print("=" * 72)
    ok, commit = stress(run_git, "run_git (posix_spawn)")
    print(f"  resolved HEAD = {commit}")

    print("\n" + "=" * 72)
    print("CONTRAST — the naive forking approach under the same load (isolated child)")
    print("=" * 72)
    print(f"  {naive_under_load_isolated()}")

    print("\n" + "=" * 72)
    print("SUMMARY")
    print("=" * 72)
    print(f"  [{'PASS' if ok else 'FAIL'}] run_git is reliable under heavy multithreading")
    print("  Use run_git() (os.posix_spawn) — it never forks, so the macOS")
    print("  fork-from-multithreaded crash cannot occur.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
