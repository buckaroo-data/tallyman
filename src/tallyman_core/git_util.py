"""Fork-safe git primitive (#25).

Every runtime git call in ``src/`` must go through here, never bare
``subprocess.run(["git", ...])``. Bare git forks; forking ``git`` from the
long-lived multithreaded companion can ``SIGSEGV`` on macOS (the demo
platform), and Linux CI will not reproduce it — so ``test_reset_to_revision``
statically forbids ``subprocess`` git in ``src/`` and routes everything here.

``run_git`` is ``spawn.run`` with ``git`` in front: ``os.posix_spawn``, a
vfork-style spawn that does not clone the parent's address space or threads,
so the hazard cannot fire by construction. ``_git_state_guard`` runs xorq's
provenance capture through it too.
"""

from __future__ import annotations

from pathlib import Path

from tallyman_core.spawn import run


def run_git(args: list[str], *, cwd: Path | str | None = None, timeout: float = 10.0) -> tuple[int, str, str]:
    """Run ``git <args>`` fork-free; return ``(returncode, stdout, stderr)``.

    *cwd* is passed as ``git -C <cwd>`` (arg-based, so no chdir/fork plumbing).
    A signal death (e.g. SIGSEGV, or the *timeout* SIGKILL) is reported as a
    negative returncode rather than raising, so callers can degrade. ``git``
    is resolved on PATH at call time because ``posix_spawn`` does not search
    PATH.
    """
    argv = ["git"]
    if cwd is not None:
        argv += ["-C", str(cwd)]
    return run(argv + list(args), timeout=timeout)
