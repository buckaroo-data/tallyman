"""Fork-safe git primitive (#25).

Every runtime git call in ``src/`` must go through here, never bare
``subprocess.run(["git", ...])``. Bare git forks; forking ``git`` from the
long-lived multithreaded companion can ``SIGSEGV`` on macOS (the demo
platform), and Linux CI will not reproduce it — so ``test_reset_to_revision``
statically forbids ``subprocess`` git in ``src/`` and routes everything here.

``os.posix_spawn`` is a vfork-style spawn that does not clone the parent's
address space or threads, so the hazard cannot fire by construction. This is
the same spawn approach ``_git_state_guard`` uses for xorq's provenance capture.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path


def run_git(args: list[str], *, cwd: Path | str | None = None, timeout: float = 10.0) -> tuple[int, str, str]:
    """Run ``git <args>`` fork-free; return ``(returncode, stdout, stderr)``.

    *cwd* is passed as ``git -C <cwd>`` (arg-based, so no chdir/fork plumbing).
    A signal death (e.g. SIGSEGV) is reported as a negative returncode rather
    than raising, so callers can degrade. ``git`` is resolved on PATH at call
    time because ``posix_spawn`` does not search PATH.
    """
    git = shutil.which("git")
    if git is None:
        raise FileNotFoundError("git not found on PATH")
    full = [git]
    if cwd is not None:
        full += ["-C", str(cwd)]
    full += list(args)
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        file_actions = [
            (os.POSIX_SPAWN_DUP2, out.fileno(), 1),
            (os.POSIX_SPAWN_DUP2, err.fileno(), 2),
        ]
        pid = os.posix_spawn(git, full, os.environ, file_actions=file_actions)
        _, status = os.waitpid(pid, 0)
        out.seek(0)
        err.seek(0)
        stdout = out.read().decode(errors="replace").strip()
        stderr = err.read().decode(errors="replace").strip()
    if os.WIFSIGNALED(status):
        return -os.WTERMSIG(status), stdout, stderr
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status), stdout, stderr
    return -1, stdout, stderr
