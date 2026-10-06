"""Run a child process without fork (#305).

Every child tallyman runs to completion goes through ``run``, never ``subprocess``. A forked child of a multithreaded
process can die before it reaches ``exec`` (ADR-001). On macOS it does every time once pyproj was first imported on a
thread that has since exited: ``import pyproj`` opens PROJ's ``proj.db`` and registers a ``pthread_atfork`` child
handler that closes it, Apple's libsqlite3 logs that close through ``os_log``, and ``os_log`` faults in the child.
xorq's first pandas ``execute`` imports pyproj (through geopandas) on whatever thread runs it, so a companion route
on an AnyIO worker is enough.

``os.posix_spawn`` does not clone the parent's address space or threads and runs no atfork handlers, so neither
hazard can fire. ``tests/test_fork_safety.py`` statically forbids starting a child through fork in ``src/``.
"""

from __future__ import annotations

import os
import shutil
import signal
import tempfile
import time


def _wait(pid: int, timeout: float | None) -> int:
    """Reap *pid*; SIGKILL it once *timeout* seconds have passed (never, when None).

    Callers may hold the per-project flock, so a wedged child (git's index.lock
    contention, an fs hang) must surface as a signal death, never a stuck thread.
    """
    if timeout is None:
        return os.waitpid(pid, 0)[1]
    deadline = time.monotonic() + timeout
    while True:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done == pid:
            return status
        if time.monotonic() >= deadline:
            os.kill(pid, signal.SIGKILL)
            _, status = os.waitpid(pid, 0)
            return status
        time.sleep(0.01)


def run(argv: list[str], *, timeout: float | None = None) -> tuple[int, str, str]:
    """Run *argv* fork-free; return ``(returncode, stdout, stderr)``.

    A bare ``argv[0]`` is resolved on PATH at call time, because ``posix_spawn``
    does not search PATH; a missing program raises ``FileNotFoundError``. A
    signal death (e.g. SIGSEGV, or the *timeout* SIGKILL) is reported as a
    negative returncode rather than raising, so callers can degrade.
    """
    exe = argv[0] if os.sep in argv[0] else shutil.which(argv[0])
    if exe is None:
        raise FileNotFoundError(f"{argv[0]} not found on PATH")
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        file_actions = [
            (os.POSIX_SPAWN_DUP2, out.fileno(), 1),
            (os.POSIX_SPAWN_DUP2, err.fileno(), 2),
        ]
        pid = os.posix_spawn(exe, [exe, *argv[1:]], os.environ, file_actions=file_actions)
        status = _wait(pid, timeout)
        out.seek(0)
        err.seek(0)
        stdout = out.read().decode(errors="replace").strip()
        stderr = err.read().decode(errors="replace").strip()
    if os.WIFSIGNALED(status):
        return -os.WTERMSIG(status), stdout, stderr
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status), stdout, stderr
    return -1, stdout, stderr
