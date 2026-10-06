"""Start a child process without fork (#305).

Every child tallyman starts goes through this module, never ``subprocess`` or ``os``: ``run`` for one that runs to
completion, ``start`` for a long-lived one (Buckaroo). A forked child of a multithreaded process can die before it
reaches ``exec`` (ADR-001). On macOS it does every time once pyproj was first imported on a thread that has since
exited: ``import pyproj`` opens PROJ's ``proj.db`` and registers a ``pthread_atfork`` child handler that closes it,
Apple's libsqlite3 logs that close through ``os_log``, and ``os_log`` faults in the child. xorq's first pandas
``execute`` imports pyproj (through geopandas) on whatever thread runs it, so a companion route on an AnyIO worker is
enough. ``pin_pyproj`` keeps that from happening in a tallyman process, for the forks a library makes.

``os.posix_spawn`` does not clone the parent's address space or threads and runs no atfork handlers, so neither
hazard can fire. It does none of what ``subprocess`` does for a child either, so ``run`` does it: the signals CPython
ignores are back at their defaults, stdin is /dev/null, and a child whose wait is interrupted is killed and reaped.
``tests/test_fork_safety.py`` statically forbids starting a child anywhere in ``src/`` but here.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

# The signals CPython ignores at startup. A child inherits an ignored signal ignored; subprocess's restore_signals
# resets these, and so does run.
_RESTORED_SIGNALS = (signal.SIGPIPE, signal.SIGXFSZ)

# What a child of ``start`` runs first: put the ignored signals back, close every descriptor above 2 it inherited,
# then exec the real argv. ``subprocess`` closes them only through fork, or POSIX_SPAWN_CLOSEFROM, which macOS lacks.
_CLEAN_EXEC = (
    "import os, signal, sys\n"
    "for s in (signal.SIGPIPE, signal.SIGXFSZ):\n"
    "    signal.signal(s, signal.SIG_DFL)\n"
    "os.closerange(3, max(map(int, os.listdir('/dev/fd'))) + 1)\n"
    "os.execv(sys.argv[1], sys.argv[1:])\n"
)


def pin_pyproj() -> None:
    """Import pyproj on this thread, which must live as long as the process: call it on the main thread, first.

    A later ``import pyproj`` on a worker thread is then a no-op, so the thread that registered PROJ's atfork handler
    never exits and the process's forks stay safe, including the ones ``run`` and ``start`` cannot route: a library's
    own ``subprocess`` call, a multiprocessing worker.
    """
    with contextlib.suppress(ImportError):
        import pyproj  # noqa: F401


def _program(name: str) -> str:
    """*name* as a path to exec. ``posix_spawn`` does not search PATH, so a bare name is resolved on it now."""
    exe = name if os.sep in name else shutil.which(name)
    if exe is None:
        raise FileNotFoundError(f"{name} not found on PATH")
    return exe


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


def _spawn_and_wait(argv: list[str], outputs: list[tuple], timeout: float | None) -> int:
    """Spawn *argv* (``argv[0]`` a path) with stdin on /dev/null and *outputs* for fds 1 and 2; return its exit code."""
    file_actions = [(os.POSIX_SPAWN_OPEN, 0, os.devnull, os.O_RDONLY, 0), *outputs]
    pid = os.posix_spawn(argv[0], argv, os.environ, file_actions=file_actions, setsigdef=_RESTORED_SIGNALS)
    try:
        status = _wait(pid, timeout)
    except BaseException:  # a KeyboardInterrupt mid-wait: leave no child running, and no zombie
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
        with contextlib.suppress(ChildProcessError):
            os.waitpid(pid, 0)
        raise
    return os.waitstatus_to_exitcode(status)


def _read(f) -> str:
    f.seek(0)
    return f.read().decode(errors="replace").strip()


def run(argv: list[str], *, timeout: float | None = None, capture: bool = True) -> tuple[int, str, str]:
    """Run *argv* fork-free; return ``(returncode, stdout, stderr)``.

    A bare ``argv[0]`` is resolved on PATH at call time; a missing program raises ``FileNotFoundError``. A signal
    death (e.g. SIGSEGV, or the *timeout* SIGKILL) is reported as a negative returncode rather than raising, so callers
    can degrade. The child reads /dev/null. Without *capture* its output goes there too, and both strings are empty.
    """
    argv = [_program(argv[0]), *argv[1:]]
    if not capture:
        devnull = [(os.POSIX_SPAWN_OPEN, fd, os.devnull, os.O_WRONLY, 0) for fd in (1, 2)]
        return _spawn_and_wait(argv, devnull, timeout), "", ""
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        outputs = [(os.POSIX_SPAWN_DUP2, out.fileno(), 1), (os.POSIX_SPAWN_DUP2, err.fileno(), 2)]
        rc = _spawn_and_wait(argv, outputs, timeout)
        return rc, _read(out), _read(err)


def start(argv: list[str], *, stderr) -> subprocess.Popen:
    """Start the long-lived *argv* fork-free, with text pipes on its stdin and stdout and *stderr* as its stderr.

    ``subprocess`` takes its ``posix_spawn`` path only for an absolute executable, ``close_fds=False`` and none of the
    arguments that force a fork; ``tests/test_fork_safety.py`` checks this call at run time. ``close_fds=False`` hands
    the child every inheritable descriptor, and a C library opens them that way (PEP 446 covers only Python's), so the
    child is a bare interpreter that closes them and then execs *argv*.
    """
    return subprocess.Popen(
        [sys.executable, "-I", "-S", "-c", _CLEAN_EXEC, _program(argv[0]), *argv[1:]],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr,
        text=True,
        bufsize=1,
        close_fds=False,
    )
