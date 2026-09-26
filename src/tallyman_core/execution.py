"""One execution at a time on the shared backend, per process (#118).

Tallyman executes an entry's reads on one default backend per process, xorq's DataFusion session
(``xorq.config.default_backend``). Two threads executing on it at once fail with ``RuntimeError: Already borrowed``,
and both processes of a normal tallyman execute from many threads: the companion serves page reads from FastAPI's
thread pool and also builds and heals, and FastMCP runs tool calls on a thread pool. ``execution_lock`` is held around
every execution, so they run one at a time in each process. Another process has its own backend and its own lock.

The lock order is ``project_lock`` first, then ``execution_lock``. A heal takes ``project_lock`` and then executes, so a
thread that held the execution lock and then waited for a project lock could wait forever on a thread that holds that
project lock and waits to execute. Anything that can heal (``cached_result_expr``, ``ensure_materialized``) therefore
runs before the lock is taken::

    expr = cached_result_expr(project, content_hash)
    with execution_lock():
        df = expr.execute()

and ``project_lock`` raises rather than take a new ``flock`` in a thread that holds this lock.

The lock is re-entrant and counts its depth per thread, so a locked helper can call another and ``project_lock`` can ask
whether the current thread holds it. It blocks with no timeout, like ``project_lock``, and it is never held across an
``await``. A user recipe that executes while it is imported does not take it.
"""

from __future__ import annotations

import contextlib
import threading

_lock = threading.RLock()
_held = threading.local()


def holds_execution_lock() -> bool:
    """Whether the current thread holds ``execution_lock``."""
    return getattr(_held, "depth", 0) > 0


@contextlib.contextmanager
def execution_lock():
    """Hold the process-wide lock on the shared backend while executing (#118). Re-entrant within a thread."""
    with _lock:
        _held.depth = getattr(_held, "depth", 0) + 1
        try:
            yield
        finally:
            _held.depth -= 1
