"""The companion's blocking actions, announced before they run and recorded when they end.

Some companion work blocks for seconds: making an entry's files exist, the POST that asks Buckaroo to load a session,
a staleness scan, a diff. Buckaroo serves one request at a time, so a load that arrives while another session's stats
are running waits for them, and from the companion that wait is indistinguishable from slow work. ``track`` wraps such
a step. It tells every listener the action is about to start (``action_start``), and again when it ends
(``action_end``, carrying the duration). The companion forwards both over SSE; the header shows what is running and
the last action to finish, and ``GET /{project}/api/timing`` returns the recent ones beside the spans Buckaroo posts
back, on the same clock.

In memory and per process: a restart starts empty, and an action that runs in the MCP process is not seen here.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager

log = logging.getLogger("tallyman.activity")

# How many finished actions the process keeps.
DEFAULT_KEEP = 200


def _now_ms() -> float:
    return time.time() * 1000


class ActivityTracker:
    """Running and recently finished actions, and the listeners told about each start and end.

    Listeners are called on the thread that started or ended the action, outside the lock, and an exception in one is
    logged and dropped so a broken listener never fails the work it observes.
    """

    def __init__(self, keep: int = DEFAULT_KEEP):
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._running: dict[int, dict] = {}
        self._recent: deque[dict] = deque(maxlen=keep)
        self._listeners: list[Callable[[dict], None]] = []

    def add_listener(self, fn: Callable[[dict], None]) -> None:
        with self._lock:
            self._listeners.append(fn)

    def remove_listener(self, fn: Callable[[dict], None]) -> None:
        with self._lock:
            if fn in self._listeners:
                self._listeners.remove(fn)

    def _emit(self, kind: str, action: dict) -> None:
        with self._lock:
            listeners = list(self._listeners)
        event = {"kind": kind, "action": dict(action)}
        for fn in listeners:
            try:
                fn(event)
            except Exception:  # noqa: BLE001 - a listener must never fail the action it observes
                log.debug("activity listener failed for %s", kind, exc_info=True)

    def begin(self, name: str, detail: str = "", project: str | None = None) -> dict:
        action = {
            "id": next(self._ids),
            "name": name,
            "detail": detail,
            "project": project,
            "started_ms": _now_ms(),
            "ended_ms": None,
            "status": "running",
            "error": None,
        }
        with self._lock:
            self._running[action["id"]] = action
        self._emit("action_start", action)
        return action

    def end(self, action: dict, error: str | None = None) -> None:
        with self._lock:
            self._running.pop(action["id"], None)
            action["ended_ms"] = _now_ms()
            action["status"] = "error" if error else "ok"
            action["error"] = error
            self._recent.appendleft(action)
        self._emit("action_end", action)

    @contextmanager
    def track(self, name: str, detail: str = "", project: str | None = None) -> Iterator[dict]:
        """Announce ``name`` before the body runs and record it when the body ends. An exception is recorded as the
        action's error and re-raised."""
        action = self.begin(name, detail, project)
        try:
            yield action
        except BaseException as exc:
            self.end(action, error=f"{type(exc).__name__}: {exc}")
            raise
        else:
            self.end(action)

    def snapshot(self) -> dict:
        """``{"running": [...], "recent": [...]}``: copies, running oldest first, recent newest first."""
        with self._lock:
            running = sorted((dict(a) for a in self._running.values()), key=lambda a: a["started_ms"])
            recent = [dict(a) for a in self._recent]
        return {"running": running, "recent": recent}

    def reset(self) -> None:
        with self._lock:
            self._running.clear()
            self._recent.clear()


# The process-wide tracker: the companion's sites call ``track``, and the app registers the one listener that
# forwards events over SSE.
tracker = ActivityTracker()
track = tracker.track
