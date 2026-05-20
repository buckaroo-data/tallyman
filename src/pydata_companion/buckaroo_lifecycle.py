"""Manage a Buckaroo Tornado-server subprocess.

Buckaroo is the in-table recon surface (column distributions, null counts,
sort/filter/search) and is what beat 2 of the talk's storyboard relies on.
Buckaroo runs as its own Tornado server on a separate port; the companion
embeds it via iframe and pre-warms a session per catalog entry.

Lifecycle:

1. On companion startup, spawn `python -m buckaroo.server --port 8700
   --no-browser --stdio-control` as a subprocess. The `--stdio-control`
   flag makes Buckaroo exit when its stdin is closed — the safety net we
   want if uvicorn crashes hard.
2. Watch the subprocess's stdout for the handshake line
   `BUCKAROO_PORT=<n>` so we recover the bound port (useful when
   port=0).
3. Poll `/health` until Buckaroo reports ready (~200ms typical).
4. When the entry-detail route is hit for the first time on a content
   hash, POST `/load` with the parquet path and store the returned
   `session` in `catalog/buckaroo_sessions.json` for later retrieval.
5. On shutdown (uvicorn lifespan or `atexit`), close the subprocess's
   stdin and wait briefly; if it doesn't go, SIGTERM.

The session map is persisted so that across companion restarts we don't
re-call /load redundantly on the same hash. Buckaroo itself is fresh on
each start, though, so the cached session_ids only act as a *naming*
convention — Buckaroo's own state is rebuilt on first hit either way.
This means: a `pydata serve` restart re-loads parquets lazily as entries
are viewed; that's correct.
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx

from pydata_core import catalog_dir, entry_dir

log = logging.getLogger("pydata.buckaroo")


def _port_in_use(port: int) -> bool:
    s = socket.socket()
    try:
        s.bind(("127.0.0.1", port))
    except OSError:
        return True
    finally:
        s.close()
    return False


class BuckarooUnavailable(RuntimeError):
    pass


class BuckarooManager:
    """Owns a Buckaroo server subprocess and the per-entry session cache."""

    def __init__(
        self,
        project: str,
        *,
        port: int = 8700,
        startup_timeout: float = 8.0,
        log_file: Path | None = None,
        mode: str = "buckaroo",
    ):
        """
        Args:
            mode: which Buckaroo headless pipeline the server should build for
                each session. `"buckaroo"` (default here) drives the full
                ``BuckarooInfiniteWidget`` pipeline — infinite scroll, dataflow,
                command toolbar, column statistics. `"viewer"` is the lighter
                ``DfViewer`` (sort/filter only). `"lazy"` uses the
                ``LazyInfinitePolars`` pipeline for streaming polars frames.
                The ``XorqBuckarooInfiniteWidget`` flavor is not yet reachable
                via Buckaroo's standalone server — it operates on a xorq
                expression rather than a parquet path and would need a
                hypothetical ``/load_expr`` endpoint we don't have. See
                buckaroo/xorq_buckaroo.py and TICKETS.md for the gap.
        """
        self.project = project
        self.requested_port = port
        self.bound_port: int | None = None
        self.startup_timeout = startup_timeout
        self.log_file = log_file
        self.mode = mode
        self.proc: subprocess.Popen | None = None
        self._client = httpx.Client(timeout=5.0)
        self._sessions: dict[str, str] = {}
        self._session_lock = threading.Lock()
        self._buckaroo_started_at: float | None = None
        self._load_session_file()

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    @property
    def base_url(self) -> str:
        if self.bound_port is None:
            raise BuckarooUnavailable("Buckaroo has not finished starting up")
        return f"http://127.0.0.1:{self.bound_port}"

    @property
    def ws_base_url(self) -> str:
        if self.bound_port is None:
            raise BuckarooUnavailable("Buckaroo has not finished starting up")
        return f"ws://127.0.0.1:{self.bound_port}"

    def start(self) -> None:
        """Spawn the Buckaroo subprocess and wait for the handshake."""
        if self.is_running:
            return
        if _port_in_use(self.requested_port):
            log.warning(
                "buckaroo port %d already in use; using --port=0 (random)",
                self.requested_port,
            )
            requested = 0
        else:
            requested = self.requested_port

        # Find the python that loaded *us* (the venv), not whatever's first on PATH.
        cmd = [
            sys.executable,
            "-m",
            "buckaroo.server",
            "--port",
            str(requested),
            "--no-browser",
            "--stdio-control",
        ]
        log_fp = open(self.log_file, "a") if self.log_file else subprocess.DEVNULL
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log_fp,
            text=True,
            bufsize=1,
        )
        atexit.register(self.stop)

        # Read stdout until we see the handshake. The subprocess prints
        # `BUCKAROO_PORT=<n>` early, before any other output.
        deadline = time.monotonic() + self.startup_timeout
        bound = None
        assert self.proc.stdout is not None
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                if self.proc.poll() is not None:
                    raise BuckarooUnavailable(
                        f"buckaroo exited during startup (rc={self.proc.returncode})"
                    )
                time.sleep(0.05)
                continue
            line = line.strip()
            log.debug("buckaroo stdout: %s", line)
            if line.startswith("BUCKAROO_PORT="):
                bound = int(line.split("=", 1)[1])
                break

        if bound is None:
            self.stop()
            raise BuckarooUnavailable("buckaroo did not print a port handshake")

        # Now poll /health until ready.
        self.bound_port = bound
        health_deadline = time.monotonic() + 5.0
        while time.monotonic() < health_deadline:
            try:
                r = self._client.get(f"{self.base_url}/health", timeout=1.0)
                if r.status_code == 200:
                    health = r.json()
                    started_at = health.get("started")
                    log.info("buckaroo ready on %s (started=%s)", self.base_url, started_at)
                    # If this Buckaroo started fresh (different start time from
                    # what we last saw), the cached session_ids are stale and
                    # we drop them so /load runs again on first hit.
                    if started_at != self._buckaroo_started_at:
                        if self._sessions:
                            log.info(
                                "buckaroo restart detected; invalidating %d cached sessions",
                                len(self._sessions),
                            )
                        self._sessions = {}
                        self._buckaroo_started_at = started_at
                        self._persist_sessions()
                    threading.Thread(target=self._drain_stdout, daemon=True).start()
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)

        self.stop()
        raise BuckarooUnavailable("buckaroo did not respond to /health in time")

    def _drain_stdout(self) -> None:
        if self.proc is None or self.proc.stdout is None:
            return
        try:
            for line in self.proc.stdout:
                if line.strip():
                    log.debug("buckaroo stdout: %s", line.strip())
        except Exception:
            pass

    def stop(self) -> None:
        if not self.is_running or self.proc is None:
            return
        log.info("stopping buckaroo")
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            log.warning("buckaroo didn't exit after stdin close; SIGTERM")
            self.proc.terminate()
            try:
                self.proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.proc = None
        self.bound_port = None

    # ------------------------------------------------------------------
    # session map (persisted to disk)
    # ------------------------------------------------------------------

    def _session_file_path(self) -> Path:
        return catalog_dir(self.project) / "buckaroo_sessions.json"

    def _load_session_file(self) -> None:
        p = self._session_file_path()
        if p.exists():
            try:
                data = json.loads(p.read_text())
            except json.JSONDecodeError:
                data = {}
            # Support both the legacy shape ({hash: session_id}) and the new
            # shape ({buckaroo_started_at, sessions: {...}}).
            if "sessions" in data:
                self._sessions = data.get("sessions", {})
                self._buckaroo_started_at = data.get("buckaroo_started_at")
            else:
                self._sessions = data

    def _persist_sessions(self) -> None:
        p = self._session_file_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps(
                {
                    "buckaroo_started_at": self._buckaroo_started_at,
                    "sessions": self._sessions,
                },
                indent=2,
                sort_keys=True,
            )
        )

    # ------------------------------------------------------------------
    # session creation
    # ------------------------------------------------------------------

    def ensure_session(self, content_hash: str) -> str | None:
        """Return a Buckaroo session_id for `content_hash`, creating it if needed.

        Returns None (instead of raising) if Buckaroo isn't running or the load
        fails — the companion falls back to the pandas.to_html preview in that
        case. We don't want a Buckaroo hiccup to take down the whole detail page.
        """
        if not self.is_running or self.bound_port is None:
            return None
        parquet = entry_dir(self.project, content_hash) / "result.parquet"
        if not parquet.exists():
            return None
        with self._session_lock:
            cached = self._sessions.get(content_hash)
            if cached:
                # Within one Buckaroo lifetime cached sessions are valid by
                # construction; start() resets the map on restart.
                return cached
            try:
                resp = self._client.post(
                    f"{self.base_url}/load",
                    json={
                        "path": str(parquet),
                        "no_browser": True,
                        "mode": self.mode,
                    },
                    timeout=10.0,
                )
                resp.raise_for_status()
                session_id = resp.json()["session"]
            except (httpx.HTTPError, KeyError, json.JSONDecodeError) as exc:
                log.warning("buckaroo /load failed for %s: %s", content_hash, exc)
                return None
            self._sessions[content_hash] = session_id
            self._persist_sessions()
            return session_id

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------

    def status(self) -> dict:
        return {
            "running": self.is_running,
            "port": self.bound_port,
            "session_count": len(self._sessions),
        }


def _which_python() -> str:
    return shutil.which("python") or sys.executable
