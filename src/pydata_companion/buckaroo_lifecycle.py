"""Manage a Buckaroo Tornado-server subprocess.

Buckaroo is the in-table recon surface (column distributions, null counts,
sort/filter/search) and is what beat 2 of the talk's storyboard relies on.
Buckaroo runs as its own Tornado server on a separate port; the companion
mounts the React embed (``static/buckaroo-embed.js``) into the entry-detail
page and pre-warms a WS session per catalog entry.

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
   hash, POST `/load_expr` with the entry's `xorq_build/` dir and store
   the returned `session` in `catalog/buckaroo_sessions.json` for later
   retrieval. Buckaroo serves the entry via the xorq backend with
   push-down sort/search (paging over a materialised parquet is the
   `/load` flow, which we no longer use).
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
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx

from pydata_core import entry_dir
from pydata_core.paths import artifacts_dir, project_dir
from pydata_xorq.portable import expand_to_tmp


def _entry_parquet_exists(project: str, content_hash: str) -> bool:
    """True if the project's catalog has an entry for *content_hash*.

    Used by ``_load_session_file`` to prune stale session entries whose
    project / hash combination no longer maps to anything on disk.
    """
    p = entry_dir(project, content_hash)
    return (p / "result.parquet").exists() or (p / "xorq_build").is_dir()


log = logging.getLogger("pydata.buckaroo")


def _port_in_use(port: int) -> bool:
    """Probe whether `port` is currently bound to a listener on localhost.

    SO_REUSEADDR matches Tornado's own bind options — without it, the probe
    false-positives for ~60s after a restart whenever the previous buckaroo
    had any client connections (a browser WS, the embed) open at shutdown:
    the closed connections' TIME_WAIT artifacts make a bare bind() fail with
    EADDRINUSE even though no listener exists. The real Tornado server can
    still bind such a port; the probe just disagreed and forced a port=0
    fallback the user noticed as a fresh random port on each restart.
    """
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
    """Owns a Buckaroo server subprocess and a multi-project session cache.

    Sessions are keyed by content hash (globally unique by construction), so
    one subprocess can serve sessions backed by xorq builds from any project.
    The owning project travels per-call via ``ensure_session(hash, project)``
    and is recorded alongside the session id so a restart-reload knows which
    project's parquet to find.

    Session state persists in a single global file at
    ``~/.pydata-app/buckaroo_sessions.json``. The per-project location used
    by V0.5 (``<project>/artifacts/catalog/buckaroo_sessions.json``) is gone.
    Will be replaced wholesale by a session-enumeration endpoint when
    buckaroo-data/buckaroo#860 lands; until then the file is the bookkeeping.
    """

    def __init__(
        self,
        *,
        port: int = 8700,
        startup_timeout: float = 8.0,
        log_file: Path | None = None,
    ):
        self.requested_port = port
        self.bound_port: int | None = None
        self.startup_timeout = startup_timeout
        self.log_file = log_file
        self.proc: subprocess.Popen | None = None
        self._client = httpx.Client(timeout=5.0)
        # Schema: {<content_hash>: {"session_id": str, "project": str}}.
        self._sessions: dict[str, dict] = {}
        self._session_lock = threading.Lock()
        self._buckaroo_started_at: float | None = None
        # Tmp dirs we created by expanding ${PYDATA_PROJECT_ROOT} placeholders
        # before POSTing /load_expr. Buckaroo holds the loaded xorq expression
        # open against these paths for the session lifetime, so we keep them
        # alive until stop() runs.
        self._expanded_dirs: dict[str, Path] = {}
        # Auto-restart bookkeeping. If buckaroo dies mid-session and we
        # try to restart on every page hit, a persistent failure (port
        # taken, deps missing) would thrash. Throttle to one attempt per
        # ``_restart_cooldown`` seconds.
        self._last_restart_attempt: float = 0.0
        self._restart_cooldown: float = 30.0
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
                    raise BuckarooUnavailable(f"buckaroo exited during startup (rc={self.proc.returncode})")
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
            self._cleanup_expanded_dirs()
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
        self._cleanup_expanded_dirs()

    def _cleanup_expanded_dirs(self) -> None:
        for p in self._expanded_dirs.values():
            shutil.rmtree(p, ignore_errors=True)
        self._expanded_dirs.clear()

    # ------------------------------------------------------------------
    # session map (persisted to disk)
    # ------------------------------------------------------------------

    def _session_file_path(self) -> Path:
        from pydata_core.paths import buckaroo_sessions_path

        return buckaroo_sessions_path()

    def _load_session_file(self) -> None:
        """Load the global session map, dropping entries whose parquet has
        disappeared since the last persist.

        Schema: ``{"sessions": {hash: {session_id, project}}, "buckaroo_started_at": float}``.
        Each entry's ``project`` is treated as a hint — if that project has
        no entry for the hash, the session is stale and gets dropped.
        """
        p = self._session_file_path()
        if not p.exists():
            return
        try:
            data = json.loads(p.read_text())
        except json.JSONDecodeError:
            return
        self._buckaroo_started_at = data.get("buckaroo_started_at")
        sessions = data.get("sessions", {})
        # Defensive: silently drop entries that aren't the new shape.
        kept: dict[str, dict] = {}
        for h, info in sessions.items():
            if not isinstance(info, dict):
                continue
            project = info.get("project")
            if not project:
                continue
            if not _entry_parquet_exists(project, h):
                continue
            kept[h] = {"session_id": info["session_id"], "project": project}
        self._sessions = kept

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

    def reload_project_sessions(self, project: str) -> int:
        """Hot-reload klasses for all live sessions belonging to *project*.

        Calls POST /reload_expr/<session_id> (buckaroo 0.14.9+) on each
        cached session for *project*. The session stays alive and its
        analysis/post-processing klasses are updated in place — no eviction
        or page-load round-trip to /load_expr is needed.

        Returns the number of sessions reloaded. Falls back to 0 (with a
        warning) if buckaroo isn't running or a reload call fails.
        """
        if not self.is_running or self.bound_port is None:
            return 0
        with self._session_lock:
            targets = {
                h: info["session_id"]
                for h, info in self._sessions.items()
                if info.get("project") == project
            }
        reloaded = 0
        to_evict: list[str] = []
        for content_hash, session_id in targets.items():
            try:
                resp = self._client.post(
                    f"{self.base_url}/reload_expr/{session_id}",
                    timeout=5.0,
                )
                if resp.status_code in (404, 400):
                    # Session is gone or is no longer an xorq session — evict
                    # so the next ensure_session call re-creates it cleanly.
                    log.warning(
                        "buckaroo session %s (hash %s) is stale (%d); evicting",
                        session_id, content_hash, resp.status_code,
                    )
                    to_evict.append(content_hash)
                    continue
                resp.raise_for_status()
                reloaded += 1
                log.info("reloaded klasses for session %s (hash %s)", session_id, content_hash)
            except httpx.HTTPError as exc:
                log.warning("buckaroo /reload_expr failed for session %s: %s", session_id, exc)
        if to_evict:
            with self._session_lock:
                for h in to_evict:
                    self._sessions.pop(h, None)
            self._persist_sessions()
        return reloaded

    # ------------------------------------------------------------------
    # session creation
    # ------------------------------------------------------------------

    def _maybe_restart(self) -> None:
        """If buckaroo's subprocess has exited since we last looked,
        attempt to restart it. Throttled so a persistently-failing
        startup doesn't thrash on every page view."""
        if self.is_running:
            return
        if self.proc is None:
            return  # never started; not our job to start it here
        rc = self.proc.poll()
        now = time.monotonic()
        if now - self._last_restart_attempt < self._restart_cooldown:
            return
        self._last_restart_attempt = now
        log.warning("buckaroo subprocess exited (rc=%s); attempting restart", rc)
        self.proc = None
        self.bound_port = None
        try:
            self.start()
            # T-35: after restart the bound port may have changed (random
            # port fallback if the original was in use); print both so the
            # user can correlate against ps/lsof.
            new_pid = self.proc.pid if self.proc is not None else "?"
            log.info(
                "buckaroo restarted successfully (pid=%s port=%s)",
                new_pid,
                self.bound_port,
            )
        except BuckarooUnavailable as exc:
            log.warning("buckaroo restart failed: %s (will retry after cooldown)", exc)

    def ensure_session(
        self,
        content_hash: str,
        project: str,
        column_config_overrides: dict | None = None,
    ) -> str | None:
        """Return a Buckaroo session_id for ``content_hash``, creating it if needed.

        Posts the entry's ``xorq_build/`` dir to Buckaroo's ``/load_expr``
        endpoint so the session is backed by the xorq expression (push-down
        sort/search against the underlying backend), not by paging over the
        materialised parquet.

        ``project`` names the project that owns the parquet. Sessions cache
        across projects via content hash (globally unique), so a second call
        for the same hash from a different project returns the existing
        session — the bytes are the same by definition.

        ``column_config_overrides`` is passed to ``/load_expr`` when provided
        (e.g. for promoted diff entries that carry Buckaroo coloring state).

        Returns None (instead of raising) if Buckaroo isn't running or the load
        fails — the companion falls back to the pandas.to_html preview in that
        case. We don't want a Buckaroo hiccup to take down the whole detail page.

        If the subprocess died since the last call (mid-session crash, OOM,
        or signal), one restart attempt is made — throttled by
        ``_restart_cooldown`` to avoid thrashing on a persistent failure.
        """
        self._maybe_restart()
        if not self.is_running or self.bound_port is None:
            return None
        build_dir = entry_dir(project, content_hash) / "xorq_build"
        if not build_dir.is_dir():
            return None
        with self._session_lock:
            cached = self._sessions.get(content_hash)
            if cached:
                # Within one Buckaroo lifetime cached sessions are valid by
                # construction; start() resets the map on restart.
                return cached["session_id"]
            # Expand ${PYDATA_PROJECT_ROOT} to absolute paths in a tmp copy —
            # buckaroo's xorq_loading.load_expr_build_dir calls xorq.api.load_expr
            # directly and does no placeholder handling, so an unexpanded
            # build_dir produces a session whose paged reads return zero rows.
            expanded = self._expanded_dirs.get(content_hash)
            if expanded is None:
                expanded = expand_to_tmp(build_dir, project_dir(project))
                self._expanded_dirs[content_hash] = expanded
            stat_cache = entry_dir(project, content_hash) / ".buckaroo_stat_cache"
            stat_cache.mkdir(parents=True, exist_ok=True)
            payload: dict = {
                "build_dir": str(expanded),
                "no_browser": True,
                # Buckaroo scans <project_root>/stats/*.py and
                # <project_root>/post_processing/*.py for project-authored
                # klasses. pydata stores both under artifacts/, so pass
                # artifacts_dir, not project_dir. Older buckaroo builds
                # ignore this field, so it's safe to always send.
                "project_root": str(artifacts_dir(project)),
                # Buckaroo 0.14.9+: persist computed summary stats to
                # disk so they survive a Buckaroo restart without full
                # recomputation on next /load_expr.
                "cache_storage_path": str(stat_cache),
            }
            if column_config_overrides:
                payload["column_config_overrides"] = column_config_overrides
            try:
                resp = self._client.post(
                    f"{self.base_url}/load_expr",
                    json=payload,
                    timeout=10.0,
                )
                resp.raise_for_status()
                session_id = resp.json()["session"]
            except (httpx.HTTPError, KeyError, json.JSONDecodeError) as exc:
                log.warning("buckaroo /load_expr failed for %s: %s", content_hash, exc)
                return None
            self._sessions[content_hash] = {"session_id": session_id, "project": project}
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
