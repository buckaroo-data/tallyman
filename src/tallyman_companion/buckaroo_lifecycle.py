"""Manage a Buckaroo Tornado-server subprocess.

Buckaroo is the in-table recon surface (column distributions, null counts,
sort/filter/search) and is what beat 2 of the talk's storyboard relies on.
Buckaroo runs as its own Tornado server on a separate port; the companion
mounts the React embed (``static/buckaroo-embed.js``) into the entry-detail
page and opens a WS session per catalog entry.

Buckaroo is a displayer (ADR-007, governing rule): it runs queries only for summary stats, sorting and paging.
Tallyman runs an entry's computation to completion first, and hands Buckaroo something that already exists.

Lifecycle:

1. On companion startup, spawn `python -m buckaroo.server --port 8700
   --no-browser --stdio-control` as a subprocess. The `--stdio-control`
   flag makes Buckaroo exit when its stdin is closed — the safety net we
   want if uvicorn crashes hard.
2. Watch the subprocess's stdout for the handshake line
   `BUCKAROO_PORT=<n>` so we recover the bound port (useful when
   port=0).
3. Poll `/health` until Buckaroo reports ready (~200ms typical).
4. When the entry-detail route is hit on a content hash, make sure every file the entry reads exists
   (``ensure_materialized``), then POST `/load_expr` with a build dir and a session id derived from the project and
   the hash. A worthy entry is handed a *view build*, a build whose whole graph is one read of its snapshot; a cheap
   entry is handed its own build, a small plan over files that exist.
5. On shutdown (uvicorn lifespan or `atexit`), close the subprocess's
   stdin and wait briefly; if it doesn't go, SIGTERM.

Tallyman keeps no record of Buckaroo's sessions (ADR-007 D6). The session id is a function of the project and the
content hash, so it is never stale: a repeat POST is a no-op in Buckaroo while it holds the session (same id, same
build dir, none of the config-bearing fields), and re-creates it if Buckaroo has dropped it, as Buckaroo does after an
idle hour.
"""

from __future__ import annotations

import atexit
import json
import logging
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx

from tallyman_core import (
    entry_build_dir,
    entry_expanded_build_dir,
    entry_stat_cache_dir,
    entry_view_build_dir,
)
from tallyman_core.manifest import read_manifest
from tallyman_core.paths import artifacts_dir, entry_manifest_path, project_dir
from tallyman_xorq.row_order import ROW_ORDER

log = logging.getLogger("tallyman.buckaroo")

# One view build is written per entry directory at a time (a per-path lock, like ``ensure_expanded_build``'s).
_view_locks_guard = threading.Lock()
_view_locks: dict[str, threading.Lock] = {}


def ensure_view_build(project: str, content_hash: str) -> Path:
    """The stable per-entry directory holding the *view build* of a worthy entry's snapshot (ADR-007 D6).

    A view build is a xorq build whose whole graph is one step, "read this parquet file". Buckaroo's stat-cache keys
    include the build directory's path, so the directory is stable, written once and reused. A sibling marker records
    the snapshot path the build was made for, so a project that moved (a clone at another path) regenerates it instead
    of pointing Buckaroo at a path that is gone. The snapshot must exist (``ensure_materialized`` has run).
    """
    from xorq.expr.api import deferred_read_parquet
    from xorq.ibis_yaml.compiler import build_expr

    from tallyman_xorq.materialize import snapshot_path

    dest = entry_view_build_dir(project, content_hash)
    snap = str(snapshot_path(project, content_hash))
    marker = dest.with_name(dest.name + ".complete")

    def _fresh() -> bool:
        try:
            return (dest / "expr.yaml").is_file() and marker.read_text() == snap
        except OSError:
            return False

    if _fresh():
        return dest
    with _view_locks_guard:
        lock = _view_locks.setdefault(str(dest), threading.Lock())
    with lock:
        if _fresh():
            return dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".xorq_view_build.", dir=dest.parent) as tmp:
            built = Path(build_expr(deferred_read_parquet(snap), builds_dir=Path(tmp)))
            shutil.rmtree(dest, ignore_errors=True)
            shutil.move(str(built), str(dest))
        marker.write_text(snap)
    return dest


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
    """Owns a Buckaroo server subprocess and opens sessions on it.

    A session's id is a function of the project and the entry's content hash (``session_id_for``), so one subprocess
    serves sessions for entries of any project and tallyman needs no record of which are open: Buckaroo is the only
    process that knows, and it answers a repeat ``/load_expr`` for a session it holds without redoing the work.
    """

    def __init__(
        self,
        *,
        port: int = 8700,
        startup_timeout: float = 8.0,
        log_file: Path | None = None,
        companion_base_url: str | None = None,
    ):
        self.requested_port = port
        self.bound_port: int | None = None
        self.startup_timeout = startup_timeout
        self.log_file = log_file
        # Base URL (scheme://host:port, no trailing slash) the Buckaroo server
        # can reach the companion at, so /load_expr can be told where to POST
        # per-grid-load perf spans (buckaroo#943). None → telemetry not wired
        # (tests, or a companion that didn't pass its own address); the load
        # still works, buckaroo just emits no spans for it.
        self.companion_base_url = companion_base_url.rstrip("/") if companion_base_url else None
        self.proc: subprocess.Popen | None = None
        self._client = httpx.Client(timeout=5.0)
        # Diff-compare session_ids (``diff-<a>-<b>``) Buckaroo has loaded this
        # lifetime. Reset on a Buckaroo restart: these sessions live only in the
        # subprocess's RAM. (The live diff still posts an unmaterialized join, ADR-007 D10, #188.)
        self._loaded_diff_sessions: set[str] = set()
        self._buckaroo_started_at: float | None = None
        # Tmp dirs we created by expanding ${TALLYMAN_PROJECT_ROOT} placeholders
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

    @staticmethod
    def session_id_for(project: str, content_hash: str) -> str:
        """The Buckaroo session id of an entry: ``entry-<project>-<content_hash>``.

        A function of the two and nothing else (ADR-007 D6), so it is never stale and there is nothing to remember. The
        project is in it so that one project's session is never served to another on a hash collision (#172).
        """
        return f"entry-{project}-{content_hash}"

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
                    # what we last saw), its in-RAM sessions are gone — drop our
                    # bookkeeping so /load_expr runs again on first hit.
                    self._reset_session_bookkeeping_if_restarted(started_at)
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
    # diff-compare session bookkeeping
    # ------------------------------------------------------------------

    def diff_session_is_loaded(self, session_id: str) -> bool:
        """True if Buckaroo has loaded this diff-compare session this lifetime."""
        return session_id in self._loaded_diff_sessions

    def mark_diff_session_loaded(self, session_id: str) -> None:
        """Record a successful diff ``/load_expr``.

        Dropped on a Buckaroo restart by ``_reset_session_bookkeeping_if_restarted``
        — the session exists only in the subprocess's RAM, so a stale entry would
        point a client at a session the restarted process never loaded.
        """
        self._loaded_diff_sessions.add(session_id)

    def _reset_session_bookkeeping_if_restarted(self, started_at) -> None:
        """Drop in-RAM diff-session bookkeeping when a fresh Buckaroo is detected.

        The diff-compare sessions live only in the subprocess's memory, so a restart (a new ``started_at`` from
        ``/health``) invalidates every one we've handed out. Clearing forces a re-POST on next access; that reload is
        cheap because the on-disk stat cache survives the restart. Entry sessions need no such record (ADR-007 D6):
        their ids are derived and every open re-posts.
        """
        if started_at == self._buckaroo_started_at:
            return
        self._loaded_diff_sessions.clear()
        self._buckaroo_started_at = started_at

    # ------------------------------------------------------------------
    # klass reload and forced reload (no session record needed)
    # ------------------------------------------------------------------

    def reload_project_sessions(self, project: str) -> int:
        """Hot-reload klasses for every open grid of *project*.

        A klass is a project-authored stat, post-processing or display class. Buckaroo 0.15.6 has no route that lists
        its sessions and tallyman keeps no record of them, so this posts ``/reload_expr/<session id>`` (buckaroo
        0.14.9+) for each entry of the project and treats the 404 (or 400) Buckaroo answers for an id it does not hold
        as "not open" (ADR-007 D6). That is one request per entry per klass change. The session stays alive and its
        klasses are updated in place — no page-load round-trip to /load_expr is needed.

        After a successful reload the on-disk stat cache for that entry is cleared so the next widget request
        recomputes all stats (including any newly added ones) from scratch. Without this, a stat added after the
        session was first loaded would be absent from the cache and silently omitted from the display.

        Returns the number of sessions reloaded. Falls back to 0 (with a warning) if buckaroo isn't running or a
        reload call fails.
        """
        if not self.is_running or self.bound_port is None:
            return 0
        from tallyman_xorq.build import list_entries

        reloaded = 0
        for entry in list_entries(project):
            content_hash = entry["content_hash"]
            session_id = self.session_id_for(project, content_hash)
            try:
                resp = self._client.post(f"{self.base_url}/reload_expr/{session_id}", timeout=5.0)
                if resp.status_code in (404, 400):
                    continue  # Buckaroo does not hold this session (never opened, or idle-evicted)
                resp.raise_for_status()
                self._clear_stat_cache(project, content_hash)
                reloaded += 1
                log.info("reloaded klasses for session %s (hash %s)", session_id, content_hash)
            except httpx.HTTPError as exc:
                log.warning("buckaroo /reload_expr failed for session %s: %s", session_id, exc)
        return reloaded

    def force_reload_session(self, project: str, content_hash: str) -> bool:
        """Re-run Buckaroo's pipeline for an entry's grid (``force_reload``), for an unfaithful heal (ADR-007 D6).

        The snapshot's path now holds different rows and an open grid holds stats computed from the old ones. The
        caller has already wiped the entry's stat cache; Buckaroo then recomputes for that session. Returns whether
        Buckaroo accepted the load. Never raises: a heal must not fail because a grid could not be refreshed.
        """
        if not self.is_running or self.bound_port is None:
            return False
        try:
            body = self._load_body(project, content_hash, None)
        except Exception as exc:
            log.warning("could not build a forced reload for %s: %s", content_hash, exc)
            return False
        body["force_reload"] = True
        try:
            timeout = self._load_timeout(project, content_hash)
            resp = self._client.post(f"{self.base_url}/load_expr", json=body, timeout=timeout)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            log.warning("buckaroo forced reload failed for %s: %s", content_hash, exc)
            return False
        log.info("forced a reload of the grid for %s (unfaithful heal)", content_hash)
        return True

    def _clear_stat_cache(self, project: str, content_hash: str) -> None:
        """Delete cached stat parquet files for one entry.

        The parquet files in ``.buckaroo_stat_cache/parquet/`` are written
        by xorq's ParquetSnapshotCache.  After a klass reload the cache is
        stale (it was built before the new stat existed) so we delete it
        here.  The cache directory itself is left intact; buckaroo repopulates
        it on the next widget request.
        """
        cache_dir = entry_stat_cache_dir(project, content_hash) / "parquet"
        if cache_dir.is_dir():
            shutil.rmtree(cache_dir, ignore_errors=True)
            log.info("cleared stat cache for %s/%s", project, content_hash)

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
        """Session id for ``content_hash`` (creating it if needed), or None.

        Thin wrapper over :meth:`load_session` for callers that only need the id
        and fall back to the pandas preview on failure (api_data, diff, promote).
        Surfacing *why* a load failed — for the catalog detail spinner/retry
        (#133) — goes through :meth:`load_session` directly.
        """
        return self.load_session(content_hash, project, column_config_overrides)["session_id"]

    def _load_timeout(self, project: str, content_hash: str) -> float:
        row_count = 0
        mpath = entry_manifest_path(project, content_hash)
        if mpath.exists():
            try:
                row_count = read_manifest(mpath.parent).row_count or 0
            except Exception:
                pass
        return 10.0 + row_count / 1_000_000

    def _load_body(self, project: str, content_hash: str, column_config_overrides: dict | None) -> dict:
        """The ``/load_expr`` body for an entry whose files all exist.

        A worthy entry is handed a view build of its snapshot, so Buckaroo never executes an aggregate, join or sort on
        tallyman's behalf and never writes a snapshot (ADR-007 D6). A cheap entry is handed its own expanded build, a
        stored definition over files that exist, which tallyman already executed in full when it was created.
        """
        from tallyman_xorq.portable import ensure_expanded_build  # noqa: PLC0415
        from tallyman_xorq.result_cache import cache_worthy  # noqa: PLC0415

        if cache_worthy(project, content_hash):
            build_dir = ensure_view_build(project, content_hash)
        else:
            # Expand ${TALLYMAN_PROJECT_ROOT} to absolute paths into a stable per-entry dir (not a random tmp dir) so
            # the expanded path is identical across server restarts: Buckaroo's stat keys include the build
            # directory's path, so a random tmp path makes every stat-cache lookup a miss even when the cache is fully
            # populated on disk. Marker-gated for crash safety.
            build_dir = ensure_expanded_build(
                entry_build_dir(project, content_hash),
                project_dir(project),
                entry_expanded_build_dir(project, content_hash),
            )
        stat_cache = entry_stat_cache_dir(project, content_hash)
        stat_cache.mkdir(parents=True, exist_ok=True)
        payload: dict = {
            "session": self.session_id_for(project, content_hash),
            "build_dir": str(build_dir),
            "no_browser": True,
            # Buckaroo scans <project_root>/stats/*.py and <project_root>/post_processing/*.py for project-authored
            # klasses. tallyman stores both under artifacts/, so pass artifacts_dir, not project_dir. Older buckaroo
            # builds ignore this field, so it's safe to always send.
            "project_root": str(artifacts_dir(project)),
            # Buckaroo 0.14.9+: persist computed summary stats to disk so they survive a Buckaroo restart without full
            # recomputation on next /load_expr.
            "cache_storage_path": str(stat_cache),
            # ADR-008 D8: the column with no ties that pages sort by (buckaroo-data/buckaroo#974). A page is
            # ORDER BY __row_order, or the user's keys and then __row_order, so the same request returns the same rows.
            # Buckaroo builds that predate the hint ignore it.
            "row_order_column": ROW_ORDER,
        }
        if column_config_overrides is not None:
            payload["column_config_overrides"] = column_config_overrides
        if self.companion_base_url is not None:
            # buckaroo#943: the server fire-and-forget POSTs one record per firstpull.* perf span (expr load, stats
            # pipeline + cache hit/miss, WS first payload) to this URL, keyed by the session id. Per-project so the
            # receiver knows which telemetry.jsonl to append to. Older buckaroo ignores the field.
            payload["telemetry_url"] = f"{self.companion_base_url}/{project}/api/telemetry"
        return payload

    def load_session(
        self,
        content_hash: str,
        project: str,
        column_config_overrides: dict | None = None,
    ) -> dict:
        """Open (or reuse) a Buckaroo session, returning a typed status.

        ``{"status", "session_id", "detail"}`` where ``status`` is one of
        ``ok`` (``session_id`` set), ``unavailable`` (Buckaroo not running),
        ``no_build`` (entry has no xorq build), ``timeout`` (the ``/load_expr``
        POST timed out), or ``error`` (a file the entry needs could not be made, or Buckaroo rejected the load). The
        companion surfaces this so the detail page shows a spinner, a precise error, and a retry instead of a bare "not
        available" fallback (#133) — never raising, so a Buckaroo hiccup can't take down the page.

        Tallyman finishes its own work first (ADR-007 D6, the governing rule): ``ensure_materialized`` makes every file
        the entry's plan reads, and its own snapshot, exist and verifies what it writes. Only then is Buckaroo asked to
        display anything, so a failure of the computation surfaces here, in tallyman's process, and never inside a grid
        query. The session id is derived from the project and the hash and posted every time: Buckaroo skips the work
        when it already holds that session with the same build dir (and the post carries none of the config-bearing
        fields), and creates the session again if it has dropped it.

        ``project`` names the project that owns the entry.

        ``column_config_overrides`` is passed to ``/load_expr`` when provided
        (e.g. for promoted diff entries that carry Buckaroo coloring state).

        If the subprocess died since the last call (mid-session crash, OOM,
        or signal), one restart attempt is made — throttled by
        ``_restart_cooldown`` to avoid thrashing on a persistent failure.
        """
        self._maybe_restart()
        if not self.is_running or self.bound_port is None:
            return {
                "status": "unavailable",
                "session_id": None,
                "detail": "Buckaroo is not running — start tallyman with --buckaroo.",
            }
        if not entry_build_dir(project, content_hash).is_dir():
            return {
                "status": "no_build",
                "session_id": None,
                "detail": "This entry has no xorq build to load.",
            }
        try:
            from tallyman_xorq.materialize import ensure_materialized  # noqa: PLC0415

            ensure_materialized(project, content_hash)
            payload = self._load_body(project, content_hash, column_config_overrides)
        except Exception as exc:
            log.warning("could not prepare %s for Buckaroo: %s", content_hash, exc)
            return {
                "status": "error",
                "session_id": None,
                "detail": f"Tallyman could not prepare this entry: {type(exc).__name__}: {exc}",
            }
        _load_timeout = self._load_timeout(project, content_hash)
        _t_post = time.perf_counter()
        try:
            resp = self._client.post(f"{self.base_url}/load_expr", json=payload, timeout=_load_timeout)
            resp.raise_for_status()
            session_id = resp.json()["session"]
        except httpx.TimeoutException as exc:
            log.warning("buckaroo /load_expr timed out for %s: %s", content_hash, exc)
            return {
                "status": "timeout",
                "session_id": None,
                "detail": (
                    f"Buckaroo timed out loading this entry ({_load_timeout:.1f}s) — it may be slow to materialise."
                ),
            }
        except (httpx.HTTPError, KeyError, json.JSONDecodeError) as exc:
            log.warning("buckaroo /load_expr failed for %s: %s", content_hash, exc)
            return {
                "status": "error",
                "session_id": None,
                "detail": f"Buckaroo could not load this entry: {type(exc).__name__}: {exc}",
            }
        # load_expr_ms: the companion-visible slice of the grid load — the POST to buckaroo only. The in-buckaroo
        # timing (stats, row requests) needs buckaroo-side telemetry (buckaroo-data/buckaroo#943).
        return {
            "status": "ok",
            "session_id": session_id,
            "detail": "",
            "load_expr_ms": round((time.perf_counter() - _t_post) * 1000, 1),
        }

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------

    def status(self) -> dict:
        return {
            "running": self.is_running,
            "port": self.bound_port,
        }


def _which_python() -> str:
    return shutil.which("python") or sys.executable
