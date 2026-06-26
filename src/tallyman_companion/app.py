from __future__ import annotations

import asyncio
import functools
import json
import logging
import os
import re
import time
from dataclasses import asdict
from pathlib import Path

import markdown as md_lib
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_companion.diff import build_compare_expr, strip_live_diff_color
from tallyman_core import (
    ENTRY_BUILD_DIRNAME,
    ENTRY_MANIFEST_FILENAME,
    ENTRY_SCHEMA_FILENAME,
    ENTRY_STAT_CACHE_DIRNAME,
    alias_for_hash,
    clear_errors,
    entry_dir,
    get_chart,
    get_error,
    history_for,
    list_errors,
    load_aliases,
    notebook,
    read_manifest,
    resolve_project,
    version_of_hash,
)
from tallyman_core.events import list_sessions, read_events, record_event
from tallyman_core.notebook import CellNotFound
from tallyman_core.paths import entries_dir, project_dir, validate_project_name
from tallyman_core.telemetry import read_spans, record_span
from tallyman_core.version import git_revision, version_info
from tallyman_xorq import (
    full_diff,
    list_entries,
    read_prompts,
)
from tallyman_xorq.primary_key import diff_keys
from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

log = logging.getLogger("tallyman.companion")

PKG_DIR = Path(__file__).resolve().parent
STATIC_DIR = PKG_DIR / "static"

# React SPA dist relative to the repo root (parents[2] == repo root from
# src/tallyman_companion/app.py). Works for dev; when installed as a package
# without a bundled dist the server returns 503 with a helpful build message.
_REACT_DIST = Path(__file__).resolve().parents[2] / "packages" / "app" / "dist"


class NotifyPayload(BaseModel):
    kind: str
    hash: str | None = None
    project: str | None = None  # explicit target; absent → the active project
    extra: dict | None = None


def _broadcaster() -> tuple[asyncio.Queue, list]:
    return asyncio.Queue(), []


def _render_markdown(text: str) -> str:
    """Render markdown to HTML. Empty/whitespace input returns ''."""
    if not text or not text.strip():
        return ""
    return md_lib.markdown(text, extensions=["fenced_code", "tables"])


def _dir_size(path: Path) -> int:
    """Total bytes of all files under path, using os.walk for speed."""
    total = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for f in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:
                pass
    return total


def _path_size(path: Path) -> int:
    """Bytes occupied by a path, whether it's a single file or a directory.

    The xorq snapshot cache stores a result as either a parquet file or a
    directory of parts depending on backend, so the per-expression breakdown
    can't assume one shape.
    """
    try:
        if path.is_dir():
            return _dir_size(path)
        return path.stat().st_size
    except OSError:
        return 0


# A catalog entry's content hash is the lowercase-hex name of its entry dir
# (xorq's build_path.name). Used to reject path-param junk before it can name
# anything under the entries tree.
_CONTENT_HASH_RE = re.compile(r"\A[0-9a-f]+\Z")


def _require_hash(content_hash: str) -> None:
    """Reject a content_hash path param that isn't lowercase hex.

    A content hash names an entry dir, so it's lowercase hex. Anything else can
    only ever resolve outside the entries tree — a junk value, or a "."/".."
    segment that lands on a real dir with no manifest and 500s. A 400 reads
    truer than the "not found" 404 those would otherwise hit. Endpoints that
    also accept an alias must resolve the alias first, then guard the remainder.
    """
    if not _CONTENT_HASH_RE.match(content_hash):
        raise HTTPException(400, f"malformed content hash {content_hash!r}")


def _fmt_bytes(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f} GB"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f} MB"
    if n >= 1_000:
        return f"{n / 1_000:.1f} KB"
    return f"{n} B"


# A disk-usage call walks every file under a project. The pill is refetched on
# every page mount and every new_entry SSE event across tabs, so cache the
# result briefly (api_disk_usage) to coalesce those bursts into one walk.
_DISK_USAGE_TTL = 3.0


def _compute_disk_usage(project: str) -> dict:
    """Walk a project's on-disk footprint and return the disk_usage payload.

    Covers raw input data, each entry's xorq_build / .buckaroo_stat_cache, the
    per-project compute cache (compute_cache/, where the baked result snapshots
    and source-read caches live — the single materialised copy of an entry's
    rows since #73), and the per-pair Buckaroo diff stat cache (diff_stat_cache/),
    which can dwarf the rest. No per-entry ``result.parquet`` is written any more,
    so there is no separate "results" bucket — the baked snapshots it would have
    counted are under compute_cache.

    This is the expensive path; callers should rate-limit it via the
    api_disk_usage TTL cache rather than invoking per request.
    """
    from tallyman_core.paths import compute_cache_dir as _compute_cache_dir
    from tallyman_core.paths import data_dir as _data_dir
    from tallyman_core.paths import diff_stat_cache_root as _diff_stat_cache_root
    from tallyman_core.paths import entries_dir as _entries_dir

    # raw input files
    data = _dir_size(_data_dir(project))

    root = _entries_dir(project)
    builds = cache = 0
    if root.is_dir():
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            b = entry / ENTRY_BUILD_DIRNAME
            if b.is_dir():
                builds += _dir_size(b)
            c = entry / ENTRY_STAT_CACHE_DIRNAME
            if c.is_dir():
                cache += _dir_size(c)

    # Per-project compute cache (baked result snapshots + source-read caches),
    # counted since #87. The baked snapshots are the single materialised copy of
    # an entry's rows, so this subsumes the retired per-entry "results" bucket.
    compute_cache = _dir_size(_compute_cache_dir(project))

    # Project-level diff stat cache (#12). _dir_size returns 0 for a dir that
    # doesn't exist yet, so no existence guard needed.
    diff_cache = _dir_size(_diff_stat_cache_root(project))

    total = data + builds + cache + compute_cache + diff_cache
    return {
        "data": data,
        "builds": builds,
        "cache": cache,
        "compute_cache": compute_cache,
        "diff_cache": diff_cache,
        "total": total,
        "formatted": {
            "data": _fmt_bytes(data),
            "builds": _fmt_bytes(builds),
            "cache": _fmt_bytes(cache),
            "compute_cache": _fmt_bytes(compute_cache),
            "diff_cache": _fmt_bytes(diff_cache),
            "total": _fmt_bytes(total),
        },
    }


def _snapshot_cache_path(project: str, content_hash: str):
    """Resolve the xorq snapshot-cache path for one entry, without materialising.

    Returns ``(applicable, path | None, note)``. Delegates to
    ``result_cache.baked_snapshot_path``, which returns None for an entry that
    bakes no snapshot — a cheap expression, salt identity mode, or a worthiness
    disagreement — and the snapshot path otherwise (the very file a cold read
    returns). It never calls ``.execute()``, so a cold expensive entry reports an
    absent (0 B) snapshot rather than being forced to materialise just because
    someone opened the metadata tab.
    """
    try:
        from tallyman_xorq.result_cache import baked_snapshot_path  # noqa: PLC0415

        path = baked_snapshot_path(project, content_hash)
    except Exception as exc:  # noqa: BLE001 — best-effort sizing, never 500 the tab
        return True, None, f"snapshot path unresolved: {type(exc).__name__}"
    if path is None:
        return False, None, "cheap entry — recomputed from the build, no snapshot copy"
    return True, path, ""


def _compute_entry_cache(project: str, content_hash: str) -> dict:
    """Per-expression storage breakdown: how much disk this one entry occupies.

    Separates *reclaimable cache* (the xorq snapshot copy and Buckaroo's
    per-entry stat cache — each self-heals or recomputes if deleted) from the
    *artifact* (xorq_build/, the portable expression that is the source of truth
    and must not be reclaimed). Diff stat caches keyed on a pair that includes
    this hash are reported separately: they're shared with the paired version,
    so folding them into a single expression's subtotal would double-count
    across the pair.
    """
    from tallyman_core.paths import diff_stat_cache_root as _diff_root  # noqa: PLC0415
    from tallyman_core.paths import entry_dir as _entry_dir  # noqa: PLC0415

    entry = _entry_dir(project, content_hash)
    components: list[dict] = []

    def add(key: str, label: str, path: Path, kind: str, reclaimable: bool, detail: str) -> None:
        size = _path_size(path)
        components.append(
            {
                "key": key,
                "label": label,
                "kind": kind,
                "reclaimable": reclaimable,
                "exists": path.exists(),
                "bytes": size,
                "formatted": _fmt_bytes(size),
                "detail": detail,
            }
        )

    applicable, snap_path, note = _snapshot_cache_path(project, content_hash)
    snap_size = _path_size(snap_path) if (applicable and snap_path is not None) else 0
    snap_exists = bool(applicable and snap_path is not None and snap_path.exists())
    components.append(
        {
            "key": "snapshot_cache",
            "label": "xorq snapshot cache",
            "kind": "cache",
            "reclaimable": True,
            "applicable": applicable,
            "exists": snap_exists,
            "bytes": snap_size,
            "formatted": _fmt_bytes(snap_size),
            "detail": note or "expensive entry — a cached copy in compute_cache/, recomputed on a miss",
        }
    )

    add(
        "buckaroo_stat_cache",
        ".buckaroo_stat_cache/ (summary stats)",
        entry / ".buckaroo_stat_cache",
        "cache",
        True,
        "Buckaroo's per-column summary stats for this entry",
    )
    add(
        "build",
        "xorq_build/ (expression artifact)",
        entry / "xorq_build",
        "artifact",
        False,
        "the portable expression — source of truth, not reclaimable",
    )

    # snapshot_cache carries its own (applicable/exists) keys, so it's already
    # in `components`; everything else went through add().
    cache_bytes = sum(c["bytes"] for c in components if c["kind"] == "cache")
    artifact_bytes = sum(c["bytes"] for c in components if c["kind"] == "artifact")

    # Diff stat caches whose pair name includes this hash's 12-char prefix.
    h12 = content_hash[:12]
    diff_caches: list[dict] = []
    diff_root = _diff_root(project)
    if diff_root.is_dir():
        # Map entry-dir prefixes → full hash so we can name the other side.
        prefix_map: dict[str, str] = {}
        ent_root = _entry_dir(project, content_hash).parent
        if ent_root.is_dir():
            for d in ent_root.iterdir():
                if d.is_dir():
                    prefix_map[d.name[:12]] = d.name
        for pair in diff_root.iterdir():
            if not pair.is_dir():
                continue
            parts = pair.name.split("-")
            if len(parts) != 2 or h12 not in parts:
                continue
            other12 = parts[1] if parts[0] == h12 else parts[0]
            size = _dir_size(pair)
            other_hash = prefix_map.get(other12)
            diff_caches.append(
                {
                    "pair": pair.name,
                    "other_hash_prefix": other12,
                    "other_hash": other_hash,
                    "other_alias": alias_for_hash(project, other_hash) if other_hash else None,
                    "bytes": size,
                    "formatted": _fmt_bytes(size),
                }
            )
    diff_caches.sort(key=lambda d: -d["bytes"])
    diff_cache_bytes = sum(d["bytes"] for d in diff_caches)

    total_bytes = cache_bytes + artifact_bytes + diff_cache_bytes
    try:
        from tallyman_xorq.result_cache import cache_worthy as _cw  # noqa: PLC0415

        worthy = _cw(project, content_hash)
    except Exception:  # noqa: BLE001
        worthy = False

    # Enrichment (#134): the recipe's raw source files, last-modified, the
    # cheap/expensive reason, and clickable lineage (parents + dependent
    # children). dependents_index walks every manifest — fine at single-entry,
    # single-user scale; revisit if it ever paginates.
    from datetime import datetime, timezone  # noqa: PLC0415

    from tallyman_core.paths import data_dir as _data_dir  # noqa: PLC0415
    from tallyman_xorq.dependents import dependents_index, parents_of, sources_of  # noqa: PLC0415

    manifest: dict = {}
    try:
        manifest = json.loads((entry / "manifest.json").read_text())
    except (OSError, ValueError):
        pass

    # Raw source files the recipe reads. None under source-identity mode=off —
    # kept distinct from "reads no files" so the UI can say "not tracked".
    src_digests = sources_of(project, content_hash)
    sources: list[dict] = []
    source_bytes = 0
    if src_digests is not None:
        dd = _data_dir(project)
        for rel in sorted(src_digests):
            sp = dd / rel
            sz = _path_size(sp)
            source_bytes += sz
            sources.append({"path": rel, "bytes": sz, "formatted": _fmt_bytes(sz), "exists": sp.exists()})

    # Last-modified across the entry's on-disk artifacts (a snapshot self-heal
    # makes this later than created_at).
    mtimes: list[float] = []
    for p in (snap_path if (applicable and snap_path) else None, entry / "xorq_build", entry / ".buckaroo_stat_cache"):
        if p is not None and p.exists():
            try:
                mtimes.append(p.stat().st_mtime)
            except OSError:
                pass
    modified_at = datetime.fromtimestamp(max(mtimes), tz=timezone.utc).isoformat() if mtimes else None

    parents = [
        {"hash": pr.hash, "ref": pr.ref, "follow": pr.follow, "alias": alias_for_hash(project, pr.hash)}
        for pr in parents_of(project, content_hash)
    ]
    children = [
        {"hash": ch, "alias": alias_for_hash(project, ch)}
        for ch in sorted(dependents_index(project).get(content_hash, set()))
    ]

    return {
        "project": project,
        "content_hash": content_hash,
        "cache_worthy": worthy,
        "cache_worthy_why": manifest.get("cache_worthy_why"),
        "created_at": manifest.get("created_at"),
        "modified_at": modified_at,
        "components": components,
        "diff_caches": diff_caches,
        "cache_bytes": cache_bytes,
        "cache_formatted": _fmt_bytes(cache_bytes),
        "artifact_bytes": artifact_bytes,
        "artifact_formatted": _fmt_bytes(artifact_bytes),
        "diff_cache_bytes": diff_cache_bytes,
        "diff_cache_formatted": _fmt_bytes(diff_cache_bytes),
        "source_bytes": source_bytes,
        "source_formatted": _fmt_bytes(source_bytes),
        "source_tracked": src_digests is not None,
        "sources": sources,
        "parents": parents,
        "children": children,
        "total_bytes": total_bytes,
        "total_formatted": _fmt_bytes(total_bytes),
    }


@functools.lru_cache(maxsize=128)
def _build_compare_expr(project: str, a_hash: str, b_hash: str, keys: tuple[str, ...]) -> tuple[Path, dict]:
    """Build and cache an xorq outer-join comparison expression for a diff pair.

    Keyed by (project, a_hash, b_hash, keys) — both entries are immutable
    content-addressed artifacts so the result is stable for the process lifetime.
    Bounded so the cache can't grow without limit over a long-lived process;
    eviction just rebuilds the (deterministic) comparison on the next view.

    Returns (build_path, column_config_overrides).
    """
    import tempfile

    from xorq.ibis_yaml.compiler import build_expr

    a = cached_result_expr(project, a_hash)
    b = cached_result_expr(project, b_hash)
    expr, overrides = build_compare_expr(a, b, list(keys))
    overrides = strip_live_diff_color(overrides)

    builds_dir = Path(tempfile.gettempdir()) / "tallyman_diff_builds"
    builds_dir.mkdir(parents=True, exist_ok=True)
    build_path = Path(build_expr(expr, builds_dir=str(builds_dir)))

    return build_path, overrides


def _invalidate_reset_caches() -> None:
    """Drop the companion's process-global result/compare caches after a reset.

    A reset prunes baked snapshots (``catalog_state.reset_to`` →
    ``prune_compute_cache``). ``_build_compare_expr`` serializes those snapshot
    paths into a build with no per-call ``exists()`` recheck, so a warmed diff
    pair would otherwise serve a build over a parquet the prune deleted —
    buckaroo reads zero files → empty compare grid (#80). ``cached_result_expr``
    self-heals on every read so clearing its memo is hygiene, not load-bearing,
    but we clear it for parity. Blunt global clear is correct and cheap: entries
    are content-addressed, so the next call rebuilds an identical expression.

    Called from BOTH reset paths — the in-server ``/api/reset`` and the
    cross-process CLI ``reset-to`` that arrives as a ``project_reset`` on
    ``/internal/notify`` — so the two can't drift. (The in-server path having the
    clear while the notify path lacked it was exactly #80's "Communication gap".)
    """
    _build_compare_expr.cache_clear()
    cached_result_expr.cache_clear()


def _recalc_sse_event(remap: dict, step: int | None) -> dict:
    """The recalc SSE event, in one canonical shape: ``{kind, remap, step}``.

    Both recalc emitters publish through this — the in-process ``/api/recalc``
    route and the cross-process ``/internal/notify`` path an MCP-driven recalc
    arrives on — so the event a frontend sees can't depend on which surface
    triggered it (the two used to drift: the notify path republished the raw
    ``payload.model_dump()``, burying ``remap`` under ``extra`` and dropping
    ``step``).
    """
    return {"kind": "recalc", "remap": remap, "step": step}


def _annotate_entries(project: str, entries: list[dict]) -> list[dict]:
    """Add alias/version fields and sort: named entries (by alias) first, then scratch."""
    aliases = load_aliases(project)
    by_latest = {h: a for a, h in aliases.items()}  # hash -> alias when hash is current
    for e in entries:
        h = e["content_hash"]
        if h in by_latest:
            e["alias"] = by_latest[h]
            info = version_of_hash(project, h)
            e["version"] = info[1] if info else 1
            e["is_current"] = True
            e["version_count"] = len(history_for(project, e["alias"]))
        else:
            info = version_of_hash(project, h)
            if info is not None:
                e["alias"] = info[0]
                e["version"] = info[1]
                e["is_current"] = False
                e["version_count"] = len(history_for(project, e["alias"]))
            else:
                e["alias"] = None
                e["version"] = None
                e["is_current"] = False
                e["version_count"] = 0

    # Sort: current versions of named entries first (by alias), then forensic
    # versions, then scratch, each by mtime-desc within their bucket.
    def _bucket(e):
        if e["is_current"]:
            return (0, e["alias"])
        if e["alias"]:
            return (1, e["alias"], -e.get("version", 0))
        return (2,)

    return sorted(entries, key=_bucket)


def _serve_spa() -> HTMLResponse:
    index = _REACT_DIST / "index.html"
    if index.exists():
        return HTMLResponse(index.read_text())
    raise HTTPException(
        503,
        "React app not built — run: cd packages/app && pnpm install && pnpm build",
    )


def _backfilled_log_events(project: str, seen_error_ids: set, seen_hashes: set) -> list[dict]:
    """Activity-log events synthesised from the pre-existing stores.

    The Log tab should show build failures and entries even when events.jsonl
    didn't capture them — e.g. recorded before the activity log existed, or by an
    MCP process that predates the event-emitting change (it writes errors.jsonl /
    manifests but not events.jsonl). Backfills ``build_error`` from errors.jsonl
    and ``build_ok`` / ``alias_set`` from entry manifests, skipping anything
    already in events.jsonl (deduped by error_id / content hash so the rich event
    wins). Backfilled rows carry no session id and are marked ``origin=history``.
    """
    out: list[dict] = []
    for err in list_errors(project, limit=1_000_000_000):
        if err.get("id") in seen_error_ids:
            continue
        out.append(
            {
                "ts": err.get("created_at", ""),
                "kind": "build_error",
                "category": "mcp",
                "session": None,
                "origin": "history",
                "tool": err.get("tool"),
                "prompt": err.get("prompt"),
                "code": err.get("code"),
                "message": err.get("message"),
                "traceback": err.get("traceback"),
                "error_id": err.get("id"),
                "hash": err.get("hash"),
            }
        )
    for ent in list_entries(project):
        h = ent.get("content_hash")
        if not h or h in seen_hashes:
            continue
        ev = {
            "ts": ent.get("created_at", ""),
            "category": "mcp",
            "session": None,
            "origin": "history",
            "prompt": ent.get("prompt"),
            "hash": h,
        }
        info = version_of_hash(project, h)
        if info:
            ev.update(kind="alias_set", category="alias", alias=info[0], version=info[1])
        else:
            ev["kind"] = "build_ok"
        out.append(ev)
    return out


def create_app(
    project: str | None = None,
    *,
    read_only: bool = False,
    buckaroo: "BuckarooManager | None" = None,
) -> FastAPI:
    # ``project`` (if given) seeds the active-project file on startup but is
    # never the runtime source of truth — every route re-resolves via
    # ``_current_project()`` so dropdown switches take effect without
    # rebuilding the app.
    seed = resolve_project(project)
    if seed and project:
        # CLI passed --project explicitly: make it stick by writing the file.
        try:
            from tallyman_core.paths import set_active_project as _sap

            _sap(project)
        except (FileNotFoundError, ValueError):
            pass

    # Upgrade migration: sweep any per-entry result.parquet and #71 viewer-read
    # build dirs once. The marker was bumped for this change, so an install that
    # already ran the #73 sweep re-runs it once to clear anything written since.
    # Guarded by a per-project marker, so it's a no-op on later starts.
    if seed and not read_only:
        try:
            from tallyman_xorq.build import migrate_drop_result_parquet

            migrate_drop_result_parquet(seed)
        except Exception:  # best-effort; cleanup must never block startup
            log.warning("result.parquet migration failed for %s", seed, exc_info=True)

    app = FastAPI(title="tallyman companion")
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    # Serve React SPA assets (built by `cd packages/app && pnpm build`).
    if (_REACT_DIST / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=str(_REACT_DIST / "assets")), name="spa-assets")

    @app.middleware("http")
    async def _stamp_revision(request, call_next):
        # Every companion response carries the git revision it was served from,
        # so a client (or curl -I) can see which source the backend runs — and
        # the SPA compares it against its own baked build revision to catch the
        # stale-bundle drift behind #132.
        response = await call_next(request)
        response.headers["X-Tallyman-Revision"] = git_revision()
        return response

    @app.get("/api/version")
    def api_version():
        """The git revision this companion process was launched from (#132)."""
        return {"component": "companion", **version_info()}

    def _current_project() -> str | None:
        return resolve_project() or seed

    def _require_project() -> str:
        """Used only by project-agnostic routes (notify, api/projects*)."""
        p = _current_project()
        if p is None:
            raise HTTPException(503, "No active project — visit / to create one")
        return p

    def _validate_project(p: str) -> str:
        """Validate a project name from a URL path parameter.

        Must be a syntactically legal name AND exist on disk, else 404.
        ``resolve_project(p)`` can't do this — it echoes its explicit
        argument verbatim, so it never signals an unknown project.
        """
        try:
            validate_project_name(p)
            exists = project_dir(p).is_dir()
        except ValueError:
            exists = False
        if not exists:
            raise HTTPException(404, f"project {p!r} not found")
        return p

    # In-process fan-out for SSE clients. Each client gets its own queue.
    subscribers: list[asyncio.Queue] = []

    async def publish(event: dict):
        event.setdefault("project", _current_project())
        for q in list(subscribers):
            await q.put(event)

    async def _auto_recalc_after_head_advance(project: str, name: str, *, tool: str) -> dict | None:
        """Cascade-recompute *name*'s stale followers after an in-browser head
        advance (PUT /api/code, promote_diff), in the SAME revision the checkpoint
        middleware commits — the walk takes no checkpoint of its own. ``tool`` names
        the route so a persisted cascade failure is attributed to it. Returns the
        recalc sub-report (plus ``orphan_stale``), or ``None`` when the per-project
        ``auto_recalc`` switch is off. On a real cascade, invalidates the companion
        caches, reloads buckaroo sessions, and publishes the canonical recalc SSE
        event — the same cleanup /api/recalc does. Parity with the MCP path."""
        from starlette.concurrency import run_in_threadpool  # noqa: PLC0415

        from tallyman_core.config import auto_recalc_enabled  # noqa: PLC0415
        from tallyman_xorq.recalc import auto_recalc  # noqa: PLC0415

        if not await run_in_threadpool(auto_recalc_enabled, project):
            return None
        report = await run_in_threadpool(auto_recalc, project, name, tool=tool)
        if report.get("remap"):
            _invalidate_reset_caches()
            if buckaroo:
                buckaroo.reload_project_sessions(project)
            # checkpoint_step is None on the auto path: the checkpoint middleware
            # commits the head advance + cascade as one revision *after* this
            # handler returns, so the step isn't known here. The SSE event carries
            # the remap (what the SPA refetches on); the step it ignores. Unlike
            # the MCP decorator, the response body is already serialized when the
            # middleware checkpoints, so there's nothing to backfill into.
            await publish(_recalc_sse_event(report["remap"], report.get("checkpoint_step")))
        return report

    # ------------------------------------------------------------------
    # Dispatch-boundary checkpoint (reset-to-revision). Opt-out: every
    # successful non-GET response checkpoints once, unless the route is on
    # the explicit denylist — so a new authored-state route is covered by
    # default and coverage cannot silently regress.
    # ------------------------------------------------------------------

    def _checkpoint_exempt(method: str, path: str) -> bool:
        if method in ("GET", "HEAD", "OPTIONS"):
            return True
        if path.startswith("/internal/") or path.startswith("/api/projects"):
            return True  # SSE plumbing + project lifecycle (genesis covers new)
        if "/api/result_cache" in path or path.endswith("/api/errors"):
            return True  # cache/log clears, not authored state
        if path.endswith("/api/telemetry"):
            # buckaroo perf-span ingest (#943): observability, not authored
            # state — and many spans/load, so a checkpoint each would flood git.
            return True
        if path.endswith("/api/reset"):
            return True  # reset moves `current`; it is not a new step
        if path.endswith("/api/recalc"):
            return True  # recalc self-checkpoints (one tx for the whole walk; dry-run takes none)
        if "/api/promote_diff/" in path:
            return True  # promote_diff self-checkpoints (the promote + cascade as one tx)
        return False

    @app.middleware("http")
    async def _checkpoint_after_mutation(request, call_next):
        response = await call_next(request)
        path = request.url.path
        if read_only or response.status_code >= 400 or _checkpoint_exempt(request.method, path):
            return response
        # Mutating project routes all live at /{project}/api/... — take the
        # first segment and checkpoint only when it names a real project.
        seg = path.split("/", 2)
        name = seg[1] if len(seg) > 1 else ""
        try:
            validate_project_name(name)
            known = project_dir(name).is_dir()
        except ValueError:
            known = False
        if not known:
            return response
        from starlette.concurrency import run_in_threadpool  # noqa: PLC0415

        from tallyman_core.catalog_state import checkpoint_catalog  # noqa: PLC0415

        try:
            await run_in_threadpool(checkpoint_catalog, name, f"tallyman: {request.method} {path}")
        except Exception as exc:
            log.warning("checkpoint after %s %s failed: %s", request.method, path, exc)
        return response

    @app.on_event("startup")
    async def _log_revision():
        log.info("tallyman companion revision %s", git_revision())

    @app.on_event("startup")
    async def _warm_expr_cache():
        from starlette.concurrency import run_in_threadpool  # noqa: PLC0415

        project = _current_project()
        if project is None:
            return
        base = entries_dir(project)
        if not base.is_dir():
            return
        hashes = [d.name for d in base.iterdir() if (d / ENTRY_BUILD_DIRNAME).is_dir()]
        if not hashes:
            return

        _WARMUP_BUDGET = 3.0

        def _warm():
            import time as _time  # noqa: PLC0415

            t_start = _time.perf_counter()
            warmed = 0
            for h in hashes:
                elapsed = _time.perf_counter() - t_start
                if elapsed >= _WARMUP_BUDGET:
                    log.warning(
                        "expr cache warmup hit %.1fs budget after %d/%d entries"
                        " — remaining entries will warm on first access",
                        _WARMUP_BUDGET,
                        warmed,
                        len(hashes),
                    )
                    return
                try:
                    cached_result_expr(project, h)
                    warmed += 1
                    log.debug(
                        "expr cache warmed %s (%.0fms, %.0fms elapsed)",
                        h[:12],
                        (_time.perf_counter() - t_start - elapsed) * 1000,
                        (_time.perf_counter() - t_start) * 1000,
                    )
                except Exception as exc:
                    log.warning("expr cache warmup failed for %s: %s", h[:12], exc)
            log.info(
                "expr cache warmed %d/%d entries in %.0fms",
                warmed,
                len(hashes),
                (_time.perf_counter() - t_start) * 1000,
            )

        await run_in_threadpool(_warm)

    # ------------------------------------------------------------------
    # Root redirect
    # ------------------------------------------------------------------

    @app.get("/", include_in_schema=False)
    def root():
        p = _current_project()
        if p:
            return RedirectResponse(f"/{p}/catalog")
        return _serve_spa()

    # ------------------------------------------------------------------
    # File-download routes (non-HTML, kept from original)
    # ------------------------------------------------------------------

    @app.get("/{project}/export/marimo")
    def export_marimo(project: str):
        project = _validate_project(project)
        from fastapi.responses import Response  # noqa: PLC0415

        from tallyman_core.marimo_export import notebook_to_marimo  # noqa: PLC0415

        content = notebook_to_marimo(project)
        filename = f"{project}_notebook.py"
        return Response(
            content=content,
            media_type="text/x-python",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # ------------------------------------------------------------------
    # Project-prefixed JSON API routes
    # ------------------------------------------------------------------

    @app.get("/{project}/api/sse")
    async def sse(request: Request, project: str):
        project = _validate_project(project)
        queue: asyncio.Queue = asyncio.Queue()
        subscribers.append(queue)

        async def event_stream():
            try:
                yield {"event": "hello", "data": json.dumps({"project": project})}
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=10.0)
                        yield {
                            "event": event.get("kind", "message"),
                            "data": json.dumps(event),
                        }
                    except asyncio.TimeoutError:
                        yield {"event": "ping", "data": "{}"}
            finally:
                if queue in subscribers:
                    subscribers.remove(queue)

        return EventSourceResponse(event_stream())

    @app.get("/{project}/api/data/{content_hash}")
    def api_data(project: str, content_hash: str, offset: int = 0, limit: int = 200):
        project = _validate_project(project)
        _require_hash(content_hash)
        if limit < 0:
            raise HTTPException(400, "limit must be >= 0")
        from tallyman_core.paths import entry_build_dir  # noqa: PLC0415

        if not entry_build_dir(project, content_hash).is_dir():
            raise HTTPException(404, "no entry")
        # #90: serve the page off the entry's expression. cached_result_expr
        # already hands back the result as a live single-backend expression, so
        # the window pushes down (expensive → windowed read of the baked snapshot,
        # cheap → limit pushed through to the source read) and nothing is written.
        # The old path called ensure_result, materialising the entry's entire
        # result to disk to serve one page. total is the manifest's row_count
        # (recorded at build; api_entry_detail reads it the same way), so no file
        # is needed to populate it. The manifest is written after the build dir
        # exists (build.py), so guard the read: a half-built or pruned entry still
        # serves its page off the expression with a best-effort total of 0 rather
        # than 500ing on a missing manifest — cached_result_expr needs none.
        manifest_path = entry_dir(project, content_hash) / ENTRY_MANIFEST_FILENAME
        total = 0
        if manifest_path.exists():
            total = json.loads(manifest_path.read_text()).get("row_count") or 0
        df = cached_result_expr(project, content_hash).limit(limit, offset=offset).execute()
        return {
            "data": json.loads(df.to_json(orient="records")),
            "offset": offset,
            "limit": limit,
            "total": total,
        }

    @app.get("/{project}/api/entries")
    def api_entries(project: str):
        project = _validate_project(project)
        return {
            "project": project,
            "entries": _annotate_entries(project, list_entries(project)),
        }

    @app.get("/{project}/api/entry/{content_hash}")
    def api_entry_detail(project: str, content_hash: str):
        """Full entry detail for the React SPA — resolves aliases."""
        project = _validate_project(project)
        aliases = load_aliases(project)
        alias = None
        if content_hash in aliases:
            alias = content_hash
            content_hash = aliases[alias]
        else:
            _require_hash(content_hash)

        entry = entry_dir(project, content_hash)
        if not entry.exists():
            raise HTTPException(404, f"entry {content_hash!r} not found")

        manifest = json.loads((entry / ENTRY_MANIFEST_FILENAME).read_text())
        schema = json.loads((entry / ENTRY_SCHEMA_FILENAME).read_text())
        code = (entry / "expr.py").read_text()

        if alias is None:
            alias = alias_for_hash(project, content_hash)
        info = version_of_hash(project, content_hash)
        if info is not None:
            alias = alias or info[0]
            version = info[1]
        else:
            version = None

        forensic_history: list[dict] = []
        if alias:
            hist_hashes = history_for(project, alias)
            for i, h in enumerate(hist_hashes, start=1):
                forensic_history.append({"hash": h, "version": i, "is_current": h == content_hash})

        prompt_history = read_prompts(project, content_hash)
        chart_spec = get_chart(project, content_hash)

        build_artifacts: list[dict] = []
        build_dir = entry / ENTRY_BUILD_DIRNAME
        if build_dir.is_dir():
            for f in sorted(build_dir.iterdir()):
                if not f.is_file():
                    continue
                try:
                    text = f.read_text()
                except UnicodeDecodeError:
                    text = f"(binary, {f.stat().st_size} bytes)"
                build_artifacts.append({"name": f.name, "text": text})

        from tallyman_core.display_configs import get_display_config  # noqa: PLC0415

        display_config = get_display_config(project, content_hash)

        # The Buckaroo session is loaded lazily by the SPA via GET
        # /api/session/{hash}, so the detail request never blocks on the snapshot
        # bake + /load_expr POST — that synchronous path was the multi-minute
        # hang in #133. The payload stays metadata-only; buckaroo_session is kept
        # for shape compatibility (always None now).
        buckaroo_session = None
        buckaroo_ws_base = buckaroo.ws_base_url if buckaroo and buckaroo.is_running else None

        # #73: row count comes from the manifest, not a result.parquet (cheap
        # entries no longer write one at build time).
        total_rows = (manifest or {}).get("row_count", 0)

        return {
            "project": project,
            "content_hash": content_hash,
            "manifest": manifest,
            "schema": schema,
            "code": code,
            "alias": alias,
            "version": version,
            "forensic_history": forensic_history,
            "prompt_history": prompt_history,
            "chart_spec": chart_spec,
            "display_config": display_config,
            "build_artifacts": build_artifacts,
            "total_rows": total_rows,
            "buckaroo_session": buckaroo_session,
            "buckaroo_ws_base": buckaroo_ws_base,
        }

    @app.get("/{project}/api/entry_cache/{content_hash}")
    def api_entry_cache(project: str, content_hash: str):
        """Per-expression cache/storage breakdown for the metadata tab.

        Resolves an alias to its current hash, then attributes the entry's
        on-disk footprint across reclaimable caches vs. the build artifact.
        Computed on demand (it may reconstruct the expression to locate the
        snapshot path), so it's a separate route from the always-loaded entry
        detail.
        """
        project = _validate_project(project)
        aliases = load_aliases(project)
        if content_hash in aliases:
            content_hash = aliases[content_hash]
        else:
            _require_hash(content_hash)
        if not entry_dir(project, content_hash).exists():
            raise HTTPException(404, f"entry {content_hash!r} not found")
        return _compute_entry_cache(project, content_hash)

    @app.get("/{project}/api/session/{content_hash}")
    def get_session(project: str, content_hash: str):
        """Load the Buckaroo session for an entry, with a typed status.

        Returns ``{"status", "ws_url", "detail"}`` (#133): ``ok`` carries the
        ws_url; ``timeout``/``error``/``unavailable``/``no_build`` carry a
        human-readable ``detail`` so the SPA shows a precise message and a retry
        instead of a silent empty grid. Blocks until the session resolves or
        fails — the SPA shows a spinner meanwhile.
        """
        project = _validate_project(project)
        if buckaroo is None or not buckaroo.is_running:
            return {
                "status": "unavailable",
                "ws_url": None,
                "detail": "Buckaroo is not running — start tallyman with --buckaroo.",
            }
        from tallyman_core.display_configs import get_display_config  # noqa: PLC0415

        overrides = (get_display_config(project, content_hash) or {}).get("column_config_overrides")
        _t0 = time.perf_counter()
        result = buckaroo.load_session(content_hash, project, column_config_overrides=overrides)
        # One activity-log event per grid load: the companion-visible timing
        # (total + the /load_expr POST). Deeper pipeline timing → buckaroo#943.
        record_event(
            project,
            "buckaroo",
            origin="companion",
            status=result["status"],
            hash=content_hash,
            detail=result.get("detail") or None,
            load_ms=round((time.perf_counter() - _t0) * 1000, 1),
            load_expr_ms=result.get("load_expr_ms"),
            # The buckaroo session id is the telemetry `trace` key (buckaroo#943):
            # the Log UI joins this event to the firstpull.* spans buckaroo POSTs
            # back to /api/telemetry. None when the load failed before a session
            # was minted.
            session_id=result.get("session_id"),
        )
        ws_url = f"{buckaroo.ws_base_url}/ws/{result['session_id']}" if result["session_id"] else None
        return {"status": result["status"], "ws_url": ws_url, "detail": result["detail"]}

    @app.get("/{project}/api/notebook")
    def api_notebook(project: str):
        project = _validate_project(project)
        return {"project": project, **notebook.load(project)}

    @app.get("/{project}/api/notebook_full")
    def api_notebook_full(project: str):
        """Notebook with pre-resolved entry metadata for the React SPA."""
        project = _validate_project(project)
        data = notebook.load(project)
        aliases = load_aliases(project)
        buckaroo_available = buckaroo is not None and buckaroo.is_running
        buckaroo_ws_base = buckaroo.ws_base_url if buckaroo_available else None
        cells = []
        for c in data["cells"]:
            latest = aliases.get(c["alias"])
            entry_meta = None
            schema = None
            total_rows = 0
            chart_spec = None
            buckaroo_session = None
            if latest is not None:
                entry = entry_dir(project, latest)
                if (entry / ENTRY_MANIFEST_FILENAME).exists():
                    entry_meta = json.loads((entry / ENTRY_MANIFEST_FILENAME).read_text())
                    schema = json.loads((entry / ENTRY_SCHEMA_FILENAME).read_text())
                    # #73: row count from the manifest, not a result.parquet.
                    total_rows = entry_meta.get("row_count", 0)
                chart_spec = get_chart(project, latest)
                if buckaroo_available:
                    buckaroo_session = buckaroo.ensure_session(latest, project)
            info = version_of_hash(project, latest) if latest else None
            cells.append(
                {
                    **c,
                    "latest_hash": latest,
                    "version": info[1] if info else None,
                    "entry_meta": entry_meta,
                    "schema": schema,
                    "total_rows": total_rows,
                    "chart_spec": chart_spec,
                    "buckaroo_session": buckaroo_session,
                }
            )
        return {
            "project": project,
            "cells": cells,
            "buckaroo_ws_base": buckaroo_ws_base,
            "buckaroo_available": buckaroo_available,
        }

    @app.get("/{project}/diff/{alias}", include_in_schema=False)
    def diff_default_versions(project: str, alias: str):
        """Redirect /diff/{alias} to /diff/{alias}/{n-1}/{n} (latest two versions)."""
        project = _validate_project(project)
        hashes = history_for(project, alias)
        if not hashes:
            raise HTTPException(404, f"alias {alias!r} not found or has no history")
        n = len(hashes)
        va = max(1, n - 1)
        vb = n
        if va == vb:
            va = 1
        return RedirectResponse(f"/{project}/diff/{alias}/{va}/{vb}")

    @app.get("/{project}/api/diff_data/{alias}/{va:int}/{vb:int}")
    def api_diff_data(project: str, alias: str, va: int, vb: int):
        """Diff data as JSON for the React SPA."""
        project = _validate_project(project)
        hashes = history_for(project, alias)
        if not hashes or va < 1 or vb < 1 or va > len(hashes) or vb > len(hashes):
            raise HTTPException(404, "version out of range")
        if va == vb:
            raise HTTPException(400, f"V{va} → V{vb} is the same version")
        a_hash = hashes[va - 1]
        b_hash = hashes[vb - 1]
        a_dir = entry_dir(project, a_hash)
        b_dir = entry_dir(project, b_hash)
        if not a_dir.exists() or not b_dir.exists():
            raise HTTPException(404, "one or both entries missing")

        try:
            a_expr = cached_result_expr(project, a_hash)
            b_expr = cached_result_expr(project, b_hash)
            keys = diff_keys(project, a_hash, b_hash) or None
            diff = full_diff(
                a_dir,
                b_dir,
                a_label=f"V{va}",
                b_label=f"V{vb}",
                a_expr=a_expr,
                b_expr=b_expr,
                keys=keys,
            )
        except Exception as exc:
            # Full traceback to the server log; a concise type+message to the
            # client (the diff UI renders `detail`). Avoids dumping internal
            # paths/source if this endpoint ever sits behind anything but
            # localhost, while still surfacing the real error.
            log.exception("diff_data failed for %s/%s V%d→V%d", project, alias, va, vb)
            raise HTTPException(500, detail=f"{type(exc).__name__}: {exc}") from exc

        compare_session = None
        buckaroo_ws_base_url = None
        if buckaroo is not None and buckaroo.is_running:
            if not keys:
                keys = diff["keyed"]["keys"] if diff.get("keyed") else None
            if keys:
                session_id = f"diff-{a_hash[:12]}-{b_hash[:12]}"
                try:
                    build_path, overrides = _build_compare_expr(project, a_hash, b_hash, tuple(keys))
                    if buckaroo.diff_session_is_loaded(session_id):
                        compare_session = session_id
                        buckaroo_ws_base_url = buckaroo.ws_base_url
                    else:
                        from tallyman_core.paths import diff_stat_cache_dir  # noqa: PLC0415

                        stat_cache = diff_stat_cache_dir(project, a_hash, b_hash)
                        stat_cache.mkdir(parents=True, exist_ok=True)
                        _diff_extras = Path(__file__).parent / "diff_extras"
                        resp = buckaroo._client.post(
                            f"{buckaroo.base_url}/load_expr",
                            json={
                                "session": session_id,
                                "build_dir": str(build_path),
                                "no_browser": True,
                                "column_config_overrides": overrides,
                                "cache_storage_path": str(stat_cache),
                                "extra_grid_config": {"searchDebounceMs": 3000},
                                "project_root": str(_diff_extras),
                            },
                            timeout=30.0,
                        )
                        if resp.status_code == 200:
                            buckaroo.mark_diff_session_loaded(session_id)
                            compare_session = session_id
                            buckaroo_ws_base_url = buckaroo.ws_base_url
                except Exception as exc:
                    log.warning("diff /load_expr compare failed: %s", exc)

        return {
            "project": project,
            "alias": alias,
            "va": va,
            "vb": vb,
            "a_hash": a_hash,
            "b_hash": b_hash,
            "hashes": hashes,
            "diff": diff,
            "compare_session": compare_session,
            "buckaroo_ws_base": buckaroo_ws_base_url,
        }

    @app.post("/{project}/api/promote_diff/{alias}/{va:int}/{vb:int}")
    async def api_promote_diff(project: str, alias: str, va: int, vb: int):
        """Promote a diff between two versions to a first-class catalog entry."""
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        project = _validate_project(project)

        import textwrap  # noqa: PLC0415

        from tallyman_companion.diff import compute_column_config_overrides  # noqa: PLC0415
        from tallyman_core.aliases import AliasExists, set_alias  # noqa: PLC0415
        from tallyman_core.catalog_state import checkpoint_catalog  # noqa: PLC0415
        from tallyman_core.display_configs import set_display_config  # noqa: PLC0415
        from tallyman_xorq import build_and_persist as _build_and_persist  # noqa: PLC0415
        from tallyman_xorq.build import BuildError  # noqa: PLC0415
        from tallyman_xorq.primary_key import diff_keys  # noqa: PLC0415
        from tallyman_xorq.result_cache import cached_result_expr  # noqa: PLC0415

        hashes = history_for(project, alias)
        if not hashes:
            raise HTTPException(404, f"alias {alias!r} has no history")

        def _resolve(v: int):
            if v < 0:
                v = len(hashes) + v + 1
            if v < 1 or v > len(hashes):
                return None
            return v, hashes[v - 1]

        a = _resolve(va)
        b = _resolve(vb)
        if a is None or b is None:
            raise HTTPException(400, f"version out of range; alias has {len(hashes)} versions")
        a_idx, a_hash = a
        b_idx, b_hash = b

        keys = diff_keys(project, a_hash, b_hash) or []
        if not keys:
            raise HTTPException(400, "no stable join key detected; cannot build keyed diff")

        a_expr = cached_result_expr(project, a_hash)
        b_expr = cached_result_expr(project, b_hash)
        column_config_overrides = compute_column_config_overrides(a_expr.schema(), b_expr.schema(), keys)

        target_alias = f"diff_{alias}_v{a_idx}_v{b_idx}"
        keys_repr = repr(keys)
        code = textwrap.dedent(f"""\
            # auto-generated — diff of {alias} V{a_idx} → V{b_idx}
            # a_hash: {a_hash}
            # b_hash: {b_hash}
            from tallyman_companion.diff import build_diff_expr
            expr = build_diff_expr(
                a_hash={a_hash!r},
                b_hash={b_hash!r},
                keys={keys_repr},
            )
        """)

        try:
            result = _build_and_persist(project, code, prompt=f"promote diff {alias} V{a_idx}→V{b_idx}")
        except BuildError as exc:
            raise HTTPException(500, str(exc)) from exc

        repointed = False
        try:
            set_alias(project, target_alias, result.content_hash, expect_exists=False)
        except AliasExists:
            set_alias(project, target_alias, result.content_hash)
            repointed = True  # re-pointed an existing alias → its followers may go stale

        set_display_config(
            project,
            result.content_hash,
            {
                "column_config_overrides": column_config_overrides,
                "diff_provenance": {
                    "source_alias": alias,
                    "va": a_idx,
                    "vb": b_idx,
                    "a_hash": a_hash,
                    "b_hash": b_hash,
                    "keys": keys,
                },
            },
        )

        # D5 parity: re-pointing an existing alias cascades to its followers in the
        # same revision (the walk runs before this route's checkpoint).
        recalc_report = (
            await _auto_recalc_after_head_advance(project, target_alias, tool="companion_promote_diff")
            if repointed
            else None
        )

        checkpoint_catalog(project, f"tallyman: catalog_promote_diff {alias} V{a_idx}→V{b_idx}")
        await publish({"kind": "entry_added", "hash": result.content_hash})

        out = {
            "alias": target_alias,
            "hash": result.content_hash,
            "source_alias": alias,
            "va": a_idx,
            "vb": b_idx,
            "a_hash": a_hash,
            "b_hash": b_hash,
            "keys": keys,
            "row_count": result.row_count,
        }
        if recalc_report is not None:
            out["recalc"] = recalc_report
        return out

    @app.get("/{project}/api/error/{error_id}")
    def api_error_single(project: str, error_id: str):
        """Single error record for the React SPA."""
        project = _validate_project(project)
        rec = get_error(project, error_id)
        if rec is None:
            raise HTTPException(404, f"error {error_id!r} not found")
        return {"project": project, "error": rec}

    @app.get("/{project}/api/aliases")
    def api_aliases(project: str):
        project = _validate_project(project)
        aliases = load_aliases(project)
        return {
            "project": project,
            "aliases": [
                {
                    "name": name,
                    "hash": hash_,
                    "history": history_for(project, name),
                }
                for name, hash_ in sorted(aliases.items())
            ],
        }

    @app.get("/{project}/api/errors")
    def api_errors(project: str, limit: int = 20):
        project = _validate_project(project)
        return {"project": project, "errors": list_errors(project, limit=limit)}

    @app.get("/{project}/api/log")
    def api_log(
        project: str,
        categories: str | None = None,
        sessions: str | None = None,
        limit: int = 300,
    ):
        """The project activity log: a linear, cross-session feed of MCP build
        runs (success/failure with traceback), alias creations, and Buckaroo
        grid loads. ``categories`` (mcp,alias,buckaroo) and ``sessions`` are
        comma-separated filters; the response also lists the distinct sessions
        seen, for the UI's per-session filter.
        """
        project = _validate_project(project)
        cats = [c for c in categories.split(",") if c] if categories else None
        sess = [s for s in sessions.split(",") if s] if sessions else None
        # events.jsonl (rich: session, traceback, timing) merged with a backfill
        # from the pre-existing stores, so build failures and entries show even
        # when no event was recorded for them. Read raw + unfiltered to dedup,
        # then filter / sort / limit the merged feed.
        raw = read_events(project, limit=1_000_000_000)
        seen_error_ids = {e["error_id"] for e in raw if e.get("error_id")}
        seen_hashes = {e["hash"] for e in raw if e.get("hash") and e.get("kind") in ("build_ok", "alias_set")}
        merged = raw + _backfilled_log_events(project, seen_error_ids, seen_hashes)
        if cats:
            merged = [e for e in merged if e.get("category") in cats]
        if sess:
            merged = [e for e in merged if e.get("session") in sess]
        merged.sort(key=lambda e: e.get("ts", ""), reverse=True)
        return {
            "project": project,
            "events": merged[:limit],
            "sessions": list_sessions(project),
        }

    @app.post("/{project}/api/telemetry")
    async def api_telemetry_ingest(project: str, payload: dict):
        """Receive one grid-load perf span from the Buckaroo server (buckaroo#943).

        Buckaroo fire-and-forget POSTs ``{trace, source, name, t_start_ms,
        t_end_ms, attrs}`` here as each ``firstpull.*`` span closes (we pass this
        URL on ``/load_expr``). ``trace`` is the buckaroo session id, matching the
        ``session_id`` on the parent ``buckaroo`` activity-log event, so the Log
        UI joins a load to its spans. Always 200 (even on a rejected record) so a
        best-effort telemetry POST never reads as a server error on buckaroo's
        side. Not gated on ``read_only``: buckaroo only runs in ``tallyman run``,
        but accepting the span is pure observability, never authored state.
        """
        project = _validate_project(project)
        stored = record_span(project, payload)
        return {"ok": stored is not None}

    @app.get("/{project}/api/telemetry")
    def api_telemetry_read(project: str, trace: str | None = None, limit: int = 500):
        """Persisted grid-load spans, newest-first, optionally one ``trace``.

        The Log UI calls this with ``trace=<session_id>`` to render the per-load
        span waterfall when a ``buckaroo`` event is expanded.
        """
        project = _validate_project(project)
        return {"project": project, "trace": trace, "spans": read_spans(project, trace=trace, limit=limit)}

    @app.delete("/{project}/api/errors")
    def api_clear_errors(project: str):
        """Permanently drop all build-failure records for a project."""
        project = _validate_project(project)
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        return {"project": project, "cleared": clear_errors(project)}

    # Per-app, per-project TTL cache so a burst of page mounts / new_entry
    # refreshes shares one filesystem walk. App-scoped (not module-level) so
    # test apps over isolated homes never read each other's cached totals.
    _disk_usage_cache: dict[str, tuple[float, dict]] = {}

    @app.get("/{project}/api/disk_usage")
    def api_disk_usage(project: str):
        project = _validate_project(project)
        now = time.monotonic()
        cached = _disk_usage_cache.get(project)
        if cached is not None and now - cached[0] < _DISK_USAGE_TTL:
            return cached[1]
        payload = _compute_disk_usage(project)
        _disk_usage_cache[project] = (now, payload)
        return payload

    @app.get("/{project}/api/result_cache")
    def api_result_cache(project: str):
        project = _validate_project(project)
        import datetime  # noqa: PLC0415

        from tallyman_core.paths import entries_dir as _entries_dir  # noqa: PLC0415

        root = _entries_dir(project)
        entries = []
        if root.is_dir():
            for entry in root.iterdir():
                if not entry.is_dir():
                    continue
                try:
                    m = read_manifest(entry)
                except Exception:
                    continue
                content_hash = entry.name
                # The page lists the baked snapshots that exist on disk right now —
                # the rows the delete button can actually evict. cache_bytes (#87) is
                # a cheap pre-filter: None for a cheap entry (bakes nothing), so skip
                # it without deriving a path. For an expensive entry, resolve the
                # snapshot and skip it when the file is gone — a delete unlinks the
                # snapshot but leaves cache_bytes set, so keying the listing on
                # cache_bytes alone showed a phantom row (stale size, freed bytes
                # still in the total, a delete button that 404s) until the entry was
                # next viewed and re-baked. Size from the live file so the figure
                # tracks disk. baked_snapshot_path re-imports the recipe, but only
                # for the (few) expensive entries the pre-filter lets through, and
                # this admin page isn't on a hot path.
                if m.cache_bytes is None:
                    continue
                try:
                    snap = baked_snapshot_path(project, content_hash)
                    if snap is None or not snap.exists():
                        continue
                    size = snap.stat().st_size
                except Exception:
                    continue
                # created_at is an ISO-8601 UTC string; render it in the table's
                # existing "%Y-%m-%d %H:%M:%S" shape (CachePage renders `created`
                # verbatim) so the Created column is unchanged.
                try:
                    created = datetime.datetime.fromisoformat(m.created_at).strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError):
                    created = m.created_at or ""
                alias = alias_for_hash(project, content_hash)
                info = version_of_hash(project, content_hash)
                version = info[1] if info else None
                is_current = alias is not None
                if not is_current and info is not None:
                    alias = info[0]
                    is_current = False
                entries.append(
                    {
                        "hash": content_hash,
                        "size": size,
                        "size_formatted": _fmt_bytes(size),
                        # row_count is int|None on the manifest, but CacheEntry
                        # types it number and CachePage calls .toLocaleString()
                        # with no null guard — coerce so a null can't reach JS.
                        "row_count": int(m.row_count or 0),
                        "created": created,
                        "alias": alias,
                        "version": version,
                        "is_current": is_current,
                        "prompt": m.prompt,
                    }
                )
        entries.sort(key=lambda e: -e["size"])
        total = sum(e["size"] for e in entries)
        return {
            "project": project,
            "entries": entries,
            "total": total,
            "total_formatted": _fmt_bytes(total),
        }

    @app.delete("/{project}/api/result_cache/{content_hash}")
    async def api_delete_result_cache(project: str, content_hash: str):
        project = _validate_project(project)
        if read_only:
            raise HTTPException(403, "serve mode")
        # Reject a non-hex hash up front: a 400 reads truer than the
        # "already evicted" 404 below (see _require_hash).
        _require_hash(content_hash)
        # Evict the baked .cache() snapshot — the single materialised copy. None
        # for a cheap entry (nothing to delete) or one already evicted; reading
        # the entry's viewer page afterward self-heals (re-bakes) it.
        p = baked_snapshot_path(project, content_hash)
        if p is None or not p.exists():
            raise HTTPException(404, "no baked snapshot for this entry")
        try:
            p.unlink()
        except OSError as exc:
            raise HTTPException(500, str(exc))
        # Defense-in-depth, not load-bearing for correctness: since ee0a90a,
        # cached_result_expr is a thin non-memoised wrapper that re-checks
        # path.exists() and self-heals on every read, so the next viewer read
        # (api_data) / ensure_session heal re-bakes the evicted snapshot even with a
        # warm memo left in place (proven by
        # test_cached_result_expr_self_heals_after_warm_then_evict). We still clear
        # it because it's cheap for an admin delete: content-addressed entries just
        # re-reconstruct (and the evicted one re-bakes) on the next read.
        cached_result_expr.cache_clear()
        return {"ok": True, "hash": content_hash}

    @app.patch("/{project}/api/notebook")
    async def patch_notebook(project: str, payload: dict):
        project = _validate_project(project)
        if read_only:
            raise HTTPException(403, "serve mode")
        action = payload.get("action")
        cell_id = payload.get("cell_id", "")
        try:
            if action == "reorder":
                notebook.reorder(project, cell_id, int(payload["new_index"]))
            elif action == "remove":
                notebook.remove(project, cell_id)
            else:
                raise HTTPException(400, f"unknown action {action!r}")
        except CellNotFound:
            raise HTTPException(404, f"cell {cell_id!r} not found")
        await publish({"kind": "notebook_changed"})
        return {"ok": True}

    @app.put("/{project}/api/code/{alias}")
    async def put_code(project: str, alias: str, payload: dict):
        project = _validate_project(project)
        if read_only:
            raise HTTPException(403, "serve mode")
        from tallyman_core import carry_forward_entry_config  # noqa: PLC0415
        from tallyman_core import get_alias as _get_alias  # noqa: PLC0415
        from tallyman_core import set_alias as _set_alias  # noqa: PLC0415
        from tallyman_xorq import build_and_persist as _build_and_persist  # noqa: PLC0415
        from tallyman_xorq.build import BuildError  # noqa: PLC0415

        prev_hash = _get_alias(project, alias)
        if prev_hash is None:
            raise HTTPException(404, f"alias {alias!r} not found")
        code = payload.get("code", "")
        if not code.strip():
            raise HTTPException(400, "code is empty")
        try:
            res = _build_and_persist(project, code, prompt=payload.get("prompt") or None)
        except BuildError as exc:
            from tallyman_core import record_error as _record_error  # noqa: PLC0415

            rec = _record_error(project, code=code, message=str(exc), tool="api_code")
            await publish({"kind": "build_failed", "error_id": rec["id"], "tool": "api_code"})
            raise HTTPException(400, str(exc))
        from tallyman_xorq.dependents import references_own_alias  # noqa: PLC0415

        if references_own_alias(project, res.content_hash, alias):
            from tallyman_core import record_error as _record_error  # noqa: PLC0415

            msg = (
                f"this revision references its own alias {alias!r}, which makes an opaque, "
                f"permanently-stale (follows-its-own-head) entry. Write a self-contained recipe: "
                f"inline the source (read_project_file / read_csv …) for a source-shaped entry, or "
                f"reference the previous version by hash with pinned_expr_from_alias({prev_hash!r}) "
                f"for an expensive one."
            )
            rec = _record_error(project, code=code, message=msg, tool="api_code")
            await publish({"kind": "build_failed", "error_id": rec["id"], "tool": "api_code"})
            raise HTTPException(400, msg)
        info = _set_alias(project, alias, res.content_hash, expect_exists=True)
        # Chart + display config are keyed by content hash; seed the new version
        # from the previous one so they follow the alias across revisions.
        carried = carry_forward_entry_config(project, prev_hash, res.content_hash)
        await publish({"kind": "new_entry", "hash": res.content_hash, "alias": alias, "version": info["version"]})
        reply = {
            "hash": res.content_hash,
            "alias": alias,
            "version": info["version"],
            "row_count": res.row_count,
        }
        if carried:
            reply["carried_over"] = carried
        # Surface the advisory nondeterminism lint, same as the MCP tool replies (#88).
        if res.lint_warnings:
            reply["lint_warnings"] = res.lint_warnings
        # Auto-recalc: cascade this in-browser revise to the alias's stale
        # followers in the SAME revision — the checkpoint middleware commits the
        # head advance + the cascade as one (the walk takes no checkpoint of its
        # own). Off → today's no-cascade behavior. Parity with catalog_revise.
        rep = await _auto_recalc_after_head_advance(project, alias, tool="companion_revise")
        if rep is not None:
            reply["recalc"] = rep
        return reply

    @app.put("/{project}/api/markdown/{cell_id}")
    async def put_markdown(project: str, cell_id: str, payload: dict):
        project = _validate_project(project)
        if read_only:
            raise HTTPException(403, "serve mode")
        markdown = payload.get("markdown", "")
        try:
            cell = notebook.edit_markdown(project, cell_id, markdown)
        except CellNotFound:
            raise HTTPException(404, f"cell {cell_id!r} not found")
        await publish({"kind": "notebook_changed"})
        return {**cell, "html": _render_markdown(cell["markdown"])}

    @app.post("/{project}/api/reset")
    async def api_reset(project: str, payload: dict):
        """Restore the project to a step number or label, then reload sessions.

        Exempt from the checkpoint middleware — a reset moves ``current``, it
        does not append a new step. (A timeline scrubber in the SPA calls this.)
        """
        project = _validate_project(project)
        if read_only:
            raise HTTPException(403, "serve mode")
        ref = (payload or {}).get("ref")
        if ref is None:
            raise HTTPException(400, "ref required: a step number or a label")
        if isinstance(ref, str) and ref.isdigit():
            ref = int(ref)
        from starlette.concurrency import run_in_threadpool  # noqa: PLC0415

        from tallyman_core.catalog_state import current_step, reset_to  # noqa: PLC0415

        try:
            await run_in_threadpool(reset_to, project, ref)
        except RuntimeError as exc:
            raise HTTPException(404, str(exc))
        # reset_to ran in-process and cleared the result-plan memo; the
        # compare-expr LRU is a companion-layer cache it can't reach, so invalidate
        # the companion caches here (#80). Shared with the cross-process notify path.
        _invalidate_reset_caches()
        reloaded = buckaroo.reload_project_sessions(project) if buckaroo else 0
        step = current_step(project)
        await publish({"kind": "project_reset", "step": step})
        return {"ok": True, "step": step, "reloaded": reloaded}

    @app.get("/{project}/api/staleness")
    def api_staleness(project: str):
        """Staleness scan for every entry — the scan-on-load reactive surface.

        Read-only (no checkpoint). Drives the per-entry staleness badge: ``stale``
        lists directly-stale hashes (their own input moved), ``transitively_stale``
        the clean descendants a recalc would carry, ``entries`` the full verdict
        map. The SPA fires this on project load / switch and after a recalc.
        """
        project = _validate_project(project)
        from tallyman_core.config import auto_recalc_enabled  # noqa: PLC0415
        from tallyman_xorq import staleness  # noqa: PLC0415
        from tallyman_xorq.recalc import classify_orphans  # noqa: PLC0415

        verdicts = staleness.scan(project)
        # D5 scan-on-load backstop, parity with the MCP catalog_scan_staleness
        # surface: classify the directly-stale set as orphans so the SPA can flag
        # unexplained staleness on load. Gated on the switch — manual mode owns its
        # own staleness, so a directly-stale follower is expected there, not a bug.
        orphan_stale = classify_orphans(project, verdicts) if auto_recalc_enabled(project) else []
        return {
            "project": project,
            "stale": sorted(h for h, v in verdicts.items() if v.stale),
            "transitively_stale": sorted(h for h, v in verdicts.items() if v.transitively_stale),
            "entries": {h: asdict(v) for h, v in verdicts.items()},
            "orphan_stale": orphan_stale,
        }

    @app.post("/{project}/api/recalc")
    async def api_recalc(project: str, payload: dict | None = None):
        """Recompute stale entries and their dependents in dependency order.

        Exempt from the checkpoint middleware — ``recalc`` takes its own single
        checkpoint for the whole walk (only on a real run that changed something),
        so the operation is one revision ``reset_to`` undoes atomically.
        ``dry_run`` (default True) previews the cone without building. A committed
        run invalidates the companion caches, reloads buckaroo sessions, and
        publishes a ``recalc`` event so open views refetch.
        """
        project = _validate_project(project)
        if read_only:
            raise HTTPException(403, "serve mode")
        payload = payload or {}
        dry_run = bool(payload.get("dry_run", True))
        roots = payload.get("roots")
        from starlette.concurrency import run_in_threadpool  # noqa: PLC0415

        from tallyman_xorq import staleness  # noqa: PLC0415
        from tallyman_xorq.recalc import recalc  # noqa: PLC0415

        if roots is None:
            verdicts = await run_in_threadpool(staleness.scan, project)
            roots = sorted(h for h, v in verdicts.items() if v.stale)
        if not roots:
            return {"status": "ok", "roots": [], "cone": [], "entries": [], "dry_run": dry_run, "remap": {}}
        report = await run_in_threadpool(recalc, project, roots, dry_run=dry_run)
        if not dry_run and report.remap:
            _invalidate_reset_caches()
            if buckaroo:
                buckaroo.reload_project_sessions(project)
            await publish(_recalc_sse_event(report.remap, report.checkpoint_step))
        return asdict(report)

    # ------------------------------------------------------------------
    # Project-agnostic routes (no project prefix)
    # ------------------------------------------------------------------

    @app.post("/internal/notify")
    async def notify(payload: NotifyPayload):
        # The payload may name its project (CLI `reset-to --project` can target
        # a non-active project); only fall back to the active one when it doesn't.
        project_name = payload.project or _require_project()
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        log.info(
            "notify kind=%s hash=%s",
            payload.kind,
            getattr(payload, "hash", None),
        )
        if payload.kind in (
            "post_processing_changed",
            "summary_stat_changed",
            "display_changed",
            "project_reset",
            "recalc",
        ):
            if payload.kind in ("project_reset", "recalc"):
                # project_reset: out-of-process CLI `reset-to` ran reset_to in the
                # CLI process, so this long-lived companion's result/compare LRUs
                # are still warm and may point at snapshots the prune deleted.
                # recalc: an MCP-driven catalog_recalc (arriving here via _notify,
                # not the in-process /api/recalc route) re-pointed aliases and
                # rebuilt entries. Either way, clear the LRUs and reload sessions —
                # the same cleanup /api/recalc and /api/reset do — so the MCP path
                # doesn't leave the viewer serving stale state (#80's communication
                # gap). Not gated on `buckaroo`: the LRUs are process memory
                # independent of the buckaroo subprocess.
                _invalidate_reset_caches()
            if buckaroo:
                reloaded = buckaroo.reload_project_sessions(project_name)
                log.info("reloaded klasses in %d buckaroo session(s) for project %s", reloaded, project_name)
        # Republish a recalc in the canonical {kind, remap, step} shape so the
        # cross-process notify path emits the identical event /api/recalc does;
        # other kinds pass through their raw payload unchanged.
        if payload.kind == "recalc":
            extra = payload.extra or {}
            event = _recalc_sse_event(extra.get("remap", {}), extra.get("step"))
        else:
            event = payload.model_dump()
        await publish(event)
        return {"ok": True, "subscribers": len(subscribers), "project": project_name}

    @app.get("/api/projects")
    def api_projects():
        from tallyman_core.paths import list_projects as _list_projects  # noqa: PLC0415

        return {
            "active": _current_project(),
            "available": _list_projects(),
        }

    @app.post("/api/projects/switch")
    async def api_projects_switch(payload: dict):
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        name = (payload or {}).get("name", "")
        try:
            from tallyman_core.paths import set_active_project as _sap  # noqa: PLC0415
            from tallyman_core.paths import validate_project_name as _vpn  # noqa: PLC0415

            _vpn(name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        previous = _current_project()
        try:
            _sap(name)
        except FileNotFoundError:
            raise HTTPException(404, f"project {name!r} not found")
        await publish({"kind": "project_switched", "name": name})
        return {"previous": previous, "active": name}

    @app.post("/api/projects/new")
    async def api_projects_new(payload: dict):
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        from tallyman_core import ensure_project as _ensure_project  # noqa: PLC0415
        from tallyman_core.paths import projects_root as _projects_root  # noqa: PLC0415
        from tallyman_core.paths import set_active_project as _sap  # noqa: PLC0415

        name = (payload or {}).get("name", "")
        with_fixture = bool((payload or {}).get("with_fixture", False))
        try:
            from tallyman_core.paths import validate_project_name as _vpn  # noqa: PLC0415

            _vpn(name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        if (_projects_root() / name).exists():
            raise HTTPException(409, f"project {name!r} already exists")
        _ensure_project(name)
        if with_fixture:
            from tallyman_cli.fixtures import write_shoe_orders  # noqa: PLC0415
            from tallyman_core import data_dir as _data_dir  # noqa: PLC0415

            write_shoe_orders(_data_dir(name) / "orders.parquet")
        # Record the step-000 genesis baseline so reset-to always has the
        # true empty start to target (plans/reset-to-revision-v2.md W7).
        from starlette.concurrency import run_in_threadpool  # noqa: PLC0415

        from tallyman_core.catalog_state import genesis as _genesis  # noqa: PLC0415

        await run_in_threadpool(_genesis, name)
        _sap(name)
        await publish({"kind": "project_switched", "name": name})
        return {"name": name, "active": name}

    @app.post("/api/projects/delete")
    async def api_projects_delete(payload: dict):
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        from tallyman_core.paths import delete_project as _delete_project  # noqa: PLC0415
        from tallyman_core.paths import list_projects as _list_projects  # noqa: PLC0415
        from tallyman_core.paths import set_active_project as _sap  # noqa: PLC0415

        names = (payload or {}).get("names", [])
        if not isinstance(names, list) or not names:
            raise HTTPException(400, "names must be a non-empty list")
        deleted, errors = [], []
        for name in names:
            try:
                _delete_project(name)
                deleted.append(name)
            except (FileNotFoundError, ValueError) as exc:
                errors.append({"name": name, "error": str(exc)})
        remaining = _list_projects()
        active = _current_project()
        # If the active project was deleted, promote a survivor (or clear).
        if active not in remaining:
            active = remaining[0] if remaining else None
            if active:
                _sap(active)
        await publish({"kind": "project_switched", "name": active or ""})
        return {"deleted": deleted, "errors": errors, "active": active, "remaining": remaining}

    # ------------------------------------------------------------------
    # SPA catch-all: serve React index.html for all unmatched GET routes.
    # Must be registered last so API routes above take priority.
    # ------------------------------------------------------------------

    @app.get("/{full_path:path}", response_class=HTMLResponse, include_in_schema=False)
    def spa_catch_all(full_path: str = ""):
        # An unmatched path under the API namespace must 404 as JSON rather than
        # falling through to index.html — otherwise a mistyped or removed
        # endpoint returns 200 HTML and the client's response.json() throws an
        # opaque parse error instead of surfacing the 404. Key on segment
        # position (top-level "/api/…" or project-prefixed "/{project}/api/…"),
        # not a substring, so a content segment named "api" (e.g. an alias in
        # "/{project}/diff/api/1/2") still routes to the SPA.
        parts = full_path.split("/")
        if parts[0] == "api" or (len(parts) >= 2 and parts[1] == "api"):
            raise HTTPException(404, "not found")
        return _serve_spa()

    return app
