from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path

import markdown as md_lib
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from pydata_companion.buckaroo_lifecycle import BuckarooManager
from pydata_companion.diff_color_maps import DIVERGING_BLUE_WHITE_RED
from pydata_core import (
    alias_for_hash,
    clear_errors,
    entry_dir,
    get_chart,
    get_error,
    history_for,
    list_errors,
    load_aliases,
    notebook,
    resolve_project,
    version_of_hash,
)
from pydata_core.notebook import CellNotFound
from pydata_core.paths import project_dir, validate_project_name
from pydata_xorq import (
    catalog_dag,
    column_lineage,
    full_diff,
    list_entries,
    read_internal_lineage,
    read_prompts,
)
from pydata_xorq.layout import layered_positions

log = logging.getLogger("pydata.companion")

PKG_DIR = Path(__file__).resolve().parent
STATIC_DIR = PKG_DIR / "static"

# React SPA dist relative to the repo root (parents[2] == repo root from
# src/pydata_companion/app.py). Works for dev; when installed as a package
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


def _read_head_rows(path: Path, n: int, pf: pq.ParquetFile | None = None) -> pa.Table:
    """Read at most the first ``n`` rows of a parquet file as an arrow Table.

    Uses ``iter_batches`` to stop as soon as enough rows have been collected,
    so a 200-row read against a multi-million-row parquet doesn't materialise
    the whole file. Returns the concatenated batches sliced to exactly ``n``.
    """
    if pf is None:
        pf = pq.ParquetFile(path)
    if n <= 0:
        return pf.schema_arrow.empty_table()
    collected: list = []
    rows_so_far = 0
    for batch in pf.iter_batches(batch_size=min(n, 10_000)):
        collected.append(batch)
        rows_so_far += batch.num_rows
        if rows_so_far >= n:
            break
    if not collected:
        return pf.schema_arrow.empty_table()
    return pa.Table.from_batches(collected).slice(0, n)


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


# A catalog entry's content hash is the lowercase-hex name of its entry dir
# (xorq's build_path.name; same charset lineage.catalog_parents matches). Used
# to reject path-param junk before it can name anything under the entries tree.
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

    Covers raw input data, each entry's result.parquet / xorq_build /
    .buckaroo_stat_cache, and the two project-level caches added in #12 — the
    xorq ParquetSnapshotCache (result_cache/) and the per-pair Buckaroo diff
    stat cache (diff_stat_cache/), either of which can dwarf the rest.

    This is the expensive path; callers should rate-limit it via the
    api_disk_usage TTL cache rather than invoking per request.
    """
    from pydata_core.paths import data_dir as _data_dir
    from pydata_core.paths import diff_stat_cache_root as _diff_stat_cache_root
    from pydata_core.paths import entries_dir as _entries_dir
    from pydata_core.paths import result_cache_dir as _result_cache_dir

    # raw input files
    data = _dir_size(_data_dir(project))

    root = _entries_dir(project)
    results = builds = cache = 0
    if root.is_dir():
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            p = entry / "result.parquet"
            if p.exists():
                try:
                    results += p.stat().st_size
                except OSError:
                    pass
            b = entry / "xorq_build"
            if b.is_dir():
                builds += _dir_size(b)
            c = entry / ".buckaroo_stat_cache"
            if c.is_dir():
                cache += _dir_size(c)

    # Project-level caches (#12), previously uncounted. _dir_size returns 0 for
    # a dir that doesn't exist yet, so no existence guard needed.
    result_cache = _dir_size(_result_cache_dir(project))
    diff_cache = _dir_size(_diff_stat_cache_root(project))

    total = data + results + builds + cache + result_cache + diff_cache
    return {
        "data": data,
        "results": results,
        "builds": builds,
        "cache": cache,
        "result_cache": result_cache,
        "diff_cache": diff_cache,
        "total": total,
        "formatted": {
            "data": _fmt_bytes(data),
            "results": _fmt_bytes(results),
            "builds": _fmt_bytes(builds),
            "cache": _fmt_bytes(cache),
            "result_cache": _fmt_bytes(result_cache),
            "diff_cache": _fmt_bytes(diff_cache),
            "total": _fmt_bytes(total),
        },
    }


def _build_compare_expr(a, b, keys: list[str]) -> tuple[Path, dict]:
    """Build an xorq outer-join comparison expression for two entry exprs.

    ``a`` / ``b`` are the two entries' (cache-wrapped) xorq expressions, so the
    comparison composes ``expr1`` ⋈ ``expr2`` and each side resolves its own
    cache when Buckaroo streams rows — nothing depends on a result.parquet.

    Returns (build_path, column_config_overrides).  Column naming mirrors
    col_join_dfs: non-key b-columns get a ``_v2`` suffix; ``membership``
    (1=a_only, 2=b_only, 3=both) and per-column ``{col}_eq`` columns drive the
    Buckaroo color mapping.
    """
    import tempfile

    import xorq.api as xo
    import xorq.vendor.ibis as ibis

    a_schema = a.schema()
    b_schema = b.schema()
    a_non_keys = [c for c in a_schema if c not in keys]
    b_non_keys = [c for c in b_schema if c not in keys]
    shared_non_keys = [c for c in a_non_keys if c in b_non_keys]

    # Land both on one backend so the outer join (and the build Buckaroo
    # streams from) is single-engine — a no-op when they already share one
    # (e.g. both reading their result.parquet on the default xorq backend),
    # which avoids an eager cross-backend transport.
    from buckaroo.compare import _align_backends

    a, b = _align_backends(a, b)

    # Rename b's non-key columns to avoid ibis collision during outer join.
    # ibis rename convention: {new_name: old_name}
    b_renamed = b.rename({f"{c}_v2": c for c in b_non_keys})
    joined = a.outer_join(b_renamed, [a[k] == b_renamed[k] for k in keys])

    # Coalesced key columns + all data columns.
    sel: list = [ibis.coalesce(a[k], b_renamed[k]).name(k) for k in keys]
    for col in a_non_keys:
        sel.append(joined[col])
        if col in b_non_keys:
            sel.append(joined[f"{col}_v2"])

    # Membership sentinel: null on the b side → a_only; null on the a side → b_only.
    a_sent = a_non_keys[0] if a_non_keys else keys[0]
    b_sent = f"{b_non_keys[0]}_v2" if b_non_keys else keys[0]
    membership = ibis.cases(
        (joined[b_sent].isnull(), ibis.literal(1).cast("int8")),
        (joined[a_sent].isnull(), ibis.literal(2).cast("int8")),
        else_=ibis.literal(3).cast("int8"),
    ).name("membership")
    sel.append(membership)

    # Per-column equality: membership + 4 if equal, membership + 0 if not.
    for col in shared_non_keys:
        eq = (
            membership
            + ibis.cases(
                (joined[col] == joined[f"{col}_v2"], ibis.literal(4).cast("int8")),
                else_=ibis.literal(0).cast("int8"),
            )
        ).name(f"{col}_eq")
        sel.append(eq)

    # Percentage and absolute change for numeric shared columns.
    # Null when only one side has a row (membership != 3); pct_delta also null when old=0.
    numeric_shared = {c for c, dtype in a_schema.items() if c in shared_non_keys and dtype.is_numeric()}
    for col in numeric_shared:
        a_val = joined[col].cast("float64")        # old / before
        b_val = joined[f"{col}_v2"].cast("float64")  # new / after
        pct = ibis.cases(
            (a_val == ibis.literal(0.0), ibis.null().cast("float64")),
            else_=(b_val - a_val) / a_val.abs(),
        ).name(f"{col}_pct_delta")
        abs_delta = (b_val - a_val).name(f"{col}_abs_delta")
        sel.append(pct)
        sel.append(abs_delta)

    expr = joined.select(*sel)

    # One session-scoped parent dir, not a fresh mkdtemp per page view: the
    # comparison expr is deterministic, so build_expr lands it in a stable
    # content-hash subdir here. Re-rendering the same diff reuses that subdir
    # instead of leaking a new /tmp dir on every load. (Disk reclamation of
    # this tree is handled separately.)
    builds_dir = Path(tempfile.gettempdir()) / "pydata_diff_builds"
    builds_dir.mkdir(parents=True, exist_ok=True)
    build_path = Path(xo.build_expr(expr, builds_dir=str(builds_dir)))

    eq_map = ["pink", "#73ae80", "#90b2b3", "#6c83b5"]
    pk_color = "#6c5fc7"
    pk_map = [pk_color] * 4
    overrides: dict = {"membership": {"merge_rule": "hidden"}}
    for col in a_non_keys:
        if col in b_non_keys:
            # Show the new (b) value; hover reveals the old (a) value.
            overrides[col] = {"merge_rule": "hidden"}
            overrides[f"{col}_eq"] = {"merge_rule": "hidden"}
            if col in numeric_shared:
                overrides[f"{col}_v2"] = {
                    "header_name": col,
                    "tooltip_config": {"tooltip_type": "simple", "val_column": col},
                    "color_map_config": {
                        "color_rule": "color_map",
                        "map_name": DIVERGING_BLUE_WHITE_RED,
                        "val_column": f"{col}_pct_delta",
                    },
                }
            else:
                overrides[f"{col}_v2"] = {
                    "header_name": col,
                    "tooltip_config": {"tooltip_type": "simple", "val_column": col},
                    "color_map_config": {
                        "color_rule": "color_categorical",
                        "map_name": eq_map,
                        "val_column": f"{col}_eq",
                    },
                }
    for k in keys:
        overrides[k] = {
            "color_map_config": {
                "color_rule": "color_categorical",
                "map_name": pk_map,
                "val_column": "membership",
            }
        }

    return build_path, overrides


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
            from pydata_core.paths import set_active_project as _sap

            _sap(project)
        except (FileNotFoundError, ValueError):
            pass

    app = FastAPI(title="pydata companion")
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    # Serve React SPA assets (built by `cd packages/app && pnpm build`).
    if (_REACT_DIST / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=str(_REACT_DIST / "assets")), name="spa-assets")

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
        if path.endswith("/api/reset"):
            return True  # reset moves `current`; it is not a new step
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

        from pydata_core.catalog_state import checkpoint_catalog  # noqa: PLC0415

        try:
            await run_in_threadpool(checkpoint_catalog, name, f"pydata: {request.method} {path}")
        except Exception as exc:
            log.warning("checkpoint after %s %s failed: %s", request.method, path, exc)
        return response

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

        from pydata_core.marimo_export import notebook_to_marimo  # noqa: PLC0415

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
        entry = entry_dir(project, content_hash)
        result_path = entry / "result.parquet"
        if not result_path.exists():
            raise HTTPException(404, "no result.parquet")
        pf = pq.ParquetFile(result_path)
        total = pf.metadata.num_rows
        needed = offset + limit if limit > 0 else 0
        if needed > 0 and total > 0:
            table = _read_head_rows(result_path, min(needed, total), pf=pf)
            df = table.slice(offset, limit).to_pandas()
        else:
            df = pf.schema_arrow.empty_table().to_pandas()
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

        manifest = json.loads((entry / "manifest.json").read_text())
        schema = json.loads((entry / "schema.json").read_text())
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

        prompt_history = read_prompts(entry)
        chart_spec = get_chart(project, content_hash)

        build_artifacts: list[dict] = []
        build_dir = entry / "xorq_build"
        if build_dir.is_dir():
            for f in sorted(build_dir.iterdir()):
                if not f.is_file():
                    continue
                try:
                    text = f.read_text()
                except UnicodeDecodeError:
                    text = f"(binary, {f.stat().st_size} bytes)"
                build_artifacts.append({"name": f.name, "text": text})

        buckaroo_session = buckaroo.ensure_session(content_hash, project) if buckaroo else None
        buckaroo_ws_base = buckaroo.ws_base_url if buckaroo and buckaroo.is_running else None

        result_path = entry / "result.parquet"
        total_rows = pq.ParquetFile(result_path).metadata.num_rows if result_path.exists() else 0

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
            "build_artifacts": build_artifacts,
            "total_rows": total_rows,
            "buckaroo_session": buckaroo_session,
            "buckaroo_ws_base": buckaroo_ws_base,
        }

    @app.get("/{project}/api/lineage/{content_hash}")
    def api_lineage(project: str, content_hash: str):
        project = _validate_project(project)
        _require_hash(content_hash)
        if not entry_dir(project, content_hash).exists():
            raise HTTPException(404)
        return {
            "internal": read_internal_lineage(project, content_hash),
        }

    @app.get("/{project}/api/catalog_dag")
    def api_catalog_dag(project: str):
        project = _validate_project(project)
        return {"project": project, **catalog_dag(project)}

    @app.get("/{project}/api/catalog_dag_layout")
    def api_catalog_dag_layout(project: str):
        """Catalog DAG with pre-computed layout positions for the React SPA."""
        project = _validate_project(project)
        dag = catalog_dag(project)
        nodes_for_layout = [{"id": n["hash"], **n} for n in dag["nodes"]]
        targets = {e["to"] for e in dag["edges"]}
        roots = [n["id"] for n in nodes_for_layout if n["id"] not in targets]
        root = roots[0] if roots else (nodes_for_layout[0]["id"] if nodes_for_layout else None)
        layout = layered_positions(nodes_for_layout, dag["edges"], root=root, x_step=200, y_step=80)
        aliases = load_aliases(project)
        by_latest = {h: a for a, h in aliases.items()}
        annotated: dict[str, dict] = {}
        for n in dag["nodes"]:
            h = n["hash"]
            info = version_of_hash(project, h)
            annotated[h] = {
                "alias": by_latest.get(h) or (info[0] if info else None),
                "version": info[1] if info else None,
            }
        return {
            "project": project,
            "nodes": dag["nodes"],
            "edges": dag["edges"],
            "positions": layout["positions"],
            "width": layout["width"],
            "height": layout["height"],
            "annotated": annotated,
        }

    @app.get("/{project}/api/lineage_layout/{content_hash}")
    def api_lineage_layout(project: str, content_hash: str):
        """Internal expression lineage with layout positions for the React SPA."""
        project = _validate_project(project)
        aliases = load_aliases(project)
        if content_hash in aliases:
            content_hash = aliases[content_hash]
        else:
            _require_hash(content_hash)
        if not entry_dir(project, content_hash).exists():
            raise HTTPException(404)
        lin = read_internal_lineage(project, content_hash)
        layout = layered_positions(lin["nodes"], lin["edges"], root=lin["root"], x_step=180, y_step=70)
        cols = column_lineage(project, content_hash)
        return {
            "project": project,
            "content_hash": content_hash,
            "lineage": lin,
            "positions": layout["positions"],
            "width": layout["width"],
            "height": layout["height"],
            "column_trees": cols,
        }

    @app.get("/{project}/api/session/{content_hash}")
    def get_session(project: str, content_hash: str):
        project = _validate_project(project)
        if buckaroo is None or not buckaroo.is_running:
            return {"ws_url": None}
        session_id = buckaroo.ensure_session(content_hash, project)
        if session_id is None:
            return {"ws_url": None}
        return {"ws_url": f"{buckaroo.ws_base_url}/ws/{session_id}"}

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
                if (entry / "manifest.json").exists():
                    entry_meta = json.loads((entry / "manifest.json").read_text())
                    schema = json.loads((entry / "schema.json").read_text())
                result_path = entry / "result.parquet"
                if result_path.exists():
                    total_rows = pq.ParquetFile(result_path).metadata.num_rows
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

        from pydata_xorq.primary_key import diff_keys  # noqa: PLC0415
        from pydata_xorq.result_cache import cached_result_expr  # noqa: PLC0415

        a_expr = cached_result_expr(project, a_hash)
        b_expr = cached_result_expr(project, b_hash)
        keys = diff_keys(project, a_hash, b_hash) or None
        diff = full_diff(
            a_dir,
            b_dir,
            a_label=f"V{va}",
            b_label=f"V{vb}",
            backend="xorq",
            a_expr=a_expr,
            b_expr=b_expr,
            keys=keys,
        )

        compare_session = None
        buckaroo_ws_base_url = None
        if buckaroo is not None and buckaroo.is_running:
            if not keys:
                keys = diff["keyed"]["keys"] if diff.get("keyed") else None
            if keys:
                session_id = f"diff-{a_hash[:12]}-{b_hash[:12]}"
                try:
                    build_path, overrides = _build_compare_expr(a_expr, b_expr, keys)
                    from pydata_core.paths import diff_stat_cache_dir  # noqa: PLC0415

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

        from pydata_core.paths import entries_dir as _entries_dir  # noqa: PLC0415

        root = _entries_dir(project)
        entries = []
        if root.is_dir():
            for entry in root.iterdir():
                if not entry.is_dir():
                    continue
                p = entry / "result.parquet"
                if not p.exists():
                    continue
                try:
                    stat = p.stat()
                    size = stat.st_size
                    created = datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                except OSError:
                    continue
                content_hash = entry.name
                try:
                    row_count = pq.ParquetFile(p).metadata.num_rows
                except Exception:
                    row_count = 0
                alias = alias_for_hash(project, content_hash)
                info = version_of_hash(project, content_hash)
                version = info[1] if info else None
                is_current = alias is not None
                if not is_current and info is not None:
                    alias = info[0]
                    is_current = False
                manifest_path = entry / "manifest.json"
                prompt = None
                if manifest_path.exists():
                    try:
                        m = json.loads(manifest_path.read_text())
                        prompt = m.get("prompt")
                    except Exception:
                        pass
                entries.append(
                    {
                        "hash": content_hash,
                        "size": size,
                        "size_formatted": _fmt_bytes(size),
                        "row_count": row_count,
                        "created": created,
                        "alias": alias,
                        "version": version,
                        "is_current": is_current,
                        "prompt": prompt,
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
        p = entry_dir(project, content_hash) / "result.parquet"
        if not p.exists():
            raise HTTPException(404, "result.parquet not found")
        try:
            p.unlink()
        except OSError as exc:
            raise HTTPException(500, str(exc))
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
        from pydata_core import get_alias as _get_alias  # noqa: PLC0415
        from pydata_core import set_alias as _set_alias  # noqa: PLC0415
        from pydata_xorq import build_and_persist as _build_and_persist  # noqa: PLC0415
        from pydata_xorq.build import BuildError  # noqa: PLC0415

        if _get_alias(project, alias) is None:
            raise HTTPException(404, f"alias {alias!r} not found")
        code = payload.get("code", "")
        if not code.strip():
            raise HTTPException(400, "code is empty")
        try:
            res = _build_and_persist(project, code, prompt=payload.get("prompt") or None)
        except BuildError as exc:
            from pydata_core import record_error as _record_error  # noqa: PLC0415

            rec = _record_error(project, code=code, message=str(exc), tool="api_code")
            await publish({"kind": "build_failed", "error_id": rec["id"], "tool": "api_code"})
            raise HTTPException(400, str(exc))
        info = _set_alias(project, alias, res.content_hash, expect_exists=True)
        await publish({"kind": "new_entry", "hash": res.content_hash, "alias": alias, "version": info["version"]})
        return {
            "hash": res.content_hash,
            "alias": alias,
            "version": info["version"],
            "row_count": res.row_count,
        }

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

        from pydata_core.catalog_state import current_step, reset_to  # noqa: PLC0415

        try:
            await run_in_threadpool(reset_to, project, ref)
        except RuntimeError as exc:
            raise HTTPException(404, str(exc))
        reloaded = buckaroo.reload_project_sessions(project) if buckaroo else 0
        step = current_step(project)
        await publish({"kind": "project_reset", "step": step})
        return {"ok": True, "step": step, "reloaded": reloaded}

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
        if payload.kind in ("post_processing_changed", "summary_stat_changed", "project_reset"):
            if buckaroo:
                reloaded = buckaroo.reload_project_sessions(project_name)
                log.info("reloaded klasses in %d buckaroo session(s) for project %s", reloaded, project_name)
        await publish(payload.model_dump())
        return {"ok": True, "subscribers": len(subscribers), "project": project_name}

    @app.get("/api/projects")
    def api_projects():
        from pydata_core.paths import list_projects as _list_projects  # noqa: PLC0415

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
            from pydata_core.paths import set_active_project as _sap  # noqa: PLC0415
            from pydata_core.paths import validate_project_name as _vpn  # noqa: PLC0415

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
        from pydata_core import ensure_project as _ensure_project  # noqa: PLC0415
        from pydata_core.paths import projects_root as _projects_root  # noqa: PLC0415
        from pydata_core.paths import set_active_project as _sap  # noqa: PLC0415

        name = (payload or {}).get("name", "")
        with_fixture = bool((payload or {}).get("with_fixture", False))
        try:
            from pydata_core.paths import validate_project_name as _vpn  # noqa: PLC0415

            _vpn(name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        if (_projects_root() / name).exists():
            raise HTTPException(409, f"project {name!r} already exists")
        _ensure_project(name)
        if with_fixture:
            from pydata_cli.fixtures import write_shoe_orders  # noqa: PLC0415
            from pydata_core import data_dir as _data_dir  # noqa: PLC0415

            write_shoe_orders(_data_dir(name) / "orders.parquet")
        # Record the step-000 genesis baseline so reset-to always has the
        # true empty start to target (plans/reset-to-revision-v2.md W7).
        from starlette.concurrency import run_in_threadpool  # noqa: PLC0415

        from pydata_core.catalog_state import genesis as _genesis  # noqa: PLC0415

        await run_in_threadpool(_genesis, name)
        _sap(name)
        await publish({"kind": "project_switched", "name": name})
        return {"name": name, "active": name}

    @app.post("/api/projects/delete")
    async def api_projects_delete(payload: dict):
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        from pydata_core.paths import delete_project as _delete_project  # noqa: PLC0415
        from pydata_core.paths import list_projects as _list_projects  # noqa: PLC0415
        from pydata_core.paths import set_active_project as _sap  # noqa: PLC0415

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
