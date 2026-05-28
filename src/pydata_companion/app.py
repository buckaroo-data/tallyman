from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import markdown as md_lib
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from pydata_companion.buckaroo_lifecycle import BuckarooManager
from pydata_core import (
    alias_for_hash,
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
TEMPLATES_DIR = PKG_DIR / "templates"
STATIC_DIR = PKG_DIR / "static"


class NotifyPayload(BaseModel):
    kind: str
    hash: str | None = None
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
        else:
            info = version_of_hash(project, h)
            if info is not None:
                e["alias"] = info[0]
                e["version"] = info[1]
                e["is_current"] = False
            else:
                e["alias"] = None
                e["version"] = None
                e["is_current"] = False
    # Sort: current versions of named entries first (by alias), then forensic
    # versions, then scratch, each by mtime-desc within their bucket.
    def _bucket(e):
        if e["is_current"]:
            return (0, e["alias"])
        if e["alias"]:
            return (1, e["alias"], -e.get("version", 0))
        return (2,)

    return sorted(entries, key=_bucket)


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
            # Project doesn't exist or name is invalid — let the empty-state
            # landing handle it.
            pass

    app = FastAPI(title="pydata companion")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals["read_only"] = read_only
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    def _current_project() -> str | None:
        return resolve_project() or seed

    def _require_project() -> str:
        p = _current_project()
        if p is None:
            # Caller is inside a route that needs a project; signal the
            # empty-state landing.
            raise HTTPException(503, "No active project — visit / to create one")
        return p

    # In-process fan-out for SSE clients. Each client gets its own queue.
    subscribers: list[asyncio.Queue] = []

    async def publish(event: dict):
        # Every event is tagged with the project it happened in so the
        # client (base.html) can ignore cross-project events without
        # touching the channel routing.
        event.setdefault("project", _current_project())
        for q in list(subscribers):
            await q.put(event)

    @app.get("/", include_in_schema=False)
    def root(request: Request):
        # No project on disk yet → show the empty-state landing page.
        if _current_project() is None:
            from pydata_core.paths import list_projects

            return templates.TemplateResponse(
                request,
                "empty_state.html",
                {"available_projects": list_projects()},
            )
        return RedirectResponse("/catalog")

    @app.get("/catalog", response_class=HTMLResponse)
    def catalog(request: Request):
        project_name = _require_project()
        entries = _annotate_entries(project_name, list_entries(project_name))
        errors = list_errors(project_name, limit=5)
        return templates.TemplateResponse(
            request,
            "catalog.html",
            {"project": project_name, "entries": entries, "errors": errors},
        )

    @app.get("/catalog/{content_hash}", response_class=HTMLResponse)
    def catalog_entry(request: Request, content_hash: str):
        project_name = _require_project()
        # Allow alias lookup too: /catalog/<alias> resolves to latest hash.
        alias = None
        version = None
        forensic_history: list[dict] = []

        aliases = load_aliases(project_name)
        if content_hash in aliases:
            alias = content_hash
            content_hash = aliases[alias]

        entry = entry_dir(project_name, content_hash)
        if not entry.exists():
            raise HTTPException(404, f"entry {content_hash} not found")
        manifest = json.loads((entry / "manifest.json").read_text())
        schema = json.loads((entry / "schema.json").read_text())
        code = (entry / "expr.py").read_text()

        if alias is None:
            alias = alias_for_hash(project_name, content_hash)
        info = version_of_hash(project_name, content_hash)
        if info is not None:
            alias = alias or info[0]
            version = info[1]
        if alias is not None:
            hist_hashes = history_for(project_name, alias)
            for i, h in enumerate(hist_hashes, start=1):
                forensic_history.append({"hash": h, "version": i, "is_current": h == content_hash})

        prompt_history = read_prompts(entry)
        chart_spec = get_chart(project_name, content_hash)
        sidebar_entries = _annotate_entries(project_name, list_entries(project_name))
        build_artifacts = []
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
        buckaroo_session = (
            buckaroo.ensure_session(content_hash, project_name) if buckaroo is not None else None
        )
        buckaroo_ws_base = (
            buckaroo.ws_base_url if buckaroo and buckaroo.is_running else None
        )

        # Only read parquet when Buckaroo isn't there to render — its WS path
        # streams rows on demand from the xorq expression, so for buckaroo-up
        # sessions we want zero parquet materialisation in the companion.
        # Citibike-scale entries (millions of rows) blow up to many GB if we
        # to_pandas() them unconditionally.
        table_html = ""
        preview_rows = 0
        result_path = entry / "result.parquet"
        if result_path.exists():
            total_rows = pq.ParquetFile(result_path).metadata.num_rows
            if buckaroo_session is None:
                # Stream just enough row groups to fill the fallback preview.
                preview_rows = min(200, total_rows)
                head_table = _read_head_rows(result_path, preview_rows)
                table_html = head_table.to_pandas().to_html(
                    classes="data-table", index=False, border=0
                )
        else:
            total_rows = 0
        return templates.TemplateResponse(
            request,
            "entry_detail.html",
            {
                "project": project_name,
                "manifest": manifest,
                "schema": schema,
                "code": code,
                "table_html": table_html,
                "preview_rows": preview_rows,
                "total_rows": total_rows,
                "prompt_history": prompt_history,
                "alias": alias,
                "version": version,
                "forensic_history": forensic_history,
                "chart_spec": chart_spec,
                "buckaroo_session": buckaroo_session,
                "buckaroo_ws_base": buckaroo_ws_base,
                "build_artifacts": build_artifacts,
                "entries": sidebar_entries,
                "current_hash": content_hash,
            },
        )

    @app.get("/api/data/{content_hash}")
    def api_data(content_hash: str, offset: int = 0, limit: int = 200):
        """Return a paginated slice of `result.parquet` as JSON.

        Defaults (offset=0, limit=200) keep payloads small for vega-embed
        on typical aggregate results. Set `limit` higher when feeding a
        chart that needs more rows; the response always reports `total` and
        `offset` so the caller can decide whether to fetch more.

        Streams row groups via ``iter_batches`` rather than reading the whole
        file — vega-embed on a citibike-scale result asking for 200 rows
        shouldn't pull millions of rows into Python.
        """
        project_name = _require_project()
        if limit < 0:
            raise HTTPException(400, "limit must be >= 0")
        entry = entry_dir(project_name, content_hash)
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

    @app.get("/api/sse")
    async def sse(request: Request):
        project_name = _require_project()
        queue: asyncio.Queue = asyncio.Queue()
        subscribers.append(queue)

        async def event_stream():
            try:
                # Hello frame so the browser knows the connection is live.
                yield {"event": "hello", "data": json.dumps({"project": project_name})}
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
                        # Heartbeat — keeps proxies from killing the connection.
                        yield {"event": "ping", "data": "{}"}
            finally:
                if queue in subscribers:
                    subscribers.remove(queue)

        return EventSourceResponse(event_stream())

    @app.post("/internal/notify")
    async def notify(payload: NotifyPayload):
        project_name = _require_project()
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        # T-34: log the kind so "where did all these /internal/notify hits come
        # from" investigations are a single grep rather than guesswork.
        log.info(
            "notify kind=%s hash=%s",
            payload.kind,
            getattr(payload, "hash", None),
        )
        await publish(payload.model_dump())
        return {"ok": True, "subscribers": len(subscribers), "project": project_name}

    # ------------------------------------------------------------------
    # Project lifecycle routes (used by the dropdown + MCP write-tools)
    # ------------------------------------------------------------------

    @app.get("/api/projects")
    def api_projects():
        from pydata_core.paths import list_projects as _list_projects

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
            from pydata_core.paths import set_active_project as _sap
            from pydata_core.paths import validate_project_name as _vpn

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
        from pydata_core import ensure_project as _ensure_project
        from pydata_core.paths import projects_root as _projects_root
        from pydata_core.paths import set_active_project as _sap

        name = (payload or {}).get("name", "")
        with_fixture = bool((payload or {}).get("with_fixture", False))
        try:
            from pydata_core.paths import validate_project_name as _vpn

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
        _sap(name)
        await publish({"kind": "project_switched", "name": name})
        return {"name": name, "active": name}

    @app.get("/api/entries")
    def api_entries():
        project_name = _require_project()
        return {
            "project": project_name,
            "entries": _annotate_entries(project_name, list_entries(project_name)),
        }

    @app.get("/lineage", response_class=HTMLResponse)
    def lineage_overview(request: Request):
        project_name = _require_project()
        dag = catalog_dag(project_name)
        node_id = lambda n: n["hash"]  # noqa: E731
        nodes_for_layout = [{"id": node_id(n), **n} for n in dag["nodes"]]
        # Heuristic root: any node with no incoming edges.
        targets = {e["to"] for e in dag["edges"]}
        roots = [n["id"] for n in nodes_for_layout if n["id"] not in targets]
        root = roots[0] if roots else (nodes_for_layout[0]["id"] if nodes_for_layout else None)
        # Catalog DAG: edges point from parent (data source) to child, so the
        # natural left-to-right layered layout fits without inversion.
        layout = layered_positions(
            nodes_for_layout, dag["edges"], root=root, x_step=200, y_step=80
        )
        # Annotate nodes with their alias if any (for nicer labels in the SVG).
        aliases = load_aliases(project_name)
        by_latest = {h: a for a, h in aliases.items()}
        annotated_by_hash: dict[str, dict] = {}
        for n in dag["nodes"]:
            h = n["hash"]
            info = version_of_hash(project_name, h)
            annotated_by_hash[h] = {
                "alias": by_latest.get(h) or (info[0] if info else None),
                "version": info[1] if info else None,
            }
        return templates.TemplateResponse(
            request,
            "lineage.html",
            {
                "project": project_name,
                "nodes": dag["nodes"],
                "edges": dag["edges"],
                "positions": layout["positions"],
                "width": layout["width"],
                "height": layout["height"],
                "annotated": annotated_by_hash,
                "kind": "catalog",
            },
        )

    @app.get("/lineage/{content_hash}", response_class=HTMLResponse)
    def lineage_entry(request: Request, content_hash: str):
        project_name = _require_project()
        aliases = load_aliases(project_name)
        if content_hash in aliases:
            content_hash = aliases[content_hash]
        if not entry_dir(project_name, content_hash).exists():
            raise HTTPException(404, f"entry {content_hash} not found")
        lin = read_internal_lineage(project_name, content_hash)
        # xorq edges point from consumer → producer; for left-to-right
        # data-flow display we want producer on the left. BFS from a *leaf*
        # gives us that — but the most reliable display is: BFS from the
        # root (consumer at the right by inverting later).
        layout = layered_positions(
            lin["nodes"], lin["edges"], root=lin["root"], x_step=180, y_step=70
        )
        cols = column_lineage(project_name, content_hash)
        return templates.TemplateResponse(
            request,
            "lineage_entry.html",
            {
                "project": project_name,
                "content_hash": content_hash,
                "lineage": lin,
                "positions": layout["positions"],
                "width": layout["width"],
                "height": layout["height"],
                "column_trees": cols,
            },
        )

    @app.get("/api/lineage/{content_hash}")
    def api_lineage(content_hash: str):
        project_name = _require_project()
        if not entry_dir(project_name, content_hash).exists():
            raise HTTPException(404)
        return {
            "internal": read_internal_lineage(project_name, content_hash),
        }

    @app.get("/api/catalog_dag")
    def api_catalog_dag():
        project_name = _require_project()
        return {"project": project_name, **catalog_dag(project_name)}

    @app.get("/notebook", response_class=HTMLResponse)
    def notebook_view(request: Request):
        project_name = _require_project()
        data = notebook.load(project_name)
        aliases = load_aliases(project_name)
        buckaroo_available = buckaroo is not None and buckaroo.is_running
        buckaroo_ws_base = buckaroo.ws_base_url if buckaroo_available else None
        rendered_cells = []
        for c in data["cells"]:
            latest = aliases.get(c["alias"])
            entry_meta = None
            preview_html = ""
            schema = None
            if latest is not None:
                entry = entry_dir(project_name, latest)
                if (entry / "manifest.json").exists():
                    entry_meta = json.loads((entry / "manifest.json").read_text())
                    schema = json.loads((entry / "schema.json").read_text())
                # Sessions are created lazily via /api/session/<hash> when the
                # cell scrolls into view. Only fall back to pandas preview when
                # Buckaroo is not available at all.
                if not buckaroo_available and (entry / "result.parquet").exists():
                    table = pq.read_table(entry / "result.parquet")
                    df = table.to_pandas()
                    n = min(20, len(df))
                    preview_html = df.head(n).to_html(
                        classes="data-table", index=False, border=0
                    )
            info = version_of_hash(project_name, latest) if latest else None
            rendered_cells.append(
                {
                    **c,
                    "latest_hash": latest,
                    "version": info[1] if info else None,
                    "entry_meta": entry_meta,
                    "schema": schema,
                    "preview_html": preview_html,
                    "markdown_html": _render_markdown(c.get("markdown", "")),
                    "buckaroo_available": buckaroo_available,
                    "chart_spec": get_chart(project_name, latest) if latest else None,
                }
            )
        return templates.TemplateResponse(
            request,
            "notebook.html",
            {
                "project": project_name,
                "cells": rendered_cells,
                "buckaroo_ws_base": buckaroo_ws_base,
                "buckaroo_available": buckaroo_available,
            },
        )

    @app.get("/api/session/{content_hash}")
    def get_session(content_hash: str):
        """Create (or return cached) Buckaroo session for a catalog entry.

        Called by the notebook's IntersectionObserver when a cell scrolls into
        view. Keeps page-load O(1) regardless of notebook size — sessions are
        created one at a time as the user scrolls, not all at once.
        """
        if buckaroo is None or not buckaroo.is_running:
            return {"ws_url": None}
        project_name = _require_project()
        session_id = buckaroo.ensure_session(content_hash, project_name)
        if session_id is None:
            return {"ws_url": None}
        return {"ws_url": f"{buckaroo.ws_base_url}/ws/{session_id}"}

    @app.get("/export/marimo")
    def export_marimo():
        """Download the default notebook as a Marimo Python file."""
        project_name = _require_project()
        from fastapi.responses import Response  # noqa: PLC0415

        from pydata_core.marimo_export import notebook_to_marimo  # noqa: PLC0415

        content = notebook_to_marimo(project_name)
        filename = f"{project_name}_notebook.py"
        return Response(
            content=content,
            media_type="text/x-python",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.patch("/api/notebook")
    async def patch_notebook(payload: dict):
        project_name = _require_project()
        if read_only:
            raise HTTPException(403, "serve mode")
        action = payload.get("action")
        cell_id = payload.get("cell_id", "")
        try:
            if action == "reorder":
                notebook.reorder(project_name, cell_id, int(payload["new_index"]))
            elif action == "remove":
                notebook.remove(project_name, cell_id)
            else:
                raise HTTPException(400, f"unknown action {action!r}")
        except CellNotFound:
            raise HTTPException(404, f"cell {cell_id!r} not found")
        await publish({"kind": "notebook_changed"})
        return {"ok": True}

    @app.put("/api/code/{alias}")
    async def put_code(alias: str, payload: dict):
        """Build a new revision of `alias` from edited code.

        T-16: lets the user fix a typo or tweak an expression from the
        browser without having to round-trip through Claude. The new
        revision becomes V_{n+1}; V_n stays in forensic history.
        """
        project_name = _require_project()
        if read_only:
            raise HTTPException(403, "serve mode")
        from pydata_core import get_alias as _get_alias
        from pydata_core import set_alias as _set_alias
        from pydata_xorq import build_and_persist as _build_and_persist
        from pydata_xorq.build import BuildError

        if _get_alias(project_name, alias) is None:
            raise HTTPException(404, f"alias {alias!r} not found")
        code = payload.get("code", "")
        if not code.strip():
            raise HTTPException(400, "code is empty")
        try:
            res = _build_and_persist(project_name, code, prompt=payload.get("prompt") or None)
        except BuildError as exc:
            from pydata_core import record_error as _record_error
            rec = _record_error(
                project_name, code=code, message=str(exc), tool="api_code"
            )
            await publish({"kind": "build_failed", "error_id": rec["id"], "tool": "api_code"})
            raise HTTPException(400, str(exc))
        info = _set_alias(project_name, alias, res.content_hash, expect_exists=True)
        await publish({"kind": "new_entry", "hash": res.content_hash, "alias": alias, "version": info["version"]})
        return {
            "hash": res.content_hash,
            "alias": alias,
            "version": info["version"],
            "row_count": res.row_count,
        }

    @app.put("/api/markdown/{cell_id}")
    async def put_markdown(cell_id: str, payload: dict):
        project_name = _require_project()
        if read_only:
            raise HTTPException(403, "serve mode")
        markdown = payload.get("markdown", "")
        try:
            cell = notebook.edit_markdown(project_name, cell_id, markdown)
        except CellNotFound:
            raise HTTPException(404, f"cell {cell_id!r} not found")
        await publish({"kind": "notebook_changed"})
        return {**cell, "html": _render_markdown(cell["markdown"])}

    @app.get("/api/notebook")
    def api_notebook():
        project_name = _require_project()
        return {"project": project_name, **notebook.load(project_name)}

    @app.get("/diff/{alias}", response_class=HTMLResponse)
    def diff_default(request: Request, alias: str):
        project_name = _require_project()
        hashes = history_for(project_name, alias)
        if not hashes:
            raise HTTPException(404, f"alias {alias!r} has no history")
        if len(hashes) < 2:
            raise HTTPException(
                400, f"alias {alias!r} only has one version — nothing to diff"
            )
        return _render_diff(request, alias, len(hashes) - 1, len(hashes))

    @app.get("/diff/{alias}/{va:int}/{vb:int}", response_class=HTMLResponse)
    def diff_versions(request: Request, alias: str, va: int, vb: int):
        return _render_diff(request, alias, va, vb)

    def _render_diff(request: Request, alias: str, va: int, vb: int):
        project_name = _require_project()
        hashes = history_for(project_name, alias)
        if not hashes or va < 1 or vb < 1 or va > len(hashes) or vb > len(hashes):
            raise HTTPException(404, "version out of range")
        a_hash = hashes[va - 1]
        b_hash = hashes[vb - 1]
        a_dir = entry_dir(project_name, a_hash)
        b_dir = entry_dir(project_name, b_hash)
        if not a_dir.exists() or not b_dir.exists():
            raise HTTPException(404, "one or both entries missing")
        diff = full_diff(a_dir, b_dir, a_label=f"V{va}", b_label=f"V{vb}")
        return templates.TemplateResponse(
            request,
            "diff.html",
            {
                "project": project_name,
                "alias": alias,
                "va": va,
                "vb": vb,
                "a_hash": a_hash,
                "b_hash": b_hash,
                "hashes": hashes,
                "diff": diff,
            },
        )

    @app.get("/api/aliases")
    def api_aliases():
        project_name = _require_project()
        aliases = load_aliases(project_name)
        return {
            "project": project_name,
            "aliases": [
                {
                    "name": name,
                    "hash": hash_,
                    "history": history_for(project_name, name),
                }
                for name, hash_ in sorted(aliases.items())
            ],
        }

    @app.get("/api/errors")
    def api_errors(limit: int = 20):
        project_name = _require_project()
        return {"project": project_name, "errors": list_errors(project_name, limit=limit)}

    @app.get("/errors/{error_id}", response_class=HTMLResponse)
    def error_detail(request: Request, error_id: str):
        project_name = _require_project()
        rec = get_error(project_name, error_id)
        if rec is None:
            raise HTTPException(404, f"error {error_id} not found")
        sidebar_entries = _annotate_entries(project_name, list_entries(project_name))
        return templates.TemplateResponse(
            request,
            "error_detail.html",
            {
                "project": project_name,
                "error": rec,
                "entries": sidebar_entries,
                "current_hash": None,
            },
        )

    return app
