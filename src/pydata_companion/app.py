from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from pydata_core import (
    alias_for_hash,
    entry_dir,
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
    full_diff,
    list_entries,
    read_internal_lineage,
    read_prompts,
)
from pydata_xorq.layout import layered_positions

PKG_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PKG_DIR / "templates"
STATIC_DIR = PKG_DIR / "static"


class NotifyPayload(BaseModel):
    kind: str
    hash: str | None = None
    extra: dict | None = None


def _broadcaster() -> tuple[asyncio.Queue, list]:
    return asyncio.Queue(), []


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


def create_app(project: str | None = None, *, read_only: bool = False) -> FastAPI:
    project_name = resolve_project(project)
    app = FastAPI(title="pydata companion")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals["read_only"] = read_only
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # In-process fan-out for SSE clients. Each client gets its own queue.
    subscribers: list[asyncio.Queue] = []

    async def publish(event: dict):
        for q in list(subscribers):
            await q.put(event)

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/catalog")

    @app.get("/catalog", response_class=HTMLResponse)
    def catalog(request: Request):
        entries = _annotate_entries(project_name, list_entries(project_name))
        errors = list_errors(project_name, limit=5)
        return templates.TemplateResponse(
            request,
            "catalog.html",
            {"project": project_name, "entries": entries, "errors": errors},
        )

    @app.get("/catalog/{content_hash}", response_class=HTMLResponse)
    def catalog_entry(request: Request, content_hash: str):
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

        # V0: render the parquet as HTML directly. Cap rows to keep payload small.
        table = pq.read_table(entry / "result.parquet")
        df = table.to_pandas()
        preview_rows = min(200, len(df))
        table_html = df.head(preview_rows).to_html(
            classes="data-table", index=False, border=0
        )

        prompt_history = read_prompts(entry)
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
                "total_rows": len(df),
                "prompt_history": prompt_history,
                "alias": alias,
                "version": version,
                "forensic_history": forensic_history,
            },
        )

    @app.get("/api/sse")
    async def sse(request: Request):
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
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        await publish(payload.model_dump())
        return {"ok": True, "subscribers": len(subscribers)}

    @app.get("/api/entries")
    def api_entries():
        return {
            "project": project_name,
            "entries": _annotate_entries(project_name, list_entries(project_name)),
        }

    @app.get("/lineage", response_class=HTMLResponse)
    def lineage_overview(request: Request):
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
            },
        )

    @app.get("/api/lineage/{content_hash}")
    def api_lineage(content_hash: str):
        if not entry_dir(project_name, content_hash).exists():
            raise HTTPException(404)
        return {
            "internal": read_internal_lineage(project_name, content_hash),
        }

    @app.get("/api/catalog_dag")
    def api_catalog_dag():
        return {"project": project_name, **catalog_dag(project_name)}

    @app.get("/notebook", response_class=HTMLResponse)
    def notebook_view(request: Request):
        data = notebook.load(project_name)
        aliases = load_aliases(project_name)
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
                if (entry / "result.parquet").exists():
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
                }
            )
        return templates.TemplateResponse(
            request,
            "notebook.html",
            {"project": project_name, "cells": rendered_cells},
        )

    @app.patch("/api/notebook")
    async def patch_notebook(payload: dict):
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

    @app.put("/api/markdown/{cell_id}")
    async def put_markdown(cell_id: str, payload: dict):
        if read_only:
            raise HTTPException(403, "serve mode")
        markdown = payload.get("markdown", "")
        try:
            cell = notebook.edit_markdown(project_name, cell_id, markdown)
        except CellNotFound:
            raise HTTPException(404, f"cell {cell_id!r} not found")
        await publish({"kind": "notebook_changed"})
        return cell

    @app.get("/api/notebook")
    def api_notebook():
        return {"project": project_name, **notebook.load(project_name)}

    @app.get("/diff/{alias}", response_class=HTMLResponse)
    def diff_default(request: Request, alias: str):
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
        return {"project": project_name, "errors": list_errors(project_name, limit=limit)}

    @app.get("/errors/{error_id}", response_class=HTMLResponse)
    def error_detail(request: Request, error_id: str):
        rec = get_error(project_name, error_id)
        if rec is None:
            raise HTTPException(404, f"error {error_id} not found")
        return templates.TemplateResponse(
            request,
            "error_detail.html",
            {"project": project_name, "error": rec},
        )

    return app
