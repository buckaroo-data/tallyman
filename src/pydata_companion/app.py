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
    resolve_project,
    version_of_hash,
)
from pydata_xorq import list_entries, read_prompts

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
