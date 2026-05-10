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

from pydata_core import entries_dir, entry_dir, resolve_project
from pydata_xorq import list_entries

PKG_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PKG_DIR / "templates"
STATIC_DIR = PKG_DIR / "static"


class NotifyPayload(BaseModel):
    kind: str
    hash: str | None = None
    extra: dict | None = None


def _broadcaster() -> tuple[asyncio.Queue, list]:
    return asyncio.Queue(), []


def create_app(project: str | None = None) -> FastAPI:
    project_name = resolve_project(project)
    app = FastAPI(title="pydata companion")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
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
        entries = list_entries(project_name)
        return templates.TemplateResponse(
            request,
            "catalog.html",
            {"project": project_name, "entries": entries},
        )

    @app.get("/catalog/{content_hash}", response_class=HTMLResponse)
    def catalog_entry(request: Request, content_hash: str):
        entry = entry_dir(project_name, content_hash)
        if not entry.exists():
            raise HTTPException(404, f"entry {content_hash} not found")
        manifest = json.loads((entry / "manifest.json").read_text())
        schema = json.loads((entry / "schema.json").read_text())
        code = (entry / "expr.py").read_text()

        # V0: render the parquet as HTML directly. Cap rows to keep payload small.
        table = pq.read_table(entry / "result.parquet")
        df = table.to_pandas()
        preview_rows = min(200, len(df))
        table_html = df.head(preview_rows).to_html(
            classes="data-table", index=False, border=0
        )

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
        await publish(payload.model_dump())
        return {"ok": True, "subscribers": len(subscribers)}

    @app.get("/api/entries")
    def api_entries():
        return {"project": project_name, "entries": list_entries(project_name)}

    return app
