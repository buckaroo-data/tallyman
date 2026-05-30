from __future__ import annotations

import asyncio
import json
import logging
import os
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


def _fmt_bytes(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f} GB"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f} MB"
    if n >= 1_000:
        return f"{n / 1_000:.1f} KB"
    return f"{n} B"


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
    membership = (
        ibis.cases(
            (joined[b_sent].isnull(), ibis.literal(1).cast("int8")),
            (joined[a_sent].isnull(), ibis.literal(2).cast("int8")),
            else_=ibis.literal(3).cast("int8"),
        )
        .name("membership")
    )
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

    expr = joined.select(*sel)

    builds_dir = tempfile.mkdtemp(prefix="pydata_diff_")
    build_path = Path(xo.build_expr(expr, builds_dir=builds_dir))

    # Color config mirrors col_join_dfs: pink=a_only, teal=b_only, green=both.
    eq_map = ["pink", "#73ae80", "#90b2b3", "#6c83b5"]
    pk_color = "#6c5fc7"
    pk_map = [pk_color] * 4
    overrides: dict = {"membership": {"merge_rule": "hidden"}}
    for col in a_non_keys:
        if col in b_non_keys:
            overrides[f"{col}_v2"] = {"merge_rule": "hidden"}
            overrides[f"{col}_eq"] = {"merge_rule": "hidden"}
            overrides[col] = {
                "tooltip_config": {"tooltip_type": "simple", "val_column": f"{col}_v2"},
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
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals["read_only"] = read_only
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

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
    # Root redirect
    # ------------------------------------------------------------------

    @app.get("/", include_in_schema=False)
    def root(request: Request):
        if _current_project() is None:
            from pydata_core.paths import list_projects

            return templates.TemplateResponse(
                request,
                "empty_state.html",
                {"available_projects": list_projects()},
            )
        return RedirectResponse(f"/{_current_project()}/catalog")

    # ------------------------------------------------------------------
    # Project-prefixed HTML routes
    # ------------------------------------------------------------------

    @app.get("/{project}/catalog", response_class=HTMLResponse)
    def catalog(request: Request, project: str, missing: str | None = None):
        project = _validate_project(project)
        entries = _annotate_entries(project, list_entries(project))
        errors = list_errors(project, limit=5)
        return templates.TemplateResponse(
            request,
            "catalog.html",
            {
                "project": project,
                "entries": entries,
                "errors": errors,
                "missing_hash": missing,
            },
        )

    @app.get("/{project}/catalog/{content_hash}", response_class=HTMLResponse)
    def catalog_entry(request: Request, project: str, content_hash: str):
        project = _validate_project(project)
        alias = None
        version = None
        forensic_history: list[dict] = []

        aliases = load_aliases(project)
        if content_hash in aliases:
            alias = content_hash
            content_hash = aliases[alias]

        entry = entry_dir(project, content_hash)
        if not entry.exists():
            return RedirectResponse(f"/{project}/catalog?missing={content_hash}", status_code=303)
        manifest = json.loads((entry / "manifest.json").read_text())
        schema = json.loads((entry / "schema.json").read_text())
        code = (entry / "expr.py").read_text()

        if alias is None:
            alias = alias_for_hash(project, content_hash)
        info = version_of_hash(project, content_hash)
        if info is not None:
            alias = alias or info[0]
            version = info[1]
        if alias is not None:
            hist_hashes = history_for(project, alias)
            for i, h in enumerate(hist_hashes, start=1):
                forensic_history.append({"hash": h, "version": i, "is_current": h == content_hash})

        prompt_history = read_prompts(entry)
        chart_spec = get_chart(project, content_hash)
        sidebar_entries = _annotate_entries(project, list_entries(project))
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
        buckaroo_session = buckaroo.ensure_session(content_hash, project) if buckaroo is not None else None
        buckaroo_ws_base = buckaroo.ws_base_url if buckaroo and buckaroo.is_running else None

        table_html = ""
        preview_rows = 0
        result_path = entry / "result.parquet"
        if result_path.exists():
            total_rows = pq.ParquetFile(result_path).metadata.num_rows
            if buckaroo_session is None:
                preview_rows = min(200, total_rows)
                head_table = _read_head_rows(result_path, preview_rows)
                table_html = head_table.to_pandas().to_html(classes="data-table", index=False, border=0)
        else:
            total_rows = 0
        return templates.TemplateResponse(
            request,
            "entry_detail.html",
            {
                "project": project,
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

    @app.get("/{project}/lineage", response_class=HTMLResponse)
    def lineage_overview(request: Request, project: str):
        project = _validate_project(project)
        dag = catalog_dag(project)
        node_id = lambda n: n["hash"]  # noqa: E731
        nodes_for_layout = [{"id": node_id(n), **n} for n in dag["nodes"]]
        targets = {e["to"] for e in dag["edges"]}
        roots = [n["id"] for n in nodes_for_layout if n["id"] not in targets]
        root = roots[0] if roots else (nodes_for_layout[0]["id"] if nodes_for_layout else None)
        layout = layered_positions(nodes_for_layout, dag["edges"], root=root, x_step=200, y_step=80)
        aliases = load_aliases(project)
        by_latest = {h: a for a, h in aliases.items()}
        annotated_by_hash: dict[str, dict] = {}
        for n in dag["nodes"]:
            h = n["hash"]
            info = version_of_hash(project, h)
            annotated_by_hash[h] = {
                "alias": by_latest.get(h) or (info[0] if info else None),
                "version": info[1] if info else None,
            }
        return templates.TemplateResponse(
            request,
            "lineage.html",
            {
                "project": project,
                "nodes": dag["nodes"],
                "edges": dag["edges"],
                "positions": layout["positions"],
                "width": layout["width"],
                "height": layout["height"],
                "annotated": annotated_by_hash,
                "kind": "catalog",
            },
        )

    @app.get("/{project}/lineage/{content_hash}", response_class=HTMLResponse)
    def lineage_entry(request: Request, project: str, content_hash: str):
        project = _validate_project(project)
        aliases = load_aliases(project)
        if content_hash in aliases:
            content_hash = aliases[content_hash]
        if not entry_dir(project, content_hash).exists():
            raise HTTPException(404, f"entry {content_hash} not found")
        lin = read_internal_lineage(project, content_hash)
        layout = layered_positions(lin["nodes"], lin["edges"], root=lin["root"], x_step=180, y_step=70)
        cols = column_lineage(project, content_hash)
        return templates.TemplateResponse(
            request,
            "lineage_entry.html",
            {
                "project": project,
                "content_hash": content_hash,
                "lineage": lin,
                "positions": layout["positions"],
                "width": layout["width"],
                "height": layout["height"],
                "column_trees": cols,
            },
        )

    @app.get("/{project}/notebook", response_class=HTMLResponse)
    def notebook_view(request: Request, project: str):
        project = _validate_project(project)
        data = notebook.load(project)
        aliases = load_aliases(project)
        buckaroo_available = buckaroo is not None and buckaroo.is_running
        buckaroo_ws_base = buckaroo.ws_base_url if buckaroo_available else None
        rendered_cells = []
        for c in data["cells"]:
            latest = aliases.get(c["alias"])
            entry_meta = None
            preview_html = ""
            schema = None
            if latest is not None:
                entry = entry_dir(project, latest)
                if (entry / "manifest.json").exists():
                    entry_meta = json.loads((entry / "manifest.json").read_text())
                    schema = json.loads((entry / "schema.json").read_text())
                if not buckaroo_available and (entry / "result.parquet").exists():
                    table = pq.read_table(entry / "result.parquet")
                    df = table.to_pandas()
                    n = min(20, len(df))
                    preview_html = df.head(n).to_html(classes="data-table", index=False, border=0)
            info = version_of_hash(project, latest) if latest else None
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
                    "chart_spec": get_chart(project, latest) if latest else None,
                }
            )
        return templates.TemplateResponse(
            request,
            "notebook.html",
            {
                "project": project,
                "cells": rendered_cells,
                "buckaroo_ws_base": buckaroo_ws_base,
                "buckaroo_available": buckaroo_available,
            },
        )

    @app.get("/{project}/diff/{alias}", response_class=HTMLResponse)
    def diff_default(request: Request, project: str, alias: str):
        project = _validate_project(project)
        hashes = history_for(project, alias)
        if not hashes:
            raise HTTPException(404, f"alias {alias!r} has no history")
        if len(hashes) < 2:
            raise HTTPException(400, f"alias {alias!r} only has one version — nothing to diff")
        return _render_diff(request, project, alias, len(hashes) - 1, len(hashes))

    @app.get("/{project}/diff/{alias}/{va:int}/{vb:int}", response_class=HTMLResponse)
    def diff_versions(request: Request, project: str, alias: str, va: int, vb: int):
        project = _validate_project(project)
        return _render_diff(request, project, alias, va, vb)

    def _render_diff(request: Request, project: str, alias: str, va: int, vb: int):
        hashes = history_for(project, alias)
        if not hashes or va < 1 or vb < 1 or va > len(hashes) or vb > len(hashes):
            raise HTTPException(404, "version out of range")
        a_hash = hashes[va - 1]
        b_hash = hashes[vb - 1]
        a_dir = entry_dir(project, a_hash)
        b_dir = entry_dir(project, b_hash)
        if not a_dir.exists() or not b_dir.exists():
            raise HTTPException(404, "one or both entries missing")
        # Diff the two entry *expressions*, not their result.parquet files.
        # cached_result_expr wraps each in xorq's snapshot cache when the
        # computation is expensive (aggregate/join/sort/UDF/CSV) and leaves
        # cheap parquet-read-and-project entries uncached — so each side
        # resolves its own cache (hit → read, miss → recompute) and a deleted
        # result.parquet never blanks the diff.
        from pydata_xorq.primary_key import diff_keys
        from pydata_xorq.result_cache import cached_result_expr
        a_expr = cached_result_expr(project, a_hash)
        b_expr = cached_result_expr(project, b_hash)
        # Resolve the join key from cached lineage (detect-once, inherit across
        # row-preserving revisions) instead of re-scanning both sides here.
        keys = diff_keys(project, a_hash, b_hash) or None
        diff = full_diff(a_dir, b_dir, a_label=f"V{va}", b_label=f"V{vb}",
                         backend="xorq", a_expr=a_expr, b_expr=b_expr, keys=keys)

        # Attempt a live Buckaroo comparison if the server is up — it streams
        # rows from the composed comparison expression on demand.
        compare_session = None
        buckaroo_ws_base = None
        if buckaroo is not None and buckaroo.is_running:
            if not keys:
                keys = diff["keyed"]["keys"] if diff.get("keyed") else None
            if keys:
                session_id = f"diff-{a_hash[:12]}-{b_hash[:12]}"
                try:
                    build_path, overrides = _build_compare_expr(a_expr, b_expr, keys)
                    # Persist Buckaroo's summary stats for this comparison so
                    # re-opening the same pair reuses them instead of
                    # recomputing over the full join (the bulk of /load_expr).
                    from pydata_core.paths import diff_stat_cache_dir
                    stat_cache = diff_stat_cache_dir(project, a_hash, b_hash)
                    stat_cache.mkdir(parents=True, exist_ok=True)
                    resp = buckaroo._client.post(
                        f"{buckaroo.base_url}/load_expr",
                        json={
                            "session": session_id,
                            "build_dir": str(build_path),
                            "no_browser": True,
                            "column_config_overrides": overrides,
                            "cache_storage_path": str(stat_cache),
                        },
                        timeout=30.0,
                    )
                    if resp.status_code == 200:
                        compare_session = session_id
                        buckaroo_ws_base = buckaroo.ws_base_url
                except Exception as exc:
                    log.warning("diff /load_expr compare failed: %s", exc)

        return templates.TemplateResponse(
            request,
            "diff.html",
            {
                "project": project,
                "alias": alias,
                "va": va,
                "vb": vb,
                "a_hash": a_hash,
                "b_hash": b_hash,
                "hashes": hashes,
                "diff": diff,
                "compare_session": compare_session,
                "buckaroo_ws_base": buckaroo_ws_base,
            },
        )

    @app.get("/{project}/errors/{error_id}", response_class=HTMLResponse)
    def error_detail(request: Request, project: str, error_id: str):
        project = _validate_project(project)
        rec = get_error(project, error_id)
        if rec is None:
            raise HTTPException(404, f"error {error_id} not found")
        sidebar_entries = _annotate_entries(project, list_entries(project))
        return templates.TemplateResponse(
            request,
            "error_detail.html",
            {
                "project": project,
                "error": rec,
                "entries": sidebar_entries,
                "current_hash": None,
            },
        )

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
    # Project-prefixed API routes (called from JS)
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

    @app.get("/{project}/api/lineage/{content_hash}")
    def api_lineage(project: str, content_hash: str):
        project = _validate_project(project)
        if not entry_dir(project, content_hash).exists():
            raise HTTPException(404)
        return {
            "internal": read_internal_lineage(project, content_hash),
        }

    @app.get("/{project}/api/catalog_dag")
    def api_catalog_dag(project: str):
        project = _validate_project(project)
        return {"project": project, **catalog_dag(project)}

    @app.get("/{project}/api/session/{content_hash}")
    def get_session(project: str, content_hash: str):
        project = _validate_project(project)
        if buckaroo is None or not buckaroo.is_running:
            return {"ws_url": None}
        session_id = buckaroo.ensure_session(content_hash, project)
        if session_id is None:
            return {"ws_url": None}
        return {"ws_url": f"{buckaroo.ws_base_url}/ws/{session_id}"}

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
        from pydata_core import get_alias as _get_alias
        from pydata_core import set_alias as _set_alias
        from pydata_xorq import build_and_persist as _build_and_persist
        from pydata_xorq.build import BuildError

        if _get_alias(project, alias) is None:
            raise HTTPException(404, f"alias {alias!r} not found")
        code = payload.get("code", "")
        if not code.strip():
            raise HTTPException(400, "code is empty")
        try:
            res = _build_and_persist(project, code, prompt=payload.get("prompt") or None)
        except BuildError as exc:
            from pydata_core import record_error as _record_error

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

    @app.get("/{project}/api/notebook")
    def api_notebook(project: str):
        project = _validate_project(project)
        return {"project": project, **notebook.load(project)}

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

    @app.get("/{project}/api/disk_usage")
    def api_disk_usage(project: str):
        project = _validate_project(project)
        from pydata_core.paths import data_dir as _data_dir
        from pydata_core.paths import entries_dir as _entries_dir

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
        total = data + results + builds + cache
        return {
            "data": data,
            "results": results,
            "builds": builds,
            "cache": cache,
            "total": total,
            "formatted": {
                "data": _fmt_bytes(data),
                "results": _fmt_bytes(results),
                "builds": _fmt_bytes(builds),
                "cache": _fmt_bytes(cache),
                "total": _fmt_bytes(total),
            },
        }

    # ------------------------------------------------------------------
    # Project-agnostic routes (no project prefix)
    # ------------------------------------------------------------------

    @app.post("/internal/notify")
    async def notify(payload: NotifyPayload):
        project_name = _require_project()
        if read_only:
            raise HTTPException(403, "companion is in read-only (serve) mode")
        log.info(
            "notify kind=%s hash=%s",
            payload.kind,
            getattr(payload, "hash", None),
        )
        if payload.kind in ("post_processing_changed", "summary_stat_changed"):
            if buckaroo:
                reloaded = buckaroo.reload_project_sessions(project_name)
                log.info("reloaded klasses in %d buckaroo session(s) for project %s", reloaded, project_name)
        await publish(payload.model_dump())
        return {"ok": True, "subscribers": len(subscribers), "project": project_name}

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

    return app
