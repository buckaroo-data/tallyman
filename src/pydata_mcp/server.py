from __future__ import annotations

import logging
import os
import sys

import httpx
from fastmcp import FastMCP

from pydata_core import (
    AliasExists,
    AliasNotFound,
    CellNotFound,
    ChartSpecError,
    PostProcessingRunError,
    PostProcessingSourceError,
    export_notebook_path,
    StatSourceError,
    alias_for_hash,
    entry_dir,
    get_alias,
    history_for,
    list_post_processings,
    list_stats,
    notebook,
    record_error,
    remove_alias,
    remove_post_processing,
    remove_stat,
    rename_alias,
    resolve_project,
    run_post_processing,
    set_alias,
    set_chart,
    write_post_processing,
    write_stat,
)
from pydata_xorq import BuildError, build_and_persist, full_diff, list_entries

log = logging.getLogger("pydata_mcp")
COMPANION_URL = os.environ.get("PYDATA_COMPANION_URL", "http://127.0.0.1:7860")

mcp = FastMCP("pydata")


def _notify(kind: str, content_hash: str | None = None, **extra) -> None:
    """Best-effort POST to the companion's /internal/notify. Never raise."""
    try:
        with httpx.Client(timeout=2.0) as client:
            client.post(
                f"{COMPANION_URL}/internal/notify",
                json={"kind": kind, "hash": content_hash, "extra": extra or None},
            )
    except Exception as exc:
        # Companion may not be up; that's allowed. Log and move on.
        print(f"[pydata_mcp] notify failed ({kind}): {exc}", file=sys.stderr)


@mcp.tool()
def catalog_run(code: str, prompt: str = "") -> dict:
    """Execute a xorq expression script and persist it as a catalog entry.

    The provided code MUST bind a top-level variable named `expr` to a xorq/ibis
    expression. Imports allowed; all imports happen in a fresh module scope.

    Use `xorq.api as xo` and the deferred readers:
        import xorq.api as xo
        t = xo.deferred_read_parquet("/path/to/file.parquet")
        expr = t.group_by("region").aggregate(total=t.price.sum())

    Use `xorq.vendor.ibis as ibis` if you need ibis directly. Do NOT `import ibis`.

    Args:
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description (the user's intent).

    Returns:
        dict with keys: hash, row_count, execute_seconds, schema, entry_path.
    """
    project = resolve_project()
    out = _run_and_record(project, code, prompt, tool="catalog_run")
    if "error" in out:
        return out
    out.pop("_build", None)
    _notify("new_entry", content_hash=out["hash"])
    return out


@mcp.tool()
def catalog_load_parquet(
    rel_path: str, prompt: str = "", name: str = ""
) -> dict:
    """Register a parquet file from the project's data/ directory as a catalog entry.

    Use this for simple "load this file" steps. The agent does not need to write
    any xorq code — pass a path relative to the project's `data/` directory.

    Args:
        rel_path: Path relative to `<project>/data/`. Example: "orders.parquet".
        prompt: Optional human-readable description (the user's intent).
        name: Optional alias to register the entry under. If provided, the
            entry behaves like one created by `catalog_create` (named, appended
            to the notebook). Errors if the alias already exists.

    Returns:
        Same shape as catalog_run, plus `alias`/`version` when `name` is set.
    """
    project = resolve_project()
    if name and get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists. Use catalog_revise to update it."}
    code = (
        "from pydata_xorq.io import from_project\n"
        f"expr = from_project({rel_path!r})\n"
    )
    out = _run_and_record(project, code, prompt, tool="catalog_load_parquet")
    if "error" in out:
        return out
    out.pop("_build", None)
    if name:
        info = set_alias(project, name, out["hash"], expect_exists=False)
        notebook.append(project, name, markdown=prompt or "")
        out["alias"] = info["name"]
        out["version"] = info["version"]
        _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
        _notify("notebook_changed")
    else:
        _notify("new_entry", content_hash=out["hash"])
    return out


def _run_and_record(project: str, code: str, prompt: str, *, tool: str = "catalog_run") -> dict:
    """Shared body for tools that compile-and-persist; returns the tool reply dict."""
    try:
        result = build_and_persist(project=project, code=code, prompt=prompt or None)
    except BuildError as exc:
        rec = record_error(
            project, code=code, message=str(exc), prompt=prompt or None, tool=tool
        )
        _notify("build_failed", error_id=rec["id"], tool=tool)
        return {"error": str(exc), "error_id": rec["id"]}
    return {
        "_build": result,
        "hash": result.content_hash,
        "row_count": result.row_count,
        "execute_seconds": result.execute_seconds,
        "schema": result.schema,
        "entry_path": str(result.entry_path),
    }


@mcp.tool()
def catalog_create(name: str, code: str, prompt: str = "") -> dict:
    """Execute and persist a NAMED catalog entry (alias).

    Use this when the user gives a concept a name ("name this `shoe_sales`").
    Errors if the alias already exists — use `catalog_revise` to update an
    existing alias instead.

    Args:
        name: The alias for this entry. Must not already exist in the project.
        code: A self-contained Python script that binds `expr`. Same conventions
            as `catalog_run`. Use `from pydata_xorq.io import from_project` for
            project-local data files.
        prompt: Optional human-readable description.

    Returns:
        Same shape as catalog_run, plus `alias` and `version`.
    """
    project = resolve_project()
    if get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists. Use catalog_revise to update it."}
    out = _run_and_record(project, code, prompt, tool="catalog_create")
    if "error" in out:
        return out
    info = set_alias(project, name, out["hash"], expect_exists=False)
    notebook.append(project, name, markdown=prompt or "")
    out.pop("_build", None)
    out["alias"] = info["name"]
    out["version"] = info["version"]
    _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
    _notify("notebook_changed")
    return out


@mcp.tool()
def catalog_revise(name: str, code: str, prompt: str = "") -> dict:
    """Persist a NEW VERSION of an existing alias.

    The alias is repointed to the new content hash. The previous hash remains
    in the catalog as a forensic artifact and is recorded in alias_history.

    Args:
        name: An existing alias.
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description of what changed.
    """
    project = resolve_project()
    if get_alias(project, name) is None:
        return {"error": f"alias {name!r} does not exist. Use catalog_create to create it."}
    out = _run_and_record(project, code, prompt, tool="catalog_revise")
    if "error" in out:
        return out
    info = set_alias(project, name, out["hash"], expect_exists=True)
    out.pop("_build", None)
    out["alias"] = info["name"]
    out["version"] = info["version"]
    _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
    return out


@mcp.tool()
def catalog_alias(hash: str, name: str) -> dict:
    """Promote an existing scratch (unnamed) entry to a named entry.

    Use this when an entry was created via `catalog_run` and you decide
    post-hoc that it deserves a name. Errors if the alias already exists.
    """
    project = resolve_project()
    if not entry_dir(project, hash).exists():
        return {"error": f"no catalog entry for hash {hash!r}"}
    if get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists"}
    info = set_alias(project, name, hash, expect_exists=False)
    notebook.append(project, name)
    _notify("alias_changed", content_hash=hash, alias=name, version=info["version"])
    _notify("notebook_changed")
    return {"hash": hash, "alias": name, "version": info["version"]}


@mcp.tool()
def catalog_rename(old_name: str, new_name: str) -> dict:
    """Rename an existing alias. Preserves version history and notebook position."""
    project = resolve_project()
    try:
        info = rename_alias(project, old_name, new_name)
    except AliasNotFound:
        return {"error": f"alias {old_name!r} does not exist"}
    except AliasExists:
        return {"error": f"alias {new_name!r} already exists"}
    notebook.rename_alias_in_cells(project, old_name, new_name)
    _notify("alias_renamed", old=old_name, new=new_name, content_hash=info["hash"])
    _notify("notebook_changed")
    return info


@mcp.tool()
def catalog_unalias(name: str) -> dict:
    """Remove an alias (the named entries become scratch again).

    Catalog entries themselves remain; only the named handle is dropped. Any
    notebook cells anchored on this alias are removed.
    """
    project = resolve_project()
    if get_alias(project, name) is None:
        return {"error": f"alias {name!r} does not exist"}
    remove_alias(project, name)
    n = notebook.remove_alias_from_cells(project, name)
    _notify("alias_changed", alias=name)
    if n:
        _notify("notebook_changed")
    return {"removed_alias": name, "notebook_cells_removed": n}


@mcp.tool()
def notebook_reorder(cell_id: str, new_index: int) -> dict:
    """Move a notebook cell to a new position (0-based)."""
    project = resolve_project()
    try:
        cell = notebook.reorder(project, cell_id, new_index)
    except CellNotFound:
        return {"error": f"no cell with id {cell_id!r}"}
    _notify("notebook_changed")
    return {"cell": cell, "new_index": new_index}


@mcp.tool()
def notebook_remove(cell_id: str) -> dict:
    """Remove a cell from the notebook. The underlying catalog entry is unaffected."""
    project = resolve_project()
    try:
        notebook.remove(project, cell_id)
    except CellNotFound:
        return {"error": f"no cell with id {cell_id!r}"}
    _notify("notebook_changed")
    return {"removed": cell_id}


@mcp.tool()
def notebook_edit_markdown(cell_id: str, markdown: str) -> dict:
    """Replace the markdown text shown above a cell."""
    project = resolve_project()
    try:
        cell = notebook.edit_markdown(project, cell_id, markdown)
    except CellNotFound:
        return {"error": f"no cell with id {cell_id!r}"}
    _notify("notebook_changed")
    return {"cell": cell}


@mcp.tool()
def catalog_chart(hash_or_alias: str, vega_spec: dict | str) -> dict:
    """Attach a Vega-Lite spec to a catalog entry.

    The spec is rendered above the table on the entry's detail page via
    vega-embed in the browser. The spec is stored as-is — no server-side
    validation. Use the entry's columns by name; vega-embed will load the
    parquet's data automatically (companion serves it as JSON to the chart).

    Args:
        hash_or_alias: Target entry. Aliases resolve to their latest hash.
        vega_spec: A Vega-Lite v5 JSON spec (dict or JSON string).

    Example spec::

        {"mark": "bar",
         "encoding": {"x": {"field": "region", "type": "nominal"},
                      "y": {"field": "total", "type": "quantitative"}}}
    """
    project = resolve_project()
    target_hash = hash_or_alias
    if not entry_dir(project, target_hash).exists():
        resolved = get_alias(project, hash_or_alias)
        if resolved is None:
            return {"error": f"no entry for {hash_or_alias!r}"}
        target_hash = resolved
    try:
        path = set_chart(project, target_hash, vega_spec)
    except ChartSpecError as exc:
        return {"error": str(exc)}
    _notify("chart_attached", content_hash=target_hash)
    return {"hash": target_hash, "spec_path": str(path)}


@mcp.tool()
def catalog_diff(name: str, va: int = -2, vb: int = -1) -> dict:
    """Diff two versions of an alias.

    Default compares the previous version to the current one (V_{n-1} vs V_n).
    Returns code/schema/stats/head/keyed diff summaries.

    Args:
        name: An existing alias.
        va, vb: 1-based version indices, or negative for from-end (-1 = latest).
    """
    project = resolve_project()
    hashes = history_for(project, name)
    if not hashes:
        return {"error": f"alias {name!r} has no history"}

    def resolve(v):
        if v < 0:
            v = len(hashes) + v + 1  # -1 -> last, -2 -> penultimate
        if v < 1 or v > len(hashes):
            return None
        return v, hashes[v - 1]

    a = resolve(va)
    b = resolve(vb)
    if a is None or b is None:
        return {"error": f"version out of range; alias has {len(hashes)} versions"}
    a_idx, a_hash = a
    b_idx, b_hash = b
    a_dir = entry_dir(project, a_hash)
    b_dir = entry_dir(project, b_hash)
    if not a_dir.exists() or not b_dir.exists():
        return {"error": "one or both entries are missing on disk"}

    diff = full_diff(a_dir, b_dir, a_label=f"V{a_idx}", b_label=f"V{b_idx}")
    return {
        "alias": name,
        "before": {"version": a_idx, "hash": a_hash},
        "after": {"version": b_idx, "hash": b_hash},
        "schema": diff["schema"],
        "stats": diff["stats"],
        "keyed_summary": (
            {k: v for k, v in diff["keyed"].items() if k != "table_html"}
            if diff["keyed"]
            else None
        ),
    }


@mcp.tool()
def catalog_add_summary_stat(name: str, source: str) -> dict:
    """Register a project-authored column summary stat.

    ``source`` must define a callable ``compute(col)`` that takes an ibis
    column expression and returns an ibis expression (scalar). The
    function is validated by dry-running it against a 1-row in-memory
    table before the file lands on disk — syntax / type / shape errors
    surface in the tool response, not later in the buckaroo log.

    The stat shows up alongside the built-in xorq column stats the next
    time a catalog entry's buckaroo session is loaded (clicking a
    different entry, or after a buckaroo restart). V1 does NOT hot-swap
    already-loaded sessions.

    ``source`` example::

        def compute(col):
            return (col.cast("string").contains("@").sum() / col.count())

    The function name MUST be ``compute``; the *filename* is ``<name>.py``
    under ``<project>/stats/``, and the file's stem is the stat's key in
    every column's stats panel.

    Args:
        name: filename stem (no ``.py``). Must be a valid python identifier.
        source: the file body. Must define ``compute(col) -> ibis_expr``.
    """
    project = resolve_project()
    try:
        path = write_stat(project, name, source)
    except StatSourceError as exc:
        return {"error": str(exc)}
    _notify("stats_changed", extra={"action": "add", "name": name})
    return {"name": name, "path": str(path)}


@mcp.tool()
def catalog_remove_summary_stat(name: str) -> dict:
    """Soft-delete a project-authored stat.

    Moves ``stats/<name>.py`` to ``stats/_disabled/<name>.py``. The
    ``_disabled/`` subdirectory is ignored by buckaroo's scanner (the
    ``_`` prefix is the convention) so the stat disappears from new
    sessions on the next load. To hard-delete, ``rm`` the file under
    ``stats/_disabled/`` manually.
    """
    project = resolve_project()
    new_path = remove_stat(project, name)
    if new_path is None:
        return {"error": f"no stat named {name!r}"}
    _notify("stats_changed", extra={"action": "remove", "name": name})
    return {"name": name, "moved_to": str(new_path)}


@mcp.tool()
def catalog_list_summary_stats() -> list[dict]:
    """List every project-authored summary stat (active + disabled).

    Returns ``[{name, path, source, disabled}]`` sorted with active stats
    first, then disabled (parked) ones.
    """
    project = resolve_project()
    return list_stats(project)


@mcp.tool()
def catalog_add_post_processing(name: str, source: str) -> dict:
    """Register a project-authored post-processing function.

    ``source`` must define a callable ``process(expr)`` that takes the
    table expression and returns either a transformed ibis expression or
    a pandas DataFrame. The function is validated by dry-running it
    against a small in-memory table before the file lands on disk —
    syntax / type / shape errors surface in the tool response, not later
    in the buckaroo log.

    The function shows up as a new option in the embed's "post
    processing" dropdown the next time a catalog entry's buckaroo
    session is loaded (clicking a different entry, revising an alias,
    or after a buckaroo restart). V1 does NOT hot-swap already-loaded
    sessions.

    ``source`` example — flag low-volume regions::

        def process(expr):
            return expr.filter(expr.n < 100)

    The function name MUST be ``process``; the *filename* is
    ``<name>.py`` under ``<project>/post_processing/``, and the file's
    stem is the option label in the dropdown.

    Args:
        name: filename stem (no ``.py``). Must be a valid python identifier.
        source: file body. Must define ``process(expr) -> ibis_expr | DataFrame``.
    """
    project = resolve_project()
    try:
        path = write_post_processing(project, name, source)
    except PostProcessingSourceError as exc:
        return {"error": str(exc)}
    _notify("post_processing_changed", extra={"action": "add", "name": name})
    return {"name": name, "path": str(path)}


@mcp.tool()
def catalog_remove_post_processing(name: str) -> dict:
    """Soft-delete a project-authored post-processing function.

    Moves ``post_processing/<name>.py`` to
    ``post_processing/_disabled/<name>.py``. The ``_disabled/``
    subdirectory is ignored by buckaroo's scanner so the option
    disappears from the dropdown on the next session load.
    """
    project = resolve_project()
    new_path = remove_post_processing(project, name)
    if new_path is None:
        return {"error": f"no post-processing named {name!r}"}
    _notify("post_processing_changed", extra={"action": "remove", "name": name})
    return {"name": name, "moved_to": str(new_path)}


@mcp.tool()
def catalog_list_post_processings() -> list[dict]:
    """List every project-authored post-processing function (active + disabled).

    Returns ``[{name, path, source, disabled}]`` sorted with active
    entries first, then disabled (parked) ones.
    """
    project = resolve_project()
    return list_post_processings(project)


@mcp.tool()
def catalog_run_post_processing(code: str, entry: str) -> dict:
    """Test a post-processing function against a real catalog entry's result.

    Use this to iterate on ``process(expr)`` code before committing it with
    ``catalog_add_post_processing``. The function is executed against the
    entry's stored ``result.parquet`` — you see real output rows, not the
    small dry-run memtable that ``catalog_add_post_processing`` validates
    against.

    Typical workflow::

        # 1. Draft the function.
        catalog_run_post_processing(
            code="def process(expr):\\n    return expr.filter(expr.n > 10)\\n",
            entry="shoe_sales",
        )
        # 2. Iterate until the preview looks right.
        # 3. Commit it.
        catalog_add_post_processing("high_volume", code)

    ``entry`` can be an alias name (e.g. ``"shoe_sales"``) or a raw content
    hash. The current alias revision is used when both exist.

    Args:
        code: Python source defining ``process(expr) -> ibis_expr | DataFrame``.
        entry: Alias name or content hash of the catalog entry to test against.

    Returns:
        dict with keys: ``entry`` (resolved hash), ``row_count``, ``columns``,
        ``preview`` (first 20 rows as list-of-dicts). On error, ``{"error": ...}``.
    """
    project = resolve_project()
    try:
        return run_post_processing(project, entry, code)
    except PostProcessingRunError as exc:
        return {"error": str(exc)}


@mcp.tool()
def catalog_export_marimo() -> dict:
    """Export the project's notebook to a Marimo Python notebook file.

    Converts the default notebook to a ``.py`` file that can be opened and
    run directly with ``marimo edit notebook_marimo.py``. Each notebook cell
    becomes a pair of Marimo cells: a markdown narrative cell and a code cell
    that re-runs the xorq expression and displays the result DataFrame.

    The file is written to ``<project_dir>/notebook_marimo.py`` and is also
    downloadable via the companion's ``/export/marimo`` route.

    Returns:
        dict with keys: ``path`` (absolute path of the written file),
        ``cells`` (number of notebook cells exported).
    """
    project = resolve_project()
    try:
        path = export_notebook_path(project)
    except Exception as exc:
        return {"error": str(exc)}
    from pydata_core.notebook import load as _load  # noqa: PLC0415
    n = len(_load(project).get("cells", []))
    return {"path": str(path), "cells": n}


@mcp.tool()
def catalog_list() -> list[dict]:
    """List every entry in the current project's catalog (most recent first).

    Each entry is annotated with `alias` and `version` if it currently
    corresponds to a named version. Scratch entries (no alias) are also
    included.
    """
    project = resolve_project()
    entries = list_entries(project)
    for e in entries:
        a = alias_for_hash(project, e["content_hash"])
        if a is not None:
            from pydata_core import version_of_hash

            info = version_of_hash(project, e["content_hash"])
            e["alias"] = a
            e["version"] = info[1] if info else 1
        else:
            e["alias"] = None
            e["version"] = None
    return entries


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PYDATA_LOG_LEVEL", "INFO"),
        format="[%(name)s] %(message)s",
    )
    mcp.run()


if __name__ == "__main__":
    main()
