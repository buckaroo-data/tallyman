"""Notebook structure: an ordered list of cells anchored on aliases.

Each cell carries a stable cell_id (independent of the alias name, so
catalog_rename can update the alias without touching the notebook) plus
free-form markdown displayed above the code. A cell's *content* is always
the current latest version of its alias — there's no notebook-level
version pinning. Forensic versions stay in the catalog, not in the
notebook.

V0 has one default notebook per project at `notebooks/default.json`:

    {
      "cells": [
        {"cell_id": "abc123...", "alias": "shoe_sales", "markdown": "..."},
        ...
      ]
    }
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

from pydata_core.paths import ensure_project, project_dir


class CellNotFound(KeyError):
    pass


def _path(project: str, name: str = "default") -> Path:
    return project_dir(project) / "notebooks" / f"{name}.json"


def load(project: str, name: str = "default") -> dict:
    p = _path(project, name)
    if not p.exists():
        return {"cells": []}
    try:
        data = json.loads(p.read_text())
    except json.JSONDecodeError:
        return {"cells": []}
    data.setdefault("cells", [])
    return data


def _save(project: str, data: dict, name: str = "default") -> None:
    ensure_project(project)
    p = _path(project, name)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2))


def find_cell_by_alias(project: str, alias: str, name: str = "default") -> dict | None:
    for c in load(project, name)["cells"]:
        if c["alias"] == alias:
            return c
    return None


def append(project: str, alias: str, markdown: str | None = None, name: str = "default") -> dict:
    """Append a cell for `alias`. If a cell for this alias already exists,
    return the existing one unchanged (idempotent)."""
    data = load(project, name)
    for c in data["cells"]:
        if c["alias"] == alias:
            return c
    cell = {
        "cell_id": uuid.uuid4().hex[:12],
        "alias": alias,
        "markdown": markdown or "",
    }
    data["cells"].append(cell)
    _save(project, data, name)
    return cell


def remove(project: str, cell_id: str, name: str = "default") -> None:
    data = load(project, name)
    before = len(data["cells"])
    data["cells"] = [c for c in data["cells"] if c["cell_id"] != cell_id]
    if len(data["cells"]) == before:
        raise CellNotFound(cell_id)
    _save(project, data, name)


def reorder(project: str, cell_id: str, new_index: int, name: str = "default") -> dict:
    data = load(project, name)
    indices = [i for i, c in enumerate(data["cells"]) if c["cell_id"] == cell_id]
    if not indices:
        raise CellNotFound(cell_id)
    [idx] = indices
    cell = data["cells"].pop(idx)
    new_index = max(0, min(new_index, len(data["cells"])))
    data["cells"].insert(new_index, cell)
    _save(project, data, name)
    return cell


def edit_markdown(project: str, cell_id: str, markdown: str, name: str = "default") -> dict:
    data = load(project, name)
    for c in data["cells"]:
        if c["cell_id"] == cell_id:
            c["markdown"] = markdown
            _save(project, data, name)
            return c
    raise CellNotFound(cell_id)


def rename_alias_in_cells(project: str, old: str, new: str, name: str = "default") -> int:
    """Keep notebook in sync when an alias is renamed in the catalog."""
    data = load(project, name)
    n = 0
    for c in data["cells"]:
        if c["alias"] == old:
            c["alias"] = new
            n += 1
    if n:
        _save(project, data, name)
    return n


def remove_alias_from_cells(project: str, alias: str, name: str = "default") -> int:
    """Drop any cell whose alias matches (called when an alias is deleted)."""
    data = load(project, name)
    before = len(data["cells"])
    data["cells"] = [c for c in data["cells"] if c["alias"] != alias]
    removed = before - len(data["cells"])
    if removed:
        _save(project, data, name)
    return removed
