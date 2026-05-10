from __future__ import annotations

import os
from pathlib import Path

PYDATA_HOME = Path(os.environ.get("PYDATA_HOME", Path.home() / ".pydata-app"))
DEFAULT_PROJECT = "spike"


def projects_root() -> Path:
    return PYDATA_HOME / "projects"


def project_dir(name: str) -> Path:
    return projects_root() / name


def catalog_dir(project: str) -> Path:
    return project_dir(project) / "catalog"


def entries_dir(project: str) -> Path:
    return catalog_dir(project) / "entries"


def entry_dir(project: str, content_hash: str) -> Path:
    return entries_dir(project) / content_hash


def data_dir(project: str) -> Path:
    return project_dir(project) / "data"


def resolve_project(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    return os.environ.get("PYDATA_PROJECT", DEFAULT_PROJECT)


def ensure_project(project: str) -> Path:
    p = project_dir(project)
    (p / "catalog" / "entries").mkdir(parents=True, exist_ok=True)
    (p / "data").mkdir(parents=True, exist_ok=True)
    (p / "notebooks").mkdir(parents=True, exist_ok=True)
    return p
