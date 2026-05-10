from __future__ import annotations

import os
from pathlib import Path

DEFAULT_PROJECT = "spike"


def pydata_home() -> Path:
    """Resolve PYDATA_HOME at call time so test fixtures can override via env."""
    return Path(os.environ.get("PYDATA_HOME", Path.home() / ".pydata-app"))


def projects_root() -> Path:
    return pydata_home() / "projects"


def project_dir(name: str) -> Path:
    """Resolve the project directory for `name`.

    Standard layout is `<PYDATA_HOME>/projects/<name>/`. For the `pydata serve`
    use case (where a colleague's project tree is sitting in some arbitrary
    location), PYDATA_PROJECT_PATH can override this — but only for the
    project named by PYDATA_PROJECT, since override-by-name doesn't compose
    with multi-project queries.
    """
    override = os.environ.get("PYDATA_PROJECT_PATH")
    active = os.environ.get("PYDATA_PROJECT", DEFAULT_PROJECT)
    if override and name == active:
        return Path(override)
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
