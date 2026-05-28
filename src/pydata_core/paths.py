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


def catalog_repo_path() -> Path:
    """Path to the xorq-backed catalog git repo.

    Resolved at call time so tests can override via PYDATA_CATALOG_REPO.
    Defaults to <repo_root>/catalog/ (detected from the installed package
    location, which works for editable installs).
    """
    override = os.environ.get("PYDATA_CATALOG_REPO")
    if override:
        return Path(override)
    # pydata_core/__init__.py lives at src/pydata_core/__init__.py
    # → parents[0] = src/pydata_core, [1] = src, [2] = repo root
    import pydata_core as _pkg  # local import to avoid circular at module load
    return Path(_pkg.__file__).resolve().parents[2] / "catalog"


def ensure_project(project: str) -> Path:
    p = project_dir(project)
    (p / "catalog" / "entries").mkdir(parents=True, exist_ok=True)
    (p / "data").mkdir(parents=True, exist_ok=True)
    (p / "notebooks").mkdir(parents=True, exist_ok=True)
    return p
