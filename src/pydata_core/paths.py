"""Filesystem layout + active-project resolution.

Layout (per-project):

    ~/.pydata-app/
    ├── active_project                 # one-line plain text; source of truth
    ├── buckaroo_sessions.json         # global Buckaroo session map
    └── projects/<name>/
        ├── artifacts/                 # everything the system produces
        │   ├── catalog/entries/<hash>/
        │   ├── catalog/aliases.json, alias_history.json, chart_specs/
        │   ├── post_processing/<name>.py
        │   ├── stats/<name>.py
        │   ├── exports/...            # marimo .py, screenshots, CSVs
        │   └── errors.jsonl
        ├── data/                      # input parquets (fixtures or user)
        └── notebooks/notebook.json

The active project lives in a single one-line file at
``~/.pydata-app/active_project``. ``resolve_project()`` reads it on every
call; ``set_active_project()`` writes it atomically. ``PYDATA_PROJECT``
env is consulted only to seed the file on first resolution when nothing
is configured — once the file exists, env is ignored.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Roots
# ---------------------------------------------------------------------------


def pydata_home() -> Path:
    """Resolve PYDATA_HOME at call time so test fixtures can override via env."""
    return Path(os.environ.get("PYDATA_HOME", Path.home() / ".pydata-app"))


def projects_root() -> Path:
    return pydata_home() / "projects"


def active_project_file_path() -> Path:
    return pydata_home() / "active_project"


def buckaroo_sessions_path() -> Path:
    """Global Buckaroo session table (one file, all projects).

    Schema: ``{<content_hash>: {session_id, project, buckaroo_started_at}}``.
    Migrates to a Buckaroo-side enumeration endpoint once
    buckaroo-data/buckaroo#860 lands.
    """
    return pydata_home() / "buckaroo_sessions.json"


# ---------------------------------------------------------------------------
# Project name validation
# ---------------------------------------------------------------------------

# Lowercase + alphanum + hyphen + underscore, max 32 chars, must start with
# an alphanum. Matches Docker image / npm package / GitHub slug conventions
# and avoids macOS case-folding collisions.
PROJECT_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")

# Names blocked even if they otherwise match the regex (or trivially don't).
RESERVED_PROJECT_NAMES = frozenset({"active_project", "buckaroo_sessions", ".", "..", ""})


def validate_project_name(name: str) -> None:
    """Raise ValueError if *name* is not a legal project name.

    Rules: lowercase + alphanum + hyphen + underscore, max 32 chars,
    must start with an alphanum. Reserved names blocked.
    """
    if name in RESERVED_PROJECT_NAMES:
        raise ValueError(f"{name!r} is a reserved project name")
    if not isinstance(name, str) or not PROJECT_NAME_RE.match(name):
        raise ValueError(
            f"{name!r} is not a valid project name "
            r"(must match ^[a-z0-9][a-z0-9_-]{0,31}$)"
        )


# ---------------------------------------------------------------------------
# Project directories
# ---------------------------------------------------------------------------


def project_dir(name: str) -> Path:
    """Project root.

    Supports ``PYDATA_PROJECT_PATH`` for ``pydata serve <path>`` mode:
    if the override is set AND *name* matches the active project, returns
    the override path. Otherwise ``<projects_root>/<name>``.

    Serve mode is the narrow case where ``PYDATA_PROJECT`` env is still
    consulted at runtime — serving someone else's project tree shouldn't
    clobber the active-project file used by the main companion. Outside
    serve mode the file is the only source of truth (see ``resolve_project``).
    """
    override = os.environ.get("PYDATA_PROJECT_PATH")
    if override:
        active = os.environ.get("PYDATA_PROJECT") or _read_active_project_file()
        if active is not None and name == active:
            return Path(override)
    return projects_root() / name


def artifacts_dir(project: str) -> Path:
    return project_dir(project) / "artifacts"


def catalog_dir(project: str) -> Path:
    return artifacts_dir(project) / "catalog"


def entries_dir(project: str) -> Path:
    return catalog_dir(project) / "entries"


def entry_dir(project: str, content_hash: str) -> Path:
    return entries_dir(project) / content_hash


def result_cache_dir(project: str) -> Path:
    """Root for xorq's ParquetSnapshotCache of catalog-entry results.

    Materialised results are content-addressed parquet files written here by
    xorq, not the per-entry result.parquet — so a deleted cache file simply
    recomputes on next access.
    """
    return catalog_dir(project) / "result_cache"


def diff_stat_cache_root(project: str) -> Path:
    """Parent of all per-pair diff stat caches for a project."""
    return catalog_dir(project) / "diff_stat_cache"


def diff_stat_cache_dir(project: str, a_hash: str, b_hash: str) -> Path:
    """Buckaroo summary-stat cache for a comparison (diff) session.

    Keyed by the entry pair, not the joined data — Buckaroo stores only the
    small per-column summary stats here, so re-opening the same diff reuses
    them instead of recomputing over the full join.
    """
    return diff_stat_cache_root(project) / f"{a_hash[:12]}-{b_hash[:12]}"


def post_processing_dir(project: str) -> Path:
    return artifacts_dir(project) / "post_processing"


def stats_dir(project: str) -> Path:
    return artifacts_dir(project) / "stats"


def exports_dir(project: str) -> Path:
    return artifacts_dir(project) / "exports"


def errors_path(project: str) -> Path:
    return artifacts_dir(project) / "errors.jsonl"


def data_dir(project: str) -> Path:
    return project_dir(project) / "data"


def notebooks_dir(project: str) -> Path:
    return project_dir(project) / "notebooks"


# ---------------------------------------------------------------------------
# Active-project file: source of truth at runtime
# ---------------------------------------------------------------------------


def _read_active_project_file() -> str | None:
    p = active_project_file_path()
    if not p.exists():
        return None
    name = p.read_text().strip()
    return name or None


def _write_active_project_file(name: str) -> None:
    """Atomic write via tempfile + os.replace."""
    p = active_project_file_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp") if p.suffix else p.with_name(p.name + ".tmp")
    tmp.write_text(name + "\n")
    os.replace(tmp, p)


def set_active_project(name: str) -> str:
    """Validate, ensure the project exists on disk, write the file atomically.

    Raises ``ValueError`` on invalid name, ``FileNotFoundError`` if the
    project directory doesn't exist yet (callers should ``ensure_project``
    first, or use the higher-level ``/api/projects/new`` route).
    """
    validate_project_name(name)
    if not (projects_root() / name).is_dir():
        raise FileNotFoundError(f"project {name!r} not found at {projects_root() / name}")
    _write_active_project_file(name)
    return name


def resolve_project(explicit: str | None = None) -> str | None:
    """Return the currently-active project name, or None.

    Precedence:

    1. *explicit* argument (used by ``pydata <cmd> --project foo``).
    2. ``~/.pydata-app/active_project`` file.
    3. ``PYDATA_PROJECT`` env, **only as a one-shot seed** when the file
       is absent and the named project already exists on disk. The seed
       writes the file so subsequent calls don't re-read env.
    4. ``None`` — caller decides (typically: redirect to empty-state page).
    """
    if explicit:
        return explicit
    from_file = _read_active_project_file()
    if from_file is not None:
        return from_file
    from_env = os.environ.get("PYDATA_PROJECT")
    if from_env and (projects_root() / from_env).is_dir():
        _write_active_project_file(from_env)
        return from_env
    return None


# ---------------------------------------------------------------------------
# xorq catalog repo path (per-project)
# ---------------------------------------------------------------------------


def catalog_repo_path() -> Path:
    """Path the xorq CLI uses for its ``catalog`` git repo.

    Lives at ``<project>/artifacts/catalog/`` — same dir as the pydata
    catalog entries. ``xorq catalog init`` writes a ``.git/`` alongside
    them.

    ``PYDATA_CATALOG_REPO`` env overrides for tests or external repos.
    """
    override = os.environ.get("PYDATA_CATALOG_REPO")
    if override:
        return Path(override)
    project = resolve_project()
    if project is None:
        raise RuntimeError("catalog_repo_path called with no active project")
    return catalog_dir(project)


# ---------------------------------------------------------------------------
# Project lifecycle
# ---------------------------------------------------------------------------


def ensure_project(project: str) -> Path:
    """Create the project tree if absent; idempotent.

    Validates the name first; ``ensure_project("BadName")`` raises
    ``ValueError`` without touching disk. ``errors.jsonl`` is a file, not
    a dir — created lazily on first write by ``pydata_core.errors``.
    """
    validate_project_name(project)
    p = project_dir(project)
    (p / "artifacts" / "catalog" / "entries").mkdir(parents=True, exist_ok=True)
    (p / "artifacts" / "post_processing").mkdir(parents=True, exist_ok=True)
    (p / "artifacts" / "stats").mkdir(parents=True, exist_ok=True)
    (p / "artifacts" / "exports").mkdir(parents=True, exist_ok=True)
    (p / "data").mkdir(parents=True, exist_ok=True)
    (p / "notebooks").mkdir(parents=True, exist_ok=True)
    return p


def list_projects() -> list[str]:
    """Return alphabetically-sorted names of every project under projects_root.

    Skips entries that aren't directories or whose names fail validation
    (defensive against debris in ``projects/``).
    """
    root = projects_root()
    if not root.is_dir():
        return []
    out = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        try:
            validate_project_name(child.name)
        except ValueError:
            continue
        out.append(child.name)
    return out
