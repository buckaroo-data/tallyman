"""Project-aware data loading helpers.

Convention: every project owns its data under `<project_dir>/data/`. User code
references files by *relative* name. The current implementation still embeds
absolute paths in the xorq build (so the artifact isn't portable across
machines yet — see plan.md), but recording the relative path here lays the
groundwork for a future `pydata pack` step that rewrites the build to be
machine-independent.
"""
from __future__ import annotations

from pathlib import Path

from pydata_core import data_dir, resolve_project


class ProjectDataNotFound(FileNotFoundError):
    pass


def project_path(rel_path: str, project: str | None = None) -> Path:
    """Resolve `rel_path` against the project's data dir. Raises if absent."""
    proj = resolve_project(project)
    base = data_dir(proj)
    candidate = (base / rel_path).resolve()
    # Defensive: keep resolution inside the project's data dir.
    try:
        candidate.relative_to(base.resolve())
    except ValueError as exc:
        raise ProjectDataNotFound(
            f"{rel_path!r} resolves outside {base} (got {candidate})"
        ) from exc
    if not candidate.is_file():
        raise ProjectDataNotFound(f"{candidate} not found")
    return candidate


def from_project(rel_path: str, project: str | None = None):
    """Load a parquet file from `<project>/data/<rel_path>` as a xorq expression.

    Use this in user code instead of `xo.deferred_read_parquet(absolute_path)`
    so the catalog entry records a project-relative reference.
    """
    import xorq.api as xo

    return xo.deferred_read_parquet(str(project_path(rel_path, project)))
