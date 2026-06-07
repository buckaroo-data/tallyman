"""Project-aware data loading helpers.

Convention: every project owns its data under `<project_dir>/data/`. User code
references files by *relative* name. The current implementation still embeds
absolute paths in the xorq build (so the artifact isn't portable across
machines yet — see plan.md), but recording the relative path here lays the
groundwork for a future `tallyman pack` step that rewrites the build to be
machine-independent.
"""

from __future__ import annotations

from pathlib import Path

from tallyman_core import data_dir, entry_dir, get_alias, resolve_project


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
        raise ProjectDataNotFound(f"{rel_path!r} resolves outside {base} (got {candidate})") from exc
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


def from_catalog(alias_or_hash: str, project: str | None = None):
    """Load an existing catalog entry's result as a xorq expression.

    This is how you chain catalog entries: build A, then build B from A's result.
    The resulting build's `expr.yaml` references A's `result.parquet`, which
    `tallyman_xorq.lineage` uses to derive catalog-level parent links.

    Args:
        alias_or_hash: An alias (e.g. "shoe_sales") or a content hash. Aliases
            resolve to their *current* latest hash.
        project: Project name override (defaults to active TALLYMAN_PROJECT).
    """
    import xorq.api as xo

    proj = resolve_project(project)
    target = entry_dir(proj, alias_or_hash)
    if not target.exists():
        latest = get_alias(proj, alias_or_hash)
        if latest is None:
            raise ProjectDataNotFound(f"catalog entry {alias_or_hash!r} not found in project {proj!r}")
        target = entry_dir(proj, latest)
    parquet = target / "result.parquet"
    if not parquet.exists():
        raise ProjectDataNotFound(f"{parquet} not found")
    return xo.deferred_read_parquet(str(parquet))
