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

    Source-content identity (see tallyman_xorq.source_identity): in `cas`
    mode the read goes through a content-addressed clone so the path xorq
    hashes embeds the content digest; in `salt` mode the digest is recorded
    for build_and_persist to mix into the entry hash; `off` is the plain
    path read.
    """
    import xorq.api as xo

    from tallyman_xorq import source_identity as si

    proj = resolve_project(project)
    path = project_path(rel_path, proj)
    if si.mode() != "off":
        digest = si.digest_for(proj, path)
        si.note_source(rel_path, digest)
        if si.mode() == "cas":
            path = si.ensure_cas_path(proj, path, digest)
    return xo.deferred_read_parquet(str(path))


def from_catalog(alias_or_hash: str, project: str | None = None):
    """Load an existing catalog entry's result as a xorq expression.

    This is how you chain catalog entries: build A, then build B from A's result.
    Returns the entry's *expression*, composed at the expression level rather
    than read back from a materialised ``result.parquet`` (#73). For an
    expensive parent (Aggregate / Join / …) that expression resolves its own
    ``result_cache`` snapshot, so the parent's work is computed once and shared;
    a cheap parent recomputes (pushdown makes it ~free). Either way the child no
    longer depends on the parent having a ``result.parquet`` on disk.

    Because the child's build now serialises the parent's own source reads
    (wrapped in a cache node) rather than a path to the parent entry, the
    inter-entry catalog DAG (``tallyman_xorq.lineage.catalog_parents``) no longer
    recovers a parent edge from this. Cross-entry lineage is deferred to a future
    semantic-dependency mechanism — see #73.

    Args:
        alias_or_hash: An alias (e.g. "shoe_sales") or a content hash. Aliases
            resolve to their *current* latest hash.
        project: Project name override (defaults to active TALLYMAN_PROJECT).
    """
    from tallyman_xorq.result_cache import cached_result_expr

    proj = resolve_project(project)
    content_hash = alias_or_hash if entry_dir(proj, alias_or_hash).exists() else get_alias(proj, alias_or_hash)
    if content_hash is None or not entry_dir(proj, content_hash).exists():
        raise ProjectDataNotFound(f"catalog entry {alias_or_hash!r} not found in project {proj!r}")
    return cached_result_expr(proj, content_hash)
