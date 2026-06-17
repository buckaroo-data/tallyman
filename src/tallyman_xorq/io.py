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
    from xorq.expr.api import deferred_read_parquet

    from tallyman_xorq import source_identity as si

    proj = resolve_project(project)
    path = project_path(rel_path, proj)
    if si.mode() != "off":
        digest = si.digest_for(proj, path)
        si.note_source(rel_path, digest)
        if si.mode() == "cas":
            path = si.ensure_cas_path(proj, path, digest)
    return deferred_read_parquet(str(path))


def from_catalog(alias_or_hash: str, project: str | None = None):
    """Load an existing catalog entry's result as a xorq expression.

    This is how you chain catalog entries: build A, then build B from A's result.
    Returns the entry's *expression* on the in-process default backend (#73/#75),
    not a read of a materialised entry ``result.parquet``. An expensive parent
    (Aggregate / Join / …) resolves to a direct read of its baked ``result_cache``
    snapshot, so its work is computed once and shared; a cheap parent re-runs its
    recipe (pushdown makes that ~free). Crucially both root on the *same* default
    backend, so combining two ``from_catalog`` parents — or a ``from_catalog`` and
    a ``from_project`` — in one ``union`` / ``join`` resolves to a single backend
    rather than raising "Multiple backends found" (#75). Either way the child no
    longer depends on the parent having a ``result.parquet`` on disk.

    Because the child's build serialises the parent's source reads (cheap) or a
    read of the parent's baked snapshot (expensive) rather than a path to the
    parent entry, the inter-entry edge no longer survives in the child's
    ``expr.yaml``. Instead this resolves the parent's build-time content hash and
    records it (with the original ``ref`` and ``follow`` read-intent) via
    ``parent_capture.note_parent``; ``build_and_persist`` persists it into
    ``manifest.parents`` so ``tallyman_xorq.lineage.catalog_parents`` recovers the
    DAG edge (#84).

    Args:
        alias_or_hash: An alias (e.g. "shoe_sales") or a content hash. Aliases
            resolve to their *current* latest hash.
        project: Project name override (defaults to active TALLYMAN_PROJECT).
    """
    from tallyman_xorq.result_cache import _RECONSTRUCTING, _resolve_noncyclic_hash, cached_result_expr

    proj = resolve_project(project)
    # An argument that names an existing entry dir is a literal hash (a pin);
    # otherwise it is an alias to follow.
    is_hash = entry_dir(proj, alias_or_hash).exists()
    content_hash = alias_or_hash if is_hash else get_alias(proj, alias_or_hash)
    if content_hash is None or not entry_dir(proj, content_hash).exists():
        raise ProjectDataNotFound(f"catalog entry {alias_or_hash!r} not found in project {proj!r}")
    # A recipe that reads from_catalog of its own (now-current) alias would
    # resolve to itself and recurse forever; step back to the build-time parent (#74).
    content_hash = _resolve_noncyclic_hash(proj, alias_or_hash, content_hash)
    # Record the cross-entry edge for the manifest DAG (#84), but only for the
    # entry under construction: composing a parent reconstructs its recipe, which
    # re-runs that parent's own from_catalog calls. Those resolve with
    # _RECONSTRUCTING non-empty, so gating on it keeps grandparents out of the
    # child's direct-parent list.
    if not _RECONSTRUCTING.get():
        from tallyman_xorq import parent_capture as pc

        pc.note_parent(content_hash, ref=alias_or_hash, follow=not is_hash)
    return cached_result_expr(proj, content_hash)
