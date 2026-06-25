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


def read_project_file(rel_path: str, project: str | None = None):
    """Load a raw data file from `<project>/data/<rel_path>` as a xorq expression.

    Use this to bring raw input files (parquet, csv, etc.) into the catalog's
    expression layer. This is the root of the dependency DAG — the file has no
    catalog hash, no recipe, no lineage entry. Everything built on top of it
    flows through tracked_expr_from_alias.

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
    if si.mode() == "cas":
        recorded = _reconstructing_source_digest(proj, rel_path)
        if recorded is not None:
            # Reconstruction (#115): this read_project_file is re-running an already-built
            # entry's recipe (expr.py), not authoring a new entry. Resolve to the FROZEN
            # .cas clone the entry was built from — named by the digest in its
            # manifest.sources — instead of re-digesting the live file, which may have
            # been edited in place since the build. Note that same frozen digest (the
            # source collector is independent of tracked_expr_from_alias's _RECONSTRUCTING-gated
            # parent capture, so both fire) so a child build reconstructing this entry
            # records the frozen digest and gc_cas keeps the clone alive.
            si.note_source(rel_path, recorded)
            return deferred_read_parquet(str(si.recon_cas_path(proj, path, recorded)))
    if si.mode() != "off":
        digest = si.digest_for(proj, path)
        si.note_source(rel_path, digest)
        if si.mode() == "cas":
            path = si.ensure_cas_path(proj, path, digest)
    return deferred_read_parquet(str(path))


def tallyman_read_csv(path: str, schema=None, **kwargs):
    """Read a CSV into a xorq expression with ``original_row_order`` injected.

    Use this instead of ``xo.deferred_read_csv`` for all CSV ingests. It adds
    a 0-based ``original_row_order`` column (the source file's row sequence)
    which serves as the canonical total order for snapshot baking — making
    the entry's result digest byte-stable across builds.

    Because ``original_row_order`` requires a ``RowNumber`` window op, entries
    built with this function are classified as snapshot-worthy and bake a
    sorted parquet snapshot on first build. Subsequent reads are fast
    (parquet scan, not CSV re-parse).

    Args:
        path: Absolute path to the CSV file.
        schema: Optional ibis schema for the columns (same as deferred_read_csv).
        **kwargs: Forwarded to ``xo.deferred_read_csv``.
    """
    import xorq.api as xo
    import xorq.vendor.ibis as ibis

    t = xo.deferred_read_csv(path, schema=schema, **kwargs)
    return t.mutate(original_row_order=ibis.row_number()).order_by("original_row_order")


def _reconstructing_source_digest(proj: str, rel_path: str) -> str | None:
    """The digest *rel_path* was built with, if a recipe for *proj* is being
    reconstructed on this call stack (#115); otherwise None.

    Reads the per-entry ``{rel_path: digest}`` map ``result_cache._recipe_expr``
    threads through ``_RECON_SOURCES``. Returns None outside reconstruction (the build
    path), when the reconstructing entry belongs to a different project, or when this
    source was not recorded — read_project_file then falls through to its live-file path.
    """
    from tallyman_xorq.result_cache import _RECON_SOURCES

    ctx = _RECON_SOURCES.get()
    if ctx is None:
        return None
    recon_proj, sources = ctx
    if recon_proj != proj:
        return None
    return sources.get(rel_path)


def tracked_expr_from_alias(alias: str, project: str | None = None):
    """Load a catalog entry by alias and record the parent edge in the manifest DAG.

    This is the standard way to chain catalog entries: build A, then build B
    from A using ``tracked_expr_from_alias("a")``. The call records A as a
    declared parent of B in ``manifest.parents``, which is how staleness
    propagation and the lineage graph are maintained.

    Accepts an alias only (not a content hash). Resolves the alias to its
    current head entry. Pass the alias name as it appears in ``catalog_list``.
    To read by hash without tracking, use ``pinned_expr_from_alias``.

    Returns the entry's expression on the in-process default backend — an
    expensive parent (Aggregate / Join / ...) resolves to a direct read of its
    baked result_cache snapshot, a cheap parent re-runs its recipe (pushdown
    makes that ~free). Either way the materialization detail is hidden from
    the caller.

    The parent edge is suppressed during reconstruction (when ``_RECONSTRUCTING``
    is True) so that re-running a child recipe to compute its expression doesn't
    accidentally record grandparents as direct parents of the entry under
    construction.

    Args:
        alias: A catalog alias (e.g. "shoe_sales"). Must be an alias, not a
            content hash — pass hashes to pinned_expr_from_alias instead.
        project: Project name override (defaults to active TALLYMAN_PROJECT).
    """
    from tallyman_xorq.result_cache import _RECONSTRUCTING, _resolve_noncyclic_hash, cached_result_expr

    proj = resolve_project(project)
    if entry_dir(proj, alias).exists():
        raise ProjectDataNotFound(
            f"{alias!r} is a content hash; tracked_expr_from_alias only accepts aliases. "
            "Use pinned_expr_from_alias for hash-based reads."
        )
    content_hash = get_alias(proj, alias)
    if content_hash is None or not entry_dir(proj, content_hash).exists():
        raise ProjectDataNotFound(f"catalog alias {alias!r} not found in project {proj!r}")
    content_hash = _resolve_noncyclic_hash(proj, alias, content_hash)
    if not _RECONSTRUCTING.get():
        from tallyman_xorq import parent_capture as pc

        pc.note_parent(content_hash, ref=alias, follow=True)
    return cached_result_expr(proj, content_hash)


def pinned_expr_from_alias(alias_or_hash: str, project: str | None = None):
    """Load a catalog entry by alias or content hash, recording a pinned (non-following) edge.

    Like tracked_expr_from_alias but pinned: the dependency edge is recorded in
    manifest.parents with follow=False, so recalc knows the child exists but will
    not advance it when the parent alias moves. Use this when you want to stay on
    a specific version of a parent rather than following alias changes.

    Accepts an alias OR a content hash. Unlike tracked_expr_from_alias (alias-only,
    follow=True), this records follow=False regardless of whether you pass an alias
    or a hash — the caller is explicitly opting out of alias-following.

    The parent edge is suppressed during reconstruction (same as tracked_expr_from_alias)
    so re-running a child's recipe doesn't accidentally write to the manifest.

    Args:
        alias_or_hash: An alias (e.g. "shoe_sales") or a content hash.
        project: Project name override (defaults to active TALLYMAN_PROJECT).
    """
    from tallyman_xorq.result_cache import _RECONSTRUCTING, _resolve_noncyclic_hash, cached_result_expr

    proj = resolve_project(project)
    is_hash = entry_dir(proj, alias_or_hash).exists()
    content_hash = alias_or_hash if is_hash else get_alias(proj, alias_or_hash)
    if content_hash is None or not entry_dir(proj, content_hash).exists():
        raise ProjectDataNotFound(f"catalog entry {alias_or_hash!r} not found in project {proj!r}")
    content_hash = _resolve_noncyclic_hash(proj, alias_or_hash, content_hash)
    if not _RECONSTRUCTING.get():
        from tallyman_xorq import parent_capture as pc

        pc.note_parent(content_hash, ref=alias_or_hash, follow=False)
    return cached_result_expr(proj, content_hash)
