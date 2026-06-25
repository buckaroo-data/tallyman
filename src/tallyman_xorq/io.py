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

from tallyman_core import artifacts_dir, data_dir, entry_dir, get_alias, resolve_project


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


# Pinned polars parquet-write settings for the ordered-CSV intermediate. Held
# constant so the bytes — and therefore the snapshot digest — are reproducible
# run-to-run within an environment. A polars/library upgrade may change the
# bytes; rebuild-is-fine here per the project's single-user rule.
_CSV_PARQUET_WRITE = {
    "compression": "zstd",
    "compression_level": 3,
    "row_group_size": 122880,
    "statistics": True,
}

# ibis primitive -> polars dtype, for reading a CSV with an explicit schema.
# Covers the types the MCP documents for tallyman_read_csv; an unmapped type
# raises rather than silently inferring, so a schema mistake fails loudly.
_IBIS_TO_POLARS = {
    "int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
    "uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32", "uint64": "UInt64",
    "float32": "Float32", "float64": "Float64",
    "string": "String", "bool": "Boolean", "boolean": "Boolean",
    "date": "Date",
}


def _polars_overrides(schema):
    """Translate an ibis schema to polars ``schema_overrides`` dtypes."""
    import polars as pl

    overrides = {}
    for name, dtype in zip(schema.names, schema.types):
        key = str(dtype)
        if key.startswith("timestamp"):
            overrides[name] = pl.Datetime
            continue
        pl_name = _IBIS_TO_POLARS.get(key)
        if pl_name is None:
            raise ValueError(
                f"tallyman_read_csv: column {name!r} has ibis type {key!r}, which has no "
                f"polars mapping. Supported: {sorted(_IBIS_TO_POLARS)} (+ timestamp*)."
            )
        overrides[name] = getattr(pl, pl_name)
    return overrides


def _ordered_csv_parquet(path: str, schema) -> Path:
    """Materialise *path* to a row-order-stable parquet, keyed on the CSV's stat
    signature, and return its path (idempotent — skips the write if it exists).

    polars ``scan_csv -> with_row_index -> sink_parquet`` preserves source file
    order (unlike datafusion's parallel scan, whose row order is nondeterministic
    above the repartition threshold), so ``original_row_order`` is the true 0..N-1
    file sequence and the written bytes are reproducible. See
    ``plans/adr-result-digest-canonical-ordering.md``.
    """
    import hashlib

    import polars as pl

    src = Path(path)
    st = src.stat()
    schema_sig = repr(sorted(zip(schema.names, [str(t) for t in schema.types]))) if schema is not None else "none"
    key = hashlib.md5(  # noqa: S324 — naming key, not a security boundary
        f"{src.resolve()}|{st.st_mtime_ns}|{st.st_size}|{schema_sig}".encode()
    ).hexdigest()

    out_dir = artifacts_dir(resolve_project(None)) / "csv_ordered"
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"{key}.parquet"
    if target.exists():
        return target

    lf = pl.scan_csv(
        str(src),
        **({"schema_overrides": _polars_overrides(schema), "infer_schema_length": 0} if schema is not None else {}),
    ).with_row_index("original_row_order")
    if schema is not None:
        cols = list(schema.names)
    else:
        cols = [c for c in lf.collect_schema().names() if c != "original_row_order"]
    # original_row_order last (matches the prior mutate-appends-a-column shape)
    # and cast to int64 (with_row_index yields uint32) for the documented schema.
    lf = lf.select([*cols, pl.col("original_row_order").cast(pl.Int64)])

    tmp = target.with_suffix(".parquet.tmp")
    lf.sink_parquet(str(tmp), **_CSV_PARQUET_WRITE)
    tmp.replace(target)  # atomic — a crash mid-write never leaves a partial at `target`
    return target


def tallyman_read_csv(path: str, schema=None):
    """Read a CSV into a xorq expression with a stable ``original_row_order``.

    Use this instead of ``xo.deferred_read_csv`` for all CSV ingests. It adds a
    0-based ``original_row_order`` column holding the source file's true row
    sequence, which serves as the canonical total order for snapshot baking —
    making the entry's result digest byte-stable across builds.

    The ordering is produced by an order-preserving polars read
    (``scan_csv -> with_row_index``) materialised to a stat-addressed parquet,
    *not* by ``ibis.row_number()``: a bare datafusion ``ROW_NUMBER() OVER ()``
    numbers rows in the nondeterministic arrival order of a parallel CSV scan
    (any file over datafusion's ~10 MB repartition threshold), so it would not
    pin file order at all. See ``plans/adr-result-digest-canonical-ordering.md``.

    The returned expression is a ``deferred_read_parquet`` of that stable file
    with a top-level ``order_by("original_row_order")``, so it is classified
    snapshot-worthy and bakes a canonically-ordered parquet snapshot on build;
    the trailing sort also re-imposes the canonical order even if the parquet
    scan itself reparallelises.

    Args:
        path: Absolute path to the CSV file.
        schema: Optional ibis schema for the columns (same as deferred_read_csv).
            When omitted, polars infers types.
    """
    import xorq.api as xo

    parquet_path = _ordered_csv_parquet(path, schema)
    return xo.deferred_read_parquet(str(parquet_path)).order_by("original_row_order")


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
