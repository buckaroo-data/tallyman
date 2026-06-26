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


def _ts_time_unit(scale: int | None) -> str:
    """ibis timestamp scale (sub-second digits) -> a polars Datetime time_unit.

    polars supports only ``ms``/``us``/``ns``; ibis scale is 0..9. Round each ibis
    scale up to the nearest representable polars unit (#145).
    """
    if scale is None:
        return "us"
    if scale <= 3:
        return "ms"
    if scale <= 6:
        return "us"
    return "ns"


def _polars_dtype(dtype):
    """Translate one ibis dtype to a concrete polars dtype.

    Inspects the dtype *object* (predicates + attributes) rather than its string
    form, so parametric and non-nullable types map correctly: ``decimal(p, s)``
    keeps its precision/scale (#144), ``timestamp`` keeps tz + sub-µs precision
    (#145), and a non-nullable ``!int64`` strips the marker before the primitive
    lookup (#144 — a CSV column is nullable regardless of the schema's intent).
    An unmapped type still raises loudly, so a schema mistake fails fast.
    """
    import polars as pl

    if dtype.is_timestamp():
        return pl.Datetime(_ts_time_unit(dtype.scale), time_zone=dtype.timezone)
    if dtype.is_decimal():
        return pl.Decimal(dtype.precision, dtype.scale if dtype.scale is not None else 0)
    if dtype.is_time():
        return pl.Time
    if dtype.is_date():
        return pl.Date
    key = str(dtype).lstrip("!")  # drop the non-nullable marker before the primitive lookup
    pl_name = _IBIS_TO_POLARS.get(key)
    if pl_name is None:
        raise ValueError(
            f"tallyman_read_csv: column type {key!r} has no polars mapping. Supported: "
            f"{sorted(_IBIS_TO_POLARS)} (+ date, time, decimal[(p, s)], timestamp[(scale[, tz])])."
        )
    return getattr(pl, pl_name)


def _polars_overrides(schema):
    """Translate an ibis schema to polars ``schema_overrides`` dtypes (by name)."""
    return {name: _polars_dtype(dtype) for name, dtype in zip(schema.names, schema.types)}


def _csv_ordered_dir() -> Path:
    """Project-independent home for the ordered-CSV intermediates.

    Keyed on the absolute CSV path (+ schema + reader options), so the same file
    resolves to the same intermediate regardless of which project is active —
    unlike a per-project artifacts dir, which makes a reconstruction running
    under a *different* active project miss the cache and re-read the CSV (#9).
    Per-test isolation still holds: ``TALLYMAN_HOME`` points at a tmp dir.
    """
    from tallyman_core.paths import tallyman_home

    out_dir = tallyman_home() / "csv_ordered"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _ordered_csv_key(path: str, schema, scan_kwargs: dict) -> str:
    """Stable, CSV-stat-independent key for the ordered intermediate.

    Derived from the absolute path, the schema, and the reader options only —
    NOT the CSV's mtime/size — so a reconstruction can resolve the intermediate
    without touching (or even stat-ing) the source CSV. Freshness is a separate,
    best-effort mtime sidecar (see ``_ordered_csv_parquet``).
    """
    import hashlib

    schema_sig = repr(sorted(zip(schema.names, [str(t) for t in schema.types]))) if schema is not None else "none"
    kwargs_sig = repr(sorted((k, repr(v)) for k, v in scan_kwargs.items()))
    raw = f"{Path(path).resolve()}|{schema_sig}|{kwargs_sig}"
    return hashlib.md5(raw.encode()).hexdigest()  # noqa: S324 — cache key, not a security boundary


def _ordered_csv_parquet(path: str, schema, scan_kwargs: dict) -> Path:
    """Materialise *path* to a row-order-stable parquet and return its path.

    The CSV is read exactly once — at first ingest (and again only if its mtime
    changes). The intermediate is addressed by (absolute path, schema, reader
    options), not by the CSV's stat, so a faithful reconstruction resolves it
    without re-reading or even stat-ing the CSV; if the source has since been
    moved or deleted the existing intermediate is still served rather than
    raising (#6). A sidecar records the *source* CSV's mtime so a changed CSV
    re-materialises (#8 — mtime invalidates; bytes/size are unnecessary). The
    sidecar tracks the source's mtime, never a derived parquet's, so the
    result-digest / snapshot bake can never bump it into a re-ingest loop.

    polars ``scan_csv -> with_row_index -> sink_parquet`` preserves source file
    order (unlike datafusion's parallel scan, whose row order is nondeterministic
    above the repartition threshold), so ``original_row_order`` is the true
    0..N-1 file sequence and the bytes are reproducible. See
    ``plans/adr-result-digest-canonical-ordering.md``.
    """
    import os
    import uuid

    import polars as pl

    src = Path(path)
    out_dir = _csv_ordered_dir()
    key = _ordered_csv_key(path, schema, scan_kwargs)
    target = out_dir / f"{key}.parquet"
    stamp = out_dir / f"{key}.mtime"

    if target.exists():
        try:
            current = str(src.stat().st_mtime_ns)
        except OSError:
            return target  # source gone — the baked intermediate IS the source of truth (#6)
        try:
            recorded = stamp.read_text()
        except OSError:
            recorded = None
        if recorded == current:
            return target  # unchanged source — reuse, never re-read the CSV (#6)
        # mtime changed → fall through and re-materialise (#8)

    st = src.stat()  # the single point that touches the CSV; it must be present here
    overrides: dict = {}
    if schema is not None:
        overrides = {"schema_overrides": _polars_overrides(schema), "infer_schema_length": 0}
    lf = pl.scan_csv(str(src), **{**overrides, **scan_kwargs}).with_row_index("original_row_order")
    if schema is not None:
        cols = list(schema.names)
    else:
        cols = [c for c in lf.collect_schema().names() if c != "original_row_order"]
    # original_row_order last (matches the prior mutate-appends-a-column shape)
    # and cast to int64 (with_row_index yields uint32) for the documented schema.
    lf = lf.select([*cols, pl.col("original_row_order").cast(pl.Int64)])

    # Unique temp name (not a fixed {key}.parquet.tmp) so two builds writing the
    # same key concurrently can't corrupt each other's partial file (#7); the
    # rename is atomic and last-writer-wins on identical bytes.
    tmp = out_dir / f"{key}.{uuid.uuid4().hex}.tmp"
    try:
        lf.sink_parquet(str(tmp), **_CSV_PARQUET_WRITE)
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    stamp.write_text(str(st.st_mtime_ns))
    return target


def tallyman_read_csv(path: str, schema=None, **kwargs):
    """Read a CSV into a xorq expression with a stable ``original_row_order``.

    Use this instead of ``xo.deferred_read_csv`` for all CSV ingests. It adds a
    0-based ``original_row_order`` column holding the source file's true row
    sequence, which serves as the canonical total order for snapshot baking —
    making the entry's result digest byte-stable across builds.

    The CSV is read exactly once, at ingest: an order-preserving polars read
    (``scan_csv -> with_row_index``) materialises it to a parquet intermediate,
    and the returned expression is a ``deferred_read_parquet`` of that file with
    a top-level ``order_by("original_row_order")``. All downstream computation
    (the recipe, the snapshot bake, the result digest) runs on the parquet, never
    the CSV. polars is used rather than ``ibis.row_number()`` because a bare
    datafusion ``ROW_NUMBER() OVER ()`` numbers rows in the nondeterministic
    arrival order of a parallel CSV scan (any file over datafusion's ~10 MB
    repartition threshold), so it would not pin file order at all. See
    ``plans/adr-result-digest-canonical-ordering.md``.

    Args:
        path: Absolute path to the CSV file.
        schema: Optional ibis schema for the columns (same as deferred_read_csv).
            When omitted, polars infers types.
        **kwargs: Forwarded to ``polars.scan_csv`` — reader options such as
            ``separator``, ``skip_rows``, ``null_values``, ``quote_char``,
            ``has_header``, ``encoding``. They participate in the intermediate's
            cache key, so changing one re-ingests.
    """
    import xorq.api as xo

    parquet_path = _ordered_csv_parquet(path, schema, kwargs)
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
