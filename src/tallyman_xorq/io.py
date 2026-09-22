"""Project-aware data loading helpers.

Convention: every project owns its data under `<project_dir>/data/`. User code
references files by *relative* name. The build rewrites absolute filesystem
paths to `${TALLYMAN_PROJECT_ROOT}` placeholders on write and expands them on
load, so recording the relative intent here is what makes the artifact portable
across machines (and packable via `tallyman pack`; see docs/architecture.md).
"""

from __future__ import annotations

from pathlib import Path

from tallyman_core import data_dir, entry_dir, get_alias, resolve_project
from tallyman_xorq.row_order import ROW_ORDER


class ProjectDataNotFound(FileNotFoundError):
    pass


# Set this to run recipes that still read a file directly, while the test corpus is rewritten onto imported source
# aliases (ADR-011 stage 2). Scaffolding, with an expiry: it goes with the rewrite, and nothing in production sets it.
_LEGACY_READS_ENV = "TALLYMAN_LEGACY_FILE_READS"


def _source_entry_read(project: str, fn: str, path: str):
    """The expression a raw read resolves to, or None when the caller must be refused (ADR-011 D2).

    A file enters the catalog by an explicit import and by nothing else, so a raw read is a build error in an
    authored recipe. It survives in exactly two places: the recipe the importer generates for a source entry, where
    it resolves to that entry's own snapshot, and the reconstruction of an entry built before the import path
    existed, where the caller resolves it from the digest the manifest recorded.
    """
    import os

    from tallyman_xorq.source_import import source_entry_context

    content_hash = source_entry_context(project)
    if content_hash is not None:
        from xorq.expr.api import deferred_read_parquet

        from tallyman_xorq.materialize import snapshot_path

        return deferred_read_parquet(str(snapshot_path(project, content_hash)))
    if _reconstructing_source_digest(project, _relative_to_data(project, Path(path))) is not None:
        return None  # a pre-ADR-011 entry replaying its recipe; the caller resolves it from the recorded digest
    if os.environ.get(_LEGACY_READS_ENV):
        return None
    from tallyman_xorq.build import BuildError

    raise BuildError(
        f"{fn}({path!r}) reads a file tallyman does not own, which is not allowed in a recipe: the entry's rows "
        "would depend on a file anyone can edit or delete, and nothing would record which bytes it was built from. "
        f"Import the file first — catalog_import_source({path!r}, '<alias>') — and then read it in the recipe with "
        "tracked_expr_from_alias('<alias>'), which follows the alias, or pinned_expr_from_alias('<alias>-v<N>') to "
        "pin one version of it."
    )


def project_path(rel_path: str, project: str | None = None, must_exist: bool = True) -> Path:
    """Resolve `rel_path` against the project's data dir. Raises if absent.

    Pass ``must_exist=False`` for digest-pinned reconstruction: it serves the frozen
    ``.cas`` clone (``recon_cas_path``) and must not require the live source to still
    be present. The traversal guard still applies regardless.
    """
    proj = resolve_project(project)
    base = data_dir(proj)
    candidate = (base / rel_path).resolve()
    # Defensive: keep resolution inside the project's data dir.
    try:
        candidate.relative_to(base.resolve())
    except ValueError as exc:
        raise ProjectDataNotFound(f"{rel_path!r} resolves outside {base} (got {candidate})") from exc
    if must_exist and not candidate.is_file():
        raise ProjectDataNotFound(f"{candidate} not found")
    return candidate


def read_project_file(rel_path: str, project: str | None = None):
    """Load a raw data file from `<project>/data/<rel_path>` as a xorq expression.

    NOT for an authored recipe (ADR-011 D2): a file enters the catalog only through
    `catalog_import_source` / `update_and_depend`, and a recipe reads the resulting
    source alias with `tracked_expr_from_alias`. Calling this in an authored recipe is
    a build error naming the import. It survives here for the recipe the importer
    generates for a source entry — where it resolves to that entry's own snapshot —
    and for reconstructing an entry built before the import path existed.

    The file is never read directly (ADR-008 D2). It goes through source
    identity (tallyman_xorq.source_identity): its content digest is taken, in
    `cas` mode it is cloned to `data/.cas/<digest><suffix>`, and in `salt` mode
    the digest is recorded for build_and_persist to mix into the entry hash.
    Then an *ordered copy* of it is written under the project's `compute_cache/`
    (tallyman_xorq.ordered_copy): the same rows in file order plus a last column,
    `__row_order`, `0..N-1`, which pages sort by. The returned expression is a
    plain read of that copy, so the entry's hash covers the source's content
    (the copy's name is a function of the digest) and editing the file forks it.
    """
    from xorq.expr.api import deferred_read_parquet

    from tallyman_xorq import ordered_copy as oc
    from tallyman_xorq import source_identity as si

    proj = resolve_project(project)
    pinned = _source_entry_read(proj, "read_project_file", rel_path)
    if pinned is not None:
        return pinned
    reader = oc.parquet_reader()
    recorded = _reconstructing_source_digest(proj, rel_path)
    if recorded is not None:
        # Reconstruction (#115): this read_project_file is re-running an already-built entry's recipe (expr.py), not
        # authoring a new entry. Resolve to the copy of the bytes the entry was BUILT from, named by the digest in its
        # manifest.sources, instead of re-digesting the live file, which may have been edited in place since. Note
        # that same frozen digest so a child build reconstructing this entry records it.
        si.note_source(rel_path, recorded)
        return deferred_read_parquet(str(oc.existing_ordered_copy(proj, digest=recorded, reader=reader)))
    path = project_path(rel_path, proj)
    digest = si.digest_for(proj, path)
    if si.mode() != "off":
        si.note_source(rel_path, digest)
    source = si.ensure_cas_path(proj, path, digest) if si.mode() == "cas" else path
    copy = oc.ensure_ordered_copy(proj, source, digest=digest, rel=rel_path, reader=reader)
    return deferred_read_parquet(str(copy))


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
    form, so parametric and non-nullable types map correctly: ``timestamp`` keeps
    tz + sub-µs precision (#145), and a non-nullable ``!int64`` strips the marker
    before the primitive lookup (#144 — a CSV column is nullable regardless of the
    schema's intent). ``decimal`` reads as ``float64`` for now — exact-decimal
    parsing is deferred to a future UI-driven buckaroo autoclean step (#150).
    An unmapped type still raises loudly, so a schema mistake fails fast.
    """
    import polars as pl

    if dtype.is_timestamp():
        return pl.Datetime(_ts_time_unit(dtype.scale), time_zone=dtype.timezone)
    if dtype.is_decimal():
        return pl.Float64  # exact decimal deferred to a UI autoclean step (#150)
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


# Rows polars samples to infer an unpinned column's type. The cheap first try;
# the #143 escalation ladder widens it on a parse failure.
_DEFAULT_INFER = 100
_INFER = "infer"  # dtype token: keep a column's name/position but infer its type
_REST = "&rest"  # wildcard closure: the remaining tail columns (ADR D3)
_BEGIN = "&begin"  # wildcard closure: the leading head columns (specced, not yet implemented)

# scan_csv options tallyman_read_csv owns and must not accept as pass-through
# **kwargs: every internal scan_csv call hardcodes infer_schema_length (the
# escalation ladder) and schema_overrides (derived from schema=), so forwarding
# either collides ("multiple values for keyword argument") or silently bypasses
# the schema system. Rejected up front with a message steering to schema=.
_RESERVED_SCAN_KWARGS = ("infer_schema_length", "schema_overrides")


def _spec_sig(spec) -> str:
    """A stable string for the cache key, covering every schema-spec shape.

    Order matters for the positional tuple form (it binds by index) so it is not
    sorted; the by-name forms are sorted (binding is order-independent).
    """
    if spec is None:
        return "none"
    if isinstance(spec, (tuple, list)):
        return "pos:" + repr([(str(n), str(d)) for n, d in spec])
    if hasattr(spec, "names") and hasattr(spec, "types"):  # ibis schema
        return "name:" + repr(sorted((n, str(t)) for n, t in zip(spec.names, spec.types)))
    if isinstance(spec, dict):
        return "name:" + repr(sorted((str(k), str(v)) for k, v in spec.items()))
    raise ValueError(f"tallyman_read_csv: unsupported schema spec type {type(spec).__name__!r}.")


def _resolve_spec_dtype(value):
    """A spec dtype value -> a polars dtype, or None for the ``"infer"`` token."""
    if isinstance(value, str) and value == _INFER:
        return None
    import xorq.vendor.ibis as ibis

    return _polars_dtype(ibis.dtype(value) if isinstance(value, str) else value)


def _spec_pairs(spec):
    """Normalise a by-name spec (ibis schema or plain dict) to a list of (name, dtype-value)."""
    if hasattr(spec, "names") and hasattr(spec, "types"):
        return list(zip(spec.names, spec.types))
    return list(spec.items())


def _normalize_schema(spec, header: list[str], *, reserved: tuple[str, ...] = ()) -> dict:
    """Resolve a schema spec against the CSV *header* into a polars parse plan.

    *header* is the CSV's full header. *reserved* names tallyman-managed columns
    (currently ``__row_order``) that the caller must not spec: they are
    excluded from the columns the spec has to cover, but kept in the diagnostics
    so an error message matches the file the user is looking at rather than a
    silently reserved-stripped subset. Threading ``reserved`` in — rather than
    pre-stripping the header at the call site — keeps the totality check, the
    diagnostics, and positional binding all reading one consistent header.

    Returns ``{overrides, rename, out_names, all_pinned}``:
      - ``overrides``  — ``{header_name: polars_dtype}`` for pinned columns, keyed by the
        name polars reads (positional renames are applied *after* the read).
      - ``rename``     — ``{header_name: output_name}`` where they differ (positional only).
      - ``out_names``  — output column names in column order (the final ``select``).
      - ``all_pinned`` — True if no column is inferred (then ``infer_schema_length=0``).

    Raises the actionable ValueError on a by-name miss, a non-total spec, an
    over-long positional spec, a by-name spec that names a reserved column, or a
    positional spec whose reserved column is not the trailing column — these are
    the #143 error-contract surfaces.
    """
    reserved = tuple(r for r in reserved if r in header)
    speccable = [h for h in header if h not in reserved]
    reserved_hint = ""
    if reserved:
        names = ", ".join(repr(r) for r in reserved)
        reserved_hint = (
            f" (the file also has tallyman's reserved column {names}, which tallyman "
            "manages automatically — leave it out of the schema.)"
        )

    overrides: dict = {}
    rename: dict = {}
    inferred = False

    if isinstance(spec, (tuple, list)):  # ---------------- bind BY POSITION ----------------
        cells = [(str(n), d) for n, d in spec]
        if cells and cells[0][0] == _BEGIN:
            raise ValueError(
                "tallyman_read_csv: '&begin' is specced but not yet implemented (ADR D3); use explicit "
                "leading cells plus a trailing ('&rest', ...) instead."
            )
        rest_dtype = None
        if cells and cells[-1][0] == _REST:
            rest_dtype = cells[-1][1]
            cells = cells[:-1]
        if any(n in (_REST, _BEGIN) for n, _ in cells):
            raise ValueError("tallyman_read_csv: '&rest' must be the last cell and '&begin' the first.")
        # Positional cells bind by physical column position. Excluding a reserved column
        # that is not the trailing column would silently shift every later cell onto the
        # wrong data column, so require the reserved column(s) to be a trailing suffix
        # (header[:len(speccable)] == speccable); otherwise steer to a by-name schema.
        if reserved and header[: len(speccable)] != speccable:
            raise ValueError(
                f"tallyman_read_csv: a positional schema cannot bind {list(header)} — its reserved "
                f"column {list(reserved)} is not the last column, and positional cells bind by physical "
                "column position, so excluding it would silently rebind later cells onto the wrong data "
                "column. Use a by-name schema (a dict or ibis schema keyed on the header names) instead."
            )
        if len(cells) > len(speccable):
            raise ValueError(
                f"tallyman_read_csv: positional schema has {len(cells)} columns but the CSV header has "
                f"{len(speccable)} {list(speccable)}.{reserved_hint}"
            )
        if rest_dtype is None and len(cells) != len(speccable):
            uncovered = list(speccable[len(cells):])
            raise ValueError(
                f"tallyman_read_csv: schema is not total — it leaves column(s) {uncovered} unspecified. "
                'Name every column, or close the spec with ("&rest", "infer").'
            )
        out_names = []
        for i, (out_name, dt) in enumerate(cells):
            orig = speccable[i]
            out_names.append(out_name)
            if out_name != orig:
                rename[orig] = out_name
            pl_dt = _resolve_spec_dtype(dt)
            if pl_dt is None:
                inferred = True
            else:
                overrides[orig] = pl_dt
        for orig in speccable[len(cells):]:  # the &rest tail keeps its header name
            out_names.append(orig)
            pl_dt = _resolve_spec_dtype(rest_dtype)
            if pl_dt is None:
                inferred = True
            else:
                overrides[orig] = pl_dt
        return {"overrides": overrides, "rename": rename, "out_names": out_names, "all_pinned": not inferred}

    # ---------------- bind BY NAME (ibis schema or dict) ----------------
    rest_dtype = None
    named: dict = {}
    for name, dt in _spec_pairs(spec):
        if str(name) == _BEGIN:
            raise ValueError("tallyman_read_csv: '&begin' is positional; it is not valid in a by-name schema.")
        if str(name) == _REST:
            rest_dtype = dt
            continue
        named[str(name)] = dt
    named_reserved = [n for n in named if n in reserved]
    if named_reserved:
        raise ValueError(
            f"tallyman_read_csv: schema names tallyman's reserved column {sorted(named_reserved)}, which "
            "tallyman manages automatically — remove it from the schema (it is added and ordered for you)."
        )
    missing = [n for n in named if n not in speccable]
    if missing:
        raise ValueError(
            f"tallyman_read_csv: schema column(s) {sorted(missing)} are not in the CSV header {list(speccable)}. "
            "Bind by name only when the names match the header; to rename by position pass a tuple-of-tuples "
            'schema, e.g. schema=(("Date", "date"), ("&rest", "infer")).' + reserved_hint
        )
    uncovered = [h for h in speccable if h not in named]
    if rest_dtype is None and uncovered:
        raise ValueError(
            f"tallyman_read_csv: schema is not total — it leaves column(s) {uncovered} unspecified. "
            'Name every column, or close the spec with "&rest": "infer".'
        )
    for name, dt in named.items():
        pl_dt = _resolve_spec_dtype(dt)
        if pl_dt is None:
            inferred = True
        else:
            overrides[name] = pl_dt
    for name in uncovered:
        pl_dt = _resolve_spec_dtype(rest_dtype)
        if pl_dt is None:
            inferred = True
        else:
            overrides[name] = pl_dt
    return {"overrides": overrides, "rename": {}, "out_names": list(speccable), "all_pinned": not inferred}


_ESCALATED_INFER = 10_000  # the middle rung of the #143 infer ladder; whole-file (None) is the terminal


def _polars_to_ibis(dt) -> str:
    """A polars dtype -> an ibis type string, for the suggested-schema hint.

    The primitive table is derived by inverting ``_IBIS_TO_POLARS`` (the reader's
    forward map) so the two can't drift — a suggestion can never name a type the
    reader then rejects. ``date``/``time`` are added because the reader maps those
    via predicates in ``_polars_dtype`` rather than the name table; many-to-one
    ibis spellings (bool/boolean) collapse to the canonical name that appears last
    in ``_IBIS_TO_POLARS``.
    """
    import polars as pl

    # polars dtype class -> canonical ibis name (last forward entry wins a collision).
    reverse = {getattr(pl, pl_name): ibis_name for ibis_name, pl_name in _IBIS_TO_POLARS.items()}
    reverse.setdefault(pl.Date, "date")
    reverse.setdefault(pl.Time, "time")
    for pl_dt, name in reverse.items():
        if dt == pl_dt:
            return name
    if isinstance(dt, pl.Datetime):
        return "timestamp"
    if isinstance(dt, pl.Decimal):
        return "decimal"
    return "string"  # safe fallback — an unmapped column still reads as string


def _suggest_schema_dsl(src: Path, scan_kwargs: dict, reserved: tuple[str, ...] = ()) -> str:
    """Whole-file-infer *src* and render it as the paste-ready tuple-of-tuples DSL.

    *reserved* columns (tallyman-managed, e.g. ``__row_order``) are dropped:
    they must not appear in the suggestion because the caller cannot spec them —
    a suggestion carrying one is un-pasteable (the totality check excludes the
    reserved column, so pasting it back over-counts the columns and raises).
    """
    import polars as pl

    inferred = pl.scan_csv(str(src), infer_schema_length=None, **scan_kwargs).collect_schema()
    pairs = tuple((name, _polars_to_ibis(dt)) for name, dt in inferred.items() if name not in reserved)
    return repr(pairs)


def _materialize_ordered(src: Path, schema, scan_kwargs: dict, tmp_path: Path, *, write=None) -> None:
    """Read *src* (a CSV) into a row-order-stable parquet at *tmp_path* (#143).

    Inference mode (no schema, or a spec with inferred columns) escalates the
    infer window on a parse failure — 100 -> 10k -> whole-file — because a value
    that breaks an *inferred* type is tallyman's to resolve; whole-file infer
    always resolves (a mixed column falls back to string). Explicit mode (every
    column pinned) never escalates: a pinned type that cannot parse is the
    caller's mistake, so it raises with a paste-ready suggested schema.

    The last column is ``__row_order`` (ADR-008 D2). A CSV that already has a
    column of that name has it overwritten, which is the right outcome for a
    file tallyman exported.

    *write* takes the parsed LazyFrame of one rung of the ladder and puts it on
    disk; an import passes one that streams the rows into pyarrow, so a source
    snapshot has the layout every other snapshot has (ADR-011 D1). The default
    is polars' own parquet sink, which is what the ordered copies of ADR-008 D2
    still use.
    """
    import polars as pl

    from tallyman_xorq.ordered_copy import _WRITE

    # Header (names only) drives the schema plan; infer_schema_length=0 reads
    # just the header line, no type sampling.
    header = list(pl.scan_csv(str(src), infer_schema_length=0, **scan_kwargs).collect_schema().names())
    has_row_order = ROW_ORDER in header
    # __row_order is tallyman's reserved index — it is not the caller's to spec, so it is threaded as `reserved`
    # through every header reader (the schema plan, the diagnostics, and the suggested-schema hint) rather than
    # stripped ad hoc.
    reserved = (ROW_ORDER,) if has_row_order else ()

    plan = None
    explicit_only = False
    if schema is not None:
        plan = _normalize_schema(schema, header, reserved=reserved)
        # A spec may not PRODUCE the reserved name either: renaming a data column onto __row_order collides with the
        # index appended below (a raw polars DuplicateError at sink). Reject the reserved output name up front.
        if ROW_ORDER in plan["out_names"]:
            raise ValueError(
                f"tallyman_read_csv: {ROW_ORDER!r} is reserved for tallyman's row index and cannot be a "
                f"schema output column name. Rename that column to something other than {ROW_ORDER!r} in the schema."
            )
        explicit_only = plan["all_pinned"]

    def _ordered(infer_len):
        if schema is None:
            lf = pl.scan_csv(str(src), infer_schema_length=infer_len, **scan_kwargs)
        else:
            il = 0 if plan["all_pinned"] else infer_len
            lf = pl.scan_csv(str(src), schema_overrides=plan["overrides"], infer_schema_length=il, **scan_kwargs)
        if has_row_order:
            lf = lf.drop(ROW_ORDER)  # overwritten by the fresh index below
        lf = lf.with_row_index(ROW_ORDER)
        if schema is not None and plan["rename"]:
            lf = lf.rename(plan["rename"])
        if schema is None:
            cols = [c for c in lf.collect_schema().names() if c != ROW_ORDER]
        else:
            cols = [c for c in plan["out_names"] if c != ROW_ORDER]
        # __row_order last and cast to int64 (a fresh with_row_index yields uint32).
        return lf.select([*cols, pl.col(ROW_ORDER).cast(pl.Int64)])

    if write is None:

        def write(frame, dest: Path) -> None:
            frame.sink_parquet(str(dest), **_WRITE)

    ladder = [_DEFAULT_INFER] if explicit_only else [_DEFAULT_INFER, _ESCALATED_INFER, None]
    last_exc = None
    for infer_len in ladder:
        try:
            write(_ordered(infer_len), tmp_path)
            return
        except pl.exceptions.ComputeError as exc:
            last_exc = exc
    raise ValueError(
        f"tallyman_read_csv: the schema does not parse {src.name}: {last_exc}. "
        f"Suggested schema (whole-file inference): schema={_suggest_schema_dsl(src, scan_kwargs, reserved)}"
    )


def tallyman_read_csv(path: str, schema=None, project: str | None = None, **kwargs):
    """Read a CSV into a xorq expression with a stable ``__row_order``.

    NOT for an authored recipe (ADR-011 D2), for the same reason as
    ``read_project_file``: import the CSV with ``catalog_import_source(path, alias,
    separator=..., schema=...)`` and read the source alias. The reader options move
    to the import call and are recorded on the entry (ADR-011 D12).

    Use this instead of ``xo.deferred_read_csv`` for all CSV ingests. The CSV
    goes through source identity like a parquet source (``read_project_file``):
    its content digest is taken, it is cloned to ``data/.cas/``, and an ordered
    copy of the clone is written under the project's ``compute_cache/``. polars
    reads the clone (``scan_csv -> with_row_index -> sink_parquet``), which
    preserves the file's row order (unlike datafusion's parallel scan, whose row
    order is nondeterministic above the repartition threshold, about 10 MB) and
    numbers the rows in a last column, ``__row_order``, ``0..N-1``. See
    ``plans/ADR-008-row-order-of-reads.md``.

    The CSV is read exactly once, at ingest, and the returned expression is a
    plain ``deferred_read_parquet`` of the copy: no sort, one row-order column.
    Because the copy is named by the CSV's content and the reader options,
    editing the CSV and running the same recipe gives a new content hash, and the
    earlier entry keeps the rows it was built from (#168).

    Args:
        path: Absolute path to the CSV file.
        schema: Optional ibis schema for the columns (same as deferred_read_csv).
            When omitted, polars infers types.
        project: Project name override (defaults to the active project).
        **kwargs: Forwarded to ``polars.scan_csv`` — reader options such as
            ``separator``, ``skip_rows``, ``null_values``, ``quote_char``,
            ``has_header``, ``encoding``. They participate in the copy's key,
            so changing one re-ingests. ``infer_schema_length`` and
            ``schema_overrides`` are managed internally (see ``_RESERVED_SCAN_KWARGS``)
            and rejected — they would collide with the values every internal
            ``scan_csv`` call already sets.
    """
    from xorq.expr.api import deferred_read_parquet

    from tallyman_xorq import ordered_copy as oc
    from tallyman_xorq import source_identity as si

    reserved = [k for k in _RESERVED_SCAN_KWARGS if k in kwargs]
    if reserved:
        raise ValueError(
            f"tallyman_read_csv: {reserved} is managed internally, not a pass-through "
            "polars.scan_csv reader option. Type inference escalates automatically "
            "(100 -> 10k -> whole-file); to pin column types pass schema= (an ibis schema, "
            "plain dict, or tuple-of-tuples), never schema_overrides."
        )
    proj = resolve_project(project)
    pinned = _source_entry_read(proj, "tallyman_read_csv", path)
    if pinned is not None:
        return pinned
    reader = oc.csv_reader(schema, kwargs)
    src = Path(path)
    rel = _relative_to_data(proj, src)
    recorded = _reconstructing_source_digest(proj, rel)
    if recorded is not None:
        si.note_source(rel, recorded)
        return deferred_read_parquet(str(oc.existing_ordered_copy(proj, digest=recorded, reader=reader)))
    digest = si.digest_for(proj, src)
    if si.mode() != "off":
        si.note_source(rel, digest)
    source = si.ensure_cas_path(proj, src, digest) if si.mode() == "cas" else src
    copy = oc.ensure_ordered_copy(proj, source, digest=digest, rel=rel, reader=reader)
    return deferred_read_parquet(str(copy))


def _relative_to_data(proj: str, path: Path) -> str:
    """The name a source is recorded under in ``manifest.sources``: relative to the project's data dir when it sits
    there, the absolute path otherwise (a CSV can live anywhere)."""
    try:
        return str(path.resolve().relative_to(data_dir(proj).resolve()))
    except ValueError:
        return str(path)


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


def _note_parent_records(proj: str, content_hash: str) -> None:
    """Fold the parent's recorded source digests, and its ordered copies when its graph is inlined, into the build.

    The child's ``manifest.sources`` is the closure record ``gc_cas`` walks to keep clones alive, so it carries every
    digest its parent recorded, and the child keeps its own leaves alive even if the parent entry is later evicted.
    A CHEAP parent's graph is inlined into the child's build (a worthy parent is a bare read of its snapshot), so the
    child reads that parent's ordered copies directly and needs the records that let ``ensure_materialized`` make a
    deleted one again (ADR-007 D13). ``note_source`` and ``note_ordered_copy`` are no-ops outside a build's collect
    window, so this costs nothing on a plain read.
    """
    from tallyman_core import read_manifest
    from tallyman_xorq import ordered_copy as oc
    from tallyman_xorq import source_identity as si

    try:
        manifest = read_manifest(entry_dir(proj, content_hash))
    except (OSError, ValueError):
        return
    for rel_path, digest in (manifest.sources or {}).items():
        si.note_source(rel_path, digest)
    if manifest.cache_worthy is False:
        for key, record in (manifest.ordered_copies or {}).items():
            oc.note_ordered_copy(key, record)


def tracked_expr_from_alias(alias: str, project: str | None = None):
    """Load a catalog entry by alias and record the parent edge in the manifest DAG.

    This is the standard way to chain catalog entries: build A, then build B
    from A using ``tracked_expr_from_alias("a")``. The call records A as a
    declared parent of B in ``manifest.parents``, which is how staleness
    propagation and the lineage graph are maintained.

    Accepts an alias only (not a content hash). Resolves the alias to its
    current head entry. Pass the alias name as it appears in ``catalog_list``.
    To read by hash without tracking, use ``pinned_expr_from_alias``.

    Returns the parent entry's result on the in-process default backend
    (``cached_result_expr``, ADR-007 D3): for a worthy parent a bare read of
    its snapshot, whose path contains the parent's content hash, so the child's
    identity is a function of the parent's; for a cheap parent the parent's own
    frozen graph. The snapshot is made to exist first (``ensure_materialized``),
    since a child cannot be built over a file that is missing. When this alias is
    revised, recalc mints a new version of the child expression; the child entry
    built *now* stays bound to the parent revision recorded at build time forever.

    The parent edge is suppressed during reconstruction (when ``_RECONSTRUCTING``
    is True — the structural-nondeterminism diagnostic re-running a recipe) so a
    diagnostic re-run doesn't accidentally record grandparents as direct parents
    of the entry under construction.

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
    _note_parent_records(proj, content_hash)
    return cached_result_expr(proj, content_hash)


def pinned_expr_from_alias(ref: str, project: str | None = None):
    """Load a catalog entry by content hash or version reference, pinned.

    Like tracked_expr_from_alias but pinned: the dependency edge is recorded in
    manifest.parents with follow=False, so recalc knows the child exists but will
    not advance it when the parent alias moves. Use this when you want to stay on
    a specific version of a parent rather than following alias changes.

    Accepts an explicit version reference ``"<alias>-v<N>"`` only (1-based into the
    alias history — the V1…Vn the UI shows). Two other forms are rejected:

    - a bare alias (#166), which reads like a pin but resolves to whatever the head
      happens to be when the recipe is built, so the recipe text under-determines
      the entry. A pinned reference must denote the same entry forever;
    - a bare content hash (ADR-011 D5). Every parent edge names an alias, followed
      or pinned at a version, so no opaque hash appears in a recipe or in
      ``manifest.parents``. An entry with no alias — one built by ``catalog_run`` —
      must be named (``catalog_alias``) before anything can build on it.

    The parent edge is suppressed during reconstruction (same as tracked_expr_from_alias)
    so re-running a child's recipe doesn't accidentally write to the manifest.

    Args:
        ref: A version reference like "shoe_sales-v2".
        project: Project name override (defaults to active TALLYMAN_PROJECT).
    """
    from tallyman_core.aliases import VERSION_REF_RE, history_for, resolve_version_ref, version_of_hash
    from tallyman_xorq.result_cache import _RECONSTRUCTING, _resolve_noncyclic_hash, cached_result_expr

    proj = resolve_project(project)
    if entry_dir(proj, ref).exists():
        named = version_of_hash(proj, ref)
        steer = (
            f"Pin it as {named[0] + '-v' + str(named[1])!r} instead."
            if named
            else f"Entry {ref} heads no alias history, so there is no version to name: give it a name with "
            "catalog_alias first, then pin it as '<alias>-v<N>'."
        )
        raise ProjectDataNotFound(
            f"pinned_expr_from_alias({ref!r}) is a bare content hash, which a recipe may not name (ADR-011 D5): "
            f"a parent edge names an alias and a version of it, so the DAG is readable and a hash never leaks into "
            f"a recipe. {steer}"
        )
    elif get_alias(proj, ref) is not None:
        raise ProjectDataNotFound(
            f"pinned_expr_from_alias({ref!r}) is a bare alias, which would silently pin "
            f"whatever the head happens to be right now (#166). Pin an exact version — "
            f"{ref + '-v<N>'!r} — or use tracked_expr_from_alias({ref!r}) to follow the "
            "alias. A bare content hash is not the way either (ADR-011 D5)."
        )
    else:
        content_hash = resolve_version_ref(proj, ref)
        if content_hash is None:
            m = VERSION_REF_RE.match(ref)
            hist = history_for(proj, m["name"]) if m else []
            if hist:
                raise ProjectDataNotFound(
                    f"pinned_expr_from_alias({ref!r}): alias {m['name']!r} has "
                    f"{len(hist)} version(s) (v1..v{len(hist)})"
                )
            raise ProjectDataNotFound(f"catalog entry {ref!r} not found in project {proj!r}")
    if not entry_dir(proj, content_hash).exists():
        raise ProjectDataNotFound(f"catalog entry {ref!r} not found in project {proj!r}")
    content_hash = _resolve_noncyclic_hash(proj, ref, content_hash)
    if not _RECONSTRUCTING.get():
        from tallyman_xorq import parent_capture as pc

        pc.note_parent(content_hash, ref=ref, follow=False)
    _note_parent_records(proj, content_hash)
    return cached_result_expr(proj, content_hash)
