"""Project-aware data loading helpers.

Convention: every project owns its data under `<project_dir>/data/`. User code
references files by *relative* name. The build rewrites absolute filesystem
paths to `${TALLYMAN_PROJECT_ROOT}` placeholders on write and expands them on
load, so recording the relative intent here is what makes the artifact portable
across machines (and packable via `tallyman pack`; see docs/architecture.md).
"""

from __future__ import annotations

import contextlib
import contextvars
from pathlib import Path

from tallyman_core import data_dir, entry_dir, get_alias, resolve_project


class ProjectDataNotFound(FileNotFoundError):
    pass


# A replay-time freeze for alias-pinned parents (#160). ``pinned_expr_from_alias("p")``
# stores the alias *name* in the recipe, so a naive replay re-resolves the name to p's
# CURRENT head and the pin silently advances — the docs promise a pin "stays on that
# exact revision" and is "never disturbed by recalc". During a recalc replay the walk
# knows the revision the pin resolved to at build (``manifest.parents[i].hash``) and
# sets this map ``{alias_name: frozen_hash}`` so ``pinned_expr_from_alias`` re-resolves
# to the recorded revision instead of the live head. A pin by *hash* stores the hash
# literally and never consults this map — it is already replay-durable.
_PIN_FREEZE: contextvars.ContextVar[dict[str, str] | None] = contextvars.ContextVar("tallyman_pin_freeze", default=None)


@contextlib.contextmanager
def pin_freeze(mapping: dict[str, str]):
    """Freeze alias-pinned parents to their recorded revisions for the duration of a
    recalc replay (#160). ``mapping`` is ``{alias_name: content_hash}``; while it is
    active, ``pinned_expr_from_alias(name)`` resolves ``name`` to ``content_hash``
    rather than the alias's current head."""
    token = _PIN_FREEZE.set(mapping)
    try:
        yield
    finally:
        _PIN_FREEZE.reset(token)


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
            #
            # must_exist=False: recon_cas_path serves the frozen clone without the live
            # source, so a moved/deleted source must not defeat the pinned read. This
            # block MUST run before the existence-requiring project_path below.
            recon_path = project_path(rel_path, proj, must_exist=False)
            si.note_source(rel_path, recorded)
            return deferred_read_parquet(str(si.recon_cas_path(proj, recon_path, recorded)))
    path = project_path(rel_path, proj)
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
    "int8": "Int8",
    "int16": "Int16",
    "int32": "Int32",
    "int64": "Int64",
    "uint8": "UInt8",
    "uint16": "UInt16",
    "uint32": "UInt32",
    "uint64": "UInt64",
    "float32": "Float32",
    "float64": "Float64",
    "string": "String",
    "bool": "Boolean",
    "boolean": "Boolean",
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
    (currently ``original_row_order``) that the caller must not spec: they are
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
            uncovered = list(speccable[len(cells) :])
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
        for orig in speccable[len(cells) :]:  # the &rest tail keeps its header name
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

    *reserved* columns (tallyman-managed, e.g. ``original_row_order``) are dropped:
    they must not appear in the suggestion because the caller cannot spec them —
    a suggestion carrying one is un-pasteable (the totality check excludes the
    reserved column, so pasting it back over-counts the columns and raises).
    """
    import polars as pl

    inferred = pl.scan_csv(str(src), infer_schema_length=None, **scan_kwargs).collect_schema()
    pairs = tuple((name, _polars_to_ibis(dt)) for name, dt in inferred.items() if name not in reserved)
    return repr(pairs)


def _validate_existing_row_order(src: Path, scan_kwargs: dict) -> None:
    """The CSV already has an ``original_row_order`` column, colliding with the row
    index tallyman would add via ``with_row_index`` (a raw polars ``DuplicateError``
    the infer ladder does not catch). Reuse the column only if it *is* the canonical
    order — the contiguous ``0..N-1`` file sequence tallyman itself writes, each row
    exactly one more than the last. Otherwise the name is a coincidence carrying
    foreign data, so raise loudly rather than trust a non-canonical order.
    """
    import polars as pl

    reserved = (
        f"tallyman_read_csv: {src.name} already has a column named 'original_row_order', which is "
        "reserved for tallyman's canonical row index. {why} Rename the column in the source CSV."
    )
    try:
        series = (
            pl.scan_csv(
                str(src),
                schema_overrides={"original_row_order": pl.Int64},
                infer_schema_length=0,
                **scan_kwargs,
            )
            .select("original_row_order")
            .collect()
            .to_series()
        )
    except pl.exceptions.ComputeError as exc:
        raise ValueError(reserved.format(why=f"Its values are not integers ({exc}).")) from exc
    n = series.len()
    expected = pl.int_range(0, n, dtype=pl.Int64, eager=True)
    if series.null_count() or not (series == expected).all():
        raise ValueError(
            reserved.format(
                why=f"Its values are not the contiguous 0..{n - 1} sequence (each row must be one more than the last)."
            )
        )


def _materialize_ordered(src: Path, schema, scan_kwargs: dict, tmp_path: Path) -> None:
    """Read *src* into a row-order-stable parquet at *tmp_path* (#143).

    Inference mode (no schema, or a spec with inferred columns) escalates the
    infer window on a parse failure — 100 -> 10k -> whole-file — because a value
    that breaks an *inferred* type is tallyman's to resolve; whole-file infer
    always resolves (a mixed column falls back to string). Explicit mode (every
    column pinned) never escalates: a pinned type that cannot parse is the
    caller's mistake, so it raises with a paste-ready suggested schema.
    """
    import polars as pl

    # Header (names only) drives both the schema plan and the row-index collision
    # check; infer_schema_length=0 reads just the header line, no type sampling.
    header = list(pl.scan_csv(str(src), infer_schema_length=0, **scan_kwargs).collect_schema().names())
    has_row_order = "original_row_order" in header
    # original_row_order is tallyman's reserved index — _ordered re-appends it, and
    # _validate_existing_row_order (below) vets a pre-existing one. It is not the caller's
    # to spec, so it is threaded as `reserved` through every header reader (the schema
    # plan, the diagnostics, and the suggested-schema hint) rather than stripped ad hoc.
    reserved = ("original_row_order",) if has_row_order else ()
    if has_row_order:
        _validate_existing_row_order(src, scan_kwargs)  # reuse it, or raise — never with_row_index over it

    plan = None
    explicit_only = False
    if schema is not None:
        plan = _normalize_schema(schema, header, reserved=reserved)
        # A spec may not PRODUCE the reserved name either: renaming a data column onto
        # 'original_row_order' collides with the index _ordered re-appends (a raw polars
        # DuplicateError at sink). Reject the reserved output name up front with the same
        # guidance as the pre-existing-column clash.
        if "original_row_order" in plan["out_names"]:
            raise ValueError(
                "tallyman_read_csv: 'original_row_order' is reserved for tallyman's canonical "
                "row index and cannot be a schema output column name. Rename that column to "
                "something other than 'original_row_order' in the schema."
            )
        explicit_only = plan["all_pinned"]

    def _ordered(infer_len):
        if schema is None:
            lf = pl.scan_csv(str(src), infer_schema_length=infer_len, **scan_kwargs)
            if not has_row_order:
                lf = lf.with_row_index("original_row_order")
            cols = [c for c in lf.collect_schema().names() if c != "original_row_order"]
        else:
            il = 0 if plan["all_pinned"] else infer_len
            lf = pl.scan_csv(str(src), schema_overrides=plan["overrides"], infer_schema_length=il, **scan_kwargs)
            if not has_row_order:
                lf = lf.with_row_index("original_row_order")
            if plan["rename"]:
                lf = lf.rename(plan["rename"])
            cols = [c for c in plan["out_names"] if c != "original_row_order"]
        # original_row_order last and cast to int64 (a fresh with_row_index yields
        # uint32; a reused in-CSV column is stripped from `cols` and re-appended here).
        return lf.select([*cols, pl.col("original_row_order").cast(pl.Int64)])

    ladder = [_DEFAULT_INFER] if explicit_only else [_DEFAULT_INFER, _ESCALATED_INFER, None]
    last_exc = None
    for infer_len in ladder:
        try:
            _ordered(infer_len).sink_parquet(str(tmp_path), **_CSV_PARQUET_WRITE)
            return
        except pl.exceptions.ComputeError as exc:
            last_exc = exc
    raise ValueError(
        f"tallyman_read_csv: the schema does not parse {src.name}: {last_exc}. "
        f"Suggested schema (whole-file inference): schema={_suggest_schema_dsl(src, scan_kwargs, reserved)}"
    )


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

    kwargs_sig = repr(sorted((k, repr(v)) for k, v in scan_kwargs.items()))
    raw = f"{Path(path).resolve()}|{_spec_sig(schema)}|{kwargs_sig}"
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
    # Unique temp name (not a fixed {key}.parquet.tmp) so two builds writing the
    # same key concurrently can't corrupt each other's partial file (#7); the
    # rename is atomic and last-writer-wins on identical bytes.
    tmp = out_dir / f"{key}.{uuid.uuid4().hex}.tmp"
    try:
        _materialize_ordered(src, schema, scan_kwargs, tmp)
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
            cache key, so changing one re-ingests. ``infer_schema_length`` and
            ``schema_overrides`` are managed internally (see ``_RESERVED_SCAN_KWARGS``)
            and rejected — they would collide with the values every internal
            ``scan_csv`` call already sets.
    """
    import xorq.api as xo

    reserved = [k for k in _RESERVED_SCAN_KWARGS if k in kwargs]
    if reserved:
        raise ValueError(
            f"tallyman_read_csv: {reserved} is managed internally, not a pass-through "
            "polars.scan_csv reader option. Type inference escalates automatically "
            "(100 -> 10k -> whole-file); to pin column types pass schema= (an ibis schema, "
            "plain dict, or tuple-of-tuples), never schema_overrides."
        )
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
    if is_hash:
        content_hash = alias_or_hash  # a literal hash pin is already replay-durable
    else:
        # An alias-name pin: during a recalc replay, resolve to the revision recorded
        # at build (the freeze map) so the pin is not silently advanced by the replay
        # (#160); otherwise resolve the alias's current head as usual.
        frozen = _PIN_FREEZE.get()
        content_hash = frozen.get(alias_or_hash) if frozen else None
        if content_hash is None:
            content_hash = get_alias(proj, alias_or_hash)
    if content_hash is None or not entry_dir(proj, content_hash).exists():
        raise ProjectDataNotFound(f"catalog entry {alias_or_hash!r} not found in project {proj!r}")
    content_hash = _resolve_noncyclic_hash(proj, alias_or_hash, content_hash)
    if not _RECONSTRUCTING.get():
        from tallyman_xorq import parent_capture as pc

        pc.note_parent(content_hash, ref=alias_or_hash, follow=False)
    return cached_result_expr(proj, content_hash)
