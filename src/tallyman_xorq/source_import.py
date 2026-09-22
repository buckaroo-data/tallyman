"""Importing a file: the one way data enters the catalog (ADR-011).

A raw input is not something a build reads. It is an **alias** whose versions are ordinary entries, and the only way
to advance one is ``update_and_depend`` — the explicit act of copying a file into the arena (the files tallyman owns)
and pointing a source alias at the resulting entry.

One import does four things:

1. digests the outside file and clones its bytes to ``data/.cas/<digest><suffix>``, verified after the write
   (ADR-011 D9). The clone is the imported file as it was, kept so a CSV read with the wrong schema can be imported
   again without the outside file (ADR-005's suggestion-and-retry contract), and so the snapshot below can be
   written again from it;
2. writes ONE parquet snapshot of it, in file order plus a last ``__row_order`` column, at
   ``compute_cache/result_cache/<content_hash>.parquet``. A source entry is worthy and **its snapshot is the ordered
   copy** — ``compute_cache/ordered_sources/`` and the copy key of ADR-008 D2 do not exist for an import, because the
   entry hash names the file and the reader options are recorded on the entry that used them (ADR-011 D12);
3. writes the entry: a generated recipe, a frozen ``xorq_build/``, a schema and a manifest whose ``provenance``
   records the outside path, the digest and the reader options;
4. points the source alias at it, appending a version.

The **content hash of a source entry is a function of its bytes and its reader options**, and of nothing else, so two
imports of identical bytes under two aliases mint one entry and share one file.

That snapshot is **cache** in the sense of ADR-007 D13 — a file is cache if ``ensure_materialized`` can re-create it
— because the clone holds the bytes and the entry holds the reader options. A deleted one is written again from the
clone and verified against the recorded ``result_digest``, like any other snapshot; the Cache page offers to delete
it like any other. Only when the clone is gone as well are the rows unrecoverable, and then the snapshot is pinned
and the error names the re-import (``materialize._heal_a_source``).

After the import the outside path is **provenance**: recorded on the entry and never read again. Deleting, moving or
editing the original file has no effect on any build.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

perf_log = logging.getLogger("tallyman.perf")

# What ``manifest.cache_worthy_why`` records for a source entry: worthy, but not because a computation earned it.
_WORTHY_WHY = "source version (imported data, not a computation)"

# The shape of xorq's build hash, which an entry's directory name, URLs and aliases all assume.
HASH_LEN = 12

_CSV_SUFFIXES = frozenset({".csv", ".tsv", ".txt"})
_PARQUET_SUFFIXES = frozenset({".parquet", ".pq"})

# The generated recipe of a source entry, executing on this call stack, as ``(project, content_hash)``.
# ``read_project_file`` and ``tallyman_read_csv`` are build errors in an authored recipe (ADR-011 D2) and resolve to
# this entry's snapshot inside it — the one place a raw read survives.
_SOURCE_ENTRY: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar("_source_entry", default=None)


class SourceImportError(ValueError):
    """An import that cannot be performed as asked: a bad path, an alias collision, or a version claim that is wrong."""


# ---------------------------------------------------------------------------
# identity
# ---------------------------------------------------------------------------


def source_entry_hash(digest: str, reader: dict) -> str:
    """The content hash of the entry an import mints: its bytes and its reader options, and nothing else.

    Same 12-hex shape as xorq's build hash, so nothing downstream (entry directories, URLs, aliases) notices the
    difference. Two imports of the same bytes under the same reader are the same entry.
    """
    from tallyman_xorq.ordered_copy import _reader_signature

    payload = f"source|{digest}|{_reader_signature(reader)}"
    return hashlib.md5(payload.encode()).hexdigest()[:HASH_LEN]  # noqa: S324 — an identity, not a credential


def is_source_entry(project: str, content_hash: str) -> bool:
    """Whether the entry is a version of a source alias — i.e. whether its manifest records an import."""
    from tallyman_core import read_manifest
    from tallyman_core.paths import entry_dir

    try:
        return read_manifest(entry_dir(project, content_hash)).provenance is not None
    except (OSError, ValueError):
        return False


def source_entry_context(project: str) -> str | None:
    """The source entry whose generated recipe is running on this stack, if it belongs to *project*."""
    ctx = _SOURCE_ENTRY.get()
    if ctx is None or ctx[0] != project:
        return None
    return ctx[1]


def in_source_recipe(project: str, content_hash: str):
    """Run the generated recipe of *content_hash* with its raw reads resolved to that entry's snapshot (D2)."""
    return _SOURCE_ENTRY.set((project, content_hash))


def release_source_recipe(token) -> None:
    _SOURCE_ENTRY.reset(token)


# ---------------------------------------------------------------------------
# reader options, fixed at import (D12)
# ---------------------------------------------------------------------------


def _reader_for(path: Path, schema, reader_options: dict) -> dict:
    """The reader a source is read with, decided once, here, from the file's suffix and the caller's options.

    A CSV's delimiter, schema overrides and inference settings are named in the import call and recorded on the entry.
    Two recipes cannot read one file two ways; import it twice under two aliases.
    """
    from tallyman_xorq import ordered_copy as oc
    from tallyman_xorq.io import _RESERVED_SCAN_KWARGS

    suffix = path.suffix.lower()
    if suffix in _PARQUET_SUFFIXES:
        if schema is not None or reader_options:
            named = sorted([*reader_options, *(["schema"] if schema is not None else [])])
            raise SourceImportError(
                f"{path.name} is a parquet file, whose rows and types are in the file — the reader option(s) "
                f"{named} apply to a CSV only. Import it without them."
            )
        return oc.parquet_reader()
    if suffix in _CSV_SUFFIXES:
        reserved = [k for k in _RESERVED_SCAN_KWARGS if k in reader_options]
        if reserved:
            raise SourceImportError(
                f"{reserved} is managed internally, not a pass-through polars.scan_csv reader option. Type "
                "inference escalates automatically (100 -> 10k -> whole-file); to pin column types pass schema= "
                "(an ibis schema, plain dict, or tuple-of-tuples), never schema_overrides."
            )
        reader = oc.csv_reader(schema, reader_options)
        if reader.get("lossless") is False:
            raise SourceImportError(
                "a reader option of this import does not survive JSON, so the entry could not record how the file "
                "was read. Pass reader options as plain values (strings, numbers, lists), not callables."
            )
        return reader
    raise SourceImportError(
        f"tallyman imports parquet ({', '.join(sorted(_PARQUET_SUFFIXES))}) and CSV "
        f"({', '.join(sorted(_CSV_SUFFIXES))}) files; {path.name} has suffix {path.suffix!r}. Convert it first."
    )


# ---------------------------------------------------------------------------
# the version table (D3, D11)
# ---------------------------------------------------------------------------


def _plan_version(alias: str, history: list[str], content_hash: str, pinned_version: int | None) -> tuple[bool, int]:
    """Decide what this import does: ``(mint, version)``, or raise.

    The full case table of ADR-011 D3, plus D11's rule that history is append-only and monotonic — two version
    numbers never denote the same bytes, and a version number never moves backwards while history is intact.
    """
    head = len(history)  # the head's 1-based version; 0 when the alias is absent
    already = history.index(content_hash) + 1 if content_hash in history else None

    if pinned_version is None:
        if already == head and head:  # the bytes are the head's: a no-op
            return False, head
        if already is not None:  # the bytes are an older version's (D11)
            raise SourceImportError(
                f"these bytes are already {alias}-v{already}, and {alias} is at v{head}. Version history is "
                f"append-only: a v{head + 1} whose rows equal v{already}'s would make the version number meaningless. "
                f"To put {alias} back on v{already}, reset the catalog to the revision before v{already + 1} "
                f"(tallyman reset / catalog_reset_to), or import these bytes under a different alias."
            )
        return True, head + 1

    if pinned_version < 1:
        raise SourceImportError(f"pinned_version must be 1 or greater; got {pinned_version}.")
    if pinned_version > head + 1:
        raise SourceImportError(
            f"pinned_version={pinned_version} would skip version(s): {alias} has {head} version(s) and versions "
            f"cannot be skipped, so the next one is v{head + 1}."
        )
    if pinned_version <= head:
        claimed = history[pinned_version - 1]
        if claimed == content_hash:
            return False, pinned_version  # the file IS that version; the head does not move
        raise SourceImportError(
            f"this file is not {alias}-v{pinned_version}: that version is entry {claimed}, and these bytes are "
            f"{content_hash}. Import without pinned_version to mint the next version, or point at the file "
            f"{alias}-v{pinned_version} was imported from."
        )
    if already is not None:  # pinned at head+1, but these bytes are already a version (D11)
        raise SourceImportError(
            f"these bytes are already {alias}-v{already}, so they cannot also be v{pinned_version}. "
            f"Two version numbers never denote the same bytes."
        )
    return True, pinned_version


# ---------------------------------------------------------------------------
# writing a source entry
# ---------------------------------------------------------------------------


def _numbered(batches):
    """*batches*, each with a last ``__row_order`` column continuing the count over the whole stream (ADR-008 D2)."""
    import numpy as np
    import pyarrow as pa

    from tallyman_xorq.row_order import ROW_ORDER

    written = 0
    for batch in batches:
        if not batch.num_rows:
            continue
        numbers = pa.array(np.arange(written, written + batch.num_rows, dtype=np.int64))
        yield batch.append_column(pa.field(ROW_ORDER, pa.int64()), numbers)
        written += batch.num_rows


def _write_parquet_snapshot(clone: Path, dest: Path) -> None:
    """Copy the parquet *clone* to *dest*, in file order, numbering the rows in a last ``__row_order``.

    pyarrow reads and writes every type a parquet file can hold, so the snapshot's schema is the imported file's
    plus ``__row_order``, and a recipe over the source alias sees the types the file has. polars, which wrote this
    before, turned a ``date32`` into a timestamp, a ``time32`` into a ``time64`` and a map into a list of structs
    (#197). An existing ``__row_order`` is dropped and written again, last.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from tallyman_xorq.materialize import write_pinned_parquet
    from tallyman_xorq.ordered_copy import ORDERED_COPY_ROW_GROUP_ROWS
    from tallyman_xorq.row_order import ROW_ORDER

    with pq.ParquetFile(clone) as source:
        kept = [f for f in source.schema_arrow if f.name != ROW_ORDER]
        schema = pa.schema([*kept, pa.field(ROW_ORDER, pa.int64())])
        batches = source.iter_batches(columns=[f.name for f in kept])
        write_pinned_parquet(_numbered(batches), schema, dest, row_group_rows=ORDERED_COPY_ROW_GROUP_ROWS)


def _write_csv_snapshot(clone: Path, reader: dict, dest: Path) -> None:
    """Parse the CSV *clone* with polars under the recorded reader options, and write it with pyarrow.

    polars is here because it is the only reader that holds the file's row order (datafusion's parallel scan does
    not, above its repartition threshold) and the only one that applies the schema DSL and the inference ladder of
    ADR-005. It does not write the file: its batches go to the same writer every other snapshot uses.
    """
    from tallyman_xorq.io import _materialize_ordered
    from tallyman_xorq.ordered_copy import _spec_from_json

    _materialize_ordered(
        clone, _spec_from_json(reader["schema"]), dict(reader["scan_kwargs"]), dest, write=_write_frames
    )


def _write_frames(frame, dest: Path) -> None:
    """Write the rows of the polars LazyFrame *frame* to *dest*, in order, without ever collecting the whole thing.

    ``collect_batches`` pulls the streaming engine one chunk at a time, so memory is bounded by a row group and a
    source larger than RAM imports the way a big one is supposed to. The frame already carries ``__row_order`` last
    (``io._materialize_ordered``), and its schema is taken from the query rather than from the first batch, so a
    CSV with a header and no rows still writes a file with the right columns.
    """
    import polars as pl

    from tallyman_xorq.materialize import write_pinned_parquet
    from tallyman_xorq.ordered_copy import ORDERED_COPY_ROW_GROUP_ROWS

    schema = pl.DataFrame(schema=frame.collect_schema()).to_arrow().schema
    batches = (
        batch
        for chunk in frame.collect_batches(chunk_size=ORDERED_COPY_ROW_GROUP_ROWS)
        for batch in chunk.to_arrow().to_batches()
    )
    write_pinned_parquet(batches, schema, dest, row_group_rows=ORDERED_COPY_ROW_GROUP_ROWS)


def _write_snapshot(clone: Path, reader: dict, dest: Path) -> str:
    """Write the ordered parquet of *clone* to *dest*, and return the content digest of what was written.

    One writer for both readers: pyarrow, in the pinned layout of ``materialize._PARQUET_OPTIONS`` with row groups of
    ``ORDERED_COPY_ROW_GROUP_ROWS``, which ``SNAPSHOT_FORMAT_VERSION`` covers (ADR-009 D3). A parquet source needs
    no parser at all; a CSV is parsed by polars and written here. polars cannot write the layout itself — 1.40.1
    (installed) and 1.44.2 (latest) expose neither the parquet format version nor the page index
    (pola-rs/polars#12752) — and a file written twice to be re-encoded is worse than a file written once.
    """
    from tallyman_xorq.digest import content_digest

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.stem}.{uuid.uuid4().hex}.tmp")
    try:
        if reader["kind"] == "parquet":
            _write_parquet_snapshot(clone, tmp)
        else:
            _write_csv_snapshot(clone, reader, tmp)
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)
    return content_digest(dest)


def source_clone_path(project: str, provenance) -> Path:
    """The clone of a source version's imported bytes: ``data/.cas/<digest><suffix>``.

    The bytes as they arrived, kept beside the snapshot (ADR-011, open question 1). They are what re-creates the
    snapshot when it is deleted, and what ADR-005's suggestion-and-retry contract re-reads when a CSV was imported
    under the wrong schema, so the entry survives the outside file going away.
    """
    from tallyman_core.paths import data_dir

    return data_dir(project) / ".cas" / f"{provenance.digest}{provenance.suffix}"


def rewrite_source_snapshot(project: str, content_hash: str, provenance) -> str:
    """Write the snapshot of the source entry *content_hash* again, from its clone; return the digest written.

    ``ensure_materialized`` calls this for a source version whose file was deleted (ADR-011 D1). The reader options
    are the ones recorded at import (D12), so the rows are parsed exactly as they were the first time.
    """
    from tallyman_xorq.materialize import snapshot_path

    clone = source_clone_path(project, provenance)
    return _write_snapshot(clone, provenance.reader, snapshot_path(project, content_hash))


def _recipe(outside_path: Path, alias: str, version: int, digest: str, reader: dict) -> str:
    """The generated recipe of a source entry: what the Code tab shows, and what its frozen build is made from.

    The read resolves to this entry's own snapshot (``_SOURCE_ENTRY``), not to the path in the call — the path is
    provenance and is never read again. This is the one recipe in which a raw read is allowed (ADR-011 D2).
    """
    return (
        f"# {alias}-v{version}: a source version, generated by catalog_import_source. Do not edit.\n"
        "# A source version is data: its rows are the bytes imported from the path below, ordered and\n"
        "# numbered in __row_order. The path is provenance — the build reads tallyman's own copy of it.\n"
        f"#   imported from: {outside_path}\n"
        f"#   content:       md5:{digest}\n"
        f"#   reader:        {json.dumps(reader, sort_keys=True)}\n"
        "from tallyman_xorq.io import read_project_file\n"
        "\n"
        f"expr = read_project_file({str(outside_path)!r})\n"
    )


def _mint(
    project: str,
    outside_path: Path,
    *,
    digest: str,
    reader: dict,
    content_hash: str,
    alias: str,
    version: int,
    prompt: str | None,
) -> dict:
    """Write the entry, its snapshot and its clone. Returns ``{"row_count", "schema"}``."""
    import pyarrow.parquet as pq

    from tallyman_core import (
        Manifest,
        atomic_write_text,
        entry_build_dir,
        entry_dir,
        entry_schema_path,
        project_dir,
        write_manifest,
    )
    from tallyman_core.manifest import SourceProvenance
    from tallyman_xorq import source_identity as si
    from tallyman_xorq._git_state_guard import install_git_state_guard
    from tallyman_xorq.build import BuildError, _import_script
    from tallyman_xorq.materialize import SNAPSHOT_FORMAT_VERSION, engine_versions, snapshot_path
    from tallyman_xorq.portable import PLACEHOLDER, make_portable_inplace
    from tallyman_xorq.source_cache import rewrite_for_build
    from tallyman_xorq.worthiness import Verdict

    # 1. The bytes, as imported, into the arena. ensure_cas_path digests what it wrote (ADR-011 D9).
    clone = si.ensure_cas_path(project, outside_path, digest)

    # 2. The one snapshot, named by the entry hash. An existing one (a second alias over the same bytes) is kept:
    #    the hash is a function of the bytes and the reader, so it already holds exactly these rows.
    snapshot = snapshot_path(project, content_hash)
    if snapshot.exists():
        from tallyman_xorq.digest import content_digest

        result_digest = content_digest(snapshot)
    else:
        result_digest = _write_snapshot(clone, reader, snapshot)
    arrow_schema = pq.read_schema(snapshot)
    row_count = pq.ParquetFile(snapshot).metadata.num_rows

    # 3. The entry: a generated recipe, its frozen build, a schema and a manifest.
    install_git_state_guard()
    code = _recipe(outside_path, alias, version, digest, reader)
    token = in_source_recipe(project, content_hash)
    try:
        module, tmp_script = _import_script(code)
    finally:
        release_source_recipe(token)
    try:
        expr = getattr(module, "expr", None)
        if expr is None:
            raise BuildError(f"the generated recipe of {alias}-v{version} bound no 'expr'")
        target = entry_dir(project, content_hash)
        target.mkdir(parents=True, exist_ok=True)
        try:
            from xorq.ibis_yaml.compiler import build_expr

            ordered = rewrite_for_build(expr, project, verdict=Verdict(True, _WORTHY_WHY))
            with tempfile.TemporaryDirectory(prefix="tallyman_source_") as builds_str:
                build_path = Path(build_expr(ordered, builds_dir=Path(builds_str)))
                xorq_build = entry_build_dir(project, content_hash)
                xorq_build.mkdir(parents=True, exist_ok=True)
                for item in build_path.rglob("*"):
                    dest = xorq_build / item.relative_to(build_path)
                    if item.is_dir():
                        dest.mkdir(parents=True, exist_ok=True)
                    else:
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        dest.write_bytes(item.read_bytes())
            make_portable_inplace(xorq_build, project_dir(project))

            (target / "expr.py").write_text(code.replace(str(project_dir(project)), PLACEHOLDER))
            schema_doc = {
                "fields": [{"name": f.name, "type": str(f.type)} for f in arrow_schema],
                "row_count": row_count,
            }
            atomic_write_text(entry_schema_path(project, content_hash), json.dumps(schema_doc, indent=2))
            write_manifest(
                target,
                Manifest(
                    content_hash=content_hash,
                    project=project,
                    prompt=prompt,
                    row_count=row_count,
                    execute_seconds=0.0,
                    cache_worthy=True,
                    cache_worthy_why=_WORTHY_WHY,
                    cache_bytes=snapshot.stat().st_size,
                    result_digest=result_digest,
                    snapshot_format=SNAPSHOT_FORMAT_VERSION,
                    engine_versions=engine_versions(),
                    provenance=SourceProvenance(
                        alias=alias,
                        version=version,
                        path=str(outside_path),
                        digest=digest,
                        suffix=outside_path.suffix,
                        reader=reader,
                        imported_at=datetime.now(timezone.utc).isoformat(),
                    ),
                ),
            )
        except Exception:
            shutil.rmtree(target, ignore_errors=True)
            raise
    finally:
        import sys

        sys.modules.pop(getattr(module, "__name__", "") or "", None)
        tmp_script.unlink(missing_ok=True)
    perf_log.info("import %s-v%s -> %s (%s rows, %s)", alias, version, content_hash, row_count, outside_path)
    return {"row_count": row_count, "schema": schema_doc}



# ---------------------------------------------------------------------------
# the one way to advance a source alias (D3)
# ---------------------------------------------------------------------------


def update_and_depend(
    outside_path: str | Path,
    alias: str,
    pinned_version: int | None = None,
    *,
    project: str | None = None,
    prompt: str | None = None,
    schema=None,
    **reader_options,
) -> dict:
    """Import *outside_path* and point the source alias *alias* at the resulting entry (ADR-011 D3).

    The official, and only, way to advance a source alias. What happens depends on the alias's history and on whether
    the caller claims a version:

    ==================================================  =========================================
    state                                               behaviour
    ==================================================  =========================================
    alias absent                                        import, mint v1, return v1
    ``pinned_version=None``, bytes differ from head     mint the next version, return it
    ``pinned_version=None``, bytes equal head           no-op, return head
    ``pinned_version=N`` exists, digest matches         no-op, return vN
    ``pinned_version=N`` exists, digest differs         error: the file is not the version claimed
    ``pinned_version`` = head+1, bytes differ           mint it, return it
    ``pinned_version`` beyond head+1                    error: versions cannot be skipped
    ``pinned_version=N`` < head, digest matches         return vN; the head does not move
    bytes match a version older than the head           error: history is append-only (D11)
    ==================================================  =========================================

    Args:
        outside_path: Any path to a parquet or CSV file. ``data/`` is not special — a file is imported from wherever
            it is, and after the import the path is provenance and is never read again.
        alias: The source alias. It may not already name a catalog alias, and a catalog alias may not later take it.
        pinned_version: The version the caller claims this file is, when they want the claim checked.
        project: Project name override (defaults to the active project).
        prompt: Optional human-readable description, recorded on the entry.
        schema: For a CSV, the column types (an ibis schema, a dict, or the tuple-of-tuples DSL).
        **reader_options: For a CSV, ``polars.scan_csv`` options (``separator``, ``skip_rows``, ``null_values`` …).
            They are recorded on the entry and never re-derived at build time (ADR-011 D12).

    Returns:
        ``{"alias", "version", "hash", "created", "path", "digest", "row_count", "schema"}``. ``created`` is False
        when the import was a no-op.

    Raises:
        SourceImportError: for every row of the table above that is an error, and for a path that is not an importable
            file.
    """
    from tallyman_core import ensure_project, resolve_project
    from tallyman_core.aliases import SOURCE_KIND, alias_kind, history_for, set_alias, validate_alias_name
    from tallyman_core.catalog_state import project_lock
    from tallyman_xorq import source_identity as si

    proj = resolve_project(project)
    ensure_project(proj)

    src = Path(outside_path).expanduser()
    if src.is_dir():
        raise SourceImportError(
            f"{src} is a directory. An import is one file to one alias; a dataset that arrives as several parts is "
            "open question 2 of ADR-011 and has no defined part order yet. Concatenate the parts into one file "
            "first, or import each part under its own alias."
        )
    if not src.is_file():
        raise SourceImportError(f"{src} is not a file, so there is nothing to import.")

    try:
        validate_alias_name(alias)
    except ValueError as exc:  # a name matching the '<alias>-v<N>' version syntax (#166)
        raise SourceImportError(str(exc)) from exc
    kind = alias_kind(proj, alias)
    if kind is not None and kind != SOURCE_KIND:
        raise SourceImportError(
            f"{alias!r} is a catalog alias (a computation), so it cannot also name an imported file. "
            "Import under a different name, or rename the catalog alias first."
        )

    reader = _reader_for(src, schema, reader_options)
    digest = si._digest_file(src)
    content_hash = source_entry_hash(digest, reader)

    with project_lock(proj):
        history = history_for(proj, alias)
        mint, version = _plan_version(alias, history, content_hash, pinned_version)
        from tallyman_core.paths import entry_dir
        from tallyman_xorq.materialize import snapshot_path

        # A no-op whose entry is gone is repaired here, and so is a missing snapshot: the caller has handed us the
        # bytes, which is cheaper than going through the clone, and an entry directory is not cache at all.
        if mint or not (entry_dir(proj, content_hash).is_dir() and snapshot_path(proj, content_hash).exists()):
            written = _mint(
                proj,
                src,
                digest=digest,
                reader=reader,
                content_hash=content_hash,
                alias=alias,
                version=version,
                prompt=prompt,
            )
        else:
            from tallyman_core import entry_schema_path, read_manifest

            written = {
                "row_count": read_manifest(entry_dir(proj, content_hash)).row_count,
                "schema": json.loads(entry_schema_path(proj, content_hash).read_text()),
            }
        if mint:
            set_alias(proj, alias, content_hash, kind=SOURCE_KIND)

    return {
        "alias": alias,
        "version": version,
        "hash": content_hash,
        "created": mint,
        "path": str(src),
        "digest": digest,
        "row_count": written["row_count"],
        "schema": written["schema"],
    }
