"""Tallyman owns result materialization (ADR-007 D4 and D5, ADR-009 D1, D3 and D6).

A **worthy** entry has a **snapshot**: a parquet file, ``compute_cache/result_cache/<content_hash>.parquet``, written
once when the entry is created and read by everything after. Nothing else writes it. xorq's cache nodes are not in
any build, so no other process runs an entry's expensive computation, writes result files or repairs tallyman's cache
(ADR-007, the governing rule).

``materialize`` is that one writer. The build calls it and so does every heal, so the contract's "result bytes are
manufactured exactly once" has one routine to hold to. It runs the entry's frozen build on a single-partition
connection, so a float aggregate merges its partial sums in one order (ADR-009 D1), streams the rows through a writer
that fixes the layout of the file (ADR-009 D3) and numbers them in a last column, ``__row_order`` (ADR-008 D2), and
returns the content digest of the file it wrote, read back (ADR-009 D2).

``ensure_materialized`` is the one entry point that makes files exist: every snapshot an entry's plan reads is on
disk before anything executes (ADR-007 D5).
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from tallyman_xorq.row_order import ROW_ORDER, ROW_ORDER_RIGHT

perf_log = logging.getLogger("tallyman.perf")

# The snapshot format (ADR-009 D3). The row-group size decides the batch boundaries an entry built on this file sees,
# and an ungrouped float total depends on them (#187), so the row-group size and the materialization connection's batch
# size are part of the reproducibility contract: changing either is a corpus rebuild, and the version below stands for
# both (and for the row groups a source snapshot is written in, ``ordered_copy.ORDERED_COPY_ROW_GROUP_ROWS``).
SNAPSHOT_ROW_GROUP_ROWS = 1_048_576
SNAPSHOT_BATCH_SIZE = 8192
SNAPSHOT_FORMAT_VERSION = 1

_PARQUET_OPTIONS = {
    "compression": "zstd",
    "compression_level": 3,
    "version": "2.6",
    "data_page_version": "1.0",
    "write_statistics": True,
    "write_page_index": True,  # what lets a page be fetched as a range of __row_order without decoding a row group
}

RESULT_CACHE_DIRNAME = "result_cache"


def snapshots_dir(project: str) -> Path:
    from tallyman_core.paths import compute_cache_dir

    return compute_cache_dir(project) / RESULT_CACHE_DIRNAME


def snapshot_path(project: str, content_hash: str) -> Path:
    """Where the entry's snapshot lives: a function of the content hash and nothing else (ADR-007 D2)."""
    return snapshots_dir(project) / f"{content_hash}.parquet"


def engine_versions() -> dict[str, str]:
    """The versions of what decides an entry's bytes, recorded at build so a mismatch at a heal can be attributed."""
    from importlib.metadata import PackageNotFoundError, version

    out = {}
    for key, dist in (("xorq", "xorq"), ("xorq_datafusion", "xorq-datafusion"), ("pyarrow", "pyarrow")):
        try:
            out[key] = version(dist)
        except PackageNotFoundError:
            out[key] = "unknown"
    return out


def single_partition_backend():
    """A fresh datafusion backend that runs every query as one stream, with an explicit batch size (ADR-009 D1).

    With one partition the partial results of an aggregate are merged in one order, so a float ``SUM`` or ``AVG`` is
    bit-stable on any machine, and a plan that keeps rows streams them in the parent file's order. It is a separate
    connection from the default backend that serves page reads: a long materialization must not share a context with
    them, and the default keeps the machine's core count of partitions.
    """
    from tallyman_xorq.backend import connect

    con = connect()
    con.raw_sql("SET datafusion.execution.target_partitions = 1")
    con.raw_sql(f"SET datafusion.execution.batch_size = {SNAPSHOT_BATCH_SIZE}")
    return con


@dataclass
class Materialized:
    """What one materialization wrote."""

    path: Path
    digest: str  # the content digest of the file as read back, ``arrow-sha256:<hex>``
    row_count: int
    schema: pa.Schema  # read from the written file (parquet changes some types: timestamp[s] comes back [ms])
    # Only when ``check_reproducible``: whether the second run wrote the same digest, and the columns that differed.
    reproducible: bool | None = None
    differing_columns: list[str] = field(default_factory=list)


def row_groups(batches, rows: int):
    """The rows of *batches*, in order, as tables of *rows* rows each; the last one may be shorter.

    The one place a stream is cut into row groups, so a file's layout is a function of its rows and not of how the
    producer batched them (ADR-009 D3). Memory is bounded by one row group.
    """
    pending: list[pa.RecordBatch] = []
    pending_rows = 0
    for batch in batches:
        if not batch.num_rows:
            continue
        pending.append(batch)
        pending_rows += batch.num_rows
        while pending_rows >= rows:
            table = pa.Table.from_batches(pending)
            yield table.slice(0, rows)
            tail = table.slice(rows)
            pending, pending_rows = tail.to_batches(), tail.num_rows
    if pending_rows:
        yield pa.Table.from_batches(pending)


def write_pinned_parquet(batches, schema: pa.Schema, dest: Path, *, row_group_rows: int) -> None:
    """Write *batches*, in order, to *dest* in the pinned parquet settings of ``_PARQUET_OPTIONS`` (ADR-009 D3).

    Every file under ``compute_cache/`` goes through here or through ``_stream_to_parquet``, so ``result_cache/``
    holds one shape of parquet: the same format version, page index and compression, whether the rows were computed
    by an entry's build or parsed out of an imported file (ADR-011 D1). Each row group is combined into contiguous
    arrays first, so the bytes do not depend on the producer's chunking.
    """
    with pq.ParquetWriter(dest, schema, **_PARQUET_OPTIONS) as writer:
        for table in row_groups(batches, row_group_rows):
            if not table.schema.equals(schema, check_metadata=False):
                table = table.cast(schema)
            writer.write_table(table.combine_chunks(), row_group_size=row_group_rows)


def _stream_to_parquet(expr, dest: Path) -> tuple[int, pa.Schema]:
    """Write the rows of *expr* to *dest* in the snapshot format, numbering them in a last ``__row_order`` column.

    The writer drops an inherited ``__row_order`` (each materialization overwrites it with positions in its own file)
    and ibis's ``__row_order_right`` (a join's leftover copy of the right side's, which would collide in the next join,
    ADR-008 D6). It regroups the stream into row groups of ``SNAPSHOT_ROW_GROUP_ROWS`` and combines each into
    contiguous arrays, so the file does not depend on how the engine batched the rows. Memory is bounded by one row
    group.
    """
    reader = expr.to_pyarrow_batches()
    kept = [f for f in reader.schema if f.name not in (ROW_ORDER, ROW_ORDER_RIGHT)]
    out_schema = pa.schema([*kept, pa.field(ROW_ORDER, pa.int64())])
    names = [f.name for f in kept]
    written = 0

    with pq.ParquetWriter(dest, out_schema, **_PARQUET_OPTIONS) as writer:
        for table in row_groups(reader, SNAPSHOT_ROW_GROUP_ROWS):
            n = table.num_rows
            numbered = table.select(names).append_column(
                pa.field(ROW_ORDER, pa.int64()), pa.array(np.arange(written, written + n, dtype=np.int64))
            )
            if not numbered.schema.equals(out_schema, check_metadata=False):
                numbered = numbered.cast(out_schema)
            writer.write_table(numbered.combine_chunks(), row_group_size=SNAPSHOT_ROW_GROUP_ROWS)
            written += n
    return written, out_schema


def _run_once(project: str, content_hash: str, dest: Path) -> int:
    """Run the entry's frozen build on a single-partition connection and write the result to *dest*."""
    from tallyman_xorq.result_cache import load_entry_expr, rebind_onto

    expr = rebind_onto(load_entry_expr(project, content_hash), single_partition_backend())
    rows, _ = _stream_to_parquet(expr, dest)
    return rows


def _temp_beside(path: Path) -> Path:
    # A unique temp name in the destination directory: same filesystem for the replace, and no two writers share one.
    return path.with_name(f".{path.stem}.{uuid.uuid4().hex}.tmp")


def materialize(project: str, content_hash: str, *, check_reproducible: bool = False) -> Materialized:
    """Run the entry's build and write its snapshot; return what was written (ADR-007 D4).

    It always runs the query and replaces whatever is at the path, so an entry that is added again after a reset is
    honest and the reproducibility check below has something to compare. It takes the project's write lock, and the
    file only ever changes by an atomic replace of a complete one. It does not read the manifest: a create calls it
    before the manifest exists.

    With ``check_reproducible`` (what a create does, ADR-009 D6) it runs the query twice through the same writer and
    compares the two content digests. The second file is discarded. When they differ the entry is not reproducible,
    and ``differing_columns`` names the columns whose digests differ. A heal runs the query once.
    """
    from tallyman_core.catalog_state import project_lock
    from tallyman_xorq.digest import file_digests

    dest = snapshot_path(project, content_hash)
    with project_lock(project):
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = _temp_beside(dest)
        try:
            rows = _run_once(project, content_hash, tmp)
            digest, columns = file_digests(tmp)
            reproducible: bool | None = None
            differing: list[str] = []
            if check_reproducible:
                second = _temp_beside(dest)
                try:
                    _run_once(project, content_hash, second)
                    digest_again, columns_again = file_digests(second)
                finally:
                    second.unlink(missing_ok=True)
                reproducible = digest_again == digest
                if not reproducible:
                    differing = [c for c in columns if columns[c] != columns_again.get(c)] or list(columns)
            os.replace(tmp, dest)
        finally:
            tmp.unlink(missing_ok=True)
    return Materialized(dest, digest, rows, pq.read_schema(dest), reproducible, differing)


# ---------------------------------------------------------------------------
# ensure_materialized
# ---------------------------------------------------------------------------


def _heal(project: str, content_hash: str) -> None:
    """Re-create the entry's snapshot, verified against the manifest's ``result_digest`` before it is served."""
    from tallyman_core.catalog_state import project_lock
    from tallyman_xorq.result_cache import _verify_self_heal

    with project_lock(project):
        if snapshot_path(project, content_hash).exists():  # a peer thread or process healed it while we waited
            return
        result = materialize(project, content_hash)
        perf_log.debug("ensure_materialized healed %s", content_hash)
        _verify_self_heal(project, content_hash, result.digest)


def _recreate(project: str, owner_hash: str, path: Path) -> None:
    """Make a missing file the owner's plan reads again (ADR-007 D13).

    There is one class of file to make again: another entry's snapshot, named by its content hash, made
    by recursing on it. A source's ordered copy was the second class and is gone — a source is an entry,
    so its rows come back through this same path (``_heal_a_source`` under ``_ensure``), ADR-011 D1.
    """
    from tallyman_xorq.build import BuildError

    if path.parent == snapshots_dir(project):
        from tallyman_core.paths import entry_dir

        parent = path.stem
        if not entry_dir(project, parent).is_dir():
            raise BuildError(
                f"entry {owner_hash} in {project!r} reads the snapshot of entry {parent}, which is not in this "
                "catalog, so it cannot be made again"
            )
        ensure_materialized(project, parent)
    else:
        raise BuildError(
            f"entry {owner_hash} in {project!r} reads {path}, which tallyman did not write and cannot make again"
        )
    if not path.exists():
        raise BuildError(f"entry {owner_hash} in {project!r}: {path} is still missing after it was made again")


def _source_provenance(project: str, content_hash: str):
    """The import recorded on the entry, or None when it is not a source version."""
    from tallyman_core import read_manifest
    from tallyman_core.paths import entry_dir

    try:
        return read_manifest(entry_dir(project, content_hash)).provenance
    except (OSError, ValueError):
        return None


def _source_names(project: str, content_hash: str, provenance) -> tuple[tuple[str, int] | None, str]:
    """``(held, imported)``: how a message names a source version (ADR-011 D1).

    ``held`` is ``(alias, version)`` as the alias store has it now, or None when no source alias holds the entry
    (``source_import.current_source_version``). ``imported`` is the ``<alias>-v<N>`` the version was imported as,
    from ``provenance``: history, which a rename or an unalias leaves behind. A message names the version by
    ``held``, and mentions ``imported`` only where the two differ.
    """
    from tallyman_xorq.source_import import current_source_version

    return current_source_version(project, content_hash, provenance), f"{provenance.alias}-v{provenance.version}"


def _heal_a_source(project: str, content_hash: str) -> bool:
    """Re-create a source version's snapshot from the clone of the bytes it was imported from (ADR-011 D1).

    A source snapshot is cache in the sense of ADR-007 D13 — ``ensure_materialized`` can re-create it — because the
    clone under ``data/.cas`` holds the imported bytes and the entry records the reader options that read them. It
    cannot go through ``materialize``: the entry's build reads the very snapshot that is missing, so the rows come
    from the clone instead. What is written is then verified against the recorded ``result_digest`` like any other
    re-created snapshot.

    Only a clone that is gone as well makes the version unrecoverable, and then the error names the file that is
    missing and the re-import that repairs it: the alias and version that hold the entry now (``_source_names``), so
    the advised call re-imports into the version it names rather than minting an alias under the name the version
    was imported as, and the reader options the entry recorded (``source_import.import_call``), without which a CSV
    names another entry. When no source alias holds it there is no version to re-import into, and the error says what
    an import of the same bytes would do instead. Returns whether this entry is a source version, so the general
    path can stop.
    """
    from tallyman_core.catalog_state import project_lock
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.result_cache import _verify_self_heal
    from tallyman_xorq.source_import import import_call, rewrite_source_snapshot, source_clone_path

    provenance = _source_provenance(project, content_hash)
    if provenance is None:
        return False
    with project_lock(project):
        if snapshot_path(project, content_hash).exists():  # a peer healed it while we waited
            return True
        clone = source_clone_path(project, provenance)
        if not clone.is_file():
            held, imported = _source_names(project, content_hash, provenance)
            lost = (
                f"cannot be made again: {snapshot_path(project, content_hash)} is not on disk and neither is the "
                f"clone of the imported bytes, {clone}."
            )
            if held is None:
                raise BuildError(
                    f"the data imported as {imported} (entry {content_hash}), which no source alias holds now, "
                    f"{lost} Importing the same bytes again, "
                    f"{import_call(provenance.path, None, provenance.reader)}, writes these rows again as a new "
                    "version of <alias>."
                )
            alias, version = held
            also = "" if f"{alias}-v{version}" == imported else f", imported as {imported}"
            raise BuildError(
                f"the data of {alias}-v{version} (entry {content_hash}{also}) {lost} Import them again with "
                f"{import_call(provenance.path, alias, provenance.reader, pinned_version=version)}."
            )
        digest = rewrite_source_snapshot(project, content_hash, provenance)
        perf_log.debug("ensure_materialized re-imported %s from %s", content_hash, clone.name)
        _verify_self_heal(project, content_hash, digest)
    return True


def _ensure(project: str, content_hash: str) -> bool:
    """``ensure_materialized``, returning whether the entry is worthy (the caller usually needs to know)."""
    from tallyman_xorq.result_cache import _resolve_result_plan, cache_worthy

    worthy = cache_worthy(project, content_hash)
    if worthy and snapshot_path(project, content_hash).exists():
        return True
    if _heal_a_source(project, content_hash):
        return worthy
    plan = _resolve_result_plan(project, content_hash)
    for path in plan.reads:
        if not path.exists():
            _recreate(project, content_hash, path)
    if worthy and not snapshot_path(project, content_hash).exists():
        _heal(project, content_hash)
    return worthy


def ensure_materialized(project: str, content_hash: str) -> None:
    """Guarantee that every file an entry's plan reads, and its own snapshot, is on disk (ADR-007 D5).

    1. A worthy entry whose snapshot exists is done, and no build is loaded.
    2. Otherwise load the entry's build (the plan is kept in the existing LRU) and collect every file its ``Read``
       nodes point at.
    3. Re-create each that is missing: every one is another entry's snapshot, made by recursing on the hash in its
       file name. A source version is re-created from the clone of its imported bytes (``_heal_a_source``).
    4. If the entry is worthy, heal its own snapshot and verify it.

    Every caller that composes or executes an entry goes through here, so nothing ever runs over a file that is
    missing. When nothing can make a file again, the error names the source file.
    """
    _ensure(project, content_hash)


def pinned_reason(project: str, content_hash: str) -> str | None:
    """Why the entry's snapshot must not be deleted, or None when it may be (ADR-009 D6, ADR-007 D12).

    A snapshot is pinned when it cannot be made again faithfully: the recipe is not reproducible (two runs at create
    time gave different digests), a heal already produced different rows than were built, or it is a source
    version whose clone of the imported bytes is gone (ADR-011 D1 — with the clone it is ordinary cache, made again
    from those bytes). The Cache page's delete leaves such a file alone and says why, naming a source version by the
    alias that holds it now (``_source_names``). ``compute_cache/`` as a whole is still deletable by definition.
    """
    from tallyman_core import read_manifest
    from tallyman_core.errors import list_errors
    from tallyman_core.paths import entry_dir
    from tallyman_xorq.source_import import source_clone_path

    try:
        manifest = read_manifest(entry_dir(project, content_hash))
    except (OSError, ValueError):
        manifest = None
    if manifest is not None and manifest.provenance is not None:
        clone = source_clone_path(project, manifest.provenance)
        if not clone.is_file():
            held, imported = _source_names(project, content_hash, manifest.provenance)
            if held is None:
                name = f"the source version imported as {imported}, which no source alias holds now"
            else:
                current = f"{held[0]}-v{held[1]}"
                name = f"the source version {current}" + ("" if current == imported else f" (imported as {imported})")
            return (
                f"this file is the last copy of {name}: the clone of the bytes imported from "
                f"{manifest.provenance.path} is gone from {clone}, so nothing can make it again and it is kept"
            )
    if manifest is not None and manifest.reproducible is False:
        columns = ", ".join(manifest.nonreproducible_columns or [])
        return (
            "this entry's query is not reproducible (two runs at create time gave different results"
            + (f" in {columns}" if columns else "")
            + "), so its snapshot cannot be re-created faithfully and is kept"
        )
    for record in list_errors(project, limit=1_000_000_000):
        if record.get("code") == "unfaithful_heal" and record.get("hash") == content_hash:
            return "a heal of this snapshot produced different rows than were built, so it is kept"
    return None
