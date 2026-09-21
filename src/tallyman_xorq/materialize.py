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

``ensure_materialized`` is the one entry point that makes files exist: every snapshot, ordered copy and clone an
entry's plan reads is on disk before anything executes (ADR-007 D5).
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
# both (and for the layout of the ordered copies of sources, ``ordered_copy.ORDERED_COPY_ROW_GROUP_ROWS``).
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

        def flush(table: pa.Table) -> None:
            nonlocal written
            n = table.num_rows
            numbered = table.select(names).append_column(
                pa.field(ROW_ORDER, pa.int64()), pa.array(np.arange(written, written + n, dtype=np.int64))
            )
            if not numbered.schema.equals(out_schema, check_metadata=False):
                numbered = numbered.cast(out_schema)
            writer.write_table(numbered.combine_chunks(), row_group_size=SNAPSHOT_ROW_GROUP_ROWS)
            written += n

        pending: list[pa.RecordBatch] = []
        pending_rows = 0
        for batch in reader:
            if not batch.num_rows:
                continue
            pending.append(batch)
            pending_rows += batch.num_rows
            while pending_rows >= SNAPSHOT_ROW_GROUP_ROWS:
                table = pa.Table.from_batches(pending)
                flush(table.slice(0, SNAPSHOT_ROW_GROUP_ROWS))
                tail = table.slice(SNAPSHOT_ROW_GROUP_ROWS)
                pending, pending_rows = tail.to_batches(), tail.num_rows
        if pending_rows:
            flush(pa.Table.from_batches(pending))
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
    """Make a missing file the owner's plan reads again, by the rule for its class (ADR-007 D13)."""
    from tallyman_xorq import ordered_copy as oc
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
    elif oc.is_ordered_copy_path(project, path):
        oc.recreate_ordered_copy(project, owner_hash, path)
    else:
        raise BuildError(
            f"entry {owner_hash} in {project!r} reads {path}, which tallyman did not write and cannot make again"
        )
    if not path.exists():
        raise BuildError(f"entry {owner_hash} in {project!r}: {path} is still missing after it was made again")


def _ensure(project: str, content_hash: str) -> bool:
    """``ensure_materialized``, returning whether the entry is worthy (the caller usually needs to know)."""
    from tallyman_xorq.result_cache import _resolve_result_plan, cache_worthy

    worthy = cache_worthy(project, content_hash)
    if worthy and snapshot_path(project, content_hash).exists():
        return True
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
    3. Re-create each that is missing by the rule for its class: a snapshot by recursing on the hash in its file name,
       an ordered copy of a source from its clone, a clone from the live source while the bytes still match.
    4. If the entry is worthy, heal its own snapshot and verify it.

    Every caller that composes or executes an entry goes through here, so nothing ever runs over a file that is
    missing. When nothing can make a file again, the error names the source file.
    """
    _ensure(project, content_hash)


def pinned_reason(project: str, content_hash: str) -> str | None:
    """Why the entry's snapshot must not be deleted, or None when it may be (ADR-009 D6, ADR-007 D12).

    A snapshot is pinned when it cannot be made again faithfully: the recipe is not reproducible (two runs at create
    time gave different digests), or a heal already produced different rows than were built. The Cache page's delete
    leaves such a file alone and says why. ``compute_cache/`` as a whole is still deletable by definition.
    """
    from tallyman_core import read_manifest
    from tallyman_core.errors import list_errors
    from tallyman_core.paths import entry_dir

    try:
        manifest = read_manifest(entry_dir(project, content_hash))
    except (OSError, ValueError):
        manifest = None
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
