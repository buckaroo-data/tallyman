"""Ordered copies of sources (ADR-008 D2, ADR-007 D13): the one way a file enters a recipe.

A source (a parquet file or a CSV under the project) is read through its content-addressed clone (``data/.cas``), and
polars writes a parquet copy of it, in file order, with one more column at the end: ``__row_order``, ``0..N-1``. That
copy is what a recipe reads. Nothing reads the source or the clone directly, so:

- every file tallyman reads carries the column that pages sort by;
- the copy is keyed by the source's content digest and the reader options, so editing a source and running the same
  recipe forks the entry's hash (#168) and the old entry keeps the rows it was built from;
- the copy is cache (ADR-007 D13). It lives under ``compute_cache/``, so a project that is packed, cloned or emptied
  loses it, and ``ensure_materialized`` makes it again from the clone with the reader options the manifest records,
  then checks it against the content digest recorded when it was first written.

The layout of the copy is part of the reproducibility contract (ADR-009 D3, #187): an ungrouped float total depends on
the row-group boundaries of the file it reads, so the row-group size below is frozen with the snapshot format version.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import os
import uuid
from pathlib import Path

from tallyman_xorq.row_order import ROW_ORDER

perf_log = logging.getLogger("tallyman.perf")

# Pinned polars parquet-write settings for an ordered copy. Held constant so the layout, and therefore any float total
# computed straight from a source, is reproducible. Changing one is a corpus rebuild (SNAPSHOT_FORMAT_VERSION).
ORDERED_COPY_ROW_GROUP_ROWS = 122_880
_WRITE = {
    "compression": "zstd",
    "compression_level": 3,
    "row_group_size": ORDERED_COPY_ROW_GROUP_ROWS,
    "statistics": True,
}

ORDERED_COPIES_DIRNAME = "ordered_sources"


class SourceUnavailable(FileNotFoundError):
    """A file the entry needs cannot be made again: its clone is gone and the live source is not the bytes it read."""


def ordered_copies_dir(project: str) -> Path:
    from tallyman_core.paths import compute_cache_dir

    return compute_cache_dir(project) / ORDERED_COPIES_DIRNAME


def is_ordered_copy_path(project: str, path: Path) -> bool:
    return Path(path).parent == ordered_copies_dir(project)


# ---------------------------------------------------------------------------
# readers: what the manifest records so a copy can be made again
# ---------------------------------------------------------------------------


def parquet_reader() -> dict:
    return {"kind": "parquet"}


def csv_reader(schema, scan_kwargs: dict) -> dict:
    """The reader options of a CSV source, in a form that goes into JSON and can be replayed.

    ``lossless`` is False when a ``scan_csv`` option does not survive JSON (the copy is then keyed correctly but cannot
    be made again from the manifest alone, and the error says so).
    """
    try:
        json.dumps(scan_kwargs, sort_keys=True)
        lossless = True
    except TypeError:
        lossless = False
    return {
        "kind": "csv",
        "schema": _spec_to_json(schema),
        "scan_kwargs": json.loads(json.dumps(scan_kwargs, sort_keys=True, default=repr)),
        "lossless": lossless,
    }


def _spec_to_json(spec):
    if spec is None:
        return None
    if isinstance(spec, (tuple, list)):
        return {"form": "positional", "cells": [[str(n), str(d)] for n, d in spec]}
    if hasattr(spec, "names") and hasattr(spec, "types"):
        return {"form": "named", "cells": [[str(n), str(t)] for n, t in zip(spec.names, spec.types)]}
    if isinstance(spec, dict):
        return {"form": "named", "cells": [[str(k), str(v)] for k, v in spec.items()]}
    raise ValueError(f"tallyman_read_csv: unsupported schema spec type {type(spec).__name__!r}.")


def _spec_from_json(doc):
    if doc is None:
        return None
    cells = [(n, d) for n, d in doc["cells"]]
    return tuple(cells) if doc["form"] == "positional" else dict(cells)


def _reader_signature(reader: dict) -> str:
    return json.dumps({k: v for k, v in reader.items() if k != "lossless"}, sort_keys=True)


def copy_key(digest: str, reader: dict) -> str:
    """The copy's file stem: a function of the source's content and the reader options, nothing else."""
    return hashlib.md5(f"{digest}|{_reader_signature(reader)}".encode()).hexdigest()  # noqa: S324 — a cache key


# ---------------------------------------------------------------------------
# collecting the records a build needs for its manifest
# ---------------------------------------------------------------------------

_collector: contextvars.ContextVar[dict[str, dict] | None] = contextvars.ContextVar(
    "tallyman_ordered_copy_collector", default=None
)


def begin_collect() -> contextvars.Token:
    return _collector.set({})


def note_ordered_copy(key: str, record: dict) -> None:
    bag = _collector.get()
    if bag is not None:
        bag[key] = record


def end_collect(token: contextvars.Token) -> dict[str, dict]:
    bag = _collector.get() or {}
    _collector.reset(token)
    return dict(bag)


# ---------------------------------------------------------------------------
# writing
# ---------------------------------------------------------------------------


def _write_parquet_copy(src: Path, dest: Path) -> None:
    import polars as pl

    lf = pl.scan_parquet(str(src))
    columns = [c for c in lf.collect_schema().names() if c != ROW_ORDER]  # an existing __row_order is overwritten
    lf.select(columns).with_row_index(ROW_ORDER).select([*columns, pl.col(ROW_ORDER).cast(pl.Int64)]).sink_parquet(
        str(dest), **_WRITE
    )


def _write_copy(src: Path, reader: dict, dest: Path) -> None:
    if reader["kind"] == "parquet":
        _write_parquet_copy(src, dest)
        return
    from tallyman_xorq.io import _materialize_ordered

    _materialize_ordered(src, _spec_from_json(reader["schema"]), dict(reader["scan_kwargs"]), dest)


def _digest_sidecar(path: Path) -> Path:
    return path.with_suffix(".digest")


def _content_digest_of(path: Path) -> str:
    """The copy's content digest: the sidecar written with it, or a read-back when the sidecar is gone."""
    from tallyman_xorq.digest import content_digest

    sidecar = _digest_sidecar(path)
    try:
        return sidecar.read_text().strip()
    except OSError:
        digest = content_digest(path)
        try:
            sidecar.write_text(digest)
        except OSError:
            pass
        return digest


def _write_atomically(src: Path, reader: dict, target: Path) -> str:
    """Write the copy to a unique temp name beside its destination, replace, and return its content digest."""
    from tallyman_xorq.digest import content_digest

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.stem}.{uuid.uuid4().hex}.tmp")
    try:
        _write_copy(src, reader, tmp)
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    digest = content_digest(target)
    _digest_sidecar(target).write_text(digest)
    return digest


def ensure_ordered_copy(project: str, source: Path, *, digest: str, rel: str, reader: dict) -> Path:
    """The ordered copy of *source* (whose content digest is *digest*), written if it is not on disk yet.

    *source* is the file polars reads: the clone in cas mode, the live file otherwise. The copy is recorded for the
    build in progress, so its manifest can make it again.
    """
    from tallyman_core.catalog_state import project_lock

    key = copy_key(digest, reader)
    target = ordered_copies_dir(project) / f"{key}.parquet"
    if not target.exists():
        with project_lock(project):
            if not target.exists():  # a peer may have written it while we waited
                _write_atomically(source, reader, target)
    note_ordered_copy(
        key,
        {
            "source": rel,
            "digest": digest,
            "suffix": Path(source).suffix,
            "reader": reader,
            "content_digest": _content_digest_of(target),
        },
    )
    return target


def existing_ordered_copy(project: str, *, digest: str, reader: dict) -> Path:
    """The path a copy of a source with this digest and reader has, without touching the source (reconstruction)."""
    return ordered_copies_dir(project) / f"{copy_key(digest, reader)}.parquet"


# ---------------------------------------------------------------------------
# making one again
# ---------------------------------------------------------------------------


def _live_source(project: str, source: str) -> Path:
    from tallyman_core.paths import data_dir

    p = Path(source)
    return p if p.is_absolute() else data_dir(project) / source


def _clone_or_live(project: str, record: dict) -> Path:
    """The file to re-read a source from: its clone, made again from the live source if that still has the bytes."""
    from tallyman_core.paths import data_dir
    from tallyman_xorq import source_identity as si

    digest, suffix = record["digest"], record.get("suffix", "")
    clone = data_dir(project) / ".cas" / f"{digest}{suffix}"
    if clone.exists():
        return clone
    live = _live_source(project, record["source"])
    if live.is_file() and si._digest_file(live) == digest:
        return si.ensure_cas_path(project, live, digest) if si.mode() == "cas" else live
    raise SourceUnavailable(
        f"the source file {record['source']!r} cannot be read the way it was when the entry was built: its "
        f"content-addressed clone ({digest}{suffix}) is gone and the file on disk no longer has those bytes. The "
        "rows the entry was built from are unrecoverable; rebuild the entry from the current file."
    )


def _find_record(project: str, owner_hash: str, key: str) -> dict | None:
    """The record of the copy *key*: from the owner's manifest, else from any entry in the catalog that recorded it.

    The build that first reads a source records its copy, and entries are never deleted, so the record is always in
    some manifest, whichever path (``tracked_expr_from_alias``, a diff, ``cached_result_expr`` in a recipe) carried the
    copy into the owner's plan. A copy's key is a function of the source digest and the reader options, so any
    manifest's record of it makes the same file.
    """
    from tallyman_core.manifest import read_manifest
    from tallyman_core.paths import entries_dir, entry_dir

    def records_of(entry: Path) -> dict:
        try:
            return read_manifest(entry).ordered_copies or {}
        except (OSError, ValueError):
            return {}

    record = records_of(entry_dir(project, owner_hash)).get(key)
    if record is not None:
        return record
    base = entries_dir(project)
    for entry in sorted(base.iterdir()) if base.is_dir() else []:
        record = records_of(entry).get(key)
        if record is not None:
            return record
    return None


def recreate_ordered_copy(project: str, owner_hash: str, path: Path) -> None:
    """Make a deleted ordered copy again for the entry *owner_hash*, and check it against its recorded digest.

    Uses the reader options recorded by the entry that wrote the copy (``_find_record``). A copy whose digest differs
    from the one recorded when it was first written is served all the same, but never silently (ADR-007 D5): a durable
    error record and a warning.
    """
    from tallyman_core.catalog_state import project_lock

    key = Path(path).stem
    record = _find_record(project, owner_hash, key)
    if record is None:
        raise SourceUnavailable(
            f"entry {owner_hash} reads {path.name} but no manifest in the catalog records the source it was made "
            "from, so it cannot be made again; rebuild the entry"
        )
    reader = record["reader"]
    if reader.get("lossless") is False:
        raise SourceUnavailable(
            f"the reader options of {record['source']!r} were not JSON-serializable, so {path.name} cannot be made "
            "again from the manifest; rebuild the entry"
        )
    with project_lock(project):
        if path.exists():
            return
        src = _clone_or_live(project, record)
        digest = _write_atomically(src, reader, path)
    if digest != record["content_digest"]:
        message = (
            f"the ordered copy of {record['source']!r} was made again with content digest {digest}, not the "
            f"{record['content_digest']} recorded when it was first written (a change in polars, or a source that is "
            "not what it was)"
        )
        perf_log.warning("recreate_ordered_copy %s: %s", key, message)
        try:
            from tallyman_core.errors import record_error

            record_error(project, code="unfaithful_ordered_copy", message=message, hash=owner_hash)
        except Exception:
            perf_log.debug("unfaithful ordered copy record failed for %s", key, exc_info=True)


def describe_read(project: str, path: Path, records: dict[str, dict] | None = None) -> str:
    """A phrase naming what a Read of *path* is, for an error that has to say what an entry reads.

    ``records`` are the ordered copies the build in progress collected, so a copy is named by the source it was made
    from and not by its key.
    """
    from tallyman_core.aliases import alias_for_hash
    from tallyman_xorq.materialize import snapshots_dir

    path = Path(path)
    if path.parent == snapshots_dir(project):
        alias = alias_for_hash(project, path.stem)
        return f"entry {path.stem}" + (f" ({alias})" if alias else "")
    record = (records or {}).get(path.stem)
    if record is not None:
        return f"the source {record['source']}"
    return f"the file {path.name}"
