"""Reader options, and the row-group size a source snapshot is written in.

An import names how a file is read — once, in the import call — and records it on the source entry
(ADR-011 D12). This module holds that record's shape: ``parquet_reader`` / ``csv_reader`` build it,
``_spec_to_json`` / ``_spec_from_json`` round-trip a CSV schema spec through JSON, and
``_reader_signature`` is the part of it the source entry's content hash covers, so two recipes cannot
read one file two ways and a function-valued option cannot fork the hash on every build (#215).

What used to be here was the **ordered copy** of ADR-008 D2: every source read through its clone into a
second parquet store, ``compute_cache/ordered_sources/<key>.parquet``, keyed by
``md5(digest ǀ reader signature)``. ADR-011 D1 subsumes it. A raw input is an entry now, the entry hash
names its one snapshot under ``compute_cache/result_cache/``, and the reader options live on the entry
that used them — so the second store, the copy key, and the machinery that made a deleted copy again
(``ensure_ordered_copy``, ``recreate_ordered_copy``, the per-build ``ordered_copies`` manifest record)
all have nothing left to address. ``ensure_materialized`` re-creates a source snapshot from the clone
instead (``materialize._heal_a_source``).

The layout of that snapshot is still part of the reproducibility contract (ADR-009 D3, #187): an
ungrouped float total depends on the row-group boundaries of the file it reads, so the row-group size
below is frozen with the snapshot format version.
"""

from __future__ import annotations

import json
from pathlib import Path

# Row-group size of a source snapshot. Held constant so the layout, and therefore any float total
# computed straight from a source, is reproducible. Changing it is a corpus rebuild
# (``materialize.SNAPSHOT_FORMAT_VERSION``).
ORDERED_COPY_ROW_GROUP_ROWS = 122_880


# ---------------------------------------------------------------------------
# readers: what the entry records so its snapshot can be written again
# ---------------------------------------------------------------------------


def parquet_reader() -> dict:
    return {"kind": "parquet"}


def csv_reader(schema, scan_kwargs: dict) -> dict:
    """The reader options of a CSV source, in a form that goes into JSON and can be replayed.

    ``lossless`` is False when a ``scan_csv`` option does not survive JSON; the import refuses such a
    call outright (``source_import._reader_for``), because an entry that cannot say how its file was
    read cannot write its snapshot again.
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
    """The part of a reader a source entry's content hash covers (``source_import.source_entry_hash``)."""
    return json.dumps({k: v for k, v in reader.items() if k != "lossless"}, sort_keys=True)


# ---------------------------------------------------------------------------
# naming a Read in an error
# ---------------------------------------------------------------------------


def describe_read(project: str, path: Path) -> str:
    """A phrase naming what a Read of *path* is, for an error that has to say what an entry reads.

    Every file a build reads is a snapshot now, so the phrase names the entry that owns it and its
    alias; anything else is named by its file name and is, by construction, a bug.
    """
    from tallyman_core.aliases import alias_for_hash
    from tallyman_xorq.materialize import snapshots_dir

    path = Path(path)
    if path.parent == snapshots_dir(project):
        alias = alias_for_hash(project, path.stem)
        return f"entry {path.stem}" + (f" ({alias})" if alias else "")
    return f"the file {path.name}"
