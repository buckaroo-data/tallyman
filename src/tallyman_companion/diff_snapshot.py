"""The live diff's join as a file that Buckaroo reads.

Handed the outer join of two entries, Buckaroo runs that join for each of its summary-stat queries (one batch
aggregate and one histogram per column, 80 to 130 queries on a diff of 19 to 45 columns), for its row counts, and for
every page, sort and search afterwards. Handed a parquet file, its whole plan is one bare read of it.

So tallyman runs the join once, here, and writes the rows to ``compute_cache/diff_cache/`` in the snapshot format
(``materialize.write_expr_snapshot``). Buckaroo gets a view build of that file, the same build a worthy entry's grid
gets (``buckaroo_lifecycle.ensure_parquet_view_build``): a graph with one read in it and no history of the join. The
file is named by the identity of both sides' rows and the join keys (``diff_identity``), so it is
never stale: a heal that changes a side's rows, or a different key, is another file. It is cache. Anything may delete
it, and the next open of the pair writes it again.

``plans/diff-performance-proposals.md``, section 4a, has the measurements behind this and what it leaves undone.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from tallyman_companion.buckaroo_lifecycle import diff_data_id, ensure_parquet_view_build
from tallyman_companion.diff import build_compare_expr, compute_column_config_overrides, strip_live_diff_color
from tallyman_core.execution import execution_lock
from tallyman_core.paths import diff_snapshot_path, diff_view_build_dir
from tallyman_xorq.diff import file_diff
from tallyman_xorq.materialize import write_expr_snapshot
from tallyman_xorq.row_order import ROW_ORDER

perf_log = logging.getLogger("tallyman.perf")

# Bump when ``build_compare_expr`` changes the columns, their order or their types: a file written by the old layout
# names the same rows and has the wrong shape.
DIFF_VIEW_VERSION = 1

# One diff file is written per path at a time: a second open of a pair that is still being written waits for the first
# and finds the file, instead of running the join again.
_write_locks_guard = threading.Lock()
_write_locks: dict[str, threading.Lock] = {}

# ``membership`` (``build_compare_expr``): 1 in the before side only, 2 in the after side only, 3 in both.
_ONLY_BEFORE, _ONLY_AFTER, _BOTH = 1, 2, 3


@dataclass(frozen=True)
class DiffView:
    """What a diff's Buckaroo session and its page need, once the join is a file."""

    identity: str  # ``diff_identity``: what the file and its session are named by, and Buckaroo's ``data_id``
    path: Path  # the parquet file of the join
    build_dir: Path  # the view build Buckaroo loads
    column_config_overrides: dict
    matched: int
    only_before: int
    only_after: int


def diff_identity(project: str, a_hash: str, b_hash: str, keys: tuple[str, ...] | list[str]) -> str:
    """What the diff's file, its view build and its Buckaroo session are named by, and what Buckaroo keys its stats by.

    ``diff_data_id`` names the rows of both sides and the join keys, so it changes when a heal changes a side's rows.
    The layout version covers the compare expression's own shape. Order matters: swapping the two sides is another diff.
    """
    rows = diff_data_id(project, a_hash, b_hash, tuple(keys))
    return hashlib.sha256(f"{DIFF_VIEW_VERSION}\0{rows}".encode()).hexdigest()


def diff_view_overrides(a_schema, b_schema, keys: list[str]) -> dict:
    """The ``column_config_overrides`` of the live compare grid.

    ``build_compare_expr``'s, without the numeric coloring that the live view's display klasses set per view
    (``strip_live_diff_color``), and with ``__row_order`` hidden: the file ends in one, as every file tallyman writes
    does, and the compare grid never showed one.
    """
    overrides = strip_live_diff_color(compute_column_config_overrides(a_schema, b_schema, keys))
    overrides[ROW_ORDER] = {"merge_rule": "hidden"}
    return overrides


def _ensure_snapshot(path: Path, a_expr, b_expr, keys: list[str]) -> None:
    """Write the join to *path* unless a complete file is already there."""
    if path.is_file():
        return
    with _write_locks_guard:
        lock = _write_locks.setdefault(str(path), threading.Lock())
    with lock:
        if path.is_file():  # a peer thread wrote it while we waited
            return
        expr, _ = build_compare_expr(a_expr, b_expr, keys)
        t0 = time.monotonic()
        rows = write_expr_snapshot(expr, path)
        perf_log.info(
            "diff snapshot %s: %d rows, %.1f MB, %.2fs",
            path.name,
            rows,
            path.stat().st_size / 1e6,
            time.monotonic() - t0,
        )


def _membership_counts(path: Path) -> dict[int, int]:
    """Rows per ``membership`` value in the file: one group-by over the file, not a join."""
    from xorq.expr.api import deferred_read_parquet

    t = deferred_read_parquet(str(path))
    with execution_lock():
        counts = t.group_by("membership").aggregate(n=t.count()).execute()
    return {int(m): int(n) for m, n in zip(counts["membership"], counts["n"], strict=True)}


def open_diff_view(project: str, a_hash: str, b_hash: str, keys: list[str], a_expr, b_expr) -> DiffView:
    """Make the diff's file and its view build exist, and read the three row counts off the file.

    *a_expr* and *b_expr* are the two entries' ``cached_result_expr``, so every file they read exists. The first open of
    a pair runs the join and writes the file (seconds at millions of rows); every later open reads a footer and a
    group-by.
    """
    identity = diff_identity(project, a_hash, b_hash, keys)
    path = diff_snapshot_path(project, a_hash, b_hash, identity)
    _ensure_snapshot(path, a_expr, b_expr, keys)
    build_dir = ensure_parquet_view_build(path, diff_view_build_dir(project, a_hash, b_hash, identity))
    counts = _membership_counts(path)
    return DiffView(
        identity=identity,
        path=path,
        build_dir=build_dir,
        column_config_overrides=diff_view_overrides(a_expr.schema(), b_expr.schema(), keys),
        matched=counts.get(_BOTH, 0),
        only_before=counts.get(_ONLY_BEFORE, 0),
        only_after=counts.get(_ONLY_AFTER, 0),
    )


def view_diff(view: DiffView, a_entry: Path, b_entry: Path, *, a_label: str, b_label: str, keys: list[str]) -> dict:
    """The ``diff`` the page gets when Buckaroo has the compare view: ``full_diff``'s shape, without its queries.

    The code and schema diffs are file reads, and the keyed counts come from the file. The page draws the stats table,
    the head and the keyed preview only when Buckaroo has no session, so they are empty here, in the shape the page
    expects, rather than computed and thrown away.
    """
    files = file_diff(a_entry, b_entry, a_label=a_label, b_label=b_label)
    row_count = files["schema"]["row_count"]
    return {
        **files,
        "stats": [],
        "head": {
            "n": 0,
            "a_total": row_count["before"] or 0,
            "b_total": row_count["after"] or 0,
            "before": "",
            "after": "",
        },
        "keyed": {
            "keys": list(keys),
            "matched": view.matched,
            "only_before": view.only_before,
            "only_after": view.only_after,
            "table_html": "",
        },
    }
