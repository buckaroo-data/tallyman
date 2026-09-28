"""ADR-008 evidence (Consequences): what a page request holds in memory at depth.

ADR-008 D5 serves every page as ``ORDER BY __row_order LIMIT n OFFSET k``. Its cost table measures latency. This
script measures peak process memory for the same requests, on the same file shape as ``spike_row_order_paging.py``,
and for a range request (``__row_order >= k AND __row_order < k + n``), which ADR-008 notes as a later optimization.

Each case runs in a fresh process, so its peak is its own; the first case imports xorq and does nothing, as the floor.

    uv run python scripts/spike_deep_page_memory.py
"""

from __future__ import annotations

import resource
import subprocess
import sys
import tempfile
from pathlib import Path

ROW = "__row_order"
N = 3_000_000
CASES = (
    "import only",
    "bare limit 50",
    "sorted 0",
    "sorted 1000000",
    "sorted 2900000",
    "range 2900000",
    "chart 100000",
)


def run_case(path: str, case: str) -> None:
    import xorq.api as xo

    kind, _, arg = case.partition(" ")
    if kind != "import":
        t = xo.connect().read_parquet(path)
        if kind == "bare":
            t.limit(50).execute()
        elif kind == "sorted":
            t.order_by(ROW).limit(50, offset=int(arg)).execute()
        elif kind == "range":
            k = int(arg)
            t.filter((t[ROW] >= k) & (t[ROW] < k + 50)).order_by(ROW).execute()
        elif kind == "chart":
            t.order_by(ROW).limit(int(arg)).execute()  # the chart pull through /api/data
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_mb = peak / 1e6 if sys.platform == "darwin" else peak / 1e3  # bytes on macOS, kilobytes on Linux
    print(f"  {case:16s} peak process memory {peak_mb:7.0f} MB")


def main() -> None:
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    rng = np.random.default_rng(9)
    cols = {"g": rng.integers(0, 200, N)}
    cols |= {f"v{i}": rng.random(N) for i in range(12)}
    cols[ROW] = np.arange(N)
    table = pa.table(cols)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "snapshot.parquet"
        pq.write_table(table, path, compression="zstd", row_group_size=1_048_576, write_page_index=True)
        on_disk, as_arrow = path.stat().st_size / 1e6, table.nbytes / 1e6
        print(f"file: {on_disk:.0f} MB on disk, {as_arrow:.0f} MB as Arrow, {N:,} rows x {table.num_columns} columns")
        for case in CASES:
            out = subprocess.run([sys.executable, __file__, str(path), case], capture_output=True, text=True)
            print(out.stdout.rstrip() or out.stderr.strip()[-300:])


if __name__ == "__main__":
    if len(sys.argv) == 3:
        run_case(sys.argv[1], sys.argv[2])
    else:
        main()
