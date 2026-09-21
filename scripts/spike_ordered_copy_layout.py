"""ADR-008 (D2, open question 1) and ADR-009 (D3) evidence: the ordered copy of a source, which polars writes.

The ordered copy is not written by the snapshot writer of ADR-009 D3. An ungrouped float total depends on the layout
of the file it reads (#187), so the layout of an ordered copy is part of the reproducibility contract too.

Questions, asked with polars running on 1, 3 and the default number of threads:

1. CSV source: are the row groups exactly the size asked for, and is the layout the same on every thread count?
2. CSV source: is the row index 0..N-1 in file order?
3. Parquet source with many row groups: does ``scan_parquet().with_row_index()`` number the rows in FILE order?
   ADR-008 open question 1 said this needed checking.

polars reads ``POLARS_MAX_THREADS`` when it is imported, so each thread count runs in a child process.

    uv run python scripts/spike_ordered_copy_layout.py
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

N = 1_500_000
WRITE = {"compression": "zstd", "compression_level": 3, "row_group_size": 122880, "statistics": True}  # io.py


def child() -> None:
    import numpy as np
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    home = Path(tempfile.mkdtemp(prefix="spike_ordered_copy_"))
    rng = np.random.default_rng(7)
    positions = np.arange(N)

    def layout(path: Path) -> tuple[list[int], str]:
        md = pq.ParquetFile(path).metadata
        sizes = [md.row_group(i).num_rows for i in range(md.num_row_groups)]
        return sizes, hashlib.sha256(repr(sizes).encode()).hexdigest()[:12]

    def ordered_copy(scan, out: Path) -> None:
        indexed = scan.with_row_index("__row_order")
        indexed.select(["id", "v", pl.col("__row_order").cast(pl.Int64)]).sink_parquet(str(out), **WRITE)

    csv = home / "src.csv"
    pl.DataFrame({"id": positions, "v": rng.normal(size=N)}).write_csv(csv)
    from_csv = home / "from_csv.parquet"
    ordered_copy(pl.scan_csv(str(csv)), from_csv)

    parquet = home / "src.parquet"
    pq.write_table(pa.table({"id": positions, "v": rng.normal(size=N)}), parquet, row_group_size=8192)
    from_parquet = home / "from_parquet.parquet"
    ordered_copy(pl.scan_parquet(str(parquet)), from_parquet)

    result = {"threads": pl.thread_pool_size(), "polars": pl.__version__}
    for name, path in (("csv", from_csv), ("parquet", from_parquet)):
        sizes, signature = layout(path)
        table = pq.read_table(path)
        result[name] = {
            "row_groups": len(sizes),
            "full_groups_exact": all(s == WRITE["row_group_size"] for s in sizes[:-1]),
            "layout": signature,
            "index_is_0_to_n": bool((table["__row_order"].to_numpy() == positions).all()),
            "index_is_file_position": bool((table["__row_order"].to_numpy() == table["id"].to_numpy()).all()),
        }
    result["source_row_groups"] = pq.ParquetFile(parquet).metadata.num_row_groups
    print(json.dumps(result))


def main() -> None:
    print(f"{N:,} rows; row groups of {WRITE['row_group_size']:,} asked for\n")
    for threads in ("1", "3", None):
        env = dict(os.environ)
        if threads is None:
            env.pop("POLARS_MAX_THREADS", None)
        else:
            env["POLARS_MAX_THREADS"] = threads
        done = subprocess.run(
            [sys.executable, __file__, "--child"], env=env, capture_output=True, text=True, check=True
        )
        r = json.loads(done.stdout.strip().splitlines()[-1])
        groups = r["source_row_groups"]
        print(f"polars {r['polars']} on {r['threads']} thread(s); the parquet source has {groups} row groups")
        for name in ("csv", "parquet"):
            s = r[name]
            print(
                f"   {name:8s} source: {s['row_groups']} row groups, full groups exact={s['full_groups_exact']}, "
                f"layout {s['layout']}, index 0..N-1={s['index_is_0_to_n']}, "
                f"index equals file position={s['index_is_file_position']}"
            )


if __name__ == "__main__":
    child() if "--child" in sys.argv else main()
