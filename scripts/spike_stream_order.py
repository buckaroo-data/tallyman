"""ADR-007 evidence (open question 1, the alternative that was not taken) and ADR-009 (D1).

On the connection ``materialize`` uses (``target_partitions = 1``), do the rows of a row-preserving plan reach the
writer in the parent file's order? If so, a writer could number rows in parent order with no ``__row_order`` column
carried through the recipe, which is what materializing every entry would have relied on. It is also why a bare
``limit`` or a window function with no order is repeatable at materialization: by the engine's behaviour, not by the
query's own meaning (ADR-008 D10).

The parent is 3,000,000 rows in 100,000-row groups, above the 10,485,760-byte scan-split threshold. ``id`` is the file
position. Each plan is streamed three times per connection.

    uv run python scripts/spike_stream_order.py
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_stream_order_"))
os.environ["XORQ_CACHE_DIR"] = str(HOME / "_global_xorq")  # must be set before xorq is imported

import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import xorq.api as xo  # noqa: E402

N = 3_000_000
RUNS = 3


def in_file_order(expr) -> bool:
    last = -1
    for batch in expr.to_pyarrow_batches():
        ids = batch.column("id").to_numpy()
        if len(ids) == 0:
            continue
        if ids[0] < last or (np.diff(ids) < 0).any():
            return False
        last = ids[-1]
    return True


def plans(t) -> dict:
    return {
        "bare read": t,
        "filter + computed column": t.filter(t.g < 150).mutate(w=t.v * 2),
        "select two columns": t.select("id", "s"),
        "string filter + cast": t.filter(t.s.endswith("7")).mutate(gf=t.g.cast("float64")),
        "window function with no order": t.mutate(c=t.v.cumsum()),
    }


def main() -> None:
    rng = np.random.default_rng(11)
    parent = HOME / "parent.parquet"
    columns = {
        "id": np.arange(N),
        "g": rng.integers(0, 200, size=N),
        "v": rng.normal(size=N),
        "s": pa.array([f"row-{i % 1000}" for i in range(N)]),
    }
    pq.write_table(pa.table(columns), parent, row_group_size=100_000)
    print(
        f"parent: {parent.stat().st_size / 1e6:.0f} MB, {pq.ParquetFile(parent).metadata.num_row_groups} row groups\n"
    )

    for label, partitions in (("target_partitions = 1", 1), ("default connection", None)):
        print(label)
        for name in plans(xo.deferred_read_parquet(str(parent))):
            results = []
            for _ in range(RUNS):
                con = xo.connect()
                if partitions is not None:
                    con.raw_sql(f"SET datafusion.execution.target_partitions = {partitions}")
                results.append(in_file_order(plans(xo.deferred_read_parquet(str(parent), con=con))[name]))
            print(f"   {name:32s} streamed in file order: {results}")


if __name__ == "__main__":
    main()
