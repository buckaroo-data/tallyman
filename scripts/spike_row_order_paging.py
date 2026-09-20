"""ADR-008 evidence: paging by a baked ``__row_order`` column.

``__row_order`` holds 0..N-1 in the physical row order of a file tallyman wrote. The script answers, on the
DEFAULT parallel connection with no engine settings:

1. Is ``ORDER BY __row_order LIMIT n OFFSET k`` repeatable and correct, and what does it cost?
2. Does declaring the file's order to the engine (``WITH ORDER``) remove the sort?
3. Does a user sort on a column with ties page repeatably, without and with ``__row_order`` as the last key?
4. What does a range request (``__row_order >= k AND __row_order < k + n``) cost at depth, without and with a
   parquet page index in the file?
5. Can tallyman alter a cheap recipe so the column survives a ``select`` that does not name it, and ends up last?
   (``carry_row_order`` below is the rewrite; a ``select`` is an allow-list, so an unnamed column is dropped
   whether or not anyone can see it.)

For reference it also times the first draft's approach, a bare LIMIT/OFFSET on a single-partition connection.

    uv run python scripts/spike_row_order_paging.py
"""

from __future__ import annotations

import hashlib
import statistics
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import xorq.api as xo
import xorq.vendor.ibis.expr.operations as ops
from xorq.common.utils.graph_utils import replace_nodes

ROW = "__row_order"
N = 3_000_000
REQUESTS = 6
OFFSETS = (0, 1_000_000, 2_900_000)


def carry_row_order(expr):
    """Rewrite a row-preserving expression so ``__row_order`` reaches the output as the last column."""

    def replacer(node, kwargs):
        if kwargs:
            node = node.__recreate__(kwargs)
        if isinstance(node, ops.Project) and ROW in node.parent.schema and ROW not in node.values:
            return ops.Project(node.parent, {**node.values, ROW: ops.Field(node.parent, ROW)})
        if isinstance(node, ops.DropColumns) and ROW in node.columns_to_drop:
            keep = frozenset(c for c in node.columns_to_drop if c != ROW)
            return ops.DropColumns(node.parent, keep) if keep else node.parent
        return node

    out = replace_nodes(replacer, expr).to_expr()
    if ROW in out.columns and out.columns[-1] != ROW:
        out = out.select([c for c in out.columns if c != ROW] + [ROW])
    return out


def measure(expr, first_col: str = ROW) -> tuple[int, list[int], float]:
    pages, firsts, secs = set(), set(), []
    for _ in range(REQUESTS):
        t0 = time.perf_counter()
        df = expr.execute()
        secs.append(time.perf_counter() - t0)
        pages.add(hashlib.md5(df.to_csv(index=False).encode()).hexdigest())
        firsts.add(int(df[first_col].iloc[0]))
    return len(pages), sorted(firsts)[:3], statistics.median(secs) * 1000


def report(label: str, expr, want: int) -> None:
    n, firsts, ms = measure(expr)
    print(f"   {label}: {n} distinct pages / {REQUESTS}, first {ROW} {firsts} (want {want}), median {ms:.0f} ms")


def operators(con, expr) -> str:
    plan = con.raw_sql("EXPLAIN " + xo.to_sql(expr)).to_pandas()
    physical = plan[plan.iloc[:, 0] == "physical_plan"].iloc[0, 1]
    return " <- ".join(line.strip().split(":")[0] for line in physical.splitlines() if line.strip())


def main() -> None:
    rng = np.random.default_rng(9)
    cols = {"g": rng.integers(0, 200, N)}
    cols |= {f"v{i}": rng.random(N) for i in range(12)}
    cols[ROW] = np.arange(N)
    table = pa.table(cols)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "snapshot.parquet"
        pq.write_table(table, path, compression="zstd", row_group_size=1_048_576)
        size = path.stat().st_size / 1e6
        print(f"file: {size:.0f} MB, {N:,} rows x {table.num_columns} columns, written in {ROW} order\n")

        con = xo.connect()
        t = con.read_parquet(str(path))

        print(f"1. default connection, ORDER BY {ROW} LIMIT 50 OFFSET k")
        for k in OFFSETS:
            report(f"offset {k:>9,}", t.order_by(ROW).limit(50, offset=k), k)
        print("   operators:", operators(con, t.order_by(ROW).limit(50, offset=1_000_000)))

        print("\n2. the same, with the file's order declared to the engine")
        ordered = xo.connect()
        ordered.raw_sql(
            f"CREATE EXTERNAL TABLE snapshot_ordered STORED AS PARQUET LOCATION '{path}' WITH ORDER ({ROW} ASC)"
        )
        o = ordered.table("snapshot_ordered")
        for k in OFFSETS:
            report(f"offset {k:>9,}", o.order_by(ROW).limit(50, offset=k), k)
        print("   operators:", operators(ordered, o.order_by(ROW).limit(50, offset=1_000_000)))

        print("\n3. a user sort on g (200 distinct values, so ties of about 15,000 rows), offset 100,000")
        for label, keys in (("ORDER BY g", ["g"]), (f"ORDER BY g, {ROW}", ["g", ROW])):
            n, _, ms = measure(t.order_by(keys).limit(50, offset=100_000))
            print(f"   {label:26s}: {n} distinct pages / {REQUESTS}, median {ms:.0f} ms")

        print(f"\n4. a range request, {ROW} >= k AND {ROW} < k + 50")
        indexed = Path(tmp) / "snapshot_page_index.parquet"
        pq.write_table(table, indexed, compression="zstd", row_group_size=1_048_576, write_page_index=True)
        for label, file in (("row-group statistics only", path), ("with a parquet page index", indexed)):
            r = xo.connect().read_parquet(str(file))
            print(f"   {label}:")
            for k in OFFSETS:
                report(f"  rows from {k:>9,}", r.filter((r[ROW] >= k) & (r[ROW] < k + 50)).order_by(ROW), k)

        print("\n5. a cheap recipe that does not name the column")
        recipe = t.filter(t.g < 100).select("g", "v0").mutate(z=t.v0 * 2)
        print(f"   as written:  columns {list(recipe.columns)}")
        carried = carry_row_order(recipe)
        print(f"   as altered:  columns {list(carried.columns)}")
        want = int(np.flatnonzero(cols["g"] < 100)[100_000])
        report("page at offset 100,000", carried.order_by(ROW).limit(50, offset=100_000), want)
        dropped = carry_row_order(t.drop(ROW, "v11"))
        last, kept = dropped.columns[-1], "v11" in dropped.columns
        print(f"   t.drop({ROW!r}, 'v11') as altered: last column {last!r}, v11 kept: {kept}")

        print("\nreference: first draft's approach, bare LIMIT/OFFSET on a single-partition connection")
        single = xo.connect()
        single.raw_sql("SET datafusion.execution.target_partitions = 1")
        s = single.read_parquet(str(path))
        for k in OFFSETS:
            report(f"offset {k:>9,}", s.limit(50, offset=k), k)


if __name__ == "__main__":
    main()
