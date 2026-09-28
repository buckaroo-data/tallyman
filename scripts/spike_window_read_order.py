"""ADR-008 evidence: which DataFusion settings make unsorted LIMIT/OFFSET windows repeatable and in file order?

Writes parquet files above ``datafusion.optimizer.repartition_file_min_size`` whose rows are physically sorted by
``id``, then issues the same window request several times per configuration, for two plan shapes:

* ``bare``   — ``read_parquet -> limit/offset`` (a worthy entry's snapshot read)
* ``shaped`` — ``read_parquet -> filter -> mutate -> limit/offset`` (a cheap entry's plan over a snapshot)

For each it reports how many distinct pages came back, the first ids seen against the file-order answer, the
exchange operators in the physical plan (the governing variable: a window is in file order exactly when no
exchange operator sits between the scan and the limit), and the median latency. Two row-group sizes are used
because ``repartition_file_scans = false`` alone returns the file-order page for one and not for the other.

It then times a full-scan aggregate under each configuration, which is the cost of applying an order-pinning
setting to a connection that also runs aggregates.

A last section checks two ops that ``classify_build`` treats as cheap and that have more than a scan under them:
``union`` (``UnionExec`` emits one partition per input, so an exchange operator survives ``target_partitions = 1``)
and ``distinct`` (a hash aggregate).

    uv run python scripts/spike_window_read_order.py
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

N = 3_000_000
REQUESTS = 8
EXCHANGES = ("RepartitionExec", "CoalescePartitionsExec", "SortPreservingMergeExec")
CONFIGS = {
    "default": [],
    "repartition_file_scans=false": ["SET datafusion.optimizer.repartition_file_scans = false"],
    "rfs=false + round_robin=false": [
        "SET datafusion.optimizer.repartition_file_scans = false",
        "SET datafusion.optimizer.enable_round_robin_repartition = false",
    ],
    "target_partitions=1": ["SET datafusion.execution.target_partitions = 1"],
}


def connect(stmts: list[str]):
    con = xo.connect()
    for stmt in stmts:
        con.raw_sql(stmt)
    return con


def shapes(t):
    return {"bare": t, "shaped": t.filter(t.id % 3 != 0).mutate(z=t.id * 2)}


def file_order_first_id(shape: str, offset: int) -> int:
    if shape == "bare":
        return offset
    # kept ids are 1, 2, 4, 5, 7, 8, ...: the k-th (0-based) is 3 * (k // 2) + 1 + (k % 2)
    return 3 * (offset // 2) + 1 + (offset % 2)


def plan_summary(con, expr) -> str:
    plan = con.raw_sql("EXPLAIN " + xo.to_sql(expr)).to_pandas()
    physical = plan[plan.iloc[:, 0] == "physical_plan"].iloc[0, 1]
    found = [name for name in EXCHANGES if name in physical]
    groups = physical.split("file_groups={")[1].split(" group")[0]
    return f"{groups} file group(s), exchanges: {', '.join(found) or 'none'}"


def cheap_ops_beyond_a_scan(tmp: Path) -> None:
    rng = np.random.default_rng(1)
    half = N // 2
    paths = []
    for i in range(2):
        path = tmp / f"part{i}.parquet"
        part = pa.table({"id": np.arange(half) + i * half, "g": rng.integers(0, 5_000, half), "v": rng.random(half)})
        pq.write_table(part, path, row_group_size=100_000)
        paths.append(path)
    print("\n=== cheap ops with more than a scan under them (two files, ids 0..N/2-1 and N/2..N-1)")
    for label in ("default", "target_partitions=1"):
        con = connect(CONFIGS[label])
        a, b = (con.read_parquet(str(path)) for path in paths)
        for name, expr, offset in (
            ("union", a.union(b), 1_400_000),
            ("distinct", a.select("g").distinct(), 2_000),
        ):
            window = expr.limit(50, offset=offset)
            pages = {hashlib.md5(window.execute().to_csv(index=False).encode()).hexdigest() for _ in range(REQUESTS)}
            physical = con.raw_sql("EXPLAIN " + xo.to_sql(window)).to_pandas()
            physical = physical[physical.iloc[:, 0] == "physical_plan"].iloc[0, 1]
            found = [op for op in (*EXCHANGES, "UnionExec") if op in physical]
            print(
                f"{label:30s} {name:8s} offset={offset:>9,}: {len(pages)} distinct pages / {REQUESTS} "
                f"| operators: {', '.join(found) or 'none'}"
            )


def main() -> None:
    rng = np.random.default_rng(3)
    table = pa.table({"id": np.arange(N), "g": rng.integers(0, 50_000, N), "v": rng.random(N), "w": rng.random(N)})
    probe = xo.connect()
    threshold = probe.raw_sql("SHOW datafusion.optimizer.repartition_file_min_size").to_pandas().iloc[0, 1]
    partitions = probe.raw_sql("SHOW datafusion.execution.target_partitions").to_pandas().iloc[0, 1]
    print(f"engine: repartition_file_min_size={threshold} target_partitions={partitions}")

    with tempfile.TemporaryDirectory() as tmp:
        for row_group_size in (8_192, 100_000):
            path = Path(tmp) / f"sorted_by_id_rg{row_group_size}.parquet"
            pq.write_table(table, path, row_group_size=row_group_size)
            print(f"\n=== {path.stat().st_size / 1e6:.1f} MB file, rows sorted by id, row groups of {row_group_size:,}")
            for label, stmts in CONFIGS.items():
                con = connect(stmts)
                t = con.read_parquet(str(path))
                for shape, expr in shapes(t).items():
                    for offset in (0, 1_000_000):
                        window = expr.limit(50, offset=offset)
                        pages, firsts, secs = set(), set(), []
                        for _ in range(REQUESTS):
                            t0 = time.perf_counter()
                            df = window.execute()
                            secs.append(time.perf_counter() - t0)
                            pages.add(hashlib.md5(df.to_csv(index=False).encode()).hexdigest())
                            firsts.add(int(df["id"].iloc[0]))
                        want = file_order_first_id(shape, offset)
                        print(
                            f"{label:30s} {shape:6s} offset={offset:>9,}: {len(pages)} distinct pages / {REQUESTS}, "
                            f"first ids {sorted(firsts)[:3]} (file order: {want}), "
                            f"median {statistics.median(secs) * 1000:.0f} ms | {plan_summary(con, window)}"
                        )
                agg = t.group_by("g").agg(n=t.count(), s=t.v.sum())
                secs = []
                for _ in range(3):
                    t0 = time.perf_counter()
                    agg.execute()
                    secs.append(time.perf_counter() - t0)
                print(f"{label:30s} full-scan aggregate (50k groups): median {statistics.median(secs) * 1000:.0f} ms")
        cheap_ops_beyond_a_scan(Path(tmp))


if __name__ == "__main__":
    main()
