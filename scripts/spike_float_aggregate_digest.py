"""ADR-009 evidence: a float aggregate is not bit-reproducible under DataFusion's parallel partial aggregation.

Runs the same canonically ordered group-by six times per configuration and hashes the result's Arrow buffers.
A float ``SUM``/``AVG`` comes back with different low bits from run to run under the default partition count, and
with identical bits under ``target_partitions = 1``. An integer-only aggregate is the control: it is identical in
both configurations. Median wall time is printed so the cost of single-partition execution is visible.

    uv run python scripts/spike_float_aggregate_digest.py
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
RUNS = 6


def value_digest(table: pa.Table) -> str:
    h = hashlib.sha256()
    for col in table.columns:
        for chunk in col.chunks:
            for buf in chunk.buffers():
                if buf is not None:
                    h.update(buf)
    return h.hexdigest()[:12]


def run(path: Path, partitions: int | None, kind: str) -> tuple[str, float, pa.Table]:
    con = xo.connect()
    if partitions is not None:
        con.raw_sql(f"SET datafusion.execution.target_partitions = {partitions}")
    t = con.read_parquet(str(path))
    if kind == "float":
        expr = t.group_by("g").agg(n=t.count(), s=t.v.sum(), m=t.v.mean()).order_by("g")
    else:
        expr = t.group_by("g").agg(n=t.count(), s=t.k.sum(), hi=t.k.max()).order_by("g")
    t0 = time.perf_counter()
    table = expr.to_pyarrow()
    return value_digest(table), time.perf_counter() - t0, table


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "t.parquet"
        rng = np.random.default_rng(5)
        source = pa.table(
            {"g": rng.integers(0, 1_000, N), "v": rng.random(N) * 1e6, "k": rng.integers(0, 1_000_000, N)}
        )
        pq.write_table(source, path, row_group_size=100_000)
        groups = pq.ParquetFile(path).metadata.num_row_groups
        print(f"source: {N:,} rows, {groups} row groups, {path.stat().st_size / 1e6:.1f} MB\n")
        for kind in ("float", "int"):
            for label, partitions in (("default partitions", None), ("target_partitions=1", 1)):
                results = [run(path, partitions, kind) for _ in range(RUNS)]
                digests = [d for d, _, _ in results]
                median_ms = statistics.median(s for _, s, _ in results) * 1000
                distinct = len(set(digests))
                line = f"{kind:5s} {label:20s} distinct digests over {RUNS} runs: {distinct}, median {median_ms:.0f} ms"
                if kind == "float" and len(set(digests)) > 1:
                    a, b = results[0][2].column("s").to_numpy(), results[1][2].column("s").to_numpy()
                    rel = np.max(np.abs(a - b) / np.abs(a))
                    line += f", max relative difference between two runs {rel:.1e}"
                print(line)


if __name__ == "__main__":
    main()
