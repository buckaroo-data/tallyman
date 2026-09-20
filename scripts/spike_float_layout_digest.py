"""ADR-009 evidence (D1, D3) and #187: what single-partition execution leaves undetermined in a float aggregate.

``target_partitions = 1`` removes the run-to-run drift of a float aggregate. This script asks whether the result is
then a function of the rows alone. It writes the same rows, in the same order, as four parquet files that differ only
in row-group size, and bit-compares several query shapes across them on a single-partition connection:

* ``A``  an ungrouped ``SUM`` / ``AVG``;
* ``B``  the same over a subquery sorted by row position, to test whether ordering the addends helps;
* ``C``  ``GROUP BY`` a literal key;
* ``D``  ``GROUP BY`` a single-valued key the optimizer cannot fold (``ro - ro``);
* ``E``  the window form ``SUM(v) OVER ()``, which is what an ibis percent-of-total compiles to.

Every shape is stable run to run on one file. Shape A, and B with it, differs between files. The physical plan
printed for B shows why sorting changes nothing: DataFusion removes the sort, since ``SUM`` needs no ordered input.
The variable is association (where the running total is cut into sub-sums), not the order of the addends.

    uv run python scripts/spike_float_layout_digest.py
"""

from __future__ import annotations

import struct
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import xorq.api as xo

N = 3_000_000
ROW_GROUP_SIZES = (1_048_576, 777_777, 100_000, 8_192)
SHAPES = {
    "A ungrouped": "SELECT SUM(v) s, AVG(v) m FROM {t}",
    "B sorted first": "SELECT SUM(v) s, AVG(v) m FROM (SELECT * FROM {t} ORDER BY ro)",
    "C group by literal": "SELECT SUM(v) s, AVG(v) m FROM (SELECT v, 1 AS k FROM {t}) GROUP BY k",
    "D group by ro - ro": "SELECT SUM(v) s, AVG(v) m FROM (SELECT v, ro - ro AS k FROM {t}) GROUP BY k",
    "E window SUM() OVER ()": "SELECT MIN(tot) s, MAX(tot) m FROM (SELECT SUM(v) OVER () AS tot FROM {t})",
}


def bits(value) -> str:
    return struct.pack("<d", float(value)).hex()


def operators(con, sql: str) -> str:
    plan = con.raw_sql("EXPLAIN " + sql).to_pandas()
    physical = plan[plan.iloc[:, 0] == "physical_plan"].iloc[0, 1]
    return " <- ".join(line.strip().split(":")[0] for line in physical.splitlines() if line.strip())


def main() -> None:
    rng = np.random.default_rng(5)
    table = pa.table({"ro": np.arange(N), "v": rng.random(N) * 1e6})
    results: dict[str, dict[int, tuple[str, str]]] = {shape: {} for shape in SHAPES}
    plans: dict[str, str] = {}
    with tempfile.TemporaryDirectory() as tmp:
        for size in ROW_GROUP_SIZES:
            path = Path(tmp) / f"rows_rg{size}.parquet"
            pq.write_table(table, path, row_group_size=size, compression="zstd")
            con = xo.connect()
            con.raw_sql("SET datafusion.execution.target_partitions = 1")
            name = f"t_{size}"
            con.read_parquet(str(path), table_name=name)
            for shape, sql in SHAPES.items():
                runs = set()
                for _ in range(2):
                    row = con.raw_sql(sql.format(t=name)).to_pandas().iloc[0]
                    runs.add((bits(row.s), bits(row.m)))
                assert len(runs) == 1, f"{shape} is not stable run to run on row groups of {size:,}"
                results[shape][size] = runs.pop()
                plans.setdefault(shape, operators(con, sql.format(t=name)))

    print(f"{N:,} rows, the same order in every file; target_partitions = 1; row-group sizes {ROW_GROUP_SIZES}\n")
    for shape, by_size in results.items():
        distinct = len(set(by_size.values()))
        low_bytes = [by_size[size][0][:4] for size in ROW_GROUP_SIZES]
        print(f"{shape:24s} distinct results across the four files: {distinct}   SUM, lowest two bytes: {low_bytes}")
        print(f"{'':24s} plan: {plans[shape]}")


if __name__ == "__main__":
    main()
