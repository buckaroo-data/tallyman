"""ADR-009 evidence (D1): making a LOADED BUILD run single-partition.

``scripts/spike_float_aggregate_digest.py`` runs SQL on a connection it made and configured itself. ``materialize``
(ADR-007 D4) executes a build that ``load_expr`` loaded, and ``load_expr`` makes its own backend objects.

Questions, in the order printed. A float SUM and AVG group-by over 3,000,000 rows, distinct value digests in 5 runs:

1. The loaded build, executed as loaded.
2. The same, while a single-partition connection exists on the side. Which backend does the build run on?
3. The loaded build rebound onto the single-partition connection with ``replace_sources``.
4. ``SET`` applied to the backends the load made, with no rebinding.
5. Does either change the process default backend, which serves page reads?

    uv run python scripts/spike_single_partition_loaded_build.py
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_single_partition_"))
os.environ["XORQ_CACHE_DIR"] = str(HOME / "_global_xorq")  # must be set before xorq is imported

import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import xorq.api as xo  # noqa: E402
from xorq.common.utils.graph_utils import find_all_sources, replace_sources  # noqa: E402
from xorq.config import default_backend  # noqa: E402
from xorq.ibis_yaml.compiler import build_expr, load_expr  # noqa: E402

N = 3_000_000
RUNS = 5
SINGLE = "SET datafusion.execution.target_partitions = 1"


def digest(expr) -> str:
    table = expr.to_pyarrow()
    h = hashlib.sha256()
    for column in ("s", "m"):
        h.update(np.asarray(table[column].to_numpy()).tobytes())
    return h.hexdigest()[:10]


def partitions(con) -> str:
    return str(con.raw_sql("SHOW datafusion.execution.target_partitions").to_pandas().iloc[0, 1])


def report(label: str, make) -> None:
    seen = {digest(make()) for _ in range(RUNS)}
    print(f"   {label}: {len(seen)} distinct digest(s) in {RUNS} runs")


def main() -> None:
    rng = np.random.default_rng(3)
    source = HOME / "source.parquet"
    table = pa.table({"g": rng.integers(0, 500, size=N), "v": rng.normal(scale=1e6, size=N)})
    pq.write_table(table, source, row_group_size=100_000)
    t = xo.deferred_read_parquet(str(source))
    build_dir = Path(
        build_expr(t.group_by("g").agg(s=t.v.sum(), m=t.v.mean()).order_by("g"), builds_dir=HOME / "builds")
    )

    print("1. the loaded build, executed as loaded")
    report("as loaded", lambda: load_expr(build_dir))

    print("2. a single-partition connection on the side; the build is not rebound")
    side = xo.connect()
    side.raw_sql(SINGLE)
    backends = find_all_sources(load_expr(build_dir))
    print(f"   side connection reports {partitions(side)}; the build's {len(backends)} backend(s) report", end=" ")
    print(
        f"{[partitions(b) for b in backends]}, and none is the side connection: {all(b is not side for b in backends)}"
    )
    report("not rebound", lambda: load_expr(build_dir))

    def rebound():
        loaded = load_expr(build_dir)
        return replace_sources({id(b): side for b in find_all_sources(loaded)}, loaded)

    def set_on_loaded():
        loaded = load_expr(build_dir)
        for b in find_all_sources(loaded):
            b.raw_sql(SINGLE)
        return loaded

    print("3. rebound onto the single-partition connection")
    report("replace_sources", rebound)
    print("4. SET on the backends the load made")
    report("SET on each", set_on_loaded)
    print(f"5. the process default backend reports {partitions(default_backend())}")


if __name__ == "__main__":
    main()
