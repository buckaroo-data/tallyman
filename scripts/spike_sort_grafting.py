"""ADR-008 evidence (D10, D11, and the uniqueness note under D5): where a unique sort has to be imposed.

Paddy's rule: when a supplied sort is not deterministic, the natural order is imposed into each ``order_by``. Today
tallyman extends an author's sort only when it is the TOP node of the expression (``source_cache._canonical_sorted``).
``original_row_order`` and ``id`` stand in for ``__row_order``, which is not implemented yet.

Questions, in the order printed:

1. An ``order_by`` followed by another step: in what order is the snapshot written?
2. A top-3 entry: is it written in rank order?
3. A sort that feeds a ``limit``, with ties at the cut: which ROWS come back, run to run?
4. A window function: are its VALUES the same run to run, with no order, a tied order, and a unique order?
5. Can parquet statistics tell a unique column from one with duplicates?

    uv run python scripts/spike_sort_grafting.py
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_sort_grafting_"))
os.environ["XORQ_CACHE_DIR"] = str(HOME / "_global_xorq")  # must be set before xorq is imported

import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import xorq.api as xo  # noqa: E402
import xorq.vendor.ibis.expr.operations as ops  # noqa: E402

from tallyman_xorq.source_cache import _canonical_sorted, _is_worthy_expr  # noqa: E402

N = 3_000_000
RUNS = 5


def connection(partitions: int | None):
    con = xo.connect()
    if partitions is not None:
        con.raw_sql(f"SET datafusion.execution.target_partitions = {partitions}")
    return con


def keys_of(expr) -> list[str]:
    node = expr.op()
    if not isinstance(node, ops.Sort):
        return []
    return [k.expr.name + ("" if k.ascending else " desc") for k in node.keys]


def nonfinal_sorts() -> None:
    small = HOME / "small.parquet"
    rows = {"name": list("abcdef"), "amount": [40, 10, 60, 20, 50, 30], "original_row_order": list(range(6))}
    pq.write_table(pa.table(rows), small)
    t = xo.deferred_read_parquet(str(small))
    by_amount = t.order_by(t.amount.desc())
    shapes = {
        "order_by last": by_amount,
        "order_by, then mutate": by_amount.mutate(double=t.amount * 2),
        "order_by, then select": by_amount.select("name", "amount", "original_row_order"),
        "order_by, then filter": by_amount.filter(t.amount > 15),
    }
    print("1. parent rows are in the order 40, 10, 60, 20, 50, 30; the author asks for amount descending")
    for label, expr in shapes.items():
        written = _canonical_sorted(expr)
        print(f"   {label:24s} worthy={_is_worthy_expr(expr)!s:5s} sort keys={keys_of(written)}")
        print(f"   {'':24s} written as {written.execute()['amount'].tolist()}")
    top3 = _canonical_sorted(by_amount.limit(3)).execute()["amount"].tolist()
    print(f"2. top 3 by amount is written as {top3}; the author asked for [60, 50, 40]")


def big_file() -> Path:
    rng = np.random.default_rng(5)
    path = HOME / "big.parquet"
    table = pa.table({"id": np.arange(N), "g": rng.integers(0, 200, size=N), "v": rng.integers(0, 1000, size=N)})
    pq.write_table(table, path, row_group_size=100_000)
    return path


def sort_feeds_limit(path: Path) -> None:
    def row_set(partitions, keys) -> str:
        t = xo.deferred_read_parquet(str(path), con=connection(partitions))
        ids = np.sort(t.order_by(keys).limit(1000).to_pyarrow()["id"].to_numpy())
        return hashlib.md5(ids.tobytes()).hexdigest()[:10]

    print(f"3. order_by(g).limit(1000) over {N:,} rows; about 15,000 rows tie on the smallest g")
    unique_answer = row_set(1, ["g", "id"])
    for label, partitions, keys in (
        ("default connection, key g", None, ["g"]),
        ("single-partition, key g", 1, ["g"]),
        ("default connection, key (g, id)", None, ["g", "id"]),
    ):
        seen = [row_set(partitions, keys) for _ in range(RUNS)]
        same = all(s == unique_answer for s in seen)
        print(f"   {label:34s} {len(set(seen))} distinct row set(s) in {RUNS} runs; equals the (g, id) answer: {same}")


def window_values(path: Path) -> None:
    def digest(partitions, shape) -> str:
        t = xo.deferred_read_parquet(str(path), con=connection(partitions))
        total = {
            "no order": lambda: t.v.cumsum(),
            "order_by g (tied)": lambda: t.v.cumsum(order_by=t.g),
            "order_by (g, id)": lambda: t.v.cumsum(order_by=[t.g, t.id]),
        }[shape]()
        out = t.mutate(c=total).order_by("id").select("c").to_pyarrow()  # final order fixed, so only values can differ
        return hashlib.md5(out["c"].to_numpy().tobytes()).hexdigest()[:10]

    print("4. cumsum() as a computed column; distinct sets of VALUES")
    for shape in ("no order", "order_by g (tied)", "order_by (g, id)"):
        for label, partitions in (("default connection", None), ("single-partition", 1)):
            seen = {digest(partitions, shape) for _ in range(RUNS)}
            print(f"   {shape:20s} {label:20s} {len(seen)} in {RUNS} runs")


def statistics() -> None:
    path = HOME / "stats.parquet"
    pq.write_table(pa.table({"unique": [0, 1, 2, 3, 4], "duplicated": [0, 0, 2, 3, 4]}), path, write_statistics=True)
    group = pq.ParquetFile(path).metadata.row_group(0)
    print("5. parquet statistics of a unique column and of one holding 0 twice")
    for i in range(2):
        s = group.column(i).statistics
        name = group.column(i).path_in_schema
        print(f"   {name:11s} min={s.min} max={s.max} nulls={s.null_count} has_distinct_count={s.has_distinct_count}")


def main() -> None:
    nonfinal_sorts()
    path = big_file()
    sort_feeds_limit(path)
    window_values(path)
    statistics()


if __name__ == "__main__":
    main()
