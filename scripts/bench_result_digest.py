"""Reproduce the measurements behind plans/adr-result-digest-canonical-ordering.md.

Quantifies, on any wide CSV:
  1. how much of a build is the per-row result digest vs the actual read,
  2. that datafusion's parallel scan order is nondeterministic but its multiset
     is not (order-sensitive digest drifts; commutative digest is stable),
  3. that a canonical order (synthetic source-order index) makes the parquet
     byte-deterministic across runs,
  4. the compression / row-group-skipping trade-off of sorting by a data column.

Schema-agnostic: columns are read as strings, so it runs on any CSV.

    uv run python scripts/bench_result_digest.py [CSV_PATH] [--runs N] [--skip-slow]

Default CSV is the parking_2015 file the ADR was measured on.
"""

from __future__ import annotations

import hashlib
import os
import sys
import time

DEFAULT_CSV = "/Users/paddy/code/parking_speeding_data_raw/parking_violations_2015_complete.csv"


def _sha_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _mb(path: str) -> float:
    return os.path.getsize(path) / 1e6


def _string_schema(csv_path: str):
    """All-string ibis schema from the CSV header, so this is dataset-agnostic."""
    import xorq.vendor.ibis as ibis  # noqa: PLC0415

    with open(csv_path) as f:
        header = f.readline().rstrip("\n")
    cols = [c.strip().strip('"') for c in header.split(",")]
    return ibis.schema({c: "string" for c in cols})


def section_digest_cost(csv_path: str, out_dir: str, skip_slow: bool) -> None:
    import xorq.api as xo  # noqa: PLC0415

    print("\n== 1. read vs digest cost ==")
    schema = _string_schema(csv_path)
    expr = xo.deferred_read_csv(csv_path, schema=schema)

    t = time.perf_counter()
    n = sum(b.num_rows for b in expr.to_pyarrow_batches())
    print(f"  xorq read CSV + stream-count (no digest): {time.perf_counter() - t:6.2f}s  rows={n:,}")

    if skip_slow:
        print("  per-row repr+sha256 digest:               skipped (--skip-slow)")
        return

    def per_row_digest(e) -> tuple[int, str]:
        # the current src/tallyman_xorq/result_cache.py:303 _digest_update loop
        h = hashlib.sha256()
        rows = 0
        for b in e.to_pyarrow_batches():
            cols = [b.column(i).to_pylist() for i in range(b.num_columns)]
            for row in zip(*cols):
                h.update(repr(row).encode())
            rows += b.num_rows
        return rows, h.hexdigest()

    t = time.perf_counter()
    rows, _ = per_row_digest(expr)
    print(f"  per-row repr+sha256 digest (current impl): {time.perf_counter() - t:6.2f}s  rows={rows:,}")


def section_order_determinism(csv_path: str) -> None:
    import polars as pl  # noqa: PLC0415
    import xorq.api as xo  # noqa: PLC0415

    print("\n== 2. order nondeterministic, multiset deterministic ==")
    expr = xo.deferred_read_csv(csv_path, schema=_string_schema(csv_path))

    def order_sensitive(e) -> str:
        h = hashlib.sha256()
        for b in e.to_pyarrow_batches():
            h.update(pl.from_arrow(b).hash_rows().to_numpy().tobytes())
        return h.hexdigest()[:16]

    def commutative(e) -> int:
        # additive fold mod 2**64: sum of all per-row hashes, independent of
        # batch framing and row order (addition commutes). Python-int accumulate
        # + mask keeps it exact and warning-free.
        mask = (1 << 64) - 1
        acc = 0
        for b in e.to_pyarrow_batches():
            acc = (acc + int(pl.from_arrow(b).hash_rows().to_numpy().sum())) & mask
        return acc

    os_hashes = {order_sensitive(expr) for _ in range(2)}
    comm = {commutative(expr) for _ in range(2)}
    print(f"  order-sensitive digest across 2 runs:  {'STABLE' if len(os_hashes) == 1 else 'DRIFTS'}  {os_hashes}")
    print(f"  commutative (additive) digest 2 runs:  {'STABLE' if len(comm) == 1 else 'DRIFTS'}  {comm}")


def section_canonical_parquet(csv_path: str, out_dir: str, runs: int) -> None:
    import polars as pl  # noqa: PLC0415

    print(f"\n== 3. canonical order -> deterministic parquet ({runs} runs) ==")
    out = os.path.join(out_dir, "canon.parquet")
    hashes = []
    for i in range(runs):
        t = time.perf_counter()
        (
            pl.scan_csv(csv_path, infer_schema_length=0)
            .with_row_index("original_row_order")
            .sink_parquet(out, compression="zstd")
        )
        hashes.append(_sha_file(out))
        print(f"  run {i + 1}: {time.perf_counter() - t:5.1f}s  {_mb(out):.0f}MB  {hashes[-1][:16]}")
    print(f"  all {runs} byte-identical: {len(set(hashes)) == 1}")


def section_compression_skipping(csv_path: str, out_dir: str) -> None:
    import polars as pl  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    print("\n== 4. compression + row-group skipping (sort by data column) ==")
    rg = 200_000
    df = pl.read_csv(csv_path, infer_schema=False, ignore_errors=True)
    sort_col = df.columns[0]  # first column stands in for "the queried column"

    variants = {
        "file_order": df,
        "shuffled": df.sample(fraction=1.0, shuffle=True, seed=42),
        f"sort_{sort_col}": df.sort(sort_col),
    }
    paths = {}
    for name, d in variants.items():
        p = os.path.join(out_dir, f"cmp_{name}.parquet")
        d.write_parquet(p, compression="zstd", row_group_size=rg)
        paths[name] = p
        print(f"  {name:18s} {_mb(p):6.0f} MB")

    # selective predicate on the first column; count row groups whose stats overlap
    vals = df[sort_col].drop_nulls()
    lo = vals.sort()[int(len(vals) * 0.5)]
    hi = vals.sort()[min(len(vals) - 1, int(len(vals) * 0.503))]

    def overlap(path: str) -> tuple[int, int]:
        pf = pq.ParquetFile(path)
        ci = pf.schema_arrow.names.index(sort_col)
        md = pf.metadata
        touched = 0
        for g in range(md.num_row_groups):
            st = md.row_group(g).column(ci).statistics
            if st is None or st.min is None or not (st.max < lo or st.min > hi):
                touched += 1
        return touched, md.num_row_groups

    print(f"  predicate {sort_col} in [{lo}, {hi}]:")
    for name in ("file_order", f"sort_{sort_col}"):
        t, ntot = overlap(paths[name])
        print(f"    {name:18s} row-groups read {t}/{ntot} ({100 * t / ntot:.0f}%)")


def main() -> None:
    argv = sys.argv[1:]
    runs = 5
    if "--runs" in argv:
        i = argv.index("--runs")
        runs = int(argv[i + 1])
        del argv[i : i + 2]
    skip_slow = "--skip-slow" in argv
    positionals = [a for a in argv if not a.startswith("--")]
    csv_path = positionals[0] if positionals else DEFAULT_CSV
    out_dir = os.environ.get("TMPDIR", "/tmp").rstrip("/")

    if not os.path.isfile(csv_path):
        sys.exit(f"CSV not found: {csv_path}")
    print(f"CSV: {csv_path}  ({_mb(csv_path) / 1000:.1f} GB)")

    section_digest_cost(csv_path, out_dir, skip_slow)
    section_order_determinism(csv_path)
    section_canonical_parquet(csv_path, out_dir, runs)
    section_compression_skipping(csv_path, out_dir)


if __name__ == "__main__":
    main()
