"""Measure the cost of forcing deterministic datafusion scan order via session config.

Backs the "Make datafusion's scan deterministic via session config" alternative in
plans/adr-result-digest-canonical-ordering.md. Quantifies, on any wide CSV:

  1. a full read (force full decode of every cell) under datafusion config variants
     vs polars, and
  2. a read -> group_by -> aggregate under the same variants,

reporting wall time and CPU-seconds/wall (≈ average cores used — the parallelism
signal). The point: for a *single* input file, the only source of scan parallelism
is splitting that file into byte ranges across partitions, and that same split is
what reorders rows. So every datafusion knob that pins order (target_partitions=1,
repartition_file_scans=false) also serializes the scan — they are the same
mechanism. polars preserves file order while staying multi-threaded, so it is the
only lever that gives deterministic order without the serialization tax.

    uv run python scripts/bench_scan_order.py [CSV_PATH] \
        [--group-cols registration_state,plate_id] [--runs N]

Default CSV is the parking_2015 file the ADR was measured on. All columns are read
as strings, so the read work is pure CSV field-splitting (dataset-agnostic);
--group-cols must name real columns for the aggregate section.
"""

from __future__ import annotations

import argparse
import os
import time

DEFAULT_CSV = "/Users/paddy/code/parking_speeding_data_raw/parking_violations_2015_complete.csv"

VARIANTS = [
    ("datafusion default", lambda c: c),
    ("datafusion target_partitions=1", lambda c: c.with_target_partitions(1)),
    ("datafusion repartition_file_scans=false", lambda c: c.with_repartition_file_scans(False)),
]


def _string_schema(csv_path: str):
    import xorq.vendor.ibis as ibis  # noqa: PLC0415

    with open(csv_path) as f:
        header = f.readline().rstrip("\n")
    cols = [c.strip().strip('"') for c in header.split(",")]
    return ibis.schema({c: "string" for c in cols})


def _timed(fn):
    """(wall_s, cpu_s, result). cpu_s = user+sys for this process (includes engine threads)."""
    t0 = time.perf_counter()
    c0 = sum(os.times()[:2])
    res = fn()
    cpu = sum(os.times()[:2]) - c0
    return time.perf_counter() - t0, cpu, res


def _make_backend(configure):
    import xorq.api as xo  # noqa: PLC0415
    from xorq_datafusion import SessionConfig  # noqa: PLC0415

    return xo.connect(session_config=configure(SessionConfig()))


def _best(runs, fn):
    best = None
    for _ in range(runs):
        wall, cpu, res = _timed(fn)
        if best is None or wall < best[0]:
            best = (wall, cpu, res)
    return best


def section_read(csv, schema, runs):
    print("\n== full read (force full decode, all columns) ==", flush=True)
    out = {}
    for label, configure in VARIANTS:
        con = _make_backend(configure)
        wall, cpu, n = _best(runs, lambda: sum(b.num_rows for b in con.read_csv(csv, schema=schema).to_pyarrow_batches()))
        out[label] = (wall, cpu / wall)
        print(f"  {label:<44} {wall:6.2f}s  {cpu / wall:5.1f} cores  rows={n:,}", flush=True)

    import polars as pl  # noqa: PLC0415

    for label, collect in [
        ("polars scan_csv.collect()", lambda lf: lf.collect()),
        ("polars scan_csv.collect(streaming)", lambda lf: lf.collect(engine="streaming")),
    ]:
        wall, cpu, n = _best(runs, lambda: collect(pl.scan_csv(csv, infer_schema=False)).height)
        out[label] = (wall, cpu / wall)
        print(f"  {label:<44} {wall:6.2f}s  {cpu / wall:5.1f} cores  rows={n:,}", flush=True)
    return out


def section_agg(csv, schema, group_cols, runs):
    import polars as pl  # noqa: PLC0415

    out = {}
    for gcol in group_cols:
        print(f"\n== read -> group_by({gcol}) -> count ==", flush=True)
        for label, configure in VARIANTS:
            con = _make_backend(configure)

            def run():
                t = con.read_csv(csv, schema=schema)
                agg = t.group_by(gcol).aggregate(n=t[t.columns[0]].count())
                return sum(b.num_rows for b in agg.to_pyarrow_batches())

            wall, cpu, g = _best(runs, run)
            out[(gcol, label)] = (wall, cpu / wall)
            print(f"  {label:<44} {wall:6.2f}s  {cpu / wall:5.1f} cores  groups={g:,}", flush=True)

        wall, cpu, g = _best(runs, lambda: pl.scan_csv(csv, infer_schema=False).group_by(gcol).agg(pl.len()).collect().height)
        out[(gcol, "polars group_by.agg.collect()")] = (wall, cpu / wall)
        print(f"  {'polars group_by.agg.collect()':<44} {wall:6.2f}s  {cpu / wall:5.1f} cores  groups={g:,}", flush=True)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv", nargs="?", default=DEFAULT_CSV)
    ap.add_argument("--group-cols", default="registration_state,plate_id")
    ap.add_argument("--runs", type=int, default=2)
    args = ap.parse_args()

    schema = _string_schema(args.csv)
    print(f"cpus={os.cpu_count()}  csv={args.csv}\nwarming page cache ...", flush=True)
    _make_backend(lambda c: c).read_csv(args.csv, schema=schema).count().execute()

    section_read(args.csv, schema, args.runs)
    section_agg(args.csv, schema, [c.strip() for c in args.group_cols.split(",") if c.strip()], args.runs)


if __name__ == "__main__":
    main()
