# DataFusion scan ordering — findings and decision

- **Status:** Resolved (2026-06-25) — **stick with polars for the ingest reader.**
- **Origin:** Investigation behind `plans/adr-result-digest-canonical-ordering.md`,
  prompted by datafusion's nondeterministic parallel scan order producing false
  "drift" on the order-sensitive result digest.
- **Repro:** `scripts/bench_scan_order.py` (read + aggregate timings);
  `scripts/bench_result_digest.py` (the ADR's digest/order measurements).

## Decision

Use **polars** for the canonical ingest read. It is the only lever that delivers a
deterministic, file-order scan *without* giving up parallelism. Every datafusion
config knob that pins scan order does so by serializing the scan — for a single
input file, order and parallelism are the same mechanism (see Benchmarks). The
branch's canonical-sort-then-hash-the-bytes identity model is unchanged; this only
settles which reader produces the ordered ingest snapshot.

This resolves open question #4 in the ADR. The datafusion-config route was confirmed
reachable and content-hash-safe, but rejected on cost.

## The problem (recap)

datafusion executes an unordered file scan across multiple partitions/threads in
parallel, so it emits rows in a different physical order on every run. The row
*multiset* is identical run to run (a full-column `SUM` is stable); only the
sequence changes. An order-sensitive digest over an identical, deterministic recipe
therefore hashes differently each run, so `verify_result_faithful` reports false
drift and self-heal fires spurious warnings. Measured on `parking_2015` (3.2 GB CSV,
11.8M rows × 43 cols): physical rows 0–4 differ between runs; `SUM(summons_number)`
is stable at `76367348214693862`.

## Is the nondeterminism a datafusion bug?

No — it is expected behavior, confirmed from three independent angles:

- **Maintainer statement, issue [#10572](https://github.com/apache/datafusion/issues/10572)**
  (Andrew Lamb, PMC chair): *"DataFusion reads row groups in parallel, potentially
  out of order, with multiple threads as an optimization. To preserve the order of
  the data you can either set the configuration
  `datafusion.optimizer.repartition_file_scans` to `false` or else communicate the
  order of the data in the files using the `CREATE EXTERNAL TABLE .. WITH ORDER`
  clause and then explicitly ask for that order in your query."*
- **SELECT docs:** without `ORDER BY`, row order is unspecified and not guaranteed
  deterministic; tied `ORDER BY` keys are also unspecified.
- **The project's own test harness** requires `rowsort` to compare any query lacking
  an `ORDER BY` — direct evidence the engine does not promise a stable scan order.

Related: [#15833](https://github.com/apache/datafusion/issues/15833) (a round-robin
`RepartitionExec` scrambles already-sorted data after a window function — same
class), and [#13261](https://github.com/apache/datafusion/issues/13261) (open feature
request: there is **no built-in row-position / insertion-order column**).

## Config flags that affect output order

Source of truth: `datafusion/common/src/config.rs` and the
[Configuration Settings](https://datafusion.apache.org/user-guide/configs.html) page.

| Flag | Default | Effect on row order |
| --- | --- | --- |
| `datafusion.execution.target_partitions` | `0` → CPU cores | The master knob. `1` → single partition → no round-robin, no file split, no arbitrary coalesce → deterministic file-order output. Also single-threaded. |
| `datafusion.optimizer.repartition_file_scans` | `true` | When true (+ `target_partitions>1` + file ≥ min size), a single file is byte-range-split across partitions and merged → reorders. `false` keeps one file in one partition, read in order. (alamb's recommended flag.) |
| `datafusion.optimizer.enable_round_robin_repartition` | `true` | Inserts `RepartitionExec(RoundRobinBatch)` → "arbitrary interleaving (and thus unordered)." Not exposed as a xorq builder method, but reachable via the generic `.set(...)`. |
| `datafusion.optimizer.repartition_file_min_size` | `1048576` (1 MiB) | Threshold below which a file is never split. Small test files look "deterministic" only because they are under this. |
| `datafusion.optimizer.prefer_existing_sort` | `false` | Preserves only a *declared* ordering (`preserve_order=true` on `RepartitionExec` + `SortPreservingMergeExec`). No-op for an undeclared scan. |

Not relevant to a plain scan: `repartition_aggregations`, `repartition_windows`,
`repartition_sorts`, `enable_sort_pushdown` (only matter with aggregates / windows /
`ORDER BY`); `parquet.allow_single_file_parallelism` (write path only);
`coalesce_batches`, `batch_size`, `collect_statistics` (do not reorder).

### Declaring an order instead of serializing

`ListingOptions::with_file_sort_order(...)` / SQL `CREATE EXTERNAL TABLE ... WITH
ORDER (col ASC)` *asserts* the files are already sorted on a column. This populates
`FileScanConfig.output_ordering`, makes the scan advertise `output_ordering() =
Some(...)`, and merges partitions with `SortPreservingMergeExec` (order-preserving)
instead of `CoalescePartitionsExec` (arbitrary). It is a **correctness contract**:
datafusion trusts the assertion and does not verify or physically sort — a wrong
declaration silently yields wrong results, and it is only honored when files are
non-overlapping by min/max stats per group. Within a single partition, read order is
preserved deterministically; all the nondeterminism is multi-partition interleaving
plus byte-range splitting.

The maintainer-recommended pattern for stable insertion order is exactly what the
branch already does: materialize a monotonic 0…N column at write time and (optionally)
declare it — `original_row_order` is that column.

## Reachability from tallyman (xorq)

There is no upstream `datafusion-python` in the venv; xorq ships its own Rust binding
`xorq_datafusion` (0.2.7). Its `SessionConfig` exposes `with_target_partitions`,
`with_repartition_file_scans`, `with_repartition_file_min_size`,
`with_repartition_{aggregations,joins,sorts,windows}`, `with_batch_size`,
`with_parquet_pruning`, and a generic `set(key, value)` for any `datafusion.*` key
(e.g. `enable_round_robin_repartition`, which has no dedicated builder method).

- `xo.connect(session_config=cfg)` → `do_connect(config)` → `SessionContext(config=cfg)`.
  Verified end-to-end: after injecting the flags, `information_schema.df_settings`
  reports `target_partitions=1`, `repartition_file_scans=false`,
  `enable_round_robin_repartition=false`.
- **Content-hash-safe:** a non-default `SessionConfig` does *not* move the backend
  profile hash — the prefix is identical (`b18172e2…`) between default and configured
  backends; only the `_N` idx suffix differs, which tallyman already pins to `-1`
  (`config.py:269`). So configuring these flags would not perturb any cache key.
- Injection points if ever needed: `src/tallyman_xorq/backend.py:connect()` (currently
  `do_connect(None)`); for the ingest read specifically, the executing backend is the
  *default* backend (`xorq.config.default_backend()`), so it would be set via
  `xo.set_backend(xo.connect(session_config=cfg))` early at startup.

So the config route is genuinely available. It is rejected on cost, not reachability.

## Benchmarks

`parking_2015` CSV (3.2 GB, 11.8M × 43), all-string read, warm page cache, best of 2
runs, 14-core machine. "avg cores" = CPU-seconds / wall (the parallelism signal).

### Full read (force full decode of every cell)

| engine / config | wall | avg cores |
| --- | --- | --- |
| datafusion default (14 partitions) | 2.5s | 4.5 |
| datafusion `target_partitions=1` | 6.9s | 1.0 |
| datafusion `repartition_file_scans=false` | 7.0s | 1.0 |
| datafusion `tp=1` + `rfs=false` | 6.9s | 1.0 |
| polars `scan_csv.collect()` | 2.6–3.2s | 3–4 |
| polars `scan_csv.collect(streaming)` | 2.8–3.7s | 4 |

Either datafusion order-pinning flag serializes the read: ~2.7× slower (2.5s → 6.9s),
1.0 core. polars ties datafusion's *parallel* default while preserving file order.

### Read → group_by → aggregate

| config | `registration_state` (69 groups) | `plate_id` (3.08M groups) |
| --- | --- | --- |
| datafusion default | 0.9–2.1s / 3–8 cores | 0.54s / 11.0 cores |
| datafusion `target_partitions=1` | 3.4–3.7s / 1.0 core | 3.91s / 1.0 core |
| datafusion `repartition_file_scans=false` | 3.3–3.8s / 1.0 core | 3.43s / 1.2 cores |
| polars `group_by.agg.collect()` | 0.8–1.5s / 2–3 cores | 1.09s / 3.6 cores |

The decisive finding: `repartition_file_scans=false` **collapses to ~1 core even with
a downstream aggregate**, in both low- and high-cardinality cases. datafusion does not
fan the single scan partition back out for the aggregate, so the whole pipeline runs
serially. (`rfs=false` keeps parallelism only with *multiple* input files, where each
file stays its own partition — not a single-CSV ingest.)

### Why no config gives both order and parallelism (single file)

For a single input file, the only source of scan parallelism is splitting that file
into byte ranges across partitions — and that same split is what reorders the rows.
Pinning order and keeping parallelism are therefore mutually exclusive *through
config*: `target_partitions=1` and `repartition_file_scans=false` both serialize,
because they both remove the one parallelism axis. The total CPU work is actually
*lower* serialized (~6.9 CPU-s vs ~11 for the parallel read — parallelism spends ~60%
extra CPU on repartition/coalesce overhead to buy the wall-clock speedup), but the
wall time triples. polars sidesteps the dichotomy entirely: it preserves file order
*and* uses 3–4 cores, because its multi-threaded CSV reader is order-preserving by
construction rather than order-scrambling.

## What this means for the ADR

- **Identity model is unchanged.** Result identity stays the row multiset via
  canonical-order snapshot bytes (Option A): inject `original_row_order` at ingest,
  sort by it before baking, pin write settings, `result_digest = sha256(snapshot
  bytes)`. Order is paid for at the materialization pass that already happens.
- **Ingest reader = polars.** It produces the ordered ingest snapshot cheaply and
  deterministically. The datafusion read stays available for derived entries that run
  through the expression layer; only the ingest root swaps.
- **datafusion config knobs: documented and shelved.** Reachable and hash-safe, but
  the slowest path (~2.7× read tax, serializes everything downstream). Recorded so the
  option is not re-investigated.

## Follow-ups (not blocking)

- Wire the polars ingest into `tallyman_read_csv` / the ingest root and confirm the
  baked snapshot is byte-identical across repeated builds (the ADR measured 5/5
  byte-identical for `scan_csv → with_row_index → sink_parquet`).
- Pin polars write settings (codec, compression level, `row_group_size`, statistics)
  alongside the order, so the snapshot hash is reproducible — same requirement as the
  datafusion bake path.
- Derived results (aggregate/join output) have no source order: sort-by-all-columns
  for Option A, or fall back to the order-insensitive commutative digest (Option B).
  Still open (ADR open question #1).

## Sources

- datafusion issues: [#10572](https://github.com/apache/datafusion/issues/10572),
  [#15833](https://github.com/apache/datafusion/issues/15833),
  [#13261](https://github.com/apache/datafusion/issues/13261)
- [Configuration Settings](https://datafusion.apache.org/user-guide/configs.html),
  [DDL `WITH ORDER`](https://datafusion.apache.org/user-guide/sql/ddl.html),
  [SELECT semantics](https://datafusion.apache.org/user-guide/sql/select.html),
  [Ordering analysis blog](https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/)
- Source: `common/src/config.rs`, `datasource/src/file_scan_config/mod.rs`,
  `datasource/src/file_groups.rs`
- xorq binding: `xorq_datafusion` 0.2.7 (`SessionConfig`, `SessionContext`),
  `xorq/backends/xorq_datafusion/__init__.py`, `xorq/config.py`
