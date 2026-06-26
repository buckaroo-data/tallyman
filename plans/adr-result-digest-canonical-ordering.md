# ADR: Result digest as a row-multiset, and canonical snapshot ordering

- **Status:** Proposed (2026-06-25)
- **Affected code:** `src/tallyman_xorq/result_cache.py` (`_digest_update`,
  `count_and_result_digest`, `result_digest`, `verify_result_faithful`),
  `src/tallyman_xorq/build.py` (`build_and_persist` execute path,
  `result_cache.py:486-501`), `src/tallyman_xorq/io.py` (`read_project_file` —
  the ingest root)
- **Related:** `plans/89-determinism-prereqs-execution.md` (#83 result-digest,
  #88 determinism lint, #89 epic), `plans/adr-result-cache-cost-rubric.md` (#30
  cheap/worthy classification), `plans/adr-source-identity-content-hash.md`
  (`content_hash` as cache key),
  `plans/datafusion-scan-order-findings.md` (ingest-reader decision: polars; the
  datafusion scan-order config research and benchmarks behind open question #4)

## Problem

Adding `parking_2015` (NYC parking violations 2015: 3.2 GB CSV, 11,809,217 rows ×
43 columns) takes **112.5s**; the derived `issue_date`-cast entry
(`6a414b2c0b64`) takes **106.9s**. Reading the CSV is not the cost. datafusion
reads the file and forces full row evaluation in **3.5s** (multi-threaded, ~565%
CPU). The remaining ~109s is the build-time result digest (#83).

The digest, `_digest_update` at `result_cache.py:303`, pulls every batch into
Python objects and hashes one row at a time:

```python
def _digest_update(h, batch) -> int:
    cols = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
    for row in zip(*cols):
        h.update(repr(row).encode())
    return batch.num_rows
```

Over 11.8M rows × 43 columns this `repr()`-per-row loop is single-threaded
Python. Isolated on the materialised result it takes **159.6s** versus **1.0s**
to stream the same Arrow batches and count them — a ~155× overhead. It runs on
every build: the cheap path uses it as its force-evaluation pass
(`build.py:497`), the worthy path runs it on top of baking the snapshot
(`build.py:493`).

There is a second, deeper problem. The digest is **order-sensitive** by design
(`result_cache.py:286-290`: "an unordered query that reshuffles is, correctly,
drift"). But datafusion executes an unordered scan in parallel and emits rows in
a **different order on every run**. Two runs of an order-sensitive digest over
the identical recipe produce different hashes; two runs of an order-insensitive
digest over the same recipe agree. The row *multiset* is identical run to run
(the full-column `SUM` is stable); only the physical sequence changes.

Consequence: for any result large enough to parallelize, the recorded
`result_digest` will not match a recompute, so `verify_result_faithful`
(`result_cache.py:335`) returns **False — false drift — for a fully
deterministic recipe**, and self-heal fires spurious `tallyman.perf` warnings.
The #83 tests pass only because their `orders_parquet` fixture is small enough to
stay single-batch / single-thread, where order is incidentally stable. The
order-sensitivity assumption breaks the moment a read parallelizes. datafusion
reshuffles unordered reads as a matter of course, so order-sensitivity does not
*detect* nondeterminism here; it *manufactures* it.

## Evidence

All figures measured on the `parking_2015` CSV (3.2 GB, 11.8M × 43) this session.
Reproduce with `scripts/bench_result_digest.py <csv>`.

### Cost of the current digest

| Step | Time |
| --- | --- |
| xorq read CSV + force eval via streaming count (no digest) | 3.65s |
| xorq read + cast + force eval (no digest) | 3.50s |
| stream Arrow batches + count, on materialised result | 1.0s |
| **current per-row `repr()`+sha256 digest, same result** | **159.6s** |
| build with digest, recorded in manifest (`parking_2015`) | 112.5s |
| build with digest, recorded in manifest (cast child) | 106.9s |

### Row order is nondeterministic; the multiset is not

| Check (same expression, two+ runs) | Result |
| --- | --- |
| Physical rows 0–4 (`summons_number`) | **differ** run to run |
| `SUM(summons_number)` over all rows | **identical** (76367348214693862) |
| Order-*sensitive* digest (hash row tuples in stream order) | **differ** |
| Order-*insensitive* digest (commutative fold of row hashes) | **identical**, 4.5s |
| `xo.to_parquet` of the same expr, two writes | **differ** (not byte-stable) |

### Hashing the snapshot bytes: cheap, but only sound under canonical order

| Check | Result |
| --- | --- |
| Hash a 406 MB parquet file | 0.28s |
| Two identical writes (same data, same settings) | **same** bytes |
| Same rows, `row_group_size` 200k vs 5M | **differ** (framing-dependent) |
| Same rows, zstd vs snappy | **differ** (codec-dependent) |
| `polars hash_rows` order-sensitive digest | 1.2s, framing-independent |

### Canonical order makes the snapshot byte-deterministic

| Pipeline | Result |
| --- | --- |
| `scan_csv → sink_parquet` (no sort), repeated | byte-identical, ~5s (polars preserves file order) |
| `scan_csv → with_row_index → sink_parquet`, **5 runs** | **all 5 byte-identical**, ~5s |
| `with_row_index` column (`original_row_order`) | 0…N-1, unique, monotonic — a true total order |
| `order_by(summons_number) → to_parquet`, two writes | same bytes, but `summons_number` is **not** unique |
| Distinct `summons_number` / distinct full rows | 10,951,240 / 10,951,241 — exactly **one** key maps to two different rows |

`summons_number` is not a safe canonical key: it has 857,977 collisions, of which
857,976 are exact full-row duplicates (interchangeable) and exactly one is a
genuine same-key/different-row pair whose tie order is unpinned. A synthetic
source-order index has no ties.

### Which canonical order — clustering needs knowledge we do not have

Same writer (polars, zstd, `row_group_size`=200k):

| Physical order | Size | Row groups read for a selective filter |
| --- | --- | --- |
| file order (`original_row_order`) | 418 MB | summons range: 60/60 · issue_dt month: 60/60 (**no skipping**) |
| shuffled | 452 MB | 60/60 |
| sorted by `summons_number` | 243 MB | summons range: **1/60** |
| sorted by `issue_dt` | 379 MB | issue_dt month: **6/60** |

Sorting by a data column wins ~42% compression and 10–30× row-group skipping —
but **only for queries whose predicate matches the sort column**. `sort_summons`
does nothing for an `issue_dt` query. tallyman ingests unknown datasets with
unknown access patterns, so there is no dominant column to cluster on; choosing
one is an unplaceable bet. File order carries mild incidental clustering (418 MB
vs 452 MB shuffled) but no skipping.

## Decision

### 1. Stop hashing rows in pure Python

The per-row `repr()`+sha256 loop is removed regardless of which identity model we
adopt below. Forcing row-level evaluation and counting (the cheap path's real
obligation — catch a failing cast, get the row count) is the 1.0s streaming pass;
it does not require a Python hash of every row.

### 2. A result's identity is its row *multiset*, not its row *sequence*

datafusion's execution model makes physical row order a nondeterministic-but-
benign artifact of parallel scheduling. The digest must be invariant to it.
Order-sensitivity does not buy drift detection here; it produces false positives
on every parallel read. The genuine nondeterminism sources the digest exists to
catch (`sample()`, `now()`, unordered `limit`, impure UDF) all change the
multiset or the values, so a multiset-level identity still catches them, and the
#88 op lint already flags these recipes structurally at build time.

### 3. Compute the multiset digest one of two ways (decision point for review)

**Option A — canonical order, then hash the snapshot bytes (recommended).**
Impose a deterministic total order on the materialised snapshot and set
`result_digest = sha256(snapshot bytes)`. The worthy path already writes the
snapshot, so the digest costs ~0.3s with no extra pass. Requirements:

- A synthetic `original_row_order` column added at the ingest root
  (`read_project_file` / `deferred_read_csv` wrapper) as the universal total
  order. It needs no knowledge of the data, doubles as the sensible default
  display order ("as the data arrived"), and is tie-free.
- The snapshot bake sorts by the canonical key (today it bakes in datafusion's
  nondeterministic order) and pins write settings (codec, compression level,
  `row_group_size`, writer version). Build and verify share one code path, so
  their bytes match. A library upgrade changes the hash; rebuild-is-fine here per
  the project's single-user rule.

**Option B — order-insensitive commutative digest.** Vectorized per-row hash
(`hash_rows`) folded with a commutative, **additive** combiner (not XOR — XOR
cancels even-count duplicate rows; addition does not), carried in one streaming
pass at ~4.5s, constant memory. Needs no ordering machinery and works on any
result including derived ones.

Recommendation: **A** for ingest roots and any entry that bakes a snapshot, since
it is near-free and also delivers a content-addressable, display-stable snapshot.
Fall back to **B** where no canonical order exists (see open questions).

### 4. `original_row_order` is the canonical key, not a data column

Data-column clustering (the 243 MB / 30× skipping win) requires knowing the
access pattern, which this system does not. Clustering by a chosen column becomes
an **optional, per-entry opt-in** a user can declare later, not a default. The
default canonical order is source order via `original_row_order`.

## Alternatives considered

- **Keep the order-sensitive digest, add an explicit `ORDER BY` before it.** A
  full sort of 11.8M rows (~22s via xorq) to make the *digest* stable is paying a
  sort for an identity check that does not need an order at all. Rejected for the
  digest; an explicit sort is still the mechanism behind Option A's snapshot
  ordering, where it also buys display determinism.
- **Hash the parquet stream without canonical order.** Cheap (0.3s) but
  framing-, codec-, and order-dependent (table above), so it reports drift on
  recipes that did not drift. Only sound once order and write settings are
  pinned, which is Option A.
- **Switch the ingest reader to polars.** `scan_csv → sink_parquet` is ~5s,
  order-preserving, and byte-deterministic across 5 runs, versus
  `xo.to_parquet` at ~16s and not byte-stable. A force-full-decode read is
  ~2.6–3.7s at 3–4 cores — polars ties datafusion's *parallel* default while
  preserving file order, so unlike the datafusion config knobs (next bullet) it
  gives deterministic ingest order without serializing. Out of scope for this
  ADR (identity model), but noted as a strong candidate for the ingest root and
  recorded here so it is not re-discovered.
- **Pin datafusion's scan order via session config instead of a canonical sort.**
  xorq exposes the underlying datafusion knobs through
  `xo.connect(session_config=...)` — `SessionConfig().with_target_partitions(1)`,
  `.with_repartition_file_scans(False)`, plus a generic `.set(key, value)` for any
  `datafusion.*` option (e.g. `enable_round_robin_repartition`) — and setting them
  does not move the backend profile hash, so the route is reachable and
  content-hash-safe. It is, however, the *slowest* path to a deterministic ingest.
  For a single input file the only source of scan parallelism is splitting that file
  into byte ranges across partitions, and that same split is what reorders the rows,
  so every knob that pins order also serializes the scan. Measured on the
  parking_2015 CSV (warm cache, 14-core box, best of 2; reproduce with
  `scripts/bench_scan_order.py`):

  | full read (force full decode, 43 cols) | wall | avg cores |
  | --- | --- | --- |
  | datafusion default | 2.5s | 4.5 |
  | `target_partitions=1` | 6.9s | 1.0 |
  | `repartition_file_scans=false` | 7.0s | 1.0 |
  | polars `scan_csv.collect()` | 2.6–3.2s | 3–4 |

  The serialization is not confined to the scan. A downstream `group_by(plate_id)`
  aggregate (3.1M groups) runs at ~11 cores / 0.5s under the default but collapses to
  ~1 core / 3.4–3.9s under either flag: datafusion does not fan the single scan
  partition back out for the aggregate, so the whole pipeline runs serially.
  (`repartition_file_scans=false` would keep parallelism only with *multiple* input
  files, where each file stays its own partition — not tallyman's single-CSV ingest.)
  There is thus no datafusion config that buys deterministic order *and* parallelism
  for a single-file source; the two are the same mechanism. Rejected as the ingest
  lever — it pays a ~2.7× read tax (and serializes everything downstream) for an order
  the canonical sort already delivers at the bake, where a materialization pass runs
  anyway.

## Open questions (for review)

1. **Derived results have no source order.** `original_row_order` exists at
   ingest. The output of an aggregate/join has no natural order to preserve. For
   Option A there, sort by all output columns (still byte-deterministic, since
   duplicate rows are interchangeable) or fall back to Option B. Which?
2. **One model or two?** Option A everywhere (sort-by-all for derived) keeps a
   single "hash the bytes" story; A-for-snapshots / B-for-the-rest avoids sorting
   wide derived results. Preference?
3. **Cheap entries that bake nothing.** Skip the digest entirely when the #88
   lint proves the recipe deterministic, or always compute Option B? Skipping is
   cheaper; always-on is uniform.
4. **Ingest reader — RESOLVED (2026-06-25): polars.** Of the three measured options —
   (a) keep datafusion's parallel read and impose order with
   `.order_by(original_row_order)` on the bake; (b) swap the ingest reader to polars
   (order-preserving *and* multi-threaded, ~2.6–3.7s); (c) pin datafusion's scan order
   via session config (reachable + hash-safe, but ~2.7× slower because pinning order
   *is* serializing the single-file scan) — we take **(b)**. It is the only lever that
   gives a deterministic, file-order read without surrendering parallelism. Full
   write-up, benchmarks, and the datafusion config research in
   `plans/datafusion-scan-order-findings.md`.

## Testing

The current #83 tests (`test_result_cache.py:622-679`) assert stability and
drift detection on a single-batch fixture; they do not pin the properties this
change turns on. Add, and watch the contract tests pass on the new
implementation while the perf test flips:

- **Run-to-run stability on a *parallel* read** — a fixture large enough to span
  multiple batches/threads; the digest must be identical across runs. This fails
  against today's order-sensitive digest (the bug above) and passes after.
- **Order-independence** — digesting the same multiset in two different row
  orders yields the same value (Option B), or the canonical sort produces
  identical bytes (Option A).
- **Duplicate sensitivity** — a result differing only by one duplicated row must
  change the digest (guards the XOR-cancellation trap; forces additive fold).
- **Genuine drift still caught** — a `now()` / `sample()` recipe digests
  differently across runs (multiset changed).
- **Perf bound** — digest of a ~1M-row frame completes well under a threshold;
  fails at 160s today, passes vectorized.

## Consequences

- Build time for large reads drops from ~110s to a few seconds; the digest stops
  being the dominant per-build cost.
- `verify_result_faithful` and self-heal stop emitting false drift on parallel
  reads, which unblocks the #89 reactive-staleness signal from acting on a
  polluted input.
- Under Option A, snapshots become byte-reproducible and display in a stable,
  meaningful default order; the snapshot file hash doubles as the identity.
- The digest is no longer sensitive to row order. A recipe that *should* preserve
  a specific order (a genuine top-level `ORDER BY`) is still handled — the sort
  makes the order deterministic and Option A hashes it in that order — but
  reordering of an unordered result is correctly treated as the same result.
