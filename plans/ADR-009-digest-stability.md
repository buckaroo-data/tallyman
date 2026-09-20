# ADR: Digest stability (a heal is flagged only when the result changed)

- **Status:** Proposed (2026-09-18, revised 2026-09-20: D3 gains two format
  requirements from `plans/ADR-008-row-order-of-reads.md`). Amends
  `plans/ADR-004-result-digest-canonical-ordering.md` (Option A's "hash the
  snapshot bytes") and decision D5 of
  `plans/ADR-006-read-path-loads-builds.md` (the canonical sort), which said
  "`result_digest` keeps its file-hash definition". The canonical sort itself
  is unchanged and is still required.
- **Reading decision labels:** a bare label such as "D2" in this document
  always means this ADR's own decision. Another ADR's decision is always
  written with its ADR number and a few words saying what it decides.
- **Context:** the 2026-09-18 cache audit (tallyman @ `a748ea6`, xorq 0.3.26,
  xorq-datafusion 0.2.7, pyarrow 21.0.0). No ticket filed yet.
- **Affected code:** `src/tallyman_xorq/result_cache.py`
  (`snapshot_file_digest`, `verify_result_faithful`, `_verify_self_heal`),
  `src/tallyman_xorq/build.py` (the execute-once step),
  `src/tallyman_xorq/backend.py` (a single-partition connection),
  `src/tallyman_core/manifest.py` (`result_digest`), and the `materialize`
  writer that decision D4 of `plans/ADR-007-tallyman-owned-materialization.md`
  introduces (one writer for snapshots, used by the build and by every heal).
  D2 and D3 assume that writer. If ADR-007 were rejected, D1 would stand as
  written and D2 would need restating against xorq's writer.
- **Related ADRs:** `plans/ADR-008-row-order-of-reads.md` (uses the same
  single-partition setting for a different job, and has an open question this
  digest would answer).
- **Evidence:** `scripts/spike_float_aggregate_digest.py`,
  `scripts/spike_logical_digest.py`.

## Terms

- **Materialize:** run an entry's computation once and write the result to a
  parquet file. That file is the entry's **snapshot**.
- **Worthy entry:** an entry tallyman materializes. A cheap entry is one it
  does not, and it records no digest.
- **`result_digest`:** the value recorded in a worthy entry's manifest when its
  snapshot is first written, against which every later rewrite is checked.
- **Heal:** re-create a snapshot that is missing from disk by re-running the
  entry's build.
- **Unfaithful heal:** a heal whose digest does not match the recorded one.
- **Canonical sort:** the fixed total order in which a snapshot's rows are
  written (the author's `order_by` keys, then an inherited `__row_order`, then
  the remaining columns), from ADR-006 decision D5.
- **Partition:** one of the parallel streams DataFusion splits a query into.
  `target_partitions = 1` runs a query as a single stream.

## Problem

`result_digest` is the SHA-256 of the snapshot file's bytes. The contract gives
it one job: witnessing that a later rematerialization reproduced the original.
When a heal's digest does not match, tallyman writes a durable
`unfaithful_heal` record, pushes an SSE event, wipes the entry's Buckaroo stat
cache, evicts its session, and pins and badges the entry (ADR-006 decisions D7,
loud verification; D10, the Buckaroo-state wipe; and D12, the pin and badge).
That response is right for a recipe that calls `sample()`. It is
expensive, sticky and misleading when nothing about the result changed, and
today two things trigger it without a change in the result.

**1. A float aggregate is not bit-reproducible under parallel execution.**
DataFusion sums each partition separately and merges the partial sums in
arrival order. Float addition is not associative, so the low bits move from
run to run. A group-by over 3,000,000 rows with a float `SUM` and `AVG`,
canonically ordered, six runs per configuration:

| Aggregate | Partitions | Distinct digests in 6 runs | Median |
| --- | --- | --- | --- |
| float | default (14) | 6 | 6 ms |
| float | `target_partitions = 1` | 1 | 20 ms |
| integer | default (14) | 1 | 7 ms |
| integer | `target_partitions = 1` | 1 | 31 ms |

The largest relative difference between two float runs was 6.4e-16. At the
tallyman level the audit healed a float-aggregate entry six times: six
different files, six `unfaithful_heal` records, six stat-cache wipes. An
integer-only aggregate healed byte-identical six times. The canonical sort
cannot help, since the rows are in the same order and it is the values that
differ. Aggregate is the usual reason an entry is worthy, so most expensive
entries with a float measure are flagged after any eviction. The message
blames "execution (#83), a fixed graph that runs differently each execute",
which sends the author to a recipe that is deterministic. The default partition
count is also the machine's core count, so the same build merges differently
on a different machine.

**2. File bytes depend on things that are not the result.**

- How the stream was batched. The same rows handed to a parquet writer as
  100,000-row batches and as 8,192-row batches gave different files unless each
  row group was first combined into contiguous arrays.
- The writer's version. The footer carries
  `created_by = 'parquet-cpp-arrow version 21.0.0'`, so after a pyarrow upgrade
  every heal would mismatch.
- Under xorq's writer today, whether the cache node was the root of the
  executed expression (1,234 bytes against 858 for the same rows).

ADR-004 accepted "a library upgrade changes the hash; rebuild is fine". That
was written before a mismatch became loud. With those three ADR-006 decisions
in place, an
upgrade would pin and badge every entry that gets evicted until the corpus is
rebuilt.

## Decisions

### D1. Materialization runs single-partition

`materialize` (ADR-007 decision D4, the one writer of snapshots) executes on a
connection configured with
`SET datafusion.execution.target_partitions = 1`. The build and every heal use
it, so both run one plan with one merge order, on any machine. It is a
different connection object from ADR-008's window connection, with the same
setting, because a long materialization must not share a context with page
reads.

Cost: about 3x on the spike's aggregate. ADR-004 measured a 3.1M-group
aggregate at 0.5 s parallel against 3.4 to 3.9 s single-partition, and a full
43-column read at 2.5 s against 6.9 s, on the 11.8M-row parking file. A
materialization runs once per entry and once per heal, never on a read.

ADR-004 rejected pinning scan order through session config as the *ingest*
lever, because it serializes everything downstream. This decision uses the same
setting for a different job and accepts that cost knowingly: reproducible
arithmetic is the point, and no setting gives both.

**Gate.** Rebuild the parking corpus under both settings before committing. If
the single-partition rebuild is unacceptable, fall back to **D1b**: only plans
with a floating-point reduction or window run single-partition. That choice is
made once, at build, from the expression, and recorded in the manifest, so a
heal never re-derives it.

*Rejected:* round floats before hashing. A value next to a rounding boundary
flips under one unit of noise in the last place, and among millions of values
some are.
*Rejected:* compare with a tolerance. When a heal runs the original values are
gone and only the digest is left.
*Rejected:* treat a mismatch as advisory when the plan has a float aggregate.
That removes verification from the entries most likely to be expensive.
*Rejected:* drop the canonical sort, since single-partition output is ordered
anyway. The sort is what puts the author's keys first in the order the rows
are numbered (`__row_order`, ADR-008 decision D2), and it keeps the result independent of the hash
table's emission order, which an engine upgrade can change.

### D2. `result_digest` is a digest of the snapshot's content, computed from the file as read back

The digest is a SHA-256 over the snapshot's ordered Arrow data:

- one hash stream per column for validity (one byte per row), one for lengths
  (variable-width types) and one for values, with null slots zeroed or emptied,
  since a null slot may hold anything;
- each stream seeded with the column name and its logical type, where `string`,
  `large_string` and `string_view` are one type;
- the streams combined in schema order together with the row count;
- stored with an algorithm prefix (`arrow-sha256:`) so a future definition can
  never be compared against this one by accident.

Separate streams matter. The first version of the spike fed validity and
values into one hasher per column, and the digest then depended on where batch
boundaries fell.

One function computes it, from the written file read back, for both the build
and verify. This is the "one derivation route" rule behind ADR-006 decision D8
(the snapshot-key tripwire: the build and the read share one derivation). The
spike shows why it cannot be computed from the stream handed to the writer: the
writer coerced a `timestamp[s]` column to `timestamp[ms]`, and the two digests
differed.

Measured on 3,000,000 rows with nulls, NaNs, strings, booleans and timestamps:

| Check | Result |
| --- | --- |
| Same rows as 8,192-row, 100,000-row and single batches | 1 digest |
| Same file read back in 1,000-row batches | same |
| Same rows written Snappy with 8,192-row groups instead of zstd with 1,048,576 | same |
| Same file read through DataFusion with `order_by(id)` | same |
| One value changed | differs |
| One null replaced by `0.0` | differs |
| First two rows swapped | differs |
| Read back and hash a 57 MB file (127 MB of Arrow data) | 0.12 s, against 0.02 s for a file-bytes hash |

The digest stays order-sensitive, so the canonical sort is still what makes it
reproducible. `__row_order` is a column of the file like any other, so the
digest covers it.

This reverses part of the reasoning in ADR-006 decision D5 (the canonical
sort). That decision rejected the multiset digest
partly because "verify must read every row and the digest stops being a hash
of the artifact". Both are true of this digest as well. They are accepted here
because verify runs only on a heal and in the sweep, at roughly 1 GB/s of Arrow
data, and because being a hash of the artifact is exactly what turns a writer
upgrade into a false alarm.

*Rejected:* keep file bytes and pin the writer's settings. It is reproducible
today (the spike gets one digest across batch sizes once each row group is
combined) and six times cheaper to verify. It also freezes the codec and
row-group size for the life of the corpus, and it fails on the first pyarrow
upgrade.
*Rejected:* polars `hash_rows`. Its documentation does not guarantee stable
results across polars versions.

### D3. The snapshot's format

Tallyman's writer (ADR-007 decision D4) regroups the record-batch stream into
row groups of 1,048,576 rows, combines each row group into contiguous arrays,
and writes zstd level 3, parquet format 2.6, statistics on. For the spike's rows that is
57.1 MB, 3 row groups and a 2.7 KB footer, against 100.3 MB, 367 row groups
and a 228.5 KB footer in the shape xorq writes (one Snappy row group per
8,192-row batch). Memory is bounded by one row group.

ADR-008 adds two requirements. The writer numbers the rows in a last column
named `__row_order` (ADR-008 decision D2). And it writes a parquet page index,
which is what lets a page be fetched as a range of `__row_order` values without
decoding a whole row group: 19 to 24 ms at any depth with the index against 79
to 90 ms without it, on a 287 MB file (`scripts/spike_row_order_paging.py` on
the ADR-008 branch).

Combining each row group keeps the file bytes reproducible as well. Nothing
depends on that after D2, and it means two writes of the same entry can still
be compared with `cmp` when debugging. Because of D2 these settings can change later without
touching any digest.

### D4. A mismatch record names its likely cause

With D1 and D2 in place the causes left are the recipe's own nondeterminism
(`sample()`, `now()`, an impure UDF), source drift under `off` identity mode,
and an engine upgrade that changed results. The manifest records the xorq,
xorq-datafusion and pyarrow versions at build, and the `unfaithful_heal` record
carries them alongside the versions at heal. When they differ, the message says
so instead of blaming the recipe. The contract's attribution table gains that
row.

### D5. It lands with the rebuild

Every worthy entry's digest is recomputed by the corpus rebuild of ADR-007
decision D9 (one change, one rebuild).
The manifest field keeps its name. Tests that cannot fail first ride with the
fix; the float-aggregate heal test and a batch-boundary digest test fail on
`main` and belong in the failing-tests commit.

## Consequences

- A float-aggregate entry heals to the same digest, and a pyarrow upgrade no
  longer flags anything. The loud responses of ADR-006 decisions D10 and D12
  are left for the causes they were designed for.
- Materializations and heals are slower, by about 3x on aggregation at spike scale and up
  to 7x in ADR-004's parking measurement. Reads are unaffected.
- Verify decodes the file instead of hashing its bytes. It runs on a heal and
  in `catalog_scan_staleness(verify_results=True)`, never on a read.
- Snapshots are smaller, and their footers were 85 times smaller in the spike,
  which matters to every page request that opens one.
- `docs/system-contract.md` changes in "Result digest" and in the manifest
  table ("SHA-256 of the baked result snapshot"), and its verification table
  gains the engine-change row.
- The same function can digest the CSV intermediate (ADR-008, open
  question 1).

## Open questions

1. **Rebuild time under single-partition materialization.** Decides D1 against
   D1b.
2. **Nested types.** The spike covers fixed-width, boolean, string and binary
   columns. Lists, structs and maps need a recursive definition.
3. **Types parquet cannot store as given.** A `timestamp[s]` column comes back
   as `timestamp[ms]`, so the snapshot's schema differs from the entry's
   recorded schema. Either the writer refuses such a column, or the entry's
   schema is recorded from the snapshot. `__row_order` pushes toward the second
   answer, since the writer adds a column the entry's graph does not have.
4. **Engine upgrades.** Single-partition execution fixes the merge order within
   one engine version. Nothing guarantees float results across versions. D4
   attributes that case and the remedy stays a rebuild.
