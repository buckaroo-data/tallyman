# ADR: Digest stability (a heal is flagged only when the result changed)

- **Status:** Accepted (2026-09-22), implemented. Where the text below says a
  decision is "not yet confirmed" or "Proposed", it was implemented as written;
  the differences between the text and the code are under "Implementation
  notes". Open defect touching it: #208 (an unfaithful heal reaches only the
  healed entry).
- **Amends:** `plans/ADR-004-result-digest-canonical-ordering.md` (Option A's
  "hash the snapshot bytes") and decision D5 of
  `plans/ADR-006-read-path-loads-builds.md` (the canonical sort), which said
  "`result_digest` keeps its file-hash definition". The canonical sort itself is
  unchanged and is still required.
- **Reading decision labels:** a bare label such as "D2" in this document
  always means this ADR's own decision. Another ADR's decision is always
  written with its ADR number and a few words saying what it decides.
- **Context:** the 2026-09-18 cache audit (tallyman @ `a748ea6`, xorq 0.3.26,
  xorq-datafusion 0.2.7, pyarrow 21.0.0).
- **Tickets:** #187 (a float total depends on the parent file's layout; D1 and
  D3), #185 (non-pure recipes, which D6 only partly covers).
- **Affected code:** `src/tallyman_xorq/result_cache.py`
  (`snapshot_file_digest`, `verify_result_faithful`, `_verify_self_heal`),
  `src/tallyman_xorq/build.py` (the execute-once step),
  `src/tallyman_xorq/backend.py` (a single-partition connection),
  `src/tallyman_core/manifest.py` (`result_digest`), and the `materialize`
  writer that decision D4 of `plans/ADR-007-tallyman-owned-materialization.md`
  introduces (one writer for snapshots, used by the build and by every heal).
  D2 and D3 assume that writer. If ADR-007 were rejected, D1 would stand as
  written and D2 would need restating against xorq's writer.
- **Related ADRs:** `plans/ADR-008-row-order-of-reads.md` (its first draft used
  the same single-partition setting for page reads, and it now rejects that;
  its open question 5, a digest for an ordered copy of a source, is one this
  digest would answer).
- **Evidence:** `scripts/spike_float_aggregate_digest.py`,
  `scripts/spike_logical_digest.py`, and
  `scripts/spike_float_layout_digest.py` (D1 and D3: what the layout of the
  parent file does to a float total), and two from the second review:
  `scripts/spike_single_partition_loaded_build.py` (D1: a loaded build has to
  be rebound) and `scripts/spike_ordered_copy_layout.py` (D3: the layout
  polars writes).

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
`SET datafusion.execution.target_partitions = 1` and an explicit
`datafusion.execution.batch_size`. The build and every heal use it, so both run
one plan with one merge order, on any machine, given input files with the same
layout (see "What this does not fix" below). It is a separate connection from
the default backend that serves page reads, because a long materialization
must not share a context with them.

Making that connection is not enough. `materialize` executes a build that
`load_expr` loaded, and `load_expr` makes its own backend objects, so a loaded
build ignores a single-partition connection it was never bound to
(`scripts/spike_single_partition_loaded_build.py`, a float `SUM` and `AVG`
group-by over 3,000,000 rows):

| How the loaded build is executed | Distinct digests in 5 runs |
| --- | --- |
| as loaded | 5 |
| as loaded, while a single-partition connection exists on the side | 5 (the build's own backend reports 14 partitions) |
| rebound onto that connection with `replace_sources` | 1 |
| `SET` applied to each backend the load made | 1 |

Either of the last two works, and neither changes the process default
backend, which still reports 14. `materialize` rebinds. That is what
`_rebind_to_default_backend` (ADR-006 decision D3, rebind composition onto the
default backend) already does, aimed at the materialization connection.

The same connection is why a bare `limit`, or a window function with no order,
is repeatable at materialization: on it a row-preserving plan streams its rows
in the parent file's order (`scripts/spike_stream_order.py`). That is the
engine's behaviour and not the query's meaning, which is what decision D10 of
`plans/ADR-008-row-order-of-reads.md` (the natural order is imposed on every
`order_by`) is for.

Cost: about 3x on the spike's aggregate. ADR-004 measured a 3.1M-group
aggregate at 0.5 s parallel against 3.4 to 3.9 s single-partition, and a full
43-column read at 2.5 s against 6.9 s, on the 11.8M-row parking file. A
materialization runs once per entry and once per heal, never on a read.

ADR-004 rejected pinning scan order through session config as the *ingest*
lever, because it serializes everything downstream. This decision uses the same
setting for a different job and accepts that cost knowingly: reproducible
arithmetic is the point, and no setting gives both.

Paddy's call in the grilling session (2026-09-20): "I want a cohesive system
that works reliably, then we can worry about speed problems as they come up."
So every materialization runs single-partition, and this decision carries no
speed gate. If the cost becomes a problem, the known variant is to run
single-partition only for a plan with a floating-point reduction or window,
decided once at build from the expression and recorded in the manifest so that
a heal never re-derives it.

**What this does not fix.** A single stream fixes the order in which partial
results are merged. It does not make a float total a function of the rows
alone. With one partition and the same rows in the same order, an ungrouped
float `SUM` or `AVG` took three different bit patterns across four copies of
one file that differed only in row-group size (1,048,576, 777,777, 100,000 and
8,192 rows), each stable run to run (`scripts/spike_float_layout_digest.py`).
The variable is association, meaning where the running total is cut into
sub-sums. The likely mechanism, not confirmed in DataFusion's source, is that
the ungrouped accumulator sums each record batch as a block while batch
boundaries follow row-group boundaries. Sorting the input first changes
nothing, because DataFusion removes the sort: the plan is
`AggregateExec <- DataSourceExec` with or without it. A grouped aggregate,
`GROUP BY` a constant key, and the window form `SUM(v) OVER ()` were all
independent of the layout, so an ibis percent-of-total is not affected. Only a
true ungrouped reduction is exposed.

So the layout of every file an entry reads is part of what makes its digest
reproducible. Tallyman writes all of those files (snapshots through D3, and
sources through the ordered copies of ADR-008 decision D2), which is why
pinning the layout is enough. D3 pins it. #187 tracks the rest: confirming the
mechanism, other reductions, and results across machines.

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
to 90 ms without it, on a 287 MB file (`scripts/spike_row_order_paging.py`).

The entry's recorded schema (`schema.json`) is read from the written file and
not from the expression. The file is what every consumer reads, the writer
adds a column the graph does not have (`__row_order`), and parquet changes some
types on the way in: a `timestamp[s]` column comes back as `timestamp[ms]`.

Combining each row group keeps the file bytes reproducible as well. Nothing
depends on that after D2, and it means two writes of the same entry can still
be compared with `cmp` when debugging.

Because of D2 the codec, the compression level and the statistics can change
later without touching this entry's digest. The row-group size cannot. It
decides the batch boundaries that an entry built on this file sees, and an
ungrouped float total depends on them (D1). An earlier draft said every
setting here was free to change. The row-group size and the materialization
connection's `batch_size` are therefore part of the reproducibility contract:
the manifest records a snapshot format version that stands for both, and
changing either is a corpus rebuild.

The same holds for the ordered copy of a source (ADR-008 decision D2), which
polars writes and this writer does not. Its layout is pinned separately, in
`_CSV_PARQUET_WRITE` (`io.py:91-96`, row groups of 122,880 rows), and an entry
that totals a float column straight from a source reads that layout. The
format version covers those settings too. The second review checked that
polars honours them: `sink_parquet` wrote full row groups of exactly 122,880
rows, with an identical layout, on 1, 3 and 14 threads, from a CSV source and
from a parquet one (`scripts/spike_ordered_copy_layout.py`, polars 1.40.1).

The size is fixed in rows and not in bytes. A table with long text columns
therefore holds a large row group in memory while it is written, and the
contract above means the size cannot be tuned for one table.

### D4. A mismatch record names its likely cause

With D1 and D2 in place the causes left are the recipe's own nondeterminism
(`sample()`, `now()`, an impure UDF), source drift under `off` identity mode,
and an engine upgrade that changed results. The manifest records the xorq,
xorq-datafusion and pyarrow versions at build, together with the snapshot
format version (D3), and the `unfaithful_heal` record carries them alongside
the values at heal. When they differ, the message says so instead of blaming
the recipe. The contract's attribution table gains that row. One cause has no
attribution yet: a parent that is not reproducible was rewritten, so every
entry built on it heals to different rows. That belongs to #185.

### D5. It lands with the rebuild

Every worthy entry's digest is recomputed by the corpus rebuild of ADR-007
decision D9 (one change, one rebuild). The manifest field keeps its name.

### D6. Create runs the query twice and compares

Decided by Paddy in the grilling session (2026-09-20): "call the same query
twice... put some tests around this."

Today the build lint from #88 (`_nondeterminism_warnings`, `build.py`) warns
when a recipe uses one of five known non-pure operations, and nothing records
its verdict. Beyond that warning, tallyman learns that an entry is not
reproducible only when a deleted file is rewritten and its digest differs. By
then the original rows are gone, and everything built on them disagrees with
the new file without anyone knowing. So a check moves to the moment a
materialized entry is created:

- `materialize` runs the entry's query twice through the same writer, on the
  same single-partition connection (D1), and compares the two content digests
  (D2). Both runs go through the writer because D2's digest is defined on the
  file as read back. The second file is then discarded.
- When they match, the entry is recorded as reproducible, with its digest.
- When they differ, the build still succeeds, because a recipe that calls
  `sample()` is legitimate. The entry is recorded as not reproducible, its file
  is pinned (never deleted by tallyman, ADR-007 decision D12), it is badged in
  the UI, and the build result tells the author so, with the columns whose
  digests differed. This is the state ADR-006 decision D12 (unfaithful entries
  are pinned and badged) reaches after the damage is done. D6 reaches it first.
- A cheap entry is not checked here. An earlier draft ran it twice as well, and
  materialized and pinned it when the runs differed. That made a third kind of
  entry, a graph the classifier calls cheap with a file the manifest says
  exists, and whether an entry has a file stopped being a function of its
  graph, which decisions D2, D3 and D5 of
  `plans/ADR-007-tallyman-owned-materialization.md` all rely on. Paddy moved
  it to #185 on 2026-09-20. Until that is settled a cheap entry that calls
  `random()` behaves as it does today: the #88 lint warns, and the entry
  re-runs on every read. The second review proposed a smaller answer, recorded
  in ADR-008 decision D4 (the test for cheap) and not yet confirmed by Paddy:
  an operation that is not pure makes an entry worthy. Such a recipe is then
  checked here like any other materialized entry, and no cheap entry calls
  `random()`.

The cost is a second execution of every create of a materialized entry. It is
accepted under the priority recorded in ADR-007 ("a cohesive system that works
reliably" first).

Running twice has two limits, both part of #185. It cannot see `today()`,
since both runs agree; the #88 lint does. And it cannot see what an entry
inherits: an entry built on a non-reproducible parent reads the same parent
file in both runs and is recorded as reproducible, which holds only while that
file survives. The pin protects the file from the Cache page's delete and from
nothing else, since `compute_cache/` is deletable by definition (ADR-007
decision D7, the cold state is an empty `compute_cache`).

A heal runs the query once and is verified against the recorded digest, as
now. Only a create runs it twice, since a create has nothing recorded to
compare against. After D6 a mismatch at a heal means something changed
underneath a reproducible entry, which D4 attributes.

## Testing

Every test below goes in the failing-tests commit and is seen red on CI before
the change lands (ADR-007 decision D9, the order of work). A test of a function
that does not exist yet fails on import, and that counts as red. Paddy,
2026-09-20: do normal TDD.

- **Float aggregate reproduces** (D1). An entry with a float `SUM` and `AVG`
  over a source large enough to run in parallel is created, its file deleted,
  and the entry reopened, three times. Every rewrite must match the recorded
  digest, and no `unfaithful_heal` record may be written.
- **Digest ignores batching and format.** The same rows delivered as 8,192-row
  batches, 100,000-row batches and one table give one digest, and so do the
  same rows written Snappy with small row groups.
- **Digest sees what it must.** One changed value, one null replaced by `0.0`,
  and two swapped rows each change the digest.
- **Create detects a non-reproducible recipe.** A recipe whose UDF returns a
  different value on every call builds successfully, is recorded as not
  reproducible, names the offending column, and has a pinned file.
- **Create passes a reproducible recipe.** A deterministic recipe is recorded
  as reproducible, and the query is observed to run exactly twice.
- **The layout is pinned** (D1, D3). Every snapshot has row groups of 1,048,576
  rows, every ordered copy of a source has row groups of 122,880 rows, the
  materialization connection reports the pinned `batch_size`, and the manifest
  records the snapshot format version.
- **The build runs on the single-partition connection** (D1). While
  `materialize` executes a loaded build, every backend the plan touches reports
  `target_partitions = 1`, and the process default backend does not.
- **A heal runs once** (D6). Reopening an entry whose file was deleted runs its
  query exactly once.
- **A pinned file survives an explicit delete.** The Cache page's delete skips
  it and says why.
- **The schema comes from the file.** An entry with a `timestamp[s]` column
  records `timestamp[ms]`, and every entry's recorded schema ends in
  `__row_order`.

## Consequences

- A float-aggregate entry heals to the same digest, and a pyarrow upgrade no
  longer flags anything. The loud responses of ADR-006 decisions D10 and D12
  are left for the causes they were designed for.
- Materializations and heals are slower, by about 3x on aggregation at spike
  scale and up to 7x in ADR-004's parking measurement, and a create runs its
  query twice (D6). Reads are unaffected.
- A materialized entry whose recipe is not reproducible is known to be so from
  the moment it is created, and the Cache page's delete leaves its file alone.
  What that does not yet cover (a cheap entry, an entry that inherits the
  problem from its parent, a file lost with `compute_cache/`) is #185.
- Verify decodes the file instead of hashing its bytes. It runs on a heal and
  in `catalog_scan_staleness(verify_results=True)`, never on a read.
- Snapshots are smaller, and their footers were 85 times smaller in the spike,
  which matters to every page request that opens one.
- `docs/system-contract.md` changes in "Result digest" and in the manifest
  table ("SHA-256 of the baked result snapshot"), and its verification table
  gains the engine-change row.
- The same function can digest an ordered copy of a source (ADR-008, open
  question 5).
- The row-group size and the materialization connection's `batch_size` are
  frozen for the life of the corpus, and so are the settings polars writes an
  ordered copy of a source with. Changing any of them is a rebuild (D3).

## Implementation notes

- **Where the code is.** `src/tallyman_xorq/digest.py` (`content_digest`, `column_digests`, `digests_of_batches`) and
  `src/tallyman_xorq/materialize.py` (the writer, the single-partition connection, the two runs at create, the format
  version and the engine versions).
- **Nested types (open question 1).** Lists (and fixed-size and large lists), structs and maps are hashed recursively:
  each keeps its own validity, lengths and values streams and one set per child, a map is hashed as a list of
  `(key, value)` entries, and a type outside these (a union, an extension type) falls back to hashing the Python
  values, which is slow and correct.
- **The per-column digests** are what `differing_columns` reports when the two runs at create differ. A recipe with a
  random column that is also part of the canonical sort key can reorder rows between the runs, so other columns may
  be reported too.
- **D3.** `SNAPSHOT_FORMAT_VERSION = 1` stands for the snapshot row-group size (1,048,576), the materialization
  connection's batch size (8,192) and the ordered-copy row-group size (122,880). It is recorded in the manifest with
  the xorq, xorq-datafusion and pyarrow versions.
- **D4.** The heal record says the engine changed, naming the versions, when any recorded version or the format
  differs from today's, and otherwise keeps the structural (#88) and execution (#83) attribution.


## Open questions

1. **Nested types.** The spike covers fixed-width, boolean, string and binary
   columns. Lists, structs and maps need a recursive definition.
2. **Engine upgrades.** Single-partition execution fixes the merge order within
   one engine version. Nothing guarantees float results across versions. D4
   attributes that case and the remedy stays a rebuild.
3. **What else decides a float's low bits.** D1 and D3 pin the two variables
   found so far, the merge order and the batch boundaries. #187 tracks the
   mechanism, other reductions (variance, correlation, window frames), a filter
   between the scan and the aggregate, and results across machines and CPU
   architectures.
