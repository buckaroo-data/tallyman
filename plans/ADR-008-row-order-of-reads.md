# ADR: Row order of reads (a window is a function of its request)

- **Status:** Proposed (2026-09-18). Amends
  `plans/ADR-005-intelligent-csv-import.md` INV-2 (the trailing
  `order_by("original_row_order")`). Corrects a threshold quoted in
  `plans/ADR-006-read-path-loads-builds.md` D5 and three other places.
- **Context:** the 2026-09-18 cache audit (tallyman @ `a748ea6`, buckaroo
  0.15.4, xorq 0.3.26, xorq-datafusion 0.2.7). No ticket filed yet.
- **Affected code:** `src/tallyman_companion/app.py` (`api_data`,
  `app.py:930`, and the chart data it feeds), `src/tallyman_xorq/backend.py`
  (a second connection), `src/tallyman_xorq/io.py` (`tallyman_read_csv`,
  `io.py:627`), `src/tallyman_xorq/result_cache.py` (`_EXPENSIVE_OPS`),
  `src/tallyman_xorq/primary_key.py` (inheritance gate, `primary_key.py:204`).
  Buckaroo's paging is Buckaroo's code and is covered by D6.
- **Related ADRs:** `plans/ADR-004-result-digest-canonical-ordering.md` (why a
  canonical stored order exists, and the cost of pinning scan order on a
  connection that also aggregates), `plans/ADR-007-tallyman-owned-materialization.md`
  (what a snapshot is, and the corpus rebuild this shares),
  `plans/ADR-009-digest-stability.md`.
- **Evidence:** `scripts/spike_window_read_order.py`. All figures below are
  from that script on a 14-core machine.

## Problem

ADR-006 D5 made the bake deterministic: a worthy entry's snapshot is written in
a canonical total order, so the file is the same on every heal. Nothing made the
read of that file deterministic. `/api/data` serves a page as
`cached_result_expr(project, hash).limit(limit, offset=offset).execute()`
(`app.py:930`), charts pull `limit=100000` through the same endpoint, and
Buckaroo pages the grid the same way in its own process. An unsorted
`LIMIT/OFFSET` takes rows in whatever order the plan delivers them.

Eight identical requests for 50 rows from a 91 MB parquet file whose rows are
physically sorted by `id`:

| Plan | Offset | Distinct pages out of 8 | First id seen (file order would give) |
| --- | --- | --- | --- |
| read, limit | 0 | 5 | 0, 434176, 1294336 (0) |
| read, limit | 1,000,000 | 8 | 1360448, 1368640, 1565248 (1000000) |
| read, filter, computed column, limit | 0 | 8 | 1, 221185, 647168 (1) |
| read, filter, computed column, limit | 1,000,000 | 8 | 746336, 754526, 967522 (1500001) |

Paging through a large entry therefore repeats some rows and never shows
others, a chart over more than 100,000 rows changes each time it mounts, and an
author's own `order_by` is not honoured on screen: the file is sorted and the
window over it is not.

**The governing variable** is whether the physical plan has an exchange
operator (`RepartitionExec`, `CoalescePartitionsExec`) between the scan and the
limit. When it does, the rows reaching the limit are ordered by partition
arrival. There are three ways to get one:

- DataFusion splits a file scan into `target_partitions` byte ranges when the
  file is larger than `datafusion.optimizer.repartition_file_min_size`. In this
  engine that is 10,485,760 bytes (`SHOW ...` on a fresh `xo.connect()`), not
  the 1 MiB the repo states.
- Above a single-partition scan the planner still inserts
  `RepartitionExec: RoundRobinBatch(14)` under a filter or projection.
- A hash aggregate or join repartitions by key. This, and not file splitting,
  is what the #171 probe observed: its fixture is a 2.07 MB file, below the
  split threshold, and its plan is an aggregate.

**What INV-2 was for.** ADR-005 INV-2 keeps a trailing
`order_by("original_row_order")` on every `tallyman_read_csv` expression so that
the entry is snapshot-worthy and its order is canonical. Its costs, measured in
the audit:

- Every entry in a CSV lineage is worthy for that Sort alone, so every revision
  bakes a full sorted copy. A 9.3 MB CSV with four trivial revisions produced
  37 MB of snapshots. One real project holds 779 MB of data and 19 GB of
  `compute_cache`, with ten snapshots of 0.45 to 3.7 GB that are all
  `why=ops:Sort,SortKey`.
- Primary-key inheritance is gated on `not cache_worthy`
  (`primary_key.py:204`), so it never applies in a CSV lineage and every
  revision pays a full-table distinct scan.
- Above 10 MB it does not deliver a stable order on screen, for the reason
  above.

## Decisions

### D1. The contract: a window is a function of `(content_hash, sort, offset, limit)`

The same request returns the same rows in the same order, in any process and
any cache state. With no sort the rows come in the entry's **stored order**:

- for a worthy entry, the order of its snapshot, which is the canonical order
  of ADR-006 D5 (the author's `order_by` keys, then `original_row_order`, then
  the remaining sortable columns);
- for a cheap entry, the order its plan yields when executed with no exchange
  operator over inputs that each have a stored order.

This is the system contract's I1 applied to a page: a page is part of the
result a hash names.

### D2. Tallyman's windows run on a single-partition connection

A second connection, configured once with
`SET datafusion.execution.target_partitions = 1`, serves `/api/data` and chart
sampling. Everything else (stats, diffs, primary-key search, materialization)
stays on the parallel default backend. Binding onto it is the rebind ADR-006 D3
already does, which moves no data.

With that setting the plan for both shapes above is
`GlobalLimitExec <- (FilterExec) <- DataSourceExec` over one file group, with
no exchange operator:

| Setting | read, limit | read, filter, computed column, limit | 3M-row aggregate |
| --- | --- | --- | --- |
| default | 5 to 8 pages of 8, wrong rows | 6 to 8 pages of 8, wrong rows | 10 ms |
| `repartition_file_scans = false` | 1 of 8, file order | 1 of 8, **wrong page** on 100,000-row groups (first id 1423744, not 1500001); file order on 8,192-row groups | 25 ms |
| that plus `enable_round_robin_repartition = false` | 1 of 8, file order | 1 of 8, file order | 35 ms |
| `target_partitions = 1` | 1 of 8, file order | 1 of 8, file order | 33 ms |

`repartition_file_scans = false` alone is not enough. It keeps the scan in one
piece, the planner adds a round-robin repartition under the filter anyway, and
the page is then decided by how 14 partitions interleave. It returned the same
wrong page 8 times out of 8 on one file and the right page on another, which
is how a fix that is not one passes a test.

The aggregate column is the reason for a second connection: an order-pinning
setting on the shared backend would make every aggregate about three times
slower (ADR-004 measured 0.5 s against 3.4 to 3.9 s at 11.8M rows).

*Rejected:* the pair of optimizer settings. It works today, and it depends on
the optimizer having no other rule that introduces an exchange.
`target_partitions = 1` removes them by construction.
*Rejected:* an explicit `ORDER BY` on every window. Correct, and a full sort
of the entry per page.

### D3. The test asserts the plan's shape, not the absence of observed shuffling

The window path's test compiles a window on the window connection and asserts
that the physical plan contains no `RepartitionExec`, `CoalescePartitionsExec`
or `SortPreservingMergeExec`. A test that watches for shuffled rows passes on
any fixture below the split threshold, which is how the 1 MiB figure went
unchallenged and how `repartition_file_scans = false` looked sufficient.

### D4. A cheap plan that keeps an exchange operator is classed worthy

`Union` is not in `_EXPENSIVE_OPS`, and `UnionExec` emits one partition per
input, so `CoalescePartitionsExec` survives `target_partitions = 1`: a union of
two files returned 2 distinct pages out of 8 on the single-partition
connection. Such an entry has no stored order to serve. `Union` (with
`Intersection` and `Difference`, which plan as joins) joins `_EXPENSIVE_OPS`,
so the entry is materialized in canonical order and its windows become bare
reads. D3's assertion is what reports the next op of this kind.

*Rejected:* sort such windows on demand. A full sort of the union per page.

### D5. `tallyman_read_csv` drops its trailing `order_by` (amends ADR-005 INV-2)

`io.py:627` returns `deferred_read_parquet(intermediate)` with no sort. INV-1
stays: `original_row_order` is still a column holding `0..N-1` in file order,
and polars writes the intermediate in that order. The root entry becomes a
cheap bare read of the intermediate, whose stored order is file order, so D1
and D2 give file-order pages with no Sort and no second copy.

What INV-2 provided, and what replaces it:

| INV-2 gave | Replacement |
| --- | --- |
| A canonical display order | D1 and D2. INV-2 did not deliver this above 10 MB. |
| A parquet boundary for chained children | A cheap root's graph is one read node. |
| A `result_digest` on the root, so a re-parse that produced different rows would be caught | Lost as it stands: cheap entries record no digest (ADR-006 D9). See open question 1. |

Every hash in every CSV lineage changes, so this rides the corpus rebuild of
ADR-007 D9. It does not depend on ADR-007: with or without bare-read chaining
the root becomes cheap, and children become cheap unless they do expensive
work themselves.

### D6. Buckaroo's half is a Buckaroo issue

Buckaroo pages in its own process on its own connection, so the grid has the
same defect and tallyman cannot fix it from outside. The issue to file asks
for D1's contract (unsorted windows repeatable and in stored order; sorted
windows repeatable when the sort key has ties) and leaves the mechanism to
Buckaroo. After ADR-007 D6 the file Buckaroo reads for a worthy entry is
tallyman's snapshot, so the stored order is already in place on the file side.
Not filed yet.

### D7. Correct the threshold

`repartition_file_min_size` is 10,485,760 in this engine. Four places say 1 MiB
and are corrected with this change: `plans/ADR-006-read-path-loads-builds.md:98`,
`plans/datafusion-scan-order-findings.md:64`,
`tests/test_parquet_digest_order_probe.py:5` (whose `> 1_048_576` size guard
does not establish a split scan and is not what makes that test meaningful;
its aggregate is), and `src/tallyman_xorq/source_cache.py:98`.
`tests/test_tallyman_read_csv.py:159` already says about 10 MB.

## Consequences

- `/api/data` pages and chart samples become repeatable and come in stored
  order, for worthy and cheap entries alike.
- Latency on the window connection in the spike: 1 to 4 ms at offset 0 and 13
  to 34 ms at offset 1,000,000, against 1 to 10 ms for the unstable default.
  DataFusion decodes and discards the skipped rows, so the cost grows with the
  offset (open question 2).
- CSV lineages stop baking sorted copies. With ADR-007, revisions of a CSV
  entry are cheap reads over one intermediate file, and primary-key
  inheritance applies to them.
- Union entries gain a materialized copy, which is the price of having a
  defined row order.
- The grid stays unstable above 10 MB until Buckaroo's half lands.

## Open questions

1. **A digest for the CSV intermediate.** The intermediate parquet is the
   record of a parse and is re-created from the CAS clone if deleted. Recording
   its digest in the root entry's manifest, and verifying it on re-creation,
   would restore what D5 gives up. It wants ADR-009's digest definition, and it
   touches where the intermediate lives: `csv_ordered` is global, is never
   collected, and is not packed.
2. **Deep offsets.** A window far into a wide snapshot decodes everything
   before it. Because tallyman writes the snapshot (ADR-007, ADR-009), a bare
   read's window is a row range, and row-group metadata says which one or two
   row groups hold it. Measure on the parking corpus before building anything.
3. **`Distinct` is classed cheap** although it executes as a hash aggregate.
   Its single-partition window was repeatable in the spike (1 page of 8, no
   exchange operator), which reflects the aggregate's emission order in this
   engine version rather than a guarantee. Classing it worthy would make its
   order canonical and stop a full scan per page.
