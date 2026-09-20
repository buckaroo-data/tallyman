# ADR: Row order of reads (every file carries `__row_order`, every page sorts by it)

- **Status:** Proposed (2026-09-18, revised 2026-09-20 in the grilling
  session, and again the same day after a review of PR #184, which made the
  fix for #168 a precondition of D2 and D7). Awaiting Paddy's review; nothing
  here is implemented. The first draft pinned row order with an engine setting. Paddy
  proposed baking a row-order column into every file tallyman writes and
  sorting every page by it. The measurements below favour that, so it is now
  the decision and the engine setting is the rejected alternative under D5.
  Amends `plans/ADR-005-intelligent-csv-import.md` INV-1 (the name and position
  of the row-order column) and INV-2 (the trailing `order_by`). Corrects a
  threshold quoted in `plans/ADR-006-read-path-loads-builds.md` decision D5
  (the canonical sort) and three other places.
- **Context:** the 2026-09-18 cache audit (tallyman @ `a748ea6`, buckaroo
  0.15.4, xorq 0.3.26, xorq-datafusion 0.2.7).
- **Tickets:** #168 (CSV sources bypass source identity; D2 and D7 depend on
  its fix), buckaroo-data/buckaroo#974 (Buckaroo's half of D5, see D8), #188
  (diffs, moved out of ADR-007).
- **Affected code:** `src/tallyman_xorq/source_cache.py` (`rewrite_for_build`,
  `_tie_break_order`), `src/tallyman_xorq/io.py` (`read_project_file`,
  `tallyman_read_csv`, `io.py:627`, and for #168 `_ordered_csv_key` and
  `_ordered_csv_parquet`), `src/tallyman_xorq/source_identity.py` (the three
  steps a CSV now goes through), `src/tallyman_xorq/result_cache.py`
  (`_EXPENSIVE_OPS`, `classify_build`), `src/tallyman_companion/app.py`
  (`api_data`, `app.py:930`, and the chart data it feeds),
  `src/tallyman_xorq/primary_key.py` (candidate selection,
  `primary_key.py:219`), `src/tallyman_companion/diff.py`
  (`build_compare_expr`), and the `materialize` writer introduced by
  `plans/ADR-007-tallyman-owned-materialization.md` decision D4. Buckaroo's
  paging is Buckaroo's code and is covered by D8.
- **Related ADRs:** `plans/ADR-004-result-digest-canonical-ordering.md` (why a
  canonical stored order exists), `plans/ADR-007-tallyman-owned-materialization.md`
  (what a snapshot is, and the corpus rebuild this shares),
  `plans/ADR-009-digest-stability.md` (the file format, which D5 adds a
  requirement to).
- **Evidence:** `scripts/spike_row_order_paging.py` (the decisions),
  `scripts/spike_window_read_order.py` (the problem, and the rejected
  engine-setting approach), `scripts/spike_csv_source_identity.py` (D2 and D7:
  what a CSV edit does to a content hash) and
  `scripts/spike_deep_page_memory.py` (the memory figures under Consequences).
  All figures are from those scripts on a 14-core machine.

## Terms

- **Entry:** one catalog computation, stored under its content hash.
- **Materialize:** run an entry's computation once and write the result to a
  parquet file. That file is the entry's **snapshot**.
- **Worthy entry:** an entry tallyman materializes. **Cheap entry:** one it
  does not, whose small plan re-runs on every read. D4 draws the line.
- **Page request:** a request for `limit` rows starting at `offset`. `/api/data`,
  charts and Buckaroo's grid all issue them. The first draft of this ADR called
  it a "window".
- **Row-preserving:** each output row comes from exactly one input row, and no
  input row produces more than one output row. Filters, column selections,
  computed columns, renames and casts qualify. Aggregates, joins, unions,
  distincts and unnests do not.
- **Tie:** two or more rows with equal values in every sort key.
- **Exchange operator:** a DataFusion physical-plan step (`RepartitionExec`,
  `CoalescePartitionsExec`) that moves rows between parallel partitions. After
  one, rows arrive in whatever order the partitions finish.

## Problem

Decision D5 of ADR-006 (the canonical sort) made the write deterministic: a
worthy entry's snapshot is written in a fixed total order, so the file is the
same on every rebuild. Nothing made the read of that file deterministic.
`/api/data` serves a page as
`cached_result_expr(project, hash).limit(limit, offset=offset).execute()`
(`app.py:930`), charts pull `limit=100000` through the same endpoint, and
Buckaroo pages the grid the same way in its own process. A `LIMIT/OFFSET` with
no `ORDER BY` takes rows in whatever order the plan delivers them.

Eight identical requests for 50 rows from a 91 MB parquet file whose rows are
physically sorted by `id` (`scripts/spike_window_read_order.py`):

| Plan | Offset | Distinct pages out of 8 | First id seen (file order would give) |
| --- | --- | --- | --- |
| read, limit | 0 | 5 | 0, 434176, 1294336 (0) |
| read, limit | 1,000,000 | 8 | 1360448, 1368640, 1565248 (1000000) |
| read, filter, computed column, limit | 0 | 8 | 1, 221185, 647168 (1) |
| read, filter, computed column, limit | 1,000,000 | 8 | 746336, 754526, 967522 (1500001) |

A sort does not help when its key has ties. Sorting 3,000,000 rows by a column
with 200 distinct values and asking for the page at offset 100,000 returned 6
different pages for 6 identical requests (`scripts/spike_row_order_paging.py`).
Sorting the grid by a category or a date is exactly that case.

Paging through a large entry therefore repeats some rows and never shows
others, a chart over more than 100,000 rows changes each time it mounts, and an
author's own `order_by` is not honoured on screen: the file is sorted and the
page taken from it is not.

**The governing variable** for the unsorted case is whether the physical plan
has an exchange operator between the scan and the limit. There are three ways
to get one:

- DataFusion splits a file scan into byte ranges, one per partition, when the
  file is larger than `datafusion.optimizer.repartition_file_min_size`. In this
  engine that is 10,485,760 bytes (`SHOW ...` on a fresh `xo.connect()`), not
  the 1 MiB the repo states.
- Above a single-partition scan the planner still inserts
  `RepartitionExec: RoundRobinBatch(14)` under a filter or projection.
- A hash aggregate or join repartitions by key. This, and not file splitting,
  is what the #171 probe observed: its fixture is a 2.07 MB file, below the
  split threshold, and its plan is an aggregate.

**What INV-2 of ADR-005 was for.** INV-2 keeps a trailing
`order_by("original_row_order")` on every `tallyman_read_csv` expression so
that the entry is worthy and its order is canonical. Its costs, measured in the
audit:

- Every entry in a CSV lineage is worthy for that Sort alone, so every revision
  writes a full sorted copy. A 9.3 MB CSV with four trivial revisions produced
  37 MB of snapshots. One real project holds 779 MB of data and 19 GB of
  `compute_cache`, with ten snapshots of 0.45 to 3.7 GB that are all
  `why=ops:Sort,SortKey`.
- Primary-key inheritance is gated on `not cache_worthy`
  (`primary_key.py:204`), so it never applies in a CSV lineage and every
  revision pays a full-table distinct scan.
- Above 10 MB it does not deliver a stable order on screen, for the reason
  above.

## Decisions

### D1. The contract: a page is a function of `(content_hash, sort, offset, limit)`

The same page request returns the same rows in the same order, in any process
and any cache state, with or without a user sort. With no user sort the rows
come in `__row_order` order (D2). This is the system contract's invariant I1
("a content hash names a fixed result") applied to a page.

### D2. Every file tallyman reads carries `__row_order`

`__row_order` is an `int64` column holding `0..N-1` in the file's physical row
order. It is the last column, and it is visible: Buckaroo shows it as the final
column of the table.

Two writers produce it:

- **`materialize`** (ADR-007 decision D4, the one writer of snapshots) numbers
  the rows of the canonically sorted stream as it writes them. If the stream
  already has a `__row_order` inherited from a parent, the writer replaces that
  one column. Each materialization therefore overwrites `__row_order` with
  positions in its own file.
- **Ingest.** A source file enters tallyman through an ordered copy: polars
  scans it, `with_row_index` numbers the rows in file order, and the copy is
  written with the column last. The copy is keyed by the source's content and
  written once. Both kinds of source go through source identity first
  (`si.digest_for`, then `si.ensure_cas_path`, then `si.note_source`, the three
  steps `read_project_file` performs for parquet today, `io.py:78-84`), and the
  ordered copy is built from the content-addressed clone, which stays the
  immutable input. A source that already has a `__row_order` column has it
  overwritten, which is the right outcome for a file tallyman exported.

  CSVs have the ordered-copy step today and not the keying. The first draft of
  this decision said they already worked this way, which was wrong. The
  intermediate under `csv_ordered/` is keyed by `md5(absolute path | schema |
  reader options)` (`io.py:515-527`), it is overwritten in place when the CSV's
  mtime changes (`io.py:558-582`), and `tallyman_read_csv` never touches source
  identity. That is #168, and D7 cannot land before its fix. Building the copy
  from the clone is enough: the key function already hashes the path, and the
  clone's path carries the digest, which is the device of
  `plans/ADR-002-source-identity-content-hash.md`. The clone never changes, so
  the in-place overwrite becomes dead code and is deleted.

The canonical sort's tie-break (`_tie_break_order`) puts an inherited
`__row_order` where `original_row_order` is today: after the author's own
`order_by` keys and before the remaining columns. A worthy entry that keeps its
parent's rows, such as one adding a window function, therefore keeps the
parent's order.

*Rejected:* `row_number()` inside the entry's graph. It needs the same global
sort, adds a window function to every worthy build, and leaves contiguity to
the engine. A counter in the writer is contiguous and physical by construction,
which D5's range requests depend on.
*Rejected:* a row number kept only as file metadata. DataFusion exposes no
parquet row number that a query can sort or filter by, so it has to be a column.

### D3. A cheap entry that drops `__row_order` is a build error

`__row_order` is metadata that happens to be a column, and it is not to be
deleted. A column selection is an allow-list: `t.select("g", "n")` drops every
column it does not name, so an author who is not thinking about row order
drops it without meaning to. When a cheap entry's output lacks `__row_order`,
the build fails, and the error names the parent entry, says that a
row-preserving view must keep `__row_order` so that paging stays repeatable,
and shows the fix (`t.select("g", "n", "__row_order")`). The MCP tool
descriptions say the same thing up front. This is the feedback channel
`tallyman_read_csv` already uses when a CSV has a column named
`original_row_order`.

A worthy entry is exempt, because the writer numbers its rows (D2). An author
changes `__row_order` by asking for an order: `order_by` makes the entry worthy,
and the writer numbers the rows in the requested order. Assigning to the column
directly stays an error (D6).

Tallyman makes one alteration of its own, at the top of the expression only: it
moves `__row_order` to the last position, since a computed column added after
it would otherwise push it into the middle of the table.

The column can be copied for debugging.
`foo_v1.mutate(__row_order_v1=foo_v1["__row_order"])` gives the new entry both
columns. Once the new entry is materialized, `__row_order` holds its own
positions and `__row_order_v1` still says where each row sat in the parent.

*Rejected:* carry the column automatically. `rewrite_for_build` can rewrite
every column selection in a cheap graph to keep it, and the spike shows that
working: `t.filter(t.g < 100).select("g", "v0").mutate(z=t.v0 * 2)` comes out
with columns `['g', 'v0', 'z', '__row_order']` and pages repeatably. It was
rejected because the recipe text and the entry's columns would then disagree,
because it is surgery inside an expression an LLM wrote, and because a mistake
the author can fix in one line is better reported than silently repaired. The
cost accepted is a failed build whenever a select list forgets the column.
*Rejected:* carry the column and hide it from the grid. Paddy's call: it is
shown, as the final column.

### D4. Cheap means row-preserving over one file; everything else is materialized

A cheap entry inherits its row order, so it must be a row-preserving plan over
exactly one file. The classifier changes from a deny-list to an allow-list: an
entry is cheap only if every relation operation in its graph is known to be
row-preserving (a file read, a filter, a column selection, a computed column, a
rename, a cast, a column drop). Anything else is worthy, including operations
nobody has thought about yet. Today's `_EXPENSIVE_OPS` deny-list classes
`Union`, `Distinct` and `Unnest` as cheap, and none of them can carry one
parent's row order.

A new or unknown operation now costs a copy (safe) instead of unstable paging
(unsafe). `classify_build` (which reads the serialized build) and
`_is_worthy_expr` (which reads the live expression) flip together, as they must
today.

Supporting measurement from the first draft: a union of two files returned 2
different pages for 8 identical requests even on a single-partition
connection, because `UnionExec` emits one partition per input and an exchange
operator merges them.

### D5. Every page request orders by `__row_order`

- No user sort: `ORDER BY __row_order`.
- User sort: the user's keys, then `__row_order` ascending as the last key,
  which breaks every tie.

That is the whole rule, for every entry and both processes. Two faster paths
exist and are deliberately not part of this decision (Paddy, 2026-09-20: a
cohesive system that works reliably comes first, and speed problems are handled
as they come up):

- **Declared file order.** Telling the engine the file is already sorted by
  `__row_order` removes the sort from the plan.
- **Range request.** For the unfiltered, unsorted view of a materialized file a
  row's position equals its `__row_order`, so a page can be fetched as
  `__row_order >= offset AND __row_order < offset + limit` instead of `OFFSET`.

Both are measured below so the numbers are on hand when they are wanted.

Measured on 3,000,000 rows by 14 columns (287 MB), on the default parallel
connection with no engine settings. Every row of the table returned the correct
page 6 times out of 6:

| Page request | Offset 0 | Offset 1,000,000 | Offset 2,900,000 |
| --- | --- | --- | --- |
| `ORDER BY __row_order LIMIT 50 OFFSET k` | 100 ms | 332 ms | 374 ms |
| The same, with the file's order declared to the engine (`WITH ORDER`) | 25 ms | 102 ms | 242 ms |
| Range request, row-group statistics only | 90 ms | 90 ms | 79 ms |
| Range request, file written with a parquet page index | 19 ms | 24 ms | 23 ms |
| First draft's approach: bare `LIMIT/OFFSET`, single-partition connection | 21 ms | 90 ms | 249 ms |

And the sorted case, at offset 100,000: `ORDER BY g` gave 6 distinct pages in 6
requests (214 ms); `ORDER BY g, __row_order` gave 1 (302 ms).

One consequence for the file format, recorded in ADR-009 decision D3: the
writer puts `__row_order` last. It also emits a parquet page index, which costs
nothing now and is what takes a later range request from 90 ms to 20 ms.

Declaring the file's order to the engine removes the sort from the plan
(`GlobalLimitExec <- SortPreservingMergeExec <- DataSourceExec`, no
`SortExec`). The spike registers the file through
`CREATE EXTERNAL TABLE ... WITH ORDER`. Whether the declaration can travel
inside a xorq build is untested (open question 3), so it is an optimization
here and not part of the decision.

*Rejected:* the first draft's decision, a second connection with
`target_partitions = 1` for page requests. It makes unsorted pages repeatable
(bottom row of the table) at the same cost as a declared order. It was
rejected because:

- it depends on the engine's planner never introducing an exchange operator,
  and `repartition_file_scans = false`, which looked sufficient in an earlier
  experiment, returned the same wrong page 8 times out of 8 for a filtered plan
  on a file with 100,000-row groups;
- it has to be reproduced inside Buckaroo's process;
- it does nothing for a user sort with ties;
- it fails for a union.

An `ORDER BY` on a column with no ties is repeatable by the query's own
semantics, in any engine and any process.

### D6. The exact name `__row_order` is reserved

- A recipe may read the column and may copy it under another name (D3). A
  recipe that assigns to `__row_order` is a build error, because arbitrary
  values could contain ties or gaps, and D5 depends on `0..N-1` with neither.
- Only the exact name is special. `__row_order_v1`, or any other name an author
  picks for a copy, is ordinary data and survives materialization.
- A join of two entries leaves the right side's copy behind under ibis's
  collision name, `__row_order_right`, and a three-way join silently keeps only
  the first two. That column is ordinary data too: it says where the row sat in
  the right-hand parent. The writer replaces only `__row_order` itself.
- The primary-key search skips it. Nothing excludes `original_row_order` from
  the candidates today (`primary_key.py:219`), and a column that is unique in
  every table would win the search for any table without a string or id key.
  Row positions shift between versions, so a diff keyed on it would be
  meaningless.
- `build_compare_expr` drops it from both sides before joining. A promoted
  diff is a worthy entry, since it contains a join, and gets its own when it is
  materialized. The live diff grid has none until #188 lands (diffs built as
  entries, moved out of ADR-007), so its paging stays as it is today.

### D7. `tallyman_read_csv` loses its trailing `order_by`, and its column becomes `__row_order`

Amends ADR-005. INV-2: `io.py:627` returns a plain read of the intermediate
parquet with no `order_by`. INV-1: the row-index column is named `__row_order`
and written last, so a CSV root has one row-order column and not two with
identical values. The root entry becomes a cheap read of the intermediate, and
D5 gives file-order pages with no Sort and no second copy.

This depends on the fix for #168 (D2). Today the trailing Sort is the only
thing that keeps a CSV root's rows fixed: it makes the root worthy, so a baked
snapshot freezes them, and a heal that read a changed intermediate would be
flagged. Without the Sort and without the fix, editing a CSV and re-running the
same recipe gives the same content hash, so no new version is created, and the
first version's frozen build returns the edited rows (`[10, 999, 30, 40]` where
it was built from `[10, 20, 30]`). With the ordered copy keyed on the clone, the
edit gives a new hash and the first version keeps its rows
(`scripts/spike_csv_source_identity.py`). The hash does not move today either,
with the Sort in place, so an edited CSV under an unchanged recipe produces no
new version. The fix for #168 corrects that as well.

What INV-2 provided, and what replaces it:

| INV-2 gave | Replacement |
| --- | --- |
| A canonical display order | D5. INV-2 did not deliver this above 10 MB. |
| A parquet boundary for chained children | A cheap root's graph is one read node. |
| Fixed rows under the root's hash, through its baked snapshot | The content-keyed ordered copy of D2, written once. Requires the fix for #168. |
| A `result_digest` on the root, so a re-parse that produced different rows would be caught | Lost as it stands: cheap entries record no digest (ADR-006 decision D9, "no cheap-entry digests"). It matters only when an ordered copy is deleted and re-created. See open question 5. |

Every hash in every CSV lineage changes, so this rides the corpus rebuild of
ADR-007 decision D9 ("one change, one rebuild").

### D8. Buckaroo's half is one hint and one Buckaroo issue

Buckaroo pages in its own process, so the grid needs the same rule and tallyman
cannot apply it from outside. Tallyman passes the column's name in the
`/load_expr` payload as a hint. The Buckaroo issue asks that, given the hint,
Buckaroo sorts by it when the user has chosen no sort, appends it as the last
key of any user sort, and may use range requests for the unfiltered, unsorted
view. Without the hint Buckaroo behaves as it does now. Filed as
buckaroo-data/buckaroo#974.

The issue reproduces the defect through Buckaroo's own page builder.
`_window_to_parquet` (`buckaroo/xorq_buckaroo.py`, lines 302-330 in 0.15.6)
sorts on a single key, `expr.order_by(expr[sort_col].asc())`, and applies no
`order_by` at all when the user has chosen no sort. Six identical calls for the
same 50 rows of a 27 MB file gave 5 different results unsorted and 6 sorted by
a column with 200 distinct values.

### D9. Correct the threshold

`repartition_file_min_size` is 10,485,760 in this engine. Four places say 1 MiB
and are corrected with this change: `plans/ADR-006-read-path-loads-builds.md:98`,
`plans/datafusion-scan-order-findings.md:64`,
`tests/test_parquet_digest_order_probe.py:5` (whose `> 1_048_576` size guard
does not establish a split scan and is not what makes that test meaningful;
its aggregate is), and `src/tallyman_xorq/source_cache.py:98`.
`tests/test_tallyman_read_csv.py:159` already says about 10 MB.

## Testing

Every test below goes in the failing-tests commit and is seen red on CI before
the change lands (ADR-007 decision D9, the order of work). A test of a function
that does not exist yet fails on import, and that counts as red. Paddy,
2026-09-20: do normal TDD.

- **Repeatable pages** (D5). Eight identical `/api/data` requests at a deep
  offset into an entry whose file is larger than 10,485,760 bytes each return
  exactly the rows at positions `offset` to `offset + limit - 1` in
  `__row_order` order, for a worthy entry and for a cheap one. Asserting only
  that the eight agree is not enough: one rejected setting returned the same
  wrong page 8 times out of 8 (D5). The sorted case is Buckaroo's code and is
  tested there (buckaroo-data/buckaroo#974).
- **An edited CSV forks the hash** (D2, #168). Editing a CSV and re-running the
  same recipe gives a new content hash, and the earlier entry still returns the
  rows it was built from.
- **The column** (D2). Every file tallyman writes ends in `__row_order`, holding
  `0..N-1` with no gaps, and a materialization replaces an inherited one.
- **Dropping it fails the build** (D3). A cheap recipe whose select list omits
  the column raises a build error whose message contains the corrected select.
  A worthy recipe that omits it builds.
- **Asking for an order renumbers** (D3). An `order_by` recipe's file is
  numbered in the requested order, and assigning to the column is a build
  error.
- **A debugging copy survives** (D6). `__row_order_v1` is still present, with
  the parent's positions, after the child is materialized.
- **Classification** (D4). A union, a distinct, an unnest and an operation the
  allow-list has never seen are all classed worthy.
- **Reserved** (D6). The primary-key search never returns `__row_order`, and a
  diff has no `__row_order_v2` column.
- **CSV roots** (D7). A `tallyman_read_csv` entry has no Sort in its build, is
  classed cheap, and has exactly one row-order column.
- **The hint** (D8). The `/load_expr` payload names `__row_order`.

## Consequences

- Pages are repeatable for unsorted and sorted requests, in tallyman and in
  Buckaroo, with no engine settings and no second connection.
- Every table shows one more column, at the end. A join result also shows
  `__row_order_right` unless the recipe drops it.
- A recipe whose select list forgets `__row_order` fails to build until the
  author adds it.
- CSV lineages stop writing sorted copies. With ADR-007, revisions of a CSV
  entry are cheap reads over one intermediate file, and primary-key inheritance
  applies to them.
- Unions, distincts and unnests are materialized, which is the price of having
  a defined row order.
- Each source costs one ordered copy, about the size of the source. A CSV
  source also gains a content-addressed clone of the CSV, which is
  copy-on-write where the filesystem offers it.
- An unsorted page costs a sort of one column unless the file's order is
  declared (100 to 374 ms against 25 to 242 ms in the spike). A range request
  costs about 20 ms at any depth.
- A sorted page also holds more in memory the deeper it is. On the spike's
  file (336 MB as Arrow) peak process memory was 626 MB at offset 0, 1,142 MB
  at 1,000,000 and 1,176 MB at 2,900,000, against 300 MB for a bare limit and
  248 MB for a range request (`scripts/spike_deep_page_memory.py`). The corpus
  holds a 3.68 GB snapshot. Paddy, 2026-09-20: a performance matter, taken up
  after correctness. The range request is the path with bounded memory.
- The grid stays unstable until Buckaroo's half (buckaroo-data/buckaroo#974)
  lands: above 10 MB when unsorted, and at any size when sorted by a column
  with ties.

## Open questions

1. **Ordered copies of parquet sources.** D2 adds a copy per parquet source.
   Adopted as the uniform rule under Paddy's "cohesive first" priority, and not
   yet confirmed by him in so many words. It also assumes polars numbers a
   parquet scan's rows in file order, as ADR-004 measured for CSV, which needs
   checking.
2. **Renaming `original_row_order`.** D7 replaces it with `__row_order`.
   Adopted on the same basis, and also not yet confirmed. The alternative keeps it as a data column meaning "line of
   the source file", at the cost of two identical columns on every CSV root.
3. **Declaring a file's order inside a xorq build.** It works through DDL on a
   connection. If it can ride in a build's read node, Buckaroo's unsorted pages
   get the cheaper plan too.
4. **Deep offsets on a cheap entry.** Positions in a filtered view have gaps,
   so it pages with `OFFSET`, whose cost grows with depth.
5. **A digest for an ordered copy.** An ordered copy is the record of a parse
   and is re-created from the clone if deleted. With the copy keyed by content
   (D2) a given path always holds the same parse, so what is left unverified is
   a re-creation, for example after a polars upgrade that parses differently.
   Recording the copy's digest in the root entry's manifest, and verifying it
   on re-creation, would cover that. It wants ADR-009's digest definition, and
   it touches where the copies live: `csv_ordered` is global, is never
   collected, and is not packed (ADR-007 open question 3). Nothing re-creates a
   missing copy automatically yet, which ADR-007 decision D5 (one entry point
   makes files exist) records as an accepted gap.
