# ADR: Row order of reads (every file carries `__row_order`, every page sorts by it)

- **Status:** Accepted (2026-09-22). Implemented in #189.
  History: implemented in buckaroo-data/tallyman#189 on 2026-09-21, at Paddy's request to implement the
  set, and accepted on 2026-09-22, when `plans/ADR-010-immutable-store-one-owner.md` (a proposal to replace
  this set) was rejected and #189 was chosen as the direction. Where this text says a decision is "not yet
  confirmed" or "Proposed", it was implemented as written; the differences between the text and the code
  are under "Implementation notes" below. The defects the review of #189 found are open as #193 to #211;
  those touching this ADR are #199, #200, #205 and #206. Three more, #197, #198 and #211, were about the
  ordered copy of D2, which `plans/ADR-011-sources-are-aliases.md` replaced with the source entry's snapshot,
  so they no longer apply.
  Original status: Proposed (2026-09-18, revised 2026-09-20 in the grilling session, and again the same day after a review of PR #184, which made the fix for #168 a precondition of D2 and D7, and a third time that day after a second review: D4 and D6 were tightened, and D10 to D12 are new). Awaiting Paddy's review; nothing here is implemented. The first draft pinned row order with an engine setting. Paddy proposed baking a row-order column into every file tallyman writes and sorting every page by it. The measurements below favour that, so it is now the decision and the engine setting is the rejected alternative under D5. Amends `plans/ADR-005-intelligent-csv-import.md` INV-1 (the name and position of the row-order column) and INV-2 (the trailing `order_by`). Corrects a threshold quoted in `plans/ADR-006-read-path-loads-builds.md` decision D5 (the canonical sort) and three other places.
  **Extended by `plans/ADR-011-sources-are-aliases.md` (2026-09-22, PR #218).** D2 and D7 here make a raw
  `xo.deferred_read_parquet` a build error, so that every source enters through an ordered copy. ADR-011 D2
  applies the same rule to `read_project_file` and `tallyman_read_csv` themselves: there is now no way to
  author a read of a file the catalog does not own, and the sanctioned route in is an explicit import.
  The ordered copy is then not a separate kind of file. An imported source version is an ordinary entry
  (ADR-011 D1) and the copy **is** that entry's snapshot: one parquet in
  `compute_cache/result_cache/`, named by the entry's content hash, written by pyarrow in the pinned
  layout of ADR-009. `compute_cache/ordered_sources/`, the `copy_key`
  (`md5(digest ǀ reader signature)`) that named files in it, `manifest.ordered_copies` and the
  `ensure_ordered_copy` / `existing_ordered_copy` / `recreate_ordered_copy` trio are all deleted.
  What survives from `ordered_copy.py` is what describes a read rather than performs one:
  `ORDERED_COPY_ROW_GROUP_ROWS`, the reader descriptors (`parquet_reader`, `csv_reader`) and the reader
  signature that the source entry's hash is built from.
  Everything this ADR decided about row order is unchanged: every file tallyman writes still carries
  `__row_order` as a last `int64` column, `0..N-1`, and every page still sorts by it.
- **Context:** the 2026-09-18 cache audit (tallyman @ `a748ea6`, buckaroo
  0.15.4, xorq 0.3.26, xorq-datafusion 0.2.7).
- **Tickets:** #168 (CSV sources bypass source identity; D2 and D7 depend on
  its fix), buckaroo-data/buckaroo#974 (Buckaroo's half of D5, see D8), #188
  (diffs, moved out of ADR-007), #12 (the classifier reads `expr.yaml` with a
  regex, which D4 retires), #146 (ordering inside window functions and
  ordered aggregates, which D10 leaves there).
- **Affected code:** `src/tallyman_xorq/source_cache.py` (`rewrite_for_build`,
  `_tie_break_order`, `_canonical_sorted`, `_is_worthy_expr`),
  `src/tallyman_xorq/build.py` (`_csv_direct_read_check`, which gains a
  parquet sibling, and the hint that turns ibis's name-collision error into
  an instruction), `src/tallyman_xorq/io.py` (`read_project_file`,
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
  what a CSV edit does to a content hash),
  `scripts/spike_deep_page_memory.py` (the memory figures under Consequences),
  and four from the second review: `scripts/spike_cheap_classifier.py` (D4),
  `scripts/spike_row_order_joins.py` (D6), `scripts/spike_sort_grafting.py`
  (D10, D11 and the note on parquet statistics under D5) and
  `scripts/spike_ordered_copy_layout.py` (D2 and open question 1).
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
- **Natural order:** the order of rows in the file they came from, which
  `__row_order` records (D2).
- **Graft:** to add sort keys to a query that its author did not write.
- **Hoist:** to take the keys of a sort from lower in a recipe and lead the
  top-level sort with them (D11).
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
  overwritten, which is the right outcome for a file tallyman exported. The
  copy lives under the project's `compute_cache/`, and `ensure_materialized`
  makes it again from the clone when it is missing (decision D13 of
  `plans/ADR-007-tallyman-owned-materialization.md`, which files are cache).
  D12 closes the one way a parquet file could enter without a copy.

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
parent's order. D10 applies the same tie-break to every sort in a recipe, not
only to the last one.

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
and the writer numbers the rows in the requested order. That holds for an
`order_by` anywhere in the recipe only because of D11. As first drafted it was
true of an `order_by` that is the recipe's last step, and of no other.
Assigning to the column directly stays an error (D6).

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
rename, a cast, a column drop, a drop of null rows, a fill of nulls). Anything
else is worthy, including operations nobody has thought about yet. Today's
`_EXPENSIVE_OPS` deny-list classes `Union`, `Distinct` and `Unnest` as cheap,
and none of them can carry one parent's row order.

A list of relation operations is not enough, which the second review measured
(`scripts/spike_cheap_classifier.py`). Paddy confirmed on 2026-09-20 that two
kinds of entry stay (open question 1 of
`plans/ADR-007-tallyman-owned-materialization.md`), so this test is what
guarantees that a cheap entry pages repeatably, and it has to look inside the
allowed operations as well. The parent has 6 rows:

| Recipe shape | Relation operations | Today's deny-list | Relation allow-list alone | Rows | `__row_order` unique |
| --- | --- | --- | --- | --- | --- |
| `t.select("k", "__row_order", tag=t.tags.unnest())` | read, select | cheap | cheap | 8 | no |
| `t.mutate(rn=ibis.row_number())` | read, select | worthy | cheap | 6 | yes |
| `t.mutate(prev=t.v.lag())` | read, select | worthy | cheap | 6 | yes |
| `t.mutate(r=ibis.random())` | read, select | cheap | cheap | 6 | yes |
| `t.filter(t.k.isin(u.k))` | two reads, filter, select | cheap | cheap | 3 | yes |

An `unnest` written inside a select is an ordinary column selection to a list
of relation operations. It multiplied the rows and duplicated `__row_order`,
which breaks the "no ties" that D5 depends on. A window function inside a
computed column is an ordinary selection too, and its values depend on the
order the rows arrive in (D10).

So the test has three parts, each decided on the live expression by the class
of the operation:

- every relation operation is on the list above;
- the plan reads exactly one file;
- no value operation multiplies rows (`Unnest`), depends on row order
  (`WindowFunction`, which also covers `row_number`, `lag` and a total used
  inside a computed column), or is not pure (`Impure`, which is `random()` and
  `uuid()`; `now()` and `today()` by name, since xorq's ibis classes them as
  constants; and any UDF, matched as it is today).

The verdict is computed once, when the entry is built, and recorded in the
manifest. `result_cache.cache_worthy()` and the gate on primary-key
inheritance (`primary_key.py:204`) read the manifest and stop classifying.
`classify_build` and its regex over `expr.yaml` are retired, which closes #12.
A regex cannot hold an allow-list: `op:` in that file also matches `DataType`,
`FrozenDict`, `float` and `tuple`, which are not operations, and `Field` and
`Literal`, which are not relations. There is then one implementation, and
nothing to keep in lockstep.

A new or unknown operation now costs a copy (safe) instead of unstable paging
(unsafe). So does a second file, and so does `random()`.

The three parts are what keeping two kinds requires, as proposed in the second
review, and Paddy has not confirmed the list in so many words. The "not pure"
part reaches into #185 (non-pure recipes). It makes a recipe that calls
`random()` worthy, so decision D6 of `plans/ADR-009-digest-stability.md`
(create runs the query twice) checks it and pins its file, and the cheap half
of #185 has nothing left to decide. If that is unwanted, striking `Impure` and
the two names restores today's behaviour, where such an entry re-runs on every
read.

Supporting measurement from the first draft: a union of two files returned 2
different pages for 8 identical requests even on a single-partition
connection, because `UnionExec` emits one partition per input and an exchange
operator merges them.

### D5. Every page request orders by `__row_order`

- No user sort: `ORDER BY __row_order`.
- User sort: the user's keys, then `__row_order` ascending as the last key,
  which breaks every tie.

That is the whole rule, for every entry and both processes. It is the
page-request half of Paddy's rule in D10. Two faster paths
exist and are deliberately not part of this decision (Paddy, 2026-09-20: a
cohesive system that works reliably comes first, and speed problems are handled
as they come up):

- **Declared file order.** Telling the engine the file is already sorted by
  `__row_order` removes the sort from the plan.
- **Range request.** For the unfiltered, unsorted view of a materialized file a
  row's position equals its `__row_order`, so a page can be fetched as
  `__row_order >= offset AND __row_order < offset + limit` instead of `OFFSET`.

Both are measured below so the numbers are on hand when they are wanted.

A third was asked about in the second review: leaving the last key off when
the user's sort key is already unique. Parquet's statistics cannot show that.
pyarrow writes a minimum, a maximum and a null count for each column chunk and
no distinct count, so a unique column and one holding a value twice have the
same statistics (`scripts/spike_sort_grafting.py`), and a distinct count would
be per row group in any case. Tallyman's writer could record the fact itself,
since rows reach it sorted and ties on the author's keys are adjacent. It
would save little: when the leading key is unique the comparison never reaches
`__row_order`, and when it is not, the key is needed. It could apply to page
requests only, because the keys D10 adds at build time are part of the hashed
graph, and it would have to be repeated inside Buckaroo. Not planned.

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
- Only the exact name is special, and ibis's collision name for it, which the
  next item covers. `__row_order_v1`, or any other name an author picks for a
  copy, is ordinary data and survives materialization.
- A join of two entries leaves the right side's copy behind under ibis's
  collision name, `__row_order_right`. The writer drops that one column, as
  well as replacing `__row_order`. An earlier draft kept it as ordinary data
  and said that a three-way join "silently keeps only the first two". Both
  were wrong (`scripts/spike_row_order_joins.py`). A three-way join written in
  one recipe shows the expected columns and passes `build_expr`, then raises
  `IntegrityError: Name collisions: {'__row_order_right'}` from the canonical
  sort, which is the next build step. A join entry that kept the column fails
  the same way as soon as it is joined to a third entry, and with the column
  dropped from its file that second join builds. For a three-way join in one
  recipe the author has to drop `__row_order` from the right-hand inputs, and
  the build turns ibis's message into that instruction, since the author never
  wrote the name it complains about. An author who wants the right-hand
  positions keeps them under a name of their own, as D3 describes. Proposed in
  the second review as part of what keeping two kinds requires, and not yet
  confirmed by Paddy in so many words.
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
| A `result_digest` on the root, so a re-parse that produced different rows would be caught | A digest recorded for the ordered copy itself, which a re-created copy is checked against (ADR-007 decision D13, which files are cache). Cheap entries still record no digest of their own (ADR-006 decision D9, "no cheap-entry digests"). Not yet confirmed; see open question 5. |

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

### D10. The natural order is imposed on every `order_by`

Paddy's rule, 2026-09-20: "every query should have a unique sortby clause, if
the base query didn't have one, it needs to be grafted onto the query so that
everything else is order deterministic", and then: "when the user/mcp supplied
sort isn't deterministic, impose the natural order into each order by so the
resulting query becomes deterministic."

The reading that was put to him the same day, which he did not overrule:

- **Always.** Tallyman cannot know whether a supplied sort is unique without
  scanning the table (D5 records why parquet statistics do not help), and a
  last key added to a sort that is already unique changes nothing.
- **The natural order, then the remaining sortable columns.** The natural
  order alone is unique only while rows come one for one from a single file
  tallyman wrote. Above a join or a union it has ties, above an outer join it
  has nulls, a value-level `unnest` duplicates it (D4), and after an aggregate
  it is gone. With the remaining columns after it the sort is total up to rows
  that are identical in every sortable column, and those write the same bytes
  in either order. This is the tie-break `_tie_break_order` already builds.
- **At every sort in the recipe, and on every page request.** Page requests
  are D5. The build-time half is new: `_canonical_sorted` extends an author's
  sort only when it is the top node of the expression
  (`source_cache.py:113`), and leaves every other `Sort` node as written.

Why this has to reach every sort: a sort that feeds a `limit` decides which
rows the entry holds, and the top of the expression is too late to break its
ties. `order_by(g).limit(1000)` over 3,000,000 rows, where about 15,000
rows tie on the smallest `g`, five runs each
(`scripts/spike_sort_grafting.py`):

| Connection | Sort key | Distinct sets of rows in 5 runs | Equals the rows `(g, id)` picks |
| --- | --- | --- | --- |
| default | `g` | 5 | no |
| `target_partitions = 1` | `g` | 1 | yes |
| default | `g`, then the row position | 1 | yes |

The middle row is how this case works today. It is repeatable because
materialization runs single-partition (decision D1 of
`plans/ADR-009-digest-stability.md`), which is the engine's behaviour and the
kind of dependence D5 rejects for page requests. The last row is repeatable by
the query's own meaning, on any connection. A cheap entry cannot contain a
sort, because a sort makes an entry worthy (D4), so the build-time half only
ever runs at materialization.

The added keys are part of the build's graph, so the content hash covers them,
and this rides the corpus rebuild of ADR-007 decision D9 ("one change, one
rebuild").

Left to #146 (a lint for row-ordering nondeterminism): the `order_by` inside a
window function, and ordered aggregates such as `first`, `last` and `collect`.
They are the same rule. `cumsum()` with no order gave 5 distinct sets of
values in 5 runs on the default connection and 1 on a single-partition one.
Ordered by the tied key `g`, and by `g` then the row position, it gave 1 on
both. An entry with a window function is always materialized, so it is
repeatable today, by the engine's behaviour again. Only `cumsum()` was
measured.

*Rejected:* add the keys only when the supplied sort is not unique. That needs
a scan of the table for every sort, to save a key that costs almost nothing
when the leading key is unique.

### D11. A sort that is not the recipe's last step is hoisted, or the build fails

`_canonical_sorted` recognizes an author's sort only when it is the top node.
When another step follows, the check fails and the whole expression is wrapped
in a sort that leads with the inherited row order. That column is unique, so
the author's sort has no effect on what is written. The parent's rows are in
the order 40, 10, 60, 20, 50, 30 and the author asks for `amount` descending
(`scripts/spike_sort_grafting.py`):

| Recipe | Classed | Written as |
| --- | --- | --- |
| `order_by` last | worthy | 60, 50, 40, 30, 20, 10 |
| `order_by`, then `mutate` | worthy | 40, 10, 60, 20, 50, 30 |
| `order_by`, then `select` | worthy | 40, 10, 60, 20, 50, 30 |
| `order_by`, then `filter(amount > 15)` | worthy | 40, 60, 20, 50, 30 |
| `order_by`, then `limit(3)` | worthy | 40, 60, 50 |

Each of these is classed worthy because of that sort, so the author pays for a
full copy that ignores it, and a top-three entry is not shown in rank order.
Under Paddy's rule (D10) the author did supply a sort, so the system keeps it:

- From the top of the expression, walk down through steps that keep the order
  of rows (a selection, a computed column, a filter, a limit, a column drop, a
  rename, a cast, a drop of null rows, a fill of nulls) to the nearest sort.
- When each of that sort's keys is still an output column, unchanged (a rename
  is followed), the top-level sort leads with those keys, then the tie-break
  of D10.
- When a key did not survive, because it was dropped, overwritten, or was an
  expression and not a column, the build fails. The error names the key and
  tells the author to keep the column or to sort as the last step. This is the
  choice D3 makes for a dropped `__row_order`: report what the author can fix
  in one line, and do not repair it silently.

Paddy, 2026-09-20: "I like your suggestion." The change is about 40 lines in
`_canonical_sorted`.

*Rejected:* make any `order_by` that is not the last step a build error. It is
one rule, but a top-N recipe would then have to be written
`order_by(...).limit(n).order_by(...)`.

### D12. A raw parquet read is a build error

D2 says every file tallyman reads carries `__row_order`, and that both kinds
of source go through source identity first. Neither is enforced for parquet.
A recipe can call `xo.deferred_read_parquet(abs_path)`, and tallyman's own
hints recommend it (`build.py:199`, `source_cache.py:133`, and the namespace
note in a tool description, `src/tallyman_mcp/server.py:275`). Such a read has no digest, no clone and
no `manifest.sources` record, which is the defect of #168 for a parquet file.
It also has no ordered copy, so a root entry built on it has no `__row_order`
to page by. Only the CSV form is banned today (`_csv_direct_read_check`).

A read of a parquet file that tallyman did not write becomes a build error
that points the author to `read_project_file`, next to the CSV check, and the
three hints change with it. Tallyman's files are the snapshots and ordered
copies under the project's `compute_cache/` (decision D13 of
`plans/ADR-007-tallyman-owned-materialization.md`, which files are cache), so
the check is on the path of each `Read`. Nine files under `tests/` call
`deferred_read_parquet` today, 23 calls in all, so the change carries test
churn.

Proposed in the second review as part of what keeping two kinds requires, and
not yet confirmed by Paddy in so many words.

*Rejected:* rewrite a raw read into an ingest at build time. It is the surgery
inside an author's expression that D3 turned down, and the recipe text would
name one file while the entry read another.

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
- **The cheap test looks inside** (D4). A value-level `unnest`, a window
  function in a computed column, `random()` and a filter against a second file
  are each classed worthy, a drop of null rows is classed cheap, and the
  verdict is read from the manifest with no `expr.yaml` parsed.
- **Joins** (D6). A join entry's file has no `__row_order_right`, joining it to
  a third entry builds, and a three-way join in one recipe fails with a
  message that says to drop `__row_order` from the right-hand inputs.
- **Every sort is total** (D10). An `order_by(g).limit(k)` recipe over a file
  above the split threshold, with ties on `g` at the cut, holds exactly the
  rows that `(g, __row_order)` picks.
- **A sort that is not last is kept** (D11). A top-three recipe is written in
  rank order, `order_by` then `mutate` is written in the sorted order, and a
  recipe that drops its sort key in a later select fails to build with a
  message that names the key.
- **Raw reads** (D12). A recipe that calls `xo.deferred_read_parquet` on a
  source file fails to build with a message that names `read_project_file`.

## Consequences

- Pages are repeatable for unsorted and sorted requests, in tallyman and in
  Buckaroo, with no engine settings and no second connection.
- Every table shows one more column, at the end. A join result does not carry
  `__row_order_right`, because the writer drops it (D6).
- A recipe whose select list forgets `__row_order` fails to build until the
  author adds it. So does a three-way join that keeps the column on its
  right-hand inputs (D6), a recipe that drops the key of a sort it made
  earlier (D11), and a recipe that reads a parquet file directly (D12).
- Every sort in a recipe gains keys its author did not write (D10), and a sort
  that is not the last step now decides the order that is written (D11).
- Whether an entry is cheap is decided once, at build, and read from the
  manifest afterwards (D4). More recipes are worthy than before: one that
  unnests inside a select, reads a second file, or calls `random()`.
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

## Implementation notes

- **Where the code is.** `src/tallyman_xorq/row_order.py` (the reserved name, the tie-break, hoisting, the assignment
  and join checks, and `page`, the one helper that turns an entry and a sort into a page),
  `src/tallyman_xorq/worthiness.py` (`classify_expr`, the D4 test) and `src/tallyman_xorq/ordered_copy.py` (D2).
  `source_cache.rewrite_for_build` is the one place the checks run.
- **D6, three-way joins.** The second review found that ibis raised `IntegrityError` from the canonical sort. The
  canonical sort is now built without going through that ibis path, and the same recipe then builds and loses the third
  entry's `__row_order` silently. So the build checks for it explicitly: a join chain whose first side and two or more
  right-hand sides carry the column is a build error that shows the `.drop("__row_order")` fix. `translate_collision`
  still turns the raw ibis message into the same instruction if it appears at execution.
- **D6, rename.** `t.rename(__row_order="x")` cannot assign to the column: ibis resolves a rename onto an existing name in
  favour of the existing column, so the graph holds no assignment to detect. `select` and `mutate` assign, and are
  build errors.
- **D11, hoisting.** Only the keys the author wrote count. The tie-break appended to a sort (`__row_order` and the
  other columns) is added again at the top, so a later select may drop those columns.
- **D5.** `/api/data` pages with `row_order.page`, which orders by the user's keys and then `__row_order`; the route takes no
  user sort yet. Buckaroo's half is unchanged and waits on buckaroo-data/buckaroo#974 (tallyman sends
  `row_order_column`, which 0.15.6 ignores).
- **Open question 1 and 2** were implemented as the uniform rule: every parquet source gets an ordered copy, and the
  column is `__row_order`. **Open question 5** is answered by ADR-007 D13: the manifest's `ordered_copies` records the
  copy's content digest, and a re-created copy is checked against it (a mismatch is loud and served).
- **A CSV that already has a `__row_order` column** has it overwritten, like a parquet source; the previous check that
  the column was a canonical `0..N-1` sequence is gone, and `original_row_order` is ordinary data.


## Open questions

1. **Ordered copies of parquet sources.** D2 adds a copy per parquet source.
   Adopted as the uniform rule under Paddy's "cohesive first" priority, and not
   yet confirmed by him in so many words. With two kinds of entry kept, a
   cheap root pages by this column, so the copy is needed. It assumed polars
   numbers a parquet scan's rows in file order, as ADR-004 measured for CSV.
   Checked in the second review (`scripts/spike_ordered_copy_layout.py`, polars
   1.40.1): `scan_parquet().with_row_index()` numbered a source of 184 row
   groups in file order on 1, 3 and 14 threads.
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
   collected, and is not packed (ADR-007 open question 3). The second review
   made re-creation a designed path and not a gap: ADR-007 decision D13 (which
   files are cache) puts the copies under the project's `compute_cache/`, has
   `ensure_materialized` make a missing one again from the clone, and records
   the digest the new copy is checked against. That answer follows from
   D13's rule and is not yet confirmed by Paddy in so many words.
