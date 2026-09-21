# ADR: An immutable result store, every entry materialized, one owning process

- **Status:** Proposed (2026-09-21). Decided in outline by Paddy the same day, after the review of
  buckaroo-data/tallyman#189 produced #193 to #211 ("Go with A"). Not implemented.
- **Amends:** `plans/ADR-007-tallyman-owned-materialization.md`, `plans/ADR-008-row-order-of-reads.md` and
  `plans/ADR-009-digest-stability.md`. Those three are on the branches of #184 (text) and #189 (implementation), not
  yet on `main`; the paths here resolve once #189 merges. The table under "What happens to ADR-007, 008 and 009" says
  which of their decisions stand, which change and which are retired.
- **Reading decision labels:** a bare label such as "D4" means this ADR's own decision. Another ADR's decision is
  written with its number and a few words saying what it decides.
- **Tickets this closes or narrows:** listed per decision, and summarised under "Issues".
- **Affected code:** `src/tallyman_xorq/materialize.py`, `ordered_copy.py` (removed), `row_order.py`, `worthiness.py`,
  `digest.py`, `result_cache.py`, `build.py`, `io.py`, `source_identity.py`, `staleness.py`, `recalc.py`;
  `src/tallyman_core/catalog_state.py` (`reset_to`, `project_lock`, the bullpen), `paths.py`, `catalog.py`;
  `src/tallyman_companion/app.py` and `buckaroo_lifecycle.py`; `src/tallyman_mcp/server.py`; `src/tallyman_cli/main.py`.

## Terms

- **Entry:** one named recipe (an xorq expression written by the agent) at one content hash.
- **Content hash:** the hash of an entry's build, which covers its recipe and the paths of the files it reads.
- **Cheap / worthy:** the classifier's verdict on an entry. Cheap means a row-preserving view over one file (ADR-008
  D4, the allow-list of row-preserving operations); worthy means everything else. Until now cheap entries got no file
  of their own.
- **Store:** the untracked directory of files tallyman writes and reads data from, introduced by D5.
- **Store object:** one file in the store. There are three kinds: a source clone, a CSV intermediate, and a result.
- **Source clone:** a copy of a raw source file, named by the digest of its content (ADR-002, source identity).
- **CSV intermediate:** the parquet file `tallyman_read_csv` writes from a CSV (ADR-005), named by the CSV's digest
  and the reader options.
- **Result:** the parquet file holding an entry's rows, named by the entry's content hash.
- **Ordered copy:** a polars-written copy of a source with `__row_order` added (ADR-008 D2). Removed by D4.
- **Heal:** re-creating a missing result by running its entry's build again.
- **Owner:** the one process allowed to write a project home (D10).
- **Multiset:** a collection of rows where order does not matter and duplicates count.

## Problem

The review of #189 filed nineteen issues, #193 to #211. None is an xorq bug. Sorted by what produced them:

| Source | Issues |
| --- | --- |
| Row order carried as data through recipes (`__row_order`, ordered copies, the canonical sort) | #197, #199, #200, #205, #206, #207 |
| Result files with a lifecycle: reset moves them, a failed build deletes them, a heal re-verifies them, a pin protects them | #193, #194, #195, #196, #203, #208, #209, #211 |
| Two kinds of entry | #204, #208 |
| Buckaroo executing expressions rather than reading files | #201, #202, #210 |
| Ingest round trips | #198 |

The older tracker has the same shape: lifecycle (#14, #15, #35, #76, #77, #80, #96, #97), order and determinism (#83,
#88, #125, #146, #147, #171, #187), what a hash names (#115, #162, #163, #166, #168), and two processes sharing state
(#118, #156, #183, #186, #190).

Four decisions produce most of this:

1. Reproducibility was defined as the same rows in the same order, verified by digest. ibis relations are unordered
   and DataFusion runs many partitions, so every operator that touches order needs its own rule, and ADR-008 grew to
   twelve decisions while leaving #205 and #206 open.
2. A cheap entry has no file, so its order and identity must be carried through the recipe. The second review of #184
   found this is the only reason `__row_order` has to be visible to recipes.
3. Data files have several owners: snapshots, ordered copies, CAS clones, the bullpen (the directory reset parks files
   in), the git-tracked `compute_cache.jsonl`, and a pin kept in `errors.jsonl`. Reset, heal, failed builds and CAS GC
   each move or delete them.
4. Two processes (the MCP server and the companion) both write, sharing state through a file lock, and Buckaroo is a
   third executor.

## Decisions

### D1. Every entry is materialized

Every entry gets a result at create time, whatever the classifier says. The verdict is still computed and recorded in
the manifest as `cache_worthy`, so a future cost rubric (#30) has data, but nothing reads it to decide how to serve
an entry. The lazy-view path for cheap entries is deleted, together with its tests.

A child reads its parent's result with a bare parquet read (ADR-007 D3, chaining is a bare read), for every parent.

Cost: a root entry that only reads a source now writes a second copy of it. That is accepted under the "cohesion
before speed" rule. A special case for pure reads can come back if the disk use is measured to be a problem.

Closes #204 (`cache_worthy` falls back to "a snapshot exists"). Removes the cheap-children half of #208.

### D2. Reproducible means the same multiset of rows

A rebuild of an entry is faithful when it produces the same multiset of rows, with float columns compared within a
tolerance. Row order is part of the result only where the author's last step is a sort, and even then ties may come
out in any order.

What follows:

- Paging is repeatable within one result file (D4), not across rebuilds. After a rebuild, row 5 may be a different
  row. ADR-008 D1's contract becomes: a page is a function of `(result file, sort, offset, limit)`.
- `result_digest` is order-insensitive: a multiset hash of per-row hashes, where each row is hashed with ADR-009 D2's
  per-column normalisation (null slots zeroed, logical types in the seed). Floats are hashed exactly, so a digest
  can differ where the rows are equal within tolerance. The digest is a fast equality check. When two digests
  differ, the comparison that decides is D3's.
- Materialization runs with the engine's default partitioning. ADR-009 D1 (single-partition execution) is retired,
  since its only purpose was stable order and stable float totals.

Closes #187 (a float total depends on the parent's row-group layout) and #200 (the diff compares positions).

### D3. Create runs the query twice; the comparison is by multiset

ADR-009 D6 (create runs the query twice and compares) stands. The comparison becomes: equal digests, or, when the
digests differ, both results sorted by every sortable column and compared column by column, floats with
`rel_tol=1e-9`. An entry that fails the comparison is recorded `reproducible: false` in its manifest and keeps its
first result.

Every Sort node gets its remaining sortable columns appended as tie-breaks. This is ADR-008 D10's intent (Paddy's
rule, "every query should have a unique sortby clause") without `__row_order`. It matters for content only where a
sort feeds a `limit` or a window, since `order_by(g).limit(1000)` picks different rows when `g` has ties. Rows tied
on every sortable column differ only in nested or unsortable columns. When that happens, the run-twice comparison
catches it and the entry is marked not reproducible. That is how #205 is handled; it is not fixed at the sort.

ADR-008 D11 (a non-final sort is hoisted or the build fails) is retired: an inner sort's order matters only through a
`limit` or window, and the tie-break covers that.

### D4. `__row_order` is written by the writer and hidden from recipes

- The result writer (ADR-007 D4, the one writer) stamps `__row_order` as `0..N-1` in stream order, as the last
  column. It is the only thing that writes it.
- Every read of a result that a recipe sees (a parent read at chaining, D1) drops `__row_order`. A recipe cannot
  select, join on, collide with or carry it.
- A recipe whose output has a column named exactly `__row_order` is a build error, so the writer never overwrites
  author data. Other names such as `__row_order_v1` are ordinary columns.
- Page requests order by `__row_order` with no user sort and use it as the last key of a user sort (ADR-008 D5,
  unchanged). Buckaroo shows the column, as Paddy asked.
- Ordered copies are removed. A source is read from its clone or CSV intermediate, and the root entry's result gets
  its `__row_order` from the writer. `ordered_copy.py` and `manifest.ordered_copies` go.

Closes #197 (ordered copies change parquet types), #199 (`assert_joinable` refuses semi and anti joins), #206 (the
writer drops `__row_order_right`), #207 (ordered copies are never listed or removed) and #211 (the ordered copy's
digest sidecar). Removes most of `row_order.py`: `canonical_sorted`'s natural-order graft, hoisting,
`require_on_cheap`, `assert_joinable` and `translate_collision`.

### D5. An immutable, content-addressed store

All data files tallyman writes live in one untracked store under the project home. Each store object is named by what
determines its content:

| Kind | Name | Re-created from |
| --- | --- | --- |
| Source clone | source content digest | the live source, when its digest still matches |
| CSV intermediate | CSV digest + reader options | the clone and the reader options recorded in the manifest |
| Result | content hash | the entry's build, reading its parents' results |

Entry build directories (`entries/<hash>/`, untracked, holding the manifest and expanded build) are keyed by content
hash and follow the same rules.

Rules:

- An object is written once: to a temporary name in the same directory, then published with `os.replace`. A failed
  write leaves only its temporary file, which the next start removes.
- An object is never overwritten. If the name exists, the writer returns it. A re-add of a recipe that already has a
  result reuses the result, which for a non-reproducible entry means its first result stands.
- A manifest is written once, at create (today's only caller is `build.py:648`). Anything that changes after create
  lives in git-tracked files.
- Nothing deletes an object except `tallyman gc` (D8).

Closes #193 (a failed build deletes the snapshot already on disk) and #209's class of copied-path problems for results
(a result's name does not depend on the project path, which also closes #77).

### D6. Reset moves pointers and nothing else

`reset_to` runs `git reset --hard` on the catalog repo and reloads the alias pointers. It does not touch the store.
The bullpen, `compute_cache.jsonl`, `prune_compute_cache`, the copy-back from the bullpen and the `gc_cas` call on
reset are removed. A reset forward finds every object where it was.

Closes #194 (reset restores an older manifest over a newer snapshot) and #195 (reset back unpins a non-reproducible
snapshot), and #22 (the checkpoint's cost grows with the cache). Replaces ADR-007 D14.

### D7. A missing object is re-created; a different result makes descendants stale

`ensure_materialized` (ADR-007 D5, the one entry point that makes files exist) keeps its role, with one re-create
rule per object kind (the table in D5). A source whose live file no longer matches its digest cannot be re-created,
and the error names the source and the entries that need it.

When a re-created result's digest differs from the manifest's `result_digest` and D3's comparison also fails:

- the new result is kept (the old one is gone, so there is nothing to choose between);
- an event is written to the project log, naming the entry, both digests and the engine versions (ADR-009 D4's
  content, not its pin);
- every descendant is marked stale, because its result was computed from rows that no longer exist. The existing
  staleness scan and recalc then rebuild them.

No pin, no badge in `errors.jsonl`, and no verification on the read path. A heal runs as an owner job (D10), never
under a page request, and any Buckaroo reload happens after the job finishes.

Closes #196 (the pin lives in `errors.jsonl`), #203 (heal checks run under the project lock) and the parent half of
#208 (an unfaithful heal changes cheap children's rows under their hashes). Retires ADR-006 D10 and D12 (unfaithful
entries wipe Buckaroo state and are pinned).

### D8. Garbage collection is explicit

`tallyman gc [--apply]` lists, and with `--apply` deletes, the store objects not reachable from its roots. Roots:

- every entry an alias points at in the current catalog step and the last N steps (N from the project config,
  default 20), and their ancestors;
- every result of an entry recorded `reproducible: false`, and every source clone whose live source has changed,
  regardless of reachability, because neither can be re-created. These go only when named explicitly.

Without `--apply` it prints each object's kind, size and why it would go. The Cache page shows the store's size by
kind. Nothing runs GC automatically.

Closes #207's "no way to see it" for every kind, and #35 (manifest-less leftovers). Covers the storage part of #185
(where a non-reproducible result lives): it lives in the store and GC leaves it alone.

### D9. Buckaroo is handed result files only

Every grid, chart and diff operand is given to Buckaroo as the path of a result file. Buckaroo never receives an xorq
expression or a build. Its session is keyed by `(project, content hash)`, and the owner (D10) runs at most one load per
key at a time.

Closes #210 (reads no longer load builds, so there is no LRU of them), #172 (sessions keyed by hash across projects),
#177 (a klass reload no longer needs to wipe stat caches keyed on a file that did not change). Narrows #202 (the
single-flight load removes the double load on concurrent opens) and #201 (the klass reload becomes an owner job,
off the event loop). Diffs stay with #188, whose operands are now two files.

### D10. One process owns the project home

The companion (`tallyman run`) is the only process that writes to `TALLYMAN_HOME`.

- **The lock.** At start the companion takes an exclusive, non-blocking `flock` on `$TALLYMAN_HOME/server.lock` and
  holds it until it exits. It writes its pid, port, start time, tallyman version and Buckaroo's pid into the file.
  The kernel releases the lock when the process dies, so a crash leaves no stale lock, and the file's contents are
  only information. The lock is per home and not per project, because the companion switches projects.
- **A second start fails**, naming the owner: `tallyman is already running: pid 4812 on :7860 since 14:02 (log: …)`.
- **Buckaroo.** Before starting Buckaroo, the owner checks the Buckaroo port for a listener (`lsof -ti
  TCP:<port> -sTCP:LISTEN`). A listener it did not start is an error that names that pid.
- **The MCP server is a client.** At start it tries the lock without blocking. If it gets it, no owner is running:
  it releases it and fails every tool call with "start `tallyman run`". If the lock is held, it reads the port and
  calls `/health`, which returns the owner's pid; a mismatch (another process on the port) is an error.
- **All writes are owner endpoints:** create, revise, recalc, promote, reset, customizations, gc. Work that can take
  more than a moment is a job: the endpoint returns a job id, the MCP tool waits up to its timeout and returns the
  result or the job id, and a second tool polls.
- **The CLI's writing commands** (`reset-to`, `gc`) call the owner when one is running, and when none is, take the
  lock themselves for the length of the command.
- **Inside the owner**, writes for a project go through one worker queue off the event loop. Reads never wait on it,
  because they only open immutable store objects. The cross-process `project_lock` becomes an in-process lock.
- `/health` and the app's status bar show the owner's pid and start time.

Closes #183 (two servers on one project go undetected), #186 (a page request waits behind a build in the other
process) and #190 (a build on the event loop freezes the UI). Replaces ADR-007 D11.

### D11. One change, one rebuild

D1, D2 and D4 change content hashes and digests of every entry. They land together behind one corpus rebuild, as
ADR-007 D9 did. No migration code: the project rule is to rebuild.

## What happens to ADR-007, 008 and 009

| Decision | Fate |
| --- | --- |
| ADR-007 D1, builds carry no cache nodes | Stands |
| ADR-007 D2, a snapshot's location is a function of the content hash | Stands, inside the store (D5) |
| ADR-007 D3, chaining is a bare read of a worthy parent | Every parent (D1), dropping `__row_order` (D4) |
| ADR-007 D4, one writer | Stands; it stamps `__row_order` and runs multi-partition (D2) |
| ADR-007 D5, `ensure_materialized` | Stands with D5's re-create table and D7's stale marking |
| ADR-007 D6, Buckaroo is handed something that exists | Narrowed to result files (D9) |
| ADR-007 D7, the cold state is an empty cache | Stands: an empty store |
| ADR-007 D8, a sentinel keeps xorq's cache out | Stands |
| ADR-007 D9, one change, one rebuild | Replaced by D11 |
| ADR-007 D11, one write at a time per project | Replaced by D10 |
| ADR-007 D12, files are deleted only by explicit action | Stands, made concrete by D8 |
| ADR-007 D13, a file is cache only if it can be re-created | Stands; D5's table is the list |
| ADR-007 D14, a reset leaves `compute_cache/` alone | Replaced by D6 |
| ADR-008 D1, a page is a function of `(content_hash, …)` | Restated per result file (D2) |
| ADR-008 D2, every file carries `__row_order` | Results only, from the writer; ordered copies removed (D4) |
| ADR-008 D3, a cheap entry that drops `__row_order` is a build error | Retired |
| ADR-008 D4, cheap means row-preserving | Kept as a recorded verdict only (D1) |
| ADR-008 D5, every page orders by `__row_order` | Stands |
| ADR-008 D6, the name is reserved | Narrowed: an output column named `__row_order` is a build error (D4) |
| ADR-008 D7, `tallyman_read_csv` loses its trailing `order_by` | Stands |
| ADR-008 D8, Buckaroo's hint (buckaroo#974) | Stands |
| ADR-008 D9, correct the threshold | Stands (done) |
| ADR-008 D10, natural order on every `order_by` | Tie-break by the node's sortable columns (D3) |
| ADR-008 D11, hoist a non-final sort | Retired |
| ADR-008 D12, a raw parquet read is a build error | Stands, for source identity |
| ADR-009 D1, single-partition materialization | Retired (D2) |
| ADR-009 D2, content digest of the file as read back | Order-insensitive (D2) |
| ADR-009 D3, the snapshot format | Stands |
| ADR-009 D4, a mismatch names its cause | Stands as a log event, no pin (D7) |
| ADR-009 D5, it lands with the rebuild | Replaced by D11 |
| ADR-009 D6, create runs twice | Stands, multiset comparison (D3) |

## Issues

- **Closed by this ADR:** #22, #35, #77, #172, #177, #183, #186, #187, #190, #193, #194, #195, #196, #197, #199, #200,
  #203, #204, #206, #207, #208, #210, #211.
- **Narrowed:** #185 (storage covered by D8; propagation of the verdict to children still open), #201, #202, #205.
- **Unaffected, still to fix on their own:** #198 (CSV reader options through JSON), #191 (staleness of a CSV outside
  `data/`), #168 (CSV source identity), #209 for the expanded build directory's recorded project root.

## Testing

Failing tests first, in one commit, per the project's TDD rule:

- A build that fails after a result exists for its hash leaves that result byte-identical (D5).
- Reset back and forward leaves every store object in place and a non-reproducible entry's result unchanged (D6).
- A recipe cannot see `__row_order` from a parent; a three-way join and a semi-join chain over entries build (D4).
- A result's `__row_order` is `0..N-1`, last, and a page with no sort returns rows in that order (D4).
- `result_digest` is equal for the same rows written in two orders (D2).
- A float SUM computed at 1 and 14 partitions passes the run-twice comparison (D3).
- `order_by(g).limit(k)` with ties on `g` gives the same row set in five runs (D3).
- Deleting a parent's result and changing what it rebuilds to marks its children stale (D7).
- `tallyman gc` without `--apply` deletes nothing; with it, a non-reproducible result survives (D8).
- A second `tallyman run` on the same home exits non-zero, naming the first one's pid (D10).
- The MCP server with no owner running fails each tool call with the start instruction (D10).
- A page request completes while a build is running (D10).
- The ADR-007 D8 sentinel test stays green.

## Consequences

- Disk use grows: every entry has a result, including a root entry that only reads a source.
- Create gets slower: every entry is written, and run twice (ADR-009 D6 already made that true for worthy entries).
- A rebuild can change the order of an unsorted entry's rows. Anyone comparing two versions must compare by key or
  as a multiset, which the diff already should.
- The MCP server stops working when the companion is not running. That is the point of D10, and the error says what
  to do.
- `row_order.py`, `ordered_copy.py`, the bullpen and the cross-process lock shrink or go. The code that remains has
  one kind of entry, one kind of file per job, and one writer process.

## Open questions

1. **The row hash.** D2 needs a per-row hash that is stable across library versions. `polars.DataFrame.hash_rows`
   is documented as not stable across versions. The likely answer is xxhash-64 or SHA-256 over digest.py's
   normalised column values, row by row. Needs a measurement at 18M rows.
2. **Buckaroo's paging over a file.** D9 assumes Buckaroo has a load route that takes a parquet path and pages it in
   file order when no sort is set. That route and its order must be checked in buckaroo 0.15.6 before D9 is
   implemented; if it does not page in file order, buckaroo#974 is still needed.
3. **The store's location and name.** `compute_cache/` could become `store/`. A rename is free with the rebuild.
4. **Whether the MCP server should start the owner** when none is running, instead of failing. Starting it ties the
   companion's lifetime to a Claude Code session, which is why D10 fails instead.
5. **N in D8.** Whether 20 steps is the right default for GC roots, or whether the roots should be every step in the
   catalog's history until disk use says otherwise.
