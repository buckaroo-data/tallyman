# ADR: Tallyman owns result materialization (no xorq cache nodes in builds)

- **Status:** Implemented in buckaroo-data/tallyman#189 (2026-09-21), at Paddy's request to implement the
  set. Where this text says a decision is "not yet confirmed" or "Proposed", it was implemented as
  written; the differences between the text and the code are under "Implementation notes" below.
  Original status: Proposed (2026-09-18, revised 2026-09-20 in the grilling session, which added the governing rule, decisions D10 to D12, and the resolution recorded under D5, and again the same day after a review of PR #184: D10 moved out to #188, the verify sweep left D5's callers, D6's session-ending clause was dropped, and D12's rule was restated, and a third time that day after a second review of PR #184: two kinds of entry were confirmed (open question 1), D5 lost its accepted gap, D6 gained the klass reload, and D13 and D14 are new). Awaiting Paddy's review; nothing here is implemented. Supersedes two decisions of `plans/ADR-006-read-path-loads-builds.md`: its D4 (chaining inlines the parent's cache node) and its D8 (the manifest records the snapshot key and reads assert it). Five other ADR-006 decisions keep their intent: D5 (the canonical sort), D6 (a missing build is a hard error), D7 (verification runs in production and is loud), D10 (an unfaithful heal wipes the entry's Buckaroo state) and D12 (unfaithful entries are pinned and badged). The last three attach to `ensure_materialized`, which is this ADR's D5.
- **Reading decision labels:** a bare label such as "D5" in this document
  always means this ADR's own decision. Another ADR's decision is always
  written with its ADR number and a few words saying what it decides.
- **Context:** the 2026-09-18 cache audit (tallyman @ `a748ea6`, buckaroo
  0.15.4, xorq 0.3.26). One finding is filed upstream of tallyman:
  buckaroo-data/buckaroo#972 (`/load_expr` has no `cache_dir`). The direction
  was set by Paddy the same day: "I want to depend on xorq as little as
  possible for caching."
- **Tickets:** #188 (diffs, moved out of this ADR), #186 (the waiting that
  D11's lock causes), #185 (non-pure recipes), #183 (two servers on one
  project), #168 (CSV source identity, which the shared rebuild of D9 needs),
  #118 (concurrent reads on the shared backend, which D11 does not cover),
  #77 (an empty grid on a clone of the project at another path, which D2
  closes), #76 (closed: how bare-read chaining failed the last time it was
  tried, which D5 answers), #22 (the checkpoint's cost grows with the cache,
  which D14 removes).
- **Affected code:** `src/tallyman_xorq/source_cache.py` (`rewrite_for_build`),
  `src/tallyman_xorq/result_cache.py` (`_resolve_result_plan`,
  `cached_result_expr`, `entry_graph_expr`, `baked_snapshot_path`,
  `_cached_node_path`, `_assert_recorded_snapshot_key`, `_verify_self_heal`),
  `src/tallyman_xorq/build.py` (the execute-once step, `build.py:496-557`),
  `src/tallyman_xorq/io.py` (`tracked_expr_from_alias`,
  `pinned_expr_from_alias`), `src/tallyman_xorq/portable.py`
  (`rewrite_cache_dirs`), `src/tallyman_core/manifest.py` (`snapshot_key`),
  `src/tallyman_companion/buckaroo_lifecycle.py` (`load_session`,
  `reload_project_sessions`), `src/tallyman_core/catalog_state.py`
  (`reset_to`, `prune_compute_cache`, `restore_from_bullpen`,
  `capture_tallyman_state`, `_gc_cas_clones`),
  `src/tallyman_xorq/source_identity.py` (`gc_cas`, `recon_cas_path`),
  `docs/system-contract.md`.
- **Related ADRs:** `plans/ADR-002-source-identity-content-hash.md` (content in
  the path; D3 reuses the device), `plans/ADR-003-result-cache-cost-rubric.md`
  (its motivating misclassification; see Consequences),
  `plans/ADR-008-row-order-of-reads.md` and
  `plans/ADR-009-digest-stability.md` (the other two hash- or digest-changing
  decisions that share this ADR's corpus rebuild).
- **Evidence:** `scripts/spike_bare_read_chaining.py` (results under D3),
  `scripts/spike_reset_roundtrip.py` (D13 and D14: a reset back and forward,
  played through on today's code), `scripts/spike_stream_order.py` (open
  question 1, the alternative that was not taken), and the audit measurements
  quoted in the Problem section.

## Terms

- **Entry:** one catalog computation, stored under its content hash.
- **Build:** the `xorq_build/` directory inside an entry, which is the entry's
  computation graph written to disk by xorq.
- **Replay a build:** load that directory back into a live expression and
  execute it.
- **Materialize:** run an entry's computation once and write the result to a
  parquet file. That file is the entry's **snapshot**, and it lives under
  `compute_cache/result_cache/`.
- **Worthy entry:** an entry tallyman materializes, because its graph does work
  that is expensive or that cannot inherit a row order (an aggregate, join,
  sort, window function, UDF, union). **Cheap entry:** one it does not, whose
  small plan re-runs on every read.
- **Cache node:** xorq's `CachedNode`, a marker inside a graph meaning "look for
  a file with this key, and if it is missing run the graph below me and write
  it".
- **Chaining:** a recipe building on another entry through
  `tracked_expr_from_alias` or `pinned_expr_from_alias`.
- **Heal:** re-create a snapshot that is missing from disk by re-running the
  entry's build.
- **Session:** one grid's state inside the Buckaroo server process.
- **View build:** a build whose whole graph is one step, "read this parquet
  file".
- **Live diff:** the compare grid shown when two versions are diffed. A
  **promoted diff** is one that has been saved as a catalog entry.
- **Checkpoint:** tallyman's step that zips new entries and makes one git
  commit in the catalog repository.
- **Clone:** the copy of a source file under `data/.cas/`, named by the digest
  of the source's bytes, which a build reads in place of the live file
  (`plans/ADR-002-source-identity-content-hash.md`).
- **Ordered copy:** the parquet copy of a source that carries `__row_order`,
  which a root entry reads (decision D2 of
  `plans/ADR-008-row-order-of-reads.md`).
- **Reset:** `reset_to`, which returns the catalog to an earlier step. The
  **bullpen** is the directory a reset moves retired files into, so that a
  later reset forward can bring them back.
- **Klass:** a summary stat, post-processing or display class written for the
  project, which Buckaroo loads into each grid.

## Problem

Tallyman's result cache is xorq's cache. `rewrite_for_build`
(`source_cache.py:241-243`) wraps every worthy expression in
`.cache(cache=ParquetSnapshotCache(...))`, and that one call decides everything
else: the file's name is xorq's snapshot key, its directory is a base path that
xorq does not serialize, its writer is xorq's `ParquetStorage`, it is written as
a side effect of `loaded.count().execute()` (`build.py:540`,
`result_cache.py:733`), a missing ancestor is regenerated by a nested cache node
noticing its own file is gone, and Buckaroo is handed a build that contains all
of this.

The audit found the following, each a consequence of that arrangement.

1. **Snapshots escape the project.** xorq serializes a cache node's
   `relative_path` and never its base path, and `load_expr(cache_dir=...)`
   redirects only the root cache node.
   - Buckaroo's `/load_expr` calls `load_expr(build_dir)` with no `cache_dir`,
     so the viewer never reads tallyman's snapshot. The first view re-executes
     the whole graph inside Buckaroo and writes a second copy under
     `~/.cache/xorq/result_cache/`. On the dev machine that directory held 68
     files and 14 GB, with the same keys and sizes as the project's
     `compute_cache` (buckaroo#972).
   - `build.py:496` loads the new build with `cache_dir` but without
     `rewrite_cache_dirs`, so nested ancestor nodes resolve to `~/.cache/xorq`
     while a child builds. Every child build re-executes its expensive parent
     and leaves a duplicate there. Reproduced in scratch homes where Buckaroo
     never ran.
2. **The writer is unsafe under concurrency.** `ParquetStorage` writes through
   a fixed `<key>.parquet.tmp`. Four concurrent cold writers of one key gave
   three `FileNotFoundError`s and a corrupt final file, and because a hit is
   decided by file existence the corrupt file is then served forever.
   `_heal_lock` covers threads in one process only, builds take no lock, and
   FastMCP runs sync tools in a threadpool, so parallel tool calls do build
   concurrently.
3. **The file shape is poor.** One row group per DataFusion batch (at most
   8,192 rows), Snappy. A real 3.68 GB snapshot has 9,525 row groups and a
   44.9 MB footer. `cached_result_expr` constructs a fresh read on every call
   (`result_cache.py:742`), so that footer is opened again for every page
   request, and every call registers another table in the shared backend
   (40 reads, 40 tables).
4. **The bytes depend on how the node was executed.** xorq stamps provenance
   metadata only when the cache node is the root of the executed expression:
   1,234 bytes against 858 for the same rows. Build and heal agree today only
   because both happen to call `count()`.
5. **Inlined chaining misclassifies children.** Under ADR-006 decision D4
   (chaining inlines the parent's cache node) a child's
   graph contains its parent's Aggregate and canonical Sort, so a filter over a
   cached aggregate is itself worthy and writes a full copy. This is ADR-003's
   motivating bug. Graphs also grow with chain depth, which is the cost the
   pre-#74 per-entry parquet boundary existed to avoid (#82).
6. **Tallyman carries code whose only job is to aim xorq's cache.**
   `rewrite_cache_dirs`, the contract's "every loader must supply `cache_dir`",
   `manifest.snapshot_key` with `_assert_recorded_snapshot_key` (ADR-006
   decision D8, the snapshot-key tripwire),
   and `_cached_node_path`.

xorq is frozen upstream for this project, so every item above needs a
tallyman-side workaround for as long as xorq's cache is in the path.

## Governing rule

Stated by Paddy in the grilling session (2026-09-19 and 2026-09-20), and the
reason several decisions below are consequences and not separate choices:

> Buckaroo is a very good displayer. It runs queries only for summary stats,
> sorting and paging. Anything more is done by tallyman first. If something
> tallyman asked Buckaroo to display does not exist, that is on tallyman.

> When the MCP asks tallyman to create an entry, tallyman should run the query
> and materialize the parquet if necessary immediately. Tallyman shouldn't call
> Buckaroo to display an entry until the original query has finished.

> I want a cohesive system that works reliably, then we can worry about speed
> problems as they come up. We don't have a cohesive system now.

That last statement sets the priority for all three ADRs of this set (this one,
`plans/ADR-008-row-order-of-reads.md` and `plans/ADR-009-digest-stability.md`):
where a uniform rule and a faster special case compete, the uniform rule is the
decision and the faster path is noted for later.

So tallyman runs an entry's computation, or a diff, to completion before it
asks Buckaroo to show anything. No other process runs an entry's expensive
computation, writes result files, or repairs tallyman's cache. Besides being
simpler, this puts every failure of a computation in tallyman's process, where
it can be logged and reported, and none inside a grid query in Buckaroo.

One exception is known and accepted for now. The live diff still hands
Buckaroo an unmaterialized join, because the decision that fixed it (D10) was
moved out of this set to #188.

## Decisions

### D1. Builds carry no cache nodes

`rewrite_for_build` keeps the in-memory-read rejection and the canonical sort
(ADR-006 decision D5, so the sort stays inside the build) and stops calling
`.cache()`:

```python
if _is_worthy_expr(expr):
    expr = _canonical_sorted(expr)
```

The source-read injection (`source_cache.py:215-234`) is deleted with it. It has
no producer today: `deferred_read_csv` is a build error and no JSON reader
exists. A recipe that arrives already containing a `CachedNode` becomes a build
error next to the in-memory check, because a node with default storage would
write under `~/.cache/xorq`.

`source_cache.py:229` and `:243` are the only places in `src/` that create a
cache node, so this one change removes xorq's cache from tallyman's data path.
xorq remains the expression, build, load, hashing and execution layer.

Removing the node alone would turn every worthy entry into a recompute entry
(`_resolve_result_plan` treats "no `CachedNode` on top" as recompute,
`result_cache.py:642-649`), so D2 to D6 land in the same change.

*Rejected:* patch ADR-006 decision D4 (inlined chaining) in place. Deep-rewrite
the build's load (a one-line
change, verified to stop the build leak with no hash change), wait for
buckaroo#972, put a cross-process lock around xorq's writer, and pre-seed
xorq's files with a better writer (a hit is a bare existence check, so tallyman
can write `<key>.parquet` itself). This keeps every hash and needs no rebuild.
It also keeps the file's name as xorq's tokenization of the graph (so the
snapshot-key tripwire of ADR-006 decision D8 stays), keeps item 5, and leaves each fix as a workaround for
behaviour in a dependency that cannot be changed from here.

### D2. A snapshot's location is a function of the content hash

`snapshot_path(project, content_hash)` returns
`compute_cache/result_cache/<content_hash>.parquet`. Whether an entry has a
snapshot comes from the manifest's `cache_worthy`, as it does now.

`cached_result_expr` keeps its name and signature (ADR-006 decision D2, "keep
the facade"). For a worthy
entry it returns one bare read of `snapshot_path`, memoized per
`(project, content_hash)` for the life of the process, so repeated reads stop
piling up tables in the shared backend: one read has one table name. It does
not stop the footer being opened per query, because xorq registers a deferred
read's table again on every execute (`xorq/expr/api.py`,
`_transform_deferred_reads`). With the format of ADR-009 decision D3 that is a
2.7 KB read. A worthy entry whose file exists is served without loading its
build at all. For a cheap entry it returns the loaded graph, as now.

Deleted: `manifest.snapshot_key`, `_assert_recorded_snapshot_key`,
`_cached_node_path`, `rewrite_cache_dirs`, and the `cache_dir` argument. With
the path computed from the hash there is no second derivation for a tripwire to
compare against.

*Rejected:* `<content_hash>-<digest>.parquet`, which would make a child name
its parent's exact bytes. An unfaithful heal (a heal whose result does not
match the recorded digest, ADR-006 decision D12) would then write a
file no existing child can find, and a flagged condition would become a hard
failure for the whole subtree. The digest stays in the manifest, where verify
already looks.

### D3. Chaining through a worthy parent is a bare read of its snapshot

`tracked_expr_from_alias` and `pinned_expr_from_alias` return
`deferred_read_parquet(snapshot_path(parent))` for a worthy parent, after
making sure the file exists (D5). For a cheap parent they return the parent's
loaded graph, which by D1 contains no cache nodes. `entry_graph_expr` and
`cached_result_expr` become the same function.

The child's hash covers the literal path of the parent's snapshot, and that
path contains the parent's content hash. A child's identity is therefore a
function of its parent's identity, which is the device ADR-002 already uses for
sources: if you want content identity, put it in the path.

Measured with `scripts/spike_bare_read_chaining.py` (raw xorq, no cache nodes):

| Question | Result |
| --- | --- |
| Child hash, snapshot rewritten with other bytes at the same path | unchanged |
| Child hash, same bytes at another path | changes |
| `build_expr` of a child while the parent's snapshot is absent | raises `FileNotFoundError: local path does not exist` |
| `load_expr` of the child's build while the snapshot is absent | succeeds |
| Executing it in that state | raises `ValueError: At least one path is required` |
| Parent re-materialized from its own build, then the child executed | digest unchanged; child rows match the reference (19,777) |
| `classify_build` of a filter + computed column over the snapshot | cheap |
| `classify_build` of the same child with the parent graph inlined | worthy (`ops:Aggregate,Sort,SortKey`) |
| Child `expr.yaml` | carries the literal path; 3,434 bytes against 6,903 inlined |
| Files written under `XORQ_CACHE_DIR` | none |

So the graph is cut at every worthy entry, a child of an aggregate is cheap,
and the literal path in `expr.yaml` means `make_portable_inplace` handles it
with the existing `${TALLYMAN_PROJECT_ROOT}` placeholder.

*Rejected:* keep inlining the parent's graph without its cache node. Every read
of every descendant would re-run the parent's expensive subgraph.

### D4. One writer, used by the build and by every heal

`materialize(project, content_hash)` loads the entry's build, executes it as a
record-batch stream, and writes the snapshot itself:

- a unique temp name in the destination directory, then `os.replace`;
- under the project's write lock (D11). A heal repeats the existence check
  inside the lock, so a second reader waits and then finds the file. A create
  does not look: it always runs the query and replaces whatever is at the
  path. That keeps an entry that is added again after a reset honest (D14),
  and decision D6 of `plans/ADR-009-digest-stability.md` (create runs the
  query twice) needs it;
- it numbers the rows as it writes them, in a last column named `__row_order`
  (decision D2 of `plans/ADR-008-row-order-of-reads.md`);
- it returns the digest of what it wrote.

The build's execute-once step and every heal call this function, so the
contract's I2 ("result bytes are manufactured exactly once; a self-heal is
reproduce-and-verify") holds because there is one routine that manufactures
bytes. The file format and the digest definition are ADR-009's.

*Rejected:* keep `ParquetStorage` behind a tallyman lock. That fixes the race
and keeps items 3 and 4.

### D5. One entry point makes files exist: `ensure_materialized`

`ensure_materialized(project, content_hash)` guarantees that every snapshot an
entry's plan reads is on disk before anything executes:

1. If the entry is worthy and its snapshot exists, return. No build is loaded.
2. Otherwise load the entry's build and collect every file its `Read` nodes
   point at. The list is kept with the loaded plan in the existing LRU.
3. Re-create each of those that is missing, by the rule for its class (D13): a
   snapshot by recursing on the hash in its file name, an ordered copy of a
   source from its clone, a clone from the live source while the bytes still
   match.
4. If the entry is worthy, `materialize` it.

Every file it writes is verified against the manifest's `result_digest` before
it is served. A mismatch takes the existing path: a durable `unfaithful_heal`
record and the SSE event (ADR-006 decision D7, loud verification), the
stat-cache wipe and session eviction (ADR-006 decision D10), and the pin and
badge (ADR-006 decision D12).

Callers: the canonical read (`cached_result_expr`, on every call, where step 1
or a handful of `stat` calls is the whole cost, and which covers diff
composition), chaining at mint time (D3), and `load_session` (D6).

The verify sweep (`verify_sweep`, `staleness.py:118`, reached through
`catalog_scan_staleness(verify_results=True)`) is not a caller. It checks the
files that exist, reports each entry that recorded a digest as faithful,
unfaithful or absent, and writes nothing, which is what it does today. A sweep
that called this function would rewrite every deleted snapshot in the project,
which D12 forbids. Nothing is lost by leaving absent files alone: every file
this function writes is verified before it is served, so an absent file is
checked at the moment it next exists. A file that exists with the wrong digest
is reported through the same loud path and left in place, since deleting it is
the user's action (D12).

An earlier draft stopped at snapshots. Step 2 collected only reads under
`compute_cache/result_cache/`, and a missing ordered copy of a source
(decision D2 of `plans/ADR-008-row-order-of-reads.md`) surfaced inside
Buckaroo as `At least one path is required`. Paddy accepted that on
2026-09-20 as the case of someone deleting a directory by hand. The second
review played a reset through and found the same state reachable by an
ordinary reset back and forward, on today's code
(`scripts/spike_reset_roundtrip.py`, results under D13). After decision D7 of
`plans/ADR-008-row-order-of-reads.md` (a CSV root becomes a cheap read) every
page of a CSV-rooted cheap entry reads that copy, so the gap is closed here
and not left to follow-on work. When nothing can re-create a file, because the
clone is gone and the live source has changed, the error names the source
file.

ADR-006 decision D4 rejected bare-read chaining because "builds stay
non-self-contained and the pre-heal choreography stays load-bearing forever".
What that decision bought was a build that repairs its own ancestors when
something other than tallyman executes it cold. Buckaroo is the only such
thing, and the comment at `buckaroo_lifecycle.py:570-575` says the design
relied on it: "Buckaroo's replay of the build regenerates any evicted snapshot
through ordinary cache mechanics on first query." Under the governing rule
Buckaroo never does that, so the property has no user and nothing is given up.
This was question 1 of the grilling session, resolved 2026-09-20.

Bare-read chaining has failed here once before. #76 (closed) is a child
frozen on its parent's snapshot path and read after a reset had pruned that
snapshot, or on a clone of the project that never had it: the read died with
`At least one path is required` and nothing recomputed the parent. ADR-006
decision D4 (chaining inlines the parent's cache node) was the answer then.
This function is the answer now, and the
"files exist before anything runs" test below is #76's reproduction.

The old pre-heal was also weaker than this function. It was a
`cached_result_expr` call with a discarded result at one call site, and
ancestors were healed only because reads then re-executed recipes. Here the
requirement is a precondition of the one canonical read that every in-process
consumer already uses (contract invariant I3, "one read semantics"), and it is
computed from the build's own reads, so it cannot drift from what execution
opens.

*Rejected:* derive the required snapshots from `manifest.parents`. It needs only
JSON reads, but it depends on the recorded edges being complete and on each
parent's recorded worthiness matching what chaining did when the child was
minted. Those edges are recalc policy and are not complete today: a promoted
diff's recipe calls `build_diff_expr`, which reads both sides through
`cached_result_expr` and records no parent edge at all. The build's reads are
exactly what the engine will open.

### D6. Buckaroo is handed something that already exists

This is the governing rule applied to entry grids.

For a worthy entry `load_session` calls `ensure_materialized`, then posts a
view build of the snapshot, written once to a stable per-entry directory
(Buckaroo's stat-cache keys include the build directory's path, which is why
the expanded build already lives at a stable path). Buckaroo never executes an
aggregate, join or sort on tallyman's behalf and never writes a snapshot, and
tallyman stops depending on buckaroo#972, whose premise (teach Buckaroo where
tallyman's cache is) the rule contradicts. The grid and `/api/data` read the
same file (contract invariant I5, "one question, one path").

For a cheap entry it calls `ensure_materialized` and posts the entry's own
expanded build. A cheap build is a view in the database sense: a stored
definition (filter these rows, keep these columns) over files that exist.
Buckaroo's stats, sorts and pages run through it. Tallyman has already executed
that plan once, in full, at build time, so an error in it has already surfaced
in tallyman.

Paddy confirmed this reading on 2026-09-20 (question 4 of the grilling
session). The words that decide it are "materialize the parquet if necessary":
a file is written when the entry is created and only for a worthy entry, and
nothing is written when an entry is viewed. The alternative, writing a file the
first time a cheap entry is opened so that Buckaroo only ever reads one file,
was set aside. It costs a wait on first open and a full copy per viewed
revision; the audit measured 19 GB of cache against 779 MB of data when every
CSV revision wrote a copy.

The timing half of the rule already holds for creation. The build executes the
entry once before it writes the manifest, a worthy entry's snapshot is written
in that step (D4), and an entry with no manifest is treated as absent, so
Buckaroo cannot be asked to display an entry whose query is still running. The
same ordering now covers a snapshot that was deleted later: `load_session`
waits for `ensure_materialized` before it posts anything.

The same rule covers the session itself. Buckaroo drops a session that has had
no browser attached for an hour, and tallyman's session map assumes a session
lives as long as the Buckaroo process, so an entry reopened after an idle hour
is handed an id Buckaroo no longer knows and the grid never loads. Tallyman
stops remembering sessions. The session id is derived from the project and the
content hash, and `load_session` posts `/load_expr` with that id every time.
Buckaroo 0.15.6 skips the work when it already has a session with that id and
the same build directory, provided the post carries none of
`component_config`, `column_config_overrides`, `extra_grid_config`, `init_sd`
and `skip_stat_columns` (`buckaroo/server/handlers.py`, lines 429-454).
Tallyman sends none of them for an ordinary entry, so the repeat post is a
no-op there. A promoted diff entry sends `column_config_overrides` and so
reloads on every open, which #188 covers. Putting the project in the id also
closes #172 (one project's session served to another on a hash collision).

One caller still needs to know which grids are open. When a klass is added or
changed, `reload_project_sessions` (`buckaroo_lifecycle.py:386`, four call
sites in `app.py`) posts `/reload_expr/<session id>` to every session of the
project, and it finds them in the record this decision deletes. Buckaroo
0.15.6 has no route that lists sessions. With derived ids none is needed:
tallyman posts `/reload_expr/<id>` for each entry of the project and treats
the 404 that Buckaroo returns for an unknown session as "not open", a case
the function already handles. That is one request per entry per klass change.
Found in the second review; the first draft of this decision did not mention
the reload.

Deleting a snapshot (D12) ends no session. A tab that already has the entry
open fails on its next query, inside Buckaroo, with the
`At least one path is required` error above. That is accepted: the user
deleted the file on purpose, and reopening the entry fixes it, because every
open goes through `ensure_materialized` before it posts. The session Buckaroo
still holds then works again. xorq registers the path afresh on every query,
and a faithful rewrite has the same content, so the session's stats are still
right. An earlier draft ended every session whose plan read the deleted file.
It was dropped in review: it needs an index from each snapshot to every entry
that reads it, and Buckaroo has no route that ends a session.

An unfaithful heal is the one event that leaves a session wrong, since the
path now holds different rows and the session holds stats computed from the
old ones. ADR-006 decision D10 handles it today through `evict_session`, which
works by dropping tallyman's own record of the session so that the next load
mints a new one. With no record to drop, the companion's unfaithful-heal hook
wipes the entry's stat cache, as now, and posts `/load_expr` for the entry's
id with `force_reload: true`, which Buckaroo already accepts and which re-runs
its pipeline for that session. Sessions of entries built on the healed file
are stale too, as they are today; that is part of #185.

This resembles what #104 removed: #102's viewer build over
`<entry>/result.parquet`. #104's objection was two materialized copies per
entry and two read paths. Here there is one copy and the view build only points
at it.

*Rejected:* post the entry's own build for worthy entries too. With no cache
node in it, Buckaroo would re-run the full computation for every page and stat
query.

### D7. The cold state is an empty `compute_cache`

The contract's cold seam today is the `cache_dir` argument, and the standing
tests exercise it by deleting `compute_cache/` and reading again
(`tests/test_lineage_faithful_reads.py`). That remains the test: with
`compute_cache/` removed, the canonical read must reproduce every snapshot the
entry needs, each with its recorded digest. The `cache_dir` parameter leaves
the contract, since nothing in a build resolves through it any more.

### D8. A sentinel test keeps xorq's cache out of the path

With `XORQ_CACHE_DIR` pointing at an empty sentinel directory, a build, a
chained child build, a view, an eviction and a heal must leave the sentinel
empty. The test fails on `main` today (finding 1), so it belongs in the
failing-tests commit, and it outlives this change as the check that no xorq
cache node has crept back in.

xorq reads `XORQ_CACHE_DIR` once, when it is first imported, and
`tests/conftest.py` points the whole test session at one directory for that
reason. So the sentinel cannot be a fresh directory per test. The test either
asserts that no file appears in the session's directory while it runs, or runs
its steps in a child process with a value of its own.

### D9. One change, one rebuild

Removing the cache node changes the hash of every worthy entry, and bare-read
chaining changes every child's. This lands together with ADR-008's change to
`tallyman_read_csv`, the fix for #168 that the change depends on (CSV sources
go through source identity, ADR-008 decision D2), and ADR-009's digest
definition, behind a single corpus rebuild. After the rebuild,
`~/.cache/xorq/result_cache` (14 GB) and the older leaks under
`~/.cache/xorq/parquet/` (2.6 GB) can be deleted by hand.

Order of work, agreed 2026-09-20. Nothing starts until Paddy has reviewed all
three ADRs.

1. One commit of failing tests covering everything the three ADRs change, plus
   the audit's independent bugs, pushed and seen red on CI.
2. The redesign as one change, then the corpus rebuild.
3. The independent bugs that remain, which change no hash: the chart error
   loop, eager notebook sessions, the staleness scan re-hashing every source
   per entry, `tallyman pack` shipping the cache, the primary-key search that
   never converges, and the over-broad stat-cache wipes.

### D10. Diffs: moved to a follow-on (#188)

This decision said that every diff is built as an entry before it is
displayed, with an unnamed diff stored as an ephemeral entry under
`compute_cache/`. Paddy moved it out of this set on 2026-09-20, so that the set
stays about the core structure of the cache. Its text, and what the review
found about it, are in #188. The number is kept so that references to D11 and
D12 stay valid.

What this set still does for diffs:

- `build_compare_expr` drops `__row_order` from both sides before joining
  (decision D6 of `plans/ADR-008-row-order-of-reads.md`).
- Both sides are read through `cached_result_expr`, so both files exist before
  the join is composed (D5). A diff is the one consumer that needs two files
  at once, which makes it the natural test of D5.
- A promoted diff is an ordinary worthy entry. It contains a join, so it is
  materialized by D4 like any other.

Until #188 lands the live diff works as it does today. It posts an
unmaterialized join to Buckaroo (`_build_compare_expr`, `app.py:418-441`),
which is the known exception recorded under the governing rule.

### D11. One write at a time per project

Every write takes the project's existing lock: a build, a materialization, a
promote and a recalc, as well as the checkpoint that takes it today.
`_project_lock` (`catalog_state.py:232-242`) is an OS file lock on
`.checkpoint.lock`, so it holds between the two processes of a normal tallyman,
the MCP server and the companion, which both build. It becomes re-entrant
within a process, since a promote builds and then checkpoints.

This replaces the per-entry lock of this ADR's first draft and closes the audit
finding that two builds of one entry can end with the failing one deleting the
winner's directory (`build.py:447-468`, `618-627`). FastMCP runs tool calls on
a thread pool, so parallel tool calls did build at once. They now queue.

Three limits, recorded here so that the implementation does not have to
discover them:

- Re-entrant has to mean per thread. `_project_lock` takes `flock` on a fresh
  file descriptor, so a nested acquire in one process blocks forever. The
  companion also moves work between threads with `run_in_threadpool`, so the
  lock cannot be held across an `await`.
- Whether a recalc takes the lock once per build or once for the whole walk is
  left to the implementation. Either is correct.
- The lock covers writes only. Concurrent reads on the shared default backend
  fail with `RuntimeError: Already borrowed`. That is #118, and this ADR does
  not change it.

The lock is blocking and has no timeout, and the work it now covers is long: a
materialization runs single-partition, and a create runs its query twice
(ADR-009 decisions D1 and D6). A heal runs it once, since the recorded digest
is what it is compared against.
A page request that needs a heal therefore waits behind any build in the other
process. Paddy, 2026-09-20: correct first. #186 tracks the waiting.

Two tallyman servers pointed at one project is unsupported. Paddy: "you have
done something diabolical and deserve the results." The file lock would still
serialize their writes on one machine, and nothing else about them is safe,
because each holds in-process state the other never sees.
buckaroo-data/tallyman#183 tracks detecting that case and refusing to start.

### D12. Files are deleted only by an explicit user action

Nothing deletes a materialized file on its own, and nothing writes one
speculatively. A file is written only because something is about to read it: a
read of the entry, or a build or a read of an entry whose plan reads its file
(a page request, a chart, chaining at mint time, a recalc). Those are D5's
callers. There is no disk budget yet. That is the rewrite of
`plans/ADR-003-result-cache-cost-rubric.md`, deferred.

- The startup warm-up stops calling `cached_result_expr`. Today it heals
  deleted snapshots until a 3 s budget is spent, and the budget is checked only
  between entries (`app.py:807-824`). That undoes the Cache page's delete
  button, and one large heal blocks startup for as long as it takes.
- The verify sweep reads and never writes (D5).
- A reset does not touch `compute_cache/` (D14).
- An explicit delete skips an entry marked not reproducible (decision D6 of
  `plans/ADR-009-digest-stability.md`), whose file cannot be recreated. The
  skip protects the file from the Cache page only. `compute_cache/` as a whole
  is still deletable by definition (D7), and where such a file should live is
  part of #185.

### D13. A file is cache only if `ensure_materialized` can re-create it

Three classes of file sit behind an entry, and until now only the first had a
whole lifecycle:

| File | Written by | If it is missing | So it is |
| --- | --- | --- | --- |
| Snapshot, `compute_cache/result_cache/<content_hash>.parquet` | `materialize` (D4) | re-run the entry's build and verify the digest (D5) | cache |
| Ordered copy of a source (decision D2 of `plans/ADR-008-row-order-of-reads.md`) | ingest, through polars | re-run ingest on the clone, with the reader options the manifest records | cache |
| Clone, `data/.cas/<digest><suffix>` | `ensure_cas_path` | copy the live source again, but only while its bytes still hash to the digest | data |

The rule: a file is cache only if `ensure_materialized` can re-create it from
files that are not cache, and check what it made. Everything under
`compute_cache/` has to satisfy that, and then anything may delete it (D7,
D12). A file that is not cache is never deleted by machinery.

What follows from the rule:

- **Ordered copies are cache, so they live under `compute_cache/`**, inside
  the project, where the portable-path placeholder covers them and the cold
  test of D7 deletes them. `csv_ordered/` under `TALLYMAN_HOME` is retired.
  For `ensure_materialized` to re-run ingest, the manifest's `sources` records
  each source's reader options (the schema spec and the `scan_csv` options)
  next to its digest. It holds only `{path: digest}` today. A re-created copy
  is checked against a digest recorded when the copy was first written, which
  is the rule D5 applies to snapshots. Where the copies live and the recorded
  digest both follow from the rule and are not yet confirmed by Paddy in so
  many words. They settle open question 3 here and open question 5 of
  `plans/ADR-008-row-order-of-reads.md`.
- **Clones are data.** A clone is the only frozen copy of the bytes an entry
  was built from, and it cannot be made again once the live file has been
  edited. `gc_cas` deletes one today as soon as no surviving entry refers to
  it, which D14 changes. `recon_cas_path` already re-clones a missing clone
  from the live source when the live bytes still hash to the digest, but it is
  reached only by re-running a recipe, which reads stopped doing in ADR-006.
  That check moves into step 3 of D5.
- **The snapshot of an entry that is not reproducible** (decision D6 of
  `plans/ADR-009-digest-stability.md`, create runs the query twice and
  compares) is data that sits in the cache directory. Where it should live is
  part of #185, as D12 already says.

Measured on today's code (`scripts/spike_reset_roundtrip.py`). Step s1 holds
an entry over `orders.parquet`. Step s2 adds a cheap entry and a worthy entry
over `extra.parquet`, which is never touched:

| After | Cheap entry | Worthy entry |
| --- | --- | --- |
| building s2 | reads | reads |
| a reset to s1, then a reset forward to s2 | `ValueError: At least one path is required` | reads, from the snapshot the bullpen gave back |
| the same, then `compute_cache/` emptied | the same error | the same error, from the heal |

The reset to s1 deleted the clone of `extra.parquet`, the reset forward
restored the entries that read it, and nothing on the read path makes a clone
again. This is a defect in today's code and does not depend on this ADR.

*Rejected:* keep ordered copies next to the clone under `data/.cas/`, as the
fix for #168 first proposed. `gc_cas` deletes every file in that directory
whose stem is not a live source digest, and an ordered copy is named by a key
and not by a digest, so every reset would delete every ordered copy.

### D14. A reset leaves `compute_cache/` alone

`reset_to` (`catalog_state.py:327`) returns the catalog to an earlier step
with `git reset --hard`, and then reconciles the files git does not track. For
`compute_cache/` it keeps a git-tracked list, `compute_cache.jsonl`, of every
file that was under the directory at each checkpoint
(`capture_tallyman_state`). A reset moves each file that is not on the target
step's list into the bullpen (`prune_compute_cache`), copies back each listed
file that is missing (`restore_from_bullpen`, a bare `shutil.copy2` onto the
final path), and then deletes every clone that no surviving entry refers to
(`gc_cas`). None of the three ADRs of this set mentioned it before the second
review.

Against this ADR that machinery is a second owner of the cache:

- the copy back is a second writer of snapshot files, outside D4. It is not
  atomic, and since D5 treats "the file exists" as a hit, a page request can
  open a half-copied file;
- the prune moves files that D12 says only the user deletes, including the
  file of an entry that is not reproducible, which cannot be rebuilt;
- the list records whatever is under the directory, so it would pick up the
  writer's temp files after a crash;
- capturing the list is a cost every checkpoint pays, and it grows with the
  cache (#22).

Decision: a reset stops managing `compute_cache/`. Snapshots are named by
content hash, so a file left behind by a retired entry cannot be served for
another entry. It is unreferenced disk until that entry comes back or the user
deletes it, and a snapshot that is missing after a reset is healed and
verified like any other (D5). `compute_cache.jsonl`, `prune_compute_cache` and
the `compute_cache/` half of `restore_from_bullpen` are deleted.

The prune existed for one property: after a reset, an expression that is added
again should compute cold, so that a rehearsed demo shows real work. A create
always runs the query (D4), so that property holds without the prune.

Clones are data (D13), so a reset stops deleting them. It moves a clone that
no surviving entry refers to into the bullpen, and a reset forward brings it
back, which is how a reset already treats entry directories.

Three options were played through in the second review. Keeping today's
machinery needs an atomic, verified copy back and an exception for files that
cannot be rebuilt. Pruning without restoring keeps one writer and still needs
that exception. Leaving the directory alone needs neither. Paddy, 2026-09-20:
"I guess your suggestions sound good."

Cost accepted: the snapshot of a retired entry stays on disk until the user
deletes it. The Cache page lists files by entry, so it needs a row for files
whose entry is not in the catalog.

## Testing

Every test below goes in the failing-tests commit and is seen red on CI before
the change lands (D9, step 1). A test of a function that does not exist yet
fails on import, and that counts as red. An earlier draft let such tests ride
with the change. Paddy, 2026-09-20: do normal TDD.

- **Sentinel** (D8). A build, a chained child build, a view, a delete and a
  reopen write nothing under xorq's cache directory.
- **Concurrent builds** (D11). Two threads building the same entry both
  return it, and the entry's directory is intact afterwards.
- **Forgotten session** (D6). After Buckaroo has dropped a session,
  reopening the entry yields a grid that loads.
- **Warm-up leaves deleted files alone** (D12). A snapshot deleted before
  startup is still absent after startup with no requests made.
- **Identity** (D3). A child's hash changes when its parent's snapshot path
  changes and not when the file's bytes do, and a filter over an aggregate's
  snapshot is classed cheap.
- **Files exist before anything runs** (D5). With an ancestor's snapshot
  deleted, opening a descendant rewrites the ancestor first, verifies it, and
  never executes a plan whose file is missing.
- **Cold equals warm** (D7). With `compute_cache/` removed, every snapshot an
  entry needs is reproduced with its recorded digest.
- **Handoff** (D6). A worthy entry's grid is posted a view build of its
  snapshot, and Buckaroo writes nothing under `compute_cache/result_cache/`.
- **Two files at once** (D5, D10). With both sides' snapshots deleted,
  composing a diff rewrites and verifies both before the join is built, and the
  diff carries no row-order column from either side.
- **Explicit delete** (D12). After a snapshot is deleted, the next open
  rewrites it and verifies it before Buckaroo is called.
- **The sweep writes nothing** (D5, D12). With a snapshot deleted, a verify
  sweep reports the entry as absent, and the file is still absent afterwards.
- **Forced reload** (D6). After an unfaithful heal, Buckaroo receives a
  `/load_expr` for that entry's session id with `force_reload` set.
- **Klass reload without a session record** (D6). After a klass is added, a
  grid that is open shows it, and an entry that was never opened has no
  session afterwards.
- **A create always runs** (D4, D14). Building an entry whose snapshot file is
  already on disk runs the query and replaces the file.
- **Every class of file is re-created** (D13). With an ordered copy deleted,
  opening a root entry makes it again from the clone and checks it. With a
  clone deleted and the live source unchanged, the clone is made again. With
  the live source changed as well, the error names the source file.
- **A reset back and forward breaks nothing** (D13, D14). After a reset to an
  earlier step and a reset forward, every entry of the restored step reads,
  cheap and worthy, and still reads after `compute_cache/` is emptied. The
  reset moved and copied nothing under `compute_cache/`.

## Consequences

- **Retired:** the `.cache()` call and the source-read injection in
  `rewrite_for_build`; `rewrite_cache_dirs`; `_cached_node_path`;
  `manifest.snapshot_key` and `_assert_recorded_snapshot_key`; the `baked` /
  `recompute` plan split keyed on a `CachedNode`; the thread-only `_heal_lock`
  and the `(FileNotFoundError, ValueError)` retry around xorq's shared temp
  file; `entry_graph_expr` as a separate function; tallyman's record of
  Buckaroo sessions (`_sessions`, `~/.tallyman/buckaroo_sessions.json`) and
  `evict_session`, which worked by dropping an entry of it (D6);
  `compute_cache.jsonl`, `prune_compute_cache` and the `compute_cache/` half
  of `restore_from_bullpen` (D14); `csv_ordered/` under `TALLYMAN_HOME` (D13).
- **#77:** closed by D2, though not tested on a second machine. #77 is an
  empty grid on a clone of the project at another path. The snapshot's name
  came from xorq's tokenization of a graph that contains the build-time path,
  so a heal on the new machine wrote its file under the key that machine
  computed, while the child's frozen build read the key computed at build
  time. Under D2 the name is the entry's recorded content hash, which is its
  directory name and is the same on every machine, and the literal path in a
  child's build goes through the `${TALLYMAN_PROJECT_ROOT}` placeholder. D5
  then heals the file where the build reads it.
- **ADR-006:** its D4 (inlined chaining) and D8 (snapshot-key tripwire) are
  superseded. Its D2's "snapshot path derived from the loaded expression"
  becomes `snapshot_path`. Its D3 (rebind composition onto the default backend)
  is still needed for cheap parents and for diff composition. Its D5 (canonical
  sort) and D6 (a missing build is a hard error) are unchanged. Its D7, D10 and
  D12 (loud verification, the Buckaroo-state wipe, the pin and badge) attach to
  `ensure_materialized`.
- **`docs/system-contract.md`** needs rewriting in Part 1 §4 (xorq caching
  becomes background, not mechanism), "Content hash" (no cache injection in the
  hashed expression; parents appear as snapshot paths), "Manifest" (drop
  `snapshot_key`), "Worthiness", write path steps 2 and 5, the read path,
  "Chaining", and the `[^preheal]` footnote, which currently describes
  bare-read chaining as a retired workaround.
- **`plans/remove-ondemand-result-parquet.md`:** its premise that xorq's
  snapshot is the single materialized copy is replaced. Tallyman's snapshot is
  the single copy.
- **ADR-003:** chained descendants of an expensive entry are cheap, so its
  motivating lineage stops filling the cache when the iterations chain off the
  join. A revision that restates the join in its own recipe is still worthy
  and still writes a copy, so the budget and eviction half of ADR-003 remains
  and should be rewritten against this design.
- **CSV lineages:** a child of a `tallyman_read_csv` entry reads the parent's
  snapshot and no longer inherits its Sort, so revisions stop baking one full
  sorted copy each. The root entry's own copy is ADR-008's subject.
- **Buckaroo's stat keys** for a worthy entry become a function of one read of
  one content-addressed path.
- **Source identity `salt` mode:** `rewrite_for_build` returns early under
  `salt` because xorq's path-only snapshot keys would collide. A snapshot named
  by the entry's content hash has no such collision, so the early return should
  become unnecessary. Not tested. The early return also skips the canonical
  sort, so while it stays a worthy entry built under `salt` is written in the
  engine's order.
- **Cost accepted:** a worthy parent's snapshot must exist before a child can
  be built. A build also no longer repairs its own ancestors when something
  outside tallyman executes it, and under the governing rule nothing does. A
  tab open on an entry whose file the user deletes errors until the entry is
  reopened (D6). The snapshot of an entry that a reset retired stays on disk
  until the user deletes it (D14).

## Implementation notes

What the implementation does that the text above does not say, or says differently:

- **Where the code is.** `src/tallyman_xorq/materialize.py` holds `snapshot_path`, `materialize`,
  `ensure_materialized`, `single_partition_backend` and `pinned_reason`; `src/tallyman_xorq/ordered_copy.py` holds
  ingest and the re-creation of ordered copies; `src/tallyman_xorq/result_cache.py` keeps `cached_result_expr` as the
  canonical read. `cached_result_expr` and the heal both go through one internal `_ensure`, which returns whether the
  entry is worthy.
- **Open question 3 (where ordered copies live).** `compute_cache/ordered_sources/<key>.parquet`, where the key is an
  md5 of the source's content digest and the reader options. It uses the digest and not the clone's path, so a project
  cloned to another path names the same file. A sidecar `<key>.digest` beside each copy holds its content digest, and
  is read back from the file if it is missing.
- **The manifest.** `sources` keeps its shape (`{path: digest}`). The reader options and the copy's content digest are
  in a new field, `ordered_copies`, keyed by the copy's file stem. A child records its parent's `ordered_copies` only
  when the parent is cheap, because only then does the child's build read those copies directly.
- **The view build (D6)** is written to `<entry>/.xorq_view_build/`, and a sibling `.complete` marker records the
  snapshot path it was made for, so a project that moved regenerates it.
- **The verify sweep (D5, D12)** reports `absent` (the hashes whose snapshot is missing) next to `results`,
  `unfaithful` and `errors`, and writes nothing.
- **Pinned files (D12).** `materialize.pinned_reason` answers whether a snapshot may be deleted: the manifest says
  `reproducible: false`, or an `unfaithful_heal` record exists for the hash. `/api/result_cache` reports `pinned` and
  `pinned_reason` per row and lists a file whose entry is gone as `orphan`; the delete route answers 409 with the reason.
- **The lock (D11).** `catalog_state.project_lock` is public and re-entrant per thread. `build_and_persist` holds it for
  the whole build, including the recipe import, so a chained build waits for its parent's materialization.
- **A create publishes its snapshot last (D4, #193).** The build calls `materialize(..., publish=False)`, which leaves
  the file complete at its temp name, and moves it into place with `publish_snapshot` after the manifest is written.
  A build that fails before then removes only its temp file, so a file already at the path, such as the one a reset
  left on disk (D14), is kept. A heal publishes at once. A crash between the manifest write and the publish leaves a
  complete entry over the file that was there before, or over none, which a heal then makes.
- **Clones (D13, D14).** `ensure_cas_path` clones to a unique temp name, since two builders cloning one source shared
  one. `gc_cas` moves clones into `<catalog>/bullpen/cas/` when given a bullpen.
- **Not done here.** The independent bugs listed under D9 step 3 (the chart error loop, eager notebook sessions, the
  staleness scan re-hashing every source per entry, `tallyman pack` shipping the cache, the primary-key search, the
  over-broad stat-cache wipes) change no hash and are follow-on work. #185, #186 and #188 are unchanged.


## Open questions

1. **Do two kinds of entry survive?** Closed on 2026-09-20. Paddy: "yep keep
   two kinds". A worthy entry is materialized when it is created, and a cheap
   entry is a stored plan over files that exist (D6). The number is kept so
   that references to open questions 2 and 3 stay valid.

   The alternative was to materialize every entry when it is created. It
   would have removed the cheap and worthy classifier, the build error of
   ADR-008 decision D3, the view case in D6 and open question 2, given every
   entry a digest, and cut every graph at one entry deep, which is the parquet
   boundary Paddy wanted in June. Its cost is one file per entry: one project
   measured 19 GB of cache for 779 MB of data while every CSV revision wrote a
   file. The second review measured that it was workable. On the
   single-partition connection `materialize` uses, every row-preserving plan
   streamed its rows in the parent file's order, three runs out of three,
   against none on the default connection (`scripts/spike_stream_order.py`),
   so the writer could have numbered rows in parent order with no column
   carried through the recipe.

   What keeping two kinds costs is that the test for "cheap" becomes a
   guarantee. A cheap entry has no file of its own and pages by its parent's
   `__row_order`, so the test has to ensure that the column is still present
   and still unique at the top of the plan. Four decisions of
   `plans/ADR-008-row-order-of-reads.md` carry that, and each was tightened or
   added in the second review: D3 (dropping the column is a build error), D4
   (the test itself), D6 (joins) and D12 (a raw parquet read is a build
   error).
2. **Deep cheap chains.** Nothing cuts the graph between cheap entries. Still
   open, now that open question 1 is closed with two kinds.
3. **Where ordered copies of sources live.** ADR-008 decision D2 adds one per
   parquet source. `csv_ordered/` is global today, is never collected, and is
   not included by `tallyman pack`. Its path is also outside the project root,
   so `make_portable_inplace` does not rewrite it and a CSV entry's build is
   not portable. Settled in outline by D13: an ordered copy is cache, so it
   lives under the project's `compute_cache/`, and `ensure_materialized` makes
   it again from the clone when it is missing, so `tallyman pack` has no need
   to ship it. Not yet confirmed by Paddy in so many words. What remains is
   the directory's name.

Closed in the grilling session: eviction policy (D12).

Reopened in review and moved out of this set: an unfaithful parent's
descendants. The grilling session closed it on the grounds that ADR-009
decision D6 finds a non-reproducible entry when it is created and pins its
file. The pin holds against the Cache page only, the file lives under
`compute_cache/`, which D7 defines as deletable, and an entry built on a
non-reproducible parent is itself recorded as reproducible, because both of
its runs read the same parent file. #185 has it. The collection of ephemeral
diff entries went to #188 with D10.
