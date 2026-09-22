# Caching across the stack

Tallyman sits on two other projects that each cache aggressively: xorq
(deferred expression execution) and Buckaroo (the dataframe viewer). This
doc maps every cache in the three layers — what work each one saves, what
its key is, and when it is invalidated. Bottom-up: xorq first, then what
tallyman does with it, then Buckaroo.

Tallyman does not use xorq's cache for results. No build contains a xorq
cache node (ADR-007 D1, builds carry no cache nodes): xorq stays the layer
that builds, hashes, loads and executes expressions, and the files that hold
results are tallyman's own. The xorq section is here because Buckaroo's stat
cache (the summary statistics Buckaroo computes for each column of a grid) is
built on it, and because it explains what the design replaced.

The dominant pattern everywhere is content-addressing: keys are derived
from immutable inputs (an expression's structure, an entry's content
hash, a source file's digest), so entries never go stale and "invalidation"
is usually a space decision, not a correctness one. The exceptions are
called out as they appear and collected at the end.

## xorq: the expression cache

Calling `.cache()` on an expression wraps it in a `CachedNode`
(`xorq/expr/relations.py`). At execution time xorq computes a key for the
uncached parent expression; if a stored result exists under that key it
reads it back, otherwise it executes the parent and writes the result.
Hits and misses are emitted as OpenTelemetry events.

What it saves: re-executing the entire upstream expression graph — remote
fetches, joins, aggregations, UDF runs.

### Strategies: what goes into the key

The key is the expression's dask-style token (`xorq.common.utils.dasher`),
and the two strategies differ in exactly one input — whether the source
file's modification time participates:

| | ModificationTimeStrategy | SnapshotStrategy |
|---|---|---|
| Key includes | expression structure + source file mtime | expression structure + source file path only |
| Detects upstream data changes | yes — mtime change means a new key, so a recompute | no — file contents can change and the cache still hits |
| Invalidation | automatic | manual delete or TTL only |
| Backing classes | `ParquetCache`, `SourceCache` | `ParquetSnapshotCache`, `ParquetTTLSnapshotCache` |

Both strategies tokenize the full expression structure, so any structural
change — a filter threshold, a column selection, a schema change —
produces a new key and a clean miss. Schema is part of the key, so schema
drift never serves stale-shaped data.

Snapshot's blindness to data changes is deliberate (reproducible build
artifacts keyed on what the expression *is*, not on what the source file
happened to contain), but it is the one real footgun in the stack:
`ParquetSnapshotCache`'s own docstring notes it does not re-key when
source files change. Tallyman puts a source's content in the path of the
file a recipe reads, so xorq's path-only keys are content-honest without
tallyman using either strategy; see below.

### Storage backends

`xorq/caching/storage.py`:

- `ParquetStorage` — writes `<base_path>/<key>.parquet`, default base
  `~/.cache/xorq/`. Writes go to a `.tmp` file then rename, so a crashed
  write never leaves a half-cache. Provenance metadata (expression hash,
  strategy) is stamped into the parquet on the root cache node.
- `SourceStorage` — writes tables into a database backend (postgres,
  snowflake, or a local engine) instead of files; the key becomes the
  table name.
- `ParquetTTLStorage` — `exists()` returns false once the file's mtime
  exceeds the TTL (default one day). The only time-based expiry in xorq.

Nothing evicts otherwise. Content-addressed entries are append-only;
cleanup is manual.

### In-process memoization

Tokenization is memoized per top-level `tokenize()` call (parent-token,
`DatabaseTable`-normalization, and expression-metadata memos installed via
a `with_caches()` context). Deliberately not process-global: a global memo
would mask filesystem changes within one long-lived process.

## tallyman

Tallyman writes its own result files. It decides at build time whether an
entry is worth materializing, writes the file once when the entry is created,
makes sure every file an entry reads exists before anything executes, and
reads sources through copies that carry a row-order column. xorq's own
cache is not in that path. ADR-007 (`plans/ADR-007-tallyman-owned-materialization.md`)
records the problems that led there: cache files landing under
`~/.cache/xorq` outside the project, a writer that races on a fixed temporary
file, and a build that re-ran an expensive parent whenever a child was built.

### Worthy and cheap entries (`src/tallyman_xorq/worthiness.py`)

A **worthy** entry is one tallyman materializes: it writes the result to a
parquet file when the entry is created, and every read afterward reads that
file. A **cheap** entry writes nothing: its small plan re-runs on every read,
over files that exist. The verdict is computed once, at build, on the
expression the author wrote (`classify_expr`) and recorded in the manifest as
`cache_worthy`, with a short `cache_worthy_why` such as `ops:Aggregate` or
`values:Unnest`. `result_cache.cache_worthy` reads it back from the manifest;
nothing re-derives it from the serialized build.

An entry is cheap only if all of these hold:

- every relation operation in it is a file read, a filter, a column selection
  or computed column (which covers rename and cast), a column drop, a drop of
  null rows or a fill of nulls;
- it reads exactly one file;
- no value operation in it multiplies rows (`unnest`), depends on the order
  rows arrive in (a window function, which covers `row_number` and `lag`), or
  is not pure (`random()`, `uuid()`, `now()`, `today()` and any UDF).

Everything else is worthy: an aggregate, join, sort, limit, union, distinct,
sample, and any operation nobody has considered yet. The test is an allow-list
because a cheap entry pages by its parent's `__row_order` (see Row order
below), so a wrong "cheap" gives unstable paging and a wrong "worthy" costs a
copy. A UDF is matched by its base class, so a scalar UDF makes an entry
worthy without any other expensive operation (#81).

### Snapshots (`src/tallyman_xorq/materialize.py`)

A worthy entry's **snapshot** is `<compute_cache>/result_cache/<content_hash>.parquet`
(`snapshot_path`), so its name is a function of the content hash and nothing
else. `materialize` is the one writer of snapshots, used by the build and by
every heal (a **heal** re-creates a snapshot that is missing from disk):

- it loads the entry's frozen build and rebinds every backend in it onto a
  fresh single-partition connection (`single_partition_backend`, with
  `target_partitions = 1` and an explicit `batch_size`), so a float aggregate
  merges its partial sums in one order on any machine;
- it streams the result as record batches, drops `__row_order` and ibis's
  `__row_order_right` if present, and appends a new `__row_order` counting
  `0..N-1` as the last column;
- it regroups the stream into row groups of 1,048,576 rows, each combined into
  contiguous arrays, and writes zstd level 3 in parquet format 2.6, with
  statistics and a page index;
- it writes to a unique temporary name in the destination directory and then
  replaces the destination with `os.replace`, under the project's write lock.

A create always runs the query and replaces whatever file is at the path. A
create of a worthy entry runs it twice through the writer and compares the
content digests of the two files (`check_reproducible`), since at create time
nothing is recorded to compare against. If they differ the build still
succeeds, and the manifest records `reproducible: false` with the columns that
differed. The file is then **pinned**: the Cache page will not delete it, since
it cannot be re-created faithfully. A heal runs the query once and checks the
result against the recorded digest.

The row-group size and the batch size decide the batch boundaries that an
entry built on the file sees, and an ungrouped float total depends on them, so
they are part of the reproducibility contract. `SNAPSHOT_FORMAT_VERSION`
stands for both, and for the layout of ordered copies; the manifest records it
as `snapshot_format`, next to the xorq, xorq-datafusion and pyarrow versions in
`engine_versions`. Changing any of them is a corpus rebuild.

`result_digest` in the manifest is `arrow-sha256:<hex>`, a SHA-256 over the
file's Arrow data read back (`src/tallyman_xorq/digest.py`). It does not depend
on how the rows were batched, on the codec, the row-group size, the writer's
version, or whether a text column is `string` or `large_string`. It does depend
on every value, on which slots are null, on the order of the rows, and on the
column names and types. Per-column digests name the columns that differ
between two runs.

### Ordered copies of sources (`src/tallyman_xorq/ordered_copy.py`)

A recipe never reads a source file directly. `read_project_file` and
`tallyman_read_csv` take the source's content digest (md5, memoized on the
file's stat). In the default `cas` mode they also clone the source
copy-on-write to `<project>/data/.cas/<digest><suffix>`, the **clone**. Then
pyarrow (a parquet source, whose types it keeps) or polars (a CSV, which it
parses) writes an **ordered copy**: `<compute_cache>/ordered_sources/<key>.parquet`,
holding the source's rows in file order plus a last column `__row_order`, in
row groups of 122,880 rows. The recipe reads the copy. `<key>` is an md5 of the
digest and the reader options (for a CSV, the schema and the `scan_csv`
options), so the copy's name, and with it the entry's content hash, follows
the source's content in every identity mode. Editing a source and running the
same recipe forks a new entry, and the old entry keeps the rows it was built
from (#168).

The manifest records each copy under `ordered_copies`: the source path, its
digest, the reader options, and the content digest of the copy when it was
first written. That record is what lets `ensure_materialized` make a deleted
copy again. A `<key>.digest` file next to a copy caches its content digest.

`TALLYMAN_SOURCE_IDENTITY` (default `cas`) now decides what else is recorded.
`cas` keeps the clone and records `manifest.sources`. `salt` records
`manifest.sources`, mixes the digests into the entry hash, and reads the live
file when it writes a copy. `off` records no `sources`, so the source axis of
staleness reports `unknown`, and also reads the live file. `rewrite_for_build`
no longer branches on the mode.

A recipe that calls `xo.deferred_read_parquet` on a file that is not under
`compute_cache/`, or `xo.deferred_read_csv`, is a build error: such a read gets
no digest, no clone and no ordered copy.

### Reads (`cached_result_expr` and `ensure_materialized`)

Every consumer reads an entry's result through one function,
`cached_result_expr`, whose internals are the canonical read of
`docs/system-contract.md` (#163): the entry's frozen `xorq_build/` is expanded
and loaded, and reads never re-import `expr.py`. A missing or unloadable build
is a hard error naming the entry. `cached_result_expr` calls
`ensure_materialized(project, content_hash)` and then returns:

- for a worthy entry, one bare `deferred_read_parquet` of the snapshot on the
  default backend, memoized per `(project, content_hash)`. When the file exists
  the entry's build is not loaded at all;
- for a cheap entry, the loaded build's graph rebound onto the default backend.

`ensure_materialized` guarantees that every file the entry's plan reads, and
its own snapshot, is on disk before anything executes:

1. A worthy entry whose snapshot exists is done, and no build is loaded.
2. Otherwise it loads the frozen build and collects the file each `Read` node
   points at.
3. It re-creates each missing file by the rule for its class (table below).
   When nothing can, the error names the source file.
4. For a worthy entry it then heals the entry's own snapshot, under the
   project lock and after re-checking that the file is still missing, and
   verifies it.

| File | Written by | If it is missing |
|---|---|---|
| Snapshot, `compute_cache/result_cache/<hash>.parquet` | `materialize` | re-run the entry's build, and verify the digest |
| Ordered copy, `compute_cache/ordered_sources/<key>.parquet` | pyarrow or polars, at ingest | re-run ingest on the clone with the recorded reader options, and check it |
| Clone, `data/.cas/<digest><suffix>` | `ensure_cas_path` | copy the live source again, but only while its bytes still hash to the digest |

A healed snapshot is checked against the recorded `result_digest`. A mismatch
is still served, since the rows are the honest output of the frozen build, but
never silently (`_verify_self_heal`). It logs a warning that attributes the
change: an engine version that differs from the manifest's `engine_versions`, a
recipe that re-derives a different graph hash (#88), or a fixed graph that runs
differently each time (#83). It records a durable `unfaithful_heal` error,
which also pins the file, wipes the entry's Buckaroo stat cache, and fires the
registered hooks. In the companion the hooks force Buckaroo to reload the open
grid and push an SSE event. A re-created ordered copy is checked the same way
against its recorded content digest, and a mismatch records an
`unfaithful_ordered_copy` error.

Both shapes are single-backend expressions, so two entries compose (`union`,
`join`, a diff) without tripping xorq's "multiple backends" guard. Chaining
(`tracked_expr_from_alias`, `pinned_expr_from_alias`) returns
`cached_result_expr`. A child of a worthy parent therefore reads the parent's
snapshot by its literal path, which contains the parent's content hash: the
child's identity is a function of the parent's, and does not change when the
file's bytes do. The parent's snapshot is made to exist before the child is
built, since a bare read cannot be composed over a missing file. A cheap
parent's graph is inlined instead. The child's manifest records every source
digest its parent recorded, and the parent's ordered-copy records as well when
the parent is cheap, because the child's build then reads those copies itself.
The viewer's paginated reads, diffs, and post-processing all go through
`cached_result_expr`.

### Row order (`src/tallyman_xorq/row_order.py`)

Every file tallyman writes ends in an int64 `__row_order` column holding
`0..N-1` in the file's physical row order, and every page request sorts by it:
`ORDER BY __row_order`, or the user's sort keys and then `__row_order`, so the
same request returns the same rows in any process and any cache state
(`row_order.page`, which `/api/data` uses). Buckaroo pages in its own process,
and is told the column's name with `row_order_column` in the `/load_expr`
payload. The build enforces what makes that safe:

- A cheap entry has no file of its own and pages by its parent's column, so it
  must keep it. A select that omits it fails the build with the corrected
  select in the message, and tallyman moves the column to the last position at
  the top of the expression. A worthy entry may drop it, since the writer
  numbers its rows.
- Assigning to `__row_order` is an error. A copy under another name, such as
  `__row_order_v1`, is ordinary data.
- Every `order_by` in a recipe gets `__row_order`, and then the remaining
  sortable columns, appended as tie-breakers, so a sort that feeds a `limit`
  decides the same rows on any connection. A sort that is not the last step is
  hoisted: the top-level sort leads with its keys. If a key was dropped or
  overwritten, the build fails and names it.
- Every entry carries the column, and ibis names a join's right-hand copy
  `__row_order_right`, so joining three entries in one recipe needs
  `.drop("__row_order")` on the right-hand inputs. The snapshot writer drops
  `__row_order_right`, so a join entry can be joined again. A diff drops the
  column from both sides, and the primary-key search skips it.

### Compute cache (`compute_cache_dir` in `src/tallyman_core/paths.py`)

`<project>/artifacts/catalog/compute_cache/` holds two directories of files
that tallyman writes: `result_cache/` (snapshots) and `ordered_sources/`
(ordered copies and their `.digest` files). By rule everything in it is cache:
a file lives here only if `ensure_materialized` can re-create it and check what
it made, so the cold state is an empty `compute_cache/`, and reading any entry
then re-creates every file it needs.

Files are deleted only by an explicit user action, and written only because
something is about to read them. The startup warm-up writes nothing, the verify
sweep (`catalog_scan_staleness(verify_results=True)`) reads and never writes,
and a reset leaves the directory alone. The Cache page's delete is the one
deleter. It answers 409 with the reason for a pinned snapshot, and it lists a
snapshot whose entry is no longer in the catalog as an orphan row so that it
can be deleted.

Every write takes the project's write lock (`catalog_state.project_lock`): a
build, a materialization, a heal and a checkpoint. It is a file lock on
`.checkpoint.lock`, so it holds between the MCP server and the companion, and
it is re-entrant within a thread. It covers writes only. Concurrent reads on
the shared default backend can still raise `Already borrowed` (#118).

`reset_to` (`src/tallyman_core/catalog_state.py`) returns the catalog with
`git reset --hard` and reconciles entry directories through the bullpen (the
directory a reset moves retired files into, so that a later reset forward can
bring them back), but it does not manage `compute_cache/` (ADR-007 D14, a reset
leaves `compute_cache/` alone). Snapshots are named by content hash, so one left behind by a retired
entry cannot be served for another entry: it is unreferenced disk until that
entry comes back or the user deletes it. A clone is data, the only frozen copy
of the bytes an entry was built from once the live file is edited, and
`<project>/data/.cas` lives outside the catalog git repo. So `reset_to` moves
the clones no surviving entry's `manifest.sources` refers to into
`<catalog>/bullpen/cas/` and never deletes one (`source_identity.gc_cas` with a
bullpen), and a reset forward copies them back. `compute_cache.jsonl` no
longer exists.

### In-memory caches in the companion

All bounded LRUs over immutable keys, so eviction means a cheap rebuild
and staleness is impossible:

- `cached_result_expr` — two memos. `_resolve_result_plan` is
  `lru_cache(256)` keyed `(project, content_hash)`: it holds a loaded build,
  its graph rebound onto the default backend, and the list of files the build
  reads. `_snapshot_read` is `lru_cache(1024)` and holds the bare read of a
  snapshot, so one read has one table name in the shared backend.
  `cached_result_expr.cache_clear()` clears both. Whether each file exists is
  checked on every call, inside `ensure_materialized`, since existence is the
  one input that stays mutable.
- `_build_compare_expr` — `lru_cache(128)` keyed
  `(project, a_hash, b_hash, keys)`; saves rebuilding diff outer-join
  expressions. Build dirs land under `$TMPDIR/tallyman_diff_builds/`.
- Disk-usage payload — per-project, 3-second TTL
  (`_DISK_USAGE_TTL` in `src/tallyman_companion/app.py`). The only
  time-based cache in tallyman; it coalesces filesystem walks during SSE
  bursts.

On companion startup a 3-second warmup budget loads the frozen builds of cheap
entries into the plan memo, so the first page request does not pay for the
load. A worthy entry is skipped, since reading it does not load its build, and
the warmup writes no file.

### Buckaroo integration caches (`src/tallyman_companion/buckaroo_lifecycle.py`)

Tallyman keeps no record of Buckaroo's sessions (ADR-007 D6, Buckaroo is handed
something that already exists). A **session** is one grid's state inside the
Buckaroo process, and its id is `entry-<project>-<content_hash>`
(`BuckarooManager.session_id_for`), a function of the two and nothing else, so
it is never stale. There is no session file.

- **Opening a grid** — `load_session` calls `ensure_materialized` first, so a
  failure of the computation surfaces in tallyman and never inside a grid
  query, then POSTs `/load_expr` with the derived id every time. Buckaroo
  skips the work when it already holds a session with that id and the same
  build directory and the post carries none of `component_config`,
  `column_config_overrides`, `extra_grid_config`, `init_sd` or
  `skip_stat_columns`. It creates the session again if it has dropped it, as it
  does after a session has been idle for an hour. A promoted diff entry sends
  `column_config_overrides`, so it reloads on every open.
- **What Buckaroo is handed** — for a worthy entry, a **view build**: a build
  whose whole graph is one read of the entry's snapshot, written once to
  `<entry>/.xorq_view_build/` and rebuilt if the snapshot's path changes (a
  sibling `.xorq_view_build.complete` file records the path it was made for).
  Buckaroo never executes an aggregate, join or sort on tallyman's behalf and
  never writes a snapshot. For a cheap entry, the entry's own expanded build,
  a small plan over files that exist. Both directories are stable, because
  Buckaroo's stat-cache keys include the build directory's path. The body also
  carries `row_order_column`, the name `__row_order`.
- **After an unfaithful heal** — the companion's hook wipes the entry's stat
  cache and POSTs `/load_expr` for the entry's id with `force_reload: true`, so
  an open grid does not keep stats computed from the old rows.
- **After a klass change** — a klass is a summary stat, post-processing or
  display class written for the project. `reload_project_sessions` POSTs
  `/reload_expr/<id>` for every entry of the project, treats the 404 that
  Buckaroo answers for an unknown session as "not open", and clears the stat
  cache of each grid it reloaded. That is one request per entry per change.
- **Per-entry stat cache** — `<entry>/.buckaroo_stat_cache/parquet/`, a
  `ParquetSnapshotCache` Buckaroo writes its summary stats into (the
  companion passes the path at `/load_expr` time). Deleted wholesale on
  stat reload so Buckaroo recomputes. Always on, orthogonal to whether an entry
  is worthy or cheap.
- **Diff stat cache** — `<project>/artifacts/catalog/diff_stat_cache/`
  `{a_hash[:12]}-{b_hash[:12]}/`, the same idea for a comparison session,
  keyed by the entry pair. Re-opening the same diff reuses the per-column
  stats instead of recomputing over the full join. The live diff still posts an
  unmaterialized join to Buckaroo (ADR-007 D10, tracked in #188), and its
  session ids are remembered in memory and cleared when `/health` reports a new
  `started` timestamp.

### Per-entry immutable records

Not caches in the eviction sense — immutable build outputs, valid forever
because the entry they describe never changes. `paths.py` draws the line
explicitly: `ENTRY_ARTIFACT_NAMES` (`xorq_build/`, `manifest.json`,
`schema.json`) are the immutable artifacts; the write-isolated perf overlay
symlinks them read-only — safe because nothing ever rewrites them — and omits
`ENTRY_CACHE_NAMES` (`.buckaroo_stat_cache`, `.xorq_build_expanded`,
`.xorq_view_build`) so a benchmark starts honestly cold.

The entry directory itself is gitignored. Its durable, git-tracked form is the
recipe zip `entries/<hash>.zip` — a deterministic archive of `expr.py`,
`xorq_build/`, `manifest.json`, and `schema.json`, written by the checkpoint
(`tallyman_core/catalog.py`). So the build dir is untracked-but-durable: the
recipe zip carries it across a clone, and `entries.jsonl` records which dirs
should exist so `reset_to` can reconcile them from the bullpen. (See the native
catalog store, `catalog.py` / `catalog_state.py`, for the full tracked surface.)

- **Primary key** (`src/tallyman_xorq/primary_key.py`) —
  `<entry>/primary_key.json` saves a full-table cardinality scan. A cheap
  row-preserving entry with no cached key inherits its parent version's
  key (if the columns still exist) without scanning. The search skips
  `__row_order`, which is unique in every table.
- **Portable build expansion** (`src/tallyman_xorq/portable.py`) —
  `<entry>/.xorq_build_expanded/` plus a `.complete` marker, written
  last, gates reuse; a crashed expansion redoes on next access. The
  expansion must live at a stable path, because Buckaroo's stat-cache keys
  include the build directory's path and a random tmp dir would miss them on
  every process restart. (Content-addressed but regenerable, so it is an
  `ENTRY_CACHE_NAME`, not an artifact — the overlay omits it rather than
  symlinking it.)
- **Manifest / schema** — `<entry>/manifest.json`, `<entry>/schema.json`:
  row counts, timings (including #87's cache-admission fields:
  `compile_seconds`, `cache_worthy`, `cache_worthy_why`, `cache_bytes`), the
  `result_digest` (a content digest of the snapshot, worthy entries only),
  `reproducible` and `nonreproducible_columns`, `snapshot_format` and
  `engine_versions`, the `ordered_copies` records, `sources` and `parents`.
  A worthy entry's schema is read from the file `materialize` wrote, which is
  why a `timestamp[s]` column is recorded as `timestamp[ms]`. Every entry's
  schema ends in `__row_order`. All of it is fixed at build so later reads
  don't re-walk the expression.
- **Alias history** — `<catalog>/aliases.jsonl`, one line per alias holding
  its current head and the append-only version log (`aliases.py`). It is a
  git-tracked file in the catalog repo, not a per-entry artifact, so it rolls
  back with `git reset` on a `reset_to`.
- **Per-hash config** — `<catalog>/chart_specs/<hash>.vl.json` and
  `<catalog>/display_configs/<hash>.json` hold an entry's chart and display
  config, keyed by content hash. These are mutable (set or cleared from the UI),
  not build outputs. Because the key is the hash, a revise mints a new hash and
  would orphan them, so `carry_forward_entry_config` (`entry_config.py`)
  copies each from the prior version unless the new version already defines its
  own — called from `catalog_revise` (`server.py`) and the companion's
  `put_code`. Post-processing and summary stats are not carried: they are
  project-global (keyed by name, not hash) and already apply to every version
  (#109).

## Buckaroo

### Python side

- **Summary-stats scope cache** (`buckaroo/dataflow/sd_cache.py`,
  `dataflow.py`) — the analysis-pipeline output is cached per scope
  (raw / cleaned / filtered), keyed by a blake2b hash of the canonical
  operation chain plus the sampled dataframe's identity. Flipping a
  search filter recomputes only the filtered scope; raw and cleaned ride
  cache hits. Invalidated when the op chain changes.
- **Dataframe-identity dedup** (`dataflow.py`) — skips the pipeline
  entirely when `(id(df), id(klasses))` is unchanged, suppressing
  redundant recomputes during autocleaning cascades.
- **Series-level LRU** (`pluggable_analysis_framework/utils.py`) —
  `lru_cache(256)` over per-column analysis functions (int-parse
  fractions, date detection), keyed by a content hash of the series that
  is stashed on the series object after first computation.
- **xorq count cache** (`buckaroo/xorq_buckaroo.py`) — a
  `WeakKeyDictionary` memoizing `expr.count().execute()` per expression
  object; counts against remote backends run hundreds of milliseconds.
  Entries vanish when the expression is garbage-collected, which is safe
  because ibis expressions are immutable.
- **File metadata cache** (`buckaroo/file_cache/`) — optional
  mtime-validated per-file stats cache, in-memory or SQLite-backed.

### JS side

- **SmartRowCache / KeyAwareSmartRowCache**
  (`packages/buckaroo-js-core/src/components/DFViewerParts/SmartRowCache.ts`)
  — the infinite-scroll row cache. Rows are held as merged
  `[start, end)` segments, one cache instance per
  `(source, sort, sort_direction)` key, so sorting by A, then B, then
  back to A keeps all three orderings warm. When a cache exceeds 4000
  rows it trims to a window around the last viewport; a full purge fires
  when `outside_df_params` (operations, post-processing) change. A
  leading request fires when the viewport scrolls within 300 rows of the
  cached edge, so the user never hits blank rows at scroll speed.
- **RowStore + Views** (`RowStore.ts`, `Views.ts`) — row contents live
  once in a rowid-keyed map; sort/filter views are `Int32Array`
  permutations or subsets over it, so multiple orderings don't duplicate
  row data. A view is replaced, not patched, when its sort or filter
  changes.
- **AG-Grid's infinite row model** — its block cache is purged explicitly
  (`purgeInfiniteCache()`) on sort change or ops change
  (`DFViewerInfinite.tsx`).
- **React `useMemo`** on column definitions, grid options, themes, and
  pinned rows — standard dependency-array invalidation. It matters most
  for grid options, where a new object identity triggers an expensive
  AG-Grid reconfiguration.

## Invalidation, in one view

Most of the stack never invalidates because it never can be stale: tallyman
entry hashes, snapshot paths (named by content hash), ordered copies (named by
a source's digest and its reader options), primary-key files, and manifests all
rely on "same key, same rows, forever". Cleanup for those is a space concern.
Only the user deletes a file, and a deleted file is made again and verified the
next time something reads it.

One documented hole breaks "never stale": execution nondeterminism. An entry
whose recipe calls `now()` / `random()` / an unseeded `sample()` or an impure
UDF produces different rows each run under one content hash, so a cold
recompute can disagree with what was built (#88). The build flags these as
advisory lint warnings (`_nondeterminism_warnings`, `build.py`), and a worthy
entry is also run twice when it is created, so a recipe that is not
reproducible is known from the start: the manifest records
`reproducible: false`, the build result names the columns that differed, and the
snapshot is pinned. That check cannot see `today()`, since both runs agree (the
lint does), or what an entry inherits from a non-reproducible parent, since both
runs read the same parent file. The runtime backstop is `result_digest`: every
heal verifies the repopulated snapshot against it before serving
(`_verify_self_heal`), and an unfaithful heal wipes the entry's stat cache,
records a durable error, pins the file, and forces Buckaroo to reload the open
grid. An engine or writer upgrade that changes results is attributed to the
versions in `engine_versions`, and the remedy is a corpus rebuild. The second
hole this section used to document — cold reads re-running `expr.py` and
re-digesting live sources, serving edited bytes under the original hash
(#115/#163) — is closed: reads load the frozen build, whose leaves are ordered
copies named by the source digest, bare reads of parent snapshots, and inlined
cheap parent graphs, so a cold read cannot see a post-build edit at all.

Where staleness is actually possible, it is handled explicitly:

- **mtime tracking** — Buckaroo's file cache invalidates automatically when the
  source file changes, and tallyman's source-digest memo
  (`source_identity.digest_for`) skips re-hashing a file whose mtime, size and
  inode are unchanged. Content stays the truth: the staleness scan forces a
  re-hash.
- **TTL** — xorq's `ParquetTTLStorage` (default one day) and the
  companion's 3-second disk-usage cache are the only clocks in the
  system.
- **State-change purges** — Buckaroo's op-chain key change, the JS
  `purgeInfiniteCache` on sort/ops change, the diff-session bookkeeping
  clearing on Buckaroo restart, and the stat-cache deletion on a klass reload
  or an unfaithful heal.

The one rule to remember: a cache keyed on a path does not notice upstream data
changes, so tallyman puts the content in the path. In every identity mode the
file a recipe reads is an ordered copy named by the source's digest and reader
options. An edited source therefore forks the entry hash at build instead of
deduping to the stale entry, and reads — warm, cold, or healing — resolve
through the frozen build to the copy the entry was built from, never the live
file. `cas` also keeps the clone, the only frozen copy of the bytes once the
live file is edited, which is what lets `ensure_materialized` make a deleted
ordered copy again. Under `off` and `salt` there is no clone, so a deleted copy
can be made again only while the live file still has the recorded bytes;
otherwise the error names the source file.
