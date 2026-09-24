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

Terms follow [architecture.md](architecture.md#terms). An **entry** is one
catalog computation, stored under its **content hash**, xorq's hash of the
entry's expression. Every file the expression reads is a snapshot named by the
content hash of the entry it holds, and a **source entry** (one version of an
imported file) is hashed from the md5 of its bytes and its reader options, so
the hash also covers the bytes of every input and the identity of every parent.

The dominant pattern everywhere is content-addressing: keys are derived
from immutable inputs (an expression's structure, an entry's content
hash, an imported file's digest), so entries never go stale and "invalidation"
is usually a space decision, not a correctness one. The exceptions are
called out as they appear and collected at the end. Where an open issue says
tallyman does not yet behave as described, the paragraph says so and cites it.

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
source files change. Tallyman names every file a recipe reads by a content
hash, so xorq's path-only keys are content-honest without tallyman using
either strategy; see below.

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

Tallyman writes its own result files. It decides at build time whether an entry
is worth materializing, writes the file once when the entry is created, makes
sure every file an entry reads exists before anything executes, and lets a data
file in only by an import, as a source entry whose snapshot carries a row-order
column. xorq's own cache is not in that path. ADR-007
(`plans/ADR-007-tallyman-owned-materialization.md`) records the problems that
led there: cache files landing under `~/.cache/xorq` outside the project, a
writer that races on a fixed temporary file, and a build that re-ran an
expensive parent whenever a child was built.

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
because a cheap entry pages by the `__row_order` of the one file it reads (see
Row order below), so a wrong "cheap" gives unstable paging and a wrong "worthy"
costs a copy. A UDF is matched by its base class, so a scalar UDF makes an entry
worthy without any other expensive operation (#81).

### Snapshots (`src/tallyman_xorq/materialize.py`)

A worthy entry's **snapshot** is
`<compute_cache>/result_cache/<content_hash>.parquet` (`snapshot_path`), so its
name is a function of the content hash and nothing else. `materialize` writes
the snapshot of every computed entry, used by the build and by every heal of one
(a **heal** re-creates a snapshot that is missing from disk); a source entry's
snapshot is written by the import's writer (`source_import._write_snapshot`), at
import and when it is healed from its clone (see "Source entries" below).
`materialize`:

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

A create always runs the query, and what it writes replaces whatever file is at
the path. A create of a worthy entry runs it twice through the writer and
compares the content digests of the two files (`check_reproducible`), since at
create time nothing is recorded to compare against. If they differ the build
still succeeds, and the manifest records `reproducible: false` with the columns
that differed. The file is then **pinned**: the Cache page will not delete it,
since it cannot be re-created faithfully. A heal runs the query once, replaces
the file at once, and checks the result against the recorded digest.

A create does not replace the file straight away. It calls
`materialize(..., publish=False)`, which leaves the finished file at its
temporary name, and the build moves it into place (`publish_snapshot`) as its
last step, after the manifest is written. A build that fails before then removes
only its temporary file, so a file already at the path is kept, such as one a
reset left on disk, which for an entry that is not reproducible is the only copy
of its rows (#193, fixed in #222). A source entry needs no such staging: the
import keeps a snapshot already at its path, which holds the same rows, since
the path is named by the bytes and the reader options.

**Pins.** `pinned_reason` decides from the entry's manifest and, for a source
entry, whether its clone is on disk. A snapshot is pinned when the manifest says
`reproducible: false`, when it holds `unfaithful_heal_digest` (the digest an
unfaithful heal wrote, below), or when it is a source entry whose clone is gone
(for a retired one, from both `data/.cas/` and the bullpen). Because the first
two are part of the manifest, the pin moves with the entry through a reset and
survives the error banner's dismiss, which deletes `errors.jsonl` (#196, fixed
in #223). For a file whose entry a reset retired, the manifest parked in the
bullpen speaks for it (`snapshot_manifest`), so the pin holds while the entry is
retired (#195).

The row-group size and the batch size decide the batch boundaries that an entry
built on the file sees, and an ungrouped float total depends on them (#187), so
they are part of the reproducibility contract. `SNAPSHOT_FORMAT_VERSION` stands
for both, and for the row groups of a source entry's snapshot; the manifest
records it as `snapshot_format`, next to the xorq, xorq-datafusion and pyarrow
versions in `engine_versions`. Changing any of them is a corpus rebuild.

`result_digest` in the manifest is `arrow-sha256:<hex>`, a SHA-256 over the
file's Arrow data read back (`src/tallyman_xorq/digest.py`). It does not depend
on how the rows were batched, on the codec, the row-group size, the writer's
version, or whether a text column is `string` or `large_string`. It does depend
on every value, on which slots are null, on the order of the rows, and on the
column names and types. Per-column digests name the columns that differ
between two runs.

### Source entries (`src/tallyman_xorq/source_import.py`)

A recipe never reads a data file. A file enters the catalog only through
`catalog_import_source` (`update_and_depend`), which makes it a **source
entry**, a version of a **source alias** (ADR-011,
`plans/ADR-011-sources-are-aliases.md`). The import:

- digests the file (md5) and clones its bytes, copy-on-write where the
  filesystem supports it and a plain copy elsewhere, to
  `<project>/data/.cas/<digest><suffix>`, the **clone**. The copy is digested
  again after it is written and refused if it does not match its name;
- writes the entry's snapshot, `<compute_cache>/result_cache/<content_hash>.parquet`,
  holding the file's rows in file order plus a last column `__row_order`. A
  parquet file is copied by pyarrow, so its column types survive; a CSV is parsed
  by polars under the schema and `scan_csv` options named in the import call,
  and its batches go to the same pyarrow writer. Either way the file has the
  pinned layout of every snapshot, in row groups of 122,880 rows;
- writes the entry: a generated recipe, a frozen build, a schema, and a manifest
  whose `provenance` records the outside path, the digest, the reader options and
  the name it was imported as.

The entry's content hash is `md5("source|<digest>|<reader signature>")`, cut to
12 hex characters, so it is a function of the bytes and the reader options and
nothing else. The reader options are fixed at import and must be plain values: a
callable, whose `repr` would carry a memory address, is refused. The outside path
is provenance and is never read again, so editing or deleting the original file
changes nothing. New data arrives by importing again under the same alias, which
mints the next version under a new hash; the old versions keep their rows.

A source entry is worthy, and its snapshot is cache in the sense of ADR-007 D13
(a file is cache only if it can be made again): the clone holds the bytes and the
manifest holds the reader options, so a deleted snapshot is written again from
the clone (`_heal_a_source`) and checked against the recorded `result_digest`
like any other heal. Nothing in a read writes a clone. With the clone gone, the
snapshot is the last copy of those rows, so `pinned_reason` pins it; if the
snapshot is gone as well, the read fails with an error naming the missing clone
and the `catalog_import_source` call, reader options included, that restores
it. Importing the same bytes again under the alias that holds them rewrites
nothing of the entry. When the snapshot is gone, it writes the clone back from
the given file, verified against the digest, and heals the snapshot. When the
snapshot is still there, it leaves a lost clone lost, so the version stays
pinned (#239).

Every other way of reading a file is a build error: `read_project_file`,
`tallyman_read_csv`, `xo.deferred_read_csv`, and `xo.deferred_read_parquet` of a
file outside `compute_cache/`. `read_project_file` is still called by the recipe
the importer generates, where a context variable (`_SOURCE_ENTRY`) resolves it to
the entry's own snapshot.

### Reads (`cached_result_expr` and `ensure_materialized`)

Every in-process consumer (page reads, charts, diffs, post-processing, a child
recipe) reads an entry's result through one function, `cached_result_expr`,
whose internals are the canonical read of `docs/system-contract.md` (#163): the
entry's frozen `xorq_build/` is expanded and loaded, and a read never re-imports
`expr.py`. (Only the attribution of an unfaithful heal re-imports it, as a
diagnostic.) Buckaroo's grid is handed a build instead; see "Buckaroo
integration caches" below. A missing or unloadable build is a hard error naming
the entry. `cached_result_expr` calls `ensure_materialized(project,
content_hash)` and then returns:

- for a worthy entry, one bare `deferred_read_parquet` of the snapshot on the
  default backend, memoized per `(project, content_hash)`. When the file exists
  the entry's build is not loaded at all;
- for a cheap entry, the loaded build's graph rebound onto the default backend.

`ensure_materialized` guarantees that every file the entry's plan reads, and
its own snapshot, is on disk before anything executes:

1. A worthy entry whose snapshot exists is done, and no build is loaded.
2. A source entry whose snapshot is missing is healed from its clone, and
   nothing else is needed: its own build reads the very file that is missing.
3. Otherwise it loads the frozen build and collects the file each `Read` node
   points at. Every one is another entry's snapshot, and a missing one is made
   again by recursing on the hash in its name.
4. For a worthy entry it then heals the entry's own snapshot, under the
   project lock and after re-checking that the file is still missing, and
   verifies it.

Whether an entry is worthy comes from its manifest. When the manifest is
missing (a half-built entry), a snapshot on disk stands in for the verdict, so a
worthy entry that has lost both is read as cheap: its whole build re-runs on
every read, and a child built earlier, whose build reads that snapshot, can no
longer be read (#204).

| File | Written by | If it is missing |
|---|---|---|
| Snapshot of a computed entry, `compute_cache/result_cache/<hash>.parquet` | `materialize` | re-run the entry's build, and verify the digest |
| Snapshot of a source entry, same directory | the import | parse the clone again with the recorded reader options, and verify the digest; with the clone gone too, raise an error naming the clone and the import that repairs it |
| Clone, `data/.cas/<digest><suffix>` | the import (`ensure_cas_path`) | nothing in a read makes it again; the snapshot is pinned while it exists; a re-import of the same bytes writes the clone back only when the snapshot is gone too (#239) |

A healed snapshot is checked against the recorded `result_digest`. A mismatch is
still served, since the rows are the honest output of the frozen build, but
never silently (`_verify_self_heal`). It logs a warning that attributes the
change: an engine version that differs from the manifest's `engine_versions`, a
recipe that re-derives a different graph hash (#88), or a fixed graph that runs
differently each time (#83). It records the digest it wrote in the manifest's
`unfaithful_heal_digest`, which pins the file, records a durable
`unfaithful_heal` error for the error banner, wipes the entry's Buckaroo stat
cache, and fires the registered hooks. That field is the only one written after
create: an unfaithful heal is the one thing that rewrites a manifest,
atomically, under the heal's lock. In the companion the hook posts a forced
reload of the entry's grid to Buckaroo and publishes an `unfaithful_heal` SSE
event. The SPA has no listener for that event; the error appears in the catalog
page's error banner the next time the page refetches. All of this runs while the
heal still holds the project lock, and the forced reload is posted whether or
not a grid is open, without a promoted diff's colouring (#203). Only the healed
entry is flagged: a cheap child of it reads the same snapshot, so the child's
rows change under its hash with no record and no reload (#208). The MCP server
registers no hook, so a heal that runs there records the pin and the error and
wipes the stat cache only. A source entry's snapshot made again from its clone
goes through the same check, so if a reader now parses the bytes differently,
the difference is recorded as an unfaithful heal and the manifest keeps the
digest of the rows that were imported.

Both shapes are single-backend expressions, so two entries compose (`union`,
`join`, a diff) without tripping xorq's "multiple backends" guard. Chaining
(`tracked_expr_from_alias`, `pinned_expr_from_alias`) returns
`cached_result_expr`. A child of a worthy parent therefore reads the parent's
snapshot by its literal path, which contains the parent's content hash: the
child's identity is a function of the parent's, and does not change when the
file's bytes do. The parent's snapshot is made to exist before the child is
built, since a bare read cannot be composed over a missing file. A cheap
parent's graph is inlined instead. The child's manifest records only its parent
edges: a source version is an entry, so the parent edges are the whole record of
what the child depends on (ADR-011 D6 deleted `manifest.sources`, the per-file
digest map children used to inherit). The viewer's paginated reads, diffs, and
post-processing all go through
`cached_result_expr`.

### Row order (`src/tallyman_xorq/row_order.py`)

Every file tallyman writes ends in an int64 `__row_order` column holding
`0..N-1` in the file's physical row order, and every page request sorts by it:
`ORDER BY __row_order`, or the user's sort keys and then `__row_order`, so the
same request returns the same rows in any process and any cache state
(`row_order.page`, which `/api/data` uses; that route takes no user sort yet).
Buckaroo pages the grid in its own process. Tallyman tells it the column's name
with `row_order_column` in the `/load_expr` payload, but Buckaroo 0.15.6, the
pinned version, ignores the hint, so the grid's pages are not yet ordered by it
(buckaroo-data/buckaroo#974). The build enforces what makes the rule safe:

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
  overwritten, the build fails and names it. Nested columns (lists, structs,
  maps) are left out of the tie-break, so two rows that tie on every other
  column can still come out in either order (#205).
- Every entry carries the column, and ibis names a join's right-hand copy
  `__row_order_right`, so joining three entries in one recipe needs
  `.drop("__row_order")` on the right-hand inputs. The check counts right-hand
  inputs without looking at the join kind, so a chain of semi or anti joins,
  which adds no right-hand columns, is refused too (#199). The snapshot writer
  drops `__row_order_right`, so a join entry can be joined again; it drops any
  column of that name, including one the author made, and a join with a
  non-default `rname` leaves its own collision column behind (#206).
- The diff grid's compare expression drops the column from both sides, but
  `full_diff`, which computes the diff page's summaries and `catalog_diff`'s
  reply, keeps it as a data column (#200). The primary-key search skips it.

### Compute cache (`compute_cache_dir` in `src/tallyman_core/paths.py`)

`<project>/artifacts/catalog/compute_cache/` holds one directory of files that
tallyman writes: `result_cache/`, the snapshots of worthy entries, source
entries included. By rule everything in it is cache: a file lives here only if
`ensure_materialized` can re-create it and check what it made, so the cold state
is an empty `compute_cache/`, and reading any entry then re-creates every file
it needs. Three kinds of snapshot are exceptions, and are pinned (see "Pins"
above): the snapshot of an entry recorded as not reproducible, which cannot be
made again faithfully; one whose heal already produced different rows than were
built (`unfaithful_heal_digest`); and the snapshot of a source entry whose clone
is gone, which is the last copy of the imported rows. A pin protects the file
from the Cache page and from nothing else (#185).

Files are deleted only by an explicit user action, and written only because
something is about to read them. The startup warm-up writes nothing here, the
verify sweep (`catalog_scan_staleness(verify_results=True)`) reads and never
writes, and a reset leaves the directory alone. The Cache page's delete is the
one deleter. The page answers 409 with the reason for a pinned snapshot. It
lists a snapshot whose entry a reset retired as a `retired` row, whose pin comes
from the manifest parked in the bullpen, and a snapshot that no entry names,
live or retired, as an `orphan` row, so that the user can delete either. A
source entry's snapshot is listed like any other.

The project's write lock (`catalog_state.project_lock`) is taken by a build
(for its whole length), an import, a materialization or heal, a checkpoint and
a reset. Other writes take no lock: alias and notebook updates,
chart and display configs and `config.json` each replace a whole file
atomically, the logs append a line, and the Cache page's delete unlinks the
file. The lock is a file
lock on `.checkpoint.lock`, so it holds between the MCP server and the
companion, and it is re-entrant within a thread. It blocks with no timeout: a
page read whose entry needs a heal waits behind any build in the other process
(#186), and the companion's `PUT /code` and `POST /promote_diff` routes build on
the event loop, so the whole UI stops answering while they wait or build (#190).
It covers no reads. Concurrent reads on the shared default backend can still
raise `Already borrowed` (#118).

`reset_to` (`src/tallyman_core/catalog_state.py`) returns the catalog with
`git reset --hard` and reconciles entry directories through the bullpen (the
directory a reset moves retired files into, so that a later reset forward can
bring them back), but it does not manage `compute_cache/` (ADR-007 D14, a
reset leaves `compute_cache/` alone). Snapshots are named by content hash, so
one left behind by a retired entry cannot be served for another entry: it is
unreferenced disk until that entry comes back or the user deletes it. A clone
is data, the only copy tallyman has of bytes it imported (the outside file is
never read again), and `<project>/data/.cas` lives outside the catalog git repo.
So `reset_to` moves the clones that no surviving source entry's
`manifest.provenance` names into `<catalog>/bullpen/cas/` and never deletes one
(`source_identity.gc_cas` with a bullpen), and a reset forward copies back the
clones a restored entry names. A reset that cannot read every surviving
manifest skips the sweep. `compute_cache.jsonl` no longer exists.

When a reset retires an entry whose directory the bullpen already holds (an
entry retired once, restored, and retired again), the live directory replaces
the parked one, since it is the one that agrees with the snapshot on disk: a
create always rewrites the snapshot, and an entry that is not reproducible
records another `result_digest` each time (#194, fixed in #223). A live
directory with no manifest, which an interrupted build leaves, never replaces a
parked one and is dropped instead. The bullpen has one live reader besides
`reset_to`: the Cache page reads a retired entry's parked manifest to decide its
snapshot's pin, and for a retired source version it counts a clone parked in
`bullpen/cas/` as present, since a reset forward brings the entry and the clone
back together (#195).

### In-memory caches in the companion

The first two are bounded LRUs over immutable keys, so eviction means a cheap
rebuild and staleness is impossible; the third is a short TTL:

- `cached_result_expr` — two memos. `_resolve_result_plan` is
  `lru_cache(256)` keyed `(project, content_hash)`: it holds a loaded build,
  its graph rebound onto the default backend, and the list of files the build
  reads. `_snapshot_read` is `lru_cache(1024)` and holds the bare read of a
  snapshot, so one read has one table name in the shared backend.
  `cached_result_expr.cache_clear()` clears both. Whether each file exists is
  checked on every call, inside `ensure_materialized`, since existence is the
  one input that stays mutable. The plan memo also keeps the build as it was
  loaded, which nothing reads again, so up to 256 unused DataFusion backends
  stay in memory (#210). The MCP server has the same two memos.
- `_build_compare_expr` — `lru_cache(128)` keyed
  `(project, a_hash, b_hash, keys)`; saves rebuilding diff outer-join
  expressions. Build dirs land under `$TMPDIR/tallyman_diff_builds/`.
- Disk-usage payload — per-project, 3-second TTL
  (`_DISK_USAGE_TTL` in `src/tallyman_companion/app.py`). The only
  time-based cache in tallyman; it coalesces filesystem walks during SSE
  bursts.

On companion startup a 3-second warmup budget loads the frozen builds of the
active project's cheap entries into the plan memo, so the first page request
does not pay for the load. A worthy entry is skipped, since reading it does not
load its build. The warmup writes nothing under `compute_cache/`; loading a
cheap entry's build can write its `.xorq_build_expanded/`.

### Buckaroo integration caches (`src/tallyman_companion/buckaroo_lifecycle.py`)

Tallyman keeps no record of Buckaroo's sessions (ADR-007 D6, Buckaroo is handed
something that already exists). A **session** is one grid's state inside the
Buckaroo process, and its id is `entry-<project>-<content_hash>`
(`BuckarooManager.session_id_for`), a function of the two and nothing else, so
it is never stale. There is no session file.

- **Opening a grid**: `load_session` calls `ensure_materialized` first, so a
  failure of the computation surfaces in tallyman and never inside a grid
  query, then POSTs `/load_expr` with the derived id every time. Buckaroo
  skips the work when it already holds a session with that id and the same
  build directory and the post carries none of `component_config`,
  `column_config_overrides`, `extra_grid_config`, `init_sd` or
  `skip_stat_columns`. It creates the session again if it has dropped it, as it
  does after a session has been idle for an hour. Nothing coalesces two opens
  of one entry, so opens that overlap both post and both run Buckaroo's
  pipeline; a promoted diff entry sends `column_config_overrides`, so it
  re-runs the pipeline on every open; and the notebook page's data route posts
  for every cell on every page load (#202).
- **What Buckaroo is handed**: for a worthy entry, a **view build**, a build
  whose whole graph is one read of the entry's snapshot, written once to
  `<entry>/.xorq_view_build/` and rebuilt if the snapshot's path changes (a
  sibling `.xorq_view_build.complete` file records the path it was made for).
  Buckaroo never executes an aggregate, join or sort on tallyman's behalf and
  never writes a snapshot. For a cheap entry, the entry's own expanded build,
  a small plan over files that exist. Both directories are stable per-entry
  paths, so Buckaroo is handed the same build after a restart, which is what its
  on-disk stat cache relies on. The body also
  carries `row_order_column`, the name `__row_order`, which Buckaroo 0.15.6
  ignores, and `project_root`, where Buckaroo looks for the project's `stats/`,
  `post_processing/` and `display/` klasses. Tallyman sends `artifacts/`,
  which holds `display/`, but it writes stats and post-processing functions
  under `artifacts/catalog/`, so Buckaroo does not find them (#170).
- **After an unfaithful heal**: `_verify_self_heal` wipes the entry's stat
  cache, and the companion's hook POSTs `/load_expr` for the entry's id with
  `force_reload: true`, so an open grid does not keep stats computed from the
  old rows. The post goes out whether or not a grid is open, which opens a
  session nobody asked for, and it carries no column colouring (#203).
- **After a klass change**: a klass is a summary stat, post-processing or
  display class written for the project. `reload_project_sessions` POSTs
  `/reload_expr/<id>` for every entry of the project, treats the 404 (or 400)
  that Buckaroo answers for an unknown session as "not open", and clears the
  stat cache of each grid it reloaded. That is one request per entry per
  change, sent one after another from the companion's event loop (#201), and
  the stat-cache wipe after each reload is more than a klass change needs
  (#177). A reset or a recalc that moved an alias reloads sessions the same way.
- **Per-entry stat cache**: `<entry>/.buckaroo_stat_cache/parquet/`, a
  `ParquetSnapshotCache` Buckaroo writes its summary stats into (the
  companion passes the path at `/load_expr` time). Deleted wholesale on
  stat reload so Buckaroo recomputes. Always on, orthogonal to whether an entry
  is worthy or cheap. In practice a first load after a Buckaroo restart has not
  been seen to hit it (#157).
- **Diff stat cache**: `<project>/artifacts/catalog/diff_stat_cache/`
  `{a_hash[:12]}-{b_hash[:12]}/`, the same idea for a comparison session,
  keyed by the entry pair. Re-opening the same diff reuses the per-column
  stats instead of recomputing over the full join. A reset or a recalc that
  moved an alias deletes the whole directory. The live diff still posts an
  unmaterialized join to Buckaroo (ADR-007 D10, which would have built every
  diff as an entry first, was moved to #188). Its session ids,
  `diff-<a[:12]>-<b[:12]>`, are remembered in the companion's memory and
  forgotten when the companion starts a Buckaroo whose `/health` reports a new
  `started` timestamp.

### Per-entry immutable records

Not caches in the eviction sense — build outputs, valid forever because the
entry they describe never changes. `paths.py` draws the line explicitly:
`ENTRY_ARTIFACT_NAMES` (`xorq_build/`, `manifest.json`, `schema.json`) are the
artifacts; the write-isolated perf overlay symlinks them read-only — safe
because the only later write, an unfaithful heal recording
`unfaithful_heal_digest` in `manifest.json`, is an atomic replace that swaps
the overlay's link for a file and leaves the original alone — and omits
`ENTRY_CACHE_NAMES` (`.buckaroo_stat_cache`, `.xorq_build_expanded`,
`.xorq_view_build`) so a benchmark starts honestly cold.

The entry directory itself is gitignored. What the catalog repository tracks for
it is the recipe zip `entries/<hash>.zip`, a deterministic archive of `expr.py`,
`xorq_build/`, `manifest.json`, and `schema.json`, written once, by the first
checkpoint after create (`tallyman_core/catalog.py`), so its manifest does not
carry an `unfaithful_heal_digest` recorded later. Nothing reads the zip back
(`catalog.py`): it records what a checkpoint committed, and a clone of the
catalog repository does not recreate entry directories from it. `entries.jsonl`
records which dirs should exist so `reset_to` can reconcile them from the
bullpen. (See the native catalog store, `catalog.py` / `catalog_state.py`, for
the full tracked surface.)

- **Primary key** (`src/tallyman_xorq/primary_key.py`) —
  `<entry>/primary_key.json` saves a full-table cardinality scan. A cheap
  row-preserving entry with no cached key inherits its parent version's
  key (if the columns still exist) without scanning. The search skips
  `__row_order`, which is unique in every table.
- **Portable build expansion** (`src/tallyman_xorq/portable.py`) —
  `<entry>/.xorq_build_expanded/` plus a `.complete` marker, written
  last, gates reuse; a crashed expansion redoes on next access. The
  expansion lives at a stable path so that Buckaroo is handed the same build
  directory after every process restart, which its on-disk stat cache was
  meant to rely on (#157). (Content-addressed but regenerable, so it is an
  `ENTRY_CACHE_NAME`, not an artifact — the overlay omits it rather than
  symlinking it.) The marker does not record which project path the build was
  expanded with, so a project copied together with these directories keeps
  reading the old location, and fails once that is gone (#209). A worthy
  entry's build is first expanded at create, when `materialize` loads it; a
  cheap entry's the first time something loads it (a read, the grid, a child's
  build, the companion's startup warm-up).
- **Manifest / schema** — `<entry>/manifest.json`, `<entry>/schema.json`:
  row counts, timings (including #87's cache-admission fields:
  `compile_seconds`, `cache_worthy`, `cache_worthy_why`, `cache_bytes`), the
  `result_digest` (a content digest of the snapshot, worthy entries only),
  `reproducible` and `nonreproducible_columns`, `snapshot_format` and
  `engine_versions`, `parents`, `unfaithful_heal_digest` once an unfaithful
  heal has pinned the file, and on a source entry `provenance` (where the file
  came from, its digest, the reader options and the name it was imported as).
  A worthy entry's schema is read from the file `materialize` wrote, which is
  why a `timestamp[s]` column is recorded as `timestamp[ms]`. Every entry's
  schema ends in `__row_order`. All of it is fixed at build so later reads
  don't re-walk the expression.
- **Alias history** — `<catalog>/aliases.jsonl`, one line per alias holding
  its current head, the append-only version log and its kind, `catalog` or
  `source` (`aliases.py`). It is a
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
entry hashes (a source entry's named by its bytes and reader options), snapshot
paths (named by content hash), primary-key files, and manifests all rely on
"same key, same rows, forever". Cleanup for those is a space concern.
Only the user deletes a file, and a deleted file is made again and verified
the next time something reads it.

One documented hole breaks "never stale": execution nondeterminism. An entry
whose recipe calls `now()` / `random()` / an unseeded `sample()` or an impure
UDF produces different rows each run under one content hash, so a cold recompute
can disagree with what was built (#88). The build flags `now()`, `today()`,
`random()`, `uuid()` and an unseeded `sample()` as advisory lint warnings
(`_nondeterminism_warnings`, `build.py`); an impure UDF is not flagged, since
purity cannot be read off the graph. A worthy entry is also run twice when it is
created, so a recipe that is not reproducible is known from the start: the
manifest records `reproducible: false`, the build result names the columns that
differed, and the snapshot is pinned. That check cannot see `today()`, since
both runs agree (the lint does), or what an entry inherits from a
non-reproducible parent, since both runs read the same parent file (#185). The
runtime backstop is `result_digest`: every heal verifies the repopulated
snapshot against it before serving (`_verify_self_heal`), and an unfaithful heal
pins the file in its manifest, wipes the entry's stat cache, records a durable
error, and, in the companion, forces Buckaroo to reload the entry's grid. It
does not reach the cheap entries that read the healed snapshot, whose rows
change with it (#208). An engine or writer upgrade that changes results is
attributed to the versions in `engine_versions`, and the remedy is a corpus
rebuild. The second hole this section used to document — cold reads re-running
`expr.py` and re-digesting live sources, serving edited bytes under the original
hash (#115/#163) — is closed: reads load the frozen build, whose leaves are bare
reads of snapshots named by content hash and inlined cheap parent graphs, and a
data file enters only by an import, so a cold read cannot see a post-build edit
at all.

Where staleness is actually possible, it is handled explicitly:

- **mtime tracking** — Buckaroo's file cache invalidates automatically when the
  source file changes. Tallyman keeps no stat-keyed memo: a file is digested
  once, when it is imported, and the staleness scan reads no file at all.
- **TTL** — xorq's `ParquetTTLStorage` (default one day), the
  companion's 3-second disk-usage cache, and Buckaroo's eviction of a session
  idle for an hour are the only clocks in the system.
- **State-change purges** — Buckaroo's op-chain key change, the JS
  `purgeInfiniteCache` on sort/ops change, the diff-session bookkeeping
  clearing on Buckaroo restart, the stat-cache deletion on a klass reload
  or an unfaithful heal, and the deletion of `diff_stat_cache/` on a reset or
  a recalc.

The one rule to remember: a cache keyed on a path does not notice upstream data
changes, so tallyman puts the content in the path. Every file a recipe reads is
a snapshot named by a content hash, and a source entry's hash is a function of
the bytes it imported and the reader options. New data arrives only as an
import, which mints a new source version under a new hash and makes the entries
following that alias stale, and reads — warm, cold, or healing — resolve through
the frozen build to the snapshot the entry was built from, never the outside
file. The clone is what lets `ensure_materialized` make a deleted source
snapshot again; with the clone gone the snapshot is pinned, and once both are
gone the error names the clone and the import that restores it.
