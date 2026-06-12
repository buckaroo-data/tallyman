# Caching across the stack

Tallyman sits on two other projects that each cache aggressively: xorq
(deferred expression execution) and Buckaroo (the dataframe viewer). This
doc maps every cache in the three layers — what work each one saves, what
its key is, and when it is invalidated. Bottom-up: xorq first, since
tallyman's persistence is built on it.

The dominant pattern everywhere is content-addressing: keys are derived
from immutable inputs (an expression's structure, an entry's content
hash), so entries never go stale and "invalidation" is usually a space
decision, not a correctness one. The exceptions are called out as they
appear and collected at the end.

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
source files change. Tallyman uses snapshot everywhere and gets away with
it because catalog entries are immutable by construction; see below.

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

Tallyman uses xorq's cache machinery wholesale — strategy, key
computation, hit/miss logic, atomic writes are all xorq's. The only
override is *where* the files land: every cache constructor is passed a
`base_path`/`cache_dir` inside the project's catalog directory instead of
the global `~/.cache/xorq`. Per-project placement makes the cache travel
with the project and lets `reset-to` manage it; a shared global directory
would break both the isolation and the reset semantics.

### Result cache (`src/tallyman_xorq/result_cache.py`)

Each catalog entry's expression is classified by `classify_build`, which
reads the serialized build (no execution) and looks for expensive ops:

- **Expensive** — contains an Aggregate / Join / Sort / window / UDF, or
  reads a non-parquet source. The expression is wrapped in a
  `ParquetSnapshotCache` rooted at
  `<project>/artifacts/catalog/result_cache/`. A hit reads the cached
  parquet; a miss (including a manually deleted file) self-heals by
  recomputing from the build.
- **Cheap** — a parquet read plus projections / renames / row-wise scalar
  math. No extra cache node: recompute costs about the same as reading a
  copy, so the entry's in-place `result.parquet` is used and regenerated
  from the build if missing.

Snapshot strategy is the right one here because an entry is an immutable,
content-addressed artifact — its result must not invalidate just because
an upstream file's mtime drifts. No TTL: entries are permanent history,
not expiring scratch.

### Compute cache (`compute_cache_dir` in `src/tallyman_core/paths.py`)

`<project>/artifacts/catalog/compute_cache/` holds xorq's sub-expression
results, seeded at build time (`src/tallyman_xorq/build.py` passes
`cache_dir=compute_cache_dir(project)` to `load_expr`). Its distinguishing
feature is reset-awareness: `catalog.yaml` records which cache files were
warm at each step, and `reset_to` (`src/tallyman_core/catalog_state.py`)
prunes or restores the cache to match the target step. Evicted files move
to `bullpen/` rather than being deleted, so a forward reset restores them
instead of recomputing, while a re-added entry still computes honestly
cold.

### In-memory caches in the companion

All bounded LRUs over immutable keys, so eviction means a cheap rebuild
and staleness is impossible:

- `cached_result_expr` — `lru_cache(256)` keyed `(project, content_hash)`;
  saves re-parsing expression YAML from disk
  (`src/tallyman_xorq/result_cache.py`).
- `_build_compare_expr` — `lru_cache(128)` keyed
  `(project, a_hash, b_hash, keys)`; saves rebuilding diff outer-join
  expressions (`src/tallyman_companion/app.py`). Build dirs land under
  `$TMPDIR/tallyman_diff_builds/`.
- `result_cache` — `lru_cache(16)`; one xorq connection per project per
  process instead of one per call.
- Disk-usage payload — per-project, 3-second TTL
  (`_DISK_USAGE_TTL` in `src/tallyman_companion/app.py`). The only
  time-based cache in tallyman; it coalesces filesystem walks during SSE
  bursts.

On companion startup, a 3-second warmup budget pre-populates
`cached_result_expr` for as many entries as fit; the rest warm lazily.

### Buckaroo integration caches (`src/tallyman_companion/buckaroo_lifecycle.py`)

- **Session map** — `buckaroo_sessions.json` in the tallyman home
  (`TALLYMAN_HOME`, default `~/.tallyman-notebooks/`) maps content hash to
  Buckaroo session id, saving a `/load_expr` POST per entry per restart.
  Invalidated when Buckaroo's `/health` reports a new `started_at`
  (restart detected, map cleared) or when an entry's parquet is evicted.
- **Per-entry stat cache** — `<entry>/.buckaroo_stat_cache/parquet/`, a
  `ParquetSnapshotCache` Buckaroo writes its summary stats into (the
  companion passes the path at `/load_expr` time). Deleted wholesale on
  stat reload so Buckaroo recomputes. Always on, orthogonal to the
  result-cache worthiness rubric.
- **Diff stat cache** — `<project>/artifacts/catalog/diff_stat_cache/`
  `{a_hash[:12]}-{b_hash[:12]}/`, the same idea for a comparison session,
  keyed by the entry pair. Re-opening the same diff reuses the per-column
  stats instead of recomputing over the full join.

### Per-entry immutable records

Not caches in the eviction sense — records that are valid forever because
the entry they describe never changes:

- **Primary key** (`src/tallyman_xorq/primary_key.py`) —
  `<entry>/primary_key.json` saves a full-table cardinality scan. A cheap
  row-preserving entry with no cached key inherits its parent version's
  key (if the columns still exist) without scanning.
- **Portable build expansion** (`src/tallyman_xorq/portable.py`) —
  `<entry>/.xorq_build_expanded/` plus a `.complete` marker, written
  last, gates reuse; a crashed expansion redoes on next access. The
  expansion must live at a stable path: xorq's snapshot key includes the
  read path, so a random tmp dir would change the expression hash and
  bust the result cache on every process restart.
- **Manifest / schema / alias history** — `manifest.json`,
  `schema.json`, `aliases.json` + `alias_history.json`: row counts,
  timings, schemas, and the append-only alias version log. They save
  re-reading parquet metadata and re-deriving version numbers.

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

Most of the stack never invalidates because it never can be stale: xorq
snapshot keys, tallyman entry hashes, primary-key files, and manifests
all rely on "same key, same bytes, forever". Cleanup for those is a
space concern (manual delete, bullpen moves on reset), and a deleted file
self-heals by recomputing.

Where staleness is actually possible, it is handled explicitly:

- **mtime tracking** — xorq's modification-time strategy and Buckaroo's
  file cache invalidate automatically when the source file changes.
- **TTL** — xorq's `ParquetTTLStorage` (default one day) and the
  companion's 3-second disk-usage cache are the only clocks in the
  system.
- **State-change purges** — Buckaroo's op-chain key change, the JS
  `purgeInfiniteCache` on sort/ops change, the session map clearing on
  Buckaroo restart, and the stat-cache deletion on reload.

The one rule to remember: snapshot-strategy caches do not notice upstream
data changes. Tallyman is safe because entries are immutable by
construction; anyone pointing xorq's snapshot caches at mutable source
files has to manage invalidation themselves.
