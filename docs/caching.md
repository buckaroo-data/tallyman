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

### Result cache (`src/tallyman_xorq/result_cache.py`, `source_cache.py`)

Tallyman decides what to materialize at *build* time and bakes the decision
into the entry's recipe, so every later read takes the same path. Before a
build, `rewrite_for_build` (`source_cache.py:89`) rewrites the submitted
expression in three steps: reject in-memory reads, inject a source-read cache
after each non-parquet file read, and — for an expensive expression — wrap the
whole thing in a top-level result cache. Both kinds of cache node are
`ParquetSnapshotCache`s whose storage resolves to the per-project compute
cache at load time (xorq's `load_expr(cache_dir=…)` rewrites every node's base
path), so there is no separate `result_cache/` directory; both land under
`<project>/artifacts/catalog/compute_cache/`.

- **Source-read cache** — every non-parquet file read (`read_csv`,
  `read_json`) gets a `.cache()` node injected immediately downstream
  (`source_cache.py:142`). The snapshot key is the read's path only, so every
  entry reading the same source hits the same cached parquet. Always on,
  independent of result-cache worthiness. `read_parquet` / `read_delta` are
  exempt (`_EXEMPT_READS`): re-reading a columnar source is already a pushdown.
- **Baked result snapshot** — an expression is *worthy* when it contains an
  Aggregate / Join / Sort / window / UDF (`_EXPENSIVE_OPS`,
  `result_cache.py:49`). A worthy expression is first put in a canonical total
  order (`_canonical_sorted`: the author's own `order_by` keys, then
  `original_row_order`, then the remaining sortable columns — ADR D5, amended)
  so the baked bytes are deterministic across the build and every later heal,
  then wrapped in a result cache with `relative_path="result_cache"`, so its
  snapshot lands under `compute_cache/result_cache/`; executing the entry at
  build time materializes it once. A non-parquet read is *not* by itself
  worthy (`classify_build`, `result_cache.py:62`) — its parse is already
  handled by the source-read cache. Worthiness is decided by two predicates that
  must agree: `_is_worthy_expr` gates the bake (`source_cache.py:62`) and
  `classify_build` records `cache_worthy` in the manifest. `_is_worthy_expr`
  matches a UDF by class ancestry (`type(node).__mro__`, `source_cache.py:84`)
  and `classify_build` by the serialized op name containing `UDF`
  (`result_cache.py:85`; a scalar UDF serializes as `op: ScalarUDF`); the two now
  agree for a scalar UDF, so a scalar-UDF-only entry is worthy under both and
  bakes its snapshot instead of recomputing the whole graph on every read (#81).

A cheap entry — a source read plus projections / renames / row-wise scalar
math — bakes no result snapshot. Recompute costs about the same as reading a
copy (a pushdown over a columnar source, or over its already-cached parse), so
an extra copy would burn storage for no savings. No `result.parquet` is
written for any entry, cheap or expensive, at build time or on demand (#73 and
follow-up).

Every consumer reads an entry's result through one function,
`cached_result_expr`, whose internals are the canonical read of
`docs/system-contract.md` (#163): expand the entry's frozen `xorq_build/`,
`load_expr(expanded, cache_dir=compute_cache)`, then the deep cache-dir
rewrite (`portable.rewrite_cache_dirs`). A missing or unloadable build is a
hard error naming the entry — reads never re-import `expr.py` (ADR D6):

- Expensive entry → `deferred_read_parquet` of the baked snapshot, on the
  default backend. The snapshot was written when the entry built, so reading it
  skips re-running the graph; the read asserts its derived snapshot key matches
  the one the manifest recorded (ADR D8). If the file was evicted since, the
  *frozen build* is executed once and the read proceeds — a self-heal
  re-checked on every call, single-flighted per `(project, content_hash)` so
  concurrent cold readers don't both run the shared op; the healed bytes are
  verified against the recorded `result_digest` before they are served, and a
  mismatch wipes the entry's Buckaroo stat cache, records a durable
  `unfaithful_heal` error, and evicts its Buckaroo session (`_verify_self_heal`;
  ADR D7/D10/D12, #79, #83).
- Cheap entry → the loaded build's graph, rebound onto the default backend and
  recomputed on read from its content-pinned sources.

Either shape is a single-backend expression, so two entries compose (`union`,
`join`, a diff) without tripping xorq's "multiple backends" guard. Chaining
(`tracked_expr_from_alias`) uses the sibling `entry_graph_expr` instead: the
parent's cache node stays in the graph, so a child's build is self-contained
and self-healing (ADR D4). The viewer's paginated reads, diffs, and
post-processing all go through `cached_result_expr`; nothing reads a
pre-existing `result.parquet`, because none is written.

Snapshot strategy is the right one here because an entry is an immutable,
content-addressed artifact — its result must not invalidate just because an
upstream file's mtime drifts. No TTL: entries are permanent history, not
expiring scratch.

How a source file's content reaches the key is set by `TALLYMAN_SOURCE_IDENTITY`
(`source_identity.py:52`). The default is `cas`: `read_project_file` reads through a
content-addressed clone at `<project>/data/.cas/<digest>` (`io.py:58`), so the
path xorq tokenizes embeds the content digest and every xorq-level key — build
hash and snapshot keys alike — is content-honest, and a rebuild over an edited
source forks the hash instead of deduping to the stale entry. `off` is the
historical path-only mode: no digest, no clone, an edit collides with the prior
entry. `salt` is the remaining exception to baking: it folds source digests into
the entry hash but leaves xorq's own keys path-only, so a baked snapshot's
path-only key would collide across entries with identical paths but different
content. Under salt, `rewrite_for_build` bakes no cache at all
(`source_cache.py:130`) and every read recomputes.

### Compute cache (`compute_cache_dir` in `src/tallyman_core/paths.py`)

`<project>/artifacts/catalog/compute_cache/` is where every cache node's
storage resolves at load time, so it holds three things: the source-read
parses, the baked result snapshots (under `result_cache/`), and any
sub-expression results xorq caches along the way. Build and view both load
against it (`build.py` and `load_entry` pass `cache_dir=compute_cache_dir`), so
a freshly added expression computes there cold and warms it for later reads.

Its distinguishing feature is reset-awareness. The warm set is recorded in
`compute_cache.jsonl` (one `{"path": relpath}` per line), a git-tracked pointer
file captured at each checkpoint; `reset_to`
(`src/tallyman_core/catalog_state.py:327`) prunes or restores the cache to
match the target step. Evicted files move to `bullpen/` rather than being
deleted, so a forward reset restores them instead of recomputing, while a
re-added entry still computes honestly cold. `reset_to` also reclaims orphaned
`cas` clones: `<project>/data/.cas` lives outside the catalog git repo, so
`git reset` can't touch it, and `_gc_cas_clones` (`catalog_state.py:349`,
`source_identity.gc_cas`) deletes any `.cas` file no surviving entry's
`manifest.sources` still references (#86).

### In-memory caches in the companion

All bounded LRUs over immutable keys, so eviction means a cheap rebuild
and staleness is impossible:

- `cached_result_expr` — its build-loading work is memoized on
  `_resolve_result_plan`, `lru_cache(256)` keyed `(project, content_hash)`;
  `cached_result_expr` re-exposes that memo's `cache_clear` / `cache_info`.
  The key finally determines the value — the plan is a pure function of the
  immutable build — and the memo saves expanding + loading the build and
  re-deriving its baked-snapshot path. `cached_result_expr` itself is a thin
  wrapper that re-checks the snapshot's on-disk presence on every call, so an
  evicted snapshot self-heals; the heal runs under a per-entry lock
  (`_heal_lock`) so only the first of several concurrent cold readers executes
  it, and a peer process that wins xorq's fixed-`.tmp` rename is caught and
  retried once if the snapshot landed, else re-raised (#79).
- `_build_compare_expr` — `lru_cache(128)` keyed
  `(project, a_hash, b_hash, keys)`; saves rebuilding diff outer-join
  expressions (`src/tallyman_companion/app.py:189`). Build dirs land under
  `$TMPDIR/tallyman_diff_builds/`.
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
  Invalidated when Buckaroo's `/health` reports a new `started` timestamp
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

Not caches in the eviction sense — immutable build outputs, valid forever
because the entry they describe never changes. `paths.py:99` draws the line
explicitly: `ENTRY_ARTIFACT_NAMES` (`xorq_build/`, `manifest.json`,
`schema.json`) are the immutable artifacts; the write-isolated perf overlay
symlinks them read-only — safe because nothing ever rewrites them — and omits
`ENTRY_CACHE_NAMES` (`.buckaroo_stat_cache`, `.xorq_build_expanded`) so a
benchmark starts honestly cold.

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
  key (if the columns still exist) without scanning.
- **Portable build expansion** (`src/tallyman_xorq/portable.py`) —
  `<entry>/.xorq_build_expanded/` plus a `.complete` marker, written
  last, gates reuse; a crashed expansion redoes on next access. The
  expansion must live at a stable path: xorq's snapshot key includes the
  read path, so a random tmp dir would change the expression hash and
  bust the result cache on every process restart. (Content-addressed but
  regenerable, so it is an `ENTRY_CACHE_NAME`, not an artifact — the overlay
  omits it rather than symlinking it.)
- **Manifest / schema** — `<entry>/manifest.json`, `<entry>/schema.json`:
  row counts, timings (including #87's cache-admission fields:
  `compile_seconds`, `cache_worthy`, `cache_bytes`), the #83 `result_digest`
  (a content hash of the executed result bytes), and the schema, all
  derived from the expression at build so later reads don't re-walk it.
- **Alias history** — `<catalog>/aliases.jsonl`, one line per alias holding
  its current head and the append-only version log (`aliases.py`). It is a
  git-tracked file in the catalog repo, not a per-entry artifact, so it rolls
  back with `git reset` on a `reset_to`.
- **Per-hash config** — `<catalog>/chart_specs/<hash>.vl.json` and
  `<catalog>/display_configs/<hash>.json` hold an entry's chart and display
  config, keyed by content hash. These are mutable (set or cleared from the UI),
  not build outputs. Because the key is the hash, a revise mints a new hash and
  would orphan them, so `carry_forward_entry_config` (`entry_config.py:21`)
  copies each from the prior version unless the new version already defines its
  own — called from `catalog_revise` (`server.py:572`) and the companion's
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

Most of the stack never invalidates because it never can be stale: xorq
snapshot keys, tallyman entry hashes, primary-key files, and manifests
all rely on "same key, same bytes, forever". Cleanup for those is a
space concern (manual delete, bullpen moves on reset), and a deleted file
self-heals by recomputing.

One documented hole breaks "never stale": execution nondeterminism. An entry
whose recipe calls `now()` / `random()` / an unseeded `sample()` or an impure
UDF produces different bytes each run under one content hash, so a cold
recompute can disagree with what was built (#88). The build flags these as
advisory lint warnings (`_nondeterminism_warnings`, `build.py`); it does not
block them. The runtime backstop is `result_digest`: every self-heal verifies
the repopulated snapshot against it before serving (`_verify_self_heal`), and
an unfaithful heal wipes the entry's stat cache, records a durable error, and
evicts its Buckaroo session (ADR D7/D10/D12). The second hole this section
used to document — cold reads re-running `expr.py` and re-digesting live
sources, serving edited bytes under the original hash (#115/#163) — is closed:
reads load the frozen build, whose leaves are content-pinned `.cas` clones and
inlined parent graphs, so a cold read cannot see a post-build edit at all.

Where staleness is actually possible, it is handled explicitly:

- **mtime tracking** — xorq's modification-time strategy and Buckaroo's
  file cache invalidate automatically when the source file changes.
- **TTL** — xorq's `ParquetTTLStorage` (default one day) and the
  companion's 3-second disk-usage cache are the only clocks in the
  system.
- **State-change purges** — Buckaroo's op-chain key change, the JS
  `purgeInfiniteCache` on sort/ops change, the session map clearing on
  Buckaroo restart, and the stat-cache deletion on reload.

The one rule to remember: snapshot-strategy caches do not notice upstream data
changes. The `cas` default makes that safe end-to-end: an edited source forks
the entry hash at build rather than deduping to the stale one, and reads —
warm, cold, or self-healing — resolve through the frozen build to the
content-addressed clone the entry was built from, never the live file. `off`
reverts to xorq's path-only identity (an edit collides silently, and a loaded
build reads whatever bytes sit at the recorded path); `salt` mixes digests
into the entry hash but bakes no snapshot cache at all (path-only keys would
collide across salted entries) and recomputes on every read.
