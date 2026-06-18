# The lifecycle of a catalog expression

This traces one expression from the moment the MCP server receives Python
code to the moment its rows render in the catalog viewer, naming every
artifact and cache write along the way and the phase at which each happens.
Two walkthroughs: a brand-new expression (everything cold), then the same
expression on a later visit (warm). It is a companion to `caching.md`, which
maps the caches statically; this doc puts them on a timeline.

The one fact that makes the timeline make sense: an entry is written to disk
at *build* time, and so is the only materialized copy of its result — an
expensive entry's `.cache()` snapshot is baked during the build, not on first
read. Just one cache is populated *lazily*, at first view: Buckaroo's stat
cache on first `/load_expr`. So "build" and "first view" each write different
things, and a restart re-runs the view-time work against caches that survived
on disk.

## Cast

| Layer | Code | Role |
|---|---|---|
| MCP server | `src/tallyman_mcp/server.py` | receives code + alias, calls the builder |
| Builder | `src/tallyman_xorq/build.py` | compiles, executes, persists the entry |
| xorq compiler | `build_expr` / `load_expr` | tokenizes to a content hash, runs the graph |
| Result cache | `src/tallyman_xorq/result_cache.py`, `source_cache.py` | bakes an expensive entry's snapshot at build; `cached_result_expr` resolves reads |
| State + git | `src/tallyman_core/catalog_state.py`, `catalog.py` | per-concern tracked files in tallyman's native catalog git repo; the checkpoint commits |
| Companion | `src/tallyman_companion/app.py` | HTTP/SSE API the React SPA talks to |
| Buckaroo mgr | `src/tallyman_companion/buckaroo_lifecycle.py` | owns the Buckaroo subprocess + session map |
| Paths | `src/tallyman_core/paths.py` | single source of truth for every on-disk location |

Throughout, `<entry>` is `entry_dir(project, content_hash)` =
`<project>/artifacts/catalog/entries/<hash>/`, and `<catalog>` is
`<project>/artifacts/catalog/`.

---

# Part 1 — A new expression, everything cold

## 1. MCP ingest

The model calls one of three tools (`server.py`):

- `catalog_run(code, prompt)` — execute and persist an *anonymous* entry
  (`server.py:203`). No alias.
- `catalog_create(name, code, prompt)` — persist a **named** entry
  (`server.py:507`). Errors if the alias already exists (`get_alias` check at
  `server.py:526`).
- `catalog_revise(name, code, ...)` — update an existing alias to a new
  version. The new version's per-hash chart and display config are carried
  forward from the prior version unless it defines its own
  (`carry_forward_entry_config`, `entry_config.py:21`; §3); the same applies to
  the companion's `put_code` code-edit path.

The code is a Python string that must bind a variable `expr` to an
ibis/xorq expression, typically built from `from_catalog("alias")` (another
entry) or `from_project("file")` (a raw source). All three converge on
`_run_and_record` → `build_and_persist(project, code, prompt)` (`build.py:328`).

## 2. Build + execute (`build_and_persist`, `build.py:328`)

In order:

1. **Import the user code** in a fresh module scope (`_import_script`,
   `build.py:308`). During the import, `from_project` records each source file
   it touches and `from_catalog` records each parent edge;
   `source_identity` / `parent_capture` collect over the import
   (`build.py:355-362`) for the `cas` (default) / `salt` identity modes and the
   parent graph.
2. **Rewrite, then compile.** `rewrite_for_build` (`build.py:387`,
   `source_cache.py:89`) rewrites the author's expression before it is built:
   reject in-memory reads, inject a source-read cache after each non-parquet
   read, and — when the expression is expensive — wrap it in a top-level result
   cache. `build_expr(rewritten, builds_dir=<tmp>)` (`build.py:398`) then
   compiles into a throwaway temp dir whose name *is* the content hash — xorq's
   dask-style token over the (rewritten) expression structure
   (`content_hash = build_path.name`, `build.py:403`). The hash is
   content-sensitive in two of the three identity modes. Under the `cas` default
   each `from_project` read already went through a `data/.cas/<digest>` clone
   (`io.py:58`), so the digest is baked into the path xorq tokenizes; under
   `salt` the digests are folded in afterward (`si.salted_hash`, `build.py:407`)
   and no cache is baked; `off` leaves the hash path-only. `expr.py` keeps the
   author's literal source; `xorq_build/` carries the cache nodes.
3. **Idempotency check** (`build.py:414`): if `<entry>` already exists for this
   hash, nothing is rebuilt — the prompt is appended as a re-run event and the
   existing `BuildResult` is returned. (This is the cheap path that Part 2
   leans on.)
4. **Lay down the entry** (`build.py:430-450`): create `<entry>/`, copy xorq's
   build output to `<entry>/xorq_build/`, rewrite project-root paths to the
   `${TALLYMAN_PROJECT_ROOT}` placeholder (`make_portable_inplace`), and write
   the author's source to `<entry>/expr.py`.
5. **Execute** (`build.py:459-496`): `load_expr(build_path,
   cache_dir=compute_cache_dir(project))` binds the expression to the
   **per-project compute cache**, then the entry is executed to force row-level
   evaluation and record a content digest of the result (`result_digest`, #83).
   This is the one place the expression is actually computed, and what it writes
   depends on worthiness: an **expensive** entry runs `loaded.count().execute()`
   (`build.py:491`), which materializes its baked result-cache snapshot under
   `compute_cache/result_cache/`, then hashes the result (`result_digest(loaded)`,
   `build.py:492`); a **cheap** entry streams the result once through
   `count_and_result_digest(loaded)` (`build.py:496`), getting its row count and
   digest in a single pass (an honest full evaluation that fails fast on a bad
   cast / UDF, but caches no rows). No `result.parquet` is written either way.
6. **Derive metadata** (`build.py:497-561`): row count and `result_digest` come
   from the execute above and schema from the expression
   (`loaded.schema().to_pyarrow()`) — no parquet is read back. Write
   `<entry>/schema.json` and `<entry>/manifest.json` (the manifest carries the
   #87 admission fields `compile_seconds`, `cache_worthy`, `cache_bytes` — the
   baked snapshot's size, or `None` for a cheap entry — plus the #83
   `result_digest`, a content hash of the executed bytes), and append the prompt
   to `<catalog>/prompts/<hash>.jsonl`.
7. **Mark persisted; the checkpoint commits.** `build_and_persist` sets
   `catalog_registered = True` (`build.py:585`), meaning the entry dir is fully
   on disk — it no longer writes git itself. Durability is the *checkpoint's*
   job: when the MCP tool returns, the `_with_checkpoint` decorator that wraps
   every tool at registration (`server.py:163`, via `_checkpointing_tool` at
   `server.py:174`) calls `checkpoint_catalog` once, which under the per-project
   lock zips any complete entry into `entries/<hash>.zip` and makes
   a single git commit + step tag (`catalog_state.py:277`). There is no
   `_xcat_add`, no xorq catalog subprocess, and no out-of-lock writer — which is
   how #48 is resolved by construction.

### What got written, and where, at build time

| Artifact | Path | Written by |
|---|---|---|
| Build recipe | `<entry>/xorq_build/` (portable) | step 4 |
| User source | `<entry>/expr.py` | step 4 |
| **Baked result snapshot** (expensive only) | `<catalog>/compute_cache/result_cache/<key>.parquet` | the execute in step 5 |
| **Compute cache** (source parses + baked snapshots) | `<catalog>/compute_cache/` | xorq, during `load_expr` / execute, step 5 |
| Schema / manifest | `<entry>/schema.json`, `<entry>/manifest.json` | step 6 |
| Prompt history | `<catalog>/prompts/<hash>.jsonl` | step 6 |
| Chart / display config (if attached or carried forward) | `<catalog>/chart_specs/<hash>.vl.json`, `<catalog>/display_configs/<hash>.json` | UI, or `carry_forward_entry_config` on revise (§3) |
| Source digests (cas/salt only) | `<project>/artifacts/source_digests.json` (stat-keyed memo) + `<entry>/manifest.json` `sources` map | `source_identity`, step 1 |
| Content source clones (cas only) | `<project>/data/.cas/<digest>` (outside the catalog repo) | `from_project` → `ensure_cas_path`, step 1 |
| Alias | `<catalog>/aliases.jsonl` | `set_alias` (§3 below) |
| Recipe zip + pointers + commit | `<catalog>/entries/<hash>.zip`, `entries.jsonl`, `compute_cache.jsonl` + a git commit | the checkpoint, step 7 |

`xorq_build/`, `manifest.json`, and `schema.json` are `ENTRY_ARTIFACT_NAMES`
(`paths.py:99`) — immutable build outputs, partitioned from the regenerable
`ENTRY_CACHE_NAMES` (`.buckaroo_stat_cache`, `.xorq_build_expanded`). The perf
overlay symlinks the artifacts read-only and omits the caches, which is the
operational proof of the split: nothing ever rewrites an artifact. There is no
`result.parquet` in this list — none is written. The single materialized copy
of an expensive entry's rows is the baked `.cache()` snapshot under
`compute_cache/result_cache/` (§6), and a cheap entry keeps no copy at all.

The entry directory is gitignored; its git-tracked durable form is the recipe
zip `entries/<hash>.zip` that the checkpoint commits (step 7). So the build dir
is untracked-but-durable — the recipe zip carries it across a clone, and
`entries.jsonl` records which dirs should exist so `reset_to` reconciles them
from the bullpen, never silently rewriting them.

What is **not** written yet — these are lazy, and that is the whole point of
the timeline:

- **`<entry>/.buckaroo_stat_cache/parquet/`** — Buckaroo's summary stats.
  Populated on the first `/load_expr`, by Buckaroo, at view time (§5).
- **`<entry>/primary_key.json`** — written on the first primary-key scan.

The result-cache snapshot is deliberately *not* on this list: it bakes at
build, in step 5, not on first read (§6).

## 3. Alias registration + notify

For a named entry, `catalog_create` calls `set_alias(project, name, hash,
expect_exists=False)` (`server.py:531`), which writes a line to the tracked
`<catalog>/aliases.jsonl` — `{"alias", "latest", "history": [...]}` — and
appends a notebook cell. Aliases live in their own tracked file now, not in a
`catalog.yaml`; #103 made that possible by giving tallyman its own native
catalog repo (the old xorq catalog package rejected extra tracked files, which
is why aliases used to be smuggled into `catalog.yaml`). Then
`_notify("new_entry", content_hash=…, alias=…, version=…)` (`server.py:536`)
POSTs the companion's `/internal/notify`, which publishes an SSE event to any
connected SPA (`app.py`). The viewer's catalog list refreshes; nothing touches
Buckaroo yet. The git commit for all of this lands when the tool returns, at
the checkpoint (§2 step 7).

## 4. The user clicks the new expression

The SPA issues `GET /{project}/api/entry/{hash-or-alias}`
(`api_entry_detail`, `app.py:534`). The handler:

1. Resolves an alias to its hash (`app.py:539-541`).
2. Reads `manifest.json`, `schema.json`, `expr.py`, forensic history, chart
   spec, and display config off disk (`app.py:548-585`) — all cheap.
3. Calls `buckaroo.ensure_session(content_hash, project,
   column_config_overrides=…)` (`app.py:589`) — this is where Buckaroo
   enters.
4. Returns the entry payload plus `buckaroo_session` (the session id) and
   `buckaroo_ws_base` (the websocket base URL) (`app.py:599-614`).

## 5. `ensure_session` → Buckaroo `/load_expr` (`buckaroo_lifecycle.py:479`)

1. `_maybe_restart()` (revives a crashed subprocess, throttled); bail to
   `None` if Buckaroo isn't running — the SPA then falls back to a
   `pandas.to_html` preview.
2. **Snapshot self-heal** (`buckaroo_lifecycle.py:518-533`): before anything
   Buckaroo-facing, `ensure_session` calls `cached_result_expr(project, hash)`.
   For an expensive entry this guarantees the baked snapshot exists on disk — on
   a cold compute cache (a fresh clone, or an entry the startup warmup didn't
   reach) it recomputes it once, single-flighted per `(project, content_hash)`
   under `_heal_lock` so two tabs hitting the same cold entry don't both run the
   shared cache op (#79). This matters because a `from_catalog` chain off an
   expensive parent embeds a *bare* read of the parent's snapshot (#75 strips
   the parent's cache node to keep one backend), so without the snapshot the
   replay below would read zero rows. Done outside the session lock so a cold
   recompute doesn't block other sessions.
3. **Session-map check** (`buckaroo_lifecycle.py:535`+): for a new entry this
   misses, so a session is created.
4. **Stable build expansion**: `ensure_expanded_build` materializes
   `${TALLYMAN_PROJECT_ROOT}` in the entry's `xorq_build/` into
   `entry_expanded_build_dir(project, hash)` — a **stable per-entry path**,
   never a random tmp dir, gated by a `.complete` marker. The path matters
   because xorq embeds the build-dir path in the expression hash that keys the
   stat cache; a random path would miss the stat cache on every restart.
5. **Stat-cache dir**: `stat_cache = <entry>/.buckaroo_stat_cache`,
   `mkdir(exist_ok=True)` — preserves an existing cache, never wipes it.
6. **POST `/load_expr`** (`buckaroo_lifecycle.py:573`) with
   `build_dir=<expanded>` (the entry's own recipe build — the #102 viewer-read
   build dir was removed in #104), `project_root=<artifacts_dir>` (where
   Buckaroo scans for project-authored stat/post-processing klasses), and
   `cache_storage_path=<stat_cache>`. Buckaroo loads the xorq expression,
   creates a session, and returns its `session` id.
7. The session id is cached in `_sessions` (keyed by content hash, shared
   across projects) and persisted to `buckaroo_sessions.json` in
   `TALLYMAN_HOME` (default `~/.tallyman-notebooks/`).

Because the stat cache is empty for a new entry, Buckaroo computes summary
stats from scratch and **writes `<entry>/.buckaroo_stat_cache/parquet/`** —
this is the cold population. With buckaroo 0.15.0 + `BUCKAROO_PERF=1` this
shows up in `~/.buckaroo/logs/server.log` as `misses=N`, `snapshots
written>0`, and a `firstpull.summary_stats secs=…` span.

## 6. The websocket and first data pull

The SPA opens `ws://…/ws/{session}` against `buckaroo_ws_base` (also served
by `GET /{project}/api/session/{hash}`, `app.py:617`, which is just
`ensure_session` + the ws URL). Over the socket Buckaroo streams the first
row window and the summary-stats payload. The JS `SmartRowCache` holds row
segments client-side from here on (see `caching.md`).

The **result cache** is read through one function, `cached_result_expr`
(`result_cache.py:403`), and — unlike the stat cache — it was already populated
at build (step 5), so a read never writes it except to self-heal an eviction.
For an expensive entry (`classify_build` worthy — Aggregate / Join / Sort /
window / UDF, `result_cache.py:49`) it returns a `deferred_read_parquet` of the
baked snapshot under `compute_cache/result_cache/`, on the default backend; if
the snapshot was evicted it runs the cache node once to repopulate, then reads
(`result_cache.py:451-473`). That heal is single-flighted per `(project,
content_hash)` under `_heal_lock`, so concurrent cold readers don't both execute
the shared cache op and trip DataFusion's "Already borrowed", and a peer process
losing xorq's fixed-`.tmp` rename retries once if the snapshot landed (#79). The
repopulated snapshot is then checked against the `result_digest` recorded at
build, logging recompute drift on a mismatch (`_warn_if_self_heal_unfaithful`;
#83). A cheap entry returns the live expression re-imported from `expr.py`,
recomputed on read. Every reader takes this path: the paginated viewer
(`api_data` calls `cached_result_expr(...).limit(...).execute()`, `app.py:517`),
both sides of a diff, and post-processing.

---

# Part 2 — The same expression, warm

Three distinct "warm" scenarios, because they hit different caches.

## A. Re-submitting identical code (build idempotency)

`build_and_persist` runs `build_expr` again, gets the **same content hash**,
finds `<entry>` already present (`build.py:414`), appends the prompt, and
returns the existing `BuildResult`. No execution, no bake, no new compute-cache
writes. The expression is never recomputed when its structure (and, under salt
mode, its source content) is unchanged.

## B. Revisiting an entry in the same companion process (RAM-warm)

`GET /api/entry/{hash}` re-reads the small JSON/`expr.py` files (cheap), then
`ensure_session` finds the hash in `_sessions` and **returns the existing
session id without calling `/load_expr`** (`buckaroo_lifecycle.py:535-539`).
No Buckaroo recompute, no stat-cache touch. The SPA reuses the same
websocket. This is the fastest path.

## C. Revisiting after a restart (disk-warm, RAM-cold)

When Buckaroo restarts, the companion detects a new `started` timestamp from
`/health` and clears the in-RAM session map
(`_reset_session_bookkeeping_if_restarted`, `buckaroo_lifecycle.py:302`) —
but it touches nothing on disk. So on the next visit:

1. `ensure_session` misses the (cleared) session map → POSTs `/load_expr`
   again.
2. `ensure_expanded_build` finds the **stable expanded build** already
   present with its `.complete` marker → reuses it, no re-expansion.
3. Buckaroo is handed the same `cache_storage_path`, finds
   `.buckaroo_stat_cache/parquet/` populated, and the `ParquetSnapshotCache`
   **hits** — summary stats are read from disk, not recomputed. (Warm signal:
   `hits>0, misses=0` in `server.log`; `firstpull.summary_stats secs=Y` with
   `Y ≪` the cold time.)

The cache survives the restart precisely because (a) nothing in the restart
path deletes it, and (b) the expanded-build path is stable, so the snapshot
key is identical across processes.

The only thing that *invalidates* the stat cache is a klass-changing event —
a summary-stat / post-processing / display-config change or a project reset,
which routes through `reload_project_sessions` → `_clear_stat_cache`
(`buckaroo_lifecycle.py:378, 431`). A plain restart is not such an event.

Similarly, the baked **result snapshot** for an expensive entry survives the
restart untouched — it is content-addressed under `compute_cache/` — so
`cached_result_expr` returns its `deferred_read_parquet` without recomputing.
It re-checks the snapshot's presence on every call and recomputes only if the
file was evicted (`result_cache.py:451-473`, single-flighted under `_heal_lock`);
a Buckaroo restart never evicts it.

---

# One-line summary of cache write phases

- **Build time** (`build_and_persist`): the baked result snapshot under
  `compute_cache/result_cache/` (expensive entries only) plus any source-read
  caches, `schema.json` / `manifest.json`, source digests (cas/salt) and the
  `data/.cas/<digest>` content clones (cas only), `expr.py`, and the prompt
  history.
- **Checkpoint** (when the tool returns, `checkpoint_catalog`): the recipe zip
  `entries/<hash>.zip`, the `aliases.jsonl` / `entries.jsonl` /
  `compute_cache.jsonl` tracked surface, and one git commit + step tag.
- **First view** (`ensure_session` → `/load_expr`): `.buckaroo_stat_cache/`,
  the stable expanded build, the Buckaroo session (RAM + `buckaroo_sessions.json`).
- **First primary-key scan**: `<entry>/primary_key.json`.

Everything keyed on the content hash is immutable and self-heals on deletion;
the only invalidations are Buckaroo restart (RAM session map) and a
klass-changing edit/reset (stat cache). A `reset_to` additionally
garbage-collects orphaned `data/.cas` source clones, which live outside the
catalog git repo (`source_identity.gc_cas`). See `caching.md` for the full
invalidation table, including the cold-reconstruction staleness hole the `cas`
default does not close (#74/#115).
