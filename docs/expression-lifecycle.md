# The lifecycle of a catalog expression

This traces one expression from the moment the MCP server receives Python
code to the moment its rows render in the catalog viewer, naming every
artifact and cache write along the way and the phase at which each happens.
Two walkthroughs: a brand-new expression (everything cold), then the same
expression on a later visit (warm). It is a companion to `caching.md`, which
maps the caches statically; this doc puts them on a timeline.

The one fact that makes the timeline make sense: an entry is written to disk
at *build* time, but two of its caches are populated *lazily* — the read-time
result cache on first `ensure_result`, and Buckaroo's stat cache on first
`/load_expr`. So "build" and "first view" each write different things, and a
restart re-runs the view-time work against caches that survived on disk.

## Cast

| Layer | Code | Role |
|---|---|---|
| MCP server | `src/tallyman_mcp/server.py` | receives code + alias, calls the builder |
| Builder | `src/tallyman_xorq/build.py` | compiles, executes, persists the entry |
| xorq compiler | `build_expr` / `load_expr` | tokenizes to a content hash, runs the graph |
| Result cache | `src/tallyman_xorq/result_cache.py` | read-time materialization for expensive entries |
| State + git | `src/tallyman_core/catalog_state.py`, `xorq_catalog` | `catalog.yaml` keys + the catalog git repo |
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
  (`server.py:202`). No alias.
- `catalog_create(name, code, prompt)` — persist a **named** entry
  (`server.py:503`). Errors if the alias already exists (`get_alias` check at
  `server.py:522`).
- `catalog_revise(name, code, ...)` — update an existing alias to a new
  version.

The code is a Python string that must bind a variable `expr` to an
ibis/xorq expression, typically built from `from_catalog("alias")` (another
entry) or `from_project("file")` (a raw source). All three converge on
`build_and_persist(project, code, prompt)` (`build.py:238`).

## 2. Build + execute (`build_and_persist`, `build.py:238`)

In order:

1. **Import the user code** in a fresh module scope (`_import_script`,
   `build.py:266`). During the import, `from_project` records each source
   file it touches; `source_identity.begin_collect`/`end_collect`
   (`build.py:264-268`) capture those for the cas/salt identity modes.
2. **Compile** with `build_expr(expr_obj, builds_dir=<tmp>)` (`build.py:279`)
   into a throwaway temp dir. The build directory's name *is* the content
   hash — xorq's dask-style token over the expression structure
   (`content_hash = build_path.name`, `build.py:284`). In `salt` identity
   mode the source digests are folded in (`si.salted_hash`, `build.py:288`)
   so the hash becomes content-sensitive.
3. **Idempotency check** (`build.py:291`): if `<entry>` already exists for
   this hash, nothing is rebuilt — the prompt is appended as a re-run event
   and the existing `BuildResult` is returned. (This is the cheap path that
   Part 2 leans on.)
4. **Lay down the entry** (`build.py:306-325`): create `<entry>/`, copy
   xorq's build output to `<entry>/xorq_build/`, rewrite project-root paths
   to the `${TALLYMAN_PROJECT_ROOT}` placeholder (`make_portable_inplace`),
   and write the user's source to `<entry>/expr.py`.
5. **Execute** (`build.py:334-346`): `load_expr(build_path, cache_dir=
   compute_cache_dir(project))` loads the expression bound to the
   **per-project compute cache**, then `loaded.to_parquet(<entry>/
   result.parquet)` runs the graph and materializes the result. This is the
   one place the expression is actually computed.
6. **Derive metadata** (`build.py:348-369`): read row count + schema from the
   parquet via pyarrow, write `<entry>/schema.json` and
   `<entry>/manifest.json`, append the prompt to `prompts.jsonl`.
7. **Register the recipe** (`build.py:382-394`): `_xcat_add` adds
   `xorq_build/` to the xorq catalog repo and commits `catalog.yaml`.
   Best-effort, but the outcome rides back on `BuildResult.catalog_registered`
   — a failed add means the recipe never reached git and is not durable (#48).

### What got written, and where, at build time

| Artifact | Path | Written by |
|---|---|---|
| Build recipe | `<entry>/xorq_build/` (portable) | step 4 |
| User source | `<entry>/expr.py` | step 4 |
| **Materialized result** | `<entry>/result.parquet` | `to_parquet`, step 5 — **always**, cheap and expensive alike |
| **Compute cache** | `<catalog>/compute_cache/` | xorq, during `load_expr`/`to_parquet`, step 5 |
| Schema / manifest / prompts | `<entry>/schema.json`, `manifest.json`, `prompts.jsonl` | step 6 |
| Source digests (cas/salt only) | `<project>/artifacts/source_digests.json` | `source_identity`, step 1 |
| Alias + pointers | `<catalog>/catalog.yaml` keys (`alias_map`, `alias_history`, `entry_hashes`) + a git commit | `set_alias` (step 3 below) + `_xcat_add`, step 7 |

What is **not** written yet — these are lazy, and that is the whole point of
the timeline:

- **`<catalog>/result_cache/`** — the read-time `ParquetSnapshotCache` for
  expensive entries. Populated on the first `ensure_result`/diff, not at
  build (see §6 and Part 2).
- **`<entry>/.buckaroo_stat_cache/parquet/`** — Buckaroo's summary stats.
  Populated on the first `/load_expr`, by Buckaroo, at view time (§5).
- **`<entry>/primary_key.json`** — written on the first primary-key scan.

## 3. Alias registration + notify

For a named entry, `catalog_create` calls `set_alias(project, name, hash,
expect_exists=False)` (`server.py:527`), which writes the `alias_map` /
`alias_history` keys in `catalog.yaml` (not separate files — #48). Then
`_notify("new_entry", content_hash=…, alias=…, version=…)` (`server.py:532`)
POSTs the companion's `/internal/notify`, which publishes an SSE event to any
connected SPA (`app.py:1193`+). The viewer's catalog list refreshes; nothing
touches Buckaroo yet.

## 4. The user clicks the new expression

The SPA issues `GET /{project}/api/entry/{hash-or-alias}`
(`api_entry_detail`, `app.py:540`). The handler:

1. Resolves an alias to its hash (`app.py:544-550`).
2. Reads `manifest.json`, `schema.json`, `expr.py`, forensic history, chart
   spec, and display config off disk (`app.py:556-593`) — all cheap.
3. Calls `buckaroo.ensure_session(content_hash, project,
   column_config_overrides=…)` (`app.py:595`) — this is where Buckaroo
   enters.
4. Returns the entry payload plus `buckaroo_session` (the session id) and
   `buckaroo_ws_base` (the websocket base URL) (`app.py:605-621`).

## 5. `ensure_session` → Buckaroo `/load_expr` (`buckaroo_lifecycle.py:479`)

1. `_maybe_restart()` (revives a crashed subprocess, throttled); bail to
   `None` if Buckaroo isn't running — the SPA then falls back to a
   `pandas.to_html` preview (`buckaroo_lifecycle.py:504-506`).
2. **Session-map check** (`buckaroo_lifecycle.py:510-515`): for a new entry
   this misses, so a session is created.
3. **Stable build expansion** (`buckaroo_lifecycle.py:516-533`):
   `ensure_expanded_build` materializes `${TALLYMAN_PROJECT_ROOT}` into
   `entry_expanded_build_dir(project, hash)` — a **stable per-entry path**,
   never a random tmp dir, gated by a `.complete` marker. The path matters
   because xorq embeds the build-dir path in the expression hash that keys
   the stat cache; a random path would miss the stat cache on every restart.
4. **Stat-cache dir** (`buckaroo_lifecycle.py:534-535`):
   `stat_cache = <entry>/.buckaroo_stat_cache`, `mkdir(exist_ok=True)` —
   preserves an existing cache, never wipes it.
5. **POST `/load_expr`** (`buckaroo_lifecycle.py:536-558`) with
   `build_dir=<expanded>`, `project_root=<artifacts_dir>` (where Buckaroo
   scans for project-authored stat/post-processing klasses), and
   `cache_storage_path=<stat_cache>`. Buckaroo loads the xorq expression,
   creates a session, and returns its `session` id.
6. The session id is cached in `_sessions` (keyed by content hash, shared
   across projects) and persisted to `buckaroo_sessions.json` in
   `TALLYMAN_HOME` (default `~/.tallyman-notebooks/`).

Because the stat cache is empty for a new entry, Buckaroo computes summary
stats from scratch and **writes `<entry>/.buckaroo_stat_cache/parquet/`** —
this is the cold population. With buckaroo 0.15.0 + `BUCKAROO_PERF=1` this
shows up in `~/.buckaroo/logs/server.log` as `misses=N`, `snapshots
written>0`, and a `firstpull.summary_stats secs=…` span.

## 6. The websocket and first data pull

The SPA opens `ws://…/ws/{session}` against `buckaroo_ws_base` (also served
by `GET /{project}/api/session/{hash}`, `app.py:692`, which is just
`ensure_session` + the ws URL). Over the socket Buckaroo streams the first
row window and the summary-stats payload. The JS `SmartRowCache` holds row
segments client-side from here on (see `caching.md`).

The read-time **result cache** is a separate path that fires only when
something calls `ensure_result` / `cached_result_expr`
(`result_cache.py:107`, `:144`) — most visibly the keyed **diff**. On first
use, for an expensive entry (`classify_build` worthy — Aggregate/Join/Sort/
window/UDF or a non-parquet read, `result_cache.py:52`), the expression is
wrapped in the project's `ParquetSnapshotCache` and executed, which writes
`<catalog>/result_cache/<key>.parquet` (`ensure_result`,
`result_cache.py:159-171`). A cheap entry skips this and reads its in-place
`result.parquet` via `deferred_read_parquet` (`result_cache.py:141`). The
plain viewer streams from the xorq expression directly, so a first *view*
does not necessarily populate `result_cache/`; a first *diff* does.

---

# Part 2 — The same expression, warm

Three distinct "warm" scenarios, because they hit different caches.

## A. Re-submitting identical code (build idempotency)

`build_and_persist` runs `build_expr` again, gets the **same content hash**,
finds `<entry>` already present (`build.py:291`), appends the prompt, and
returns the existing `BuildResult`. No execution, no `to_parquet`, no new
compute-cache writes. The expression is never recomputed when its structure
(and, under salt mode, its source content) is unchanged.

## B. Revisiting an entry in the same companion process (RAM-warm)

`GET /api/entry/{hash}` re-reads the small JSON/`expr.py` files (cheap), then
`ensure_session` finds the hash in `_sessions` and **returns the existing
session id without calling `/load_expr`** (`buckaroo_lifecycle.py:510-515`).
No Buckaroo recompute, no stat-cache touch. The SPA reuses the same
websocket. This is the fastest path.

## C. Revisiting after a restart (disk-warm, RAM-cold)

When Buckaroo restarts, the companion detects a new `started_at` from
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
(`buckaroo_lifecycle.py:419, 431`). A plain restart is not such an event.

Similarly, the read-time **result cache** for expensive entries hits on the
second `ensure_result`: `cache.exists(expr)` is true, so it returns the
stored path without recomputing (`result_cache.py:162-171`).

---

# One-line summary of cache write phases

- **Build time** (`build_and_persist`): `result.parquet`, `compute_cache/`,
  schema/manifest, source digests, `catalog.yaml` + git commit.
- **First view** (`ensure_session` → `/load_expr`): `.buckaroo_stat_cache/`,
  the stable expanded build, the Buckaroo session (RAM + `buckaroo_sessions.json`).
- **First diff / `ensure_result`** on an expensive entry:
  `<catalog>/result_cache/<key>.parquet`.
- **First primary-key scan**: `<entry>/primary_key.json`.

Everything keyed on the content hash is immutable and self-heals on deletion;
the only invalidations are Buckaroo restart (RAM session map) and a
klass-changing edit/reset (stat cache). See `caching.md` for the full
invalidation table.
