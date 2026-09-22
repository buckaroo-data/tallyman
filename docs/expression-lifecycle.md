# The lifecycle of a catalog expression

This traces one expression from the moment the MCP server receives Python
code to the moment its rows render in the catalog viewer, naming every
artifact and cache write along the way and the phase at which each happens.
Two walkthroughs: a brand-new expression (everything cold), then the same
expression on a later visit (warm). It is a companion to `caching.md`, which
maps the caches statically; this doc puts them on a timeline.

The one fact that makes the timeline make sense: an **entry** (one catalog
computation, stored under its content hash) is written to disk at *build*
time, and so is the only materialized copy of its result. A **worthy** entry,
one whose plan does expensive work such as an aggregate or a join, has a
**snapshot** (a parquet file of its rows, which tallyman writes itself), and
the snapshot is written during the build, not on first read. A **cheap** entry,
a filter or selection over one file, keeps no copy of its rows. The rest is
written lazily, when the entry is first read or viewed: Buckaroo's stat cache
on the first `/load_expr`, the view build a worthy entry's grid is handed, a
cheap entry's expanded build, and the primary key a diff joins on. So "build"
and "first view" each write different things, and a restart re-runs the
view-time work against caches that survived on disk. Other terms follow
[architecture.md](architecture.md#terms).

## Cast

| Layer | Code | Role |
|---|---|---|
| MCP server | `src/tallyman_mcp/server.py` | receives code + alias, calls the builder |
| Builder | `src/tallyman_xorq/build.py` | imports the recipe, materializes a worthy entry, persists the entry |
| xorq compiler | `build_expr` / `load_expr` | tokenizes to a content hash, runs the graph |
| Ingest | `src/tallyman_xorq/io.py`, `ordered_copy.py` | reads a source through its clone into an ordered copy that ends in `__row_order` |
| Classifier and rewrite | `worthiness.py`, `source_cache.py`, `row_order.py` | worthy or cheap, the canonical sort, the `__row_order` rules |
| Materialization | `src/tallyman_xorq/materialize.py` | `materialize` writes snapshots; `ensure_materialized` makes every file an entry reads exist |
| Reads | `src/tallyman_xorq/result_cache.py` | `cached_result_expr`, the one canonical read |
| State + git | `src/tallyman_core/catalog_state.py`, `catalog.py` | per-concern tracked files in tallyman's native catalog git repo; the checkpoint commits |
| Companion | `src/tallyman_companion/app.py` | HTTP/SSE API the React SPA talks to |
| Buckaroo mgr | `src/tallyman_companion/buckaroo_lifecycle.py` | owns the Buckaroo subprocess and opens its sessions |
| Paths | `src/tallyman_core/paths.py` | the project's directory layout (the subdirectories of `compute_cache/` and `data/.cas/` are named in `materialize.py`, `ordered_copy.py` and `source_identity.py`) |

Throughout, `<entry>` is `entry_dir(project, content_hash)` =
`<project>/artifacts/catalog/entries/<hash>/`, and `<catalog>` is
`<project>/artifacts/catalog/`.

---

# Part 1 — A new expression, everything cold

## 1. MCP ingest

The model calls one of three tools (`server.py`):

- `catalog_run(code, prompt)` — execute and persist an *anonymous* entry. No
  alias.
- `catalog_create(name, code, prompt)` — persist a **named** entry. Errors if
  the alias already exists (a `get_alias` check).
- `catalog_revise(name, code, ...)` — update an existing alias to a new
  version. The new version's per-hash chart and display config are carried
  forward from the prior version unless it defines its own
  (`carry_forward_entry_config`, `entry_config.py`; §3); the same applies to
  the companion's `put_code` code-edit path.

The code is a Python string that must bind a variable `expr` to an
ibis/xorq expression, typically built from `tracked_expr_from_alias("alias")` (another
entry), `read_project_file("file")` (a raw parquet source) or
`tallyman_read_csv(path, schema=...)` (a CSV). All three tools converge on
`_run_and_record` → `build_and_persist(project, code, prompt)`, and so does
`catalog_load_parquet(rel_path)`, which writes a one-line `read_project_file`
recipe itself. The companion's code editor (`PUT /{project}/api/code/{alias}`)
calls `build_and_persist` directly.

## 2. Build + execute (`build_and_persist`)

The whole build holds the project's write lock (`catalog_state.project_lock`),
so builds, materializations, checkpoints and resets in either process happen one
at a time. In order:

1. **Import the user code** in a fresh module scope (`_import_script`). During
   the import, each loader announces itself. `read_project_file` and
   `tallyman_read_csv` take the source's content digest, clone the source
   under the `cas` identity mode (the default) to `data/.cas/<digest><suffix>`,
   and write an **ordered copy** of it under `compute_cache/ordered_sources/`
   (the source's rows in file order, plus a last column `__row_order`), unless
   that copy is already on disk. `tracked_expr_from_alias` and
   `pinned_expr_from_alias` record each parent edge, and make the parent's
   files exist first (`ensure_materialized`), because a child cannot be built
   over a snapshot that is missing.
   `source_identity`, `parent_capture` and `ordered_copy` each collect what was
   announced, for the manifest.
2. **Check, classify, rewrite, compile.** The build refuses a raw
   `xo.deferred_read_csv` and a raw `xo.deferred_read_parquet` of a file that
   is not under `compute_cache/`, since such a read gets no digest, clone or
   ordered copy. `worthiness.classify_expr` then decides, once and on the
   expression the author wrote, whether the entry is **worthy** (materialized)
   or **cheap** (row-preserving over one file, no file of its own); the
   verdict goes in the manifest. `rewrite_for_build` rejects in-memory reads, a
   recipe that already contains a xorq cache node, an assignment to
   `__row_order`, a join chain over three entries that all carry that column
   (the error shows the `.drop("__row_order")` fix; chains of semi or anti
   joins are refused too, #199), and a cheap entry that drops the column. It
   gives a worthy
   entry the canonical sort (the author's sort keys, then `__row_order`, then
   the remaining sortable columns), and moves `__row_order` to the last
   position of a cheap entry. `build_expr(rewritten, builds_dir=<tmp>)` then
   compiles into a throwaway temp dir whose name *is* the content hash: xorq's
   dask-style token over the rewritten expression structure
   (`content_hash = build_path.name`). The hash follows the content of every
   source in every identity mode, because the path xorq tokenizes is the
   ordered copy's, whose name is a function of the source's digest. Under
   `salt` the digests are also folded in afterward (`si.salted_hash`). `expr.py`
   keeps the author's literal source.
3. **Idempotency check**: if `<entry>/manifest.json` already exists for this
   hash, nothing is rebuilt: the prompt is appended as a re-run event and the
   existing `BuildResult` is returned. (This is the cheap path that Part 2
   leans on.) A directory with no manifest, left by a build that crashed, is
   built again.
4. **Lay down the entry**: create `<entry>/`, copy xorq's build output to
   `<entry>/xorq_build/`, rewrite project-root paths to the
   `${TALLYMAN_PROJECT_ROOT}` placeholder (`make_portable_inplace`), and write
   the author's source to `<entry>/expr.py`.
5. **Execute.** This is the one place the entry's query runs, and what it writes
   depends on the verdict. A **worthy** entry is materialized: `materialize`
   loads the entry's frozen build (which writes the expanded build,
   `<entry>/.xorq_build_expanded/`), rebinds it onto a single-partition
   connection, and streams the rows through the snapshot writer into
   `compute_cache/result_cache/<content_hash>.parquet`, numbering them in a
   last `__row_order` column. It runs the query twice and compares the content
   digests of the two files, so a recipe that is not reproducible is known from
   the start (`reproducible: false`, and the file is pinned). A **cheap** entry
   streams its frozen plan once, through `stream_row_count`, which forces
   row-level evaluation (a bad cast fails here, in tallyman's process; any UDF
   makes an entry worthy, so a UDF fails in `materialize`) and yields the exact
   row count, but keeps no rows. No `result.parquet` is written either way. If
   anything from step 4 on fails, the build removes the entry directory. For a
   worthy entry whose query had started, it also removes the snapshot file at
   the entry's path, even one that was on disk before the build began, for
   example one a reset left behind (#193).
6. **Derive metadata**: the row count and the `result_digest` (an
   `arrow-sha256:` content digest of the snapshot, worthy entries only) come
   from the execute above. A worthy entry's schema is read from the file
   `materialize` wrote, since parquet changes some types (a `timestamp[s]`
   column comes back `timestamp[ms]`) and the writer adds `__row_order`; a cheap
   entry's schema comes from the expression, which already ends in that column.
   Write `<entry>/schema.json` and `<entry>/manifest.json`, and append the
   prompt to `<catalog>/prompts/<hash>.jsonl`. The manifest carries the verdict
   (`cache_worthy`, `cache_worthy_why`), `compile_seconds` and `cache_bytes`
   (the snapshot's size, or `None` for a cheap entry), `result_digest`,
   `reproducible`, `snapshot_format`, `engine_versions`, `ordered_copies`,
   `sources` and `parents`, and is written last and atomically, so its
   presence means the entry is complete.
7. **Mark persisted; the checkpoint commits.** `build_and_persist` sets
   `catalog_registered = True`, meaning the entry dir is fully on disk — it does
   not write git itself. Durability is the *checkpoint's* job: when the MCP tool
   returns, the `_with_checkpoint` decorator that wraps every tool at
   registration (via `_checkpointing_tool`) calls `checkpoint_catalog` once,
   which under the per-project lock zips any complete entry into
   `entries/<hash>.zip` and makes a single git commit + step tag. There is no
   `_xcat_add`, no xorq catalog subprocess, and no out-of-lock writer — which is
   how #48 is resolved by construction.

### What got written, and where, at build time

| Artifact | Path | Written by |
|---|---|---|
| Build recipe | `<entry>/xorq_build/` (portable) | step 4 |
| User source | `<entry>/expr.py` | step 4 |
| **Snapshot** (worthy only) | `<catalog>/compute_cache/result_cache/<content_hash>.parquet` | `materialize`, step 5 |
| Expanded build (worthy only) | `<entry>/.xorq_build_expanded/` and its `.complete` marker | `materialize` loading the build, step 5 |
| **Ordered copy** of each source | `<catalog>/compute_cache/ordered_sources/<key>.parquet` (and a `<key>.digest` file) | `read_project_file` / `tallyman_read_csv`, step 1, unless already there |
| Schema / manifest | `<entry>/schema.json`, `<entry>/manifest.json` | step 6 |
| Prompt history | `<catalog>/prompts/<hash>.jsonl` | step 6 |
| Chart / display config (if attached or carried forward) | `<catalog>/chart_specs/<hash>.vl.json`, `<catalog>/display_configs/<hash>.json` | UI, or `carry_forward_entry_config` on revise (§3) |
| Source digests | `<project>/artifacts/source_digests.json` (stat-keyed memo, every mode) + the manifest's `sources` map (`cas` and `salt`) | `source_identity`, step 1 |
| Content source clones (`cas` only) | `<project>/data/.cas/<digest><suffix>` (outside the catalog repo) | `read_project_file` → `ensure_cas_path`, step 1 |
| Alias | `<catalog>/aliases.jsonl` | `set_alias` (§3 below) |
| Recipe zip + pointers + commit | `<catalog>/entries/<hash>.zip`, `entries.jsonl` + a git commit | the checkpoint, step 7 |

`xorq_build/`, `manifest.json`, and `schema.json` are `ENTRY_ARTIFACT_NAMES` in
`paths.py` — immutable build outputs, partitioned from the regenerable
`ENTRY_CACHE_NAMES` (`.buckaroo_stat_cache`, `.xorq_build_expanded`,
`.xorq_view_build`). The perf overlay symlinks the artifacts read-only and omits
the caches, which is the operational proof of the split: nothing ever rewrites
an artifact. There is no `result.parquet` in this list — none is written. The
single materialized copy of a worthy entry's rows is its snapshot (§6), and a
cheap entry keeps no copy at all.

The entry directory is gitignored; its git-tracked durable form is the recipe
zip `entries/<hash>.zip` that the checkpoint commits (step 7). So the build dir
is untracked-but-durable — the recipe zip carries it across a clone, and
`entries.jsonl` records which dirs should exist so `reset_to` reconciles them
from the bullpen, never silently rewriting them.

What is **not** written yet — these are lazy, and that is the whole point of
the timeline:

- **`<entry>/.buckaroo_stat_cache/parquet/`** — Buckaroo's summary stats.
  Populated on the first `/load_expr`, by Buckaroo, at view time (§5).
- **`<entry>/.xorq_view_build/`** — for a worthy entry, the build Buckaroo is
  handed (§5). Written on the first view.
- **`<entry>/.xorq_build_expanded/`** — for a cheap entry, its build with the
  project's path filled back in. Written the first time something loads the
  build: a page read, the grid, a child's build, or the companion's startup
  warm-up.
- **`<entry>/primary_key.json`** — written on the first primary-key scan.

The snapshot is deliberately *not* on this list: it is written at build, in
step 5, not on first read (§6).

## 3. Alias registration + notify

For a named entry, `catalog_create` calls `set_alias(project, name, hash,
expect_exists=False)`, which rewrites the tracked `<catalog>/aliases.jsonl` (one
`{"alias", "latest", "history": [...]}` line per alias) with the new alias in
it, and then appends a notebook cell. Aliases live in their own tracked file now, not in a
`catalog.yaml`; #103 made that possible by giving tallyman its own native
catalog repo (the old xorq catalog package rejected extra tracked files, which
is why aliases used to be smuggled into `catalog.yaml`). Then
`_notify("new_entry", content_hash=…, alias=…, version=…)` POSTs the companion's
`/internal/notify`, which publishes an SSE event (a message on the HTTP stream
each open browser tab holds) to any connected SPA (`app.py`). The viewer's
catalog list refreshes; nothing touches Buckaroo yet. The git commit for all of
this lands when the tool returns, at the checkpoint (§2 step 7), so the
notification goes out a moment before the commit.

## 4. The user clicks the new expression

The SPA issues `GET /{project}/api/entry/{hash-or-alias}` (`api_entry_detail`).
The handler:

1. Resolves an alias to its hash.
2. Reads `manifest.json`, `schema.json`, `expr.py`, forensic history, chart
   spec, and display config off disk — all cheap.
3. Returns that metadata plus `buckaroo_ws_base` (the websocket base URL). The
   request never touches Buckaroo, and `buckaroo_session` is always `None`:
   loading a grid can mean re-creating a missing file, which must not block the
   page (#133).

The grid loads separately. On the catalog page the data tab
(`BuckarooDataPane`) requests `GET /{project}/api/session/{hash}`
(`get_session`) as soon as the entry opens, shows a spinner with the elapsed
time while the request blocks, and then shows the grid, or the reason it failed
with a retry button (#133). The tab stays mounted, hidden, when another tab is
shown, so its websocket stays open. On the notebook page, `LazyBuckarooEmbed`
waits until a cell scrolls near the viewport, then polls the same route until it
returns a websocket URL. The notebook's data route (`/api/notebook_full`) has
also already posted `/load_expr` for every cell (#202).

## 5. `load_session` → Buckaroo `/load_expr` (`buckaroo_lifecycle.py`)

`get_session` calls `BuckarooManager.load_session`, which returns a typed status
(`ok`, `unavailable`, `no_build`, `timeout` or `error`) so the SPA can show a
precise message and a retry.

1. `_maybe_restart()` (revives a crashed subprocess, throttled); if Buckaroo isn't
   running the status is `unavailable`.
2. **Tallyman finishes its own work first.** `ensure_materialized` makes every
   file the entry's plan reads, and its own snapshot, exist. Here everything is
   already on disk, so for a worthy entry this is a manifest read and one `stat`
   call and no build is loaded. A file that had to be re-created is verified
   before it is served (§6). A failure here becomes status `error` with the
   reason, in tallyman's process, and never surfaces as a failed grid query
   inside Buckaroo.
3. **Pick what Buckaroo is handed.** For a **worthy** entry, `ensure_view_build`
   writes a *view build* once to the stable directory
   `<entry>/.xorq_view_build/`: a build whose whole graph is one read of the
   snapshot. Buckaroo never executes the entry's aggregate, join or sort, and
   never writes a snapshot. For a **cheap** entry, `ensure_expanded_build`
   materializes `${TALLYMAN_PROJECT_ROOT}` in the entry's `xorq_build/` into
   `entry_expanded_build_dir(project, hash)`, a **stable per-entry path**, never
   a random tmp dir, gated by a `.complete` marker. Both paths are stable so
   that Buckaroo is handed the same build directory after a restart, which is
   what its on-disk stat cache is meant to rely on.
4. **Stat-cache dir**: `stat_cache = <entry>/.buckaroo_stat_cache`,
   `mkdir(exist_ok=True)` — preserves an existing cache, never wipes it.
5. **POST `/load_expr`** with `session=entry-<project>-<content_hash>`,
   `build_dir` (from step 3), `project_root=<artifacts_dir>`,
   `cache_storage_path=<stat_cache>`, and `row_order_column="__row_order"`.
   Buckaroo looks for the project's klasses (its summary stats,
   post-processing functions and display classes) in `stats/`,
   `post_processing/` and `display/` under `project_root`; tallyman keeps
   display classes in `artifacts/display/` but writes the other two under
   `artifacts/catalog/`, so Buckaroo does not find them (#170). A promoted diff
   entry adds `column_config_overrides`, and the companion adds `telemetry_url`
   when it knows its own address. Buckaroo loads the xorq expression, creates
   the session, and returns its `session` id.
6. Nothing is remembered. The session id is a function of the project and the
   hash, so tallyman keeps no session map and no session file.

Because the stat cache is empty for a new entry, Buckaroo computes summary
stats from scratch and **writes `<entry>/.buckaroo_stat_cache/parquet/`**;
this is the cold population. Buckaroo logs one line per run for it
(`xorq stat cache […]: N hit(s), M miss(es), K snapshot(s) written …`) to its
own `~/.buckaroo/logs/server.log`, and posts its per-load timings to the
companion as `firstpull.*` spans (kept in `artifacts/telemetry.jsonl`, shown when
a `buckaroo` event is expanded in the Log tab). The subprocess's stderr goes to
`<project>/buckaroo.log`, in the project `tallyman run` started with.

## 6. The websocket and first data pull

The SPA opens `ws://…/ws/{session}` from the URL the session route returned.
Over the socket Buckaroo streams the first row window and the summary-stats
payload. The JS `SmartRowCache` holds row segments client-side from here on (see
`caching.md`). Buckaroo pages in its own process. Tallyman named `__row_order`
as the `row_order_column`, but Buckaroo 0.15.6, the pinned version, ignores that
hint, so the grid's pages are not yet ordered by it
(buckaroo-data/buckaroo#974).

The **result** is read through one function, `cached_result_expr`, whose
internals load the entry's frozen build (the canonical read of
`docs/system-contract.md`). Unlike the stat cache, the snapshot was already
written at build (step 5), so a read never writes one except to re-create a
snapshot the user deleted. `cached_result_expr` calls `ensure_materialized`
first, and then:

- for a worthy entry, returns one `deferred_read_parquet` of the snapshot under
  `compute_cache/result_cache/`, on the default backend, memoized per
  `(project, content_hash)`. When the file exists the entry's build is not
  loaded at all;
- for a cheap entry, returns the loaded build's graph rebound onto the default
  backend, recomputed on read over files that exist.

If a snapshot is missing, `ensure_materialized` re-creates it under the project
lock, after re-checking that it is still missing, by running the frozen build once
through the same writer, and verifies the result against the `result_digest`
recorded at build. Missing files it reads are made first: a parent's snapshot by
recursing on the hash in its name, an ordered copy from its clone with the reader
options in the manifest, a clone from the live source while the bytes still match.
A mismatch is still served, but never silently: `_verify_self_heal` records a
durable `unfaithful_heal` error (which also pins the file), wipes the entry's
stat cache, and fires the hooks. In the companion the hook posts a forced reload
of the entry's grid to Buckaroo and publishes an `unfaithful_heal` SSE event,
which the SPA has no listener for; the error shows in the catalog page's error
banner. The checks and the hook run while the heal holds the project lock
(#203), and cheap entries that read the healed snapshot are not flagged (#208).

Every reader takes this path: the paginated viewer (`api_data` pages the entry
with `row_order.page`, `ORDER BY __row_order` and then `LIMIT/OFFSET`, so the same
request returns the same rows), charts (which fetch up to 100,000 rows through
`api_data`), both sides of a diff, chaining a child recipe, and post-processing.

---

# Part 2 — The same expression, warm

Three distinct "warm" scenarios, because they hit different caches.

## A. Re-submitting identical code (build idempotency)

`build_and_persist` runs `build_expr` again, gets the **same content hash**,
finds `<entry>` and its manifest already present, appends the prompt, and
returns the existing `BuildResult`. No execution, no materialization, and no new file: the ordered
copies of its sources are already on disk, so ingest finds them and writes
nothing. The expression is never recomputed when its structure and its sources'
content are unchanged. If a source *has* changed, its digest changes, so the
ordered copy has a new name and the same recipe forks a new entry; the old entry
keeps the rows it was built from.

## B. Revisiting an entry in the same companion process (RAM-warm)

`GET /api/entry/{hash}` re-reads the small JSON/`expr.py` files (cheap). The
session route then runs `ensure_materialized` (a manifest read and a `stat` for a
worthy entry) and POSTs `/load_expr` again with the same derived id. Buckaroo
still holds that session, with the same build dir, and the post carries none of
the config-bearing fields, so it answers from the session it holds. No Buckaroo
recompute, no stat-cache touch. The grid connects to the same session again.
Two exceptions (#202): a promoted diff entry's post carries its
`column_config_overrides`, so Buckaroo re-runs its pipeline for it on every
open, and two opens of one entry at the same moment both post and both run it.

## C. Revisiting after a restart (disk-warm, RAM-cold)

When the companion starts (or restarts) the Buckaroo subprocess, it reads the
`started` timestamp from `/health` and, if it is new, clears its in-RAM
bookkeeping of diff sessions (`_reset_session_bookkeeping_if_restarted`). It
touches nothing on disk, and there is no entry-session record to clear. So on
the next visit:

1. `load_session` POSTs `/load_expr`, and Buckaroo, which no longer holds the
   session, creates it.
2. The **stable expanded build** (cheap entry) or **view build** (worthy entry)
   is already present with its marker → reused, no re-expansion.
3. Buckaroo is handed the same `cache_storage_path`, finds
   `.buckaroo_stat_cache/parquet/` populated, and the `ParquetSnapshotCache`
   **hits** — summary stats are read from disk, not recomputed. (Warm signal:
   `hits>0, misses=0` in `server.log`; `firstpull.summary_stats secs=Y` with
   `Y ≪` the cold time.)

The cache is meant to survive the restart because (a) nothing in the restart
path deletes it, and (b) Buckaroo is handed the same build over the same files,
so it computes the same stat-cache keys. In practice a first load after a
restart has not been seen to hit it (#157).

The only things that *invalidate* the stat cache are a klass-changing event (a
summary-stat, post-processing or display change), a project reset, a recalc
that moved an alias, and an unfaithful heal. All but the last route through
`reload_project_sessions`, which POSTs `/reload_expr/<id>` for every entry of the
project, one after another (a 404 or 400 means the grid is not open; #201), and
clears the stat cache of each grid it reloaded (`_clear_stat_cache`). A plain
restart is not such an event.

Similarly, the snapshot of a worthy entry survives the restart untouched — it is
named by the entry's content hash under `compute_cache/` — so
`cached_result_expr` returns its `deferred_read_parquet` without recomputing.
`ensure_materialized` re-creates it only if the user deleted the file; a Buckaroo
restart never does.

---

# One-line summary of cache write phases

- **Build time** (`build_and_persist`): during the recipe import, the ordered
  copy of each source under `compute_cache/ordered_sources/` and (`cas` only) its
  `data/.cas/<digest>` clone, and the source-digest memo
  (`artifacts/source_digests.json`, in every mode); then `xorq_build/` and
  `expr.py`, the snapshot under `compute_cache/result_cache/` and the expanded
  build (worthy entries only), `schema.json` / `manifest.json` (whose `sources`
  map is filled under `cas` and `salt`), and the prompt history.
- **Naming** (right after the build, in the tool): `aliases.jsonl` via
  `set_alias`, and the notebook cell.
- **Checkpoint** (when the tool returns, `checkpoint_catalog`): the recipe zip
  `entries/<hash>.zip` and `entries.jsonl`, and one git commit + step tag that
  also takes in `aliases.jsonl` and the rest of the tracked surface.
- **First view** (`load_session` → `/load_expr`): `.buckaroo_stat_cache/`, and the
  stable expanded build (cheap entry, unless a read already wrote it) or view
  build (worthy entry). No session record is written anywhere.
- **First primary-key scan**: `<entry>/primary_key.json`.

Everything keyed on the content hash is immutable, and a deleted file is made
again and verified the next time something reads it. The only invalidations are
a klass-changing edit, a reset or recalc, or an unfaithful heal (all of them
the stat cache). Only the user deletes a file (the Cache page; a pinned snapshot
is refused), apart from a failed build, which deletes the snapshot at its
entry's path (#193). A `reset_to`
leaves `compute_cache/` alone, and moves the `data/.cas` source clones that no
surviving entry's `manifest.sources` refers to into `<catalog>/bullpen/cas/`
instead of deleting them, so a reset forward brings them back. A child's
`manifest.sources` records every clone its parents' builds read, so a clone stays
referenced as long as any entry built on it survives. See `caching.md` for the
full invalidation table. (The cold-reconstruction staleness hole this section
used to reference — #74/#115 — is closed: reads load the frozen build, so a cold
read cannot see a post-build source edit.)
