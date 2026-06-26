# Tallyman architecture overview

This is the top-level map of the tallyman codebase. Read it first, then follow
the cross-references into the per-subsystem docs. For a document index with a
currency note on each file, see [Related documentation](#related-documentation)
at the end.

## What tallyman is

Tallyman is a deconstructed notebook platform. Instead of a notebook file with
inline cells and outputs, the unit of work is a **catalog entry**: a single
xorq expression, compiled and stored on disk under a content hash. Claude Code
is the author — it drives tallyman through an MCP server, creating and revising
entries as xorq expressions. The entries accumulate in an on-disk,
content-addressed, git-backed catalog. A FastAPI companion server plus a React
SPA visualize that catalog in a browser, and a Buckaroo subprocess provides
interactive data grids over each entry's result.

Nothing in tallyman is a long-lived application server holding state in memory.
The catalog on disk is the source of truth. The running processes (companion,
Buckaroo, MCP server) are views and editors over it; the MCP server holds no
in-memory catalog state, and the companion holds only its SSE subscriber list.

### Big-picture flow

```
  Claude Code
      │  (MCP tool calls over stdio)
      ▼
  tallyman MCP server ──────────────► on-disk catalog
   (compiles xorq exprs,              ~/.tallyman-notebooks/projects/<project>/
    checkpoints to git)               (content-addressed entries, aliases,
      │                                notebook, git history)
      │  best-effort HTTP notify             ▲
      ▼                                      │ reads/writes
  tallyman companion (FastAPI :7860) ────────┘
      │  REST + SSE
      ▼
  React SPA (packages/app) in the browser
      │  embeds grids
      ▼
  Buckaroo subprocess (:8700) ◄─── companion POSTs xorq_build/ dirs
   (interactive dataframe grids, sort/search push down to xorq)
```

The typical loop: Claude Code calls an MCP tool to create or revise an entry,
the MCP server compiles the xorq expression and writes a content-addressed
entry plus a git checkpoint, then notifies the companion. The companion emits a
Server-Sent Event (SSE, a push message over an HTTP stream the browser holds
open; see [Live updates over SSE](#live-updates-over-sse)), the SPA refetches
the affected data, and when the user opens
an entry the companion warms a Buckaroo session so the grid loads. See
[expression-lifecycle.md](expression-lifecycle.md) for the full
create-to-view path.

Three processes run on one machine: `tallyman_mcp` (spawned by Claude Code over
stdio), `tallyman_companion` (the FastAPI app, started with `tallyman run`), and
a `buckaroo` server subprocess that the CLI/companion supervises.

## Component map

Tallyman is six subsystems: five Python packages under `src/` and one frontend
workspace under `packages/`. The dependency direction runs core ← xorq ← {mcp,
companion} ← cli; nothing in `tallyman_core` imports the web, MCP, or compute
layers.

**tallyman_core** (`src/tallyman_core/`) is the native catalog model. It owns
the on-disk representation: versioned entries keyed by content hash, mutable
aliases with version history, the notebook cell list, chart specs, display
configs, project-global post-processing and summary-stat functions, and the
prompt/error/event logs. It manages the git-backed checkpoint transaction
(capture pointers, zip recipes, stage, commit, tag) and the reset-to-revision
operation that rewinds the catalog and reconciles untracked build artifacts
through a holding area called the bullpen. A key invariant lives here:
`assert_catalog_consistent` enforces an allow-listed tracked surface and
verifies that every hash referenced by an alias, chart, or display config has a
durable recipe zip. The call direction is one-way: `catalog_state` calls
`catalog`, never the reverse. Design: [native-catalog-store.md](../plans/native-catalog-store.md).

**tallyman_xorq** (`src/tallyman_xorq/`) is the xorq integration layer. It
compiles a xorq expression into a content-addressed entry, computes the content
hash from the expression structure (and, in some source-identity modes, the
source file digests), decides whether the entry is expensive enough to bake a
result snapshot, and writes a portable build directory whose absolute paths are
rewritten to `${TALLYMAN_PROJECT_ROOT}` placeholders. It also implements
reactive staleness detection (comparing recorded manifest fields against the
current world) and the recalc cone that recomputes dependents in dependency
order. Reconstruction of an entry's expression from its persisted `expr.py`
happens here, using context variables that pin the entry's recorded source
digests. See [caching.md](caching.md) and [reactive-recalc.md](reactive-recalc.md).

**tallyman_companion** (`src/tallyman_companion/`) is the FastAPI web server on
port 7860. It surfaces catalog views over REST, pushes live updates over SSE,
manages the Buckaroo subprocess lifecycle, and handles browser-initiated
mutations (code revision, diff promotion, resets). There are no server-side
HTML templates — it serves the compiled React SPA (`packages/app/dist`) as a
catch-all and mounts `/assets` and `/static`. A checkpoint middleware wraps
mutating routes so each authored change lands as one git revision, with an
opt-out denylist for routes that self-checkpoint or should not checkpoint at
all. It builds diff expressions (outer join with membership and per-column
delta/equality sentinel columns) and computes the Buckaroo column-config
overrides that color them. No dedicated subsystem doc yet — the route table is
in `app.py`; the create-to-view path is in [expression-lifecycle.md](expression-lifecycle.md).

**tallyman_mcp** (`src/tallyman_mcp/`) is the FastMCP server that Claude Code
talks to over stdio. It exposes the catalog, notebook, and project tools
(`catalog_run`, `catalog_create`, `catalog_revise`, `catalog_alias`,
`catalog_diff`, `catalog_recalc`, the chart/display/stat/post-processing tools,
`notebook_*`, and `project_*`). Checkpointing is opt-out: every tool
auto-checkpoints at the dispatch boundary unless it is on the no-checkpoint
list. The active project is session-sticky (seeded on first tool call, surviving
disk changes within the session), and project lifecycle changes are POSTed to
the companion rather than written directly so SSE stays honest. Notifications to
the companion are best-effort and never raise. Every tool, its parameters, and
its side effects (checkpoint, SSE notify, auto-recalc) are documented in
[mcp-server.md](mcp-server.md).

**tallyman_cli** (`src/tallyman_cli/`) is the Click command-line interface
(entry point `tallyman`). It initializes projects (with synthetic fixture data),
runs the companion and the MCP service, and owns the Buckaroo subprocess via
`BuckarooManager`, which spawns `python -m buckaroo.server` on port 8700 (or a
random free port) and exits when its stdin closes. It also provides `serve`
(read-only companion against a project directory anywhere on disk), `pack`
(portable tarball excluding cache and session state), `reset-to` / `revisions`,
and storyboard `replay` for deterministic rehearsal. See [installing.md](installing.md).

**Frontend** (`packages/app/`) is the React 18 + Vite SPA. It builds to `dist/`
and is served by FastAPI as a catch-all; it drives refetches off an SSE version
counter rather than polling, defers grid loads until scroll via
`LazyBuckarooEmbed`, and remaps views to new hashes when a recalc event arrives.
Its `BuckarooEmbed` component mounts `BuckarooServerView` from `buckaroo-js-core`
directly and connects over WebSocket. No dedicated frontend doc yet.

## On-disk catalog layout

A project lives at `~/.tallyman-notebooks/projects/<project>/`. The home root is
`~/.tallyman-notebooks/` by default and is overridable with the `TALLYMAN_HOME`
environment variable (`paths.py:tallyman_home`). The single active project name
is recorded in `~/.tallyman-notebooks/active_project` (one line). The catalog
itself is a git repository at `<project>/artifacts/catalog/`.

```
~/.tallyman-notebooks/
  active_project                     # one line: the active project name
  buckaroo_sessions.json             # global session map {hash: {session_id, project, started_at}}
  projects/<project>/
    artifacts/
      catalog/                       # git repo — the tracked catalog
        entries/<hash>.zip           # tracked recipe zip (expr.py, schema.json, xorq_build/)
        entries/<hash>/              # untracked build dir (gitignored, reconciled on reset)
        entries.jsonl                # pointer list, one {hash} per line
        aliases.jsonl                # one {alias, latest, history:[V1,V2,...]} per line
        notebook.jsonl               # one {cell_id, alias, markdown} per cell
        compute_cache.jsonl          # pointer list of warm cache files
        config.json                  # project settings, e.g. {auto_recalc: bool}
        chart_specs/<hash>.vl.json   # Vega-Lite specs, keyed by content hash
        display_configs/<hash>.json  # {column_config_overrides, diff_provenance}
        post_processing/<name>.py    # process(expr) functions (_disabled/ = soft-deleted)
        stats/<name>.py              # compute(col) functions (_disabled/ = soft-deleted)
        prompts/<hash>.jsonl         # append-only per-entry prompt history
        bullpen/                     # untracked holding area for evicted entries/caches
        compute_cache/               # untracked baked result snapshots (xorq)
        diff_stat_cache/             # untracked Buckaroo diff-stat caches per entry pair
        .gitignore                   # deny-by-default (entries/*/, bullpen/, caches, *.tmp)
      display/<name>.py              # display klass files (ColAnalysis subclasses)
      errors.jsonl                   # append-only error log (outside catalog repo)
      events.jsonl                   # append-only activity log (outside catalog repo)
      exports/                       # marimo .py, screenshots, CSVs
    data/                            # user input parquets (fixtures)
    data/.cas/<digest><suffix>       # content-addressed source clones (CoW), cas mode only
```

Key formats and what's tracked vs untracked:

- **Recipe zip** (`entries/<hash>.zip`) is the durable, deterministic,
  content-addressed entry. It contains `expr.py` (the author's literal source,
  with portable placeholders), `schema.json` (field names and types), and
  `xorq_build/` (the portable expression directory with `expr.yaml` plus deps).
  The zip writer only runs inside a checkpoint.
- **Entry build dir** (`entries/<hash>/`) is ephemeral and gitignored. It holds
  `manifest.json` (the build-completeness sentinel), the load-time-expanded
  `.xorq_build_expanded/`, and per-entry caches (`.buckaroo_stat_cache/`). A
  directory without a `manifest.json` is treated as partial/crashed and is not
  zipped.
- **Manifest** (`manifest.json`) carries entry metadata: `content_hash`,
  `result_digest`, `sources` (`{rel_path: digest}`), `parents` (DAG edges),
  `row_count`, `compile_seconds`, `execute_seconds`, `cache_worthy`,
  `cache_bytes`. Written atomically (temp file + `os.replace`).
- **JSONL pointer files** (`entries.jsonl`, `compute_cache.jsonl`) and the
  structured logs (`prompts/`, `errors.jsonl`, `events.jsonl`) are
  append-oriented and line-delimited. They replaced the single `catalog.yaml`
  that older docs reference.
- **Activity logs** (`events.jsonl`, `errors.jsonl`) live in `artifacts/`,
  outside the catalog git repo, so they survive reset-to-revision and have no
  size cap. The UI filters them on read.
- **No per-entry `result.parquet`.** That layer was removed (#104); an expensive
  entry's rows live in the baked `.cache()` snapshot under `compute_cache/`, a
  cheap entry keeps no copy and recomputes on read.

All catalog writers use atomic writes, so a crash mid-write or a checkpoint
firing during a write window leaves a whole file, not a torn one.

## Core domain concepts

**Content hash (identity).** Every entry is identified by a hash derived from
its expression structure (xorq's tokenization) and, depending on the
source-identity mode, its source file digests. Identity is structural: two
entries with the same expression and inputs collapse to the same hash, which is
what makes builds idempotent. Because hashes are content-addressed and globally
unique by construction, Buckaroo sessions are keyed globally by content hash,
not per project. The source-identity mode (`off` / `cas` / `salt`, default
`cas`) is decided in [adr-source-identity-content-hash.md](../plans/adr-source-identity-content-hash.md).

**Result digest.** A second identity axis, recorded for *worthy* (snapshot-baking)
entries only. The content hash keys the expression graph; the `result_digest`
keys the executed *bytes* as a row **multiset**, not a sequence. It is the
SHA-256 of the entry's baked snapshot parquet (`snapshot_file_digest`), which the
bake writes after sorting on a synthetic `original_row_order` and pinning the
parquet write settings, so the bytes are reproducible run-to-run and the file
hash is order-insensitive. Cheap, row-preserving entries record no digest — they
have no snapshot to hash and recompute live. A mismatch when an evicted snapshot
self-heals points at execution nondeterminism (sampling, `now()`, an impure UDF,
source drift), not the unordered-scan row reshuffling the canonical ordering now
absorbs. Design: [adr-result-digest-canonical-ordering.md](../plans/adr-result-digest-canonical-ordering.md).

**Alias and V_n versions.** An alias is a named, mutable pointer (for example
`sales`) to the latest content hash of a logical entry. Each alias carries an
ordered `history` list of every hash it has pointed at (V1, V2, …, oldest
first). Revising an alias mints a new content hash, advances `latest`, and
appends to `history`; the old versions remain as forensic lineage.
`catalog_diff` resolves version indices (-1 latest, -2 previous) through this
history.

**Parent edges and following.** At build time each entry records its direct DAG
parents as `{hash, ref, follow}`. `tracked_expr_from_alias('name')` records
`follow=True`: the edge names an alias, and the child goes stale as that alias
advances. `pinned_expr_from_alias('hash')` records `follow=False`: the edge pins
an exact hash and is never disturbed by recalc. Aliases are resolved to hashes
at read time, not baked into the edge.

**Staleness.** Read-only and side-effect-free. An entry is stale on the alias
axis when a `follow=True` parent's alias head no longer equals the recorded
hash, or on the source axis when a recorded source digest no longer matches the
file's current digest. Computing staleness never executes anything; it only
compares the manifest against the current world and returns reasons.

**Recalc cone.** When an alias head advances, its dependents form a cone of
entries that may now be stale. The cone is recomputed in topological order
(Kahn's algorithm over intra-cone parent edges) so parents rebuild before
children. Auto-recalc only recomputes the followers of the alias that just
moved; pre-existing ("orphan") staleness is left in place, logged, and
classified against the recorded error store rather than treated as an
unexplained invariant break. See [reactive-recalc.md](reactive-recalc.md) and
[recalc-mechanism.md](../plans/recalc-mechanism.md).

**Result cache vs source cache (two-axis caching).** Tallyman caches along two
axes. The source axis caches reads of input files. The result axis bakes a
result snapshot for entries judged expensive (those whose expression contains
aggregates, joins, sorts, windows, or UDFs); cheap, row-preserving entries
recompute on every read even when an ancestor is expensive. Baked snapshots
self-heal on read, so a cold read transparently rematerializes. See
[caching.md](caching.md).

**Portability.** A build directory embeds absolute filesystem paths in
`expr.yaml`. On write these are rewritten to `${TALLYMAN_PROJECT_ROOT}`
placeholders; on load they are expanded into a stable per-entry directory marked
complete by a sentinel, so a project can be copied or packed and run from
anywhere on disk, and the expanded path stays consistent so Buckaroo's
snapshot-cache keys match across restarts.

**Checkpoint and reset-to-revision.** A checkpoint is an atomic git transaction
under a per-project file lock: it captures pointers, zips pending recipes,
stages all tracked files, commits once, and tags the step. Reset-to-revision
does a hard git reset to a commit and then reconciles untracked build artifacts
back to the recorded pointers, evicting to or restoring from the bullpen without
recompute (a forward reset copies back from the bullpen). Live operations never
read the bullpen.

**Live updates over SSE.** The companion pushes changes to the browser with
Server-Sent Events (SSE): the SPA opens one long-lived HTTP stream to
`GET /{project}/api/sse` through the browser's native `EventSource`, and the
server writes named event messages down it. This is the inverse of polling. The
browser never asks "anything new?" on a timer; it holds the stream open and the
server speaks when something changes. The SPA registers listeners for these
kinds: `new_entry`, `build_failed`, `notebook_changed`, `chart_attached`,
`post_processing_changed`, `summary_stat_changed`, `recalc`, and
`project_switched` (plus `hello` and `ping`, which open and keep the connection
alive). An event is a signal, not a data payload: every one increments a
monotonic `version` counter held in a React context (`SSEContext.tsx`), and
components key their effects on that counter, so a bump triggers exactly one
refetch of the affected REST resource. That is what "refetches off an SSE
version counter rather than polling" means. Two events also carry state the
refetch cannot derive: `recalc` ships the `{oldHash: newHash}` remap so an open
entry view can follow its entry to the new hash, and `project_switched` ships
the project name to navigate to. If the stream drops, the context flips to
`offline`. Sources: `SSEContext.tsx` on the browser side, the `/{project}/api/sse`
route and the `/internal/notify` fan-out in `app.py` on the server side.

## Request and data-flow paths

### catalog_run / catalog_create — author a new entry

1. Claude Code calls the MCP tool with xorq source.
2. `tallyman_xorq` compiles the expression, computes the content hash, and runs
   the build, writing the entry build dir: `expr.py`, `xorq_build/`,
   `schema.json`, and finally `manifest.json` (written last, atomically, as the
   completeness sentinel). `cache_worthy` entries bake a result snapshot into
   `compute_cache/`.
3. The MCP dispatch boundary fires a checkpoint: `tallyman_core` zips the recipe,
   stages the tracked surface, commits one git revision, and tags it.
4. The MCP server best-effort POSTs a notification to the companion.
5. The companion emits an SSE `new_entry` event; the SPA bumps its version
   counter and refetches the entry list. See [expression-lifecycle.md](expression-lifecycle.md).

### catalog_revise + auto-recalc — revise an entry and cascade

1. A revision arrives from `catalog_revise` (MCP) or `PUT /code` (companion). It
   mints a new content hash, advances the alias `latest`, and appends the old
   hash to `history`. Charts and display configs carry forward from the old hash
   to the new one only where the new hash does not already define them.
   Self-alias references are rejected — an entry following its own alias would be
   permanently stale by design.
2. Auto-recalc, if enabled for the project, walks the recalc cone of the alias's
   followers in topological order and rebuilds each, re-pointing aliases before
   replaying children. This walk is checkpoint-free.
3. The whole thing lands as one checkpoint, so head advance plus cascade is a
   single git revision that reset-to-revision undoes atomically.
4. The companion emits an SSE `recalc` event carrying `{oldHash: newHash}` for
   each remapped entry; backgrounded SPA views navigate to the new hash, focused
   views stay put. See [reactive-recalc.md](reactive-recalc.md) and
   [auto-recalc-on-revise.md](../plans/auto-recalc-on-revise.md).

### Viewing an entry grid

1. The SPA entry-detail pane mounts `LazyBuckarooEmbed`, which waits for the grid
   to scroll near the viewport (IntersectionObserver) and polls for session
   startup.
2. The companion POSTs the entry's `xorq_build/` directory to the Buckaroo
   subprocess's `/load_expr`. First the build dir is expanded into a stable
   per-entry path with `${TALLYMAN_PROJECT_ROOT}` resolved, so Buckaroo's
   snapshot-cache key matches and a cold read of an expensive entry self-heals
   its baked snapshot rather than recomputing.
3. Buckaroo creates a session keyed by content hash and streams the grid over
   WebSocket; sort and search push down to the xorq backend rather than paging a
   materialized parquet.
4. The data tab stays mounted (hidden) across tab switches to keep the WS session
   alive. Sessions live only in Buckaroo's RAM; a Buckaroo restart (detected via
   a `started_at` timestamp on `/health`) clears the session maps and the next
   view re-POSTs.

### Diffing versions

1. The SPA diff page resolves the version pair (defaulting to V_{n-1} vs V_n via
   the alias history) and requests the diff from the companion.
2. The companion builds a compare expression: an outer join of the two versions
   with a membership column (a-only / b-only / both) plus per-column `{col}_eq`,
   `{col}_pct_delta`, and `{col}_abs_delta` sentinel columns. The compare
   expression is memoized for the process lifetime because the two entry hashes
   are immutable.
3. Buckaroo column-config overrides color the diff: categorical coloring for
   key/equality columns, numeric coloring for the delta columns.
4. The diff grid loads through the same `/load_expr` path as a normal entry view,
   with diff stats cached per entry pair under `diff_stat_cache/`.

## Related documentation

Currency notes below reflect a docs-vs-code audit on 2026-06-25. They will drift;
when in doubt, the code wins.

### Architecture docs (`docs/`) — describe the current system

- [expression-lifecycle.md](expression-lifecycle.md) — one expression from MCP
  ingest to rendered rows, naming every artifact and cache write. **Current.**
- [reactive-recalc.md](reactive-recalc.md) — revise an alias, recompute its
  dependents; the cone and the dependency graph. **Current.**
- [caching.md](caching.md) — the caches across the stack and their invalidation.
  **Mostly current.**
- [installing.md](installing.md) — install and run the spike. **Mostly current.**
- [mcp-server.md](mcp-server.md) — every MCP tool and prompt Claude Code drives,
  with parameters, return shapes, and per-tool side effects. **Current.**

### Design records / ADRs (`plans/`)

- [adr-source-identity-content-hash.md](../plans/adr-source-identity-content-hash.md)
  — content-addressed source reads so `content_hash` tracks source data.
  **Mostly current.**
- [adr-git-subprocess-threading.md](../plans/adr-git-subprocess-threading.md) —
  calling git from the multithreaded server (fork-safe `posix_spawn`).
  **Mostly current.**
- [adr-result-cache-cost-rubric.md](../plans/adr-result-cache-cost-rubric.md) —
  a *proposed* cost-vs-size cache rubric. **Partially stale / not adopted:** the
  structural `cache_worthy` admission test it proposes to remove is still the
  live gatekeeper, and `ensure_result` it names was removed (#73).
- [adr-result-digest-canonical-ordering.md](../plans/adr-result-digest-canonical-ordering.md)
  — `result_digest` as a row multiset via a canonically-ordered snapshot hash,
  replacing the per-row Python digest (#137). **Current** (implemented: the
  digest is now `snapshot_file_digest`, and `tallyman_read_csv` injects
  `original_row_order`).

### Plans (`plans/`)

- [native-catalog-store.md](../plans/native-catalog-store.md) — the native
  `tallyman_core.catalog` that replaced xorq's catalog package. **Mostly current.**
- [recalc-mechanism.md](../plans/recalc-mechanism.md) — how reactive recalc
  works. **Mostly current.**
- [auto-recalc-on-revise.md](../plans/auto-recalc-on-revise.md) — atomic
  auto-recalc on revise. **Partially stale:** line numbers and a couple of
  function names drifted (`_recalc_walk` → `_replay_cone`), and the "future
  Stage C" frontend SSE listener already shipped.
- [remove-ondemand-result-parquet.md](../plans/remove-ondemand-result-parquet.md)
  — removing the on-demand `result.parquet` layer (#104). **Current.**
- [project_switcher.md](../plans/project_switcher.md) — the project switcher.
  **Mostly current.**
- [89-determinism-prereqs-execution.md](../plans/89-determinism-prereqs-execution.md)
  — clearing #89's determinism prerequisites. **Mostly current.**
- [catalog-xorq-integration-tests.md](../plans/catalog-xorq-integration-tests.md)
  — coexistence/reset integration tests. **Partially stale:** references the old
  `catalog.yaml` / `aliases.json` formats since replaced by JSONL.
- [llm-summary-stats.md](../plans/llm-summary-stats.md) — LLM-authored summary
  stats. **Partially stale:** the Buckaroo-side klass pattern it describes
  (`_Generated_*` classes) is now a `@stat()` decorator; a few signatures and
  the notify `kind` differ.

### Research notes / experiment logs (`plans/`, `demo/`) — point-in-time records

The digest investigation behind #137 (current):
[datafusion-scan-order-findings.md](../plans/datafusion-scan-order-findings.md)
— why a parallel datafusion scan emits rows in a different order each run, and
the polars-ingest decision — and
[result-digest-vs-xorq-staleness.md](../plans/result-digest-vs-xorq-staleness.md)
— why `result_digest` can't reuse xorq's content-aware cache staleness.

Older, historical:
[eda-prompt-research.md](../plans/eda-prompt-research.md),
[eda-codegen-run1.md](../plans/eda-codegen-run1.md),
[haiku-codegen-findings.md](../plans/haiku-codegen-findings.md),
[model-codegen-comparison.md](../plans/model-codegen-comparison.md),
[plotting-testcases.md](../plans/plotting-testcases.md),
[xorq-sklearn-assessment.md](../plans/xorq-sklearn-assessment.md),
[ds-demo-scripts.md](../plans/ds-demo-scripts.md),
[demo/datasets.md](../demo/datasets.md). These are historical; staleness mostly
doesn't apply, except where they assert current system behavior (a few reference
the removed `result.parquet` and the old `~/.tallyman/` path).

### Root & meta

- [README.md](../README.md) — V0 spike overview and run instructions. **Current.**
- [proposal.md](../proposal.md) — the talk pitch. **Current** (it's a pitch, not
  a spec).

The original `plan.md` (V0.6 plan) and `TICKETS.md` (V0 punchlist) were removed
as stale cruft — they predated the React SPA migration, the `result.parquet`
removal, and the JSONL catalog format. This doc supersedes them as the
architecture reference.

> Gaps: there is no dedicated reference for the REST API, the CLI, or the
> frontend SPA architecture. (The MCP tool surface and the authoring extension
> points it exposes — display klasses, summary stats, post-processing — are now
> covered in [mcp-server.md](mcp-server.md).) See the project's gap tracking for
> the current list.
