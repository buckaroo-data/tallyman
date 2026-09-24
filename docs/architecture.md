# Tallyman architecture overview

This is the top-level map of tallyman: how the system works from end to end,
and where each part is documented in more depth. Read it first. The
[Related documentation](#related-documentation) section at the end lists every
doc, with a note on how current each one is.

## What tallyman is

Tallyman is a notebook without cells. Its unit of work is a **catalog entry**:
one xorq expression (a deferred dataframe computation, which runs only when
asked), compiled and stored on disk under its **content hash**. Claude Code is
the author. It drives tallyman through an MCP server, sending Python that
builds an expression, and each successful call becomes an entry in a
git-backed catalog on disk. A FastAPI companion server and a React single-page
app (SPA) show the catalog in a browser, and a Buckaroo subprocess draws the
interactive data grid for each entry.

The catalog on disk is the source of truth, and the running processes are
views of it and editors of it. Each process keeps in-memory caches keyed by
content hash, which change how fast an answer comes back and never what the
answer is. Beyond those caches, the MCP server remembers which project its
session is working on, and the companion holds its open SSE streams and the
diff sessions it has opened in Buckaroo. Running two tallyman servers against
one project is unsupported, and nothing detects it yet (#183).

### Terms

These are the project's own terms. The other docs use them with the same
meaning.

- **Catalog:** a project's entries, aliases and notebook, kept in a git
  repository at `<project>/artifacts/catalog/`.
- **Entry:** one catalog computation, stored in `entries/<content_hash>/` and
  committed as `entries/<content_hash>.zip`.
- **Content hash:** an entry's identity, xorq's 12-character hash of the
  entry's expression after tallyman's rewrite. Every file the expression reads
  is a snapshot named by the content hash of the entry it holds, so the hash
  covers the inputs as well as the structure. A source entry (below) is the
  exception that makes this work: its hash is an md5 of the imported bytes and
  the reader options, truncated to the same 12 characters.
- **Recipe:** the Python the author submitted, kept as the entry's `expr.py`.
  It names its inputs by alias, so run again later it can mean something else.
- **Build:** the entry's `xorq_build/` directory, the expression frozen to disk
  with every input fixed. A read of a cheap entry and every heal of a computed
  entry load the build (a source entry heals from its clone);
  a worthy entry whose snapshot exists is read from the snapshot alone. The
  recipe is run again only to make a new entry (a revise or a recalc), and by
  one diagnostic after a heal that went wrong.
- **Manifest:** the entry's `manifest.json`, which records what the build does
  not say: parent hashes, the cheap-or-worthy verdict, the result digest, and
  for a source entry where its data came from.
- **Alias:** a mutable name, such as `sales`, that points at the latest content
  hash of a logical entry and keeps every hash it has pointed at, as versions
  V1, V2 and so on. An alias has a kind: a **catalog alias** names
  computations, and a **source alias** names versions of an imported file.
- **Import:** `catalog_import_source`, the one way a file enters the catalog.
  It copies the file's bytes into the project, writes one snapshot of them and
  points a source alias at the new entry. A recipe never opens a file.
- **Source entry:** one version of a source alias. It is an ordinary entry
  whose manifest also records `provenance`: the path the file was imported
  from, its digest, the reader options, and the name it was imported as. The
  path is never read again.
- **Clone:** the imported bytes, kept under `data/.cas/<digest><suffix>`,
  named by their md5 digest and checked against it when written. A source
  entry's snapshot is made again from its clone.
- **`__row_order`:** an int64 column holding `0..N-1` in a file's physical row
  order. It is the last column of every file tallyman writes, and pages of an
  entry are sorted by it.
- **Worthy and cheap entries:** a **worthy** entry is one tallyman
  materializes, because its plan does work that is expensive or that cannot
  keep its input's row order (an aggregate, a join, a sort, a window function,
  among others). A **cheap** entry is row-preserving over one file (filters,
  column selections, computed columns) and has no file of its own; its small
  plan re-runs on every read.
- **Snapshot:** the parquet file that holds a worthy entry's result,
  `compute_cache/result_cache/<content_hash>.parquet`. A source entry is worthy,
  and its snapshot holds the imported rows in the file's order.
- **Materialize:** run an entry's build to completion and write the result to
  its snapshot.
- **Heal:** make a missing snapshot again (a source entry's from its clone,
  any other by running the entry's build) and check it against what was
  recorded.
- **Result digest:** a content digest of a snapshot, recorded in the manifest
  when the snapshot is first written; every heal is checked against it.
- **Pinned snapshot:** one the Cache page refuses to delete, because it cannot
  be made again faithfully.
- **Checkpoint:** one git commit of the catalog repository, tagged `step-NNN`.
  Each operation that changes the catalog lands as one checkpoint.
- **Reset:** `reset_to`, which returns the catalog to an earlier checkpoint.
- **Bullpen:** the directory a reset moves retired entry directories and
  clones into, so that a later reset forward can bring them back.
- **Project lock:** a file lock on the project that builds, materializations,
  checkpoints and resets take, so that they happen one at a time.
- **Session:** one grid's state inside the Buckaroo process.
- **View build:** a build whose whole graph is one read of a worthy entry's
  snapshot. It is what Buckaroo is handed for a worthy entry.
- **Klass:** a summary statistic, post-processing function or display class
  written for the project, for Buckaroo's grids to use.

### Big-picture flow

```
  Claude Code
      │  MCP tool calls over stdio
      ▼
  tallyman MCP server ──────────────► on-disk project
   (builds entries,                   ~/.tallyman-notebooks/projects/<project>/
    checkpoints to git)               (entries, snapshots, aliases,
      │                                notebook, git history)
      │  best-effort HTTP notify             ▲
      ▼                                      │ reads and writes
  tallyman companion (FastAPI :7860) ────────┘
      │  REST + SSE
      ▼
  React SPA (packages/app) in the browser
      │  embeds grids
      ▼
  Buckaroo subprocess (:8700) ◄─── companion posts a build to display
   (grids: paging, sorting, search, summary statistics)
```

The typical loop: Claude Code calls an MCP tool to create or revise an entry.
The MCP server imports the code and builds the entry. It checks the
expression, decides whether the entry is worthy, freezes it, runs its query to
completion (writing the snapshot of a worthy entry), and writes the entry
directory. It then notifies the companion, best effort, and commits a
checkpoint as the tool returns. The companion publishes a Server-Sent Event
(SSE, a message on an HTTP stream the browser keeps open; see
[Live updates over SSE](#live-updates-over-sse)), and the SPA refetches what
changed. When the user opens the entry, the companion first makes sure every
file the entry reads exists, then asks Buckaroo to open a session for it, and
the grid connects to that session over a WebSocket.
[expression-lifecycle.md](expression-lifecycle.md) follows one entry through
all of this.

Three processes run on one machine: `tallyman_mcp`, which Claude Code spawns
over stdio (one per Claude Code session); `tallyman_companion`, the FastAPI app
that `tallyman run` starts; and the Buckaroo server, which `tallyman run`
spawns and stops. The MCP server and the companion both write to the catalog.
The project lock makes their builds, materializations, checkpoints and resets
happen one at a time; smaller writes (an alias, a notebook cell, a chart) are
atomic file replacements that take no lock.

## Component map

Tallyman is six parts: five Python packages under `src/` and one frontend
workspace under `packages/`. Imports mostly run one way, core ← xorq ←
{companion, mcp} ← cli. The exceptions: `tallyman_core` imports `tallyman_xorq`
lazily inside three functions (`reset_to` clears the read memo and retires
clones, and `run_post_processing` reads an entry's result), the MCP server
imports the companion's diff helpers for `catalog_promote_diff`, and the
companion imports the CLI's fixture writer for `/api/projects/new`.

**tallyman_core** (`src/tallyman_core/`) is the catalog model and store. It
owns the on-disk representation: entries keyed by content hash, aliases and
their version history, the notebook's cell list, chart specs, display configs,
the project's post-processing and summary-stat functions, per-project settings
(`config.json`), and the prompt, error and event logs. It runs the checkpoint
(record the entry pointers, zip new recipes, `git add -A`, commit once, tag the
step) and `reset_to`, which rewinds the catalog to a step and reconciles the
files git does not track through the bullpen. It holds the project lock
(`catalog_state.project_lock`). `assert_catalog_consistent` enforces an
allow-listed set of tracked paths and checks that every hash an alias, chart or
display config names has a committed recipe zip. The call direction is one
way: `catalog_state` calls `catalog`, never the reverse. Design:
[native-catalog-store.md](../plans/native-catalog-store.md).

**tallyman_xorq** (`src/tallyman_xorq/`) turns recipes into entries and serves
their results. `build.py` imports a recipe and builds the entry. `io.py` holds
what recipes read other entries with (`tracked_expr_from_alias`,
`pinned_expr_from_alias`) and the refusals of a raw file read.
`source_import.py` imports a file as a source entry: `source_identity.py` keeps
the clones, and `ordered_copy.py` holds the reader options a source entry
records and the row-group size of its snapshot.
`worthiness.py` decides cheap or worthy. `source_cache.py` and `row_order.py`
check and rewrite the expression before it is frozen. `materialize.py` writes
snapshots and makes sure the files an entry reads exist. `digest.py` computes
content digests. `result_cache.py` is the one read of an entry's result.
`portable.py` makes builds relocatable. `staleness.py`, `dependents.py` and
`recalc.py` are the reactive system, and `primary_key.py` and `diff.py` serve
version diffs. See [caching.md](caching.md) and
[reactive-recalc.md](reactive-recalc.md).

**tallyman_companion** (`src/tallyman_companion/`) is the FastAPI web server
on port 7860. It serves the catalog over REST, pushes live updates over SSE,
handles edits made in the browser (a code revision, a diff promotion, notebook
edits, a recalc, deleting a snapshot, project switching), and talks to Buckaroo
through `BuckarooManager` (`buckaroo_lifecycle.py`), which spawns the
subprocess, opens sessions and reloads them when klasses change. It has no
server-side HTML templates: it serves the built SPA (`packages/app/dist`) for
every GET that no route matches (an unmatched `/api` path gets a JSON 404), and
mounts the SPA's `/assets`. A checkpoint middleware commits one git revision
after each successful mutating request, except on an explicit list of exempt
routes (`/internal/*` and the project routes, cache and log clears, telemetry,
reset, and the routes that checkpoint themselves). `diff.py` builds the diff
comparison, an outer join with a membership column and per-column delta and
equality columns, and the Buckaroo column settings that colour it. No
dedicated doc covers the route table yet; it is in `app.py`, and the
create-to-view path is in [expression-lifecycle.md](expression-lifecycle.md).

**tallyman_mcp** (`src/tallyman_mcp/server.py`) is the FastMCP server that
Claude Code talks to over stdio: 31 tools and one prompt. Every tool
checkpoints after it succeeds unless it is on the `_NO_CHECKPOINT` list. The
active project is sticky for the session: seeded on the first call, then
changed only by `project_switch` or `project_new`, which go through the
companion so that its SSE stream stays honest. Notifications to the companion
are best effort and never raise. [mcp-server.md](mcp-server.md) documents every
tool, its parameters and its side effects.

**tallyman_cli** (`src/tallyman_cli/main.py`) is the Click command line,
`tallyman`. `init` creates a project, with a synthetic `data/orders.parquet`
unless given `--no-fixture` (written, not imported: a recipe reads it only after
`catalog_import_source`), and records its step-000 checkpoint. `run` starts the
companion and, unless given `--no-buckaroo`, the Buckaroo subprocess
(`python -m buckaroo.server --stdio-control`, which exits when its stdin
closes) on port 8700, or on a random port if 8700 is taken. `mcp` starts the
MCP server. `serve` runs a read-only companion, without Buckaroo, against a
project directory anywhere on disk. `pack` tars a project directory. `reset-to`,
`revisions` and `revisions label` move through and name checkpoints. `replay`
runs a storyboard of MCP tool calls. See [installing.md](installing.md).

**Frontend** (`packages/app/`) is a React 18 and Vite SPA with pages for the
catalog, the notebook, diffs, the Cache page, the activity log and the project
list. It refetches when an SSE event arrives instead of polling. The catalog
page's data tab asks for the entry's Buckaroo session as soon as the entry
opens, and shows a spinner, then the grid, or the reason it failed with a retry
button (#133). The notebook page's data route opens a Buckaroo session for every
cell each time the page loads (#202), and each cell's grid connects only when
the cell scrolls near the viewport (`LazyBuckarooEmbed`). The grid itself is
`BuckarooServerView` from `buckaroo-js-core`, connected to the Buckaroo process
over a WebSocket. No dedicated frontend doc yet.

## On-disk layout

A project lives at `~/.tallyman-notebooks/projects/<project>/`. The home root
is `~/.tallyman-notebooks/` by default and can be moved with the
`TALLYMAN_HOME` environment variable (`paths.tallyman_home`). The active
project's name is the one line of `~/.tallyman-notebooks/active_project`.

```
~/.tallyman-notebooks/
  active_project                       # one line: the active project's name
  projects/<project>/
    artifacts/
      catalog/                         # git repo: the catalog
        entries/<hash>.zip             # tracked recipe zip (expr.py, schema.json, manifest.json, xorq_build/)
        entries/<hash>/                # untracked entry directory (below)
        entries.jsonl                  # the entries a checkpoint recorded, one {hash} per line
        aliases.jsonl                  # one {alias, latest, history, kind} per line
        notebook.jsonl                 # one {cell_id, alias, markdown} per cell
        config.json                    # project settings, e.g. {"auto_recalc": true}
        chart_specs/<hash>.vl.json     # Vega-Lite specs, by content hash
        display_configs/<hash>.json    # {column_config_overrides, diff_provenance}
        post_processing/<name>.py      # process(expr) functions (_disabled/ holds removed ones)
        stats/<name>.py                # compute(col) functions (_disabled/ holds removed ones)
        prompts/<hash>.jsonl           # the prompts each entry was built from
        .gitignore                     # keeps the untracked paths below out of git add -A
        .checkpoint.lock               # the project lock (untracked)
        compute_cache/                 # untracked; files tallyman can make again
          result_cache/<hash>.parquet    # snapshots of worthy entries (source entries too)
        bullpen/                       # untracked; entries/ and cas/ that a reset retired
        diff_stat_cache/<a>-<b>/       # untracked; Buckaroo statistics per diffed pair
      display/<name>.py                # display klasses (outside the catalog repo)
      errors.jsonl                     # error log (outside the catalog repo)
      events.jsonl                     # activity log (outside the catalog repo)
      telemetry.jsonl                  # Buckaroo grid-load timings (outside the catalog repo)
      exports/
    data/                              # tallyman init's fixture (an import takes any path)
    data/.cas/<digest><suffix>         # clones: the bytes of every imported file
    buckaroo.log                       # the Buckaroo subprocess's stderr (tallyman run)
    notebook_marimo.py                 # written by catalog_export_marimo
```

An entry directory, `entries/<hash>/`, holds the four recipe members
(`expr.py`, `schema.json`, `manifest.json`, `xorq_build/`) and the per-entry
caches, which are made again on demand: `.xorq_build_expanded/` (the build with
the project path filled back in), `.xorq_view_build/` (a worthy entry's view
build), `.buckaroo_stat_cache/` (Buckaroo's summary statistics) and
`primary_key.json` (the key a diff joins on). `.xorq_build_expanded/` and
`.xorq_view_build/` each have a sibling `.complete` marker, written last.

Key formats, and what is tracked:

- **Recipe zip** (`entries/<hash>.zip`) is the committed record of an entry.
  Nothing reads it back, so a clone of the catalog repository does not recreate
  entry directories from it. The checkpoint writes it, deterministically, once
  per entry, and nothing else does, so it keeps the manifest as it was at
  create.
- **Manifest** (`manifest.json`) records `content_hash`, `project`,
  `created_at`, `prompt`, `row_count`, `execute_seconds`, `compile_seconds`,
  `cache_worthy` and `cache_worthy_why` (the cheap-or-worthy verdict and its
  reason), `cache_bytes` (the snapshot's size), `result_digest`, `reproducible`
  and `nonreproducible_columns`, `snapshot_format` and `engine_versions`,
  `parents` (`[{hash, ref, follow}]`), `unfaithful_heal_digest` (set by an
  unfaithful heal, and a pin) and, on a source entry only, `provenance`
  (`{alias, version, path, digest, suffix, reader, imported_at}`, where `alias`
  and `version` are the name it was imported as). It is the entry directory's
  last write, atomic, and its presence means the entry is complete: the entry
  list, the checkpoint, recalc and the build skip or rebuild a directory that
  has none. A page read still serves such a directory, and #204 describes what
  that means for a worthy entry. After create only an unfaithful heal rewrites
  the manifest, to record `unfaithful_heal_digest`, atomically, under the
  project lock.
- **`compute_cache/`** holds the files tallyman writes and can make again:
  snapshots, a source entry's included. It is untracked, a reset leaves it
  alone, and anything may delete it: the next read makes what it needs again.
  The exceptions are the pinned snapshots, which cannot be made again; one of
  them is a source entry's snapshot once its clone is gone.
- **Logs** (`errors.jsonl`, `events.jsonl`, `telemetry.jsonl`) live in
  `artifacts/`, outside the catalog repository, so a reset does not rewind
  them and a recorded failure survives it. Dismissing the error banner deletes
  `errors.jsonl`, which holds no pin (pins are decided from the manifest), and the
  readers of `errors.jsonl` skip a line that is not a JSON object, so one torn
  append does not break the banner or the error page.
- **Display klasses** (`artifacts/display/`) are also outside the catalog
  repository, so no checkpoint commits them and no reset rewinds them.
  Summary stats and post-processing functions are inside it.
- **No per-entry `result.parquet`.** That layer was removed in #104; the
  companion still sweeps any old one away once per project at startup.

Tracked catalog files are written to a temporary file and renamed into place,
so a checkpoint that fires during a write commits a whole file.
`entries.jsonl` is written only inside the checkpoint, under the project lock.

## Core domain concepts

### Identity: the content hash

An entry's content hash is the name xorq gives the build directory of the
entry's rewritten expression. xorq hashes a file read by its path alone, so
tallyman puts the content in the path: every file a recipe's expression reads is
a worthy entry's snapshot, named by that entry's content hash, and a child's
hash is therefore a function of its parent's. The chain bottoms out at source
entries, whose hash is `md5("source|<digest>|<reader signature>")` truncated to
12 hex characters (`source_import.source_entry_hash`): the md5 digest of the
imported bytes and the reader options they were parsed with, and nothing else.
It cannot come from xorq, because a source entry's generated recipe reads the
snapshot that the hash names. So the hash of any entry covers the bytes of every
file under it. Two entries with the same expression over the same inputs
collapse to one hash, which makes building idempotent. The absolute path of the
project is part of a computed entry's hash, so the same recipe in a project at
another path gets another hash; a source entry's is not, so two projects that
import one file each hold their own entry under the same hash. The clone store
underneath is [ADR-002](../plans/ADR-002-source-identity-content-hash.md)'s,
narrowed by [ADR-011](../plans/ADR-011-sources-are-aliases.md) to one mode:
every import digests and clones.

### Aliases and versions

An alias is a line `{alias, latest, history, kind}` in `aliases.jsonl`.
Revising an alias builds a new entry, moves `latest` to its
hash and appends that hash to `history`; the old versions stay as they were.
The kind is `catalog` or `source`, and a name is one or the other, never both.
`set_alias` keeps an alias's kind matching its entries: a catalog alias never
points at a source entry and a source alias never points at a computed one,
whichever route sets it. A source alias moves only by an import: revising it,
promoting a diff onto it and `catalog_alias` onto a source entry are refused,
in the MCP tools and in the companion's code-edit and promote-diff routes
alike, before anything is built. Renaming and removing a source alias work as
for a catalog alias.
`catalog_diff` and the diff page pick versions from this history (`-1` is the
latest, `-2` the one before). A Buckaroo session id is derived from the project
and the content hash, `entry-<project>-<hash>`, so tallyman keeps no record of
sessions.

### Parent edges

At build time each entry records the entries its recipe read, as
`{hash, ref, follow}` in `manifest.parents`.
`tracked_expr_from_alias("sales")` records `follow=True`: the child follows the
alias and goes stale when the alias moves. `pinned_expr_from_alias` takes only a
version reference such as `"sales-v2"` and records `follow=False`: the child
stays on that entry. A bare alias is refused (#166), because it would pin
whatever the head happened to be, and so is a bare content hash (ADR-011 D5, no
bare hashes in recipes), so every edge an authored recipe records names an
alias (a promoted diff's generated recipe is the exception: it names its two
entries by hash and records no edge); an entry with no alias,
one built by `catalog_run`, has to be named before anything can build on it. A
source alias is read the same way as any other. The edge stores the hash the
alias pointed at when the child was built, and the staleness scan looks the
alias up again to compare.

### Worthy and cheap entries

Tallyman decides once, when an entry is built, whether it is worthy, and
records the verdict in the manifest as `cache_worthy`, with a short reason in
`cache_worthy_why`
(`worthiness.classify_expr`). An entry is cheap only if every relation
operation in it is a file read, a filter, a column selection or computed
column, a column drop, a drop of null rows or a fill of nulls; it reads exactly
one file; and no value in it multiplies rows (`unnest`), depends on the order
rows arrive in (a window function) or is not pure (`random()`, `uuid()`,
`now()`, `today()`, any UDF). Everything else is worthy. The test is an
allow-list, so an operation nobody has classified costs a copy instead of
unstable paging. A cheap entry must keep `__row_order`, because it pages by the
column of the file it reads: a select that drops the column fails the build,
and the error shows the corrected select.

### Materialization

A worthy entry is materialized when it is created, by one routine,
`materialize`, which every heal of a computed entry also uses. (A source entry's
snapshot is written by the import, and made again from its clone, with the same
writer settings.) `materialize` runs the entry's build on a single-partition
connection (so a float total is merged in one order), streams the rows through a
writer with a pinned layout (zstd, row groups of 1,048,576 rows, a page index),
numbers them in a last `__row_order` column, writes a temporary file and renames
it over the snapshot, all under the project lock, and returns the content digest
of the file it wrote. A heal renames at once; a create leaves the file at its
temporary name and renames it after the manifest is written, so a failed build
never touches the file already at the path. At create it runs the query twice
and compares the two digests. If they differ, the entry still builds, the
manifest records `reproducible: false` with the columns that differed, and the
snapshot is pinned. A cheap entry writes nothing: its plan is streamed once in
full at create, so an error in it fails the tool call. Every file that holds a
result is tallyman's; no build contains a xorq cache node, and xorq's own cache
is not used. [caching.md](caching.md) has the details.

### Reads: `cached_result_expr` and `ensure_materialized`

Every consumer reads an entry's result through `cached_result_expr`: `/api/data`
pages, charts, diffs, post-processing, and a child recipe chaining off the
entry. It first calls `ensure_materialized`, which makes every file the entry's
plan reads exist before anything runs: a missing snapshot is made again by
running its entry's build, or, for a source entry, by parsing its clone again
with the reader options it recorded (`_heal_a_source`). No plan reads a clone,
and nothing in a read makes one again. With the clone gone the source entry's
snapshot is the last copy of its rows, so it is pinned; if it is gone too, the
read fails with an error naming the missing clone and the
`catalog_import_source` call, reader options included, that repairs the version.
The Buckaroo hand-off calls `ensure_materialized` too. A worthy entry then reads
as one bare read of its snapshot, without loading its build when the file
exists, and a cheap entry as its frozen plan, re-run over files that exist. A
healed snapshot is checked against the recorded `result_digest`. A mismatch is
still served, since the rows are the honest output of the frozen build, but the
heal records the digest it wrote in the manifest's `unfaithful_heal_digest`,
which pins the file, logs an `unfaithful_heal` error for the error banner, and
wipes the entry's Buckaroo statistics; when the heal runs in the companion,
Buckaroo is also told to reload the entry's grid. The pin is part of the
manifest, so it moves with the entry through a reset and survives dismissing the
banner, which deletes the error log.

### Sources: imports and source aliases

A recipe names aliases, and a build reads only files tallyman owns
([ADR-011](../plans/ADR-011-sources-are-aliases.md)). A file enters the catalog
by one call, `catalog_import_source(outside_path, alias, pinned_version=None,
schema=..., reader_options=...)` (`source_import.update_and_depend`). It
digests the file, clones the bytes to `data/.cas/<digest><suffix>` and checks
the clone against the digest, writes the source entry's snapshot, writes the
entry itself (a generated recipe, a frozen build, a schema and a manifest with
`provenance`), and appends the entry to the source alias's history. The path
can be anywhere, and after the import it is never read again, so editing,
moving or deleting the original changes no build. New data arrives by importing
again under the same alias: different bytes mint the next version, and every
entry that follows the alias goes stale exactly as it would after a revise.
The import emits the same events as a revise, runs auto-recalc on the same
switch, and lands with its cascade as one checkpoint.

The call's outcome depends on the alias's history (ADR-011 D3, the case table).
Identical bytes are a no-op returning the current version. `pinned_version=N`
claims the file is version N, or the next version to mint, and is refused if it
is neither. Versions cannot be skipped, and history only grows: bytes equal to
an older version are refused, with `reset_to` named as the way back and
`pinned_expr_from_alias("<alias>-v<N>")` as the way to read that version. Bytes
another alias of the project already holds are refused too, naming that alias:
one set of bytes, read one way, is one version under one alias, and a second
name for it is a catalog entry whose recipe is `tracked_expr_from_alias("<that
alias>")`. A re-import of a version whose snapshot is gone rewrites nothing of
the entry: it restores the clone from the given file if needed and heals the
snapshot the way a read would, verified against the recorded digest.

Reader options are fixed at import (ADR-011 D12). A parquet file takes none: its
snapshot is written by pyarrow in the file's order, so it keeps the file's
types. A CSV is parsed by polars under the schema and `scan_csv` options named in
the call, through ADR-005's schema language and inference ladder, and its rows
go to the same pyarrow writer in batches. The options are recorded on the entry
and are part of its hash, so a CSV read two ways is two imports under two
aliases. A source entry's snapshot is written with the same pyarrow settings as
any other snapshot, in row groups of 122,880 rows rather than 1,048,576.

The build refuses every other way of reading a file: `read_project_file`,
`tallyman_read_csv`, `xo.deferred_read_csv`, and `xo.deferred_read_parquet` of a
file outside `compute_cache/`, each with an error naming the import to use.
`read_project_file` survives only inside the recipe the importer generates, where
a context variable resolves it to the entry's own snapshot.

### Row order

Every file tallyman writes ends in `__row_order`, so a page served by `/api/data` is
sorted by `__row_order`, and the same request returns the same rows in any
process. (The paging helper also takes user sort keys and puts `__row_order`
after them; no route passes any yet, and Buckaroo's grid does not use the
column yet, buckaroo-data/buckaroo#974.) Every `order_by` in a recipe also gets
`__row_order` and then the remaining columns appended as tie-breakers, and a
sort followed only by steps that keep row order (filters, selections, limits)
still decides the order that is written.
[system-contract.md](system-contract.md) states the rules.

### Result digest

`result_digest` is `arrow-sha256:<hex>`, a SHA-256 over the Arrow data of a
snapshot as read back (`digest.py`). It does not change with how
the rows were batched, the codec, the row-group size or the writer's version,
and it does change with any value, any null, the order of the rows, and the
column names and types. Worthy entries record it; a cheap entry has no snapshot
and records none. Its one job is to show whether a re-created snapshot
reproduced the original. A mismatch is attributed to an engine change (the
manifest records the xorq, xorq-datafusion and pyarrow versions and the
snapshot format version), to a recipe that bakes a changing value into its
graph (#88), or to a graph that runs differently each time (#83). Running each
new worthy entry twice finds most such recipes at create; what it cannot find,
such as `today()` or a non-pure parent, is #185. Design:
[ADR-004](../plans/ADR-004-result-digest-canonical-ordering.md) and
[ADR-009](../plans/ADR-009-digest-stability.md).

### Staleness

Staleness is a judgment that runs no query, opens no data file and changes no
catalog state; it reads `aliases.jsonl` and the manifests. An entry is stale
when a `follow=True` parent's alias now points at a different hash than the one
recorded, and for no other reason (ADR-011 D6, one staleness axis). A pinned
parent never makes its child stale. A changed input file is not a reason until
it is imported: the import moves the source alias, and the entries following
it go stale through the same rule. Only an entry that is the current head of an
alias counts as directly stale (#154); a superseded version is reported with
`live=False`. A parent alias that no longer exists is reported under
`unknown_axes`.

### Recalc cone

When an alias head advances, the entries that followed it become directly
stale. They and every current head built on them form the
cone. Recalc replays each member's recipe in topological order (Kahn's
algorithm over the edges inside the cone), so a parent rebuilds and its alias
moves before its children replay. A member whose inputs did not move, such as
a child that pins a version of its parent, replays to the same hash and is left
alone. Auto-recalc, on by default for each project, runs this for the followers
of the alias a revise or an import just moved; staleness from any other cause
is left in place, logged, and classified against the recorded errors. See
[reactive-recalc.md](reactive-recalc.md).

### Portability

A build contains absolute paths in its `expr.yaml`. The build step rewrites the
project's path to a `${TALLYMAN_PROJECT_ROOT}` placeholder,
and a read fills it back in, into the stable per-entry directory
`.xorq_build_expanded/`. A project can therefore be packed, copied or cloned to
another path, and the expanded build keeps one path across restarts. The
expanded directory's marker
does not record which project path it was filled in with, so a copy that
carries the expanded directories along keeps reading the old location (#209).

### Checkpoint and reset

A checkpoint takes the project lock, records the complete entry directories in
`entries.jsonl`, zips any entry that has no recipe zip yet, runs `git add -A`,
commits once and tags `step-NNN`. The MCP server checkpoints after each tool,
and the companion after each mutating request; a recalc and a diff promotion
checkpoint themselves, once each. `reset_to` takes the lock, runs
`git reset --hard` to the step, and reconciles the files git does not track. Entry
directories the step does not list move to the bullpen; listed ones that are
missing are copied back from it. Source clones that no surviving entry refers to
move to `bullpen/cas/`, never deleted, and clones a restored entry needs are
copied back. A reset leaves `compute_cache/` alone: its files are named by
content hash, a leftover cannot be served for another entry, and a file that is
missing afterwards is healed like any other. An entry directory retired a second
time replaces the copy already parked under its name, since the live one is the
one that agrees with the snapshot on disk; a live directory with no manifest (an
interrupted build's) is dropped instead. The one live reader of the bullpen is
the Cache page: it lists a retired entry's snapshot as `retired` and decides its
pin from the manifest parked in the bullpen, counting a retired source version's
clone as present when it is parked in `bullpen/cas/`, since a reset forward
brings both back.

### The project lock

One re-entrant file lock, `catalog_state.project_lock` (a `flock` on
`artifacts/catalog/.checkpoint.lock`), is taken by a build (for its whole
length, recipe import included), an import, a materialization or heal, a
checkpoint and a reset. It holds between the MCP server and the companion, and
it is re-entrant within a thread, since a build materializes, and an import
heals, while it holds the lock. It blocks with no timeout, so a
page request whose entry needs a heal waits behind any build in the other
process (#186), and the two companion routes that build on the event loop,
`PUT /code` and `POST /promote_diff`, freeze the whole UI while they wait or
build (#190). Smaller writes to tracked files (aliases, notebook cells, charts,
display configs, `config.json`) take no lock: each replaces its whole file
atomically, so two processes editing the same file at the same moment can lose
one of the edits. The lock covers no reads either: concurrent reads on the shared
DataFusion backend can fail with `Already borrowed` (#118).

### Live updates over SSE

The companion pushes changes to the browser with Server-Sent Events: the SPA
opens one long-lived HTTP stream to
`GET /{project}/api/sse` through the browser's `EventSource`, and the server
writes named events down it. The browser never polls. The SPA listens for
`new_entry`, `build_failed`, `notebook_changed`, `chart_attached`,
`post_processing_changed`, `summary_stat_changed`, `recalc` and
`project_switched`, plus `hello` and `ping`, which open and keep the stream
alive. Each listened event except `project_switched` increments a `version`
counter in a React context (`SSEContext.tsx`), and components refetch their
REST resource when it changes. Two events carry state a refetch cannot derive:
`recalc` carries the `{oldHash: newHash}` remap, so that an entry view open in a
background tab moves to the entry's new hash (a focused tab stays put), and
`project_switched` carries the project to navigate to. The server also
publishes `entry_added`, `alias_changed`, `alias_renamed`, `display_changed`,
`project_reset` and `unfaithful_heal`, which the SPA has no listener for, so
they cause no refetch. If the stream drops, the context reports `offline`.
Sources: `SSEContext.tsx` in the browser; the `/{project}/api/sse` route and the
`/internal/notify` fan-out in `app.py` on the server.

## Request and data-flow paths

### catalog_run / catalog_create: author a new entry

1. Claude Code calls the tool with Python that binds `expr`.
2. `build_and_persist` takes the project lock and imports the code. While it
   runs, `tracked_expr_from_alias` and `pinned_expr_from_alias` record each
   parent edge and make the parent's files exist.
3. The build refuses what cannot become a sound entry: a raw file read, a bare
   content hash or bare alias passed to `pinned_expr_from_alias`, an in-memory
   table, a `.cache()` call, an assignment to `__row_order`, a cheap entry that
   drops it, and a join chain over three entries that all carry it.
   It classifies the entry, adds the canonical sort to a worthy entry, and
   freezes the expression with xorq's `build_expr`, whose directory name is the
   content hash. If a complete entry with that hash is already on disk, the
   build stops and returns it.
4. It writes the entry directory (`xorq_build/` with portable paths,
   `expr.py`) and runs the query: a worthy entry is materialized, twice, and its
   snapshot written at a temporary name; a cheap entry is streamed once and
   nothing is kept.
5. It writes `schema.json` (read from the snapshot for a worthy entry) and then
   `manifest.json`, atomically. Only then is a worthy entry's snapshot moved
   into place, as the last write to the entry's result.
6. `catalog_create` sets the alias and adds a notebook cell. The tool notifies
   the companion, and as it returns the dispatch wrapper commits a checkpoint,
   which zips the recipe.
7. The companion publishes `new_entry`, and the SPA refetches the entry list.

If the build fails after it has created the entry directory, the directory is
removed, along with the snapshot's temporary file. The file at the snapshot's
path is untouched, so a snapshot that was already there, such as one a reset
left behind, survives a failed re-add.

### catalog_import_source: bring a file in

1. Claude Code calls the tool with a path, a source alias and, for a CSV, a
   schema and reader options.
2. `update_and_depend` refuses a directory, a name that is already a catalog
   alias, and a file that is neither parquet nor CSV. It decides the reader,
   digests the file and computes the entry hash, then takes the project lock
   and consults the alias's history (the case table under
   [Sources](#sources-imports-and-source-aliases)).
3. To mint a version it clones the bytes to `data/.cas/`, writes the snapshot,
   and writes the entry directory: a generated `expr.py` that records the path,
   digest and reader options in its header, a frozen `xorq_build/`,
   `schema.json`, and `manifest.json` with `provenance`, last. A failure removes
   the entry directory it created. It then appends the entry to the alias.
4. The tool records an `alias_set` event, adds a notebook cell for a new alias
   (or carries charts and display configs forward from the previous version),
   notifies the companion with `new_entry`, and runs auto-recalc for the
   alias's followers. The checkpoint as the tool returns takes in the import
   and its cascade. A no-op import records and notifies nothing.

### catalog_revise + auto-recalc: revise an entry and cascade

1. A revision arrives from `catalog_revise` (MCP) or `PUT /code` (companion);
   both refuse a source alias, before building anything, with an error that
   names the import. It builds a new entry, moves the alias's `latest` to the
   new hash and appends it to `history`. Charts and display configs carry
   forward from the old hash to the new one where the new hash has none of its
   own. A revision that reads its own alias by name is rejected: it would follow
   its own head and be stale forever.
2. If auto-recalc is on for the project (it is by default), the followers the
   revise made stale, and every current head built on them, are rebuilt in
   topological order, each alias re-pointed before its children replay. The
   walk takes no checkpoint of its own.
3. The revise and the cascade land as one checkpoint, so a reset to the step
   before undoes both.
4. A cascade that changed anything publishes a `recalc` SSE event with the
   remap, and the companion reloads the project's Buckaroo sessions. See
   [reactive-recalc.md](reactive-recalc.md).

### Viewing an entry grid

1. The catalog page's data tab requests `GET /{project}/api/session/{hash}` as
   soon as it opens. On the notebook page, the data route (`/api/notebook_full`)
   opens a session for every cell each time the page loads or refetches (on any
   SSE event), and each cell's grid asks `/api/session` for its WebSocket URL
   once the cell scrolls near the viewport (#202).
2. The companion calls `load_session`, which runs `ensure_materialized` first,
   so a missing file is made again and checked before Buckaroo is involved. A
   failure there shows in the page with its reason and a retry button.
3. It posts `/load_expr` to Buckaroo with the session id
   `entry-<project>-<hash>`. A worthy entry is handed its view build
   (`<entry>/.xorq_view_build/`, written once); a cheap entry its own build,
   expanded into `<entry>/.xorq_build_expanded/`. The body also names the entry's
   statistics cache directory, `__row_order` as the `row_order_column`, and the
   `project_root` Buckaroo searches for klasses. That root is `artifacts/`, so
   Buckaroo finds the display klasses there but not the stats and
   post-processing functions, which tallyman writes under `artifacts/catalog/`
   (#170).
4. Buckaroo creates the session, or answers from the one it holds, and the grid
   connects over a WebSocket. Paging, sorting, search and summary statistics are
   Buckaroo's queries over the build it was handed, which for a worthy entry is a
   read of the snapshot. Buckaroo 0.15.6, the pinned version, ignores
   `row_order_column`, so the grid's pages are not yet ordered by `__row_order`
   (buckaroo-data/buckaroo#974); `/api/data` pages are.
5. Tallyman keeps no record of sessions. Every open posts `/load_expr` again
   with the same id. Buckaroo skips the work while it holds that session with
   the same build directory, and creates it again after dropping it (it drops a
   session idle for an hour, and loses them all on a restart). Two opens at once
   both post, and a promoted diff entry, which sends its column colouring,
   re-runs Buckaroo's statistics on every open (#202).

### Diffing versions

1. The diff page resolves the version pair (by default V_{n-1} against V_n) and
   requests `/{project}/api/diff_data/{alias}/{va}/{vb}`.
2. The companion reads both sides through `cached_result_expr`, so both sides'
   files exist first, and computes the code, schema, statistics, head and keyed
   diffs (`full_diff`). Those summaries still include `__row_order` as a data
   column (#200).
3. For the grid it builds a compare expression: an outer join of the two
   versions on the diff key, with `__row_order` dropped from both sides, a
   membership column (a only, b only, both), and per-column `{col}_eq`,
   `{col}_pct_delta` and `{col}_abs_delta` columns. The expression is memoized
   per pair and key (an LRU of 128, cleared on a reset or recalc).
4. The compare build is posted to Buckaroo as session `diff-<a>-<b>` (the first
   12 characters of each hash), with statistics cached per pair under
   `diff_stat_cache/`. The join is not materialized first, so Buckaroo runs it
   for every query of the diff grid (#188).

`catalog_promote_diff` and the diff page's promote button turn a diff into an
entry of its own, whose recipe calls `build_diff_expr(a_hash, b_hash, keys)`.
It contains a join, so it is worthy and materialized like any other entry.

### Resetting to a revision

`tallyman reset-to <step>` (CLI) and `POST /{project}/api/reset` (companion)
call `reset_to`, described under [Checkpoint and reset](#checkpoint-and-reset).
The companion then clears its in-memory result and compare memos and the
`diff_stat_cache/` directory, reloads the project's Buckaroo sessions, and
publishes `project_reset`. The CLI posts `project_reset` to the companion's
`/internal/notify`, which does the same clean-up. `tallyman revisions` lists the
steps, and `tallyman revisions label <step> <name>` names one.

## Known defects

These open issues describe places where the system does not yet do what the
rest of the docs say it should. The docs describe the current behaviour and cite
the issue where it matters.

Writes, the lock and processes:

- #186: the project lock is one blocking lock with no timeout, so slow work in
  one process blocks page reads in the other.
- #190: `PUT /code` and `POST /promote_diff` build on the companion's event
  loop, which freezes the UI while they wait for the lock or build.
- #183: two tallyman servers on one project are unsupported, and nothing
  detects it.

Row order and diffs:

- #199: the three-way join check also refuses chains of semi and anti joins.
- #200: `full_diff` keeps `__row_order` as a data column.
- #205: the canonical sort leaves nested columns out of its tie-break.
- #206: the snapshot writer drops any column named `__row_order_right`,
  including one the author made.
- #188: the live diff grid hands Buckaroo an unmaterialized join.

Heals and Buckaroo:

- #202: every grid open posts `/load_expr`; concurrent opens load twice, and a
  promoted diff re-runs Buckaroo's statistics on every open.
- #201: a klass reload posts `/reload_expr` once per catalog entry, one after
  another, on the event loop.
- #203: an unfaithful heal runs its checks and the forced Buckaroo reload while
  holding the project lock, and the reload opens a session for an entry nobody
  has open.
- #204: with its manifest gone, an entry's worthiness is guessed from whether a
  snapshot exists, so a worthy entry that has lost both is served as cheap.
- #208: an unfaithful heal of a worthy parent changes its cheap children's rows
  under their hashes; only the parent is flagged.
- #209: a copied project keeps reading the old path through its expanded
  builds.
- #210: the plan memo keeps each loaded build and its backend objects alive.

Design questions still open: #185 (a non-pure recipe's verdict is not recorded
or passed on to entries built on it) and #187 (an ungrouped float `SUM` depends
on the layout of the file it reads, which the snapshot format version pins).
Found while checking these docs (#233): a recipe's
`tracked_expr_from_alias` and `pinned_expr_from_alias` resolve the project from
the `active_project` file, not the MCP session's own project, so the two can
disagree after another session switches projects and a recipe then looks its
aliases up in the other project (related to #39).
Older open issues in the same areas: #118 (concurrent reads can fail with
`Already borrowed`), #170 (Buckaroo is not pointed at the project's stats and
post-processing functions) and #157 (Buckaroo's on-disk statistics cache has not
been seen to give a first-load hit).

Filed on 2026-09-24 against the same code, and described one by one in
[architecture-new.md](architecture-new.md#13-known-defects) and
[plans/open-bugs-2026-09-24.md](../plans/open-bugs-2026-09-24.md): wrong rows or
edges without an error (#228, #229, #231, #232), the import (#224, #225, #227,
#234, #237, #239), writes and leftovers (#226, #230, #240), recalc of a source
entry (#238), the SPA's missing SSE listeners (#235) and ADR-011 leftovers in
the code (#236).

These no longer apply since ADR-011 (a raw input is a source alias, PRs #217 to
#219), because the mechanism each was about is gone: #197 (a parquet source's
copy changed column types; pyarrow writes a source snapshot now), #198 (a CSV's
first read got JSON-rewritten reader options; the options are fixed at import
and must be plain values), #211 and #207 (the ordered copy's `.digest` file and
the copies left behind by source edits; ordered copies no longer exist), #191
(staleness could not resolve a CSV outside `data/`; staleness no longer reads
files), and the two staleness defects the first version of this list named (the
scan wiping the source-digest memo, and a hash-pinned child stale for good on
the source axis).

Fixed on this branch: #193 by #222 (a failed build deleted the snapshot already
on disk for its hash), and #194, #195 and #196 by #223 (a reset could pair a
non-reproducible entry's older manifest with its newer snapshot, a retired
entry's snapshot lost its pin, and the pin from an unfaithful heal lived in
`errors.jsonl`, so dismissing the error banner lifted it).

## Related documentation

Currency notes below reflect a docs-against-code check on 2026-09-22 against
the branch of #189 (ADR-007, ADR-008 and ADR-009 implemented), redone on
2026-09-24 after ADR-011 was merged into it. They will drift; when in doubt, the
code wins.

### Architecture docs (`docs/`): the current system

- [system-contract.md](system-contract.md): **normative**. The invariants and
  rules the system guarantees (identity, reads, writes, materialization, row
  order). Where the code or a descriptive doc disagrees with it, the difference
  is a bug; its last section lists the known ones. **Current.**
- [expression-lifecycle.md](expression-lifecycle.md): one expression from MCP
  ingest to rendered rows, naming every file written and when. **Current.**
- [caching.md](caching.md): every cache in xorq, tallyman and Buckaroo, what it
  saves, what keys it, and what invalidates it. **Current.**
- [reactive-recalc.md](reactive-recalc.md): revise an alias, recompute its
  dependents; the dependency graph, staleness and the cone. **Current.**
- [mcp-server.md](mcp-server.md): every MCP tool and the prompt, with
  parameters, return shapes and side effects. **Current.**
- [installing.md](installing.md): install and run tallyman. **Current.**

### Design records (`plans/ADR-*.md`)

- [ADR-001-git-subprocess-threading.md](../plans/ADR-001-git-subprocess-threading.md):
  calling git from the multithreaded server (fork-free `posix_spawn`).
  **Accepted; mostly current.**
- [ADR-002-source-identity-content-hash.md](../plans/ADR-002-source-identity-content-hash.md):
  content-addressed source clones, so `content_hash` tracks source data.
  **Narrowed by ADR-011:** the clone store stands, and a reset moves
  unreferenced clones to the bullpen instead of deleting them (ADR-007); the
  identity modes, `manifest.sources` and the source-digest memo are gone, and a
  clone is written only by an import.
- [ADR-003-result-cache-cost-rubric.md](../plans/ADR-003-result-cache-cost-rubric.md):
  a cost-against-size cache rubric. **Proposed, not adopted.** The structural
  cheap-or-worthy test it would remove still decides, now as ADR-008's
  allow-list recorded in the manifest; `classify_build` and `ensure_result`,
  which it names, are gone; ADR-007's bare-read chaining addressed its
  motivating case; its budget and eviction half is still open.
- [ADR-004-result-digest-canonical-ordering.md](../plans/ADR-004-result-digest-canonical-ordering.md):
  a canonically ordered snapshot and a digest of it, replacing the per-row
  Python digest (#137). **Partly superseded:** the digest is a content digest of
  the file read back (ADR-009), and the row-index column is `__row_order`,
  written by every writer and appended to every sort (ADR-008).
- [ADR-005-intelligent-csv-import.md](../plans/ADR-005-intelligent-csv-import.md):
  the CSV reader's schema and error contract. **Partly superseded** by ADR-008
  and ADR-011: the column is `__row_order`, the trailing `order_by` is gone, and
  the reader runs inside `catalog_import_source` with its options fixed there;
  the parsed rows are the source entry's snapshot, made again from the clone
  when missing. The schema language, inference ladder and error contract are
  unchanged.
- [ADR-006-read-path-loads-builds.md](../plans/ADR-006-read-path-loads-builds.md):
  reads load the frozen build (#163). **Partly superseded** by ADR-007: ADR-006
  D4 (chaining inlines the parent's cache node) and ADR-006 D8 (the manifest
  records a snapshot key that reads check) are retired, and ADR-006 D5 (the
  canonical sort) is amended by ADR-008 and ADR-009.
- [ADR-007-tallyman-owned-materialization.md](../plans/ADR-007-tallyman-owned-materialization.md),
  [ADR-008-row-order-of-reads.md](../plans/ADR-008-row-order-of-reads.md) and
  [ADR-009-digest-stability.md](../plans/ADR-009-digest-stability.md): the cache
  redesign that this doc and [caching.md](caching.md) describe. Tallyman writes
  its own result files, every file carries `__row_order`, and a re-created file
  is flagged only when the result changed. **Accepted (2026-09-22), implemented
  in #189.** Each has an "Implementation notes" section saying where the code
  differs from its text.
- [ADR-010-immutable-store-one-owner.md](../plans/ADR-010-immutable-store-one-owner.md):
  an immutable result store, every entry materialized, one owning process.
  **Rejected (2026-09-22)**; kept for the record. ADR-007, ADR-008 and ADR-009
  stand.
- [ADR-011-sources-are-aliases.md](../plans/ADR-011-sources-are-aliases.md):
  a raw input is a source alias whose versions are entries, a file enters only
  by `catalog_import_source`, recipes name aliases and never bare hashes, and
  staleness has one axis. **Accepted (2026-09-22), implemented** in #217, #218
  and #219, merged into #189's branch. Its implementation notes record where the
  code differs from the decisions.

### Plans (`plans/`)

- [native-catalog-store.md](../plans/native-catalog-store.md): the native
  catalog store that replaced xorq's catalog package. **Mostly current:**
  `compute_cache.jsonl` and the reset's compute-cache prune are gone (ADR-007).
- [recalc-mechanism.md](../plans/recalc-mechanism.md): how reactive recalc
  works. **Partly stale:** auto-recalc on revise now exists, and new data
  arrives by an import, which advances a source alias like a revise; there is
  no source axis.
- [auto-recalc-on-revise.md](../plans/auto-recalc-on-revise.md): atomic
  auto-recalc on revise. **Implemented; partly stale:** function names drifted
  (`_recalc_walk` is `_replay_cone`), and its "future Stage C" SSE listener
  shipped.
- [remove-ondemand-result-parquet.md](../plans/remove-ondemand-result-parquet.md):
  removing the on-demand `result.parquet` layer (#104). **Partly stale:** the
  single materialized copy is now tallyman's snapshot, not xorq's cache
  (ADR-007).
- [project_switcher.md](../plans/project_switcher.md): the project switcher.
  **Mostly current:** the home root is `~/.tallyman-notebooks/`, and there is
  no Buckaroo session file (ADR-007).
- [89-determinism-prereqs-execution.md](../plans/89-determinism-prereqs-execution.md):
  clearing #89's determinism prerequisites. **Point in time;** the digest and
  heal verification it describes were replaced by ADR-007 and ADR-009.
- [cache-soundness-audit.md](../plans/cache-soundness-audit.md): an inventory
  of the #163 bug class, 2026-07-30. **Point in time;** #167 and #189 changed
  several findings.
- [catalog-xorq-integration-tests.md](../plans/catalog-xorq-integration-tests.md):
  coexistence and reset integration tests. **Partly stale:** it refers to the
  old `catalog.yaml` and `aliases.json` formats.
- [llm-summary-stats.md](../plans/llm-summary-stats.md): LLM-authored summary
  stats. **Partly stale:** Buckaroo's `_Generated_*` classes are now a `@stat()`
  decorator, a few signatures and the notify `kind` differ, and the stats are
  not found by Buckaroo (#170).

### Research notes and experiment logs (`plans/`, `demo/`, `docs/research/`)

Point-in-time records. The digest investigation behind #137:
[datafusion-scan-order-findings.md](../plans/datafusion-scan-order-findings.md)
(why a parallel DataFusion scan emits rows in a different order each run, and
the decision to ingest with polars, which stands for CSV: an import parses a CSV
with polars, and pyarrow copies a parquet file in file order) and
[result-digest-vs-xorq-staleness.md](../plans/result-digest-vs-xorq-staleness.md)
(why `result_digest` cannot reuse xorq's cache staleness). Both have a status
note for what #189 changed.

Older, historical:
[eda-prompt-research.md](../plans/eda-prompt-research.md),
[eda-codegen-run1.md](../plans/eda-codegen-run1.md),
[haiku-codegen-findings.md](../plans/haiku-codegen-findings.md),
[model-codegen-comparison.md](../plans/model-codegen-comparison.md),
[plotting-testcases.md](../plans/plotting-testcases.md),
[xorq-sklearn-assessment.md](../plans/xorq-sklearn-assessment.md),
[ds-demo-scripts.md](../plans/ds-demo-scripts.md),
[demo/datasets.md](../demo/datasets.md), and the CSV importer research under
[docs/research/csv-importers/](research/csv-importers/). Some of them mention
the removed `result.parquet`, xorq's result cache and the old `~/.tallyman/`
path; `ds-demo-scripts.md` has a status note saying so.

### Root and meta

- [README.md](../README.md): what tallyman is, and how to run it. **Current.**
- [tallyman_explanation.md](../tallyman_explanation.md): the owner's framing of
  tallyman, a feature list and a walkthrough. **Current** in the sections
  written from the code; the owner's own sections are his and are not checked.
- [proposal.md](../proposal.md): the talk pitch. **Current** (a pitch, not a
  spec).

The original `plan.md` (V0.6 plan) and `TICKETS.md` (V0 punch list) were removed
as stale: they predated the React SPA, the `result.parquet` removal and the
JSONL catalog format. This doc replaces them as the architecture reference.

> Gaps: there is no dedicated reference for the REST API, the CLI, or the
> frontend. The MCP tool surface, including the extension points it exposes
> (display klasses, summary stats, post-processing), is covered in
> [mcp-server.md](mcp-server.md).
