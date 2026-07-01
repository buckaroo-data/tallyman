# Tallyman MCP server reference

The MCP server (`src/tallyman_mcp/server.py`) is the surface Claude Code drives.
It is a [FastMCP](https://github.com/jlowin/fastmcp) server that exposes the
catalog as **30 tools and one prompt** over stdio. Claude Code is the only
caller; a human never invokes these directly. Each tool compiles or mutates the
on-disk catalog through `tallyman_core` / `tallyman_xorq`, commits a git
revision, and best-effort notifies the running companion so the browser updates.

This doc lists every tool, its parameters, its return shape, and — the part that
matters most when reasoning about a session — its **side effects**: whether it
checkpoints, which SSE event it pushes, and whether it cascades a recalc. For
where the MCP server sits in the larger system, see
[architecture.md](architecture.md).

## How it is launched

Claude Code reads `.mcp.json` at the repo root, which registers one server named
`tallyman`:

```json
{ "command": "uv", "args": ["run", "tallyman", "mcp"],
  "cwd": "/Users/paddy/tallyman",
  "env": { "TALLYMAN_PROJECT": "spike",
           "TALLYMAN_COMPANION_URL": "http://127.0.0.1:7860" } }
```

So Claude Code spawns `uv run tallyman mcp`. The `tallyman` entry point is
`tallyman_cli.main:cli` (from `pyproject.toml`); the `mcp` subcommand
(`run_mcp` in `src/tallyman_cli/main.py`) calls `tallyman_mcp.main()`, which ends
in `mcp.run()`. With no transport argument FastMCP defaults to **stdio**, so
Claude Code talks to the server over the spawned process's stdin/stdout — one
MCP process per Claude Code session, owned by Claude Code, not by the companion.

The server reaches the companion through `COMPANION_URL`
(`os.environ.get("TALLYMAN_COMPANION_URL", "http://127.0.0.1:7860")`), firing
best-effort `POST {COMPANION_URL}/internal/notify` calls so the FastAPI companion
refreshes Buckaroo sessions and pushes SSE to open browsers. The companion need
not be up; notifications that fail are logged and dropped.

## Cross-cutting behavior

Four mechanisms apply to (almost) every tool, so they are documented once here
rather than repeated per tool.

**Auto-checkpoint (opt-out).** `mcp.tool` is monkeypatched to
`_checkpointing_tool`, which registers every tool wrapped in `_with_checkpoint`.
After a tool returns, unless its name is in the `_NO_CHECKPOINT` set or the
result carries an `error` key, the wrapper calls
`checkpoint_catalog(project, "tallyman: <tool>")` — **one git revision per
operation**, even when the tool called several `tallyman_core` mutators. A
checkpoint failure is logged, never raised. Opt-out (not opt-in) means a tool
added later is covered automatically; coverage is pinned by
`tests/test_reset_to_revision.py`. The opt-out set is exactly:

```
catalog_list, catalog_diff, catalog_scan_staleness, catalog_recalc,
catalog_promote_diff, catalog_list_summary_stats, catalog_list_post_processings,
catalog_run_post_processing, catalog_export_marimo,
project_list, project_switch, project_new
```

(`catalog_recalc` and `catalog_promote_diff` are on the list because they
*self*-checkpoint; the rest are read-only or lifecycle ops.)

**Response tagging.** `_tag_project` wraps every response: it adds a `project`
key (the active project) via `setdefault`; a tool that returns a bare list
(the `catalog_list*` family) is rewrapped as `{project, items: [...]}`; and when
the active project changed since the previous tool call, a `warning` field is
added. The per-tool "Returns" notes below describe the body's shape before this
tagging.

**Notifications.** `_notify(kind, hash, **extra)` is a 2-second best-effort POST
to the companion's `/internal/notify`, which re-publishes it as an SSE event of
that `kind`. It never raises. Pass extra data as flat kwargs (`remap=...`), not
`extra={...}`, or it nests and is silently dropped. The "Notifies" line per tool
lists the kinds it emits.

**Sticky active project.** `_resolve_active_project()` returns the in-process
`_mcp_active_project` (set by `project_switch` / `project_new`) ahead of the
on-disk `active_project` file, so switching once is sticky for the rest of the
session regardless of what another session writes to disk. The first tool call
seeds it. `SESSION_ID` (an 8-hex uuid) tags this process in `events.jsonl`.

**Auto-recalc.** `catalog_revise` and `catalog_promote_diff` advance an existing
alias head, which can make followers stale. Both route through
`_auto_recalc_after_head_advance` (when the project's `auto_recalc` switch is on):
a checkpoint-free cascade rebuilds the followers in dependency order and leaves
them in the working tree, so the head advance and the whole cascade land as one
git revision. A non-empty cascade emits a `recalc` SSE event.

## Side-effect matrix

| Tool | Checkpoints | SSE notify | Auto-recalc |
|---|---|---|---|
| `catalog_run` | yes | `new_entry`, `build_failed` | no |
| `catalog_load_parquet` | yes | `new_entry`, `build_failed`, `notebook_changed`¹ | no |
| `catalog_create` | yes | `new_entry`, `build_failed`, `notebook_changed` | no |
| `catalog_revise` | yes | `new_entry`, `build_failed`, `recalc` | yes |
| `catalog_alias` | yes | `alias_changed`, `notebook_changed` | no |
| `catalog_rename` | yes | `alias_renamed`, `notebook_changed` | no |
| `catalog_unalias` | yes | `alias_changed`, `notebook_changed`² | no |
| `notebook_reorder` | yes | `notebook_changed` | no |
| `notebook_remove` | yes | `notebook_changed` | no |
| `notebook_edit_markdown` | yes | `notebook_changed` | no |
| `catalog_chart` | yes | `chart_attached` | no |
| `catalog_diff` | no | — | no |
| `catalog_promote_diff` | self³ | `entry_added`, `recalc`⁴ | yes |
| `catalog_scan_staleness` | no | — | no |
| `catalog_recalc` | self³ | `recalc`⁴ | yes |
| `catalog_add_summary_stat` | yes | `summary_stat_changed` | no |
| `catalog_remove_summary_stat` | yes | `summary_stat_changed` | no |
| `catalog_list_summary_stats` | no | — | no |
| `catalog_add_display_klass` | yes | `display_changed` | no |
| `catalog_list_display_klasses` | yes ⚠ | — | no |
| `catalog_remove_display_klass` | yes | `display_changed` | no |
| `catalog_add_post_processing` | yes | `post_processing_changed` | no |
| `catalog_remove_post_processing` | yes | `post_processing_changed` | no |
| `catalog_list_post_processings` | no | — | no |
| `catalog_run_post_processing` | no | — | no |
| `catalog_export_marimo` | no | — | no |
| `catalog_list` | no | — | no |
| `project_list` | no | — | no |
| `project_switch` | no | — ⁵ | no |
| `project_new` | no | — ⁵ | no |

¹ `notebook_changed` only when `name` is supplied. ² only when a cell was
actually removed. ³ self-checkpoints exactly one revision; on the opt-out list so
the dispatch boundary does not double-commit. ⁴ only on a committing run that
produced a non-empty remap. ⁵ the *companion* broadcasts `project_switched`; the
MCP tool itself sends no `_notify`. ⚠ see [Known quirks](#known-quirks).

Any tool whose result carries an `error` key skips its checkpoint and sends no
notify; failures come back inline in the response (most as `{error, error_id}`
for build failures, `{error}` for validation/precondition failures), not as
raised exceptions.

---

## Authoring tools

The compile-and-persist family. The `code` argument is a self-contained Python
script that binds a top-level `expr` to a xorq/ibis expression;
`catalog_run`'s docstring is the canonical cookbook for writing it (namespaces,
the datafusion-only backend, data sourcing, and the `xorq.ml` MODELING section).
Data sourcing inside `code` uses four helpers from `tallyman_xorq.io`:
`tracked_expr_from_alias` (a catalog entry, recorded as a lineage parent),
`pinned_expr_from_alias` (a catalog entry by alias or hash, no following),
`read_project_file` (a raw parquet under `data/`), and `tallyman_read_csv` (CSV
ingest — injects an `original_row_order` column so the baked snapshot is
byte-stable across builds; use it for all CSV reads instead of
`xo.deferred_read_csv`, #137).

### `catalog_run(code, prompt="") -> dict`
Execute an expression and persist it as an **unnamed (scratch)** entry. Claude's
default authoring tool for a one-off run.
- **Params:** `code` (required) — script binding `expr`; `prompt` — optional
  intent string, recorded on the entry.
- **Returns:** `{hash, row_count, execute_seconds, schema, entry_path, url}`,
  plus `lint_warnings` when the nondeterminism lint fired. `{error, error_id}`
  on a `BuildError`.
- **Writes:** the entry build dir under `catalog/entries/<hash>/`; a `build_ok`
  or `build_error` event to `events.jsonl`.
- **Promote** a scratch entry to a name afterward with `catalog_alias`.

### `catalog_load_parquet(rel_path, prompt="", name="") -> dict`
Register a parquet under `<project>/data/` as an entry by synthesizing a
`read_project_file(rel_path)` recipe — the no-code "load this file" path. With
`name`, behaves like `catalog_create` (names the entry, appends a notebook cell).
- **Params:** `rel_path` (required) — path under `data/`; `prompt` — optional
  intent / cell markdown; `name` — optional alias.
- **Returns:** same as `catalog_run`, plus `alias` and `version` when named.
  `{error}` if the alias already exists; `{error, error_id}` on a missing file.
- Unlike `catalog_run`, it does **not** emit a `build_ok` event; success shows
  only via the notifies.

### `catalog_create(name, code, prompt="") -> dict`
Execute and persist as a **named** entry (alias) and append a notebook cell.
Called when the user names a concept ("name this `shoe_sales`").
- **Params:** `name` (required) — new alias, must not exist; `code` (required);
  `prompt` — optional, recorded on the `alias_set` event and used as cell
  markdown.
- **Returns:** `catalog_run` shape plus `alias`, `version`. `{error}` if the
  alias exists (steers to `catalog_revise`); `{error, error_id}` on build
  failure.

### `catalog_revise(name, code, prompt="") -> dict`
Persist a **new version** of an existing alias: build, repoint the alias head,
carry chart/display config forward, and cascade-recompute the alias's stale
followers. The canonical head-advance path.
- **Params:** `name` (required) — existing alias; `code` (required) — a
  self-contained recipe that **must not reference its own alias by name**
  (rejected, #135 — inline the source or pin the previous version by hash);
  `prompt` — optional.
- **Returns:** `catalog_run` shape plus `alias`, `version`; `carried_over`
  (subset of `chart` / `display_config`) when non-empty; and a `recalc`
  sub-report when the project's `auto_recalc` is on. `{error}` if the alias is
  absent; `{error, error_id}` on build failure or a self-alias rejection.
- The head advance and the entire follower cascade commit as **one** git
  revision; the `recalc` notify fires only when the cascade produced a non-empty
  remap.

## Alias tools

None recompute anything; they only move alias pointers and re-anchor notebook
cells.

### `catalog_alias(hash, name) -> dict`
Promote a scratch entry (by content hash) to a named alias and append a notebook
cell. Used after a `catalog_run` when a scratch entry earns a permanent name.
- **Params:** `hash` (required) — must name an on-disk entry; `name` (required)
  — must be free.
- **Returns:** `{hash, alias, version, url}`. `{error}` if the hash has no entry
  or the name is taken.

### `catalog_rename(old_name, new_name) -> dict`
Rename an alias, preserving its full version history and notebook position.
- **Params:** `old_name` (must exist), `new_name` (must be free).
- **Returns:** `{old, new, hash}`. `{error}` if `old_name` is absent or
  `new_name` is taken.

### `catalog_unalias(name) -> dict`
Drop an alias (its entries revert to scratch; the entry dirs are untouched) and
remove any notebook cells anchored on it.
- **Params:** `name` (must exist).
- **Returns:** `{removed_alias, notebook_cells_removed}`. Emits
  `notebook_changed` only when a cell was actually removed.

## Notebook tools

All three act on the project's single `default` notebook
(`catalog/notebook.jsonl`); `cell_id` is the 12-hex id created when an alias is
added to the notebook. A missing cell returns
`{error: "no cell with id '<cell_id>'"}` and skips the checkpoint.

### `notebook_reorder(cell_id, new_index) -> dict`
Move a cell to a 0-based position. `new_index` is clamped to range, but the
response echoes the raw argument, so an out-of-range value reports verbatim while
the cell lands at the end.
- **Returns:** `{cell, new_index}`.

### `notebook_remove(cell_id) -> dict`
Remove a cell; the underlying catalog entry is untouched. Removing the last cell
deletes `notebook.jsonl` outright.
- **Returns:** `{removed: <cell_id>}`.

### `notebook_edit_markdown(cell_id, markdown) -> dict`
Replace the markdown shown above a cell, wholesale (not a merge). An empty
string clears it.
- **Returns:** `{cell}` (the updated cell).

## Chart and diff tools

### `catalog_chart(hash_or_alias, vega_spec) -> dict`
Attach a Vega-Lite v5 spec to an entry so the companion renders a chart above the
table. The target resolves as a hash first, then as an alias (to its latest
hash).
- **Params:** `hash_or_alias` (required); `vega_spec` (required) — dict or JSON
  string, stored as-is with **no** server-side schema validation (a malformed-
  but-JSON spec only fails in the browser).
- **Returns:** `{hash, spec_path}`. `{error}` for an unknown target or invalid
  JSON.
- **Writes:** `catalog/chart_specs/<hash>.vl.json`.

### `catalog_diff(name, va=-2, vb=-1) -> dict`
**Read-only** diff of two versions of an alias (default previous vs latest).
Each side's expression comes from `cached_result_expr`, so it does not depend on
a materialized result existing.
- **Params:** `name` (required); `va`, `vb` — version indices, 1-based or
  negative-from-end (`-1` latest, `-2` penultimate).
- **Returns:** `{alias, before:{version,hash}, after:{version,hash}, schema,
  stats, keyed_summary}`. The heavyweight `table_html` is stripped; `code` and
  `head` diffs are computed but not returned. `{error}` for no history /
  out-of-range / missing-on-disk.
- On the opt-out list: never checkpoints, never notifies. For the visual,
  promotable form, use `catalog_promote_diff`.

### `catalog_promote_diff(name, va=-2, vb=-1, alias=None) -> dict`
Promote a version-over-version diff into a **first-class catalog entry** whose
expression is the keyed full-outer-join comparison (with Buckaroo diff coloring).
Unlike `catalog_diff`, the result is post-processable, chartable, and
marimo-exportable.
- **Params:** `name` (required); `va`, `vb` — version indices; `alias` — name for
  the new entry (default `diff_{name}_v{a}_v{b}`); if it already exists it is
  re-pointed (which can stale its followers and trigger the cascade).
- **Returns:** `{alias, hash, source_alias, va, vb, a_hash, b_hash, keys,
  row_count}`, plus a `recalc` sub-report only when an existing alias was
  re-pointed and `auto_recalc` is on. `{error}` for no history / out-of-range /
  no stable join key / build failure.
- **Self-checkpoints**: the checkpoint-free cascade runs before its own
  `checkpoint_catalog`, so the promote and cascade land in one revision (hence
  its place on the opt-out list). Always emits `entry_added`; emits `recalc` only
  on the re-point-with-cascade branch.

## Staleness and recalc tools

### `catalog_scan_staleness() -> dict`
**Read-only** scan of every live entry for staleness against its recorded inputs.
Purely diagnostic — never rebuilds, repoints, or checkpoints. Call it first to
see what is stale.
- **Returns:** `{stale, transitively_stale, entries, orphan_stale}`. `stale` is
  the default root set `catalog_recalc` uses. `orphan_stale` populates only under
  auto-recalc mode.

### `catalog_recalc(roots=None, dry_run=True) -> dict`
Recompute stale entries and their dependents in dependency order. Defaults to a
non-committing **dry-run preview**; call again with `dry_run=False` to commit.
- **Params:** `roots` — content hashes to recompute with their descendant cone;
  `None` defaults to every directly-stale entry. Roots naming no live entry are
  silently dropped. `dry_run` — preview when `True` (default).
- **Returns:** the `RecalcReport` (`project, roots, cone, entries, dry_run,
  status, remap, checkpoint_step, error`). Action vocabulary differs by mode
  (`rebuild`/`cascade`/`unchanged` on dry-run; `rebuilt`/`noop`/`failed`/
  `skipped` on a real run). A nothing-stale fast path returns a short
  `{status:'ok', ..., note:'nothing stale to recompute'}`.
- **Self-checkpoints arg-aware:** a dry-run takes no checkpoint; a committing run
  takes exactly one for the whole walk, so the recompute is a single revision
  `reset-to` can undo atomically. A committing run with a non-empty remap emits
  `recalc`. Build failures are encoded in `status`/`error`/per-entry `error`
  (the walk halts, the rebuilt prefix stays committed); a dependency cycle yields
  `status=='cycle'` rather than raising.

## Summary-stat tools

Project-authored per-column statistics. Source runs in a restricted-globals
sandbox; errors come back inline rather than later in the Buckaroo log. There is
no separate update tool — re-adding the same `name` overwrites.

### `catalog_add_summary_stat(name, source) -> dict`
Validate a `compute(col)` function against a 1-row ibis memtable, write it to
`<project>/stats/<name>.py`, and hot-reload it into open Buckaroo sessions.
- **Params:** `name` (required) — a valid Python identifier; `source` (required)
  — must define `compute(col)` taking one ibis column and returning an ibis
  scalar.
- **Returns:** `{name, path}`. `{error}` on any validation failure (bad
  identifier, syntax error, exec error, missing/ wrong-arity `compute`,
  non-ibis return).
- To surface a stat as a pinned row in the main table (not just the summary
  panel), follow up with `catalog_add_display_klass`.

### `catalog_remove_summary_stat(name) -> dict`
Soft-delete by moving `stats/<name>.py` to `stats/_disabled/<name>.py` (the `_`
prefix is what Buckaroo's scanner skips). Idempotent.
- **Returns:** `{name, moved_to}`. `{error}` if no such active stat.

### `catalog_list_summary_stats() -> list`
List every stat, active first then disabled, each with `{name, path, source,
disabled}`. Read-only (on the opt-out list). The returned `source` lets Claude
read/edit before overwriting.

## Display-klass tools

Project-authored `ColAnalysis` subclasses (with a string `df_display_name`) that
control column rendering and pinned rows. Source is exec'd with the Buckaroo
styling base classes already in scope; validated in a restricted sandbox before
the file lands.

### `catalog_add_display_klass(name, source) -> dict`
Validate and persist a display klass to `display/<name>.py`, then hot-reload it
into open sessions (`display_changed`). The primary use is extending
`DefaultMainStyling` (`df_display_name='main'`) or `DefaultSummaryStatsStyling`
(`'summary'`) with a `pinned_rows` entry so a stat shows as a frozen row.
- **Params:** `name` (required) — valid identifier; `source` (required) — must
  define a `ColAnalysis` subclass carrying a string `df_display_name`.
- **Returns:** `{name, path}`. `{error}` on any validation failure.

### `catalog_list_display_klasses() -> list`
List every display klass (active first, then disabled), each with `{name, path,
source, disabled}`. **See [Known quirks](#known-quirks):** unlike the other
`*_list_*` tools this one is *not* on the opt-out list, so it commits an empty
git revision on each call.

### `catalog_remove_display_klass(name) -> dict`
Soft-delete by moving `display/<name>.py` to `display/_disabled/`. Re-adding
restores it. Emits `display_changed`.
- **Returns:** `{name, moved_to}`. `{error}` if no such active klass.

## Post-processing tools

Project-authored `process(expr)` functions that transform a whole result table,
selectable in the Buckaroo embed's "post processing" dropdown. Same restricted
sandbox as summary stats (third-party imports like numpy/sklearn raise
`ImportError`; fit models via the `xorq.ml` path on `catalog_create` instead).

### `catalog_add_post_processing(name, source) -> dict`
Validate a `process(expr)` function and write it to
`<project>/post_processing/<name>.py`.
- **Params:** `name` (required) — valid identifier, becomes the dropdown label;
  `source` (required) — defines `process(expr)` returning an ibis expression or a
  pandas DataFrame.
- **Returns:** `{name, path}`. `{error}` on validation failure.
- **Validator gotcha:** the dry-run table has only columns `{a, b}`, so a
  `process` that references real column names is rejected here even when
  `catalog_run_post_processing` previewed it fine — guard with
  `if 'col' not in expr.columns: return expr`. The new option appears only on the
  next session load (V1 does not hot-swap loaded sessions).

### `catalog_remove_post_processing(name) -> dict`
Soft-delete to `post_processing/_disabled/`. Still shows in the list with
`disabled=true`. Emits `post_processing_changed`.
- **Returns:** `{name, moved_to}`. `{error}` if no such active function.

### `catalog_list_post_processings() -> list`
Read-only inventory: `{name, path, source, disabled}` per function, active first.
On the opt-out list.

### `catalog_run_post_processing(code, entry) -> dict`
**Preview** a `process(expr)` against a real entry's result without persisting
anything — the iterate-before-commit tool. Note the arg is `code` here, not
`source`.
- **Params:** `code` (required) — `process(expr)` source; `entry` (required) —
  alias or content hash to test against.
- **Returns:** `{entry, row_count, columns, preview}` (first 20 rows). `{error}`
  on any run failure.
- Reads the entry's actual result via `cached_result_expr` (a cheap entry
  recomputes; an expensive one reads its baked snapshot, self-healing if
  evicted), so the preview reflects real data — wider than `add`'s `{a, b}`
  dry-run. Persists nothing; on the opt-out list, no notify.

## Export and listing tools

### `catalog_export_marimo() -> dict`
Export the default notebook to a runnable Marimo file at
`<project_dir>/notebook_marimo.py` (also downloadable via the companion's
`/export/marimo` route). Writes an artifact, not a revision (on the opt-out
list).
- **Returns:** `{path, cells}`. `cells` is the **total** number of cells in the
  default notebook — cells with an unresolved alias or a missing `expr.py` are
  counted here but silently skipped during export, so the file may contain fewer.
  `{error}` wraps any render/write failure (including a corrupt `notebook.jsonl`,
  which is caught, and a `FileNotFoundError` from a promoted-diff cell whose
  source `expr.py` is missing).

### `catalog_list() -> list`
Enumerate the active project's catalog, most-recent-first. Each item is the
entry's full `manifest.json` plus `alias` and `version` (when the hash is an
alias head, else `None`) and `columns` (a compact `"name:type, ..."` string).
Read-only (opt-out). Includes scratch entries. Per its docstring, Claude should
call this before writing an expression against an entry it did not just build, so
it references exact column names rather than guessing.

## Project-lifecycle tools

These require the companion (`tallyman run`) to be active: the **companion** is
the sole writer of the `active_project` file and the genesis baseline, reached
via `_companion_post`. All three are on the opt-out list. On success the
companion broadcasts a `project_switched` SSE event itself (the MCP tool sends no
`_notify`). Each updates the in-process sticky `_mcp_active_project`.

### `project_list() -> dict`
List projects on disk and report the active one. **Safe even when the companion
is down** — it reads disk directly.
- **Returns:** `{active, available}`. Never errors (`available` is `[]` if no
  projects root). Call before `project_switch` to learn the legal `name` values.

### `project_switch(name) -> dict`
Retarget all subsequent `catalog_*` / `notebook_*` calls at another existing
project, by POSTing to the companion. Pins the sticky project on success.
- **Params:** `name` (required) — must exist and pass the companion's name
  validation.
- **Returns:** `{previous, active}`. On failure `{error}` (sticky unchanged):
  e.g. `companion not reachable at <url>: <exc>. Project lifecycle tools require
  'tallyman run' to be active.`, or `companion at <url> returned <status>: ...`
  (400 invalid/reserved name, 404 not found, 403 serve mode).

### `project_new(name, with_fixture=False) -> dict`
Create a new project on disk (the companion runs `ensure_project` + genesis
baseline) and switch to it.
- **Params:** `name` (required) — `^[a-z0-9][a-z0-9_-]{0,31}$`, not reserved, not
  colliding; `with_fixture` — when `True`, the companion seeds the shoe-orders
  demo parquet into `data/`.
- **Returns:** `{name, active}`. On failure `{error}` (sticky unchanged): same
  shapes as `project_switch`, with 409 on a name collision.

## The MCP prompt

### `ds_modeling_workflow(dataset, target="") -> str`
An `@mcp.prompt()` (not a tool) that returns a static guidance string scaffolding
a modeling workflow: load → engineer features → split → fit via `xorq.ml` →
score → chart. Pure function — no side effects, no I/O, not routed through the
checkpoint or tagging wrappers.
- **Params:** `dataset` (required) — a parquet under `data/`, interpolated into
  the text; `target` — column to predict (empty renders as `<target column>` for
  unsupervised work).
- It encodes the hard constraints the model must follow: fit the model *as a
  catalog entry* with `xorq.ml` (never in a post-processing sandbox, which blocks
  sklearn/scipy/numpy); copy exact fit/predict signatures from the MODELING
  section of `catalog_run`'s docstring; score in pure ibis (`deferred_sklearn_metric`
  does not survive a catalog build); `import xorq.vendor.ibis as ibis` and
  `import xorq.api as xo`; datafusion is the only backend; check columns with
  `catalog_list` first; no `project=` kwargs.

## Known quirks

- **`catalog_list_display_klasses` checkpoints.** It is the one read-only `*_list*`
  tool missing from the `_NO_CHECKPOINT` set (`server.py`), so `_with_checkpoint`
  runs `checkpoint_catalog`, which commits with `--allow-empty` and advances the
  step tag. Each call therefore appends an **empty** catalog revision — harmless
  to correctness but it inflates the git history and the step count. Adding it to
  `_NO_CHECKPOINT` (next to `catalog_list_summary_stats` /
  `catalog_list_post_processings`) would bring it in line with the other list
  tools.
