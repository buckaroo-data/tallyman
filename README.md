# tallyman-notebooks

Spike for the Tallyman London 2026 talk *"The Future of Notebooks in a Claude Code World"*.
The proposal lives in `proposal.md`. This README covers the V0 spike only.
For the system architecture — subsystem map, on-disk layout, data-flow paths, and an
index of all the docs — start with [docs/architecture.md](docs/architecture.md).

## V0 scope

End-to-end: a Claude Code MCP tool that compiles a xorq expression, materializes
a result to a content-hashed catalog entry on disk, and pushes a live update
to a browser companion via SSE.

What's working:

- **MCP tools** (FastMCP over stdio):
  - Catalog: `catalog_run`, `catalog_load_parquet`, `catalog_create`,
    `catalog_revise`, `catalog_alias`, `catalog_rename`, `catalog_unalias`,
    `catalog_list`, `catalog_diff`, `catalog_chart`, `catalog_recalc`, plus the
    summary-stat / post-processing / display-klass authoring tools.
  - Notebook: `notebook_reorder`, `notebook_remove`, `notebook_edit_markdown`.
  - Project: `project_list`, `project_new`, `project_switch`.

    See [docs/architecture.md](docs/architecture.md) for the full tool surface.
- **Companion** (FastAPI on `:7860`) — serves the React SPA
  (`packages/app/dist`) as a catch-all and exposes a JSON API + SSE under
  `/{project}/api/*`:
  - SPA tabs: **Catalog** (entry list + detail with V_n chips and forensic
    history), **Notebook** (curated narrative anchored on aliases, drag-reorder,
    inline markdown editor, × remove), **Diff** (code diff, schema diff,
    per-column stats, key-joined side-by-side, head() side-by-side), **Cache**
    (per-entry cache footprint), and **Log** (linear, filterable activity view).
  - JSON: `/{project}/api/{entries,entry/<hash>,aliases,notebook,errors,log,
    data/<hash>,diff_data/...,disk_usage,result_cache,staleness}`, plus the
    mutation routes (`PATCH notebook`, `PUT code/<alias>`,
    `PUT markdown/<cell_id>`, `POST reset`, `POST recalc`,
    `POST promote_diff/...`).
  - `/{project}/api/sse` — live updates (`new_entry`, `build_failed`,
    `alias_changed`, `notebook_changed`, `recalc`, `summary_stat_changed`).
  - `/internal/notify` — the MCP server's notification hook; fans out to SSE.
- **Buckaroo subprocess** — `tallyman run` spawns `python -m buckaroo.server`
  on `:8700` (falls back to a random port if busy), watches for the
  `BUCKAROO_PORT=...` handshake, and lazily creates per-entry sessions on
  first view by POSTing the entry's `xorq_build/` dir to Buckaroo's
  `/load_expr` endpoint (PR 776) — sort/search push down to the xorq
  backend rather than paging over a materialised parquet. The build dir is
  expanded into a stable per-entry path (`.xorq_build_expanded/`, gated by a
  `.complete` marker) so `${TALLYMAN_PROJECT_ROOT}` placeholders are resolved
  before xorq's loader sees them. Sessions are persisted in a global
  `~/.tallyman-notebooks/buckaroo_sessions.json` (keyed by content hash, shared
  across projects) and invalidated by start-time when Buckaroo restarts.
  Tear-down rides along with the companion. Disable with `--no-buckaroo`.
- **Build artifacts are portable.** xorq's absolute filesystem paths are
  rewritten to `${TALLYMAN_PROJECT_ROOT}` on write and expanded back on load.
- **`tallyman serve <project_dir>`** — read-only companion against a project
  directory that may live anywhere on disk. Mutation routes return 403.

What's NOT yet implemented:

1. Column-level lineage (xorq has the data; there is no lineage view today).
2. ML training pipeline (storyboard beats 7-8).

## Running the spike

Build the React companion UI once. The FastAPI server serves its `dist/`; Node +
pnpm are install-time prerequisites and the build artifact is not committed:

```sh
cd packages/app && pnpm install && pnpm build   # writes packages/app/dist/
```

Then start the stack:

```sh
uv sync
uv run tallyman init spike            # creates ~/.tallyman-notebooks/projects/spike/ + fixture
uv run tallyman run --project spike   # edit-mode companion on http://127.0.0.1:7860
```

In another terminal, launch Claude Code from this directory; it picks up
`.mcp.json` and exposes the `tallyman` MCP server.

Recommended prompts:

> Use catalog_load_parquet to load `orders.parquet`.
>
> Now use catalog_create to make a named entry `shoe_sales` that groups orders
> by region and totals the price.
>
> Now revise shoe_sales to filter to category == "boots" only.

Watch the browser update live as each tool call lands. Named entries float to
the top of the catalog with a V_n chip; the previous version sticks around in
forensic history.

### Serving a project as an artifact

Once you've authored a project, pack it and hand it off:

```sh
uv run tallyman pack spike -o my-project.tgz   # portable .tgz, ${TALLYMAN_PROJECT_ROOT} preserved
# colleague extracts somewhere
tar xzf my-project.tgz -C ~/projects/
uv run tallyman serve ~/projects/spike
```

The companion runs read-only: same catalog, same forensic history, no edit
affordances. Mutation routes return 403.

## Conventions worth knowing

- xorq 0.3.x reads use `xo.deferred_read_parquet` (NOT `xo.read_parquet` — that
  resolves through ibis's backend loader and fails). Use
  `import xorq.api as xo` and `import xorq.vendor.ibis as ibis`. Do NOT
  `import ibis` directly.
- Prefer `from tallyman_xorq.io import read_project_file; t = read_project_file("name.parquet")`
  over absolute paths — the catalog records project-relative intent and the
  build is portable across machines/users.
- Content hash is xorq's build hash — same code + same inputs → same hash → same
  entry dir (idempotent).
- All catalog state lives on disk. The MCP server holds no in-memory state; the
  companion only holds the SSE subscriber list.
- `TALLYMAN_PROJECT_PATH` overrides project_dir() resolution for the active project.
  Used by `tallyman serve` to point at a project directory anywhere on disk.

## Tests

```sh
uv run pytest                       # full suite
uv run pytest tests/test_pack.py    # the pack / portability proof
```
