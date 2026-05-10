# pydata-app

Spike for the PyData London 2026 talk *"The Future of Notebooks in a Claude Code World"*.
The plan and proposal live in `plan.md` / `proposal.md`. This README covers the V0 spike only.

## V0 scope

End-to-end: a Claude Code MCP tool that compiles a xorq expression, materializes
a parquet to a content-hashed catalog entry on disk, and pushes a live update
to a browser companion via SSE.

What's working:

- **MCP tools:**
  - Catalog: `catalog_run`, `catalog_load_parquet`, `catalog_create`,
    `catalog_revise`, `catalog_alias`, `catalog_rename`, `catalog_unalias`,
    `catalog_list`, `catalog_diff`.
  - Notebook: `notebook_reorder`, `notebook_remove`, `notebook_edit_markdown`.
- **Companion** (FastAPI on `:7860`):
  - `/catalog`, `/catalog/<hash>` or `/catalog/<alias>` — entry list and detail
    with V_n chips, forensic history, and a link to internal lineage.
  - `/notebook` — curated narrative: cells anchored on aliases, vertical layout,
    inline markdown editor, ↑/↓ reorder, × remove.
  - `/lineage` and `/lineage/<hash>` — catalog DAG (cross-entry parents derived
    from `from_catalog`) and per-entry internal expression DAG. Pure SVG, no
    Cytoscape dep.
  - `/diff/<alias>[/<va>/<vb>]` — version diff with code diff, schema diff,
    per-column stats, key-joined side-by-side, and head() side-by-side.
  - `/errors/<id>` — build-failure detail.
  - `/api/{entries,aliases,errors,notebook,lineage,catalog_dag}` — JSON.
  - `/api/sse` — live updates (`new_entry`, `build_failed`, `alias_changed`,
    `notebook_changed`).
- **Build artifacts are portable.** xorq's absolute filesystem paths are
  rewritten to `${PYDATA_PROJECT_ROOT}` on write and expanded back on load.
- **`pydata serve <project_dir>`** — read-only companion against a project
  directory that may live anywhere on disk. Mutation routes return 403.

What's NOT in V0 (deferred to later spikes, in this order):

1. Buckaroo iframe + Tornado subprocess (replace the `pandas.to_html` previews).
2. SortableJS drag-reorder for the notebook (current ↑/↓ buttons are the
   accessibility fallback; drag is the headline UX).
3. Vega-Lite charts attached to entries.
4. Column-level lineage (xorq has the data; current view is op-level only).
5. `pydata pack` / tar handoff (today's hand-off is `cp -r`).

## Running the spike

```sh
uv sync
uv run pydata init spike            # creates ~/.pydata-app/projects/spike/ + fixture
uv run pydata run --project spike   # edit-mode companion on http://127.0.0.1:7860
```

In another terminal, launch Claude Code from this directory; it picks up
`.mcp.json` and exposes the `pydata` MCP server.

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

Once you've authored a project, hand it off:

```sh
tar czf my-project.tgz -C ~/.pydata-app/projects spike
# colleague extracts somewhere
tar xzf my-project.tgz -C ~/projects/
uv run pydata serve ~/projects/spike
```

The companion runs read-only: same catalog, same forensic history, no edit
affordances. Mutation routes return 403.

## Conventions worth knowing

- xorq 0.3.x reads use `xo.deferred_read_parquet` (NOT `xo.read_parquet` — that
  resolves through ibis's backend loader and fails). Use
  `import xorq.api as xo` and `import xorq.vendor.ibis as ibis`. Do NOT
  `import ibis` directly.
- Prefer `from pydata_xorq.io import from_project; t = from_project("name.parquet")`
  over absolute paths — the catalog records project-relative intent and the
  build is portable across machines/users.
- Content hash is xorq's build hash — same code + same inputs → same hash → same
  entry dir (idempotent).
- All catalog state lives on disk. The MCP server holds no in-memory state; the
  companion only holds the SSE subscriber list.
- `PYDATA_PROJECT_PATH` overrides project_dir() resolution for the active project.
  Used by `pydata serve` to point at a project directory anywhere on disk.

## Tests

```sh
uv run pytest               # 76 tests, ~5s
uv run pytest tests/test_portable.py    # the portability proof
```
