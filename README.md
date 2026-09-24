# Tallyman

Tallyman — a data science environment designed for coding agents. Stop squinting at slow tables in a terminal; turn on the lights and see your data.

Two windows: Claude Code in one, a browser in the other. You work by defining a set of named results that can depend on each other, then making sure those results live up to their name. You describe them; the agent writes the queries. You tell it what `likely_customers` should mean, and a moment later the answer is a table in the browser at full size, sortable and searchable, with statistics over every column. Your prompt sits above the query the agent wrote, so you read your intent, the code that came out of it, and the result together. You notice it's catching people who already churned, so you sharpen the sentence and the agent revises the query. Everything built on top of that name updates to match, fast enough that you don't lose your place. How big the data is, what has already been computed, and what needs recomputing never enter into it.

Those queries are expressions: self-contained programs that declare the raw files and other results they read. Writing the expression is the agent's entire job. An expression is declarative and can be introspected, so tallyman reads its dependencies straight off it and builds a directed graph of the project's aliased expressions.  When a parent updates, its children are recomputed; a raw file changing on disk counts as an update too. Tallyman executes each expression once and writes the result and its summary statistics to disk. Reading it back is out of core: scrolling, sorting, and searching pull only the pieces of data needed to fill the screen, so nothing has to fit in memory and four million rows opens like four thousand.


You aren't typing `df.sort_values('lifetime_sales')` just to see which customers are at the top, you aren't waiting for the LLM to print a 10 row table like it's coming out of a 1200 baud modem.  You aren't running out of memory in the middle of a session.  You aren't building the ad-hoc cache that every long notebook grows, the pickle in /tmp behind an if not exists guard that you never quite trust. You aren't nursing a kernel along for days because one cell takes five minutes to rerun, or bracing yourself before you close the window. None of that is in your head while you work. What's in your head is the data and what it means.




# tallyman notebooks

Tallyman is my take on an AI native notebook system.  Jupyter notebooks have been the go to tool for data science for over a decade for many reasons.

1. They combine the code and results of data analysis into one UI.  This is iporant because data science programming is different in nature than typical software engineering.
2. Leverage the highly perfomant python data science tools
3. Also functions as a literate programming environment allowing you to write  rendered narrative descriptions of the process (at a data analysis level not a code comment level) that is combined with graphical outputs from cells.


Tallyman takes an AI native approach to interactive data science.  It does this by constraining the problem space of data analysis.  Jupyter is a generic programming environment that works well for data science but can be used for any type of code.  Tallyman is built specifically for tabular data analysis to be driven an LLM via an MCP.  
'

The primary object in tallyman notebooks are xorq expressions.  A xorq expression is similar to a pandas dataframe with method chained aggregations and operations applied to it.    Expressions can depend on other expressions,  joining multiple expressions into one named alias.  The important thing is the naming and the intent.  If you have an expression named `customers` and another expression named `best_customers` that depends on `customers`, updating the definition of `customers` also causes a recomputation of `best_customers`.  

All of these expressions are meant to be written by an LLM sent to tallyman via MCP, executed once, and then the results cached.  Tallyman is a system that has interactive views of these expressions, but unlike jupyter notebooks, it doesn't require the expressions/dataframes to be resident in memory.  Tallyman can view dataframes with millions of rows without blowing up memory.  

I would say that jupyter notebooks are the most used example of literate programming.  LIterate programming combines documentation with ocde and results.  Documentation and comments in notebooks explain why you are doing something.  This is tedious to write as a programmer and documentation is frequently out of date.  LLMs can make this much easier, not by easking the LLM to "write docs for this",  not by automatically asking the LLM to write "write docs for this", but instead by capturing the orignal prompt that you used to get the LLM to write an expression.  This prompt is the clearest version of your intent as expressed and understood by you the writer.  Tallyman keeps this next to each expression.  You can review intent of each expression this way.

Back to updating named expressions.  The named expression thing is really important.  In a regular notebook, it would be equivalent to `customers_df`, and hopefully you'd update it appropriately and dependent variables/cells as you update your notebooks.  Marimo puts more rigor around this Cell-DAG approach, but Marimo is putting structure around unstructured python code,  marimo doesn't cache intermediate results.  Everytime you reload a notebook, marimo goes and re-executes everything.  Furthermore marimo, can't diff between versions.

I got side tracked there talking about the dependency graph.  We also have versioned history of each named expresion, and it's prompt.  All of that is built into tallyman, and it's fast.






# tallyman-notebooks



Spike for the Tallyman London 2026 talk *"The Future of Notebooks in a Claude Code World"*.
The proposal lives in `proposal.md`. This README covers the V0 spike only.
For the system architecture — subsystem map, on-disk layout, data-flow paths, and an
index of all the docs — start with [docs/architecture.md](docs/architecture.md).

## V0 scope

End-to-end: a Claude Code MCP tool that compiles a xorq expression into a
content-hashed catalog entry on disk (running it, and writing its result to a
file when the entry does expensive work), and pushes a live update to a browser
companion via SSE.

What's working:

- **MCP tools** (FastMCP over stdio, 31 tools):
  - Catalog: `catalog_import_source`, `catalog_run`, `catalog_create`,
    `catalog_revise`, `catalog_alias`, `catalog_rename`, `catalog_unalias`,
    `catalog_list`, `catalog_diff`, `catalog_promote_diff`, `catalog_chart`,
    `catalog_chart_errors`, `catalog_scan_staleness`, `catalog_recalc`,
    `catalog_export_marimo`, plus the summary-stat / post-processing /
    display-klass authoring tools.
  - Notebook: `notebook_reorder`, `notebook_remove`, `notebook_edit_markdown`.
  - Project: `project_list`, `project_new`, `project_switch`.

    See [docs/mcp-server.md](docs/mcp-server.md) for every tool and its side
    effects.
- **Companion** (FastAPI on `:7860`) — serves the React SPA
  (`packages/app/dist`) as a catch-all and exposes a JSON API + SSE under
  `/{project}/api/*`:
  - SPA pages: **Catalog** (entry list + detail with V_n chips, forensic
    history, and a metadata tab with the entry's disk footprint, sources,
    parents and children), **Notebook** (curated narrative anchored on aliases,
    drag-reorder, inline markdown editor, × remove), **Diff** (code diff, schema
    diff, per-column stats, key-joined side-by-side, head() side-by-side, and a
    promote button), **Cache** (the result snapshots on disk, with a delete
    button; pinned snapshots cannot be deleted), **Log** (linear, filterable
    activity view), and the project list.
  - JSON: `/{project}/api/{entries,entry/<hash>,entry_cache/<hash>,
    session/<hash>,aliases,notebook,notebook_full,errors,error/<id>,log,
    data/<hash>,diff_data/...,disk_usage,result_cache,staleness,telemetry}`,
    plus the mutation routes (`PATCH notebook`, `PUT code/<alias>`,
    `PUT markdown/<cell_id>`, `POST reset`, `POST recalc`,
    `POST promote_diff/...`, `DELETE result_cache/<hash>`, `DELETE errors`).
  - `/{project}/api/sse` — live updates. The SPA listens for `new_entry`,
    `build_failed`, `notebook_changed`, `chart_attached`,
    `post_processing_changed`, `summary_stat_changed`, `recalc` and
    `project_switched`.
  - `/internal/notify` — the MCP server's notification hook; fans out to SSE.
- **Buckaroo subprocess** — `tallyman run` spawns `python -m buckaroo.server`
  on `:8700` (falls back to a random port if busy), watches for the
  `BUCKAROO_PORT=...` handshake, and opens a session for an entry by POSTing a
  build dir to Buckaroo's `/load_expr` endpoint (PR 776), after making sure
  every file the entry reads exists. It does this when an entry's catalog page
  opens, and for every cell each time the notebook page loads. A *worthy*
  entry (one whose query tallyman materialized to a result file when the entry
  was created) is handed a view build, a build that is one read of that file
  (`.xorq_view_build/`). A *cheap* entry (a filter, selection or computed column
  over one file, which keeps no file of its own) is handed its own build,
  expanded into a stable per-entry path (`.xorq_build_expanded/`, gated by a
  `.complete` marker) so `${TALLYMAN_PROJECT_ROOT}` placeholders are resolved
  before xorq's loader sees them. Buckaroo's sorting, search and summary stats
  run as queries over that build. A session's id is derived from the project and
  the content hash, so tallyman keeps no session file. Tear-down rides along with
  the companion. Disable with `--no-buckaroo`.
- **Build artifacts are portable.** xorq's absolute filesystem paths are
  rewritten to `${TALLYMAN_PROJECT_ROOT}` on write and expanded back on load
  (with one known gap for copied projects, #209).
- **`tallyman serve <project_dir>`** — read-only companion against a project
  directory that may live anywhere on disk. Mutation routes return 403, and no
  Buckaroo subprocess runs, so entry grids do not load.

What's NOT yet implemented:

1. Column-level lineage (xorq has the data; there is no lineage view today).
2. A dedicated ML training tool, `catalog_train` (storyboard beats 7-8, #2).
   Models can already be fitted as catalog entries with `xorq.ml`, as
   `catalog_run`'s tool description shows.

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

> Use catalog_import_source to import
> `~/.tallyman-notebooks/projects/spike/data/orders.parquet` as `orders`.
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

The companion runs read-only: same catalog, same forensic history, same charts.
The edit controls still show, but mutation routes return 403, and there is no
Buckaroo grid.
The archive includes `compute_cache/`, and a known defect (#209) makes a copied
project's cheap entries read from the original location; see
[docs/installing.md](docs/installing.md#sharing-a-project).

## Conventions worth knowing

- A file enters the catalog only through `catalog_import_source(path, alias)`,
  which copies its bytes into the project and makes each version of it an entry
  under a **source alias**; the path can be anywhere and is never read again.
  Import it again to bring in new data: different bytes mint the next version,
  and the entries downstream are recalculated. A CSV takes its `schema` and
  reader options in the import call, and they are fixed there.
- Recipes read entries by alias, never files:
  `tracked_expr_from_alias("orders")` follows an alias and
  `pinned_expr_from_alias("orders-v2")` pins one version. `read_project_file`,
  `tallyman_read_csv`, `xo.deferred_read_csv` and `xo.deferred_read_parquet` on a
  file outside the project's `compute_cache/` are build errors that name the
  import to use, and `xo.read_parquet` resolves through ibis's backend loader and
  fails. Use `import xorq.api as xo` and `import xorq.vendor.ibis as ibis`. Do
  NOT `import ibis` directly.
- Content hash is xorq's build hash — same code + same inputs → same hash → same
  entry dir (idempotent).
- All catalog state lives on disk. The MCP server and the companion keep only
  in-memory caches of things that never change (loaded builds and reads, keyed
  by content hash), plus the MCP session's active project and the companion's
  SSE subscribers and diff sessions.
- `TALLYMAN_PROJECT_PATH` overrides project_dir() resolution for the active project.
  Used by `tallyman serve` to point at a project directory anywhere on disk.

## Tests

```sh
uv run pytest                       # full suite
uv run pytest tests/test_pack.py    # the pack / portability proof
```
