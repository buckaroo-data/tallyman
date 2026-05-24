# pydata-app — plan

The xorq MCP app that backs the PyData London 2026 talk *"The Future of Notebooks in a Claude Code World"* (see `proposal.md`). This document is the result of a grilling pass; it supersedes earlier drafts. Decisions baked in from the grilling are stated; remaining open questions are listed at the end.

> **V0.6 implementation status (2026-05-24).** Working end-to-end on branch
> `spike/catalog-run-loop`:
>
> - 16 MCP tools — the original 13 (`catalog_*`, three `notebook_*`) plus
>   the LLM-summary-stats trio (`catalog_add_summary_stat`,
>   `catalog_remove_summary_stat`, `catalog_list_summary_stats`) backed by
>   `<project>/stats/<name>.py` files and the `project_root` field on
>   `/load_expr`. Design lives in `plans/llm-summary-stats.md`.
> - Catalog tab (named/forensic/scratch ordering, V_n chips, forensic history,
>   build-failure banner with detail page).
> - Notebook tab (cells anchored on aliases, drag-reorder via SortableJS
>   with ↑/↓ button fallback for accessibility, click-to-edit markdown,
>   × remove; auto-append on `catalog_create`/`catalog_alias`).
> - Lineage tab (catalog DAG via `from_catalog` parent edges, plus per-entry
>   internal expression DAG; pure SVG, no Cytoscape dep yet).
> - Diff tab (`/diff/<alias>[/<va>/<vb>]`): code diff (difflib unified),
>   schema diff (added/removed/changed-type), per-column stats, key-joined
>   side-by-side, head() side-by-side. All five flavors shown together.
> - Vega-Lite charts attached to entries via `catalog_chart`; entry-detail
>   embeds vega-embed and pulls data via `/api/data/<hash>`.
> - Buckaroo lifecycle: companion-managed Tornado subprocess on :8700,
>   `/load_expr` POST per entry (buckaroo PR 776 has landed), React embed
>   (`static/buckaroo-embed.js`) mounted into entry-detail and talking
>   to the buckaroo WS for paged rows + sort/search push-down.
>   `buckaroo==0.14.6` and `xorq==0.3.26` pinned from PyPI (no editable
>   sources). Search regression in 0.14.4/0.14.5 reported as
>   buckaroo-data/buckaroo#838, fixed in 0.14.6.
> - **`pydata serve <project_dir>`** read-only artifact mode and
>   **`pydata pack [<project>]`** tar-up command (portable `.tgz`,
>   `${PYDATA_PROJECT_ROOT}` placeholders preserved, optional
>   `--exclude-cache`).
> - **`pydata replay <storyboard.json>`** rehearsal mode that drives the
>   MCP tool surface deterministically (the open question #10 fallback).
> - MCP-over-stdio proven by `tests/test_mcp_stdio.py` (spawns the CLI as a
>   subprocess and round-trips real tool calls).
>
> **Not yet built (V1):** column-level lineage; ML training pipeline
> (beats 7-8); `catalog_prune` MCP tool.

---

## What we're building

A local app that demonstrates the **deconstructed notebook**: terminal Claude writes intent, xorq+Ibis on Arrow handles compute, a live browser companion is the work surface. The companion has two faces:

- **Catalog view** (default tab) — every executed expression appears here, with live Buckaroo for search/sort/filter/recon. Named entries (concepts) sit alongside scratch entries (footnotes); scratch nests under the named entry it verifies.
- **Notebook view** (second tab) — a curated, ordered, editable narrative built from named catalog entries. Reorderable, markdown-editable, exportable.

You drive the demo from a Claude Code terminal on the left half of your laptop screen; the companion lives in a browser on the right half, full-height. June 8th 2026.

---

## Mental model

Three tiers of state — the load-bearing reframe from grilling:

1. **Named catalog entries (aliased)** — `shoe_sales`, `weekday_by_hour`. The user explicitly names them ("name this X" in a prompt). Become notebook cells. Each alias names a *concept*; the latest version of the alias is the current best answer to that concept; older versions stay in the catalog as forensic artifacts but are not in the notebook view.

2. **Unnamed catalog entries (scratch)** — verifications, dives, "show your work" runs. Persisted in the catalog, content-addressed, navigable in the catalog tab. Nested as collapsed footnotes under their parent named entry (lineage-derived; agent can override). Not in the notebook unless promoted.

3. **Inline Buckaroo recon** — null counts, dtype distributions, top-K values, histograms. Free, no expression needed. Buckaroo handles per-column questions inside the table widget; eliminates a class of "tell me about column X" prompts entirely.

Three layers of curation: **disk-of-runs → catalog → notebook view**. The catalog is curated (wrong explanations get pruned); the notebook is curated further (only named entries, ordered, with markdown). Honesty over completeness; nothing is forced to live forever.

Identity is by **content hash**, never by timestamp. Aliases are mutable handles on hashes.

---

## Process topology

```
┌────────────────────────┐    MCP stdio    ┌──────────────────────────┐
│  Claude Code terminal  │ ──────────────► │  pydata_mcp server       │
│  (left half of screen) │                 │  (FastMCP, ~14 tools)    │
└────────────────────────┘                 └────────┬─────────────────┘
                                                    │ writes catalog
                                                    │ entries to disk
                                                    ▼
                                         ┌──────────────────────────┐
                                         │  catalog store on disk   │
                                         │  ~/.pydata-app/projects/ │
                                         │  <name>/catalog/...      │
                                         └────────┬─────────────────┘
                                                  │ reads
                                                  ▼
┌────────────────────────┐  HTTP + SSE  ┌─────────────────────────┐
│  Browser               │ ◄──────────  │  pydata_companion       │
│  (right half of        │              │  (FastAPI + Jinja2)     │
│   screen, full-height) │ ── click ──► │  port 7860              │
│                        │              └────────┬────────────────┘
│  ┌──────────────────┐  │                       │ spawns + supervises
│  │ React embed      │  │  WebSocket (paged     ▼
│  │ (buckaroo-embed) │◄─┼──rows, sort, search)─►┌─────────────────────┐
│  │  in entry detail │  │            :8700      │  buckaroo server    │
│  └──────────────────┘  │                       │  (subprocess, 8700) │
└────────────────────────┘                       └─────────────────────┘
```

Three processes on one machine — `pydata_mcp` (spawned by Claude Code), `pydata_companion` (the FastAPI app you start manually with `pydata run`), and a `buckaroo` server subprocess managed by the companion.

The companion notifies the browser of changes via SSE. The MCP server notifies the companion via `POST /internal/notify` after each tool call that mutates the catalog.

---

## On-disk layout

A "project" is a directory that owns one catalog and one or more notebooks. V1 has a single default notebook per project.

```
~/.pydata-app/projects/<project_name>/
├── catalog/
│   ├── entries/<content_hash>/
│   │   ├── manifest.json       # name?, version?, parent_hash?, prompt, code_path, created_at, parent_alias?
│   │   ├── expr.py             # source script (self-contained, runnable)
│   │   ├── expr.yaml           # decompiled DAG (from xorq build)
│   │   ├── result.parquet      # cached result (Buckaroo loads this)
│   │   ├── schema.json         # arrow schema + row count
│   │   └── lineage.json        # node + edge list, includes upstream catalog hashes
│   ├── aliases.json            # {alias: latest_hash}
│   ├── alias_history.json      # {alias: [hash_v1, hash_v2, ...]}
│   ├── chart_specs/            # optional Vega-Lite specs keyed by hash
│   │   └── <hash>.vl.json
│   └── buckaroo_sessions.json  # {hash: buckaroo_session_id}  (regenerated on startup)
├── notebooks/
│   └── default.json            # cell list, markdown blocks, ordering
└── pydata.toml                 # project config (name, default notebook, settings)
```

Append-only-ish: catalog entries are immutable (content-hashed), but `aliases.json`, `alias_history.json`, and `notebooks/*.json` are mutable indexes maintained by the system. Pruning a catalog entry deletes its directory and removes references; the catalog reflects what you want to keep.

---

## Pieces

Seven Python packages in one monorepo. Boundary discipline matters because the talk-quality bar is "this works end-to-end without surprises."

| Package | Owns | Depends on |
|---|---|---|
| `pydata_core` | Disk layout, manifest schema, alias bookkeeping, notebook JSON model. No HTTP, no MCP, no compute. | (stdlib + pydantic) |
| `pydata_xorq` | Compute layer. Wraps xorq's `xo.build_expr`, expr.yaml decompile, lineage extraction. Takes Python code → catalog entry on disk. | `xorq`, `pydata_core` |
| `pydata_mcp` | The MCP server. Exposes ~13 tools. Calls into `pydata_xorq` and `pydata_core`. Posts notifications to companion's `/internal/notify`. | `fastmcp`, `pydata_xorq`, `pydata_core` |
| `pydata_companion` | FastAPI app. Reads catalog from disk. Manages buckaroo subprocess. Serves Jinja2 templates + SSE. Accepts notebook edits from browser. | `fastapi`, `pydata_core`, `buckaroo` |
| `pydata_companion/static/` | Static assets: Buckaroo bundles (copied from `~/buckaroo` build output), Vega-Lite, Cytoscape, custom JS for notebook drag/edit. | (build artifact) |
| `pydata_companion/templates/` | Jinja2 templates: `base.html`, `catalog.html`, `notebook.html`, `lineage.html`, `diff.html`, fragments for SSE updates. | (templates) |
| `pydata_cli` | The `pydata` console script. `pydata init <name>` / `pydata run` / `pydata mcp` (the latter is what Claude Code spawns) / `pydata serve <project_dir>` (read-only companion against a handed-off project) / `pydata pack [<project>]` (portable `.tgz`) / `pydata replay <storyboard.json>` (deterministic rehearsal). | `click`, `pydata_companion`, `pydata_mcp` |

Single `pyproject.toml` at the repo root, src-layout, uv-managed. Python 3.13.

---

## MCP tool surface

16 tools implemented as of V0.6 (13 catalog/notebook + 3 summary-stats); names use `catalog_*`, `notebook_*`, `viewer_*` prefixes for legibility. The plan-original `viewer_*` row is still V1 because the demo so far does not need it (the agent's tool choice + SSE auto-focus covers the cases).

**Catalog mutation:**
| Tool | Behavior |
|---|---|
| `catalog_run(code)` | Execute, persist as scratch (no alias). Returns content hash. Companion auto-focuses on the new entry in the catalog tab. ✅ |
| `catalog_load_parquet(rel_path)` | Convenience: load `<project>/data/<rel_path>` as a scratch entry without writing xorq dialect. ✅ |
| `catalog_create(name, code)` | Execute, persist with alias. Auto-appends to active notebook. Errors if alias exists. ✅ |
| `catalog_revise(name, code)` | Execute, persist as new version of an existing alias. Cell updates in place; older versions become forensic. Errors if alias doesn't exist. ✅ |
| `catalog_alias(hash, name)` | Promote a scratch entry to a named entry post-hoc. Errors if alias exists. ✅ |
| `catalog_rename(old_name, new_name)` | Change an alias name. Notebook follows. ✅ |
| `catalog_unalias(name)` | Drop an alias; entries remain, notebook cell is removed. ✅ |
| `catalog_chart(hash_or_alias, vega_spec)` | Attach a Vega-Lite chart spec to an entry. Companion renders it above the table. ✅ |
| `catalog_add_summary_stat(name, source)` | Write `<project>/stats/<name>.py` (`def compute(col): ...`); validated against a 1-row ibis memtable in a restricted-globals namespace before persisting. Picked up by buckaroo on next session-load via `project_root`. ✅ |
| `catalog_remove_summary_stat(name)` | Soft-disable by moving the file to `stats/_disabled/` (buckaroo skips the `_` prefix). ✅ |
| `catalog_list_summary_stats()` | List enabled summary-stat files in the project. ✅ |
| `catalog_prune(hash_or_alias)` | Remove an entry from the catalog. Removes from notebook if present. *(V1 — not yet implemented)* |

**Notebook mutation:**
| Tool | Behavior |
|---|---|
| `notebook_reorder(cell_id, new_index)` | Move a cell. Mirrors browser drag-and-drop. ✅ |
| `notebook_remove(cell_id)` | Remove a cell from the notebook. Catalog entry untouched. ✅ |
| `notebook_edit_markdown(cell_id, markdown)` | Replace the markdown above a cell. ✅ |

**Inspection / control:**
| Tool | Behavior |
|---|---|
| `catalog_list()` | List entries (named first, then forensic, then scratch). Each annotated with `alias` and `version` when applicable. ✅ |
| `catalog_diff(name, va=-2, vb=-1)` | Forensic diff between two versions of an alias. Returns code/schema/stats/keyed-summary; the companion's `/diff/<alias>` page renders the full HTML. ✅ |
| `catalog_show(hash_or_alias)` | Manifest + schema + head() preview. *(V1 — `/api/data/<hash>` + `/catalog/<hash>` cover this for now)* |
| `catalog_lineage(hash_or_alias)` | Lineage graph as JSON. *(V1 — `/api/lineage/<hash>` and `/api/catalog_dag` cover this from the companion)* |
| `viewer_focus(hash_or_alias)` | Switch the catalog tab's focus to this entry. *(V1 — currently handled by the SSE `new_entry` auto-focus)* |
| `viewer_open_notebook()` | Switch to the notebook tab. *(V1 — same justification)* |

The agent's choice between `catalog_run`, `catalog_create`, and `catalog_revise` is the visible signal of its intent — communicated and overrideable. If the agent picks `catalog_run` when you wanted a named entry, you (or the agent on a follow-up) call `catalog_alias` to promote.

---

## Companion: routes and tabs

FastAPI app, Jinja2 templates, vanilla JS for interactions. Server-Sent Events for live updates. No SPA framework — keep moving parts low.

The companion has two modes. **Edit mode** (`pydata run`) is the authoring surface used during a session: full routes, drag-and-drop, markdown editing, MCP notifications. **Serve mode** (`pydata serve <project_dir>`) is the read-only artifact view: a colleague (or future-you) runs it against a handed-off project directory and sees exactly the catalog and notebook the author last saved. Serve mode hides drag handles, ×, and contenteditable affordances; rejects `PATCH /api/notebook`, `PUT /api/markdown/<cell_id>`, and `POST /internal/notify` with 403; and does not start the MCP server. The project directory plus a `pydata` install is the artifact — there is no `.ipynb`, no static export, no other deliverable.

| Route | Purpose |
|---|---|
| `/` | Redirect to `/catalog` |
| `/catalog` | Default tab. Two-column layout *inside the tab*: left = entry list (named first, sortable, searchable input filtering by name+prompt), right = entry detail (Buckaroo React embed + tabs: Data \| Schema \| Code \| Prompt \| Lineage \| Forensic-history). |
| `/catalog/<hash_or_alias>` | Deep link to an entry's detail view. |
| `/notebook` | Notebook tab. Vertical scroll of cells; markdown editor above each code cell (contenteditable + serialized to markdown on save); drag-handle for reorder; × for remove; chart panel above table when present. Reorder uses [SortableJS](https://sortablejs.github.io/Sortable/). |
| `/lineage` | Cytoscape DAG view of the catalog. Click a node = focus that entry in the catalog tab. |
| `/diff` | Forensic V_n vs V_n-1 (or arbitrary pair). Code diff (Pygments), schema diff, data-sample diff. |
| `/api/sse` | EventSource stream of `{kind, ...}` events. |
| `/api/notebook` | `PATCH` for reorder/remove/markdown edits from the browser. |
| `/api/markdown/<cell_id>` | `PUT` for markdown edits. |
| `/internal/notify` | Internal POST endpoint. Only `pydata_mcp` calls this. Fans out to SSE clients. |

**Tab semantics:** clicking a notebook cell in the `/notebook` tab auto-syncs `/catalog` to that cell's entry, so when you switch back to catalog the entry is already focused.

**Markdown editor:** uses a small textarea or Monaco-lite for V1; Markdown rendered with `markdown-it` on save. No WYSIWYG — keep it predictable for the live demo.

---

## Buckaroo integration

Buckaroo runs as a **live Tornado server** managed by the companion subprocess group. This enables search, sort, filter, and column-level recon (null counts, distributions) directly in the table widget — the in-line recon tier of the three-tier model.

**Lifecycle:**
1. `pydata run` starts: companion spawns `python -m buckaroo.server --port 8700 --no-browser --stdio-control` as a subprocess. Companion holds the subprocess handle; cleans up on shutdown.
2. When a new catalog entry lands (companion is notified via `/internal/notify`), the companion expands the entry's `xorq_build/` dir into a tmp copy with `${PYDATA_PROJECT_ROOT}` placeholders resolved, then POSTs that path to buckaroo's `/load_expr` endpoint (added in upstream PR 776). Buckaroo loads the xorq expression and returns a `session_id`. Stored in `buckaroo_sessions.json` keyed by content hash.
3. The entry-detail page renders a `<div data-ws-url="ws://localhost:8700/ws/<session_id>">` placeholder; the React embed (`static/buckaroo-embed.js`, built from `packages/embed/`) mounts `BuckarooServerView` into it and pages rows over the WS. Sort/search push-down lands on the underlying backend rather than re-materialising the parquet.
4. On companion startup, `buckaroo_sessions.json` is loaded but treated as a *naming hint* — buckaroo itself is fresh, so sessions are lazily re-created on first view (and the cache is invalidated when buckaroo's reported `started` timestamp doesn't match what was last persisted).

**What goes through Buckaroo vs not:**
- Tabular results → React embed (`BuckarooServerView`) mounted into the entry-detail page, talking to Buckaroo's WS.
- Chart (Vega-Lite spec attached) → renders above the embed in the entry detail panel.
- Schema, code, prompt, lineage, forensic history → Jinja2-rendered HTML in the entry detail tabs (NOT in Buckaroo).

**Why not the Buckaroo MCP server?** Different pattern: that one opens a sibling browser tab. We want Buckaroo embedded inside our companion's catalog tab, not as a sibling. We use the standalone Tornado server directly.

**Why React embed and not iframe?** The original design used `<iframe src="/s/<session>">`. The iframe model carried a FOUC (full bundle re-bootstrap per nav) and a per-cell isolation tax (separate browser contexts). The React embed mounts directly into the host page, sharing the document, and connects over WS. T-31/T-32 documented the iframe-era follow-ups; both are now moot. T-33 is the new memory baseline ticket for the embed model.

**Build prerequisites:** `packages/embed/` builds a tiny ~1.4MB ESM bundle that mounts `BuckarooServerView` from `buckaroo-js-core`. Bundle is committed at `src/pydata_companion/static/buckaroo-embed.js` so fresh clones run without Node. The Python `buckaroo` server is a normal PyPI dep — `buckaroo==0.14.6` pinned in `pyproject.toml` (PR 776 has landed; no editable source override).

---

## What to copy and from where

| Source | What | Destination |
|---|---|---|
| `~/code/xorq-mcp/xorq_web/metadata.py` | Lineage extraction from xorq DAG; expression decompile helper | `pydata_xorq/lineage.py`, `pydata_xorq/decompile.py` |
| `~/code/xorq-mcp/xorq_mcp_tool.py` | Patterns for `xo.build_expr` invocation, hash → cache path conventions, Buckaroo session bootstrap. **Caveat:** xorq-mcp pins `xorq>=0.3.8`; xorq 0.3.23 renamed `xo.read_parquet` → `xo.deferred_read_parquet` and made the former resolve through ibis backend loading (which errors). Use the deferred names. | `pydata_xorq/build.py`, `pydata_companion/buckaroo_lifecycle.py` |
| `~/code/xorq-cloud-planning/packages/app/templates/entry_detail.html` | Tab structure, prompt block layout, build-metadata grid, Pygments highlighting setup | `pydata_companion/templates/_entry_detail.html` (adapted; not literal copy) |
| `~/code/xorq-cloud-planning/packages/app/templates/base.html` | Base template scaffolding (head, nav, includes) | `pydata_companion/templates/base.html` |
| `~/code/xorq-cloud-planning/packages/app/static/` (if present) | Pygments CSS, base CSS | `pydata_companion/static/` |
| `~/code/xorq-desktop-plan/catalog-viewer-plan.md` | Conceptual reference for V_n revision chips, lineage-derived parent association | (read for design; nothing to copy directly) |
| `~/buckaroo/buckaroo/static/` (after `full_build.sh`) | Buckaroo JS bundles | `pydata_companion/static/buckaroo/` (or shipped via `buckaroo` PyPI dep) |

`~/code/xorq-mcp` is **referenced, not extended**. We build pydata-app fresh; we cherry-pick code we need.

---

## Demo storyboard (revised given the new model)

Twelve beats. Dataset: TBD (Citibike or NYC orders/sales — see open questions). Each beat is one prompt in the terminal; expected effect on the companion is annotated.

| # | Prompt (terminal) | Expected effect (companion) |
|---|---|---|
| 1 | "Load `orders.parquet` from disk; name this `orders`." | Named entry `orders` appears in catalog tab. Auto-focus. Buckaroo React embed mounts and connects to the WS. Cell appears in notebook tab. |
| 2 | (No prompt — you click a column header in Buckaroo to inspect distribution.) | Buckaroo's inline recon: histogram, null counts, top values. Audience sees the "free recon" tier. |
| 3 | "Filter `orders` to just shoe sales; name this `shoe_sales`." | Named entry `shoe_sales` appears. Notebook tab shows two cells now. |
| 4 | "To verify shoe_sales looks right, group it by state." | Scratch entry appears in catalog, nested under `shoe_sales` as a collapsed footnote (lineage-derived). Auto-focus on the scratch. **Notebook unchanged.** |
| 5 | "shoe_sales is missing some categories — refine it to include sneakers, boots, sandals all under 'shoes'." | `shoe_sales` revised to V2. Notebook cell updates in place. The "by state" scratch entry from step 4 now points at V2 (or stays linked to V1; see open question). V1 retrievable via forensic history. |
| 6 | (No prompt — switch to the Lineage tab.) | Cytoscape DAG: `orders → shoe_sales → (scratch: by-state)`. Audience sees the data-flow graph. |
| 7 | "Train a tiny logistic regression to predict shoe size from price + region; name it `shoe_size_model`." | New named entry. Notebook has three cells now. |
| 8 | "Try gradient boosting instead — refine `shoe_size_model`." | V2 of `shoe_size_model`. Cell updates with new metric. |
| 9 | (Switch to the Notebook tab.) | The narrative so far: 3 cells. Markdown above each (initially the prompts). |
| 10 | (Click on the markdown above `shoe_sales`; rewrite it for the audience.) | Markdown updates live. SSE pushes to any other connected clients. |
| 11 | (Drag `shoe_size_model` cell above `shoe_sales`.) | Reorder. Notebook narrative now leads with the model, drills into the data underneath. |
| 12 | (Hand the project off.) Open a second terminal, run `pydata serve ~/.pydata-app/projects/pydata-london-2026` against the same directory. | Second browser tab opens to the read-only companion: same catalog, same notebook, same Buckaroo. No drag handles, no edit affordances. The point: the project directory *is* the artifact. A colleague with `pydata` installed sees exactly what you see. Every cell's `expr.py` is re-runnable from upstream parquet — content-hashed, deterministic, no kernel state to lose. |

Closing line on stage: *"The notebook is the story. The catalog is what did the work. The project directory is the artifact — and it's reproducible because xorq cached every step by content hash, not by whatever happened to be in memory."*

---

## Resolved decisions (from the grilling pass)

| # | Decision | Rationale |
|---|---|---|
| 1 | Agent decides `catalog_create` vs `catalog_revise` via tool choice; user can override | Intent is visible in the tool call; demo legibility |
| 2 | Naming is user-explicit; agent extracts intent from "name this X" phrasing | The catalog is curated; only deliberate concepts get aliases |
| 3 | Auto-append on named-create; cells update in place on revise; older versions are forensic, not narrative | Notebook stays clean; the cell *is* the concept, not the iteration |
| 4 | Three tiers: named / scratch / inline-Buckaroo | Eliminates a class of recon prompts; honest about exploration |
| 5 | Scratch nests under named cells via lineage (α primary, agent override γ) | Automatic, structural, with escape hatch |
| 6 | Browser companion: tabbed top-level (Catalog default \| Notebook \| Lineage \| Diff). NOT two-pane | Stage layout: terminal left, browser right; browser is full-width |
| 7 | Curation primary in browser (drag, click-to-edit, ×); agent has parallel MCP tools | Visible on stage; agent path for prompts like "rewrite the intro" |
| 8 | Auto-focus catalog tab on new entries | "The browser updates live as the agent works" — the talk's thesis |
| 9 | Catalog itself is curated (prune wrong explanations); not append-only-forever | Honesty over completeness |
| 10 | Identity by content hash; aliases are mutable handles | Standard xorq model |
| 11 | Live Buckaroo Tornado server (port 8700), React-embedded into entry detail; sessions backed by `XorqBuckarooInfiniteWidget` via `/load_expr` | Search/sort push-down lands on the xorq backend; no parquet re-materialisation |
| 12 | Build pydata-app fresh; vendor/copy from xorq-mcp; don't extend it | Different data model; talk hardening easier in a fresh repo |

---

## Open questions

Smaller, mostly tactical:

1. **Markdown lifecycle on revise.** When `shoe_sales` goes V1 → V2, does the markdown above the cell auto-re-seed from V2's prompt, or stay with whatever the user typed? My lean: **stay with user edits; never auto-overwrite**. Provide a "re-seed from latest prompt" button as an explicit affordance.

2. **Scratch parent on revise.** When `shoe_sales` revises to V2, do existing scratch footnotes (linked via lineage to V1) move to V2, point at "alias-latest", or stay pinned to V1? My lean: **scratch is keyed on alias, not version. Footnotes follow `latest`.** Forensic V1 view shows "what scratch existed at V1" via the alias history, but day-to-day they stick to current.

3. **Charts.** Agent emits Vega-Lite directly, or `catalog_chart_auto(name, kind="bar")` infers from schema? My lean: **both available**; helper for the demo, raw spec for advanced.

4. **Multi-notebook per project.** V1 = one default notebook per project. V2 = multiple. My lean: **single in V1, design notebook IO so multi is additive later.**

5. **Reordering UX.** ✅ Resolved — SortableJS shipped in `notebook.html`, ↑/↓ button fallback retained for accessibility.

6. **Buckaroo build/dependency.** ✅ Resolved — `buckaroo==0.14.6` from PyPI. The embed bundle is committed at `src/pydata_companion/static/buckaroo-embed.js`; the Python server is a normal PyPI dep with no editable override.

7. **Project lifecycle.** `pydata init <name>` writes `~/.pydata-app/projects/<name>/`. `pydata run` from that dir, or `pydata run --project <name>` from anywhere. Question: is the project rooted in `~/.pydata-app/...` or in CWD? My lean: **`~/.pydata-app/projects/`** — keeps state separate from code. Conference dataset committed to the repo as a fixture, copied into project on `init`.

8. **Dataset.** Citibike vs orders/sales. My lean: **synthetic orders/sales** matching the storyboard's `shoe_sales` naming. Easier to curate the demo to land on shoe-specific moments.

9. **Active-cell context for "refine that".** Agent uses recent context. If unreliable in rehearsal, add `viewer_active_alias()` tool that returns whatever the user last clicked in the catalog tab.

10. **Pre-talk rehearsal mode.** ✅ Resolved — `pydata replay <storyboard.json>` ships in `pydata_cli.main:replay_storyboard`; calls MCP tools in order with optional `--delay` for stage pacing.

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| **Project-as-artifact is not yet portable** — xorq embeds absolute filesystem paths in build artifacts; `pydata serve` on a colleague's machine would fail unless paths are rewritten. Surfaced by the V0 spike. | Build-time path rewriting: replace `$PYDATA_HOME/projects/<name>/...` with a `${PYDATA_PROJECT_ROOT}` placeholder when writing the build to disk; expand at load time. Verify with a portability test that swaps PYDATA_HOME and re-runs the entry. |
| Buckaroo build environment breaks on demo machine | Pin buckaroo version; commit a working build of static assets; smoke test the full stack on the actual demo laptop two weeks out |
| Agent picks wrong tool (e.g. `catalog_create` when you wanted scratch, or vice versa) | Tool descriptions written carefully; prompts in the rehearsal use unambiguous phrasing ("name this X" → create; no name in prompt → run) |
| MCP reconnect loses session state | All state on disk, never in MCP process memory. Reconnect re-reads. (Avoids xorq-mcp's known bug.) |
| WiFi at venue fails, dataset is remote | Pre-cache dataset; offline-first |
| Live demo crashes mid-talk | Recorded fallback video, switchable in 5 seconds |
| Vega-Lite or Cytoscape fails to load | Both have CDN fallback URLs in addition to local copies |
| SSE drops silently | Heartbeat every 10s; browser auto-reconnects |
| Serve mode diverges from edit mode (different rendering, missing assets) | Same FastAPI app, same templates, same Buckaroo subprocess; mode is a single flag that gates mutation routes and hides edit affordances. Smoke test: end every rehearsal by running `pydata serve` against the project and clicking through the notebook. |
