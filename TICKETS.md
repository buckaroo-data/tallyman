# Tickets

Generated from a working-system review 2026-05-11.
Each item names the file(s) to touch and a concrete acceptance check.

Severity legend:

- **P0** — will hurt the live demo on 2026-06-08
- **P1** — visible polish; audience-noticeable but not blocking
- **P2** — planned-but-missing V1 work
- **P3** — engineering hygiene
- **P4** — design questions I punted in V0.5

---

## P0 — demo-blocking

### ~~T-01 — SSE `new_entry` redirects from any tab~~ ✅ 2026-05-11

Fixed: the SSE handler now branches on `window.location.pathname`. On
`/catalog` (no hash) it reloads to refresh the entry list; on
`/catalog/<hash>` it focuses the new entry; on other tabs it does
nothing. See `src/pydata_companion/templates/base.html`.

### ~~T-02 — Catalog index page doesn't auto-refresh~~ ✅ 2026-05-11

Fixed alongside T-01. `/catalog` now reloads on `new_entry`.

### ~~T-03 — `catalog_load_parquet` only produces scratch~~ ✅ 2026-05-11

Added optional `name` parameter; behaves like `catalog_create` when
provided. T-24 (hard-coded project name in synthesized code) fixed at
the same time — the synthesized code now relies on `resolve_project()`.

### ~~T-04 — Markdown renders as `<pre>`~~ ✅ 2026-05-11

Server-side rendering via the `markdown` package (`fenced_code` +
`tables` extensions). Raw markdown preserved in a `data-raw` attribute
so the edit-toggle textarea gets the unrendered source.

### ~~T-05 — `chart_attached` SSE has no client handler~~ ✅ 2026-05-11

Listener added in `base.html`; reloads only when on the affected entry.

### ~~T-06 — No `pydata pack` command~~ ✅ 2026-05-11

`pydata pack <project>` produces `./<project>-<YYYY-MM-DD>.tgz`,
skipping `__pycache__`, `.DS_Store`, and the stale
`buckaroo_sessions.json`. Round-trip serve verified by
`tests/test_pack.py::test_pack_round_trip_serves`. See also T-24
(also fixed) which would have invalidated builds on a rename.

### ~~T-07 — Buckaroo server is not integrated~~ ✅ done 2026-05-11

`pydata run` now manages a Buckaroo subprocess (default `:8700`, falls
back to a random port if busy). Per-entry sessions are lazily created
on first view via `POST /load`, persisted under
`catalog/buckaroo_sessions.json` with an envelope shape that records
Buckaroo's `started` timestamp for restart detection. Iframe in
`/catalog/<hash>` points at `<base>/s/<session>`. Tear-down via stdin
close (Buckaroo's `--stdio-control` flag) with SIGTERM fallback.
Disable with `--no-buckaroo` if Buckaroo's runtime deps are missing.

Files added/touched: `src/pydata_companion/buckaroo_lifecycle.py`,
`src/pydata_companion/app.py`, `src/pydata_companion/templates/entry_detail.html`,
`src/pydata_cli/main.py`, `pyproject.toml`. 8 new tests
(3 unit, 2 integration, 3 companion-level).

Known follow-ups not in scope of this ticket:
- Buckaroo-managed serve mode for richer hand-off (T-?? to file)
- WebSocket reconnect handling when the user keeps a tab open across
  Buckaroo restarts (today the iframe goes dead until reload)

---

## P1 — visible polish

### ~~T-08 — `difflib.HtmlDiff` output is ugly~~ ✅ 2026-05-11

Swapped to `difflib.unified_diff` → Pygments DiffLexer + HtmlFormatter
(monokai style, inline `noclasses=True` so it doesn't require a global
CSS file). Identical inputs short-circuit to "(identical)" sentinel.

### T-09 — No SortableJS drag-reorder for the notebook

Plan calls for SortableJS; current ↑/↓ buttons are the accessibility
fallback only.

- **Fix:** drop SortableJS into `static/` (or CDN with local fallback)
  and wire to `PATCH /api/notebook` action=reorder.
- **Verify:** drag a cell, drop, position persists across page reload.

### T-10 — Notebook full-reloads on every PATCH/PUT

`PATCH /api/notebook` (reorder/remove) and `PUT /api/markdown/<id>`
return JSON, but the client does `window.location.reload()` after.

- **Fix:** swap reloads for in-place DOM updates. Cell-replace fragment
  for reorder (or just transposing two cells); textarea-toggle on save
  without reload for markdown.
- **Verify:** drag-reorder feels smooth; markdown save doesn't flash.

### ~~T-11 — Diff page's compare-pairs link list~~ ✅ 2026-05-11

Capped to adjacent (V_n→V_n+1) pairs plus a V_1→V_n bookend when
n > 3. For 5 versions: 4 adjacent + 1 bookend = 5 links instead of 20.

### ~~T-12 — Notebook serve-mode placeholder text~~ ✅ 2026-05-11

Fixed in the T-04 template rewrite — placeholder only rendered when
`not read_only`.

### ~~T-13 — `/catalog` section headers~~ ✅ 2026-05-11

Catalog list now buckets into named / forensic / scratch sections,
each with a header showing the count. Item rendering factored into
`_entry_list_item.html` to keep the list-building loop readable.

### ~~T-14 — Raw expr.yaml viewer~~ ✅ 2026-05-11

Entry detail now has a "build artifacts" disclosure that lists every
file under the entry's `xorq_build/` (expr.yaml, expr_metadata.json,
build_metadata.json, profiles.yaml). The `${PYDATA_PROJECT_ROOT}`
placeholder is visible in the displayed text — useful for showing the
portability story to an audience. Test asserts the placeholder is
present in the rendered page.

### ~~T-15 — Error banner doesn't name the failing tool~~ ✅ 2026-05-11

`record_error` now takes a `tool` field. `_run_and_record` plumbs each
tool's name through. Catalog banner shows it as a pill; error-detail
page shows "tool: …". Tests assert both `catalog_run` and
`catalog_create` failure paths.

---

## P2 — V1 / planned-but-missing

### T-16 — Browser-side code edit

Plan resolved-decision 7 calls for browser-primary curation. Today
fixing a typo in a cell's code requires going back to Claude.

- **Fix:** add `PUT /api/code/<cell_id>` that runs the new code through
  `build_and_persist` and re-points the alias. Add an "edit code"
  button to notebook cells.
- **Verify:** edit code in browser, hit save, new V_n appears, table
  updates.

### T-17 — Column-level lineage

xorq has `xorq.common.utils.lineage_utils.build_column_trees`. We
ship op-level only.

- **Fix:** call `build_column_trees` from `pydata_xorq.lineage`,
  expose as `/lineage/<hash>?column=<name>`. Render as nested HTML
  (per xorq-mcp's `_text_tree_to_html`).
- **Verify:** click a column header in the entry detail → column-only
  lineage tree.

### T-18 — ML training pipeline

Beats 7–8 ("train logistic regression... try gradient boosting
instead"). xorq has `xorq.ml` helpers — haven't explored.

- **Fix:** TBD; needs a spike to understand xorq's ML surface, then
  decide whether to wrap as `catalog_train(name, target, features,
  model="logistic")` or expect Claude to write the xorq.ml call.
- **Verify:** beats 7–8 of the storyboard execute end-to-end.

### T-19 — `pydata replay <storyboard.json>` (plan Open Q10)

Deterministic demo replay; useful as a stage fallback and as a CI'able
demo-regression test.

- **Fix:** `pydata_cli/main.py` adds a `replay` command that reads a
  JSON list of `{tool, args}` and drives them through the in-process
  MCP tools.
- **Verify:** the storyboard JSON in `demo/storyboard.json` (to be
  written) replays from a fresh `pydata init` and lands on the same
  catalog state every time.

### T-20 — `/api/data/<hash>` has no pagination

Serves up to 5000 rows synchronously as JSON. Fine for aggregates,
bad for raw 2k+ row scratch loads.

- **Fix:** add cursor-based pagination (`?offset=&limit=`); cap the
  default to 200; let Buckaroo handle the "full data" case once T-07
  lands.

---

## P3 — engineering hygiene

### ~~T-21 — xorq cache shared across test runs~~ ✅ 2026-05-11

Conftest now sets `XORQ_CACHE_DIR` to a session-scoped `mkdtemp` at
module load (xorq freezes the env var at first import, so the env tweak
has to happen before any test or helper imports xorq).

### T-22 — Slow stdio tests aren't marked

`tests/test_mcp_stdio.py` is ~3.5s for 3 tests. Mark as
`@pytest.mark.integration` and add `addopts = "-m 'not integration'"`
default in `pyproject.toml` with explicit `pytest -m integration` for
the full run.

### ~~T-23 — `pydata init` silently re-runs~~ ✅ 2026-05-11

`pydata init` now errors if the project already exists. `--force`
proceeds (re-initialises dirs that may already be there; preserves
catalog entries and aliases). Tests cover both paths.

### ~~T-24 — `catalog_load_parquet` hard-codes the project name~~ ✅ 2026-05-11

Fixed alongside T-03. The synthesized code now reads `from_project(rel_path)`
without an explicit `project=` argument, so a project rename or rehome
no longer breaks the build.

### ~~T-25 — Dead dep: `sse-starlette`~~ ✗ wrong-headed 2026-05-11

The companion's `/api/sse` route returns `EventSourceResponse` from
`sse_starlette.sse`. My V0.5 description was wrong — I don't roll my
own SSE, the dep is in use. Closed without action.

### T-26 — No CI

Nothing prevents a broken commit from landing on `main`. Should set up
GitHub Actions running `uv sync && uv run pytest` once we push.

---

## P4 — design questions punted in V0.5

### T-27 — Markdown re-seed on revise (plan Open Q1)

Decided "stay with user edits" but never wrote a "re-seed from latest
prompt" affordance. If markdown drifts from the actual code,
demo-time confusion ensues.

- **Fix:** add a "re-seed markdown" button next to "edit" on each
  notebook cell that copies the manifest's `prompt` into the markdown.

### T-28 — Scratch-parent semantics on revise (plan Open Q2)

Catalog lineage points at the *hash at build time*, not "latest alias."
This is correct but undocumented. Users will be confused when revising
`shoe_sales` doesn't move its scratch verifications.

- **Fix:** add a UI note on the catalog DAG explaining the build-time
  binding; consider a `--reparent-scratch` flag for `catalog_revise`
  that re-runs scratch entries against the new latest.

### T-29 — Multi-notebook per project

V1 = one default notebook per project. V2 needs a tab selector and
per-notebook routing.
