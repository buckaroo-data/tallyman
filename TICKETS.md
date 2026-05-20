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

### T-31 — Buckaroo session pre-hydration (PR against buckaroo)

The biggest remaining FOUC inside the iframe is the bootstrap window:
the JS bundle parses, connects to `/ws/<session>`, fetches the data
dictionary + first page of rows, then renders. ~200–500ms even on a
warm cache.

**Buckaroo-side PR idea:** have `GET /s/<session>` inline the initial
state into the page as `<script type="application/json"
id="bk-initial-state">…</script>`. The JS bundle's bootstrap path
checks for that element first; if present it hydrates synchronously
and never opens the WS for the first render (it still opens the WS
for live updates / subsequent pages).

This would collapse the iframe bootstrap from "load JS → connect WS →
fetch data → render" to "load JS → render". Especially impactful for
the demo, where each beat creates a new entry and the iframe pops in
several times.

### T-32 — Iframe pool / multi-session caching (pydata-app side)

Even with pre-hydration, navigating between entries reloads the iframe
content because the `src` changes. To eliminate that entirely we'd
keep one iframe per visited entry, hidden, and toggle visibility on
nav. Memory cost is ~1.5 MB JS state + the grid's data per iframe;
fine for a 5–10 entry demo.

- **Where:** SPA-lite controller in `base.html`. On navigation, look up
  the target entry's iframe in a persistent `#iframe-pool` div; if
  found, move it into the detail pane's iframe slot. If not, create
  it lazily and move on next nav.
- **Trade-off:** iframes outside the viewport still keep their WS
  connections open; that's OK at the demo's scale but worth bounding
  to ~5 LRU on a longer-lived session.

### T-30 — XorqBuckarooInfiniteWidget over the standalone server

We're using `mode="buckaroo"` (BuckarooInfiniteWidget) which materialises
each catalog entry's parquet and pages over the materialised frame.
`XorqBuckarooInfiniteWidget` (`buckaroo/xorq_buckaroo.py`) is strictly
better for the demo: it operates on the xorq expression directly,
pages via `expr.limit(end - start, offset=start).execute()`, and pushes
sort/aggregation down to the backend. We can't reach it through the
standalone server today because the server only has `POST /load` /
`/load_compare` (file-path based) — there's no `/load_expr` endpoint
that takes a xorq build dir.

- **Where:** would require adding a handler to `buckaroo/server/handlers.py`
  (call it `LoadExprHandler`) that takes `{path_to_build_dir, no_browser}`,
  loads the expression via `pydata_xorq.load_entry` (or xorq's
  `load_expr_from_tgz`), instantiates `XorqBuckarooInfiniteWidget`, and
  pushes the buckaroo-state to subscribers. Could pair with a new
  `mode="xorq"` in the existing `LoadHandler` so both endpoints share
  validation.
- **In pydata-app:** swap `BuckarooManager.ensure_session` to call the
  new endpoint with the entry's `xorq_build/` dir; everything else
  (sessions cache, iframe URL, etc.) stays the same.
- **Verify:** sorting/filtering a large entry's column doesn't
  re-materialise the whole parquet, and a backend that supports
  push-down (datafusion) executes the predicate.

This is a buckaroo-side change, not a pydata-app change. Defer until
either we add the endpoint to the local buckaroo checkout or the
buckaroo project ships it.

Filed upstream as
[buckaroo-data/buckaroo#773](https://github.com/buckaroo-data/buckaroo/issues/773)
(2026-05-20).

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

### ~~T-09 — SortableJS drag-reorder~~ ✅ 2026-05-11

SortableJS via jsdelivr CDN. The `⋮⋮` grab handle on each cell's
header invokes drag; on drop we PATCH the new index. No reload — the
DOM is already in the dropped position. The ↑/↓ buttons were removed
since the drag handle covers their case (a11y note: keyboard users can
still call notebook_reorder via the MCP tool). Drag is suppressed in
serve mode.

### ~~T-10 — Full-reload on PATCH/PUT~~ ✅ 2026-05-11

- Reorder: drag-end PATCHes the new index; DOM already in correct
  position, no reload.
- Remove: PATCH succeeds, `.remove()` on the cell node.
- Markdown edit: PUT returns `{cell, html}`, client swaps the rendered
  HTML in place and removes the textarea. Escape cancels without
  saving.

Side note: I had to revisit the a11y story since the ↑/↓ buttons were
removed. For now, keyboard users can call notebook_reorder via the MCP
tool. A keyboard-driven move-cell affordance is a follow-up.

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

### ~~T-16 — Browser-side code edit~~ ✅ 2026-05-11

`PUT /api/code/<alias>` builds + revises an alias from edited code. New
revision lands as V_{n+1}; V_n stays forensic. Errors are recorded via
`record_error(tool="api_code")` and 400'd back, with the catalog banner
picking them up via SSE.

UI: `/catalog/<hash>` of a named entry now has an "edit + revise"
button next to the code header (hidden for scratch entries and in
serve mode). Click swaps the `<pre>` for a textarea + save/cancel
toolbar; save redirects to the new V_{n+1} on success.

### ~~T-17 — Column-level lineage~~ ✅ 2026-05-11

`pydata_xorq.column_lineage(project, hash)` loads the entry via the
portable load path, runs `xorq.common.utils.lineage_utils.build_column_trees`,
renders each column's tree via `build_tree(...).__str__` (the ASCII
art form). The `/lineage/<hash>` page now surfaces per-column trees
under a "column lineage" section — one `<details>` per output column,
first one open. Tests assert all three demo columns appear and that
the trees trace through Filter and Field nodes.

### T-18 — ML training pipeline

Beats 7–8 ("train logistic regression... try gradient boosting
instead"). xorq has `xorq.ml` helpers — haven't explored.

- **Fix:** TBD; needs a spike to understand xorq's ML surface, then
  decide whether to wrap as `catalog_train(name, target, features,
  model="logistic")` or expect Claude to write the xorq.ml call.
- **Verify:** beats 7–8 of the storyboard execute end-to-end.

### ~~T-19 — `pydata replay <storyboard.json>`~~ ✅ 2026-05-11

`pydata replay <file.json>` reads `{project, steps: [{tool, args,
narration?, skip?}]}` and drives the MCP tools in order. `--delay N`
paces between steps; `--continue-on-error` pushes through failures
for stage fallback. `demo/storyboard.json` exercises beats 1, 3, 4,
5, plus a chart attach — a regression test asserts it replays to a
known state under an isolated PYDATA_HOME.

### ~~T-20 — `/api/data/<hash>` pagination~~ ✅ 2026-05-11

Cursor-style pagination via `?offset=&limit=`. Default limit dropped
from 5000 → 200 (demo-safe; Buckaroo handles the full-data case
in-iframe via T-07). Response shape now includes `offset`, `limit`,
and `total` for clients that need to fetch more.

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

### T-33 — Memory-scaling integration test for the React embed

Once the iframe-to-React-embed switch lands (Option B in the chat:
local esbuild bundle that mounts `BuckarooServerView` from
`buckaroo-js-core` into a `[data-ws-url]` div), we need a test that
catches per-cell leaks before they hit the demo. Each notebook cell
becomes its own `BuckarooServerView` mount with its own WebSocket; an
N-cell notebook = N WS connections + N AG-Grid instances.

**What to measure, on the same machine, with the same fixture:**

- Server side: RSS of the Buckaroo subprocess (`buckaroo_lifecycle.py`'s
  `BuckarooManager.proc`) sampled before any cells load and after each
  cell loads. `psutil.Process(self.proc.pid).memory_info().rss` is the
  obvious hook.
- Browser side: `performance.memory.usedJSHeapSize` (Chromium only —
  the test driver will need to be Chromium) sampled at the same
  checkpoints from the rendered notebook page.

**Test shape:**

- Playwright spec under `tests/integration/` (new dir; we don't have
  one yet — slot next to `tests/test_mcp_stdio.py`-style integration
  files and gate with `@pytest.mark.integration`).
- Fixture: a project with N synthetic catalog entries, one notebook
  cell per entry. Parametrise N over `[1, 5, 10, 25]`.
- For each N: launch `pydata run`, load `/notebook`, wait for all
  embeds to reach `initial_state`, sample both metrics, then close the
  page and re-sample server RSS to catch sessions that don't release.
- Assert: server RSS growth per cell is sub-linear (i.e. shared
  per-server overhead dominates) or at least bounded by a soft cap
  (~20 MB/cell?); browser heap growth per cell stays under a soft cap
  (~10 MB/cell?). Numbers are placeholders — first run sets the
  baseline, future runs guard against regressions.

**Why this is worth a ticket and not just a follow-up:**

- The current iframe model isolates each cell in its own browser
  context; React-embed collapses them into one. Leaks that were
  invisible behind iframe boundaries become visible.
- The talk demo will load a multi-cell notebook live. If the embed
  approach leaks per cell, we will only find out when the audience
  sees it.
- Bounds the conversation about T-32 ("iframe pool") — once we have
  numbers, "should we lazy-mount on scroll?" stops being a guess.

**Files likely to touch:**

- `tests/integration/test_embed_memory.py` (new).
- `tests/integration/conftest.py` (new; project + entries fixture).
- `pyproject.toml` — add `playwright` to a `dev` or `integration`
  dependency group.
- `.github/workflows/tests.yml` — extend the integration job once the
  buckaroo path-source issue (T-26) is resolved.

Defer until Option B is merged. Should be the first test against the
new embed surface.

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

### ~~T-26 — No CI~~ ⚠ partial 2026-05-11

`.github/workflows/tests.yml` shipped as a starting point. Two jobs:
fast suite (default `-m 'not integration'`) and integration. Will not
work as-is until the buckaroo path source is replaced with a git ref
or a published PyPI version — `[tool.uv.sources] buckaroo = { path =
"../../buckaroo" }` can't resolve under `$GITHUB_WORKSPACE`. Workflow
text documents the constraint.

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
