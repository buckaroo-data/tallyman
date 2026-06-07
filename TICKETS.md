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
nothing. See `src/tallyman_companion/templates/base.html`.

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

### ~~T-06 — No `tallyman pack` command~~ ✅ 2026-05-11

`tallyman pack <project>` produces `./<project>-<YYYY-MM-DD>.tgz`,
skipping `__pycache__`, `.DS_Store`, and the stale
`buckaroo_sessions.json`. Round-trip serve verified by
`tests/test_pack.py::test_pack_round_trip_serves`. See also T-24
(also fixed) which would have invalidated builds on a rename.

### ~~T-31 — Buckaroo session pre-hydration~~ ✗ moot under React embed 2026-05-20

Premised on the iframe model where `GET /s/<session>` returned an HTML
shell that booted a JS bundle. After the iframe → React-embed swap
(5ee5cc7) the embed mounts directly into the companion's page and
hydrates over WS — there is no separate `/s/<session>` HTML to
pre-hydrate. The FOUC story is now "load WS → first frame," not
"load JS → connect WS → fetch → render"; the right next move there
is WS-side first-frame size reduction (see T-33's memory baseline)
rather than HTML pre-hydration.

### ~~T-32 — Iframe pool / multi-session caching~~ ✗ moot 2026-05-20

Same iframe → React-embed swap; there are no iframes left to pool.
Each `[data-ws-url]` div now mounts a `BuckarooServerView` in place,
and SPA-lite navigation swaps the host element via the
`MutationObserver` in `static/buckaroo-embed.js`. T-33 is the live
guard for the memory shape this changes to.

### ~~T-30 — XorqBuckarooInfiniteWidget over the standalone server~~ ✅ 2026-05-20

**Buckaroo side:** Filed as
[buckaroo-data/buckaroo#773](https://github.com/buckaroo-data/buckaroo/issues/773);
implemented as
[buckaroo-data/buckaroo#776](https://github.com/buckaroo-data/buckaroo/pull/776)
(`feat/load-expr-server`). Adds `POST /load_expr` + a `backend="xorq"`
discriminator on the existing `mode="buckaroo"` dispatch arm.

**tallyman-notebooks side:** `BuckarooManager.ensure_session` POSTs to
`/load_expr` with the entry's `xorq_build/` directory; the dead
`mode` parameter is gone. One subtlety surfaced during integration:
buckaroo's `xorq_loading.load_expr_build_dir` calls
`xorq.api.load_expr` directly, so it does **not** resolve tallyman's
`${TALLYMAN_PROJECT_ROOT}` placeholders. Without expansion the session
loads fine (schema flows from YAML) but every paged read returns
zero rows. ensure_session now `expand_to_tmp`s the build dir before
POSTing and rmtree's the tmp on `stop()`. Tracked per content_hash
so concurrent hits don't double-expand.

Verified end-to-end: `GET /catalog/shoe_sales` →
`buckaroo.server.handlers: load_expr ... rows=4 backend=xorq`.

Files touched: `src/tallyman_companion/buckaroo_lifecycle.py`,
`tests/test_buckaroo.py` (+5 tests: 4 stress, 1 row-count smoke
integration). pyproject + uv.lock pin buckaroo to the local PR 776
editable; switch to a git ref or published version once PR 776 lands.

### ~~T-07 — Buckaroo server is not integrated~~ ✅ done 2026-05-11

`tallyman run` now manages a Buckaroo subprocess (default `:8700`, falls
back to a random port if busy). Per-entry sessions are lazily created
on first view via `POST /load`, persisted under
`catalog/buckaroo_sessions.json` with an envelope shape that records
Buckaroo's `started` timestamp for restart detection. Iframe in
`/catalog/<hash>` points at `<base>/s/<session>`. Tear-down via stdin
close (Buckaroo's `--stdio-control` flag) with SIGTERM fallback.
Disable with `--no-buckaroo` if Buckaroo's runtime deps are missing.

Files added/touched: `src/tallyman_companion/buckaroo_lifecycle.py`,
`src/tallyman_companion/app.py`, `src/tallyman_companion/templates/entry_detail.html`,
`src/tallyman_cli/main.py`, `pyproject.toml`. 8 new tests
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
build_metadata.json, profiles.yaml). The `${TALLYMAN_PROJECT_ROOT}`
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

`tallyman_xorq.column_lineage(project, hash)` loads the entry via the
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

### ~~T-19 — `tallyman replay <storyboard.json>`~~ ✅ 2026-05-11

`tallyman replay <file.json>` reads `{project, steps: [{tool, args,
narration?, skip?}]}` and drives the MCP tools in order. `--delay N`
paces between steps; `--continue-on-error` pushes through failures
for stage fallback. `demo/storyboard.json` exercises beats 1, 3, 4,
5, plus a chart attach — a regression test asserts it replays to a
known state under an isolated TALLYMAN_HOME.

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

### ~~T-22 — Slow stdio tests aren't marked~~ ✅ already done

`tests/test_mcp_stdio.py` tests are all decorated with
`@pytest.mark.integration`; `pyproject.toml` has
`addopts = "-m 'not integration'"`. Closed on review 2026-05-20.

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
- For each N: launch `tallyman run`, load `/notebook`, wait for all
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

### ~~T-34 — `/internal/notify` should log the event `kind`~~ ✅ 2026-05-20

`@app.post("/internal/notify")` now `log.info`s `kind` and `hash`
before publishing to subscribers. Logger is `tallyman.companion`; INFO
level is on by default under uvicorn's `log_level="info"`.

### ~~T-35 — Companion startup banner should print buckaroo PID~~ ✅ 2026-05-20

`tallyman run` banner now includes `pid=<n>` next to the URL. After an
auto-restart in `BuckarooManager._maybe_restart`, the success log
includes both the new pid and the bound port (which can change if the
original port was reclaimed by something else).

### ~~T-23 — `tallyman init` silently re-runs~~ ✅ 2026-05-11

`tallyman init` now errors if the project already exists. `--force`
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

### ~~T-26 — No CI~~ ✅ 2026-05-24

`.github/workflows/tests.yml` runs three sequential jobs on push to
``main``/``spike/**`` and on PRs targeting ``main``: ruff lint, fast
test suite (`-m "not integration"`), and integration tests. Was
provisional until the buckaroo path source was resolved — pinning
``buckaroo==0.14.6`` from PyPI (commit a89c7b4) cleared the blocker.
The embed bundle is not built in CI; Python tests only reference its
URL, never load it.

### T-36 — `/api/data/<hash>` leaf goes Arrow → pandas → records

`api_data` in `src/tallyman_companion/app.py` currently ends with
`df.to_dict(orient="records")` after the parquet `iter_batches` →
arrow `slice()` → `to_pandas()` chain. The pandas step exists only to
re-emit records that FastAPI then re-serialises to JSON.

At the actual call volume (vega-embed asks for ≤200 rows of a
pre-aggregated result), this is fine — ~5ms per request. Tracked as
hygiene rather than a real problem: drop pandas from the leaf with
`table.slice(offset, limit).to_pylist()` (arrow → list[dict]
directly), saving one copy and the pandas import on this path.

Decision deferred — the architecturally pure answer ("send parquet
bytes, decode in browser with hyparquet, vega-embed gets
`{values: rows}` from the decoded objects") is over-engineered for
the chart-data scale we care about. Revisit only if a future caller
needs `/api/data` for larger payloads.

### ~~T-37 — Embed bundle: esbuild → Vite~~ ✅ 2026-05-24

`packages/embed/` migrated from a hand-rolled esbuild script to Vite
library mode. Key issue: `process.env.NODE_ENV` was not replaced at
build time, causing `ReferenceError: process is not defined` at
runtime. Fixed via `define: { "process.env.NODE_ENV":
JSON.stringify("production") }` in `vite.config.ts`.

Output: `src/tallyman_companion/static/buckaroo-embed.{js,css}` (both
in `.gitignore`; built locally before deploy / before demo). Build
command: `pnpm -C packages/embed build`.

### ~~T-38 — Charts in notebook view~~ ✅ 2026-05-24

`notebook_view` now passes `chart_spec` per cell (via `get_chart`).
Template renders a `.chart-panel.nb-chart` div with `data-spec` and
`data-hash` attributes for any cell that has a chart. The shared
`_vega_mount.html` partial (also used by entry_detail) fetches
`/api/data/<hash>`, merges the rows into the Vega-Lite spec, and
calls `vegaEmbed`. CSS: `.chart-panel` uses `display: block` to avoid
collision with vega-embed's own `.vega-embed { display: inline-block }`
rule.

### ~~T-39 — Post-processing MCP tools~~ ✅ 2026-05-24

Three new MCP tools mirror the summary-stats pattern:
`catalog_add_post_processing(name, source)`,
`catalog_remove_post_processing(name)`,
`catalog_list_post_processings()`. Source is validated by
`tallyman_core.post_processing.validate_post_processing_source` (exec in
restricted globals, dry-run against a 3-row memtable; accepts ibis
expr or pandas DataFrame). Scripts land in
`<project>/post_processing/<name>.py`; removal soft-deletes to
`<project>/post_processing/_disabled/`. 15 tests in
`tests/test_post_processing.py`, all passing.

### ~~T-40 — Large-parquet memory spike~~ ✅ 2026-05-24

Viewing a large expression (citibike, 57k rows) spiked companion RSS
to ~7 GB because `catalog_entry` and `api_data` both called
`pf.read()` unconditionally, materialising the full table into memory.

Fix: `_read_head_rows(path, n)` uses PyArrow `iter_batches`, stopping
once N rows are collected — O(N) memory regardless of file size.
`catalog_entry` now only reads rows when Buckaroo is unavailable
(falls back to the HTML preview); total-row count comes from
`pf.metadata.num_rows` without reading data. `api_data` uses
`_read_head_rows` capped at the requested `limit`.

### ~~T-41 — Embed size-toggle~~ ✅ 2026-05-24 (CSS layer; grid resize blocked upstream)

A cycle button on each notebook cell toggles `.nb-buckaroo` between
three heights: 260 px (compact default), 75 vh (`data-size="3q"`),
90 vh (`data-size="max"`). `nbCycleSize(cellId, btn)` drives the
`data-size` attribute; no page reload. CSS transitions on
`.buckaroo-embed` make it smooth.

Caveat: the AG Grid inside `BuckarooServerView` does not actually grow
when the host div expands, because `domLayout: 'autoHeight'` is not
exposed via `BuckarooServerViewProps` and the viewport-change signal
never reaches the grid. Tracked upstream as
[buckaroo-data/buckaroo#846](https://github.com/buckaroo-data/buckaroo/issues/846).
The CSS toggle is in place and will work once #846 is resolved.

### T-42 — BuckarooServerView does not resize when host div grows (upstream #846)

`BuckarooServerView` mounts an AG Grid with a fixed height derived
from the initial container size. When the companion toggles the
`.nb-buckaroo` div from 260 px to 75 vh, the host div grows but the
grid remains 260 px tall — extra rows are hidden.

Root cause: `domLayout: 'autoHeight'` is available in AG Grid but not
wired into `BuckarooServerViewProps`; even if it were, the component
needs to call `gridApi.sizeColumnsToFit()` or respond to a
`ResizeObserver` on the host element.

Filed upstream as
[buckaroo-data/buckaroo#846](https://github.com/buckaroo-data/buckaroo/issues/846).

- **Fix:** Once the upstream PR lands and `buckaroo-js-core` is
  published, bump the version in `packages/embed/package.json` and
  rebuild the bundle. No companion-side code changes needed.
- **Verify:** Toggle size on a notebook cell with >10 rows; all rows
  should be visible after toggle.

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
