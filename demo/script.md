# pydata-app demo script

**Talk:** *The Future of Notebooks in a Claude Code World* — PyData London 2026, 2026-06-08.

**Stage time:** ~12 minutes live + 1 min hand-off + 1 min closing.

**Thesis to land:** the notebook is the *story*; the catalog is what *did the work*; the project directory is the *artifact* — content-hashed, reproducible, hand-off-able.

**Pairing**: this script is the human-readable companion to `demo/storyboard.json`, which `pydata replay` will execute deterministically if the live demo collapses (see Fallback).

---

## Pre-flight (run 60s before going on stage)

```sh
# Terminal A — companion + buckaroo subprocess
cd ~/code/pydata-app
uv run pydata init spike --force          # fresh fixture; idempotent on catalog
uv run pydata run --project spike         # http://127.0.0.1:7860

# Terminal B — Claude Code, .mcp.json auto-wires the pydata MCP server
cd ~/code/pydata-app
claude .
```

Visual: terminal B (left half of screen, Claude Code), browser at http://127.0.0.1:7860/ (right half, full-height). Catalog tab is empty except for the fixture.

Sanity checks before walking on stage:
- `lsof -iTCP:7860 -sTCP:LISTEN` — companion responding.
- `lsof -iTCP:8700 -sTCP:LISTEN` — buckaroo subprocess up.
- Browser shows empty catalog with the dataset fixture visible under `~/.pydata-app/projects/spike/data/orders.parquet`.

---

## Beats

Each beat = one prompt typed into Claude Code. Watch the browser update live. Presenter cue in *italics*.

### 1. Load and name the dataset (~60s)

> Use `catalog_load_parquet` to load `orders.parquet`, name it `orders`. The prompt is "raw orders dataset for the demo".

*Expected:* `orders` lands as the first named entry. Catalog tab auto-focuses on it. Buckaroo React embed mounts and starts streaming rows over the WS. Notebook tab now has one cell.

*Presenter:* "The agent picked `catalog_load_parquet` because I gave it a name. If I hadn't, it would've used `catalog_run` and the entry would be unnamed scratch. The tool choice is the legible signal of intent."

### 2. Free in-table recon (~45s, no prompt)

*Click a numeric column header in the Buckaroo embed.*

*Expected:* histogram + null counts + top values appear inline. No round-trip through Claude.

*Presenter:* "This is the recon tier — three tiers in this model. Inline Buckaroo recon for per-column questions. The next tier is the catalog, which holds every executed expression. The top tier is the notebook, which holds the curated story."

### 3. LLM-authored summary stat (~75s)

> Add a summary stat called `boots_pct` that computes the percentage of rows in a column where the value is "boots". Use `catalog_add_summary_stat`.

*Expected:* the tool writes `~/.pydata-app/projects/spike/stats/boots_pct.py`, validates it against a 1-row ibis memtable, returns success. The next time a session is loaded (next beat), the new stat appears in the summary-stats bar above the `category` column.

*Presenter:* "The stats bar above the Buckaroo table is project-scoped and authored by Claude. The agent writes a `compute(col)` function in a restricted-globals namespace. No real sandbox — this is single-user local — but the validator catches arity / import / non-ibis-return mistakes before the file lands."

### 4. Filter and name (~60s)

> Create a named entry `shoe_sales` that filters `orders` to `category == 'boots'` and groups by `region` with total price.

*Expected:* `shoe_sales` named entry; notebook now has two cells; new buckaroo session; the `boots_pct` summary stat from beat 3 shows up in the new session's stats bar.

### 5. Ad-hoc validation via a post-processing function (~75s)

> We want to spot regions with too few transactions. Add a post-processing function `low_volume` that filters to rows where `n < 100`. Use `catalog_add_post_processing`.

*Expected:* the tool writes `~/.pydata-app/projects/spike/post_processing/low_volume.py`, validates it against a small in-memory table (must define `process(expr) -> ibis expression | pandas DataFrame`), returns success. The file appears as a new option in the buckaroo embed's **post processing** dropdown on the next session load.

*Click the post-processing dropdown on the `shoe_sales` embed; pick `low_volume`.* Table re-renders to only the low-volume regions.

*Presenter:* "Post-processing functions are reusable lenses on a result. Same surface as the LLM-authored summary stats from beat 3 — restricted globals, dry-run validator, soft-delete to `_disabled/` — but they transform the *table* rather than compute a per-column scalar. Over the demo we build up a validation library; one click in the dropdown switches lenses without touching the underlying expression."

### 6. Revise in place (~75s)

> Refine shoe_sales to include `boots`, `sneakers`, and `sandals` — all categories of shoe.

*Expected:* `shoe_sales` V_2 chip; V_1 moves to forensic history; notebook cell updates **in place** (no new cell). Buckaroo session re-mounts on the new content hash.

*Presenter:* "The cell is the *concept*, not the iteration. Older versions stay in the catalog as forensic artifacts but never clutter the notebook."

### 7. Forensic diff (~45s)

*Click `Forensic history` in the entry detail, then click `Diff` next to V_1.*

*Expected:* `/diff/shoe_sales/1/2` opens — code diff (difflib unified), schema diff, per-column stats, key-joined side-by-side, head() side-by-side.

*Presenter:* "Every revise leaves an auditable trail. This is the diff a reviewer would want and almost never gets in a Jupyter workflow."

### 8. Chart attached (~30s)

> Attach a Vega-Lite bar chart to shoe_sales, region on x, total on y.

*Expected:* chart panel renders above the table in the entry detail. Vega-embed pulls data from `/api/data/<hash>`.

### 9. Notebook tab — rewrite markdown (~60s)

*Click the Notebook tab. Click the markdown block above `shoe_sales`. Replace its content (or have Claude do it).*

> Use `notebook_edit_markdown` to set shoe_sales's markdown to: "Shoes by region — broader bucket (boots, sneakers, sandals)."

*Expected:* markdown updates live. SSE pushes to any other connected browser.

### 10. Drag-reorder (~30s)

*Drag the `shoe_sales` cell above `orders` using the drag handle.*

*Expected:* SortableJS reorders; the new order persists in `notebooks/default.json`; SSE pushes to peers.

*Presenter:* "The browser and the agent have parallel surfaces. The agent can call `notebook_reorder`; I can drag. The notebook is curated by whoever is closer to the work at that moment."

---

## Hand-off (~60s)

*New terminal, in front of the audience:*

```sh
uv run pydata pack spike -o /tmp/demo.tgz
mkdir /tmp/demo-handoff && tar -xzf /tmp/demo.tgz -C /tmp/demo-handoff
uv run pydata serve /tmp/demo-handoff/spike --port 7861
```

*Open http://127.0.0.1:7861/ in a second browser window alongside the first.*

*Expected:* same catalog, same notebook, same Buckaroo embed, same forensic history. **No drag handles. No × buttons. No contenteditable.** The serve mode hides edit affordances and rejects `PATCH /api/notebook`, `PUT /api/markdown/<cell_id>`, and `POST /internal/notify` with 403. No MCP server.

*Presenter:* "The project directory *is* the artifact. A colleague with `pydata` installed sees exactly what I see. Every cell's expression is re-runnable from upstream parquet — content-hashed, deterministic, no kernel state to lose."

---

## Closing line

> "The notebook is the story. The catalog is what did the work. The project directory is the artifact — and it's reproducible because xorq cached every step by content hash, not by whatever happened to be in memory."

---

## Fallback (if the agent goes off-script or the network drops)

```sh
uv run pydata replay demo/storyboard.json --delay 2
```

Runs the same beats deterministically against the MCP tool surface. Pair it with the running companion on :7860 — the browser updates the same way as the live demo. Use `--delay 2` for stage pacing.

Also rehearse a clean reset in case you need to restart mid-talk:

```sh
rm -rf ~/.pydata-app/projects/spike
uv run pydata init spike            # fresh fixture, fresh catalog
```

---

## Timing cheatsheet

| Block | Target |
|---|---|
| Pre-flight | 60s, off-stage |
| Beats 1–3 (load, recon, summary stat) | ~3 min |
| Beats 4–6 (filter, scratch, revise) | ~3 min |
| Beats 7–8 (diff, chart) | ~75s |
| Beats 9–10 (notebook, drag) | ~90s |
| Hand-off | 60s |
| Closing | 30s |
| **Total** | **~11 min** + ~1 min slack |

If running long: drop beat 8 (chart) — it lands well but isn't load-bearing for the thesis. Never drop the hand-off; that beat is the entire point of "project directory as artifact".
