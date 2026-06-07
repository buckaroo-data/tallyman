# tallyman-notebooks demo script

**Talk:** *The Future of Notebooks in a Claude Code World* — Tallyman London 2026, 2026-06-08.

**Stage time:** ~12 minutes live + 1 min hand-off + 1 min closing.

**Thesis to land:** the notebook is the *story*; the catalog is what *did the work*; the project directory is the *artifact* — content-hashed, reproducible, hand-off-able.

**Pairing**: this script is the human-readable companion to `demo/storyboard.json`, which `tallyman replay` will execute deterministically if the live demo collapses (see Fallback).

---

## Talk framing (spoken in the first ~90s, before the demo starts)

Notebooks dominate scientific data work because data is large and computation is expensive. The standard answer to "don't recompute that" is kernel memory — you run the expensive cell once, the result lives in the Python process, and you carefully work around it for the rest of the session.

That's an adhoc, fragile cache. The failure mode is familiar: you have a dataframe that's right at the edge of your machine's memory. You spent twenty minutes figuring out how to do the join without OOM-killing the kernel. Now it's in memory and you need to keep it there. You won't re-run that cell. You won't restart the kernel. If the session dies, you start over. The notebook isn't reproducible — it's reproducible *except for that one cell*, which is the whole point.

But the OOM isn't even the worst part. The worst part is what happens after. The kernel dies. You lose everything — not just the cell that crashed, but the five-minute load and the two-minute join you ran before it. So you restart, re-run the load, re-run the join, and now you're staring at the cell that crashed. You don't run it yet. You test it on a sample first. You start navigating the notebook non-linearly — you know which cells are safe to re-run and which ones might kill you. You stop restarting the kernel even when you should, because there are results in memory you can't afford to lose. The notebook has become a minefield. It looks like a reproducible document. It isn't. It's a record of one particular session that you're too scared to replay from the top.

Marimo addresses a related but different problem: it enforces that the DAG of cell dependencies in your notebook can always be solved, so stale state from out-of-order execution can't hide. That's real progress. But it doesn't help you when the cell you're avoiding is expensive, not stale. Marimo will happily re-run an expensive cell every time its inputs change — which is exactly what you were trying to avoid.

xorq gives you a better answer: describe the computation, and xorq caches the result by content hash. Re-run the cell — same hash, cache hit, instant. The kernel isn't the cache. The filesystem is.

That's what this demo shows.

---

## Pre-flight (run 60s before going on stage)

```sh
# Terminal A — companion + buckaroo subprocess
cd ~/code/tallyman-notebooks
uv run tallyman init spike --force          # fresh fixture; idempotent on catalog
uv run tallyman run --project spike         # http://127.0.0.1:7860

# Terminal B — Claude Code, .mcp.json auto-wires the tallyman MCP server
cd ~/code/tallyman-notebooks
claude .
```

Visual: terminal B (left half of screen, Claude Code), browser at http://127.0.0.1:7860/ (right half, full-height). Catalog tab is empty except for the fixture.

Sanity checks before walking on stage:
- `lsof -iTCP:7860 -sTCP:LISTEN` — companion responding.
- `lsof -iTCP:8700 -sTCP:LISTEN` — buckaroo subprocess up.
- Browser shows empty catalog with the dataset fixture visible under `~/.tallyman/projects/spike/data/orders.parquet`.

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

*Expected:* the tool writes `~/.tallyman/projects/spike/stats/boots_pct.py`, validates it against a 1-row ibis memtable, returns success. The next time a session is loaded (next beat), the new stat appears in the summary-stats bar above the `category` column.

*Presenter:* "The stats bar above the Buckaroo table is project-scoped and authored by Claude. The agent writes a `compute(col)` function in a restricted-globals namespace. No real sandbox — this is single-user local — but the validator catches arity / import / non-ibis-return mistakes before the file lands."

### 4. Filter and name (~60s)

> Create a named entry `shoe_sales` that filters `orders` to `category == 'boots'` and groups by `region` with total price.

*Expected:* `shoe_sales` named entry; notebook now has two cells; new buckaroo session; the `boots_pct` summary stat from beat 3 shows up in the new session's stats bar.

### 5. Ad-hoc validation via a post-processing function (~75s)

> We want to spot regions with too few transactions. Add a post-processing function `low_volume` that filters to rows where `n < 100`. Use `catalog_add_post_processing`.

*Expected:* the tool writes `~/.tallyman/projects/spike/post_processing/low_volume.py`, validates it against a small in-memory table (must define `process(expr) -> ibis expression | DataFrame`), returns success. The file appears as a new option in the Buckaroo embed's **post processing** dropdown on the next session load.

*Click the post-processing dropdown on the `shoe_sales` embed; pick `low_volume`.* Table re-renders to only the low-volume regions.

*Presenter:* "Post-processing functions are reusable lenses on a result. Same surface as the LLM-authored summary stats from beat 3 — restricted globals, dry-run validator, soft-delete to `_disabled/` — but they transform the *table* rather than compute a per-column scalar. As the session goes on we build up a validation library; one click in the dropdown switches lenses without touching the underlying expression."

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
uv run tallyman pack spike -o /tmp/demo.tgz
mkdir /tmp/demo-handoff && tar -xzf /tmp/demo.tgz -C /tmp/demo-handoff
uv run tallyman serve /tmp/demo-handoff/spike --port 7861
```

*Open http://127.0.0.1:7861/ in a second browser window alongside the first.*

*Expected:* same catalog, same notebook, same Buckaroo embed, same forensic history. **No drag handles. No × buttons. No contenteditable.** The serve mode hides edit affordances and rejects `PATCH /api/notebook`, `PUT /api/markdown/<cell_id>`, and `POST /internal/notify` with 403. No MCP server.

*Presenter:* "The project directory *is* the artifact. A colleague with `tallyman` installed sees exactly what I see. Every cell's expression is re-runnable from upstream parquet — content-hashed, deterministic, no kernel state to lose."

---

## Closing line

> "The notebook is the story. The catalog is what did the work. The project directory is the artifact — and it's reproducible because xorq cached every step by content hash, not by whatever happened to be in memory."

---

## Fallback (if the agent goes off-script or the network drops)

```sh
uv run tallyman replay demo/storyboard.json --delay 2
```

Runs the same beats deterministically against the MCP tool surface. Pair it with the running companion on :7860 — the browser updates the same way as the live demo. Use `--delay 2` for stage pacing.

Also rehearse a clean reset in case you need to restart mid-talk:

```sh
rm -rf ~/.tallyman/projects/spike
uv run tallyman init spike            # fresh fixture, fresh catalog
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


-- notes
We need to talk about the thesis more.  "ntoebooks are used for scientific data because it is large and expensive to compute.  In kernel memory (better phrashing for this is needed) is an adhoc buggy cache that many notebook workflows depend on, but what you really want is a way to cache expensive computations.  Xorq provides a better way to do that. with xorq you can describe the computations you want, and they will be cached.  



more about the buggy cache.  I frequently have notebooks where I have done something expensive, normally a download script, reading in a dataframe or some join operation.  Sometimes - especially with dataframes that are close to the size of my machine's memory, I will figure out how to just barely get an operation done without OOM killing my kernel, then I'll have a dataframe in memory that I have to carefully operate on.  When I'm at that state I'll have this one cell that I need to execute once, then do other adhoc analysis in the notebook without re-running that cell.

This hurts notebook reproducability, because I'm specifically not re-executing all cells in sequence since that would disrupt my flow.  Marimo helps a little bit, but with a different problem, marimo makes sure that the graph of cell dependencies in your notebook can always be solved.  for me the only time my notebook goes out of linear dependency order is when I have been changing the order for adhoc experimentation.

I have done a limited amount of AI assisted notebook coding, but I have found that it is like collaborating with another human on the same notebook.  I don't trust the state of the kernel and cells to change beneath me in a coherent way.  


I joined xorq 3 months ago because I was excited about their core framework that regularlizes and unifies a lot of data engineering tasks into a declarative DSL that addresses cachability, reproducability, verifiable lineage into a coherent dsl.  This talk builds heavily on the xorq cache system to build a system for interactive data analysis that bridges the gap between what the tallyman arrow stack is good at, claude does well, and what notebooks are good at.


when working in a notebook on a data problem (before buckaroo) I generally start by loading the dataframe and typing `df.head()`  then I poke around a bit to get a feel for the data.  Buckaroo makes this manual inspection much quicker (it's still very relevant in the claude code world).  next I try to perform some operations on the dataframe, some types of transformations to expose a particular aspect or pattern in the data.  Claude is very helpful for these parts for writing the python code.

In addition to transforms I often end up with little transforms that I apply to dataframes (simple cleaning routines).  when writing traditional notebooks, these adhoc transforms are messy throw away code, adding them to a library is cumbersome so they just die right there.  Claude and buckaroo can help with this too.  Buckaroo has a facility called post_processing functions which take a dataframe and return a dataframe,  they can be interactively applied.

