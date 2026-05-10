# pydata-app

Spike for the PyData London 2026 talk *"The Future of Notebooks in a Claude Code World"*.
The plan and proposal live in `plan.md` / `proposal.md`. This README covers the V0 spike only.

## V0 scope

End-to-end: a Claude Code MCP tool that compiles a xorq expression, materializes
a parquet to a content-hashed catalog entry on disk, and pushes a live update
to a browser companion via SSE.

What's in V0:

- One MCP tool: `catalog_run(code, prompt)` — wraps `xorq.ibis_yaml.compiler.build_expr`,
  writes `result.parquet` + manifest under `~/.pydata-app/projects/<name>/catalog/entries/<hash>/`.
- One MCP tool: `catalog_list()` — flat listing of every entry, most recent first.
- A FastAPI companion on `:7860` with `/catalog`, `/catalog/<hash>`, `/api/sse`,
  and `/internal/notify`. Tables render via `pandas.to_html` (no Buckaroo yet).
- Synthetic 2k-row shoe-orders fixture under `data/orders.parquet`.

What's NOT in V0 (deferred to later spikes, in this order):

1. Buckaroo iframe + Tornado subprocess.
2. Aliases (`catalog_create` / `catalog_revise`) and named-vs-scratch tiers.
3. Notebook tab + drag-reorder + markdown editor.
4. Lineage view (Cytoscape).
5. `pydata serve` read-only mode.

## Running the spike

```sh
uv sync
uv run pydata init spike            # creates ~/.pydata-app/projects/spike/ + fixture
uv run pydata run --project spike   # companion on http://127.0.0.1:7860 (foreground)
```

In another terminal, launch Claude Code from this directory; it picks up `.mcp.json`
and exposes the `pydata` MCP server with `catalog_run` + `catalog_list`.

Drive it with prompts like:

> Use catalog_run to load `~/.pydata-app/projects/spike/data/orders.parquet`
> with `xo.deferred_read_parquet` and aggregate total price by region.

Watch the browser update live as each tool call lands.

## Conventions worth knowing

- xorq 0.3.x reads use `xo.deferred_read_parquet` (NOT `xo.read_parquet` — that
  resolves through ibis's backend loader and fails). Use
  `import xorq.api as xo` and `import xorq.vendor.ibis as ibis`. Do NOT
  `import ibis` directly.
- Content hash is xorq's build hash — same code + same inputs → same hash → same
  entry dir (idempotent).
- All catalog state lives on disk. The MCP server holds no in-memory state; the
  companion only holds the SSE subscriber list.
