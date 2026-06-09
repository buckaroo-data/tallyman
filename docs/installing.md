# Installing tallyman

Steps to go from a fresh checkout to a working tallyman setup: the MCP server
that Claude Code drives, plus the browser companion that shows your catalog
update live.

## Prerequisites

Install these first:

- **[uv](https://docs.astral.sh/uv/)** — manages the Python environment. uv
  fetches Python 3.13 itself, so you don't need a system Python.
- **Node 18+ and [pnpm](https://pnpm.io/)** — only to build the companion's
  dataframe embed (a Vite/React bundle). The build artifact is not committed,
  so this is a one-time per-checkout step.
- **git** and **Claude Code**.

The project pins `requires-python = ">=3.13,<3.14"`. uv enforces this for you.

## 1. Clone the repo

```sh
git clone git@github.com:buckaroo-data/tallyman.git
cd tallyman
```

## 2. Install Python dependencies

```sh
uv sync
```

This creates `.venv/` in the repo and installs everything from `uv.lock`. You
do **not** activate the venv — every command below is prefixed with `uv run`,
which resolves the project's venv from the current directory automatically.

## 3. Build the companion embed (one-time)

The companion renders dataframes via a bundle that is built, not committed:

```sh
cd packages/embed && pnpm install && pnpm build
cd ../..
```

This writes `src/tallyman_companion/static/buckaroo-embed.{js,css}`. If you're
actively editing the embed, use `pnpm dev` (runs `vite build --watch`) instead.

> Keep `buckaroo-js-core` in `packages/embed/package.json` in sync with the
> `buckaroo` pin in `pyproject.toml`, and rebuild whenever either side moves.

## 4. Initialize a project

A "project" is a named catalog/notebook workspace on disk.

```sh
uv run tallyman init spike
```

This creates `~/.tallyman-notebooks/projects/spike/` and a starter fixture.
List or inspect projects later with `uv run tallyman project_list` (via MCP) or
just by looking under the projects root.

## 5. Start the companion app

The companion is a FastAPI app on `http://127.0.0.1:7860`. The MCP server pushes
live updates to it over SSE.

```sh
uv run tallyman run --project spike
```

Leave this running in its own terminal. Open <http://127.0.0.1:7860> to watch
the catalog, notebook, lineage, and diffs update as you work.

`tallyman run` also spawns a Buckaroo subprocess on `:8700` for in-table recon.
Disable it with `--no-buckaroo` if you don't need it.

## 6. Configure and approve the MCP server

The repo ships a project-scoped `.mcp.json` that registers the `tallyman`
server:

```json
{
  "mcpServers": {
    "tallyman": {
      "command": "uv",
      "args": ["run", "tallyman", "mcp"],
      "cwd": "<path-to-your-checkout>",
      "env": {
        "TALLYMAN_PROJECT": "spike",
        "TALLYMAN_COMPANION_URL": "http://127.0.0.1:7860"
      }
    }
  }
}
```

If you cloned to a different path, update `cwd` to point at your checkout.

Because `.mcp.json` is a project file, Claude Code will **not** trust it
automatically. Launch Claude Code from the repo directory and approve the
`tallyman` server when prompted:

```sh
claude
```

Verify the connection:

```sh
claude mcp list
```

You want:

```
tallyman: uv run tallyman mcp - ✓ Connected
```

If it shows `⏸ Pending approval`, restart `claude` in this directory and approve
it. To remove it later: `claude mcp remove tallyman -s project`.

## 7. Use it

With the companion running (step 5) and the MCP server approved (step 6), the
`mcp__tallyman__*` tools are available in chat. Try:

> Use `catalog_load_parquet` to load `orders.parquet`.
>
> Now use `catalog_create` to make a named entry `shoe_sales` that groups orders
> by region and totals the price.
>
> Now revise `shoe_sales` to filter to `category == "boots"` only.

Watch the browser update as each tool call lands. Named entries float to the top
of the catalog with a `V_n` chip; previous versions stay in forensic history.

## Sharing a project

A project directory is portable:

```sh
tar czf my-project.tgz -C ~/.tallyman-notebooks/projects spike
# on the other machine:
tar xzf my-project.tgz -C ~/projects/
uv run tallyman serve ~/projects/spike
```

`tallyman serve` runs the companion **read-only** against any project directory
on disk — same catalog and history, no edit affordances (mutation routes return
403).

## Reference

**Ports**

| Port | Service |
|------|---------|
| 7860 | Companion (FastAPI) |
| 8700 | Buckaroo subprocess (falls back to a random port if busy) |

**Environment variables**

| Variable | Default | Purpose |
|----------|---------|---------|
| `TALLYMAN_PROJECT` | `spike` | Active project name |
| `TALLYMAN_COMPANION_URL` | `http://127.0.0.1:7860` | Where the MCP server pushes updates |
| `TALLYMAN_HOME` | `~/.tallyman-notebooks` | Root for all project state |

**State on disk** lives under `TALLYMAN_HOME`:

```
~/.tallyman-notebooks/
├── active_project          # one-line plain text; the active project
└── projects/
    └── <name>/             # catalog, notebook, build artifacts per project
```

The active project is resolved from `TALLYMAN_PROJECT` first, then the
`active_project` file.

## Troubleshooting

- **`claude mcp list` shows "Pending approval"** — restart `claude` from the
  repo directory and approve the server.
- **Companion shows no dataframes / embed 404s** — you skipped step 3. Run the
  `pnpm build` in `packages/embed`.
- **`uv run tallyman` fails to resolve the command** — make sure you're in the
  checkout directory (or a subdirectory). `uv run` picks the venv from the
  nearest `pyproject.toml`.
- **Port 7860 already in use** — another companion is running; stop it, or pass
  `--port` to `tallyman run` and update `TALLYMAN_COMPANION_URL` to match.
</content>
</invoke>
