# Installing tallyman

Steps to go from a fresh checkout to a working tallyman setup: the MCP server
that Claude Code drives, plus the browser companion that shows your catalog
update live.

## Prerequisites

Install these first:

- **[uv](https://docs.astral.sh/uv/)** — manages the Python environment. uv
  fetches Python 3.13 itself, so you don't need a system Python.
- **Node 18+ and [pnpm](https://pnpm.io/)** — to build the React companion UI
  (a Vite SPA the FastAPI server serves). The build artifact is not committed,
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

## 3. Build the companion UI (one-time)

The companion serves a React SPA that is built, not committed:

```sh
cd packages/app && pnpm install && pnpm build
cd ../..
```

This writes `packages/app/dist/`, which the FastAPI server mounts as a
catch-all. Without it the server returns a 503 with a build reminder. If you're
actively editing the UI, use `pnpm dev` (Vite dev server) instead.

> Keep `buckaroo-js-core` in `packages/app/package.json` in sync with the
> `buckaroo` pin in `pyproject.toml`, and rebuild whenever either side moves.

## 4. Initialize a project

A "project" is a named catalog/notebook workspace on disk.

```sh
uv run tallyman init spike
```

This creates `~/.tallyman-notebooks/projects/spike/`, writes a starter fixture
(`data/orders.parquet`; pass `--no-fixture` to skip it) and records the
project's first checkpoint, step 0. There is no CLI command that lists projects:
look under `~/.tallyman-notebooks/projects/`, use the project list in the
browser, or ask Claude to call the `project_list` MCP tool.

## 5. Start the companion app

The companion is a FastAPI app on `http://127.0.0.1:7860`. The MCP server tells
it about each change with an HTTP request, and it pushes live updates on to the
browser over SSE (a stream of events the browser keeps open).

```sh
uv run tallyman run --project spike
```

Leave this running in its own terminal. Open <http://127.0.0.1:7860> to watch
the catalog, notebook and diffs update as you work.

`tallyman run` also spawns a Buckaroo subprocess on `:8700` (or on a random port
if 8700 is taken), which draws each entry's data grid. Its stderr goes to
`~/.tallyman-notebooks/projects/spike/buckaroo.log`, and Buckaroo keeps its own
log in `~/.buckaroo/logs/server.log`. Disable it with `--no-buckaroo` if you
don't need the grids; an entry's page then shows its row count and a note that
Buckaroo is not available.

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

The copy in the repo has the owner's path in `cwd`; set it to your checkout.

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

> Use `catalog_import_source` to import `orders.parquet` from the project's
> `data/` directory under the alias `orders`.
>
> Now use `catalog_create` to make a named entry `shoe_sales` that groups orders
> by region and totals the price.
>
> Now revise `shoe_sales` to filter to `category == "boots"` only.

Watch the browser update as each tool call lands. Named entries float to the top
of the catalog with a `V_n` chip; previous versions stay in forensic history.

## Sharing a project

A project directory is portable: paths inside each entry's build are stored
relative to the project, so it can be read from any location.

```sh
uv run tallyman pack spike -o my-project.tgz
# on the other machine:
tar xzf my-project.tgz -C ~/projects/
uv run tallyman serve ~/projects/spike
```

`tallyman pack` tars the whole project directory, including `compute_cache/`
(the result files tallyman can make again), so the archive can be large.

`tallyman serve` runs the companion **read-only** against any project directory
on disk: same catalog, history and charts. The browser still shows the edit
controls, but the server answers their requests with 403. It starts no Buckaroo
subprocess, so entry grids do not load.

One known defect affects copies (#209). An archive carries each entry's
`.xorq_build_expanded/` directory, a copy of the build with the original
project's absolute path filled in, and tallyman reuses it without checking the
path. A cheap entry (a filter or selection over one file, with no result file of
its own) then reads from the original location, and fails once that location is
gone.

## Reference

**Ports**

| Port | Service |
|------|---------|
| 7860 | Companion (FastAPI) |
| 8700 | Buckaroo subprocess (falls back to a random port if busy) |

**Environment variables**

| Variable | Default | Purpose |
|----------|---------|---------|
| `TALLYMAN_HOME` | `~/.tallyman-notebooks` | Root for all project state |
| `TALLYMAN_PROJECT` | none | Seeds the `active_project` file when that file does not exist yet and the named project does (see below) |
| `TALLYMAN_COMPANION_URL` | `http://127.0.0.1:7860` | Where the MCP server and the CLI send notifications |
| `TALLYMAN_AUTO_RECALC` | unset | `1`/`0` (or `true`/`false`) overrides the project's auto-recalc switch (on by default) |
| `TALLYMAN_SOURCE_IDENTITY` | `cas` | How source files are identified: `cas`, `salt` or `off` |
| `TALLYMAN_LOG_LEVEL` | `INFO` | Log level of the MCP server |

**State on disk** lives under `TALLYMAN_HOME`:

```
~/.tallyman-notebooks/
├── active_project          # one-line plain text; the active project
└── projects/
    └── <name>/             # catalog, notebook, build artifacts per project
```

[architecture.md](architecture.md#on-disk-layout) has the full layout.

The active project is the one named in the `active_project` file. A command's
`--project` flag (`tallyman run --project spike`) names a project explicitly, and
`tallyman run` writes it to the file. `TALLYMAN_PROJECT` is read only when the
file does not exist yet and the project it names exists, to create the file, so
once the file exists it wins over the variable (#39). (`tallyman serve` is the
exception: it sets the variable to the served directory's name and reads it
while it runs.) The MCP server takes its project from the file at its first tool
call and keeps using it for the rest of the Claude Code session, until
`project_switch` or `project_new` changes it.

## Troubleshooting

- **`claude mcp list` shows "Pending approval"** — restart `claude` from the
  repo directory and approve the server.
- **Companion returns 503 / no UI** — you skipped step 3. Run the `pnpm build`
  in `packages/app`.
- **`uv run tallyman` fails to resolve the command** — make sure you're in the
  checkout directory (or a subdirectory). `uv run` picks the venv from the
  nearest `pyproject.toml`.
- **Port 7860 already in use** — another companion is running; stop it, or pass
  `--port` to `tallyman run` and update `TALLYMAN_COMPANION_URL` to match. Two
  companions on one project are not supported (#183).
- **An entry's data tab says "Buckaroo not available"** — the companion was
  started with `--no-buckaroo`, or Buckaroo failed to start; `tallyman run`
  prints why. If the tab shows an error instead, its detail says whether
  tallyman could not prepare the entry or Buckaroo could not load it, and
  `buckaroo.log` in the project directory has Buckaroo's side.
- **The browser stops responding while Claude builds an entry** — a build holds
  the project's write lock, and a page that needs the same lock (to re-create a
  deleted result file, or an edit made in the browser) waits for it (#186,
  #190).
</content>
</invoke>
