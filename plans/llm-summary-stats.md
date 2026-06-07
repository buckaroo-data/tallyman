# plan — LLM-authored summary stats

A way for the agent to register new per-column summary stats at runtime
and have them appear in buckaroo's stats table on the next render, with
no buckaroo restart and no parallel registration system.

The phrase "the LLM knows xorq" is load-bearing: we assume the agent
can write a valid `col -> ibis_expr` function. We do not invent a DSL.

---

## What we're building

A stat is a single Python function that takes one ibis column expression
and returns one ibis Value (scalar or struct). The agent writes the
function, calls an MCP tool, the function lands on disk under the
project, and buckaroo picks it up the next time a `/load_expr` session
is created for an entry. No new endpoint to add the stat at the wire
level; no hot-reload inside an existing session in V1.

Example a Claude Code session might produce:

```python
# ~/.tallyman/projects/<name>/stats/percent_emails.py
def compute(col):
    return (col.cast("string").contains("@").sum() / col.count()).name("percent_emails")
```

This is exactly the function that goes into a generated `ColAnalysis`
subclass — no transformation, no parsing, no DSL. It compiles through
buckaroo's existing `compile_batch_expr` (PR #766) into the same batched
aggregate that drives every built-in stat, so the push-down model is
preserved end-to-end.

---

## Why this shape

| Decision | Rationale |
|---|---|
| Stats are pure Python functions, not a DSL | The agent knows xorq. Inventing a parallel grammar would only restrict it. The function signature `(col) -> Value` is the contract. |
| One file per stat | Trivial CRUD: write/replace/delete by filename. Mirrors how `catalog/entries/<hash>/` is organised. |
| Stats live under the project directory | The project is the artifact. A handed-off project carries its stats. `tallyman serve` shows them unchanged. |
| Buckaroo picks them up at `/load_expr` time, not via a new WS message | One scan per session-load is enough — every relevant render path goes through that. No new wire shape, no need to coordinate with running sessions. |
| Restricted exec namespace (no `__builtins__`, only `ibis` / `xorq`) | Not a real sandbox, but closes the function's reachable surface — no `__import__`, no `open`, no filesystem. Return-type checked. |
| Scalar OR struct return is fine; framework derives `provides_defaults` from the schema | Matches what built-in `ColAnalysis` classes already do. |

---

## Files and wires

```
~/.tallyman/projects/<name>/
├── catalog/                          # unchanged
├── stats/                            # NEW
│   ├── percent_emails.py             # one function per file, def compute(col): ...
│   ├── n_distinct_log10.py
│   └── _disabled/                    # (optional) parked stats kept around for history
│       └── pct_negatives.py
└── tallyman.toml                       # (optional) list of enabled stats, or "all"
```

Three new MCP tools on the tallyman-notebooks side:

| Tool | Behaviour |
|---|---|
| `catalog_add_summary_stat(name, source)` | Validate the source (parse + restricted-exec dry-run on a tiny in-memory table), write `stats/<name>.py`, return success. Errors if the function doesn't return an `ibis.Value`, or if `name` collides with a built-in stat. |
| `catalog_remove_summary_stat(name)` | Move `stats/<name>.py` → `stats/_disabled/<name>.py` (keeps the source around — see open question 3). |
| `catalog_list_summary_stats()` | Return `[{name, source, schema, applies_to_dtypes}]`. |

The companion's entry-detail page gains a small "custom stats" disclosure
under the existing build-artifacts section, listing the active stats and
the file path for each. No mutation UI in V1 — adding/removing stats is
through the agent.

---

## Buckaroo-side change

One new helper in `~/code/buckaroo/buckaroo/server/xorq_loading.py`,
called from `LoadExprHandler.post` between `load_expr_build_dir` and
the `XorqServerDataflow` construction:

```python
def load_project_stat_klasses(build_dir: str) -> list[type[ColAnalysis]]:
    """Scan <project_root>/stats/*.py, generate one ColAnalysis subclass per
    function, return the list. Caller appends to analysis_klasses."""
```

The handler reads the project root from a new field in the `/load_expr`
request body (`project_root: str`) — tallyman-notebooks passes it, headless
callers omit it and get the existing built-in stats only. Each generated
class is a thin shim:

```python
class _Generated_percent_emails(ColAnalysis):
    requires_summary = []
    provides_defaults = {"percent_emails": np.nan}
    summary_stats_display = ["percent_emails"]

    @staticmethod
    def computed_summary(_sd, col):
        return {"percent_emails": _compute_percent_emails(col)}
```

…where `_compute_percent_emails` is the function exec'd out of the
user's `.py` file, in a namespace of `{'__builtins__': {}, 'ibis': ibis,
'xorq': xorq}`. The dataflow's `analysis_klasses` is
`builtins + generated` — built-ins win on name collision (the MCP
tool already rejects collisions upstream, but defence in depth).

No change to `compile_batch_expr` or the stats pipeline — generated
classes go through the same path as the built-ins.

---

## Security boundary

The buckaroo subprocess is the data process. `exec`ing source from disk
into it is the obvious risk. Mitigations:

1. **Restricted globals on exec.** `{'__builtins__': {}, 'ibis': ibis, 'xorq': xorq}`. No `__import__`, no `open`, no `eval`. The function literally cannot reach those names.
2. **Return-type check.** Reject anything that isn't `ibis.expr.types.Value`. A function that side-effects but doesn't return an ibis expr fails the type check on first dry run.
3. **Dry-run on add.** `catalog_add_summary_stat` runs the function against a 1-row in-memory ibis table at write time. Syntax errors and shape errors fail at the MCP call, not later in the buckaroo subprocess.
4. **No `__getattribute__` walk on the input column.** The column argument is a real ibis `Column` — its attribute surface is what it is. Reaching `col.__class__.__bases__[0].__subclasses__()` is theoretically possible and a known sandbox-escape pattern, but the function still has nowhere to *call* arbitrary things to — no `__builtins__`.

This is not a real sandbox. It's a thin "no obvious foot-gun" wall.
Acceptable because: the agent is trusted, the data process is local,
and the project directory is the artifact (a handed-off project's stats
are inspectable Python files, not opaque blobs).

---

## Lifecycle

```
Claude Code terminal                tallyman-notebooks MCP                buckaroo subprocess
        │                                   │                              │
        │  catalog_add_summary_stat(        │                              │
        │      "percent_emails",            │                              │
        │      "def compute(col): ...")     │                              │
        ├──────────────────────────────────►│                              │
        │                                   │  dry-run on 1-row table      │
        │                                   │  write stats/percent_emails.py│
        │                                   │  POST /internal/notify       │
        │                                   │    kind=stats_changed        │
        │                                   │                              │
        │  (later, user clicks an entry)    │                              │
        │                                   │  GET /catalog/<hash>         │
        │                                   │  BuckarooManager.ensure_session│
        │                                   ├──────────POST /load_expr────►│
        │                                   │  body: {build_dir, project_root}
        │                                   │                              │  load_expr_build_dir(build_dir)
        │                                   │                              │  load_project_stat_klasses(project_root/stats)
        │                                   │                              │  XorqServerDataflow(expr,
        │                                   │                              │      extra_klasses=[...])
        │                                   │  ◄─session_id────────────────┤
        │                                   │                              │
```

`/internal/notify kind=stats_changed` doesn't trigger anything beyond a
client-side SSE event today — it's there so the companion's catalog page
can show "+1 stat: percent_emails" in the build banner. Sessions already
loaded keep their old `analysis_klasses` until they're re-loaded
(natural: the user clicks a different entry, that session gets the new
stats; or `/load_expr` is re-called on the same hash, which we already
do because session caches invalidate on buckaroo restart).

This means **stat changes don't propagate to a session you're currently
viewing**. If that turns out to matter for the demo, V2 ships a
`/refresh_session` endpoint that re-runs `load_project_stat_klasses` and
rebuilds the dataflow's analysis_klasses in place. Out of scope V1.

---

## Demo storyboard placement (rough)

Could slot between beats 4 ("verify shoe_sales by state") and 5
("refine shoe_sales"). New beat:

> *"I want to know what fraction of region names look like outliers —
> add a custom stat called `single_letter_region` that flags how many
> rows have a region with len ≤ 1."*

Companion effect: the stat appears in the column-stats panel for the
`region` column on `shoe_sales` and every other entry that has a
region column. Demonstrates the agent extending the *analytical
vocabulary* of the environment live — a thesis-level moment for the
talk.

---

## Open questions

1. **Per-column applicability.** `percent_emails` only makes sense on
   string columns; `n_distinct_log10` only on columns with a meaningful
   cardinality. Today's `ColAnalysis` model runs every analysis on every
   column. Cheap option: let the function check the column type and
   return `ibis.null()` for inapplicable columns. Cleaner option: a
   second metadata key `applies_to: ["string"]` next to `compute` that
   the generator reads. Lean: optional metadata, default = run-on-all.

2. **Naming collisions across stats.** Two custom stats both named
   `pct_x` — the second `add` rejects. Two stats both *emitting* a key
   called `pct_x` (e.g., one is `pct_x` and one returns a struct with a
   `pct_x` field) — also rejected at registration. The merged_sd
   namespace is flat; we don't try to scope.

3. **Removal semantics.** Soft delete via `_disabled/` keeps history;
   hard delete loses it. Lean: soft, with a separate `catalog_purge_summary_stat`
   tool if the agent wants to hard-clean. Matches the "catalog is
   curated, not append-only" principle from the main plan, but adds a
   second-class trash bin.

4. **Multi-column stats** (e.g., "correlation of price and qty",
   "delta between started_at and ended_at"). Today's `ColAnalysis`
   surface is column-at-a-time. Multi-column stats want a different
   shape — function takes the whole frame, returns a row-level stat or
   a scalar. Out of scope V1. Possible V2: a sibling `stats_frame/`
   directory with functions of signature `(expr) -> Value`.

5. **Stat failures in the batched query.** If one bad stat blows up
   `compile_batch_expr`, do we lose all the stats for that render?
   Today's pipeline doesn't try-each. Lean: V1 catches at the
   generator level (each generated `computed_summary` wraps in
   try/except, returns `{name: np.nan}` on failure, logs once). The
   batched query stays whole; the bad column just shows nan + a small
   icon in the stats panel.

6. **Editing a stat without an explicit MCP call.** A user opens
   `stats/percent_emails.py` in an editor and saves changes. Should
   `tallyman run` watch the directory and re-emit `stats_changed`?
   Probably yes for ergonomics, but V1 can require the agent to
   re-call `catalog_add_summary_stat` to pick up changes. Watcher is a
   V2 hardening item.

7. **MCP tool boundary.** Does `catalog_add_summary_stat` go in
   `tallyman_mcp` or buckaroo's own MCP surface (`pip install
   buckaroo[mcp]`)? Lean: tallyman-notebooks side. The stats live under
   tallyman-notebooks's project directory; buckaroo just consumes a directory
   path. If a future buckaroo-mcp wants to mirror this, it can.

---

## What's not in V1

- Hot-swap of stats in already-loaded sessions. Add → click an entry →
  see the stat. Don't try to invalidate existing sessions.
- Multi-column / whole-frame stats.
- Per-column-type applicability rules.
- File watcher for stats-edited-outside-the-MCP-tool.
- Sharing stats across projects.
- Built-in stat *overrides* (renaming `mean` to mean something else).
  Built-ins win on collision; if you want a different mean, name it
  something else.

---

## Sketch of the smallest viable PR shape

1. **buckaroo side** (`~/code/buckaroo` on a feature branch off
   `smorgasbord/scoped-sd`):
   - `buckaroo/server/xorq_loading.py`: add `load_project_stat_klasses(stats_dir)`
   - `buckaroo/server/handlers.py:LoadExprHandler.post`: accept optional `project_root`, call the new helper, pass `extra_klasses` through.
   - `buckaroo/server/xorq_loading.py:XorqServerDataflow`: thread `extra_klasses` into `analysis_klasses`.
   - Test: a project dir with one `stats/foo.py` produces a session whose stats include `foo`.

2. **tallyman-notebooks side**:
   - `tallyman_mcp/server.py`: add `catalog_add_summary_stat`, `catalog_remove_summary_stat`, `catalog_list_summary_stats`.
   - `tallyman_core/`: a tiny `summary_stats.py` module — write/read/list/dry-run.
   - `tallyman_companion/buckaroo_lifecycle.py:ensure_session`: pass `project_root` in the `/load_expr` body.
   - Tests: dry-run rejects bad source; `/load_expr` carries `project_root`; integration test that an added stat shows up in the `/api/data/<hash>` response (or wherever stats land in the companion's view).

About a day's work end-to-end, mostly tests.
