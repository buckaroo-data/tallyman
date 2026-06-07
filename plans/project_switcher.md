# Project switcher — plan

Status: **post-grill, ready to ticket and implement.** Fifteen decisions locked
through interactive grilling; recorded below with the reasoning that landed
each one. Implementation order at the bottom.

## Goal

A user-visible project switcher in the companion chrome:

- Dropdown in the upper-left header (replacing today's static
  `project: {{ project }}` span)
- A `+ new` button next to it that creates a fresh project and switches to it
- Switching swaps both `/catalog` and `/notebook` to the new project on
  reload
- The MCP server (separate stdio process) follows along — `catalog_*` tools
  target the currently-active project without `--project` arguments
- Every project gets an `artifacts/` directory that contains everything
  the system produces from it (catalog, post-processing, stats, errors,
  exports)

## Architecture in one paragraph

The companion is multi-project-aware: it resolves the active project
per-request by reading `~/.tallyman/active_project` (a one-line plain-text
file). The companion is the *only* writer of that file; MCP write-tools
(`project_switch`, `project_new`) POST to the companion over HTTP rather
than touching the file directly, which keeps the SSE story honest. Read-side,
both the companion and the MCP read the file fresh on every call.
`resolve_project()` is the single function that returns the active project
name everywhere; tests monkeypatch *it*, not the file. One Buckaroo
subprocess serves sessions across all projects — content hashes are globally
unique, so per-project Buckaroo instances would be a category error.

The unit of work is the project. The kernel — Jupyter's expensive
process-resident state — has been factored away; what stayed are files on
disk, content-addressed. So "switch project" is functionally as cheap as
"navigate to a different URL." There's no kernel to spin down.

## Locked decisions (15)

1. **MCP write-tools go through the companion's HTTP API.** Single
   writer of the active-project file → SSE story works cleanly. MCP
   discovers the companion via `TALLYMAN_COMPANION_URL` env (set by whatever
   launches the MCP process). Read-tools read the file directly and work
   when the companion is down.

2. **One companion, multi-project-aware. One Buckaroo subprocess.**
   `create_app` stops capturing `project_name`; every route resolves
   per-request. Buckaroo accepts `project` as a per-`ensure_session`
   argument instead of a constructor argument. No `os.execv` on switch,
   no process pool, no port-per-project.

3. **Global active project, URLs unchanged for v1.** `/catalog`,
   `/notebook`, `/catalog/<hash>` as today. URL-prefix routing
   (`/p/<project>/…`) deferred to a follow-up if/when true side-by-side
   tabs are wanted.

4. **`artifacts/` holds everything the system produces:** `catalog/`,
   `post_processing/`, `stats/`, `errors/`, `exports/`. Inputs (`data/`)
   and the document (`notebooks/notebook.json`) stay at the project root.

5. **Hard break on existing projects.** No auto-migrate, no
   back-compat reader. Users re-init. (This is a spike branch; "build
   it, don't sweat continuity" was the explicit instruction.)

6. **The file is the sole source of truth at runtime.** `TALLYMAN_PROJECT`
   env is read *only* to seed the file on first run when no file exists.
   `resolve_project()` no longer reads env after that. CLI `--project foo`
   writes the file.

7. **Project names match `^[a-z0-9][a-z0-9_-]{0,31}$`.** Lowercase
   alphanum + hyphen + underscore, ≤32 chars, must start with alphanum.
   Reserved names blocked: `.`, `..`, anything starting with `.`,
   `active_project`, `buckaroo_sessions`. Collision returns 409 from
   `/api/projects/new`.

8. **`project_new(name, with_fixture=False)` everywhere.** Dropdown
   `+ new` calls with `with_fixture=False`; MCP `project_new` defaults
   to `False`. CLI `tallyman init <name>` keeps its existing
   `--with-fixture/--no-fixture` default-on flag for back-compat with
   demo workflows.

9. **Empty-state landing page on first launch.** Companion checks for
   *any* project at startup. None → serves a one-form page asking for
   a name; submit calls `/api/projects/new` and reloads. No zombie
   `spike` default created without the user's say-so.

10. **Active-project file format: plain text, one line, project name.**
    Nothing else. Recency, timestamps, companion URL live elsewhere or
    not at all. `cat ~/.tallyman/active_project` debugs trivially.

11. **Buckaroo sessions persisted to one global file** at
    `~/.tallyman/buckaroo_sessions.json`, schema
    `{<hash>: {session_id, project, buckaroo_started_at}}`. Migrate
    to a Buckaroo-side enumeration endpoint when
    [buckaroo#860](https://github.com/buckaroo-data/buckaroo/issues/860)
    lands — at which point the file goes away entirely.

12. **SSE events tagged with `project`; client-side filter.** Every
    `publish(...)` call includes the project the event happened in.
    The client (`base.html` event listeners) checks
    `document.body.dataset.activeProject` and ignores cross-project
    events. `project_switched` is the one untagged event — every tab
    listens.

13. **Switcher hidden in `tallyman serve` / read-only mode.** Header
    shows static `project: <served-name> · serve mode`. No dropdown,
    no `+ new`. Matches the existing pattern of stripping interactive
    chrome in read-only mode.

14. **MCP tags every tool response with `project`, plus emits a
    `warning` on detected switch.** MCP process holds in-memory the
    project name from the previous tool call. If the new call resolves
    to a different project, the response includes
    `"warning": "active project changed from <old> to <new> since last
    call; proceeding with <new>"`. Forces the LLM to notice mid-
    conversation drift instead of silently committing to the wrong
    project.

15. **Tests monkeypatch `resolve_project()` itself, not the file.**
    The `project` fixture in `conftest.py` does:
    ```python
    monkeypatch.setattr("tallyman_core.paths.resolve_project",
                        lambda explicit=None: explicit or name)
    # plus re-export sites: tallyman_core.resolve_project, tallyman_mcp.server.resolve_project
    ```
    The 90 `monkeypatch.setenv("TALLYMAN_PROJECT", project)` calls in the
    suite become no-op removals in a cleanup pass. Integration tests
    (subprocess-based) write the file directly via a `set_active_project`
    helper — those are the tests that genuinely exercise the storage
    mechanism.

## Per-project layout

```
~/.tallyman/
├── active_project                       # one-line plain text
├── buckaroo_sessions.json               # global session table
└── projects/
    └── <name>/
        ├── artifacts/
        │   ├── catalog/                 # parquet entries, aliases.json, history.json
        │   ├── post_processing/         # process(expr) scripts
        │   ├── stats/                   # summary-stats scripts (Buckaroo PR #784)
        │   ├── errors/                  # build failures
        │   └── exports/                 # marimo .py, screenshots, ad-hoc dumps
        ├── data/                        # input parquets (fixtures + user-provided)
        └── notebooks/
            └── notebook.json            # cell list (markdown + alias anchors)
```

`ensure_project(name)` creates the full tree. `catalog_dir(project)` now
returns `project_dir(project) / "artifacts" / "catalog"`. Same for
`post_processing_dir`, `stats_dir`, etc. — every "output" path under
`paths.py` re-targets to under `artifacts/`.

The xorq-catalog git repo (the `xorq catalog` CLI's data dir) lives at
`artifacts/catalog/` itself — no separate `.xorq-catalog/` sibling. First
use auto-runs `xorq catalog init` there, same best-effort posture as today.
`catalog_repo_path()` derives from `resolve_project()` instead of a fixed
location.

## Companion changes

### Per-request project resolution

`create_app(project: str | None = None, *, read_only=False, buckaroo=None)`
stops capturing `project_name`. Every route helper that needs the project
calls a new `_current_project(request)` that reads `resolve_project()`.

If `project` argument was passed to `create_app`, it seeds the
active-project file on startup *if and only if* the file doesn't already
exist. After that, the file is canonical.

If no project at all exists at startup (`~/.tallyman/projects/` empty
or absent), the companion serves the **empty-state landing page** for
every route, rendering a single form that posts to `/api/projects/new`.
Once one project exists, normal routing resumes.

### New routes

```
GET  /api/projects        → {"active": "foo", "available": ["foo", "bar"]}
POST /api/projects/switch → {"name": "bar"} → writes file, publishes
                            SSE project_switched, returns new active
POST /api/projects/new    → {"name": "baz", "with_fixture": false}
                            → calls ensure_project + (optionally) writes
                            fixture, switches active to it
```

All three return 409 on name collision (for /new) or 404 on unknown
project (for /switch). Name validation lives in
`tallyman_core.paths.validate_project_name(name) -> None | raises ValueError`.

### SSE event tagging

Every existing `publish(...)` call gains a `project` field. New event:

```
event: project_switched
data: {"name": "bar"}
```

`base.html` keeps a one-listener pattern:

```js
function eventMatchesActiveProject(data) {
  return !data.project || data.project === document.body.dataset.activeProject;
}
sse.addEventListener('new_entry', (e) => {
  const data = JSON.parse(e.data);
  if (!eventMatchesActiveProject(data)) return;
  // ... existing handling
});
sse.addEventListener('project_switched', () => window.location.reload());
```

`document.body.dataset.activeProject` is set server-side from
`resolve_project()` at template render time.

### Buckaroo: lift `project` to per-session

`BuckarooManager.__init__` no longer takes `project`. `ensure_session`
gains a `project: str` parameter. The session persistence file moves to
`~/.tallyman/buckaroo_sessions.json` with the schema noted in decision
11. On startup, the manager reads the file and prunes entries for which
`project_dir(entry.project) / "artifacts" / "catalog" / "entries" / hash` no
longer exists.

When buckaroo#860 lands: drop the file, ask Buckaroo for live sessions
on companion startup, build the map in-memory from the response.

## MCP changes

### `resolve_project()` reads the file every call

`tallyman_core.paths.resolve_project()` returns the contents of
`~/.tallyman/active_project`, falling back to `os.environ.get(
"TALLYMAN_PROJECT")` (one-shot seed) on first use when the file is absent.
After the first successful resolve, the env is ignored.

### New MCP tools

```
project_list() -> {"active": "foo", "available": ["foo","bar"]}
project_switch(name: str) -> {"previous": "foo", "active": "bar"}
project_new(name: str, with_fixture: bool = False)
    -> {"name": "baz", "created_at": "...", "active": "baz"}
```

`project_switch` and `project_new` implementation: POST to
`os.environ['TALLYMAN_COMPANION_URL'] + '/api/projects/...'`. If the env is
unset or the request times out, return
`{"error": "companion not reachable; project lifecycle tools require
tallyman run to be active"}`. Read tools (`project_list`) read the file
directly and work without the companion.

### Per-response project tag + switch warning

A small decorator wraps every existing tool function:

```python
_last_project: str | None = None

def _tag_project(fn):
    @functools.wraps(fn)
    def wrapped(*a, **kw):
        global _last_project
        result = fn(*a, **kw)
        current = resolve_project()
        if isinstance(result, dict):
            result.setdefault("project", current)
            if _last_project is not None and _last_project != current:
                result["warning"] = (
                    f"active project changed from {_last_project!r} "
                    f"to {current!r} since the last call; "
                    f"proceeding with {current!r}"
                )
        _last_project = current
        return result
    return wrapped
```

Applied to every `@mcp.tool()` registration. List-returning tools (e.g.
`catalog_list`) get wrapped to return `{"project": ..., "items": [...]}`
instead of a bare list — small breaking change to the tool surface,
worth doing once and documenting.

## Chrome (templates)

`base.html` `<header>` becomes:

```html
<header>
  <span class="brand">tallyman</span>
  {% if read_only %}
    <span class="project">project: {{ project }}</span>
  {% else %}
    <div class="project-switcher">
      <form method="post" action="/api/projects/switch" id="switch-form">
        <select name="name" id="project-select" aria-label="Active project">
          {% for p in available_projects %}
            <option value="{{ p }}" {% if p == project %}selected{% endif %}>{{ p }}</option>
          {% endfor %}
        </select>
      </form>
      <button type="button" id="new-project-btn" title="Create a new project">+ new</button>
    </div>
  {% endif %}
  <nav>…</nav>
  {% if read_only %}<span class="pill">read-only · serve mode</span>{% endif %}
  <span id="sse-status" class="pill">connecting…</span>
</header>
```

A tiny inline script handles dropdown change → POST → reload, and
`+ new` → `prompt()` → POST → reload. Validation errors (409 / 400)
surface inline as a `<div role="alert">` injected next to the header.

`prompt()` is the v1 UX for the new-project name. Replace with a proper
modal in a follow-up; doesn't gate the feature.

## Test strategy

Two paths:

- **Unit tests (vast majority).** `project` fixture monkeypatches
  `resolve_project()` to return the test's project name. Tests
  drop their `monkeypatch.setenv("TALLYMAN_PROJECT", ...)` lines in a
  cleanup commit (no-op once env is dropped from runtime resolution).
  The fixture patches at every re-export site (`tallyman_core.paths`,
  `tallyman_core`, `tallyman_mcp.server`) because `from … import resolve_project`
  binds at import time.
- **Integration tests (~5 sites).** Subprocess-based tests genuinely
  exercise the file mechanism; they call a new `set_active_project(name)`
  helper that writes the file. These also exercise the MCP-over-HTTP
  flow against a real running companion.

New tests:

- `test_paths.py` — `resolve_project()` reads the file; falls back to
  env when file absent; ignores env once file exists.
- `test_paths.py` — `validate_project_name` accepts good names, rejects
  reserved names, rejects bad regex matches.
- `test_companion.py` — `/api/projects` shape; switch updates file;
  new creates dir tree; SSE `project_switched` fires on switch.
- `test_companion.py` — empty-state landing page renders when
  `~/.tallyman/projects/` is empty.
- `test_mcp.py` — `project_switch` POSTs to companion URL; returns
  error when companion unreachable; `_tag_project` decorator adds
  `project` to every response and emits `warning` on switch.
- `test_buckaroo.py` — `ensure_session(hash, project)` works for two
  projects through one manager; session file at global location;
  stale-entry pruning on manager startup.

## First-launch flow

```
1. tallyman run --project=??? (no arg)
2. companion boots
3. checks: any ~/.tallyman/projects/<name>/ exist?
   → no:  serves empty-state landing page for every route
   → yes: checks active-project file
          → file exists & names a real project: use it
          → file missing or names a deleted project:
              pick first project alphabetically, write file, use it
4. companion serves normally
```

`tallyman init <name>` continues to exist as a CLI shortcut for the
fixture-on path (since the dropdown / MCP path now default to no
fixture).

## Implementation order — tickets

- **T-43 · Active-project file + paths refactor + project name validation.**
  Adds `resolve_project()` reads-the-file (with env-seed fallback);
  `validate_project_name`; `set_active_project` helper; `paths.py` refactor
  to put output dirs under `artifacts/`. `ensure_project` creates the new
  tree. *Hard break:* the existing `spike` project on disk stops working
  until the user re-inits.
- **T-44 · Companion: per-request project resolution + project routes +
  SSE tagging + empty-state page.** `create_app` stops capturing; new
  routes for list/switch/new; every `publish()` call tagged; client-side
  filter; empty-state landing page.
- **T-45 · MCP: project tools + per-response tag + switch warning.**
  `project_list/switch/new` tools. Write-tools POST to companion; read
  tools touch the file directly. `_tag_project` decorator wraps every
  existing tool.
- **T-46 · Buckaroo lifecycle: lift `project` to per-session.** Manager
  no longer constructor-scoped; session file at global location with
  the multi-project schema; stale-entry pruning on startup. Cross-link
  buckaroo#860 for the eventual cleanup.
- **T-47 · Chrome: dropdown + new-project button + read-only hide.**
  Template changes, inline JS for change/click handlers, error rendering.

Each ticket lands as test-first (failing tests on CI before fix lands,
per repo TDD rule), bundled per the test-vs-fix axis.

## Out of scope for v1

- Renaming or deleting projects from the UI. CLI workaround:
  `rm -rf ~/.tallyman/projects/<name>`, then choose another via the
  dropdown.
- Cross-project search (catalog index spanning projects).
- URL-prefix routing (`/p/<project>/…`) for true side-by-side tabs.
- A real modal for new-project name (using `prompt()` in v1).
- A recently-used list for dropdown ordering (sort alphabetical for
  now; add `~/.tallyman/recent_projects.json` later if needed).
- Per-project Buckaroo config (memory limits, theme).
- Symlinking external project trees into the dropdown. `TALLYMAN_PROJECT_PATH`
  / `tallyman serve` stays as the escape hatch.

## Open follow-ups

- **buckaroo#860** — session enumeration endpoint with caller
  metadata; collapses our session-persistence file.
- **(deferred)** URL-prefix routing — promote project from global file
  to URL segment.
- **(deferred)** Modal UX for new-project creation.
- **(deferred)** Loud-warning channel (Q14 (c)) — already done via the
  per-response `warning` field, but a dedicated SSE channel might be
  warranted if the LLM consistently misses inline warnings.
