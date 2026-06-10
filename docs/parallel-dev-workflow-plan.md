# Plan: developing multiple tallyman PRs at once

Status: **draft for grilling** — nothing here is implemented yet.

This addresses three blockers to working on several PRs/worktrees simultaneously:

1. Only one tallyman can run at a time (port + global-state contention).
2. Restarting/resetting the single environment via Claude is slow and manual.
3. Coordinating a tallyman PR with an unreleased local-dev buckaroo without breaking CI or merging prematurely.

---

## Current state (from code, with citations)

### Runtime isolation leaks

| Resource | Location | Configurable today? |
|---|---|---|
| Companion HTTP `:7860` | `src/tallyman_cli/main.py:137` | `--port`, default 7860 |
| Buckaroo tornado `:8700` | `src/tallyman_cli/main.py:144` | `--buckaroo-port` (0 = random) |
| Vite dev `:5173` | `packages/app/vite.config.ts:14` | **hardcoded** |
| Vite proxy target | `packages/app/vite.config.ts:9` | `TALLYMAN_API_PORT`, default 7860 |
| `active_project` (global) | `~/.tallyman-notebooks/active_project` (`paths.py:47`) | only via `TALLYMAN_HOME` |
| `buckaroo_sessions.json` (global) | same dir (`paths.py:51`) | only via `TALLYMAN_HOME` |
| diff build temp (shared) | `/tmp/tallyman_diff_builds` (`app.py:233`) | **hardcoded** |
| MCP env | `.mcp.json`: `TALLYMAN_PROJECT=spike`, `TALLYMAN_COMPANION_URL=...:7860` | committed file |

Per-project artifact dirs under `~/.tallyman-notebooks/projects/<name>/` are already isolated by project name. The two truly global files are `active_project` and `buckaroo_sessions.json`, both gated only by `TALLYMAN_HOME`.

### Reset / lifecycle

- No Makefile / justfile / task runner. No `.claude/` skills or commands.
- Start = 3 manual pieces: `uv run tallyman run --project <p>`, `cd packages/app && pnpm dev`, and the MCP server (spawned by Claude from `.mcp.json`).
- Reset = `uv run tallyman reset-to <step|label> --project <p>` (`catalog_state.py:413`), which best-effort POSTs `/internal/notify` to a running companion on `:7860`.

### Buckaroo coupling (current working tree, uncommitted)

- `pyproject.toml:23` pins `buckaroo==0.14.15`; uncommitted diff adds `[tool.uv.sources] buckaroo = { path = "../code/buckaroo", editable = true }` and rewrites `uv.lock` to the editable source.
- `packages/app/package.json:16` already points `buckaroo-js-core` at `file:/Users/paddy/code/buckaroo/...` (local).
- `packages/embed/package.json:12` pins `buckaroo-js-core` to npm `0.14.8`.
- CI (`.github/workflows/tests.yml`) just runs `uv sync` — a committed editable/path source breaks CI on any box without the sibling checkout.
- No `.pre-commit-config.yaml`; git hooks are stock samples.

---

## Problem 1 — run multiple tallyman instances at once

**Setup reality:** isolation is **per separate checkout**, not per git worktree. The dev layout is paired full clones: `~/code/tallyman1` ↔ `~/code/buckaroo1`, `~/code/tallyman2` ↔ `~/code/buckaroo2`, etc. (`~/code/tallyman` with no suffix = slot 0). Each tallyman clone's `pyproject.toml` points buckaroo at its paired sibling (`../buckarooN`).

**Goal:** each checkout runs a fully isolated stack (companion + buckaroo + vite + MCP + data home) with zero cross-talk, with **no per-checkout manual configuration**.

**Design: slot = the integer suffix of the checkout directory name.**

`~/code/tallymanN` → slot `N` (no suffix → slot 0). Derive everything from `N`:

- Companion port = `7860 + N*10`
- Buckaroo port = `8700 + N*10` (or `0` for random)
- Vite port = `5173 + N*10`
- `TALLYMAN_HOME = ~/.tallyman-notebooks-N` (or `<checkout>/.tallyman-home`) — isolates `active_project`, `buckaroo_sessions.json`, and all project data in one move.

Auto-deriving from the directory name means cloning `tallyman3` "just works" with no env to remember. The slot script reads `basename "$PWD"`, strips the `tallyman` prefix, and defaults to 0.

**Code changes required (small):**

1. **Vite port configurable.** `vite.config.ts:14` → `port: Number(process.env.TALLYMAN_VITE_PORT || 5173)`. (Proxy target already reads `TALLYMAN_API_PORT`.)
2. **Diff build temp namespaced.** `app.py:233` `/tmp/tallyman_diff_builds` → include a per-instance suffix (e.g. from the slot or `TALLYMAN_HOME`) so two companions don't clobber each other's builds.
3. **Per-checkout MCP config.** `.mcp.json` is committed identically in every clone with `TALLYMAN_COMPANION_URL=...:7860` / `TALLYMAN_PROJECT=spike` baked in. Two clones would both point at `:7860`. Options:
   - (a) Use a **local, gitignored** `.mcp.json` per checkout (Claude reads project-local MCP config); commit a `.mcp.json.template` and generate the real one from the slot.
   - (b) Keep `.mcp.json` committed but have its env read from a gitignored `.env` the checkout exports.
   - Either way `TALLYMAN_COMPANION_URL`/`TALLYMAN_PROJECT` must come from the slot, not the hardcoded `:7860`/`spike`.

**Open question:** `TALLYMAN_HOME` keyed by slot (`~/.tallyman-notebooks-N`, survives a re-clone) vs. inside the checkout (`<checkout>/.tallyman-home`, dies with the clone). Recommendation: **`~/.tallyman-notebooks-N`** — data persists if you blow away and re-clone `tallymanN`, and it's still fully isolated per slot.

**Deliverable:** a sourced `scripts/slot-env.sh` (no arg — derives the slot from the checkout dir name) that exports `TALLYMAN_HOME`, `TALLYMAN_*_PORT`, `TALLYMAN_API_PORT`, `TALLYMAN_VITE_PORT`, `TALLYMAN_COMPANION_URL` consistently, consumed by both the run script and the MCP config generator.

---

## Problem 2 — fast, scripted reset/restart

**Goal:** one command (and a Claude skill wrapping it) that tears down and brings up a worktree's stack to a known baseline, with a health check, in seconds.

**Design: `scripts/tally.sh` subcommands** (single source of truth, slot-aware):

- `tally up` — start companion + buckaroo + vite for this slot (background, write PIDs/logs under `$TALLYMAN_HOME/run/`).
- `tally down` — stop only the processes this slot started (PID files; never by bare port to avoid killing browser tabs — use `lsof -ti TCP:<port> -sTCP:LISTEN`).
- `tally reset [<ref>]` — `tallyman reset-to <ref>` (default a `baseline` label) + notify companion.
- `tally restart` — `down` + `reset` + `up`.
- `tally status` — health-check each port, print PIDs/URLs.

**Claude skill** `parallel-dev` (or `tally`) under `.claude/skills/` that just shells out to `scripts/tally.sh <subcommand>` and reports status. Keeps the heavy logic in the script (testable, runnable without Claude) and the skill thin.

**Open questions:**
- Should MCP restart be part of `restart`? MCP is spawned by Claude, not the script — the skill may need to tell the user to reconnect, or we rely on companion-only restart while MCP stays connected (it just re-hits HTTP).
- Process supervision: plain background + PID files, or a tool (overmind/foreman/honcho via a `Procfile`)? Recommendation: start with PID files in the script; add a Procfile only if it earns its keep.

---

## Problem 3 — local-dev buckaroo coordination

**Default vs. exception (the governing rule):** the committed/default state of every tallyman checkout points at the **latest released buckaroo** — Python `buckaroo==X.Y.Z` from PyPI and JS `buckaroo-js-core: "X.Y.Z"` from npm. That is the only form CI and `main` ever see. Pointing at the **companion checkout** (`../buckarooN`, editable/`file:`) is a deliberate, opt-in, **never-committed** exception you take only while tallyman needs unreleased buckaroo changes. When those changes are done you cut a buckaroo release, swap tallyman back to that released pin, and only then can the tallyman PR merge.

Two separable concerns: (A) keep the companion-buckaroo swap out of any commit; (B) don't *merge* a tallyman PR before the buckaroo release that backs it exists.

### A. Guard against committing a companion-buckaroo swap

The swap is a **coordinated multi-file edit on both language sides** — so the guard and the helper scripts must treat them as one unit:
- Python: `pyproject.toml` `[tool.uv.sources] buckaroo = { path/editable }` + an editable/directory `uv.lock` entry.
- JS: `packages/app/package.json` `"buckaroo-js-core": "file:../../../buckarooN/..."` (+ `link:`) and the matching `pnpm-lock.yaml`.

The failure mode is any of those landing in a commit.

**Mechanism: a pre-commit / pre-push guard** (new `.pre-commit-config.yaml` or a repo git hook) that **fails if staged `pyproject.toml`/`uv.lock`/`package.json`/`pnpm-lock.yaml` contain a local source**:
- `pyproject.toml`: reject `[tool.uv.sources]` entries with `path =`/`editable =`/`git =` for buckaroo.
- `uv.lock`: reject `source = { editable = ... }` / `{ directory = ... }` for buckaroo.
- JS: reject `"buckaroo-js-core": "file:..."` / `"link:..."` (the released form is a semver string).

This lets you keep the local pin in your *working tree* indefinitely; the guard only triggers on commit/push. Pairs with a **mirror CI check** (a tiny workflow that greps the committed files for local sources) so the guard can't be bypassed with `--no-verify`.

**Workflow ergonomics:** two helper scripts, each acting on **both** language sides at once:
- `scripts/buckaroo-companion.sh` — swap Python + JS to the paired `../buckarooN` checkout (slot-derived path), `uv sync` + `pnpm install`. This is the opt-in dev exception.
- `scripts/buckaroo-release.sh <version>` — restore the released Python pin + npm pin, re-lock both. This is the default/committed form.

Because each `tallymanN` pairs with `../buckarooN`, the companion path differs per checkout — another reason it must never be committed. Default is released; you only run `companion` while actively changing buckaroo, and you run `release` before opening/finishing the PR.

**Open question:** rely purely on the commit guard, or also keep the swap physically uncommittable (e.g. `git update-index --assume-unchanged` on the four files, or a gitignored uv/pnpm override if one exists)? Recommendation: **guard + helper scripts** as the floor; investigate a uv-native override only if the round-trip through `release.sh` proves annoying. (Needs a uv-docs check — see open questions.)

### B. Gate merge on a buckaroo release

This is a standard GitHub merge-gate problem. Options, roughly increasing in rigor:

1. **Required status check** — a CI job `buckaroo-release-ready` that fails until `pyproject.toml` pins `buckaroo >= <required version>` AND that version resolves on PyPI (`uv pip index`/`pip index versions`). Add it to branch protection on `main` as a required check. The PR literally cannot merge until the released buckaroo exists and is pinned. **Recommended** — it's enforced, not just convention.
2. **PR body / label convention** — a "blocked: needs buckaroo release" label + a check that fails while the label is present. Lighter, but manual.
3. **Draft PR + checklist** — keep the PR in draft with a task-list item "buckaroo X.Y.Z released and pinned"; convert to ready only when done. Pure convention, no enforcement.

**Recommendation:** (1) as the enforcement floor, optionally (2) for human signalling. Encode the dependency explicitly in the PR body: `Requires buckaroo >= X.Y.Z (buckaroo PR #NNN)`.

**Open question:** does CI already have a place to add a required check, and is branch protection on `main` configured (it gates whether "required status check" is even possible)?

---

## Proposed sequencing (separate PRs)

This planning doc is its own PR. The implementation splits into focused PRs:

1. **PR: runtime isolation** — make vite port + diff-build temp configurable; slot-env script; `.mcp.json` → template + gitignored local. (Problem 1)
2. **PR: lifecycle scripts + skill** — `scripts/tally.sh` + `.claude/skills/`. (Problem 2)
3. **PR: buckaroo pin guard** — pre-commit/pre-push hook + mirror CI check + helper swap scripts. (Problem 3A)
4. **PR/config: merge gate** — `buckaroo-release-ready` CI check + branch protection. (Problem 3B)

PRs 1–3 are independent; 4 depends on nothing but touches repo settings.

---

## Open questions for the grilling

1. **Data isolation:** `TALLYMAN_HOME=~/.tallyman-notebooks-N` (survives re-clone) vs `<checkout>/.tallyman-home` (dies with clone)? Do you ever want to share project data between checkouts, or is each slot fully independent?
2. **Slot derivation:** auto from the `tallymanN` dir-name suffix (zero config, but breaks if a checkout is named differently) — acceptable, or do you want an explicit `.slot` file / env as the source of truth? How many concurrent slots realistically — 3? 5?
3. **uv local-override mechanism:** is there a uv-native way to keep a local source out of the committed lock (override file / env), or do we lean entirely on the commit guard? (Needs a uv-docs check before committing to an approach.)
4. **MCP restart story:** acceptable for `restart` to bounce only the companion and leave MCP connected over HTTP, or must MCP fully cycle?
5. **Guard strictness:** hard-fail on commit (you must run a swap script first), or auto-rewrite the pin to released form on commit? Auto-rewrite is convenient but surprising.
6. **Branch protection:** is `main` protected with required checks today, or is that a setup step here too?
7. **Frontend buckaroo (mostly settled):** Python + JS swap together via one script and both default to released. Remaining wrinkle: `packages/embed` pins npm `0.14.8` while Python pins `0.14.15` — do these need to move in lockstep on release, and does `embed` ever need the companion swap, or only `packages/app`?
8. **Scope creep check:** is the right first move just "make two run at once" (PR 1), and defer 2–4 until that's proven?
