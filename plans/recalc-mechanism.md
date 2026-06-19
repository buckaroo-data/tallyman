# How reactive recalc works

- **Status:** Built (2026-06-18) on `feat/89-reactive-recalc`. This documents the
  mechanism as implemented, not a proposal. It is the Stage 2/3 follow-on to
  `plans/reactive-staleness-recalc.md` (the staged plan) and
  `plans/adr-reactive-catalog-recalc.md` (the design questions). Stage 1
  (staleness detection) shipped in PR #126; the #115 cold-read blocker in PR #127.
- **Code:** `src/tallyman_xorq/recalc.py` (the engine), `tallyman_xorq/staleness.py`
  and `tallyman_xorq/dependents.py` (Stage 1, the inputs it reads), the MCP tools
  `catalog_scan_staleness` / `catalog_recalc` in `tallyman_mcp/server.py`, and the
  companion routes `GET /{project}/api/staleness` / `POST /{project}/api/recalc` in
  `tallyman_companion/app.py`.

## The problem

A tallyman project is a few raw sources and many derived entries. Two kinds of
change leave derived entries silently wrong:

1. **An upstream entry is revised.** `from_catalog("A")` binds the *value* alias A
   had when the child was built. Revise A and the child keeps reading A's old
   version.
2. **A raw source is edited in place.** xorq keys its caches on the path, not the
   content, so an in-place edit is invisible to identity.

Recalc closes the loop: edit a source or revise an upstream entry, then recompute
the dependents on demand instead of leaving them stale.

## Two halves: detect, then act

The consumer is split so the read side can run with no risk of mutation.

- **Detect** (`staleness.py`, Stage 1, read-only). `scan(project)` returns a
  `StaleVerdict` per entry. An entry is *stale* when an input it recorded at build
  no longer matches the world, on either of two axes:
  - **alias** — a `from_catalog("A")` parent (recorded `follow=True`) whose alias
    head now resolves to a different hash than the one recorded.
  - **source** — a recorded `manifest.sources` digest that no longer matches the
    file on disk.
  A *directly* stale entry has its own input moved; a *transitively* stale entry
  is a clean descendant of a stale ancestor in the dependency cone.
- **Act** (`recalc.py`, Stage 2). `recalc(project, roots, *, dry_run)` walks the
  roots' descendant cone and recomputes it.

## The dependency graph

`manifest.parents` records each entry's resolved `from_catalog` edges
(`{hash, ref, follow}`) at build time. `dependents.py` reads them:

- `build_dag(project)` — forward edges `{child: [ParentRef]}`, enumerated over
  every on-disk `manifest.json` (so a freshly-built, not-yet-checkpointed entry is
  seen).
- `descendant_cone(project, roots)` — the roots plus all their transitive
  dependents, **topologically ordered** (parents before children) via Kahn's
  algorithm. A cycle raises `DependencyCycleError` rather than looping.

The topological order is the load-bearing part: it guarantees a parent is
recomputed and its alias re-pointed *before* any child that reads it.

## The recalc walk

`recalc(project, roots, dry_run=False)` does this:

1. Drop any root that names no on-disk entry, then `cone = descendant_cone(roots)`.
   A cycle returns a report with `status="cycle"` and builds nothing.
2. `verdicts = scan(project)` once, for the report's per-entry staleness.
3. **Dry run** (`dry_run=True`): return the ordered cone with each entry's verdict
   and a planned `action` (`rebuild` / `cascade` / `unchanged`). Nothing is built;
   no alias moves. This is the preview the surfaces call first.
4. **Real run**: walk the cone in order. For each entry:
   - Read its persisted recipe (`expr.py`, with the `${TALLYMAN_PROJECT_ROOT}`
     placeholder expanded) and replay it through `build_and_persist` — the same
     primitive `catalog_revise` uses.
   - If the replay yields a **new** hash, record `old -> new` in the remap,
     re-point every alias whose head was the old hash via `set_alias`, carry the
     chart/display config forward (`carry_forward_entry_config`), and clear the
     in-process result-plan cache.
   - If the replay yields the **same** hash, it is a no-op: the alias is left
     untouched.
5. Take **one** checkpoint for the whole walk, but only if something actually
   changed (a non-empty remap). The recalc is then a single git revision
   `reset_to` undoes atomically.

### Why replaying the whole cone is correct and idempotent

The walk replays every entry in the cone, including ones that turn out not to
change. That is safe because of two facts:

- **A followed parent re-resolves to the advanced head.** A recipe names its
  followed parent by alias (`from_catalog("A")`). `io.from_catalog` resolves that
  to the alias's *current* head at build time (`get_alias`). Because the walk
  re-points alias A before it reaches A's children, each child's replay reads the
  advanced A. The new upstream content propagates into the child's expression
  graph (under `cas`, the content digest rides in the source path), so the child's
  `content_hash` changes and it, in turn, advances.
- **An unchanged input replays to the same hash.** `build_and_persist`
  early-returns when the target entry dir already exists. So an entry whose inputs
  did not move — and a hash-pinned child, whose literal-hash recipe re-resolves to
  the same parent — short-circuits to a no-op with no alias change. Recomputing
  the whole cone costs only the genuinely-changed entries plus a cheap re-tokenize
  for the rest.

### Stop-and-report on failure

If an entry's replay raises a `BuildError`, the walk stops there. The already-
recomputed prefix stays committed (the one checkpoint still fires because the
remap is non-empty), the failing entry is reported with `action="failed"` and the
error text, and the entries after it are `skipped`. `recalc` never raises on a
build error, so an MCP/companion caller gets a structured report, and the prefix
is recoverable with `reset_to`. This is the plan's default among the three cascade
policies (stop / skip-and-continue / all-or-nothing); the other two are not
implemented.

## The cheap-chain source leak (why a "follower" can be directly stale)

A *cheap* child inlines its parent's recipe rather than reading a baked snapshot.
Building the child therefore re-runs the parent's `from_project`, which records the
parent's source in the **child's** own `manifest.sources`. So editing a raw source
makes every cheap descendant *directly* stale on the source axis, not merely
transitively. This is expected (it falls out of the #74 reconstruction model) and
does not change recalc's behavior — the cone walk recomputes them either way. The
pure transitive/cascade case (a clean descendant carried by a stale ancestor)
shows up across an *expensive* (snapshot-baked) intermediate, whose source does
not leak downstream. See `[[reactive-staleness-edge-semantics]]`.

## Decision gates and the defaults chosen

The ADR left four questions open. The build commits to these defaults; each is
localized so a different choice is a small change.

- **Trigger model** — scan-on-load (a passive flag) plus an explicit recalc.
  `catalog_scan_staleness` / `GET …/api/staleness` is the scan-on-load surface;
  `catalog_recalc` / `POST …/api/recalc` is the explicit action. No
  auto-cascade-on-revise.
- **Cascade failure** — stop-and-report (above).
- **off-mode source axis** — reported `unknown`, never silently `fresh` (Stage 1).
- **Pinned child of an advanced alias** — stays on its pin. A `follow=False`
  recipe re-resolves to the same revision, so its replay is a no-op.

Out of scope here (deletion-side work): archive/delete-cone semantics and
multi-parent unbind (ADR Q3/Q4).

## Surfaces and checkpoint semantics

Both surfaces are thin; all logic is in `recalc.py` / `staleness.py`.

- **MCP.** `catalog_scan_staleness()` returns `{stale, transitively_stale,
  entries}`. `catalog_recalc(roots=None, dry_run=True)` previews by default;
  `roots=None` defaults to the directly-stale set (a scan-driven recalc).
- **Companion.** `GET /{project}/api/staleness` is the scan; `POST
  /{project}/api/recalc` takes `{roots?, dry_run}`. A committed run invalidates the
  companion caches, reloads buckaroo sessions, and publishes a `recalc` SSE event
  so open views refetch.

Checkpointing is arg-aware, which the generic per-op checkpoint hooks (the MCP
dispatch decorator, the companion middleware) cannot be — they key on tool name /
HTTP method, not the `dry_run` flag. So `recalc` takes its own single checkpoint
on a real run that changed something, and both surfaces exempt the recalc path
from their generic checkpoint. A dry run takes none; a real run takes exactly one.

## `result_digest` is not a recalc trigger

An entry that recomputes to the same `content_hash` but a different
`result_digest` is *nondeterministic* (`sample()`, `now()`, an unordered limit, an
impure UDF), not stale — recompute cannot make it fresh. The report surfaces it as
a `noop` with its verdict; the durable fix is the #83/#121 path, not this one.

## Worked examples

**Source edit, cheap chain.** `a = from_project("orders.parquet")`,
`b = from_catalog("a")` (cheap). Edit `orders.parquet`. Scan: both a and b
directly stale (source; b via the cheap-chain leak). `recalc([a])`: cone `[a, b]`;
a replays against the new file to a2 and alias `a` re-points; b replays, reads a2,
and advances to b2 with alias `b` re-pointed. One checkpoint. Both `rebuilt`.

**Alias advance, expensive intermediate.** `a` (root), `b` (expensive, follows
`a`), `c` (cheap, follows `b`). `catalog_revise("a", …)` advances alias `a`. Scan:
b directly stale (alias axis), c only transitively stale. `recalc([b])`: cone
`[b, c]`; b replays against the new a head to b2 (alias `b` re-pointed), then c
replays against b2 to c2 (alias `c` re-pointed).

**Pinned child.** `b = from_catalog("<a-v1-hash>")` (literal hash, `follow=False`).
Advance a. `recalc([a])`: a rebuilds; b's replay re-resolves the literal hash to
the same a-v1 entry, yields the same b hash, and is a `noop` — the pin holds.
