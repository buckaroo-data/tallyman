# The reactive system: revise an alias, recompute its dependents

A tallyman catalog is a closed system. Nothing reads a database, and no source
file changes underneath you without your knowing. The thing that moves is *you*:
you revise an alias — a source projection or an aggregation — to a new recipe,
and now everything built on top of the old version is out of date. The reactive
system is what notices that and recomputes the followers, in dependency order, on
demand.

This doc starts with the two things you actually do — build a small catalog, then
revise an alias and recompute its dependents — and then documents the surfaces
(MCP tools and the companion's HTTP API) and the edges (the staleness axes,
checkpoints, failure handling) underneath.

The short version of how it works today:

- Building or revising an alias **does not** recompute anything downstream. It
  advances that one alias and leaves its followers pointing at their old builds.
- A scan tells you which followers are now stale. A separate, explicit recalc
  recomputes them and re-points their aliases. There is no automatic cascade on
  revise.
- A recalc that rebuilds an entry advances that entry's alias to a **new
  revision** (the alias history is append-only), and takes **one** catalog
  checkpoint for the whole walk.

## Building a simple catalog

Every catalog entry is content-addressed: its `content_hash` is a function of its
recipe and its resolved inputs. An *alias* is a mutable name that points at the
latest hash for a concept and keeps the full history of hashes it has pointed at
(`aliases.jsonl`, one line per alias: `{"alias", "latest", "history": [...]}`).

A recipe is a self-contained Python script that binds a top-level `expr`. It
reads a raw file with `from_project(...)` and reads another catalog entry with
`from_catalog(...)`. Build a three-node chain — a source projection, an
aggregation over it, and a filter over that:

```python
from tallyman_mcp.server import catalog_create

# Node 1 — a source alias: project some columns out of a raw parquet.
catalog_create("orders", '''
from tallyman_xorq.io import from_project
t = from_project("orders.parquet")
expr = t.select("region", "price")
''')
# -> {"hash": <orders_v1>, "alias": "orders", "version": 1, ...}

# Node 2 — an aggregation alias that FOLLOWS orders by name.
catalog_create("by_region", '''
from tallyman_xorq.io import from_catalog
t = from_catalog("orders")
expr = t.group_by("region").agg(total=t.price.sum())
''')
# -> {"hash": <by_region_v1>, "alias": "by_region", "version": 1, ...}

# Node 3 — follows the aggregation.
catalog_create("top_regions", '''
from tallyman_xorq.io import from_catalog
t = from_catalog("by_region")
expr = t.filter(t.total > 1000)
''')
# -> {"hash": <top_regions_v1>, "alias": "top_regions", "version": 1, ...}
```

`catalog_create(name, code)` builds the recipe, persists the entry, and assigns
the alias at **version 1**. It errors if the alias already exists (use
`catalog_revise` for an existing name). There is no `project` argument on the
tool: it operates on the session's active project (set by `project_switch`); the
optional `project=` kwarg on `from_project`/`from_catalog` defaults to that same
active project, so recipes omit it.

The follow relationship is the whole game:

- `from_catalog("orders")` — an alias *name* — records the edge as **follow=True**.
  The recipe means "whatever `orders` is now," and at build time it resolves to
  `orders`'s current head.
- `from_catalog("<hash>")` — a literal hash — records the edge as
  **follow=False**: a pin to that exact revision. A pinned child never goes stale
  when its parent advances, and recalc deliberately leaves it alone.

So after these three calls: `orders → by_region → top_regions`, each aliased at
version 1, each following its parent by name.

## Revising an alias and recomputing its dependents

This is the core workflow. You revise an alias — it does not matter whether it is
a source projection (`orders`) or an aggregation (`by_region`) — and then you
recompute whatever followed it.

```python
from tallyman_mcp.server import catalog_revise, catalog_scan_staleness, catalog_recalc

# Revise the source alias: keep an extra column this time.
catalog_revise("orders", '''
from tallyman_xorq.io import from_project
t = from_project("orders.parquet")
expr = t.select("region", "price", "qty")
''')
# -> {"hash": <orders_v2>, "alias": "orders", "version": 2, ...}
```

`catalog_revise(name, code)` advances `orders` to a new hash and **version 2**.
The previous hash stays in the catalog as a forensic artifact and in
`orders`'s alias history. Nothing downstream has moved: `by_region` and
`top_regions` still point at their version-1 builds, which were computed against
`orders` v1. They are now stale.

Step 1 — scan to see what moved:

```python
catalog_scan_staleness()
# stale:               [<by_region_v1>]   directly stale — its followed parent advanced
# transitively_stale:  [<top_regions_v1>] clean, but sits downstream of a stale ancestor
```

`by_region` followed `orders` by name, and `orders`'s head no longer matches the
hash `by_region` recorded at build, so `by_region` is **directly stale** on the
alias axis. `top_regions` followed `by_region`, whose head has *not* moved yet, so
it is not directly stale — but it is **transitively stale**: a recalc rooted at
the stale set will carry it along. `orders` itself is not stale; it is the fresh
head you just built.

Step 2 — preview the recalc (always a dry run first):

```python
catalog_recalc(dry_run=True)   # roots default to the directly-stale set
# cone:    [<by_region_v1>, <top_regions_v1>]   topological, parent before child
# actions: by_region -> "rebuild", top_regions -> "cascade"
```

`dry_run` defaults to **True** on both surfaces, so a bare `catalog_recalc()`
builds nothing — it returns the ordered cone and each entry's planned action.
Always preview, then commit.

Step 3 — commit:

```python
catalog_recalc(dry_run=False)
# remap:           {<by_region_v1>: <by_region_v2>, <top_regions_v1>: <top_regions_v2>}
# checkpoint_step: one git revision for the whole walk
```

The walk replays `by_region` first. Because dependency order re-points
`by_region`'s alias *before* `top_regions` replays, `top_regions`'s
`from_catalog("by_region")` reads the advanced parent and rebuilds against it.
Both aliases now point at fresh, version-2 hashes; a re-scan is clean.

The same flow drives a revision of an *aggregation* alias. Revise `by_region` and
`top_regions` goes stale and recomputes; `orders` (its parent) is untouched. The
direction is always downstream: revising an alias makes its **followers** stale,
never its parents.

What this flow does **not** do: it does not fire automatically. `catalog_revise`
advances one alias and returns; it never recomputes dependents on your behalf. The
scan is passive. The recalc is the one deliberate step that writes.

## When are new alias revisions created

An alias is an append-only chain of hashes, not a bare pointer. `set_alias` both
moves the head (`alias_map[name] = hash`) and appends to the history
(`alias_history[name].append(hash)`), and the returned `version` is the length of
that history. So a new alias revision is created exactly when an alias head
advances to a hash it was not already pointing at:

- **On create** — `catalog_create("orders", ...)` writes version 1.
- **On revise** — `catalog_revise("orders", ...)` appends version 2 for the one
  alias you edited.
- **On recalc** — each dependent that **rebuilds to a new hash** has its alias
  advanced to a new revision. In the example above, the single committed recalc
  appended `by_region` v2 and `top_regions` v2. This is the "new alias revisions
  on each recalc" behavior: every entry whose content actually changed gets a new
  version of its alias.

Three edges make that precise:

- A **`noop`** entry — one that replays to the *same* hash because its inputs
  didn't really move — gets no new revision. Its alias is left untouched (and
  `set_alias` dedupes a consecutive duplicate hash regardless).
- The revision is appended **per advanced alias head, not per rebuilt entry**. A
  rebuilt entry that has no alias on its head — an unnamed scratch entry, or an
  intermediate node referenced only by a hash pin — produces a new hash and a
  `remap` entry but **zero** alias revisions. A head carrying two aliases advances
  both.
- A **hash-pinned** child (`from_catalog("<hash>")`) re-resolves to the same
  parent and is a `noop`, so it neither rebuilds nor advances its alias.

This per-alias revision is distinct from the catalog-level checkpoint. The alias
head advancing is bookkeeping inside `aliases.jsonl`; the checkpoint is the single
git revision that commits the whole walk (see *One revision, undoable atomically*
below). One recalc can append many alias revisions and still take exactly one
checkpoint.

## The two staleness axes

The workflow above drives the **alias axis**. There is a second axis for source
files that change on disk, which a closed notebook catalog mostly doesn't hit, but
it exists and is worth knowing.

An entry is judged on two independent axes, each tied to a kind of recorded input:

- **alias** — a `follow=True` parent (recorded when the recipe referenced a parent
  by alias name, `from_catalog("orders")`) is stale when that alias now resolves to
  a different hash than the one recorded at build. This is what fires when you
  revise an upstream alias. A `follow=False` parent (a literal-hash pin) is never
  stale on this axis: the recipe asked for *that* revision and still gets it.
- **source** — a recorded `(rel_path, digest)` is stale when the file on disk no
  longer digests to the recorded value. The check forces a faithful re-read so an
  in-place content swap can't be masked by a cached digest. In a closed catalog
  you don't expect this to fire; it covers the case where a raw parquet under
  `data/` is replaced.

A note on cheap chains: a *cheap* child inlines its parent's recipe rather than
referencing it through the catalog, so the parent's source files are collected
into the child's own `manifest.sources`. Editing that source makes the child
*directly* stale on the source axis, not merely transitively stale. This is
expected; it falls out of how cheap entries record their inputs.

`result_digest` is deliberately not a staleness input. An entry that recomputes to
the same `content_hash` but a different result is *nondeterministic*, not stale,
and recompute can't make it fresh. That is the #83/#121 concern, separate from
this system.

### Prerequisite: `cas` source identity (source axis only)

Source-axis staleness only works under content-addressed source identity, which is
the default. `source_identity.mode()` reads `TALLYMAN_SOURCE_IDENTITY` and falls
back to `"cas"` (`source_identity.py:59`). In `cas` mode, `from_project` reads each
source through a copy-on-write clone at `data/.cas/<digest><suffix>`, so the path
xorq hashes is the content identity: editing a source in place yields a different
digest, the manifest records that digest at build time, and a later scan can tell
the file moved. The clone also means an old entry's recompute still reads the bytes
it was built from after you edit the source.

Under the legacy `off` mode the manifest records no source digests, so the source
axis cannot be evaluated. A scan reports it as `unknown` rather than silently
fresh. If you have a corpus built under `off`, rebuild it — there is no migration
path and none is wanted (single-user repo). The **alias axis is unaffected** by
this mode; it works regardless.

## Detecting staleness (the scan)

The scan classifies every live entry and tells you two things per entry: whether it
is **directly stale** (its own recorded inputs moved) and whether it is
**transitively stale** (a clean entry sitting downstream of a directly stale
ancestor, so a recalc would carry it along).

### MCP

```
catalog_scan_staleness() -> dict
```

Read-only, takes no checkpoint. It returns:

```json
{
  "stale": ["<hash>", ...],               // sorted, directly stale — the natural recalc roots
  "transitively_stale": ["<hash>", ...],  // sorted, clean-but-carried
  "entries": {
    "<hash>": {
      "content_hash": "<hash>",
      "stale": true,
      "reasons": [{"axis": "alias", "ref": "orders", "was": "<orders_v1>", "now": "<orders_v2>"}],
      "unknown_axes": [],
      "transitively_stale": false
    },
    ...
  }
}
```

Each `reason` names the axis, the `ref` (alias name or source rel_path), the value
`was` at build time, and the value `now` (or `null` if the input is gone). An axis
that can't be evaluated — alias deleted, source file removed, or the entry was
built under `off` mode — lands in `unknown_axes` and never forces `stale: true`.

### HTTP

```
GET /{project}/api/staleness
```

Read-only, same data shape plus the project name:

```json
{ "project": "...", "stale": [...], "transitively_stale": [...], "entries": {...} }
```

This is the scan-on-load surface. The SPA is expected to fire it on project load
and after a recalc to drive a per-entry badge (the badge UI itself is not built yet
— see Current limits).

## Recomputing (the recalc)

Recalc takes a set of *roots*, computes their descendant cone (every entry
reachable downstream, topologically ordered parents-before-children), and replays
each recipe in that order through `build_and_persist` — the same primitive
`catalog_revise` uses. A replay reads the *current* alias heads, so once a parent
has been re-pointed to its fresh hash, its dependents replay against the advanced
parent. An entry whose inputs didn't actually move replays to the same
`content_hash` and short-circuits to a no-op.

### Preview, then commit

`dry_run` defaults to **True**. A dry run builds nothing; it returns the ordered
cone with each entry's planned action so you can see what a commit would do. Always
preview first.

Dry-run actions:

- `rebuild` — directly stale, its own inputs moved.
- `cascade` — clean, but a followed parent inside the cone will advance.
- `unchanged` — pinned to an out-of-cone parent, or a root that isn't stale.

A real run (`dry_run=False`) replays for real and reports:

- `rebuilt` — replayed to a new hash; its followed alias was re-pointed (a new
  alias revision).
- `noop` — replayed to the same hash; nothing changed, alias untouched.
- `failed` — the build raised; the walk stopped here.
- `skipped` — a later cone entry the walk never reached because it stopped.

### MCP

```
catalog_recalc(roots: list[str] | None = None, dry_run: bool = True) -> dict
```

Omit `roots` for a scan-driven recalc: it defaults to every directly-stale entry.
Pass explicit hashes to recompute a specific subtree and its dependents. Remember
`dry_run` is **True** by default — pass `dry_run=False` to commit.

The return is the full report:

```json
{
  "project": "...",
  "roots": ["<hash>", ...],
  "cone": ["<hash>", ...],          // topological, parents before children
  "entries": [
    {"content_hash": "<old>", "action": "rebuilt", "stale": true,
     "transitively_stale": false, "new_hash": "<new>", "alias": "by_region",
     "reasons": [...], "error": null},
    ...
  ],
  "dry_run": false,
  "status": "ok",                   // "ok" | "failed" | "cycle"
  "remap": {"<old>": "<new>", ...}, // changed entries only
  "checkpoint_step": 42,            // the revision the walk committed, or null
  "error": null                     // set only when status == "cycle"
}
```

When nothing is stale, the roots default to empty and you get a clean
`status: "ok"` report with an empty cone and a `"note": "nothing stale to
recompute"` — no revision is taken.

### HTTP

```
POST /{project}/api/recalc
Content-Type: application/json
{ "dry_run": true, "roots": ["<hash>", ...] }
```

Both body fields are optional; `dry_run` defaults to True and `roots` defaults to
the stale set, matching the MCP tool. The response is the same report (without the
`note` field on the empty case). A committed run additionally invalidates the
companion's result/compare caches, reloads any live buckaroo sessions, and
publishes a `recalc` SSE event. The route is blocked with a 403 in read-only
(serve) mode.

### One revision, undoable atomically

A real recalc takes exactly **one** checkpoint for the whole walk, and only when
something actually changed. Both surfaces exempt `catalog_recalc` / `/api/recalc`
from their generic per-operation checkpoint, so the recalc is the sole writer and
the entire walk is a single git revision. The per-alias re-points happen inside the
walk and only rewrite `aliases.jsonl`; they don't commit. `reset_to` the step
before the recalc puts every alias head — and every other tracked file — back where
it was, rolling back the whole cascade at once.

### Failure and cycles

The cascade-failure policy is **stop-and-report**, the plan's default. If a replay
raises, the walk stops at that entry: the already-recomputed prefix stays committed
(so its checkpoint still records), the failing entry is reported with
`action: "failed"` and the error text, and everything after it is `skipped`. The
call does not raise — a caller gets a structured `status: "failed"` report, not a
500. The two other policies in the ADR (skip-and-continue, all-or-nothing) are not
implemented.

If the dependency graph contains a cycle, recalc detects it while building the cone,
before any recipe runs, and returns `status: "cycle"` with the cycle described in
`error` and an empty `entries` list.

A root that names a directory without a `manifest.json` (a crashed mid-build
leftover, or a caller typo) is dropped before the walk — it isn't a real entry, so
it can't enter the cone. An all-bogus root set yields a clean empty report.

## Cross-process notification and SSE

The MCP server and the companion are separate processes sharing the on-disk
catalog. When a recalc commits through the **MCP** tool, the server's process has
changed the catalog but the companion's process still holds warm caches and live
buckaroo sessions. The tool closes that gap by POSTing to the companion's
`/internal/notify` with `kind="recalc"` and an `extra` carrying the `remap` and
checkpoint `step`. The companion handler then does the same cleanup the in-process
route does: clears its result/compare LRUs, reloads buckaroo sessions, and
republishes the event to SSE subscribers. The notify is best-effort — if the
companion isn't running, the MCP call still succeeds.

Both the in-process route and the cross-process notify funnel the event through one
helper, `_recalc_sse_event`, so subscribers always see the same canonical shape:

```json
{ "kind": "recalc", "remap": {"<old>": "<new>", ...}, "step": 42, "project": "..." }
```

The SSE endpoint (`GET /{project}/api/sse`) sets the stream's `event:` field to the
event's `kind`. A client must therefore register a **named** listener,
`addEventListener("recalc", …)` — a generic `onmessage` handler will not see it.

## Current limits

What works today is the backend, end to end, through both the MCP and HTTP
surfaces: scan, preview, commit, atomic revision, cross-process invalidation, and
the published SSE event. What is not yet built:

- **Frontend rendering.** The SPA's `SSEContext` listens for nine named events
  (`hello`, `ping`, `new_entry`, `build_failed`, `notebook_changed`,
  `chart_attached`, `post_processing_changed`, `summary_stat_changed`,
  `project_switched`) but **not** `recalc` — the backend emits it, nothing consumes
  it, so open views do not auto-refresh after a recalc yet. There is also no
  staleness badge and no recalc affordance in the UI, and nothing calls
  `/api/staleness` or `/api/recalc` from the frontend. For now, drive recalc through
  MCP or a direct HTTP call and refresh the view manually.
- **Deletion-side semantics.** Archive / delete-cone behavior and multi-parent
  unbind (ADR Q3/Q4) are out of scope for this system as built.
- **Alternative cascade policies.** Only stop-and-report exists.

## Trigger model

The system is scan-on-load plus explicit recalc. The scan surfaces
(`catalog_scan_staleness` / `GET …/api/staleness`) are passive — they flag
staleness but never act. Recalc (`catalog_recalc` / `POST …/api/recalc`) is the
explicit action and is always the one that writes. There is no automatic cascade on
revise: advancing an alias marks its followers stale, but recomputing them is a
deliberate step you take.
