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

- Building an alias does not recompute anything downstream; it advances that one
  alias. *Revising* an alias, by default, recomputes its stale followers in the
  same atomic checkpoint (auto-recalc — see *Trigger model*). The switch is
  per-project and env-overridable; turn it off for the explicit path below.
- A scan tells you which followers are stale. With auto-recalc off, a separate
  explicit recalc recomputes them and re-points their aliases — and that explicit
  recalc is still the path for source-file drift, which the revise trigger doesn't
  cover.
- A recalc that rebuilds an entry advances that entry's alias to a **new
  revision** (the alias history is append-only), and takes **one** catalog
  checkpoint for the whole walk.

## Building a simple catalog

Every catalog entry is content-addressed: its `content_hash` is a function of its
recipe and its resolved inputs. An *alias* is a mutable name that points at the
latest hash for a concept and keeps the full history of hashes it has pointed at
(`aliases.jsonl`, one line per alias: `{"alias", "latest", "history": [...]}`).

A recipe is a self-contained Python script that binds a top-level `expr`. It
reads a raw file with `read_project_file(...)` and reads another catalog entry with
`tracked_expr_from_alias(...)`. Build a three-node chain — a source projection, an
aggregation over it, and a filter over that:

```python
from tallyman_mcp.server import catalog_create

# Node 1 — a source alias: project some columns out of a raw parquet.
catalog_create("orders", '''
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet")
expr = t.select("region", "price")
''')
# -> {"hash": <orders_v1>, "alias": "orders", "version": 1, ...}

# Node 2 — an aggregation alias that FOLLOWS orders by name.
catalog_create("by_region", '''
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders")
expr = t.group_by("region").agg(total=t.price.sum())
''')
# -> {"hash": <by_region_v1>, "alias": "by_region", "version": 1, ...}

# Node 3 — follows the aggregation.
catalog_create("top_regions", '''
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("by_region")
expr = t.filter(t.total > 1000)
''')
# -> {"hash": <top_regions_v1>, "alias": "top_regions", "version": 1, ...}
```

`catalog_create(name, code)` builds the recipe, persists the entry, and assigns
the alias at **version 1**. It errors if the alias already exists (use
`catalog_revise` for an existing name). There is no `project` argument on the
tool: it operates on the session's active project (set by `project_switch`); the
optional `project=` kwarg on `read_project_file`/`tracked_expr_from_alias` defaults to that same
active project, so recipes omit it.

A recipe pulls in data through one of three functions in `tallyman_xorq.io`, and
which one you call is what records the dependency edge:

| Function | Reads | Records | Goes stale when |
|---|---|---|---|
| `read_project_file("orders.parquet")` | a raw file under `<project>/data/` | a **source** leaf (`rel_path → digest`) | the file's bytes change on disk |
| `tracked_expr_from_alias("orders")` | a catalog entry, by **alias** | a **parent** edge, `follow=True` | the alias advances to a new head |
| `pinned_expr_from_alias(...)` | a catalog entry, by alias **or** hash | a **parent** edge, `follow=False` | never (it pinned that exact revision) |

The follow relationship is the whole game:

- `tracked_expr_from_alias("orders")` means "whatever `orders` is now." It accepts
  an alias only — pass it a hash and it raises, because a hash has no head to
  follow — resolves to the alias's current head at build time, and records
  **follow=True**. The child goes stale and recomputes when `orders` advances.
  This is normal chaining.
- `pinned_expr_from_alias("orders")` or `pinned_expr_from_alias("<hash>")` is the
  deliberate opt-out. It accepts an alias or a hash, records **follow=False**, and
  the child stays on that exact revision: it never goes stale on the alias axis,
  and recalc finds it but leaves it alone.
- `read_project_file` is the root of every chain: a raw file with no catalog
  identity, the leaf the graph bottoms out in.

So after these three calls: `orders → by_region → top_regions`, each aliased at
version 1, each following its parent by name (`tracked_expr_from_alias`).

## Revising an alias and recomputing its dependents

This is the core workflow. You revise an alias — it does not matter whether it is
a source projection (`orders`) or an aggregation (`by_region`) — and then you
recompute whatever followed it.

```python
from tallyman_mcp.server import catalog_revise, catalog_scan_staleness, catalog_recalc

# Revise the source alias: keep an extra column this time.
catalog_revise("orders", '''
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet")
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
`tracked_expr_from_alias("by_region")` reads the advanced parent and rebuilds against it.
Both aliases now point at fresh, version-2 hashes; a re-scan is clean.

The same flow drives a revision of an *aggregation* alias. Revise `by_region` and
`top_regions` goes stale and recomputes; `orders` (its parent) is untouched. The
direction is always downstream: revising an alias makes its **followers** stale,
never its parents.

This three-step flow (revise, scan, recalc) is the explicit path — the one that
runs when auto-recalc is **off**. By default it is **on**, and `catalog_revise`
folds the recalc into itself: it advances the alias *and* recomputes its stale
followers in the same atomic checkpoint, so the scan-and-recalc dance above
collapses to a single `catalog_revise` call. See *Trigger model* for the switch
and what the auto path reports. The scan stays passive either way; it never
writes.

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
- A **hash-pinned** child (`pinned_expr_from_alias("<hash>")`) re-resolves to the same
  parent and is a `noop`, so it neither rebuilds nor advances its alias.

This per-alias revision is distinct from the catalog-level checkpoint. The alias
head advancing is bookkeeping inside `aliases.jsonl`; the checkpoint is the single
git revision that commits the whole walk (see *One revision, undoable atomically*
below). One recalc can append many alias revisions and still take exactly one
checkpoint.

## How the dependency graph is recorded and read

Staleness, the cone, and the cascade all run on a dependency graph the reactive
system never introspects live. The graph is **recorded into each entry's manifest
at build time, then read back by scanning those manifests**. There is no separate
graph database and no global index; the adjacency is rebuilt from the manifests on
each query (cheap for a notebook-sized catalog — one small JSON read per entry).

### Where the edges live

Each entry has a `manifest.json` with two fields that carry the graph:

- **`manifest.parents`** — the resolved cross-entry edges, a list of
  `{hash, ref, follow}`. `hash` is the parent's build-time content hash, `ref` the
  original argument (alias or hash), `follow` its read-intent. Empty for a root (an
  entry that reads no other entry).
- **`manifest.sources`** — the raw-file leaves, `{rel_path: digest}`: the project
  files the recipe read via `read_project_file`, each with the content digest it
  was built against. `None` when the entry was built under `off` identity mode (so
  the source axis can't be evaluated); `{}` when the recipe read no raw files.

`tracked_`/`pinned_expr_from_alias` write `parents`; `read_project_file` writes
`sources`. Together they are the only record of the graph.

### Roots, leaves, and direction

This doc uses the **data-flow** convention (as in Airflow, dbt, Spark lineage):
edges point downstream, the direction data moves.

- A **source / root** has no incoming edges — nothing it depends on: a
  `read_project_file` raw input, or an entry with empty `manifest.parents`.
- A **leaf / sink** has no outgoing edges — nothing depends on it: a final
  aggregation nobody chains off.

A recalc starts at the changed roots and flows down to the leaves. (Build-system
tools — Make, Bazel — point the arrows the other way and swap these names, which is
the usual source of confusion.)

### Cheap vs expensive parents — and what lands in `manifest.sources`

When a recipe reads a parent with `tracked_expr_from_alias`, what comes back
depends on whether the parent was classified **expensive** or **cheap** at build.
This is the materialized-vs-not distinction:

- An **expensive** parent (an Aggregate, Join, Sort, or UDF) bakes a result
  snapshot when built (materialized). A child reading it gets a *read of that baked
  snapshot* — the parent's work is computed once and shared, and the child does not
  re-run it.
- A **cheap** parent (row-wise: select, filter, mutate) bakes nothing
  (non-materialized). A child reading it gets the parent's *recipe re-run inline* —
  pushdown makes that ~free — so the parent's expression, down to its own
  `read_project_file` reads, is composed into the child.

That second case is why a cheap parent's sources show up in the **child's**
`manifest.sources`: the inlined recipe re-runs the parent's `read_project_file`
calls during the child's build, and those reads get collected into the child's own
source set. So editing that raw file makes the child **directly** stale on the
source axis, not merely transitively stale. A child reading an *expensive* parent
reads the snapshot instead, so the parent's raw sources never enter the child's
sources — there the source edit makes the *parent* directly stale, and the child
follows only when the parent's alias advances.

Which work is materialized is otherwise an implementation detail you don't see:
`tracked_expr_from_alias` returns an expression either way, and the child never
depends on the parent having a `result.parquet` on disk.

### Build-time capture, not live introspection

The edges are captured the moment a recipe runs, not by walking the built
expression afterward. While `build_and_persist` imports the recipe, two collectors
are armed, and each loader announces itself as it executes: `read_project_file`
notes the source digest, `tracked_`/`pinned_expr_from_alias` notes the resolved
parent hash. After the import those bags are written into the manifest. The capture
happens once, at build; nothing re-derives it later.

This is a **tallyman** mechanism, not an xorq one. xorq's unit is a single
expression and its content hash; it has no concept of a catalog, an alias, an
entry, or an inter-entry edge — those are all tallyman's. xorq builds one
expression and hashes it; tallyman wraps that build, harvests the edges its own
loaders announced, and writes the manifest beside xorq's artifact.

The capture is a side-channel because the parent edge is **not recoverable from
xorq's output**. Since the expression-composition change (#73/#74),
`tracked_expr_from_alias` composes the parent's *expression* into the child (cheap:
the inlined recipe; expensive: a read of the baked snapshot) rather than reading
the parent's `result.parquet` by a path that named it. The result is one flattened
graph with no node that says "this subtree was entry `<parent_hash>`." Before the
change a child read `entries/<parent_hash>/result.parquet` — a leaf path naming the
parent — and the edge was readable straight out of the expression. After it, the
only place the edge survives is the manifest tallyman wrote.

### Reading it back: one seam

All graph reads go through `tallyman_xorq.dependents`, which scans the manifests
and assembles the views the reactive system needs:

- `parents_of(hash)` / `sources_of(hash)` — one entry's recorded inputs.
- `build_dag()` — forward edges for every live entry, `{child: [parents]}`.
- `dependents_index()` — the reverse index, `{parent: {children}}`: how a recalc
  goes from a changed entry to everything downstream.
- `descendant_cone(roots)` — the roots and all their transitive dependents,
  topologically ordered (parents before children) — the recalc order.

Both `staleness` and `recalc` go through this module rather than touching manifest
fields directly, so it is the single seam over the recorded graph: swapping the
implementation (a persistent index, say) is a change to `dependents` alone.

A consequence worth noting (and a candidate future check): because the parent edge
is gone from xorq's flattened expression, you can't reconcile tallyman's recorded
parents against xorq's graph. What you *can* reconcile is the source leaves — a
child's inlined raw-file reads should equal the union of `sources` over its
transitive cheap parents. That invariant is checkable; the parent edges are only as
good as what the side-channel captured at build.

## The two staleness axes

The workflow above drives the **alias axis**. There is a second axis for source
files that change on disk, which a closed notebook catalog mostly doesn't hit, but
it exists and is worth knowing.

An entry is judged on two independent axes, each tied to a kind of recorded input:

- **alias** — a `follow=True` parent (recorded when the recipe referenced a parent
  by alias name, `tracked_expr_from_alias("orders")`) is stale when that alias now resolves to
  a different hash than the one recorded at build. This is what fires when you
  revise an upstream alias. A `follow=False` parent (a `pinned_expr_from_alias`
  reference, by alias or hash) is never stale on this axis: the recipe asked for
  *that* revision and still gets it.
- **source** — a recorded `(rel_path, digest)` is stale when the file on disk no
  longer digests to the recorded value. The check forces a faithful re-read so an
  in-place content swap can't be masked by a cached digest. In a closed catalog
  you don't expect this to fire; it covers the case where a raw parquet under
  `data/` is replaced.

A note on cheap chains: when a child reads a *cheap* (non-materialized) parent,
that parent's recipe is inlined into the child, so the parent's source files land
in the child's own `manifest.sources` (see *Cheap vs expensive parents* above).
Editing such a source makes the child **directly** stale on the source axis, not
merely transitively stale. This is expected.

`result_digest` is deliberately not a staleness input. An entry that recomputes to
the same `content_hash` but a different result is *nondeterministic*, not stale,
and recompute can't make it fresh. That is the #83/#121 concern, separate from
this system.

### Prerequisite: `cas` source identity (source axis only)

Source-axis staleness only works under content-addressed source identity, which is
the default. `source_identity.mode()` reads `TALLYMAN_SOURCE_IDENTITY` and falls
back to `"cas"` (`source_identity.py:59`). In `cas` mode, `read_project_file` reads each
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
  it, so open views do not auto-refresh after a recalc yet. Auto-recalc-on-revise
  does not change this: it emits the same `recalc` event, which the SPA still
  ignores, so a view open on a cascaded dependent needs a manual refresh. Wiring the
  listener is the one piece that makes the cascade visible in the viewer. There is
  also no staleness badge and no recalc affordance in the UI, and nothing calls
  `/api/staleness` or `/api/recalc` from the frontend. For now, drive recalc through
  MCP or a direct HTTP call and refresh the view manually.
- **Deletion-side semantics.** Archive / delete-cone behavior and multi-parent
  unbind (ADR Q3/Q4) are out of scope for this system as built.
- **Alternative cascade policies.** Only stop-and-report exists.

## Trigger model

There are two triggers: auto-recalc on revise (the default), and the explicit
scan-then-recalc path. The scan surfaces (`catalog_scan_staleness` /
`GET …/api/staleness`) are passive under both — they flag staleness but never act.

### Auto-recalc on revise (default)

Revising an alias recomputes its stale followers in the **same** checkpoint as the
head advance. Every surface that re-points an *existing* alias triggers it —
`catalog_revise`, the companion `PUT …/api/code/{alias}`, and `catalog_promote_diff`
when it re-points an existing target. The cascade is scoped to *this* revise's
followers: roots are the entries the advance made directly stale on the alias axis,
and the descendant cone expands from there. Pre-existing staleness from any other
cause is left untouched.

The whole thing is one git revision. The head advance, the rebuilt dependents, and
their re-pointed aliases are swept into the surface's single per-operation
checkpoint, so `reset_to` the step before the revise undoes the revise *and* every
dependent it recomputed, atomically. A failing dependent is stop-and-report (the
same policy as explicit recalc): the revise still lands, the failure is reported,
the rest are skipped.

The revise reply carries a `recalc` sub-report — `catalog_recalc`'s shape (`cone`,
`entries`, `remap`, `status`) plus `orphan_stale`. `orphan_stale` lists every entry
that was directly stale but *not* a follower of this revise (a drifted source, a
different alias not yet recalced, or an invariant break): not recomputed, logged at
WARNING, and classified against the durable error store — "explained by error
`<id>`" when a recorded recalc/build failure carries that hash, else "UNEXPLAINED
… file a bug." Cascade failures are persisted to `errors.jsonl` (keyed by the
failed hash) so the tie-back works across calls; that log lives outside the catalog
repo, so it survives `reset_to`.

The switch resolves env → config → default: `TALLYMAN_AUTO_RECALC` (a recognized
boolean) wins, else the `auto_recalc` key in the catalog repo's tracked
`config.json`, else the built-in default **ON**. Turn it off to get the explicit
path below.

### Explicit recalc

With the switch off, advancing an alias marks its followers stale but recomputes
nothing. `catalog_recalc` / `POST …/api/recalc` is then the deliberate action that
writes — preview with a dry run, commit with `dry_run=False`. This is also the path
for staleness the auto trigger never sees: a drifted source file, or any alias
left stale because auto-recalc was off when it was revised.
