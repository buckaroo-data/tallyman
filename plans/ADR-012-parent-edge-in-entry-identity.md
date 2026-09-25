# ADR: A parent edge is part of the entry's identity

- **Status:** Proposed (2026-09-24).
- **Context:** `tracked_expr_from_alias("agg")` and `pinned_expr_from_alias("agg-v1")`
  return the same expression when `agg`'s head is v1, and the choice between
  following and pinning reaches only `manifest.parents`, which xorq's content
  hash does not cover. Two recipes that differ only in that choice share one
  entry, and the first build's edge wins. Probed on
  `feat/adr-007-009-cache-redesign` at `1f8cb02`, with xorq 0.3.26.
- **Tickets:** #229. The recommendation also takes in the fix for #228 (a
  recipe can read a snapshot by its path and record no parent edge).
- **Related:** `plans/ADR-007-tallyman-owned-materialization.md`, for ADR-007
  D2 (a snapshot's location is a function of the content hash, and its one bare
  read is memoized so a snapshot has one table name) and ADR-007 D3 (chaining
  through a worthy parent is a bare read of its snapshot, so a child's identity
  is a function of its parent's). `plans/ADR-008-row-order-of-reads.md`, for
  ADR-008 D4 (cheap means row-preserving over one file; everything else is
  materialized). `plans/ADR-011-sources-are-aliases.md`, for ADR-011 D5 (a bare
  content hash is refused in a recipe) and ADR-011 D6 (staleness has one axis, a
  followed alias that moved).

## Terms

Project terms, as `docs/architecture.md` uses them:

- **Entry:** one dataframe result, built once and stored under
  `entries/<content hash>/`. Its **content hash** is xorq's 12-character hash
  of the expression it was built from (for a source entry, an md5 of its
  imported bytes instead).
- **Recipe:** the Python an author writes to build an entry, kept as the
  entry's `expr.py`. It reads other entries by alias.
- **Alias:** a mutable name for a line of entries. Its **head** is the entry it
  points at now, and `<alias>-v<N>` names its Nth version.
- **Worthy and cheap:** a worthy entry is run once and its result written to a
  **snapshot**, `compute_cache/result_cache/<hash>.parquet`; a cheap entry (one
  that only filters or projects one file) writes nothing and re-runs its plan
  on every read.
- **The collector:** the context variable in `parent_capture.py` that the
  readers append edges to while a recipe runs, and that `_build_and_persist`
  copies into the manifest.
- **Heal:** re-creating a missing snapshot and checking it against the recorded
  digest.
- **Auto-recalc:** the recalc a revise runs on the entries that follow the
  revised alias.

Terms this ADR uses for the problem:

- **Parent edge:** one `{hash, ref, follow}` record in an entry's
  `manifest.parents`, naming an entry its recipe read. A **followed edge**
  (`follow: true`) comes from `tracked_expr_from_alias("agg")`, and the child
  is **stale** once `agg`'s head differs from the hash the edge recorded; a
  recalc then rebuilds it. A **pinned edge** (`follow: false`) comes from
  `pinned_expr_from_alias("agg-v1")` and never makes the child stale. The
  **staleness scan** (`catalog_scan_staleness`) reports stale entries, and
  lists a stale entry that no recorded recalc error explains as
  "UNEXPLAINED".
- **Recorded edges:** the parent edges an entry's manifest holds.
- **The early return:** the branch of `_build_and_persist` (`build.py:509-528`)
  taken when the content hash a build computes already names an entry with a
  manifest. It appends the prompt to the prompt log and returns that entry,
  writing nothing else.
- **Collision:** two recipes whose recorded edges would differ compile to one
  content hash, so the second build takes the early return and keeps the first
  build's `expr.py` and recorded edges.
- **Merged entry:** the entry a collision leaves behind, pointed at by aliases
  whose recipes asked for different edges.
- **Replay:** a recalc re-running an entry's `expr.py` through
  `build_and_persist` (`recalc.py:250`).
- **Reconstruction:** `result_cache._recipe_expr` re-importing an entry's
  `expr.py` with `_RECONSTRUCTING` set, which the structural-nondeterminism
  diagnostic uses to hash a recipe twice (`result_cache.py:315-371`).

Two more terms, **edge tag** and **derived edges**, are defined under option 1b,
where they first come up.

## Problem

Both readers end the same way. `tracked_expr_from_alias("agg")` resolves the
alias to its head, calls `parent_capture.note_parent(hash, ref="agg",
follow=True)` and returns `cached_result_expr(proj, hash)` (`io.py:527-531`).
`pinned_expr_from_alias("agg-v1")` resolves the version, notes `follow=False`
and returns the same call (`io.py:598-602`). For a worthy parent that call is
one memoized `deferred_read_parquet` of its snapshot (`result_cache.py:530-542`);
for a cheap parent it is the parent's loaded graph. When `agg`'s head is v1 the
two readers return the same expression.

xorq cannot tell them apart. `build_expr` names a build by `get_expr_hash`
(`xorq/common/utils/provenance_utils.py:18`), and the hasher reduces a `Read`
to its schema, its path string and three read options (`mode`, `schema`,
`temporary`) in `snapshot_normalize_read` (`xorq/caching/strategy.py:49`). The
edge travels beside the expression, in the `parent_capture` context variable,
and lands only in the manifest (`build.py:448-452`, `build.py:639`).

Everything downstream is keyed on that hash. When the entry already has a
manifest the build takes the early return, so every alias that points at the
entry shares its one `expr.py` and one list of recorded edges. A recalc replays
that `expr.py` and moves every alias heading the entry (`recalc.py:248-271`).

Probe, the shape the issue gives:

```python
catalog_create("agg", AGG)  # group_by over orders_src, a worthy entry
catalog_create("t_child", 'from tallyman_xorq.io import tracked_expr_from_alias\nt = tracked_expr_from_alias("agg")\nexpr = t.mutate(d=t.total * 2)\n')
catalog_create("p_child", 'from tallyman_xorq.io import pinned_expr_from_alias\nt = pinned_expr_from_alias("agg-v1")\nexpr = t.mutate(d=t.total * 2)\n')
catalog_revise("agg", AGG2)  # a filter before the group_by
```

- `t_child` and `p_child` get one hash. That entry records
  `[{ref: "agg", follow: true}]`, and its `expr.py` is `t_child`'s recipe.
- The revise's auto-recalc rebuilds the entry and moves both aliases.
  `p_child`'s history becomes `[v1, v2]`, and its v2 is computed from `agg`'s
  v2.
- Built in the other order, `t_child` records `{ref: "agg-v1", follow: false}`.
  The revise leaves it where it was, and the staleness scan does not call it
  stale.

The same cause shows up in three more shapes, each probed at `1f8cb02`:

1. **A recalc can merge two entries by itself.** `p_child` pins `agg-v1`. `agg`
   is revised to v2, and `t_child` is created afterwards, reading v2, so the two
   children have different hashes. `agg` is then revised back to v1's recipe,
   which lands on v1's hash. The auto-recalc replays `t_child`, which now reads
   v1's snapshot, compiles to `p_child`'s hash and takes the early return.
   `t_child`'s alias moves to `p_child`'s entry, and its recorded edge becomes
   `{ref: "agg-v1", follow: false}`. When `agg` is revised to v2 again,
   `t_child` does not move and is not stale. Each alias had one recipe, and
   nobody wrote a colliding one.
2. **Two aliases on one entry.** `catalog_create("agg_copy", AGG)` lands on
   `agg`'s entry. A tracked child of `agg` and a tracked child of `agg_copy`
   with the same query share one entry, which records `ref: "agg"`. Revising
   `agg` moves the `agg_copy` child as well, though `agg_copy` did not move.
3. **An unused read.** A recipe that calls `tracked_expr_from_alias("other")`
   and never uses the result records an edge to `other`, but its expression has
   no read of `other`, so the hash does not depend on it. After `other` is
   revised the entry is stale, its replay compiles to the same hash and takes
   the early return with the old edge, and the entry stays stale for good. The
   staleness scan lists it as "UNEXPLAINED stale entry", the symptom
   ADR-011's Problem section was written about.

The **by-hash forms**, recipes that read an entry by its content hash instead
of through a reader, collide too. A recipe that reads `agg`'s v1 snapshot with
`xo.deferred_read_parquet(path)` (#228), or calls `cached_result_expr(project,
hash)` itself, compiles to the same hash as `t_child` and `p_child`, since a
table name is not part of the hash. Built after `t_child`, it inherits
`t_child`'s followed edge.

ADR-011 D5 (a bare content hash is refused in a recipe) makes every parent edge
name an alias, followed or pinned. That holds for recipe text. It does not hold
for entries, because an entry records the edge of whichever recipe reached its
hash first.

Underneath, an entry directory holds two kinds of fact. The build, schema,
snapshot and digest follow from the frozen expression, and the content hash
names them. The recipe text and the recorded edges say what the author asked
for, and the hash covers neither. A collision is two different requests that
freeze to one expression.

## Options

Every option below was probed by patching it into a test at `1f8cb02` and
running the shapes above under it. The results are summarized at the end of
this section.

### Option 1: the readers put the edge into the expression

The readers are the one place that knows the edge, and they build the
expression the hash covers.

#### 1a. A table name that encodes the edge

`deferred_read_parquet(path, table_name=...)` takes a name. Three reads of one
snapshot, with xorq's automatic name, `edge_agg_follow` and `edge_agg_v1_pin`,
build to one hash, and so do children over them. `snapshot_normalize_read`
leaves the name out, and `_sanitize_generated_names`
(`xorq/ibis_yaml/compiler.py:235`) only replaces automatic names with
content-based ones.

A name per edge would also undo the memo of ADR-007 D2 (a snapshot's location
is a function of the content hash, and its one bare read is memoized): one read
per snapshot becomes one read per snapshot, ref and follow flag, each
registering its own table in the shared backend. Rejected, because it does not
change the hash.

#### 1b. An edge tag

xorq has two tag nodes: relations that wrap an expression and carry a metadata
dict (`xorq/expr/relations.py:101-134`). xorq removes both before it compiles
SQL or executes (`xorq/expr/api.py:208`, `xorq/expr/api.py:413`). A plain `Tag`
is left out of the build hash; a `HashingTag` is hashed with its metadata
(`xorq/common/utils/dasher/_opaque.py:201`). An **edge tag** is a `HashingTag`
named `tallyman.edge` whose metadata is the edge, which the reader wraps around
what `cached_result_expr` returns:

```python
return cached_result_expr(proj, content_hash).hashing_tag(
    "tallyman.edge", hash=content_hash, ref=alias, follow=True
)
```

Probed by patching both readers to do this, with the manifest still filled from
the collector:

| Question | Result |
|---|---|
| Tracked and pinned child of one snapshot | Two hashes. A plain `Tag` with the same metadata gives the untagged hash. |
| The issue's probe | `p_child` keeps `{ref: "agg-v1", follow: false}` and stays put on the revise; `t_child` moves. |
| Built pinned first | `t_child` records `{ref: "agg", follow: true}` and moves. |
| The revert (shape 1) | `t_child`'s replay lands on a new entry, not on `p_child`'s, and it keeps following `agg`. |
| Two aliases on one entry (shape 2) | Two child entries; only `agg`'s follower moves. |
| A tracked child when its parent moves | Its hash changes, through the snapshot path, as today. |
| Replay with nothing moved | `noop` for both children, same hash. |
| Reconstruction | `_reconstructed_hash` equals the stored hash for both children, because the readers tag during reconstruction too. |
| The memo of ADR-007 D2 (one bare read, so one table name, per snapshot) | Both readers wrap the same memoized `Read`: one table name; `_snapshot_read` records one miss and one hit, as today. |
| The tables xorq registers in the shared default backend when a read executes | The same as today: two tables added over the same four executions. |
| Rows and SQL | Identical to the untagged read. |
| `expr.yaml` and `load_expr` | The tag survives the round trip, and rebuilding a loaded build gives the same hash. |
| Worthiness | `classify_expr` counts the tag as an unknown relation, so every child becomes worthy (`ops:HashingTag`) until `Tag` joins the allow-list (`worthiness.py:39-44`). With it, a filter child and a filter over a cheap child stay cheap. |
| Buckaroo, spawned on a random port | For a tagged cheap child, loads the build tallyman hands it (the entry's own build with the project path filled in): 4 rows, `__row_order` last. |
| The diff | The companion's compare build and `full_diff` work over two tagged cheap entries. |
| A heal | A cheap child whose parent's snapshot was deleted heals it and reads 4 rows. |
| The by-hash forms | Carry no tag, so they no longer share a hash with an aliased read. |

One shape survives while the manifest still takes its edges from
`parent_capture`: the unused read. The collector records the edge, the
expression carries no tag for it, and the entry stays stale as it does today.
The fix is to take the recorded edges from the tags. **Derived edges** are the
outermost edge tags of the expression a build hashes: walk down from the top
and stop at each edge tag, so the tags inside a cheap parent's inlined graph,
which are that parent's own edges, are not taken for the child's. With the
manifest's `parents` replaced by derived edges:

- The unused read records no edge to `other`, and is not stale after `other`
  moves.
- A join of `tracked_expr_from_alias("agg")` and `pinned_expr_from_alias("agg-v1")`
  records both edges, and hashes apart from a join of `agg` with itself.
- A union of a tracked and a pinned child records both.
- A filter over the cheap `t_child` records only `t_child`.
- Every snapshot read in these entries sits under an edge tag. The two by-hash
  forms are the only builds with a snapshot read outside one.

Derived edges are right only if every read of another entry goes through a
reader, and two kinds of read break that. A cheap entry's loaded graph carries its own
edge tags, so anything that takes that graph without wrapping it in a new tag
exposes them as outermost:

- A recipe that calls `cached_result_expr(project, <cheap entry's hash>)`
  records the cheap entry's edge (`orders_src`) as its own. The existing test
  `test_materialize.py::test_an_empty_compute_cache_is_rebuilt_for_an_entry_that_records_no_parent`
  builds exactly this recipe and fails under derived edges, since it asserts
  the child records no parent.
- A promoted diff of two versions of a cheap alias reads both by hash through
  `build_diff_expr`, and records the alias's parent (`agg`, followed) as its
  own. It would then go stale when `agg` moves, and its replay could never
  refresh it.

So derived edges come with a check on the build's reads, run on the expression
the build hashes. Walking down from the top, the build stops at each edge tag
and at each **diff-side tag** (a `HashingTag` named `tallyman.diff_side` that
`build_diff_expr` wraps around each side it reads, recorded as no edge). It
refuses the build when an edge tag it stopped at was not handed out by a reader
in this build, or when it reaches a snapshot read before any tag. Probed with
the check in place:

- The tracked, pinned, grandchild, join and union recipes build, with the edges
  listed above.
- `xo.deferred_read_parquet` of `agg`'s v1 snapshot is refused: "snapshot
  read outside any edge tag". So is `cached_result_expr` of `agg`'s v1 hash.
- `cached_result_expr` of a cheap entry's hash is refused: its `orders_src`
  tag was not handed out by a reader in this build.
- A promoted diff of two versions of a cheap alias builds, with no edges.

That check is #228's, in structural form; see the questions below. With it,
derived edges are what the hash covers, so the manifest cannot disagree with
the entry's name.

### Option 2: tallyman-level identity

The **entry key** is `md5("entry|<xorq hash>|<recorded edges as canonical
JSON>")` cut to 12 hex characters, used as the content hash. The build itself
is unchanged. Probed by renaming the build directory to the key inside
`_build_and_persist`: the issue's probe, the other build order, the revert and
the two aliases all come out right. The unused read comes out differently: the
replay after `other` moves gives a new key, so the child's alias moves to a new
entry with the same rows.

What depends on the content hash being xorq's build hash:

| What | Depends on it? |
|---|---|
| Snapshot names | No. `snapshot_path` takes the entry's hash (`materialize.py:61`), and a child's read path carries it, so ADR-007 D3 (a child's identity is a function of its parent's) holds with keys. |
| `xorq_build/` directories | No. The build is copied into `entries/<hash>/xorq_build/` (`build.py:531-541`) and loaded from `.xorq_build_expanded/`, which no hash names; `load_expr` reads the path it is given. |
| The diff | No. Diff statistics, the diff session and a promoted diff name entries by their hashes; the compare build is xorq's own build of a join, in a temporary directory (`tallyman_companion/app.py:447`). |
| The viewer | No. The session id is `entry-<project>-<hash>`, and a worthy entry's view build is xorq's build of a bare snapshot read (`buckaroo_lifecycle.py:100`). |
| The recipe zips in the catalog repository | No. `write_recipe_zip` names the zip and its members by the entry's hash, and its docstring already says why it must not use `build_path.name` (`tallyman_core/catalog.py:125-131`). |
| Source entries | Already not xorq's hash: `md5("source\|<digest>\|<reader signature>")` (`source_import.py:74`). |
| `_reconstructed_hash` | Recomputes xorq's hash from `expr.py`, which no longer equals the key (probed). It is compared only with a second reconstruction (`result_cache.py:369-371`), so this is harmless today. |

So nothing breaks, and the existing suite passes with the key patched in (see
the table at the end of this section). The cost is to the model: the build
stops describing the entry. Two entries hold byte-identical `xorq_build/`
directories and differ only in their manifests, and the architecture's rule
that "once an entry exists, its build is what it means" becomes "its build and
its recorded edges". The key also comes from the collector, so every path that
computes an entry's hash has to run the collector to get it right, and the
unused read stays an edge.

### Option 3: keep the collision and refuse it

The early return compares the recorded edges with the build's own and fails
the build when they differ. Probed by raising a `BuildError` there:

- The issue's probe: `p_child` is refused, naming `t_child`'s entry and both
  edge lists. While `agg` is at v1 and a tracked sibling exists, the author has
  no way to pin `agg-v1` with that query except to change the query.
- The revert (shape 1): the auto-recalc fails with `status: "failed"`, and
  `t_child` is left stale until `agg` moves again. The error names a collision
  the user did nothing to cause.
- Two aliases on one entry: the `agg_copy` follower is refused.
- The unused read: the replay's edges differ from the recorded ones in the hash
  of `other`, so the replay is refused and the entry stays stale.
- The by-hash forms are refused when they land on an existing entry. Built
  first, they make the aliased child that follows fail instead.

No hash changes, so there is nothing to rebuild; there is also nothing to
repair the merged entries a corpus already holds. Rejected: it turns a silent
wrong answer into an error the user often cannot act on, and recalc, which runs
without the user, is where a collision is most likely to be reached.

### Option 4: the build wraps the expression in one tag of the recorded edges

An **edge wrap** is one `HashingTag` named `tallyman.edges`, whose metadata is
the collector's edges as canonical JSON, put over the whole rewritten
expression by `_build_and_persist` just before `build_expr`. The readers do not
change. Probed: the issue's probe, the other order, the revert and the two
aliases come out right; the unused read behaves as in option 2, a new entry
with the same rows; the memo, registry, Buckaroo and diff results match option
1b. It needs the same worthiness allow-list, because a cheap entry's loaded
graph now starts with the wrap and is inlined into its children.
Reconstruction no longer equals the stored hash, because it runs without the
collector.

This is the smallest change: a few lines in `_build_and_persist` and one in
`worthiness.py`. Like option 2, it keeps the collector as the source of the
edges, so the unused read stays an edge, and the hash covers the edges only on
code paths that remember to collect them.

### The probes side by side

"Right" means each alias keeps the edge its own recipe asked for.

Columns: 1b-c is option 1b with edges from the collector, 1b-d with derived
edges and no read check, 1b-r with derived edges and the read check.

| Shape | Today | 1b-c | 1b-d | 1b-r | 2 | 3 | 4 |
|---|---|---|---|---|---|---|---|
| Issue's probe, tracked first | pin moves | right | right | right | right | pinned child refused | right |
| Pinned first | tracked child stops following | right | right | right | right | tracked child refused | right |
| Revert (shape 1) | tracked child merged into pinned | right | right | right | right | recalc fails | right |
| Two aliases (shape 2) | wrong follower moves | right | right | right | right | second child refused | right |
| Unused read (shape 3) | stale for good | stale for good | no edge, fresh | no edge, fresh | new entry, same rows | stale for good | new entry, same rows |
| Reconstruction equals stored hash | yes | yes | yes | yes | no | yes | no |
| By-hash forms | share an aliased child's hash | own hash, no edge | own hash; a cheap entry's edges recorded as the child's | refused | own hash, no edge | whichever builds second is refused | own hash, no edge |
| Promoted diff of a cheap alias | no edges | no edges (not probed) | records the alias's parent, followed | no edges | no edges (not probed) | no edges (not probed) | no edges (not probed) |
| Existing suite | 947 pass, 1 fails | not run | 946 pass, 2 fail | 946 pass, 2 fail | 947 pass, 1 fails | not run | 947 pass, 1 fails |

The last row is the whole default suite (`uv run pytest tests`, which leaves out
the `integration`, `cache_lab` and `perf` markers), run once per option with the
option patched into every test. `test_fouc.py::test_unknown_api_path_404s` fails
in every run, with no patch too, because it needs the React app built. A cell
marked "not probed" follows from the option's design: in 1b-c, 2, 3 and 4 the
edges come from the collector, which records none for a promoted diff. The
second failure under 1b-d and 1b-r is the by-hash test named under option 1b:
under 1b-d its child records the cheap parent's edge, and under 1b-r the read
check refuses its recipe. No other existing test depends on a tracked and a
pinned child sharing an entry, on an unused read being an edge, on a recipe
reading another entry by hash, or on the content hash being xorq's hash of the
untagged expression.

## Recommendation

Option 1b, with derived edges and the read check (1b-r in the table).

1. Both readers return the parent's result wrapped in an edge tag carrying
   `{hash, ref, follow}`. `ref` and `follow` are what the hash lacks today; the
   parent's `hash` adds nothing the snapshot path or the inlined graph does not
   already carry, and is there so the manifest can be derived from the tag.
   `cached_result_expr` and `_snapshot_read` do not change, so pages, charts,
   the viewer and the live diff read exactly what they read today.
2. `worthiness._cheap_relation_types` gains `xorq.expr.relations.Tag`, which
   `HashingTag` subclasses; a tag passes rows through unchanged. No other build
   rule needs to know about tags. A tag sits directly over a parent's result,
   and no parent's result holds a sort: a worthy parent is a bare read, and a
   cheap one cannot sort under ADR-008 D4 (cheap means row-preserving over one
   file; everything else is materialized). So `row_order._hoisted_keys` never
   meets a tag between the top and a sort.
3. `build_diff_expr` wraps each side it reads in a diff-side tag. A promoted
   diff's recipe is the one recipe `_build_and_persist` builds that reads by
   hash on purpose (a source entry's generated recipe is built by the import,
   outside this walk), and the tag keeps its sides' own edges out of its
   manifest.
4. `_build_and_persist` walks the expression it passes to `build_expr`, stopping
   at edge tags and diff-side tags. It refuses the build when a tag it stopped
   at was not handed out during this build, or when a snapshot read lies
   outside every tag, with a message naming the reader to use. The collector
   stays, as the list of what was handed out. Otherwise it writes the edge tags
   it stopped at, sorted, as `manifest.parents`. A handed-out edge the
   expression does not use is dropped: a read the result does not depend on is
   not a parent.
5. The early return compares the existing manifest's `parents` with the derived
   edges. With the edges inside the hash and the reads checked, they are always
   equal; a difference is reported as an internal error naming the entry, and
   is never merged.
6. `docs/architecture.md` section 4 ("Parent edges" and "What a recipe may
   read") and the text of ADR-011 D5 (a bare content hash is refused in a
   recipe) say that the edge is part of the entry's identity, and that a recipe
   reads other entries only through the readers.

Why 1b over options 2 and 4: it is the only option in which the edges come from
the same object the hash is computed from. Replay, reconstruction, and any later
code that re-derives a hash from a recipe get the edges without being told to
collect them, and the unused read stops being an edge instead of being rebuilt
into a fresh copy of the same rows. Its extra cost is the read check, which
options 2 and 4 do not need to be correct. That check is the fix #228 needs
anyway, so the cost is paid once. Option 4 is the fallback if tagging every
read turns out to cost something these probes did not see; it would still need
a separate fix for #228.

What does not change: two recipes with the same edges and the same frozen
expression still share an entry, as `agg` and `agg_copy` do above, and that is
correct. No entry's rows change, since xorq removes the tags before execution.

## The three questions

### Is a re-derived edge on the early-return path ever safe?

No. Writing the second build's edges into the existing manifest has three ways
to go, and none is right:

- **Replace.** The last build wins instead of the first: the same defect, the
  other way round, and an entry's meaning then depends on build order.
- **Union.** The merged entry records both a followed and a pinned edge to
  `agg`. The followed one makes it stale, the replay runs the single `expr.py`,
  and the recalc moves every alias heading the entry, the pinned one included.
- **Refresh an edge's hash**, which is all the unused read needs. That changes
  an existing entry's record of its inputs after the fact, against the rule
  that names resolve once, when an entry is minted.

Any rewrite also breaks two records. The manifest is written once, last and
atomically, and afterwards only an unfaithful heal rewrites it. And the recipe
zip, which the first checkpoint writes with the manifest inside it and never
writes again (`catalog.zip_pending_entries` skips an entry that has one), would
disagree with the entry directory.

A re-derived edge is safe only when it equals the recorded one, and then there
is nothing to write. The recommendation makes that the only case the early
return can see, and turns the comparison into a check.

### Does the #228 fix remove the by-hash variant of this collision?

For the form #228 describes, yes. #228's suggested fix refuses a
`deferred_read_parquet` of a snapshot unless a reader returned that path during
the build, so the raw by-hash recipe fails before it reaches `build_expr` and
cannot collide.

It does not close the other by-hash form. A recipe that imports and calls
`cached_result_expr(project, hash)` builds today, collides with `t_child` and
`p_child`, and inherits the first build's followed edge (probed). #228's
exemption, as written, allows every path `cached_result_expr` returned during
the build, which includes this call.

The recommendation cannot leave either form to #228, because derived edges are
wrong without a check on reads: a `cached_result_expr` read of a cheap entry
records that entry's own edge as the child's (option 1b). So the recommendation
takes #228's check into the walk it already makes, in place of the path list
#228 proposes: a snapshot read outside every tag, or a tag no reader handed out
in this build, fails the build. Probed, that rule accepts every aliased read,
joins and unions included, and a promoted diff over cheap entries; it refuses
the raw path read, and `cached_result_expr` of a worthy or a cheap entry. If
#228 lands first with its path list, the fix for this ADR replaces that check.

### What does each option cost the existing corpus?

| Option | Hash changes | What a rebuild does |
|---|---|---|
| 1b | Every computed entry: each reads through a reader, so its expression gains a tag, and its children's read paths change with it. Source entries keep their hashes, which come from their bytes. | Replaying every recipe (`scripts/rebuild_native_catalog.py`) mints the new hashes. A recipe that reads another entry by hash fails the read check and has to be rewritten through a reader first. |
| 2 | Every computed entry, if the key is used for all of them, as it should be. | The same. |
| 3 | None. | Nothing to rebuild, and nothing repaired either. |
| 4 | Every computed entry with at least one edge, and everything downstream of one. | The same as 1b. |

Per the project rule, a rebuild is the migration. One thing a rebuild cannot
do: undo a collision that already happened. The rebuild replays each entry's
`expr.py`, and a merged entry holds only the first recipe. The second recipe's
text was never stored: a successful create records an `alias_set` event with
the prompt and the hash, and only a failed build's event carries the code
(`tallyman_mcp/server.py:660-740`). A merged entry is rebuilt merged. Every
merged entry is on more than one alias's history, so a scan of `aliases.jsonl`
for a hash that two aliases hold lists the candidates. That scan also lists
harmless sharing, such as `agg` and `agg_copy`.

## Testing

Tests a fix PR would add, failing first:

- The issue's probe: a tracked child and a pinned child with the same query
  get two hashes; `p_child` records `{ref: "agg-v1", follow: false}`; revising
  `agg` moves `t_child` and leaves `p_child` where it was.
- The same two children built in the other order: `t_child` records
  `{ref: "agg", follow: true}` and moves on the revise.
- The revert: `p_child` pins `agg-v1`, `agg` goes to v2, `t_child` is created,
  `agg` goes back to v1's recipe. `t_child`'s alias does not point at
  `p_child`'s entry, and it moves on the next revise of `agg`.
- Two aliases on one entry: tracked children of `agg` and of `agg_copy` are two
  entries, and revising `agg` moves only the first.
- The unused read: an entry that reads `other` and does not use it has no edge
  to `other`, and after `other` is revised it is not stale, and the staleness
  scan does not list it as unexplained.
- Replay: a recalc of a pinned child with nothing moved is a `noop` on the same
  hash; a tracked child's replay after its parent moved gives a new hash.
- Reconstruction: `_reconstructed_hash` equals the stored hash for a tracked
  and a pinned child.
- The memo: both readers' results over one snapshot share one `Read` node, and
  `_snapshot_read` holds one item for it.
- Worthiness: a filter over a tagged read, and a filter over a cheap tagged
  child, are cheap, and no entry's `cache_worthy_why` names a tag.
- Derived edges: a join of a tracked and a pinned read records both; a filter
  over a cheap child records only the child; a union records both sides; the
  edges are written sorted.
- The read check: `xo.deferred_read_parquet` of a snapshot, and
  `cached_result_expr` of a worthy entry's hash, fail to build with a message
  naming `tracked_expr_from_alias` and `pinned_expr_from_alias`;
  `cached_result_expr` of a cheap entry's hash fails the same way instead of
  recording that entry's edges; a recipe that writes its own `tallyman.edge`
  tag fails too.
- A promoted diff of two versions of a cheap alias builds and records no edge.
- The early-return check: with a manifest's `parents` edited on disk, building
  the same recipe again fails with an error naming the entry, and the manifest
  is untouched.
- Rows: a tagged read and the untagged read give the same rows and the same SQL.
- Buckaroo, marked `integration`: a cheap tagged child loads with rows and
  `__row_order` last.

One existing test changes:
`test_materialize.py::test_an_empty_compute_cache_is_rebuilt_for_an_entry_that_records_no_parent`
builds its child with `cached_result_expr` of a cheap entry's hash, which the
read check refuses. What it tests, that an entry whose manifest names no parent
still heals every file it reads after `compute_cache/` is wiped, can be tested
through a promoted diff, which reads by hash and records no edge.

## Open questions

1. **An unused read.** The recommendation drops it silently, since it is not a
   dependency. It could instead be a lint warning on the build result, or a
   build error. Paddy's call; the probes show only that recording an edge the
   hash does not cover is wrong.
2. **How #228 lands.** The recommendation needs #228's check in the form of the
   edge-tag walk, so the two fixes are one change. Either #228 is fixed by this
   ADR's PR, or #228 lands first with the path list it proposes and this ADR's
   PR replaces it.
3. **Merged entries already in the corpus.** A rebuild keeps them merged. Either
   scan for them before the rebuild and rewrite the lost recipes by hand, or
   accept them.
