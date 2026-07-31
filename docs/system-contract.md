# Tallyman: primitives and the system contract

- **Status:** Normative, implemented (PR #167, 2026-07-31). Describes the
  system as built; where code or the descriptive docs disagree with it, the
  disagreement is a bug. Design decisions and pre-fix history:
  `plans/adr-read-path-loads-builds.md`.
- **Audience:** no prior xorq knowledge assumed. The xorq section below covers
  exactly as much of xorq as the rest of the doc needs, and no more.

## What tallyman is, in three sentences

Tallyman is a deconstructed notebook: instead of cells in a file, the unit of
work is a **catalog entry** — one dataframe computation, compiled and stored on
disk under a content hash, in a git-backed catalog. An LLM authors entries
through an MCP server; a web UI and the Buckaroo data grid render them. The
catalog is append-only and content-addressed: entries are immutable values,
names point at them, and history is never rewritten.

---

# Part 1 — The xorq substrate

Tallyman's compute layer is xorq, a deferred dataframe engine (an ibis fork
with its own serialization and caching). Five ideas from xorq carry everything
tallyman does.

## 1. Deferred expressions

An **expression** is an immutable graph of dataframe operations — read this
parquet file, filter, join, aggregate. Constructing it executes nothing.
Calling `.execute()` compiles the graph to a query plan and runs it on an
engine (an embedded DataFusion, by default), returning a dataframe.

Because expressions are inert values, they can be walked, rewritten, hashed,
and serialized. Everything below exploits that.

## 2. Backends, and the object-identity trap

Every leaf of an expression (a file read, a table) is bound to a **backend** —
the connection object that will execute it. A backend is described by a
**profile**: the backend's name plus its connection kwargs. Two facts about
backends shape tallyman's design:

- **One expression, one backend.** xorq refuses to execute a graph whose
  leaves span more than one backend (`Multiple backends found`). "More than
  one" is decided by **Python object identity**, not configuration: two
  separately-created connections with byte-identical profiles still count as
  two backends.
- **Profiles carry a serial-number field that is noise.** `Profile.idx` is a
  process-global counter stamped on each connection. Two profiles are the
  *same profile* for every purpose tallyman cares about iff they are equal
  with `idx` removed. Any code that compares or dedups profiles must strip
  `idx` first (xorq's own build path does this via `normalize_profiles`).

The remedy for the identity trap is **`replace_sources`**: given a mapping
{old backend object → new backend object}, it rewrites an expression rebinding
every leaf and every cache reference onto the new object. This is the
sanctioned way to take expressions that arrived on different backend objects
and compose them onto one shared backend.

**Rebinding is zero-copy.** A file read or cache node is a path plus a recipe
for reading it; swapping which connection executes it moves no data — the new
backend reads the same files at execute time. (Tallyman's backends are
stateless in-process engines; all data state lives in files.) The one node
type that *would* require a copy is a raw database table — rows living inside
the old connection's session rather than in a file — and `replace_sources`
refuses those unless passed an explicit transfer flag. Tallyman never passes
it: the build forbids the authoring patterns that create such nodes
(in-memory reads are a build error), so every leaf is a file read and the
refusal is a loud assertion that the build gate failed, never a live copy
path. Beside it, rebinding fails loudly if a build ever spans more than one
distinct backend content profile (ADR D3).

## 3. Builds: freezing an expression to disk

`build_expr(expr, builds_dir)` serializes an expression into a **build
directory**:

```
<builds_dir>/<hash>/
  expr.yaml       # the whole graph, self-contained
  profiles.yaml   # backend descriptions (content only — no live objects)
  *metadata.json  # library version, schema, etc.
```

Three properties matter:

- **The build is closed.** `expr.yaml` contains the entire graph: every
  upstream operation is written into the file (as an inline node or an
  intra-file reference), functions are pickled inline, and the chain bottoms
  out at concrete file reads. Loading a build needs nothing but the build
  directory and the files its reads point at. There are no references to
  anything outside the file except those read paths and the profiles.
- **The directory name is the expression's hash** — a 12-hex-char token over
  the normalized graph. Two structurally identical expressions over the same
  read paths produce the same hash, in any process (profile `idx` is
  normalized out before hashing).
- **File reads are hashed by path string only.** The hash does not look at a
  file's bytes, size, or mtime — just the path.[^mtime] This is deliberate
  (build artifacts should be reproducible from what the expression *is*), and
  it is the single most consequential xorq design choice for tallyman: **if
  you want content identity, you must put the content in the path.** Tallyman
  does (Part 2, CAS).

`load_expr(build_dir, cache_dir=...)` is the inverse: it reads the yaml,
mints **fresh backend objects** from the profiles (nothing is memoized — two
loads of the same build yield two distinct backend objects, which is the
identity trap again), and rebases cache storage into `cache_dir` (next
section). The `cache_dir` parameter is the seam that lets the same build run
against any cache directory, including an empty one.

## 4. Caching: snapshot keys, and existence as the only check

Calling `.cache()` on an expression wraps it in a **cache node**. At execution
time xorq computes a **key** from the wrapped subgraph and looks for
`<cache_dir>/<relative_path>/<key>.parquet`:

- file exists → read it back; the subgraph does not run;
- file missing → execute the subgraph, write the file (atomically), read it.

That is the entire mechanism. There is no invalidation step and no validation
of the file's contents: **a cache hit is decided by filename existence
alone**, and the stored bytes are trusted by assumption. Consequently the
cache is only as honest as two things: the key derivation, and whoever writes
the files.

Tallyman uses xorq's **snapshot** keying everywhere: the key is a token over
the subgraph's structure and its read *paths* — again deliberately blind to
file content and mtime.[^mtime] Under CAS paths (Part 2) this blindness is
harmless: content is in the path, so the key is content-honest. The cache node survives
serialization: a build of a cached expression carries the cache node in its
yaml, so *any* loader of that build reads through the same cache key. What is
**not** serialized is the cache's base directory — every loader must supply
`cache_dir`, which is why the parameter exists.

One xorq wart tallyman must own: xorq's load-time cache-dir rewrite walks the
graph shallowly and misses cache nodes nested *inside* another cache node's
subgraph. Tallyman's loader therefore does its own deep rewrite (over xorq's
deep-walking primitives) so that every cache node in a loaded build — nested
or not — lands in the project's cache directory, never in the global default
(`~/.cache/xorq`).

## 5. What the hash sees, and what it cannot

The expression hash covers structure and declared inputs: operations, schemas,
literals, pickled function bodies, read paths. It cannot see **execution
behavior**: a graph containing `sample()`, `now()`, an unordered `limit`, or
an impure UDF hashes identically every time while executing to different
bytes. The hash is an *input* identity, never an *output* identity. Tallyman
adds the output axis itself (Part 2, `result_digest`).

[^mtime]: xorq also ships an mtime-keyed strategy: each source file's stat
    (mtime/size/inode) folds into the cache key, so editing the file changes
    the key and the next read recomputes. Tallyman doesn't use it because it
    answers a different question. That cache is a memoization of the present:
    the key follows the data (approximately — stat, not bytes), a miss *is*
    the change signal, and the recompute silently replaces the old answer. It
    keeps no record that anything changed and no stable name connecting old
    answer to new — built to serve the current file's answer fast, nothing
    more. Tallyman needs the opposite: entries are history, so identity must
    hold still while data moves (that is what makes V3-vs-V4 meaningful), and
    change is handled explicitly instead — the manifest records what the
    world *was* (source digests, parent hashes), the staleness scan detects
    by comparing that record against the live world, recalc mints new entries
    and advances aliases, and old entries keep serving their old bytes
    forever. A key whose job is to move when data moves has nothing to hang
    that on. Bolting it under tallyman's caches would also collide with
    identity mechanically: the expression hash is hardwired to path-only
    normalization (measured in the source-identity ADR), so mtime-keyed
    caches would move while entry hashes stood still — fresh bytes under an
    old name, the #163 failure shape. CAS serves both needs with one
    mechanism: content in the path gives the hash and every cache key an
    identity that moves exactly when content moves, and the recorded digests
    give staleness something durable to compare. (One stat use survives, as
    an accelerator: the source-digest memo skips re-hashing files whose stat
    is unchanged; content stays the truth.)

---

# Part 2 — The tallyman primitives

## Project, sources, and CAS

A **project** is a directory (`~/.tallyman-notebooks/projects/<name>/`) holding
user data files (`data/`) and the catalog (`artifacts/catalog/`, a git repo).

A **source** is a user-provided data file. Sources are mutable on disk — the
user can overwrite `trips.parquet` any time — so tallyman never lets an entry
depend on a live source path. At build time each source is:

1. digested (md5, memoized on stat so unchanged files hash once),
2. cloned copy-on-write to `data/.cas/<digest><suffix>`,
3. read through the clone.

This is **CAS** (content-addressed sources), and it is the answer to xorq's
path-only hashing: the path *is* the digest, so every xorq-level key — the
expression hash, every cache key — becomes content-honest for free. Edit a
source and rebuild: the digest changes, the path changes, the hash changes, a
new entry forks. The clone is the entry's immutable input forever; the live
file is merely where the *next* build will look.

## Recipe

A **recipe** (`expr.py`) is the LLM-authored Python that defines a
computation. It must bind a variable `expr`, built from:

- `read_project_file("trips.parquet")` — read a source (through CAS);
- `tracked_expr_from_alias("trips")` — build on another entry, named by alias,
  recording a `follow=True` parent edge: this expression depends on the parent
  alias, and when that alias advances, recalc mints a new version of this
  expression;
- `pinned_expr_from_alias(<hash or "name-vN">)` — same, but `follow=False`:
  recalc never touches this expression when the parent moves. Pins name an
  exact version — a hash, or an explicit `"trips-v3"` version reference; a
  bare alias is rejected as ambiguous (it would silently pin whatever the
  head happened to be when the recipe was built — #166).

The defining property of a recipe: **it binds by name.** "trips.parquet"
means whatever bytes sit there right now; "trips" means whatever entry that
alias points at right now. A recipe therefore has a different meaning at
different moments. That is exactly what you want when *authoring* — and
exactly what you must never consult again afterward.

## Entry

An **entry** is the immutable unit of the catalog: one computation, built
once, over inputs fixed forever. On disk:

```
entries/<content_hash>/
  expr.py         # the recipe, verbatim (paths made portable)
  xorq_build/     # the frozen build (Part 1 §3), paths made portable
  manifest.json   # the closure record (below)
  schema.json
entries/<content_hash>.zip   # git-tracked durable form, written at checkpoint
```

An entry carries **two representations of its computation, with different
authority**:

| | `expr.py` (recipe) | `xorq_build/` (build) |
|---|---|---|
| binds inputs by | name (aliases, live paths) | value (parent graphs inlined, CAS paths) |
| meaning over time | drifts as names move | fixed forever |
| authoritative for | authoring: revise, display, the *next* build | semantics: every read, forever |

**The authority rule: after build time, the build is the entry's semantics
and the recipe is documentation.** Re-executing a recipe on behalf of an
existing entry asks "what would this code produce *today*" when the question
is "what did this entry produce *then*." The recipe is re-executed in exactly
one situation: minting a **new** entry (create, revise, recalc), where
resolving names to current heads is the point. (#163 is what happens when
this rule is broken: reads re-ran recipes, so historical entries silently
re-bound to today's parents.)

## Content hash

An entry's `content_hash` is xorq's expression hash (Part 1 §3), taken at
build time. Two details determine what it covers:

- the expression hashed is the **rewritten** one — cache injection (below)
  has already happened — so an entry's caching shape is part of its identity;
- every file read in the graph points at a CAS clone, so each read path
  carries a digest of the input bytes; and since parent expressions are
  inlined into the graph, the same holds all the way up the lineage.

The hash therefore names "this computation over these exact input bytes."
That one property makes builds idempotent and history append-only. Rebuild
the same computation over unchanged inputs and you land on the existing
entry: the build recognizes the hash and stops. Change anything that alters
the computation or its inputs — the recipe's logic, a source's bytes, a
parent's graph — and a new entry forks under a new hash, while every existing
entry keeps its name and its meaning.

Two limits are deliberate. The hash cannot see execution behavior (Part 1
§5): a recipe calling `sample()` or `now()` hashes identically run to run,
which is the gap `result_digest` (below) exists to police. And it makes no
attempt at cross-machine portability: absolute path prefixes participate in
the hash.

## Manifest: the closure record

`manifest.json` records everything about the build moment that the build
itself doesn't state, so that no later operation ever needs to resolve a name:

| field | meaning |
|---|---|
| `content_hash` | the entry's identity |
| `parents` | `[{hash, ref, follow}]` — each alias reference, **resolved to the exact hash it meant at build time** |
| `sources` | `{rel_path: digest}` — each source, pinned to the bytes read |
| `result_digest` | SHA-256 of the baked result snapshot (worthy entries) — the output identity |
| `snapshot_key` | the baked snapshot's cache key, recorded at build (see write path) |
| `cache_worthy`, `cache_worthy_why`, `cache_bytes` | the worthiness verdict and its evidence |
| `row_count`, `execute_seconds`, `compile_seconds`, timings | build measurements |

The manifest is written last, atomically: its presence is the "this entry is
complete" sentinel. An entry directory without one is treated as absent.

## Alias

An **alias** is a mutable name: `{alias, latest, history}` in a git-tracked
file. `latest` is the head; `history` is every hash it has pointed at (V1…Vn,
oldest first). Revising an alias mints a new entry, advances `latest`, appends
to `history`. Old entries remain, immutable, as the version history.

**Where alias resolution is legal** — names resolve in exactly three
situations, all of them *about* choosing or minting, never about serving:

1. **Minting** (build / revise / recalc): resolve heads, record the resolved
   hashes in the new entry's manifest.
2. **Selecting** (UI, diff version arithmetic): resolve "by_hour" or "V-1" to
   a hash, *then* serve that hash.
3. **Judging** (staleness scan): compare recorded parent hashes and source
   digests against current heads and current files — read-only, executing
   nothing.

Once an entry is selected, serving it consults no name again. `follow` is a
**recalc** policy (should a parent's advance mint a new version of this
entry?), never a read policy: reads are lineage-faithful for followed and
pinned parents alike.

## Worthiness: cheap and expensive entries

At build time, tallyman rewrites the author's expression before freezing it
(**the worthiness rewrite**):

- every non-parquet file read gets a cache node injected after the parse (so
  a CSV is parsed once, shared by every entry reading it);
- if the expression contains an expensive operation — Aggregate, Join, Sort,
  Window, or a UDF — the whole expression is wrapped in a top-level cache
  node. Such an entry is **worthy**: its result is materialized once at build
  (**the baked snapshot**, a parquet under the project's
  `compute_cache/result_cache/`) and read back ever after.
- everything else is **cheap**: a projection/filter/rename over columnar
  sources re-executes in pushdown time comparable to reading a copy, so no
  copy is kept.

The rewrite happens *before* hashing, so the cache nodes are part of the
entry's identity and travel inside `xorq_build/` — which is why every loader
of the build, in any process, reads through the same snapshot key.

## Result digest

For worthy entries, the build records `result_digest`: the SHA-256 of the
baked snapshot file, taken over a canonically-ordered materialization so an
engine's parallel-scan row reshuffling cannot move it — the worthiness
rewrite sorts by the author's own `order_by` keys, then `original_row_order`,
then the remaining sortable columns (ADR D5). It is
the **output** identity axis, and it has exactly one job: witnessing that a
later rematerialization reproduced the original bytes. It is never a
staleness input (an entry whose recompute differs is *nondeterministic*, not
stale — recomputing cannot make it fresh) and never part of the entry's name.

It exists because recipes are LLM-authored: an LLM can write `sample()` or an
impure UDF, the hash cannot see it (Part 1 §5), and the digest is the runtime
backstop that catches it.

---

# Part 3 — The lifecycle

## The write path (build)

`build_and_persist(project, code)`:

1. **Import the recipe** — the single moment of name resolution. During the
   import, `read_project_file` digests and clones each source (CAS) and
   records `{rel_path: digest}`; `tracked_expr_from_alias` resolves each
   alias to its current head, records the parent edge, and returns the
   parent's expression (loaded from the parent's frozen build — see read
   path).
2. **Rewrite** — the worthiness rewrite (cache injection).
3. **Freeze** — `build_expr` serializes the rewritten expression;
   `content_hash` = the build's name. If an entry with this hash already
   exists, stop: append the prompt, return the existing entry (idempotency).
4. **Lay down the entry** — copy the build in, make paths portable
   (`${TALLYMAN_PROJECT_ROOT}` placeholders), write `expr.py`.
5. **Execute once** — load the just-written build against the project's
   compute cache and run it. Worthy: the top cache node materializes the
   baked snapshot (canonically ordered), and the build records its digest
   **and its snapshot key** in the manifest. Cheap: one full streaming pass
   (honest evaluation, fails fast, keeps nothing).
6. **Record** — schema, manifest (written last, atomic).
7. **Checkpoint** — when the MCP tool returns: recipe zip, tracked pointers,
   one git commit.

The build's obligation in one line: **record everything a reader will ever
need, because the reader is forbidden from resolving anything.**

## The read path

One canonical read, used by every consumer:

```
read(project, content_hash, cache_dir=<project compute cache>):
    expand the entry's xorq_build (placeholders → real paths, stable per-entry dir)
    expr = load_expr(expanded, cache_dir=cache_dir)   # + tallyman's deep cache-dir rewrite
    return expr
```

- **Worthy entry:** the loaded graph carries its cache node, so execution
  reads the baked snapshot by key. If the file is missing (evicted), the
  cache node re-executes the *frozen* subgraph — whose leaves are CAS clones
  and inlined parent graphs, all pinned — writes the snapshot, and the result
  is verified against `result_digest` (below). Single-flighted per entry so
  concurrent cold readers don't race.
- **Cheap entry:** execution re-runs the frozen graph in pushdown time,
  reading CAS clones. Same bytes every time, by construction.
- The `cache_dir` parameter is the **cold seam**: any entry can be
  materialized against an empty temp dir, and must produce the same bytes as
  the warm read. That property is the standing regression test for every
  read-path change.

`cached_result_expr(project, hash)` is the function every in-process consumer
calls, and it is an optimization over the canonical read, nothing more: it
performs the read once per `(project, content_hash)` and keeps the loaded
expression in a bounded in-process LRU, so repeated reads (every pagination
request, both sides of a diff) skip the load. Removing it must change latency
and nothing else. That is what makes its key honest: the cached value is a
pure function of the frozen build the key names. The per-call
snapshot-existence check and the single-flighted heal stay outside the LRU,
because file existence is the one input that remains mutable (eviction).

**Chaining** (`tracked_expr_from_alias` at build time) uses the same read: the
parent's frozen build is loaded and its graph — including, for a worthy
parent, its cache node — is composed into the child. The child's build
therefore carries its own regeneration knowledge. If the parent's snapshot is
missing when the child executes, the parent's cache node re-runs its frozen
subgraph and rewrites the file, the same as any cache miss; nothing outside
the execution has to arrange for the file to exist.[^preheal]

The recipe-reconstruction machinery survives only as a diagnostic. An entry
whose build is missing or unloadable is a **hard error** naming the entry and
the remedy (rebuild) — there is no automatic recipe fallback, because a
warning on a background read is exactly how #163-class behavior stays
invisible (decided in `plans/adr-read-path-loads-builds.md`, D6).[^recon]

## Composition: diff and beyond

To compose two entries (diff's outer join, a union, any multi-entry
expression):

1. read each entry (canonical read above) — each load minted fresh backend
   objects;
2. **rebind onto shared backends, one per distinct content profile** (profile
   identity = profile minus `idx`), using `replace_sources`. For today's
   catalogs every profile is the same embedded engine, so this collapses to
   one shared backend;
3. compose. The result is a single-backend expression: it executes
   in-process, and `build_expr` serializes it into a normal single-profile
   build that Buckaroo's `/load_expr` accepts — the same handoff as an
   ordinary entry grid.

Composition of frozen builds was never the problem; backend object identity
was (#75, rediagnosed in #163). The rebind is cheap graph surgery, no data
moves.

A **promoted diff** is just an entry whose recipe pins two hashes
(`build_diff_expr(a_hash, b_hash)`) — name-free, deterministic, and built
through the ordinary write path.

## Staleness and recalc (unchanged, stated for completeness)

**Staleness** is a read-only judgment: an entry is stale on the alias axis
when a `follow=True` parent's recorded hash no longer equals that alias's
head, and on the source axis when a recorded digest no longer matches the
live file's digest. Computing staleness executes nothing and mutates nothing.

**Recalc** is the one sanctioned re-execution of recipes. When an alias head
advances (a revise), the entries that may be affected form its **cone**:
every entry reachable by walking `follow=True` parent edges backwards from
that alias — its followers, their followers, and so on. Pinned
(`follow=False`) edges are not in the cone. It runs automatically after a
revise when the project enables auto-recalc, or on demand.

Recalc rebuilds the cone in topological order, parents before children. For
each member it re-imports the member's *recipe* — the one situation where
name resolution is the point, since the goal is a new version against the
new heads — builds the result as an ordinary new entry, and advances the
member's alias before any of its children replay, so each child chains off
its parent's fresh head. Old entries are untouched; every member gains a
version, none loses one.

Two disciplines keep it predictable. **Scope:** recalc touches only followers
of the alias that moved; pre-existing staleness elsewhere is reported, not
swept up in the cascade. **Atomicity:** the head advance and its cascade land
as a single checkpoint, so one reset-to-revision undoes the whole event, and
the UI receives a single remap (`{old hash: new hash}` per member) to follow
open entries to their new versions.

## Verification

`verify_result_faithful(project, hash)` means exactly: *executing this
entry's frozen build reproduces its recorded `result_digest`.* It runs in
production, not only in tests:

- on every self-heal (a healed snapshot is checked before it is served);
- on demand, corpus-wide, via `catalog_scan_staleness(verify_results=True)`.

A failure is surfaced loudly — a durable `unfaithful_heal` record in
`errors.jsonl` (the UI badge and ADR D12's non-evictable marking), a stat
cache wipe, session eviction, an SSE event — never only a log line. Its
attribution has three classes with three different fixes:

| class | detector | meaning | response |
|---|---|---|---|
| structural (#88) | recipe re-derives a different hash, sources unchanged | author-time value baked into the graph (`pd.Timestamp.now()`) | lint; rewrite recipe |
| execution (#83) | fixed graph, digest moves across runs | `sample()`, `now()`, impure UDF | lint; pin entry to its snapshot, mark non-evictable |
| lineage (#163) | a read resolved a name post-build | machinery bug | impossible by construction under this contract |

The third row is the point of the whole design: with reads going through the
build, lineage drift has no mechanism left.

[^preheal]: This replaced a two-layer workaround. Before the #163 fix,
    chaining stripped the parent's cache node down to a bare file read of its
    snapshot (the #75 single-backend fix), so the child's build pointed at a
    parquet without knowing the file was regenerable — evict the snapshot and
    the child read nothing. To compensate, the companion called
    `cached_result_expr` on an entry just before handing its build to
    Buckaroo and discarded the result: the call existed only for its side
    effect, forcing any missing ancestor snapshots onto disk in time for
    Buckaroo's replay. The stripping and the pre-heal call were both deleted
    with the fix.

[^recon]: The subsystem the fix demoted: `_recipe_expr` re-imports an entry's
    `expr.py` to recover its expression, patching over the recipe's
    name-binding with context variables for the duration of the import.
    `_RECON_SOURCES` carries the entry's recorded `{path: digest}` map so
    that `read_project_file`, when called inside such a re-import, resolves
    each source to its frozen `.cas/<digest>` clone instead of re-digesting
    the live file (#115), and `_resolve_noncyclic_hash` walks a
    self-referencing alias back to its previous version to break re-import
    cycles (#74). Each hook re-derives a binding the frozen build already
    contains, which is why the fix retired their read-time role (and #162's
    proposed `_RECON_PARENTS` sibling was never built) rather than adding a
    fourth. The diagnostic use stays, and is the only reason the machinery
    still exists: `recipe_is_structurally_nondeterministic` re-derives the
    hash from the recipe on purpose, because re-running the recipe is exactly
    how you detect that a recipe fails to reproduce its own graph.

---

# Part 4 — The invariants

Everything above compresses to five statements. Any change that breaks one is
wrong even if every test passes.

- **I1 — A content hash names a fixed result.** Materializing an entry yields
  bytes consistent with its build — any consumer, any cache state, any time.
  The only tolerated residual is execution nondeterminism *inside a recipe*
  (#83), which the digest detects; no violation may originate in tallyman's
  own machinery.
- **I2 — Caches affect latency, never results.** Every answer must be
  byte-identical with all caches empty. Corollaries: cache keys derive only
  from immutable inputs; cached values are reproducible from the frozen build
  alone; every read path has a cold seam (`cache_dir`); a self-heal is
  reproduce-and-verify, never manufacture. Result bytes are manufactured
  exactly once, at build — a read path that writes bytes the build didn't
  define has become a second, unaudited build path.
- **I3 — One read semantics.** Every consumer that materializes an entry
  reads the frozen build through the one canonical read. The recipe is never
  re-executed on behalf of an existing entry.
- **I4 — Names resolve exactly once.** At build, resolutions are recorded in
  the manifest; after that the entry is closed. Aliases at read time select
  *which* entry to serve, never *what* an entry means. (Grep-able form:
  `get_alias`/`previous_version`/`version_of_hash` appear only in minting,
  selecting, and judging code — never in materializing code.)
- **I5 — One question, one path.** Any question answerable two ways (grid vs
  API bytes, manifest row count vs live count) either shares one canonical
  path or carries an explicit check tying the two together; disagreement is
  surfaced, never averaged over.

---

# History

This contract began as the proposed design for the #163 fix; the pre-fix
deviations and the decisions that settled the design (D1–D12) are in
[`plans/adr-read-path-loads-builds.md`](../plans/adr-read-path-loads-builds.md).
The wider audit of the same bug class is
[`plans/cache-soundness-audit.md`](../plans/cache-soundness-audit.md)
(#168–#172, buckaroo#955–#957) — the contract's rules apply to those axes
too.
