# Tallyman: primitives and the system contract

- **Status:** Normative, implemented. Describes the system as built; where
  code or the descriptive docs disagree with it, the disagreement is a bug.
  Design decisions and history: `plans/ADR-006-read-path-loads-builds.md`
  (the read path, still in force for what the later ADRs did not change) and
  `plans/ADR-007-tallyman-owned-materialization.md`,
  `plans/ADR-008-row-order-of-reads.md` and
  `plans/ADR-009-digest-stability.md` (tallyman writes its own result files,
  every file carries `__row_order`, and the digest is a content digest).
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
every leaf onto the new object. This is the
sanctioned way to take expressions that arrived on different backend objects
and compose them onto one shared backend.

**Rebinding is zero-copy.** A file read is a path plus a recipe
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

`load_expr(build_dir)` is the inverse: it reads the yaml and mints **fresh
backend objects** from the profiles (nothing is memoized — two loads of the
same build yield two distinct backend objects, which is the identity trap
again). A build that holds no cache node needs nothing else to load, and
tallyman's builds hold none (Part 2, "Materialization").

## 4. xorq's cache nodes, which tallyman does not use

Calling `.cache()` on an expression wraps it in a **cache node**. At execution
time xorq computes a key from the wrapped subgraph and looks for
`<cache_dir>/<relative_path>/<key>.parquet`: if the file exists it is read back
and the subgraph does not run, and if it is missing the subgraph runs and the
file is written. A hit is decided by filename existence alone, so the cache is
only as honest as the key derivation and whoever writes the files. The node
survives serialization, but its base directory does not: a loader has to supply
one, and xorq's load-time rewrite of it misses nodes nested inside another
cache node's subgraph.

Tallyman stopped depending on all of this (`plans/ADR-007-tallyman-owned-materialization.md`).
No build holds a cache node, so no loader has to aim one at a directory, the
file names come from tallyman and not from xorq's tokenization of a graph, and
the writer is tallyman's own (Part 2, "Materialization"). A recipe that calls
`.cache()` is a build error, because a node with default storage would write
under `~/.cache/xorq`. What remains of xorq here is the expression, build, load,
hashing and execution layer.

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
path-only hashing: the path *is* the digest, so every xorq-level key (the
expression hash, and the name of every file tallyman writes) becomes
content-honest for free. Edit a source and rebuild: the digest changes, the path
changes, the hash changes, a new entry forks. The clone is the entry's immutable
input forever; the live file is merely where the *next* build will look.

A recipe never reads the source or its clone directly. Ingest writes an
**ordered copy** of the clone under the project's `compute_cache/ordered_sources/`:
polars reads the clone in file order and writes a parquet file with the same
columns and one more at the end, `__row_order`, `0..N-1`. The copy is named by
the source's digest and the reader options (the schema and `scan_csv` options
of a CSV), and the recipe reads that file. So a recipe's reads are all files
tallyman wrote, each carrying the column that pages sort by (Part 2, "Row
order").

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

- the expression hashed is the one after the rewrite (below), which adds the
  canonical sort to a worthy entry and adds no cache node to anything;
- every file read in the graph is a file tallyman wrote under a name that
  carries content: an ordered copy named by its source's digest, or a worthy
  parent's snapshot named by the parent's content hash. So the hash covers the
  input bytes, and a child's hash is a function of its parent's.

The hash therefore names "this computation over these exact input bytes."
That one property makes builds idempotent and history append-only. Rebuild
the same computation over unchanged inputs and you land on the existing
entry: the build recognizes the hash and stops. Change anything that alters
the computation or its inputs — the recipe's logic, a source's bytes, a
parent's identity — and a new entry forks under a new hash, while every existing
entry keeps its name and its meaning.

Two limits are deliberate. The hash cannot see execution behavior (Part 1
§5): a recipe calling `sample()` or `now()` hashes identically run to run,
which is the gap `result_digest` (below) exists to police. And it makes no
attempt at cross-machine portability: absolute path prefixes participate in the
hash.

## Manifest: the closure record

`manifest.json` records everything about the build moment that the build
itself doesn't state, so that no later operation ever needs to resolve a name:

| field | meaning |
|---|---|
| `content_hash` | the entry's identity |
| `parents` | `[{hash, ref, follow}]` — each alias reference, **resolved to the exact hash it meant at build time** |
| `sources` | `{rel_path: digest}` — each source, pinned to the bytes read |
| `ordered_copies` | `{key: {source, digest, reader, content_digest}}` — each ordered copy the plan reads, with the reader options and the digest that let it be made again |
| `cache_worthy`, `cache_worthy_why`, `cache_bytes` | whether the entry is materialized, decided once at build, and the evidence |
| `result_digest` | `arrow-sha256:` digest of the snapshot's content (worthy entries) — the output identity |
| `reproducible`, `nonreproducible_columns` | whether two runs at create gave the same digest, and the columns that differed |
| `unfaithful_heal_digest` | the digest the last unfaithful heal wrote; set, it pins the snapshot. The one field written after create |
| `snapshot_format`, `engine_versions` | the format version and the xorq, xorq-datafusion and pyarrow versions at build |
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

## Worthiness: cheap and worthy entries

Whether an entry has a file of its own is decided once, when it is built, by
one test on the expression the author wrote (`worthiness.classify_expr`), and
recorded in the manifest as `cache_worthy`. Nothing derives it again.

An entry is **cheap** only if all of these hold: every relation operation is a
file read, filter, column selection, computed column, rename, cast, column
drop, drop of null rows or fill of nulls; the plan reads exactly one file; and
no value operation multiplies rows (`unnest`), depends on the order rows arrive
in (a window function, which also covers `row_number` and `lag`) or is not pure
(`random()`, `uuid()`, `now()`, `today()`, or any UDF). Anything else is
**worthy**: an aggregate, join, sort, limit, union, distinct, a second file, or
an operation nobody has classified. The list is an allow-list, so an unknown
operation costs a copy and never unstable paging.

- A **worthy** entry is **materialized**: its result is written once, when the
  entry is created, to a **snapshot**,
  `compute_cache/result_cache/<content_hash>.parquet`, and every read after that
  is a read of that file.
- A **cheap** entry writes nothing. It is a stored plan over files that exist
  (a view, in the database sense), which re-runs on every read. Tallyman ran it
  in full when the entry was created, so an error in it surfaced there.

The canonical sort, described next, is added only to a worthy entry.

## Row order

Every file tallyman writes ends in an `int64` column named `__row_order` holding
`0..N-1` in the file's physical row order. It is the last column, and it is
visible in every table. Two writers produce it: ingest (an ordered copy of a
source) and `materialize` (a snapshot), and each materialization overwrites an
inherited one with positions in its own file. It is what makes a page of an
entry a function of `(content_hash, sort, offset, limit)`: with no user sort a
page is `ORDER BY __row_order`, and with one the user's keys come first and
`__row_order` is the last key, which breaks every tie.

- A cheap entry has no file of its own, so it inherits the column and must keep
  it. A cheap recipe whose output lacks it fails to build, and the error names
  what it reads and shows the corrected select. A worthy entry may drop it,
  because the writer numbers its rows.
- A recipe may read the column and copy it under another name
  (`__row_order_v1`), and may not assign to it. To change the order of rows, sort
  them: the writer numbers the result in that order.
- Every `order_by` in a recipe gets `__row_order`, then the remaining sortable
  columns, as its last keys, so the sort is total wherever the recipe put it. A
  sort that is not the recipe's last step is kept: the top-level sort leads with
  its keys, and the build fails, naming the key, if a later step dropped or
  changed it.
- A join of two entries leaves the right side's copy under ibis's collision name,
  `__row_order_right`, and the writer drops it. Joining three entries in one
  recipe needs `.drop("__row_order")` on the right-hand inputs, and the build
  says so.
- A diff carries no row-order column from either side.

## Materialization

`materialize(project, hash)` is the one routine that writes a snapshot. The
build calls it and so does every heal, so result bytes are manufactured in one
place. It runs the entry's frozen build on a **single-partition** connection
(so a float total is merged in one order and is bit-stable on any machine),
streams the rows through a writer with a pinned layout (zstd, row groups of
1,048,576 rows, a parquet page index, `__row_order` last), writes to a unique
temp name and replaces the final file atomically, all under the project's write
lock, and returns the content digest of the file it wrote, read back. A create
runs the query twice and compares the digests; if they differ the recipe is not
reproducible, the entry still builds, and its file is **pinned**: the Cache
page's delete leaves it alone.

`ensure_materialized(project, hash)` is the one entry point that makes files
exist, and every consumer that composes or executes an entry goes through it
(the canonical read below, chaining, the Buckaroo hand-off):

1. A worthy entry whose snapshot exists is done, and no build is loaded.
2. Otherwise load the build and collect every file its `Read` nodes point at.
3. Re-create each that is missing by the rule for its class. A snapshot is made
   again by recursing on the hash in its file name. An ordered copy is made again
   from its source's clone with the reader options in the manifest, and checked
   against the digest recorded when it was first written. A clone is copied again
   from the live source while the live bytes still hash to its name.
4. If the entry is worthy, materialize it and verify the result against
   `result_digest`.

A file is cache only if this function can re-create it from files that are not
cache. Snapshots and ordered copies satisfy that and live under
`compute_cache/`, which anything may delete. Clones are data: once the live
source is edited a clone is the only copy of the bytes an entry was built from,
so nothing deletes one. When nothing can re-create a file (the clone is gone and
the live source has changed) the error names the source file.

Files are deleted only by an explicit user action, and a file is written only
because something is about to read it. The startup warm-up, the verify sweep and
a reset write and delete nothing under `compute_cache/`.

## Result digest

For worthy entries, the build records `result_digest`: `arrow-sha256:<hex>`, a
SHA-256 over the snapshot's ordered Arrow data, computed from the file read
back. It is independent of the row-group size, the codec, the writer's version,
whether a text column is `string` or `large_string`, and what a null slot holds;
it depends on every value, on which slots are null, on the order of the rows
(fixed by the canonical sort) and on the column names and types. It is the
**output** identity axis, and it has exactly one job: witnessing that a later
rematerialization reproduced the original result. It is never a staleness input
(an entry whose recompute differs is *nondeterministic*, not stale — recomputing
cannot make it fresh) and never part of the entry's name.

It exists because recipes are LLM-authored: an LLM can write `sample()` or an
impure UDF, the hash cannot see it (Part 1 §5), and the digest is the runtime
backstop that catches it. Running the query twice at create moves the catch to
the moment the entry is born.

---

# Part 3 — The lifecycle

## The write path (build)

`build_and_persist(project, code)` holds the project's write lock for the whole
build (one write at a time per project, so two builds of one entry cannot end with
the failing one deleting the winner's directory):

1. **Import the recipe** — the single moment of name resolution. During the
   import, `read_project_file` and `tallyman_read_csv` digest each source, clone
   it (CAS), write its ordered copy and record `{rel_path: digest}` and the copy's
   reader options; `tracked_expr_from_alias` resolves each alias to its current
   head, records the parent edge, and returns the parent's result (below).
2. **Check and rewrite** — reject what cannot become a sound entry (an in-memory
   read, a `.cache()` call, a raw parquet read, an assignment to `__row_order`, a
   cheap entry that drops it), classify the entry once (cheap or worthy), and add
   the canonical sort to a worthy entry.
3. **Freeze** — `build_expr` serializes the rewritten expression;
   `content_hash` = the build's name. If an entry with this hash already
   exists, stop: append the prompt, return the existing entry (idempotency).
4. **Lay down the entry** — copy the build in, make paths portable
   (`${TALLYMAN_PROJECT_ROOT}` placeholders), write `expr.py`.
5. **Execute once** — a worthy entry is materialized (Part 2, "Materialization"),
   which writes the snapshot and yields its digest, and the build records the
   digest, the reproducibility verdict and the schema read from the written file.
   A cheap entry is streamed once in full and keeps nothing (honest evaluation,
   fails fast).
6. **Record** — schema, manifest (written last, atomic).
7. **Checkpoint** — when the MCP tool returns: recipe zip, tracked pointers,
   one git commit.

The build's obligation in one line: **record everything a reader will ever
need, because the reader is forbidden from resolving anything.**

## The read path

One canonical read, used by every consumer:

```
read(project, content_hash):
    ensure_materialized(project, content_hash)     # every file the plan reads exists
    worthy entry: one bare read of its snapshot
    cheap entry:  load_expr(expanded build), rebound onto the default backend
```

- **Worthy entry:** a bare read of `compute_cache/result_cache/<content_hash>.parquet`,
  served without loading the entry's build when the file exists. If the file is
  missing, `ensure_materialized` re-runs the *frozen* build (whose reads are
  ordered copies and parents' snapshots, all made to exist first), writes the
  snapshot, and verifies it against `result_digest` before it is served.
- **Cheap entry:** execution re-runs the frozen graph, reading files that exist.
  Same rows every time, by construction.
- **The cold state is an empty `compute_cache/`:** any entry can be read after
  it is deleted, and must produce the same result as the warm read. That property
  is the standing regression test for every read-path change.

`cached_result_expr(project, hash)` is the function every in-process consumer
calls. It is `ensure_materialized` plus the read above, and it memoizes the loaded
plan (and the bare snapshot read, so a snapshot has one table name in the shared
backend) per `(project, content_hash)`. Removing the memo must change latency and
nothing else. The per-call existence check stays outside the memo, because file
existence is the one input that remains mutable.

**Chaining** (`tracked_expr_from_alias` at build time) uses the same read. A worthy
parent is a bare read of its snapshot, so the child's build holds the literal path
of that file, which contains the parent's content hash: the child's identity is a
function of its parent's. A filter over an aggregate's snapshot is therefore a
cheap entry. A cheap parent's graph is inlined. The parent's snapshot is made to
exist before the child is composed, since a child cannot be built over a file
that is missing.

The recipe-reconstruction machinery survives only as a diagnostic. An entry
whose build is missing or unloadable is a **hard error** naming the entry and
the remedy (rebuild) — there is no automatic recipe fallback, because a
warning on a background read is exactly how #163-class behavior stays
invisible (decided in `plans/ADR-006-read-path-loads-builds.md`, D6).[^recon]

## Composition: diff and beyond

To compose two entries (diff's outer join, a union, any multi-entry
expression):

1. read each entry (canonical read above) — each read makes its files exist,
   and each load minted fresh backend objects;
2. **rebind onto shared backends, one per distinct content profile** (profile
   identity = profile minus `idx`), using `replace_sources`. For today's
   catalogs every profile is the same embedded engine, so this collapses to
   one shared backend;
3. compose, dropping `__row_order` from both sides first. The result is a
   single-backend expression: it executes in-process, and `build_expr` serializes
   it into a normal single-profile build that Buckaroo's `/load_expr` accepts.

Composition of frozen builds was never the problem; backend object identity
was (#75, rediagnosed in #163). The rebind is cheap graph surgery, no data
moves.

A **promoted diff** is just an entry whose recipe pins two hashes
(`build_diff_expr(a_hash, b_hash)`) — name-free, deterministic, and built
through the ordinary write path. It contains a join, so it is worthy and is
materialized like any other. The live diff grid, which is not an entry, still hands
Buckaroo an unmaterialized join (`plans/ADR-007-tallyman-owned-materialization.md`
D10 moved that to a follow-on).

## Handing an entry to Buckaroo

Buckaroo displays: it runs queries only for summary stats, sorting and paging.
Tallyman finishes the entry's computation first (`ensure_materialized`), so no
other process runs an entry's expensive computation, writes result files or
repairs tallyman's cache, and a failure of the computation surfaces in tallyman's
process and never inside a grid query.

- A **worthy** entry's grid is handed a **view build**: a build whose whole graph
  is one bare read of the entry's snapshot, written once to a stable per-entry
  directory (Buckaroo's stat-cache keys include the build directory's path).
- A **cheap** entry's grid is handed its own expanded build, a stored plan over
  files that exist.
- Tallyman keeps no record of Buckaroo's sessions. A session id is
  `entry-<project>-<content_hash>`, posted on every open: Buckaroo skips the work
  while it holds that session with the same build directory, and creates the
  session again if it dropped it (it does after an hour without a browser). A
  klass reload posts `/reload_expr/<id>` for each entry of the project and treats
  the 404 for an id Buckaroo does not hold as "not open".
- Every `/load_expr` names `__row_order` as the row-order column, so Buckaroo can
  order its pages by it (buckaroo-data/buckaroo#974 is Buckaroo's half of that).
- After an unfaithful heal, the entry's stat cache is wiped and Buckaroo is told
  to reload the grid (`force_reload`).

## Reset

`reset_to` returns the catalog to an earlier step. It restores every tracked file
with `git reset --hard`, and reconciles the untracked entry directories through the
**bullpen**, the directory a reset moves retired files into so a reset forward can
bring them back. It leaves `compute_cache/` alone: snapshots and ordered copies are
named by content, and a file that is missing afterwards is made again and verified
like any other. It moves the source clones no surviving entry refers to into the
bullpen and never deletes them.

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

`verify_result_faithful(project, hash)` means exactly: *the snapshot on disk
still has the entry's recorded `result_digest`.* Verification runs in production,
not only in tests:

- on every heal (a snapshot `ensure_materialized` writes is checked before it is
  served);
- on demand, corpus-wide, via `catalog_scan_staleness(verify_results=True)`, which
  reads and never writes: a snapshot that is missing is reported as `absent` and
  checked at the moment it next exists.

A failure is surfaced loudly — the pin, `unfaithful_heal_digest` in the manifest
(the Cache page's delete leaves the file alone), a durable `unfaithful_heal`
record in `errors.jsonl` (the UI badge), a stat cache wipe, a forced reload of
the open Buckaroo grid, an SSE event — never only a log line. Its attribution has four classes with four
different fixes:

| class | detector | meaning | response |
|---|---|---|---|
| engine | the xorq, xorq-datafusion or pyarrow version, or the snapshot format, differs from the one recorded at build | a library upgrade changed the result | rebuild the entry; the recipe is not implicated |
| structural (#88) | recipe re-derives a different hash, sources unchanged | author-time value baked into the graph (`pd.Timestamp.now()`) | lint; rewrite recipe |
| execution (#83) | fixed graph, digest moves across runs | `sample()`, `now()`, impure UDF | lint; the file is pinned |
| lineage (#163) | a read resolved a name post-build | machinery bug | impossible by construction under this contract |

Creating a materialized entry runs its query twice, so an execution-class entry
is known from birth, and a mismatch at a later heal means something changed
underneath a reproducible entry. The lineage row is the point of the whole
design: with reads going through the build, lineage drift has no mechanism left.

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

Everything above compresses to six statements. Any change that breaks one is
wrong even if every test passes.

- **I1 — A content hash names a fixed result.** Materializing an entry yields
  bytes consistent with its build — any consumer, any cache state, any time.
  The only tolerated residual is execution nondeterminism *inside a recipe*
  (#83), which the digest detects; no violation may originate in tallyman's
  own machinery.
- **I2 — Caches affect latency, never results.** Every answer must be
  byte-identical with all caches empty. Corollaries: cache keys derive only
  from immutable inputs; cached values are reproducible from the frozen build
  alone; every read path has a cold seam (an empty `compute_cache/`); a
  self-heal is reproduce-and-verify, never manufacture. Result bytes are
  manufactured by one routine, `materialize`, which the build and every heal
  call — a read path that writes bytes some other way has become a second,
  unaudited build path.
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
  surfaced, never averaged over. A worthy entry's grid and its `/api/data` pages
  read the same file.
- **I6 — A page is a function of its request.** The same `(content_hash, sort,
  offset, limit)` returns the same rows in any process and any cache state, because
  every page is ordered by `__row_order`, which has no ties.

---

# History

This contract began as the proposed design for the #163 fix; the pre-fix
deviations and the decisions that settled the design (ADR-006 D1–D12) are in
[`plans/ADR-006-read-path-loads-builds.md`](../plans/ADR-006-read-path-loads-builds.md).
The redesign that replaced xorq's cache nodes with tallyman's own materialization,
added `__row_order` and redefined the digest is in
[`plans/ADR-007-tallyman-owned-materialization.md`](../plans/ADR-007-tallyman-owned-materialization.md),
[`plans/ADR-008-row-order-of-reads.md`](../plans/ADR-008-row-order-of-reads.md) and
[`plans/ADR-009-digest-stability.md`](../plans/ADR-009-digest-stability.md).
The wider audit of the same bug class is
[`plans/cache-soundness-audit.md`](../plans/cache-soundness-audit.md)
(#168–#172, buckaroo#955–#957) — the contract's rules apply to those axes
too.
