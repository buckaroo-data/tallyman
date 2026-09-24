# Tallyman: primitives and the system contract

- **Status:** Normative, implemented. Describes the system as built; where
  code or the descriptive docs disagree with it, the disagreement is a bug.
  The known disagreements, each with its open issue, are listed under
  [Known deviations](#known-deviations) at the end.
  Design decisions and history: `plans/ADR-006-read-path-loads-builds.md`
  (the read path, still in force for what the later ADRs did not change) and
  `plans/ADR-007-tallyman-owned-materialization.md`,
  `plans/ADR-008-row-order-of-reads.md` and
  `plans/ADR-009-digest-stability.md` (tallyman writes its own result files,
  every file carries `__row_order`, and the digest is a content digest),
  accepted on 2026-09-22 and implemented in #189.
  `plans/ADR-010-immutable-store-one-owner.md`, a later proposal to replace
  those three, was rejected the same day.
  `plans/ADR-011-sources-are-aliases.md` (a raw input is a source alias whose
  versions are entries, a file enters only by an explicit import, recipes name
  aliases and never bare hashes, and staleness has one axis), accepted the same
  day and implemented in #217, #218 and #219.
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
distinct backend content profile (ADR-006 D3, rebind composition onto the
default backend with a one-group guard).

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
  does (Part 2, "Project, imports and source entries").

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
    change is handled explicitly instead — new data is imported as a new
    version of a source alias, the manifest records which version of each
    parent an entry was built on, the staleness scan compares that record with
    the alias heads, recalc mints new entries and advances aliases, and old
    entries keep serving their old bytes forever. A key whose job is to move
    when data moves has nothing to hang that on. Bolting it under tallyman's
    caches would also collide with identity mechanically: the expression hash
    is hardwired to path-only normalization (measured in the source-identity
    ADR), so mtime-keyed caches would move while entry hashes stood still —
    fresh bytes under an old name, the #163 failure shape. Content-named files
    serve both needs with one mechanism: the hash and every cache key move
    exactly when content moves, and an import is the one event that moves
    them.

---

# Part 2 — The tallyman primitives

## Project, imports and source entries

A **project** is a directory (`~/.tallyman-notebooks/projects/<name>/`) holding
the catalog (`artifacts/catalog/`, a git repo) and the clone store
(`data/.cas/`).

**A recipe names aliases. A build reads only files tallyman owns.** A data file
the user has is mutable and outside tallyman's control, so no build ever reads
one. It enters the catalog by one explicit act, an **import**
(`catalog_import_source`, `source_import.update_and_depend`), which:

1. digests the file (md5) and clones its bytes, copy-on-write where the
   filesystem offers it, to `data/.cas/<digest><suffix>` (the **clone**),
   digesting the clone again and refusing it if it does not match its name;
2. writes one parquet file of its rows, in file order plus a last `__row_order`
   column, `0..N-1`, to `compute_cache/result_cache/<content_hash>.parquet`:
   pyarrow copies a parquet file, keeping its types, and polars parses a CSV
   under the schema and `scan_csv` options the call names;
3. writes an entry, the **source entry**, with a generated recipe, a frozen
   build, a schema and a manifest whose `provenance` records the outside path,
   the digest, the reader options and the name it was imported as;
4. appends that entry to a **source alias**, as its next version.

The outside path is provenance from then on and is never read again: editing,
moving or deleting the file changes no build. To bring in new data, import the
file again under the same alias. Different bytes mint the next version, and the
entries that follow the alias go stale exactly as after a revise. The same bytes
are a no-op.

A source entry's content hash is `md5("source|<digest>|<reader signature>")`,
truncated to 12 hex characters: the bytes and the reader options, and nothing
else. That is the answer to xorq's path-only hashing. Every file a recipe's
expression reads is a snapshot, named by the content hash of the entry it holds,
so every xorq-level key (the expression hash, and the name of every file
tallyman writes) is content-honest, and the chain of names ends at hashes of
bytes. The reader options are fixed at import (a CSV read two ways is two
imports under two aliases), and they must be plain values that the entry can
record: a callable option is refused.

The import's outcomes form a table (ADR-011 D3). With no `pinned_version`: a new
alias mints v1, bytes that differ from the head mint the next version, and bytes
equal to the head are a no-op. With `pinned_version=N`: the file must be version
N (a no-op) or, as N = head + 1, new bytes; anything else is an error, and
versions cannot be skipped. History is append-only, so bytes equal to a version
older than the head are refused, naming `reset_to` as the way back. And **one set
of bytes, read one way, is one version under one alias**: bytes another alias of
the project already holds are refused, naming that alias; a second name for them
is a catalog entry whose recipe reads `tracked_expr_from_alias("<that alias>")`.
The rule is per project: two projects importing one file each hold their own
entry, snapshot and clone under the same hash.

## Recipe

A **recipe** (`expr.py`) is the LLM-authored Python that defines a
computation. It must bind a variable `expr`, built from:

- `tracked_expr_from_alias("trips")` — build on another entry, named by alias
  (a source alias or a catalog alias), recording a `follow=True` parent edge:
  this expression depends on the parent alias, and when that alias advances,
  recalc mints a new version of this expression;
- `pinned_expr_from_alias("trips-v3")` — same, but `follow=False`: recalc never
  touches this expression when the parent moves. A pin names an exact version
  with an explicit version reference. A bare alias is rejected as ambiguous (it
  would silently pin whatever the head happened to be when the recipe was built,
  #166), and so is a bare content hash (ADR-011 D5), so every parent edge names
  an alias and no opaque hash appears in a recipe. An entry with no alias has to
  be named before anything can build on it.

A recipe never opens a file. `read_project_file`, `tallyman_read_csv`,
`xo.deferred_read_csv`, and `xo.deferred_read_parquet` of a file outside
`compute_cache/` are build errors, each naming the import to use. The one recipe
that calls `read_project_file` is the one the importer generates for a source
entry, where a context variable resolves the call to that entry's own snapshot;
the path it names is provenance.

The defining property of a recipe: **it binds by name.** "trips" means whatever
entry that alias points at right now, and for a source alias that is whichever
version of the file was imported last. A recipe therefore has a different
meaning at different moments. That is exactly what you want when *authoring* — and
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
| binds inputs by | name (aliases) | value (a worthy parent's snapshot path, a source version's included; a cheap parent's graph inlined) |
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

A source entry has both representations too, and the rule holds for it
trivially: its recipe is generated by the import and never re-run to make
anything, and its build is one read of its own snapshot.

## Content hash

An entry's `content_hash` is xorq's expression hash (Part 1 §3), taken at
build time. Two details determine what it covers:

- the expression hashed is the one after the rewrite (below), which adds the
  canonical sort to a worthy entry and adds no cache node to anything;
- every file read in the graph is a worthy parent's snapshot, named by the
  parent's content hash, so a child's hash is a function of its parent's.

A source entry's hash is the exception, and the base of every chain: it is
computed from the imported bytes and the reader options (Part 2, "Project,
imports and source entries") rather than taken from xorq, because its generated
recipe reads the snapshot that the hash names.

So the hash names "this computation over these exact input bytes."
That one property makes builds idempotent and history append-only. Rebuild
the same computation over unchanged inputs and you land on the existing
entry: the build recognizes the hash and stops. Change anything that alters
the computation or its inputs — the recipe's logic, a parent's identity, and
through a new source version the imported bytes — and a new entry forks under a
new hash, while every existing entry keeps its name and its meaning.

Two limits are deliberate. The hash cannot see execution behavior (Part 1
§5): a recipe calling `sample()` or `now()` hashes identically run to run,
which is the gap `result_digest` (below) exists to police. And it makes no
attempt at cross-machine portability: absolute path prefixes participate in a
computed entry's hash. (A source entry's hash has no path in it.)

## Manifest: the closure record

`manifest.json` records everything about the build moment that the build
itself doesn't state, so that no later operation ever needs to resolve a name:

| field | meaning |
|---|---|
| `content_hash` | the entry's identity |
| `parents` | `[{hash, ref, follow}]` — each alias reference, **resolved to the exact hash it meant at build time** |
| `provenance` | a source entry only: `{alias, version, path, digest, suffix, reader, imported_at}` — the name it was imported as, the outside path (never read again), the digest of the bytes, and the reader options that let its snapshot be made again from the clone; its presence is what makes an entry a source entry |
| `cache_worthy`, `cache_worthy_why`, `cache_bytes` | whether the entry is materialized, decided once at build, and the evidence |
| `result_digest` | `arrow-sha256:` digest of the snapshot's content (worthy entries) — the output identity |
| `reproducible`, `nonreproducible_columns` | whether two runs at create gave the same digest, and the columns that differed |
| `unfaithful_heal_digest` | the digest the last unfaithful heal wrote; set, it pins the snapshot. The only field written after create |
| `snapshot_format`, `engine_versions` | the format version and the xorq, xorq-datafusion and pyarrow versions at build |
| `row_count`, `execute_seconds`, `compile_seconds`, timings | build measurements |

The manifest is the entry directory's last write, atomic: its presence is the
"this entry is complete" sentinel. After that only an unfaithful heal rewrites
it, to record `unfaithful_heal_digest`, by an atomic replace under the project
lock. The recipe zip, written by the first checkpoint, keeps the manifest as it
was at create. An entry directory without a manifest is treated as absent by the
entry list, the checkpoint, recalc and the build, which builds it again. A page
read still serves such a directory, with the snapshot's existence standing in
for the missing `cache_worthy` (#90, #204).

## Alias

An **alias** is a mutable name: `{alias, latest, history, kind}` in a
git-tracked file. `latest` is the head; `history` is every hash it has pointed
at (V1…Vn, oldest first). Revising an alias mints a new entry, advances
`latest`, appends to `history`. Old entries remain, immutable, as the version
history.

An alias has a **kind**. A **catalog alias** names computations and advances by
revise, promote and recalc. A **source alias** names imported data and advances
only by an import: there is no recipe to revise and nothing to promote onto it,
and every surface that offers those refuses a source alias before building
anything. A name is one kind or the other, never both. **An alias's kind
matches its entries' kind**: `set_alias` never points a catalog alias at a
source entry or a source alias at a computed one, whichever route asks.
A source version is named by the alias that holds it now: `provenance` keeps
the name it was imported as, and a rename or an unalias does not rewrite it, so
a message that names a version or advises an import asks the alias store.

**Where alias resolution is legal** — names resolve in exactly three
situations, all of them *about* choosing or minting, never about serving:

1. **Minting** (build / revise / recalc): resolve heads, record the resolved
   hashes in the new entry's manifest.
2. **Selecting** (UI, diff version arithmetic): resolve "by_hour" or "V-1" to
   a hash, *then* serve that hash.
3. **Judging** (staleness scan): compare recorded parent hashes against
   current heads — read-only, executing nothing and opening no data file.

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
visible in every table. Two writers produce it: the import (a source entry's
snapshot, numbered in the imported file's order) and `materialize` (every other
snapshot), and each overwrites an inherited one with positions in its own
file. It is what makes a page of an
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
  sort followed only by steps that keep row order (filters, limits, selections,
  column drops, drops or fills of nulls) is kept: the top-level sort leads with
  its keys, and the build fails, naming the key, if a later step dropped or
  changed it or if the sort was by an expression rather than a column. Above an
  aggregate, a join or a union the order of rows is gone, and the top-level sort
  is the tie-break alone.
- A join of two entries leaves the right side's copy under ibis's collision name,
  `__row_order_right`, and the writer drops it. Joining three entries in one
  recipe needs `.drop("__row_order")` on the right-hand inputs, and the build
  says so. (Today the writer drops any column of that name, including an
  author's, #206, and the check also refuses semi and anti join chains, which
  cannot collide, #199.)
- A diff carries no row-order column from either side. The compare grid and a
  promoted diff drop it; `full_diff`, behind the diff page's summaries and
  `catalog_diff`, does not yet (#200).

## Materialization

`materialize(project, hash)` is the one routine that writes a computed entry's
snapshot. The build calls it and so does every **heal** of one (the re-creation
of a snapshot that is missing from disk), so result bytes are manufactured in
one place. A source entry's snapshot is not a result: it is written by the
import, and made again from the clone by the same code the import used
(`source_import.rewrite_source_snapshot`), through the same pinned writer. It
runs the entry's frozen build on a **single-partition** connection (so a float
total is merged in one order and is bit-stable on any machine), streams the rows
through a writer with a pinned layout (zstd, row groups of 1,048,576 rows, a
parquet page index, `__row_order` last), writes to a unique temp name and
replaces the final file atomically, all under the project's write lock, and
returns the content digest of the file it wrote, read back. A heal replaces the
file at once. A create leaves the finished file at its temp name
(`materialize(..., publish=False)`), and the build moves it into place
(`publish_snapshot`) after the manifest is written, so a build that fails
removes only its temp file and never the file already at the path. A create runs
the query twice and compares the digests; if they differ the recipe is not
reproducible, the entry still builds, and its file is **pinned**: the Cache
page's delete leaves it alone. A snapshot changes only by an atomic replace of a
complete file.

**Pins are read from the manifest alone** (`pinned_reason`): a snapshot is pinned
when the manifest says `reproducible: false`, when it holds
`unfaithful_heal_digest`, or when it is a source entry whose clone is gone. So a
pin moves with its entry through a reset, and nothing outside the entry, such as
the error log, can lift it. A snapshot whose entry a reset retired is judged by
the manifest parked in the bullpen, and a retired source version's clone counts
as present when a reset parked it there too.

`ensure_materialized(project, hash)` is the one entry point that makes files
exist, and every consumer that composes or executes an entry goes through it
(the canonical read below, chaining, the Buckaroo hand-off):

1. A worthy entry whose snapshot exists is done, and no build is loaded.
2. Otherwise load the build and collect every file its `Read` nodes point at.
3. Re-create each that is missing. Every one is another entry's snapshot, made
   again by recursing on the hash in its file name.
4. If the entry is worthy, materialize it and verify the result against
   `result_digest`. A source entry skips steps 2 and 3: its snapshot is written
   again from its clone with the reader options in its manifest, and verified
   the same way.

Whether the entry is worthy is read from the manifest, never derived again.
(With the manifest missing, the code guesses from whether a snapshot exists,
#204.)

A file is cache only if this function can re-create it from files that are not
cache. Snapshots satisfy that, a source entry's included, and live under
`compute_cache/`, which anything may delete. Clones are data: a clone is the
only copy tallyman has of bytes it imported, so nothing deletes one (a reset
moves a clone no surviving source entry names into the bullpen). A source entry
whose clone is gone is the one case where a snapshot is the last copy of its
rows, so that snapshot is pinned; if it is deleted anyway, the read fails with
an error naming the missing clone and the import call, reader options included,
that repairs the version.

Files are deleted only by an explicit user action, and a file is written only
because something is about to read it. The startup warm-up, the verify sweep and
a reset write and delete nothing under `compute_cache/`.

## Result digest

For worthy entries, the build (for a source entry, the import) records
`result_digest`: `arrow-sha256:<hex>`, a
SHA-256 over the snapshot's ordered Arrow data, computed from the file read
back. It is independent of the row-group size, the codec, the writer's version,
whether a text column is `string` or `large_string`, and what a null slot holds;
it depends on every value, on which slots are null, on the order of the rows
(fixed by the canonical sort, or for a source entry by the file's order) and on
the column names and types. It is the
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
build (one build at a time per project, so two builds of one entry cannot end
with the failing one deleting the winner's directory):

1. **Import the recipe** — the single moment of name resolution. During the
   import, `tracked_expr_from_alias` resolves each alias to its current head,
   records the parent edge, and returns the parent's result (below), and
   `pinned_expr_from_alias` does the same for a version reference. A raw file
   read, a bare hash and a bare alias raise here.
2. **Check and rewrite** — reject what cannot become a sound entry (an in-memory
   read, a `.cache()` call, a raw parquet or CSV read, an assignment to
   `__row_order`, a cheap entry that drops it, a join chain over three entries
   that all carry it), classify the entry once (cheap or worthy), add the
   canonical sort to a worthy entry, and move `__row_order` to the last column
   of a cheap one.
3. **Freeze** — `build_expr` serializes the rewritten expression;
   `content_hash` = the build's name. If an entry with this hash already
   exists, stop: append the prompt, return the existing entry (idempotency).
4. **Lay down the entry** — copy the build in, make paths portable
   (`${TALLYMAN_PROJECT_ROOT}` placeholders), write `expr.py`.
5. **Execute** — a worthy entry is materialized (Part 2, "Materialization"),
   which runs its query twice, writes the snapshot and yields its digest, and
   the build records the digest, the reproducibility verdict and the schema
   read from the written file. The snapshot stays at its temp name. A cheap
   entry is streamed once in full and keeps nothing (honest evaluation, fails
   fast).
6. **Record** — schema, manifest (atomic, the entry directory's last write), and
   then the snapshot, moved into place from its temp name. A build that fails
   before that leaves any file already at the snapshot's path as it was.
7. **Checkpoint** — when the MCP tool returns: recipe zip, tracked pointers,
   one git commit.

The build's obligation in one line: **record everything a reader will ever
need, because the reader is forbidden from resolving anything.**

## The import path

`update_and_depend(outside_path, alias, pinned_version=None, schema=None,
**reader_options)`, behind the `catalog_import_source` tool, is the only way a
source alias advances. It fixes the reader from the file's suffix and the
options, digests the file, computes the entry hash, and then, under the project
lock, decides the case (Part 2, "Project, imports and source entries"):

- **Mint:** clone the bytes and verify the clone, write the snapshot, write the
  entry (generated `expr.py`, frozen build, schema, manifest with `provenance`,
  written last), and append it to the alias. A failure removes the entry
  directory the import created, and nothing else.
- **The version already exists** (a no-op, or a repair): the entry's recipe,
  build and manifest are the record of the import that minted it, and none of
  them is rewritten. If its snapshot is gone, the clone is restored from the
  given file when it is gone too (verified against the digest), and the
  snapshot is healed exactly as `ensure_materialized` heals it: from the clone,
  verified against `result_digest`. If a reader now parses the bytes
  differently, the healed file is served but recorded as an unfaithful heal
  (which pins it), and the manifest keeps the digest of the rows the version
  was imported with: a repair never re-records a version's rows. A directory a
  crash left without a manifest is not an entry, and is written again.
- **Refuse:** a directory, a name that is a catalog alias, a file that is not
  parquet or CSV, a CSV option that does not survive JSON, and every error row
  of the case table, each with a message saying what to do instead.

A minted version is a catalog operation like a revise: the tool records the
same events, notifies the companion, runs auto-recalc for the alias's followers
when the project enables it, and lands the import and its cascade as one
checkpoint.

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
  parents' snapshots, all made to exist first), writes the snapshot, and
  verifies it against `result_digest` before it is served. For a source entry
  it parses the clone again instead.
- **Cheap entry:** execution re-runs the frozen graph, reading files that exist.
  Same rows every time, by construction.
- **The cold state is an empty `compute_cache/`:** any entry can be read after
  it is deleted, and must produce the same result as the warm read. That property
  is the standing regression test for every read-path change. The known
  exceptions are an entry recorded as not reproducible, whose snapshot is pinned
  because it cannot be made again faithfully, and the entries built on it
  (#185, #208), and a source entry whose clone is gone, whose snapshot is the
  last copy of its rows.

`cached_result_expr(project, hash)` is the function every in-process consumer
calls. It is `ensure_materialized` plus the read above, and it memoizes the loaded
plan (and the bare snapshot read, so a snapshot has one table name in the shared
backend) per `(project, content_hash)`. Removing the memo must change latency and
nothing else. The per-call existence check stays outside the memo, because file
existence is the one input that remains mutable.

**Chaining** (`tracked_expr_from_alias` at build time) uses the same read. A
worthy parent, a source entry included, is a bare read of its snapshot, so the
child's build holds the literal path of that file, which contains the parent's
content hash: the child's identity is a function of its parent's. A filter over
an aggregate's snapshot is therefore a cheap entry. A cheap parent's graph is
inlined. The parent's snapshot is made to exist before the child is composed,
since a child cannot be built over a file that is missing.

The recipe-reconstruction machinery survives only as a diagnostic. An entry
whose build is missing or unloadable is a **hard error** naming the entry and
the remedy (rebuild) — there is no automatic recipe fallback, because a
warning on a background read is exactly how #163-class behavior stays
invisible (ADR-006 D6, a missing or unloadable build is a hard error, in
`plans/ADR-006-read-path-loads-builds.md`).[^recon]

## Composition: diff and beyond

To compose two entries (diff's outer join, a union, any multi-entry
expression):

1. read each entry (canonical read above) — each read makes its files exist,
   and each load minted fresh backend objects;
2. **rebind onto the process's default backend**, using `replace_sources`.
   Every profile in a tallyman build is the same embedded engine (profile
   identity = profile minus `idx`), and a build that spans more than one
   distinct content profile fails loudly instead of being rebound;
3. compose, dropping `__row_order` from both sides first (`full_diff` does not
   yet, #200). The result is a single-backend expression: it executes
   in-process, and `build_expr` serializes it into a normal single-profile build
   that Buckaroo's `/load_expr` accepts.

Composition of frozen builds was never the problem; backend object identity
was (#75, rediagnosed in #163). The rebind is cheap graph surgery, no data
moves.

A **promoted diff** is just an entry whose recipe pins two hashes
(`build_diff_expr(a_hash, b_hash)`) — name-free, deterministic, and built
through the ordinary write path. It contains a join, so it is worthy and is
materialized like any other. The live diff grid, which is not an entry, still
hands Buckaroo an unmaterialized join: ADR-007 D10, which would have built
every diff as an entry before showing it, was moved out of
`plans/ADR-007-tallyman-owned-materialization.md` to #188.

## Handing an entry to Buckaroo

Buckaroo displays: it runs queries only for summary stats, sorting and paging.
Tallyman finishes the entry's computation first (`ensure_materialized`), so no
other process runs an entry's expensive computation, writes result files or
repairs tallyman's cache, and a failure of the computation surfaces in tallyman's
process and never inside a grid query.

- A **worthy** entry's grid is handed a **view build**: a build whose whole graph
  is one bare read of the entry's snapshot, written once to a stable per-entry
  directory, so Buckaroo is handed the same build after a restart.
- A **cheap** entry's grid is handed its own expanded build, a stored plan over
  files that exist.
- Tallyman keeps no record of Buckaroo's sessions. A **session** (one grid's
  state in the Buckaroo process) has the id `entry-<project>-<content_hash>`,
  posted on every open: Buckaroo skips the work while it holds that session with
  the same build directory and the post carries no configuration, and creates
  the session again if it dropped it (it does after an hour without a browser).
  A klass (a project-authored stat, post-processing or display class) reload
  posts `/reload_expr/<id>` for each entry of the project and treats the 404 for
  an id Buckaroo does not hold as "not open".
- Every `/load_expr` names `__row_order` as the row-order column, so Buckaroo can
  order its pages by it. Buckaroo 0.15.6, the pinned version, ignores the hint;
  buckaroo-data/buckaroo#974 is Buckaroo's half of that.
- After an unfaithful heal, the entry's stat cache is wiped and Buckaroo is told
  to reload the grid (`force_reload`).

## Reset

`reset_to` returns the catalog to an earlier step. It restores every tracked
file with `git reset --hard`, and reconciles the untracked entry directories
through the **bullpen**, the directory a reset moves retired files into so a
reset forward can bring them back. It leaves `compute_cache/` alone: snapshots
are named by content hash, and a file that is missing afterwards is made again
and verified like any other. It moves the clones no surviving source entry names
in its `provenance` into the bullpen and never deletes them, and a reset forward
copies back the clones a restored source entry names. Source aliases rewind with
every other alias, since `aliases.jsonl` is a tracked file. An entry directory
retired when the bullpen already holds one under its name replaces the parked
copy, because the live one agrees with the snapshot on disk; one with no
manifest, left by an interrupted build, is dropped instead. So a reset forward
brings back the manifest that matches the file. The bullpen has one live reader
besides `reset_to`, the Cache page, which reads a retired entry's parked
manifest (and a retired source version's parked clone) to keep its snapshot's
pin.

## Staleness and recalc

**Staleness** is a read-only judgment with one axis: an entry is stale when a
`follow=True` parent's recorded hash no longer equals that alias's head, and
for no other reason. A pinned parent never makes its child stale. A data file
that changed outside tallyman is not a reason: until it is imported again
nothing has changed in the catalog, and the import moves a source alias, which
is the one axis. Computing staleness executes nothing, opens no data file and
changes no catalog state. Only an entry that is the current head of an alias is
actionably stale; a superseded version is reported with `live=False` (#154). A
parent alias that no longer exists is reported under `unknown_axes`.

**Recalc** is the one sanctioned re-execution of recipes. When an alias head
advances (a revise, or an import that mints a new version of a source alias),
the entries that followed it by name are directly stale;
they are the roots, and the entries that may be affected form their **cone**:
the roots and every current alias head reachable from them through recorded
parent edges, followers of followers and so on. It runs automatically after a
revise when the project enables auto-recalc (the default), or on demand.

Recalc rebuilds the cone in topological order, parents before children. For
each member it re-imports the member's *recipe* — the one situation where
name resolution is the point, since the goal is a new version against the
new heads — builds the result as an ordinary new entry, and advances the
member's alias before any of its children replay, so each child chains off
its parent's fresh head. A member whose inputs did not move, such as a child
that pins a version of its parent (`follow=False`), replays to the same hash and
is left alone. Old entries are untouched; a member that rebuilds gains a version,
and none loses one.

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
- on demand, corpus-wide, via `catalog_scan_staleness(verify_results=True)`, whose
  verify sweep reads and never writes: a snapshot that is missing is reported as
  `absent` and checked at the moment it next exists.

A failure is surfaced loudly, never only as a log line: the pin, the digest the
heal wrote recorded as `unfaithful_heal_digest` in the manifest (the Cache
page's delete leaves the file alone), a durable `unfaithful_heal` record in
`errors.jsonl` (shown in the catalog page's error banner), a stat cache wipe,
and in the companion a forced reload of the entry's Buckaroo grid and an
`unfaithful_heal` SSE event. Today the SPA has no listener for that event, the
forced reload is sent even when no grid is open (#203), and cheap entries that
read the healed snapshot are not flagged (#208). Its attribution has four
classes with four different fixes:

| class | detector | meaning | response |
|---|---|---|---|
| engine | the xorq, xorq-datafusion or pyarrow version, or the snapshot format, differs from the one recorded at build | a library upgrade changed the result | rebuild the entry; the recipe is not implicated |
| structural (#88) | recipe re-derives a different hash, inputs unchanged | author-time value baked into the graph (`pd.Timestamp.now()`) | lint; rewrite recipe |
| execution (#83) | fixed graph, digest moves across runs | `sample()`, `now()`, impure UDF | lint; the file is pinned |
| lineage (#163) | a read resolved a name post-build | machinery bug | impossible by construction under this contract |

Creating a materialized entry runs its query twice, so an execution-class entry
is known from birth, and a mismatch at a later heal means something changed
underneath a reproducible entry. The lineage row is the point of the whole
design: with reads going through the build, lineage drift has no mechanism left.

[^recon]: The subsystem the fix demoted: `_recipe_expr` re-imports an entry's
    `expr.py` to recover its expression, patching over the recipe's
    name-binding with context variables for the duration of the import.
    `_resolve_noncyclic_hash` walks a self-referencing alias back to its
    previous version to break re-import cycles (#74). A second hook,
    `_RECON_SOURCES`, once resolved each raw file read to its frozen
    `.cas/<digest>` clone (#115); ADR-011 deleted it with `manifest.sources`,
    since a recipe no longer reads files at all, and a source entry's own
    generated recipe resolves its one read to its snapshot (`_SOURCE_ENTRY`).
    Each hook re-derives a binding the frozen build already contains, which is
    why the fix retired their read-time role (and #162's proposed
    `_RECON_PARENTS` sibling was never built) rather than adding another. The
    diagnostic use stays, and is the only reason the machinery
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
  selecting, and judging code — never in materializing code. The one alias
  lookup near materializing code names a source version in the text of an
  error or a pin reason, `current_source_version`, and decides no rows.)
- **I5 — One question, one path.** Any question answerable two ways (grid vs
  API bytes, manifest row count vs live count) either shares one canonical
  path or carries an explicit check tying the two together; disagreement is
  surfaced, never averaged over. A worthy entry's grid and its `/api/data` pages
  read the same file.
- **I6 — A page is a function of its request.** The same `(content_hash, sort,
  offset, limit)` returns the same rows in any process and any cache state, because
  every page is ordered by `__row_order`, which has no ties.

---

# Known deviations

Where the code breaks a rule above today. Each is an open issue; none is a
change of the rule.

- **Worthiness from the manifest.** With the manifest missing, the snapshot's
  existence stands in for the verdict, so a worthy entry that has lost both is
  read as cheap (#204).
- **Verification reaches one entry.** An unfaithful heal of a worthy parent
  changes its cheap children's rows under their hashes without a record
  (#208), and the purity of an entry is not passed on to entries built on it,
  so a child of a non-reproducible parent is recorded as reproducible (#185).
  Both are gaps in I1 and I2.
- **Row order.** The canonical sort's tie-break leaves out nested columns
  (#205); the snapshot writer drops any column named `__row_order_right` (#206);
  the three-way join check refuses semi and anti joins (#199); `full_diff` keeps
  `__row_order` as data (#200); and Buckaroo's grid does not yet order pages by
  `__row_order` (buckaroo-data/buckaroo#974), so I6 holds for `/api/data` and
  not yet for the grid.
- **Handing an entry to Buckaroo.** Concurrent opens both post `/load_expr`, and
  a promoted diff re-runs Buckaroo's statistics on every open (#202); the
  forced reload after an unfaithful heal runs under the project lock and opens a
  session nobody asked for (#203); a klass reload posts once per entry from the
  companion's event loop (#201); Buckaroo is pointed at `artifacts/` and does
  not find the project's stats and post-processing functions (#170); the live
  diff grid is an unmaterialized join (#188).
- **One writer at a time.** The project lock blocks with no timeout (#186), two
  companion routes build on the event loop and freeze the UI while they wait
  (#190), two servers on one project are not detected (#183), and concurrent
  reads on the shared backend can fail (#118). The lock covers builds,
  materializations, checkpoints and resets only: alias, notebook, chart,
  display-config and `config.json` writes replace their file atomically without
  it, so an MCP edit and a browser edit of the same file at the same moment can
  lose one of the two (no issue filed yet).
- **Names resolve once, in the right project.** A recipe's alias readers
  (`tracked_expr_from_alias`, `pinned_expr_from_alias`) resolve the project from
  the `active_project` file, while the MCP tool builds into the session's own
  project; after another session switches projects the two differ, and the
  recipe looks its aliases up in the other project (related to #39).
- **Portability.** An expanded build does not record the project path it was
  filled in with, so a copied project reads the old location (#209).
- **Float totals.** An ungrouped float `SUM` depends on the row-group layout of
  the file it reads; the snapshot format version pins that layout, and a change
  of it is a corpus rebuild (#187).

---

# History

This contract began as the proposed design for the #163 fix; the pre-fix
deviations and the decisions that settled the design (ADR-006 D1–D12) are in
[`plans/ADR-006-read-path-loads-builds.md`](../plans/ADR-006-read-path-loads-builds.md).
The redesign that replaced xorq's cache nodes with tallyman's own materialization,
added `__row_order` and redefined the digest is in
[`plans/ADR-007-tallyman-owned-materialization.md`](../plans/ADR-007-tallyman-owned-materialization.md),
[`plans/ADR-008-row-order-of-reads.md`](../plans/ADR-008-row-order-of-reads.md) and
[`plans/ADR-009-digest-stability.md`](../plans/ADR-009-digest-stability.md),
accepted on 2026-09-22 and implemented in #189.
[`plans/ADR-010-immutable-store-one-owner.md`](../plans/ADR-010-immutable-store-one-owner.md)
proposed replacing them and was rejected.
[`plans/ADR-011-sources-are-aliases.md`](../plans/ADR-011-sources-are-aliases.md)
made a raw input a source alias whose versions are entries, which removed the
source axis of staleness, the identity modes, `manifest.sources` and the ordered
copy, and with them the defects listed here before it (#191, #197, #198, #207,
#211, and the two staleness defects that had no issue). Four deviations this
section listed were fixed on the same branch: a failed build deleted the
snapshot already at its path (#193, fixed in #222), and a reset could pair a
non-reproducible entry's older manifest with its newer snapshot, a retired
entry's snapshot lost its pin, and dismissing the error banner lifted an
unfaithful heal's pin (#194, #195 and #196, fixed in #223). The wider audit of
the same bug class is
[`plans/cache-soundness-audit.md`](../plans/cache-soundness-audit.md)
(#168–#172, buckaroo#955–#957) — the contract's rules apply to those axes
too.
