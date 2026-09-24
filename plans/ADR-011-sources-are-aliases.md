# ADR: A raw input is an alias, and files enter only by an explicit import

- **Status:** Accepted (2026-09-22), implemented. Stage 1 — the import path and
  the refusals (D1, D2, D3, D5, D9, D10, D12) — is PR #217, with two later
  decisions of the same day folded into D1: the snapshot is written by pyarrow
  in the pinned layout, and it is cache that `ensure_materialized` re-creates
  from the clone. Stage 2 — D6, D8 and the rewrite of every call site that
  authored a raw file read — is PR #218, stacked on it. Amends
  `plans/ADR-002-source-identity-content-hash.md` (its modes, its `sources` map
  and its reconstruction caveat go; the clone store stays),
  `plans/ADR-005-intelligent-csv-import.md` (its reader runs at import, not in
  a recipe) and `plans/ADR-008-row-order-of-reads.md` (its refusal extends to
  `read_project_file`, and the ordered copy becomes the source entry's
  snapshot). Still outstanding: `docs/architecture.md`, `docs/caching.md`,
  `docs/expression-lifecycle.md` and `docs/system-contract.md` all describe the
  deleted source axis, and are left until PR #216 lands to avoid rewriting the
  same four files twice.
  Written from Paddy's design session the same day, after a review of PR #189
  found that a child pinned to its parent by content hash is permanently stale
  and reports itself as an UNEXPLAINED orphan. The direction is his: "treat the
  orders.parquet like an alias that got updated externally", and "we want
  people pointing at aliases not hashes. aliases are what gives us the dag,
  hashes are brittle."
- **Reading decision labels:** a bare label such as "D5" in this document
  always means this ADR's own decision. Another ADR's decision is always
  written with its ADR number and a few words saying what it decides.
- **Context:** PR #189 implements ADR-007, ADR-008 and ADR-009. Reviewing it
  turned up a class of defect that no local fix addresses: staleness is
  computed on two axes, and the second one (a recorded source digest no longer
  matches the file on disk) asks a question the system cannot always answer.
  This ADR removes the axis by making raw inputs first-class catalog objects.
- **Tickets:** none filed yet. Supersedes the open design question in the #189
  review notes ("a hash-pinned child stays stale on the source axis"), and
  removes the cause of the staleness scan's digest-memo wipe
  (`source_identity.py:87-101`).
- **Affected code:** `src/tallyman_xorq/io.py` (`read_project_file`,
  `tracked_expr_from_alias`, `pinned_expr_from_alias`,
  `_note_parent_records`), `src/tallyman_xorq/source_identity.py` (`mode`,
  `digest_for`, `ensure_cas_path`, `recon_cas_path`, `gc_cas`),
  `src/tallyman_xorq/staleness.py` (axis 2, `_force_source_rehash`),
  `src/tallyman_xorq/recalc.py` (`_classify_orphan`),
  `src/tallyman_xorq/ordered_copy.py` (the copy key and its reader options),
  `src/tallyman_core/aliases.py` (a second kind of alias),
  `src/tallyman_core/manifest.py` (`sources`),
  `src/tallyman_mcp/server.py` (`catalog_load_parquet`),
  `docs/system-contract.md`.
- **Related ADRs:** `plans/ADR-002-source-identity-content-hash.md` (the `.cas`
  clone store this builds on; its `off` and `salt` modes go, and its `sources`
  map narrows to a retention record),
  `plans/ADR-005-intelligent-csv-import.md` (reader options and the suggestion
  contract, which move to import time),
  `plans/ADR-008-row-order-of-reads.md` (its D2 and D7 — every source enters
  through an ordered copy, a raw read is a build error — are the precedent this
  extends to `read_project_file` itself),
  `plans/ADR-007-tallyman-owned-materialization.md` (D13, a file is cache only
  if it can be re-created; a source version's snapshot passes that test, because
  the clone of the imported bytes re-creates it).
- **Evidence:** the probes in the 2026-09-22 session, reproduced under Problem.

## Terms

- **Source alias:** an alias whose versions are raw input datasets rather than
  computations. `orders` v1, v2, v3.
- **Source entry:** one version of a source alias, stored as an ordinary entry
  with a content hash, a recipe and a manifest.
- **The arena:** the files tallyman owns — the clone store under `data/.cas/`
  and the ordered copies under `compute_cache/`. A file outside the arena is
  never read by a build.
- **Import:** the explicit act of copying a file into the arena and pointing a
  source alias at the resulting entry.
- **Provenance path:** the outside path a version was imported from, recorded
  on the source entry and never read again.

## Problem

Staleness has two axes (`staleness.py`). Axis 1 asks whether a followed alias
has moved; a `follow=False` parent is never stale on it, because the recipe
asked for that exact revision. Axis 2 asks whether a recorded source digest
still matches the file on disk, and it makes no such distinction — it cannot,
because `manifest.sources` does not record how the entry came to depend on the
file.

That map is written as a retention record. `io._note_parent_records`
(`io.py:537-559`) folds a parent's source digests into its child so that
`gc_cas` keeps the clones alive for as long as any live entry needs them. The
scan then reads the same map as a freshness record. One field, two meanings.

Probed on this branch, with a child pinned to its parent by hash:

```
parent 8bb2a4661c1a  worthy=True   sources={orders.parquet: 2d20a090}
child  1fba1bf46640  worthy=False  parents=[{hash: 8bb2a466, ref: 8bb2a466, follow: False}]
                                   sources={orders.parquet: 2d20a090}

# orders.parquet is edited
scan:    1fba1bf46640 stale=True  (source, orders.parquet, 2d20a090 → aa0263ca)
recalc:  8bb2a4661c1a rebuilt → 667e15b5ee07
         1fba1bf46640 noop
scan:    1fba1bf46640 stale=True  ... unchanged, permanently
orphan:  "UNEXPLAINED stale entry — staleness with no recorded recalc error; file a bug."
```

The child's data is correct: it pinned that revision and still serves it. But
the recipe replays to the same content hash, `build_and_persist` early-returns
on the existing entry dir (`build.py:514`), the manifest keeps the old digest,
and no action a user can take clears the flag. `_is_self_ref` (`recalc.py:286`)
only excuses an alias-axis reason, so every project load prints "file a bug" —
which is the signal `tests/test_auto_recalc.py:352` exists to keep trustworthy.
`catalog_recalc()` with no roots defaults to every directly-stale entry
(`server.py:1176`), so the child becomes a permanent recalc root.

Three further defects share the cause:

- The scan forces a rehash of every source (`_force_source_rehash`), and the
  forced path skips loading the memo and then overwrites `source_digests.json`
  with a single entry (`source_identity.py:87-101`), so the next build re-hashes
  every source in the project.
- `ensure_cas_path` (`source_identity.py:133-145`) copies the live file to a
  clone named by a digest computed before the copy and never re-digests what it
  wrote. A file edited mid-copy produces a clone whose name lies about its
  content.
- `recon_cas_path`'s last branch (`source_identity.py:173-180`) serves the live
  file when the clone is gone and the bytes have drifted, logging a warning.
  An entry then returns rows that its recorded digest does not describe.

Underneath all four is one assumption: that a file the user owns, at a path
tallyman does not control, is a legitimate build input.

## Governing rule

**A recipe names aliases. A build reads only files tallyman owns.**

Everything below follows from those two sentences.

## Decisions

### D1. A raw input is an alias whose versions are entries

Importing a file mints an entry and points a source alias at it. The entry is
ordinary: a content hash, a recipe, a manifest, an ordered copy carrying
`__row_order`. The alias lives in the same `aliases.jsonl` as catalog aliases,
with the same head-plus-history shape, so `orders-v2` resolves through the
existing `VERSION_REF_RE` and `resolve_version_ref` with no new syntax.

A source alias is a distinct kind. `catalog_revise` is refused on one (there is
no recipe to revise), as is promoting a diff onto it. Rename and unalias behave
as they do for catalog aliases. The kind is recorded in the alias store so
`catalog_list` can separate inputs from computations.

Consequence: a child of a source records `{hash: <version's entry hash>, ref:
"orders", follow: True}` — the same edge shape as any catalog parent. The DAG
gains its roots. Source versions become visible in the UI, diffable against
each other, and subject to the same reset semantics as everything else.

**A source entry is worthy, and its snapshot is the copy.** The ordered copy
stops being a thing of its own: `compute_cache/ordered_sources/` merges into
`compute_cache/result_cache/`, and the import writes one parquet file, named by
the source entry's content hash, carrying `__row_order` like every other
snapshot. `manifest.ordered_copies` and the copy key
(`md5(digest ǀ reader signature)`) both go, because the entry hash now names
the file and the reader options are recorded on the entry that used them (D12).

That snapshot is **cache**, in the sense of ADR-007 D13 — a file is cache if
`ensure_materialized` can re-create it. The import keeps the bytes it was given
in the clone store (`data/.cas/<digest><suffix>`, open question 1) and records
the reader options on the entry (D12), which is everything needed to write the
file again, so a deleted source snapshot is re-created from the clone and
checked against the recorded `result_digest` exactly as any other re-created
snapshot is. The Cache page lists it unpinned and its delete takes it. The
outside file has nothing to do with this: it may be gone, and the re-creation
never looks at it.

Only when the clone is gone as well are the rows unrecoverable. Then the
snapshot is the last copy, so it is pinned, the delete answers 409, and a read
of an entry whose snapshot is already gone fails with an error naming the
missing clone and the re-import that repairs it. A source version is therefore
the one kind of entry whose "cache or data" answer depends on a second file
being present, which is the price of not storing the bytes three times.

**One set of bytes, read one way, is one source version under one alias.**
Importing bytes that another alias of the project already holds, at any version,
is an error naming that alias and version. The likely way to get there is not
knowing the bytes are already in the project, and letting two aliases share the
entry meant one entry directory carrying one alias's name in its provenance and
recipe while another alias pointed at it. (This document first said two such
imports share one entry. PR #219 reversed that after the #217/#218 review found
the second import rewrote the first alias's manifest, and a failure part-way
deleted its entry.)

A second name for a source is a catalog entry whose recipe reads it:
`catalog_create("orders_eu", "... expr = tracked_expr_from_alias('orders')")`.
Nothing has to be added to force the hashes apart, because a source entry's hash
is an md5 of its bytes and a recipe's is xorq's hash of the expression, and the
new entry follows `orders`, so a re-import advances it. The rule keys on the entry
hash, so a CSV read two ways is still two imports under two aliases (D12), and
it is per project: two projects importing one file each hold their own entry,
snapshot and clone under the same hash.

### D2. Files enter only by an explicit import

`read_project_file` is refused in an authored recipe, with the same treatment
ADR-008 D7 gives a raw `xo.deferred_read_parquet`: a build error naming the
import call to use instead. It survives only inside the recipe the importer
generates for a source entry.

After the import, the outside path is provenance. It is recorded on the source
entry and never read again. Deleting, moving or editing the original file has
no effect on any build.

### D3. One function, and its full case table

```python
update_and_depend(outside_path, alias, pinned_version=None, **reader_options)
```

The official, and only, way to advance a source alias:

| state | behaviour |
|---|---|
| alias absent | import, mint v1, return v1 |
| `pinned_version=None`, bytes differ from head | mint the next version, return it |
| `pinned_version=None`, bytes equal head | no-op, return head |
| `pinned_version=N` exists, digest matches | no-op, return vN |
| `pinned_version=N` exists, digest differs | error: the file is not the version you claimed |
| `pinned_version` = head+1, bytes differ | mint it, return it |
| `pinned_version` beyond head+1 | error: versions cannot be skipped |
| `pinned_version=N` < head, digest matches | return vN; the head does not move |
| bytes match a version older than the head | error: see D11 |

**Considered and not shipped: `import_once_and_depend(outside_path, alias)`.**
An idempotent form for a standalone script that wants the same data wherever it
runs: import and mint v1 if the alias is absent, and otherwise return v1
without reading `outside_path` at all. It is recorded here because the design
session settled its semantics and it may come back when scripts outside the
catalog need to carry their own data. It is not in the shipped API, because D4
keeps imports out of recipes and nothing else has asked for it. If it returns,
note that it pins v1 forever by design, which is what makes it reproducible and
also what keeps it out of recipes.

### D4. A recipe never imports

Import is a step of its own: the MCP tool of D10, or a script the user runs. A
recipe reads `tracked_expr_from_alias("orders")`.

The reason is the shape of the edge. An import call inside a recipe would have
to resolve to a fixed version to stay reproducible, so every recipe carrying its
own import would record `follow=False` and nothing in the catalog would ever
pick up new data. Keeping imports out of recipes makes following the default and
pinning the exception.

### D5. A bare content hash is refused in a recipe

`pinned_expr_from_alias` accepts `"<alias>-v<N>"` only. A bare content hash gets
the error #166 already gives for a bare alias, naming the version reference to
use instead.

Every parent edge then names an alias, followed or pinned at a version, and no
opaque hash appears in a recipe or in `manifest.parents`. An entry with no alias
— one built by `catalog_run` — must be named before anything can build on it.

This alone removes the defect under Problem: the child that pinned
`8bb2a4661c1a` could not have been written.

### D6. Staleness has one axis

Delete axis 2. An entry is stale when a followed alias has moved, and that is
the only reason.

`manifest.sources` goes with it, rather than merely narrowing. It exists as the
retention closure `gc_cas` walks, and once every input is an entry the closure
is the DAG: a source version's file is alive exactly while its entry is alive,
which the parent edges already record. `manifest.ordered_copies` goes for the
same reason (D1 makes the copy a snapshot), and `_note_parent_records`
(`io.py:537`) — the function that folds a parent's sources and copies into its
child, and so the immediate cause of the defect under Problem — is deleted
entirely.

Deleted with them: `_force_source_rehash`, `source_digests.json`, `digest_for`'s
stat memo, the `unknown` verdict for a removed source file, and the source-axis
branch of `_classify_orphan`.

### D7. An import is a catalog operation

`update_and_depend` minting a version takes a checkpoint, emits the same events
a revise does, and triggers auto-recalc on the same per-project switch. A source
advancing is indistinguishable downstream from an alias being revised, so
`followers_of`, `descendant_cone` and the recalc walk work unchanged.

A `reset_to` rewinds source aliases along with everything else, because
`aliases.jsonl` is tracked in the catalog repo. The clones of rewound versions
stay in `.cas` or the bullpen, so a reset forward restores them.

### D8. Source identity has one mode

`TALLYMAN_SOURCE_IDENTITY` goes. Import always digests and always clones; there
is no configuration under which a raw input is unversioned. ADR-002's `off` and
`salt` modes are deleted along with `salted_hash`.

### D9. Ingest verifies what it wrote

`ensure_cas_path` digests the clone after writing and fails if it does not match
the name. `recon_cas_path`'s live-bytes fallback becomes an error: a version
tallyman promised and then lost is a failure, not a downgrade to whatever is on
disk now.

### D10. `catalog_load_parquet` is replaced

The new MCP tool is `catalog_import_source(outside_path, alias,
pinned_version=None, **reader_options)`, mapping to `update_and_depend`. The old
tool's semantics change too much to keep the name: today re-running it with an
existing alias is an error, and under this ADR it mints the next version.

### D11. Version history is append-only and monotonic

An import whose bytes match a version older than the head is an error, naming
`reset_to` as the way back. Two version numbers never denote the same bytes, and
a version number never moves backwards while history is intact.

The case is unlikely — it takes a source file that was edited and then restored
exactly — and the error keeps the alternative (a v3 whose content equals v1's,
or a head that jumps backwards) out of the model.

### D12. Reader options are fixed at import

A CSV's delimiter, schema overrides and inference settings are named once, in
the import call, and recorded on the source entry. Two recipes cannot read one
file two ways; import it twice under two aliases.

This removes the hash fork found in the #215 review, where a function-valued
reader option's `repr` carries a memory address and re-derives a new copy key on
every build. Options are evaluated once, at import, and stored.

## Testing

- A child of a source alias is stale after an import advances it, and clear
  after recalc. The permanently-stale case of Problem cannot be constructed,
  because D5 refuses the recipe.
- `import_once_and_depend` returns v1 after the alias has advanced to v3, and
  does not read the outside file.
- Every row of D3's `update_and_depend` table, including the three errors.
- An authored recipe calling `read_project_file` fails to build, and the error
  names the import call.
- An authored recipe passing a bare content hash to `pinned_expr_from_alias`
  fails, and the error names `<alias>-v<N>`.
- A source alias refuses `catalog_revise` and refuses a promoted diff.
- `reset_to` rewinds a source alias to v1 and a reset forward restores v2 from
  the bullpen.
- Editing the outside file after import changes nothing: the same build, the
  same hash, the same rows.
- Deleting the outside file after import changes nothing.
- A clone whose bytes do not match its name fails the import (D9), and a
  missing clone with a drifted source raises rather than serving live bytes.
- A deleted source snapshot is re-created from the clone, byte for byte, with
  the outside file gone; a child reads through it; the Cache page lists the row
  unpinned and its delete is taken. With the clone gone too the row is pinned,
  the delete answers 409, and a read raises an error naming the clone and the
  re-import.
- A source snapshot is written by pyarrow: format version 2.6, a page index,
  row groups of `ORDERED_COPY_ROW_GROUP_ROWS`, the same as a computed snapshot's
  in a project holding both. A parquet import touches no polars and keeps the
  file's types; a CSV import never collects the frame.
- One scan of a project with N sources performs zero digests.

## Consequences

- Every entry's hash changes, because a source entry's hash now stands between
  a recipe and its data. The corpus is rebuilt once, as it is for ADR-007 D9.
  Nothing is migrated: per the project rule, existing catalogs are rebuilt and
  existing recipes are rewritten. In this repo that is 309 `read_project_file`
  references across 55 test files, plus nine `src/` modules — the largest
  single piece of work in the ADR, and mechanical.
- The staleness scan stops touching the filesystem outside the arena, so its
  cost becomes a read of `aliases.jsonl` and the manifests. The digest-memo
  defect disappears rather than being fixed.
- A source's history becomes inspectable: which versions existed, when each was
  imported, what each was imported from, and which entries were built on each.
  Diffing v1 against v2 uses the existing diff, since both are entries.
- Data no longer arrives by being dropped in `data/`. That is a real loss of
  convenience, and the import step is the price of the model.
- `docs/architecture.md`, `docs/caching.md`, `docs/expression-lifecycle.md` and
  `docs/system-contract.md` all describe the source axis and need rewriting.
- ADR-002 is narrowed rather than superseded: its clone store survives, its
  modes and its `sources`-as-freshness reading do not.

## Implementation notes

**Stage 1 (PR #217, 2026-09-22): the import path and the refusals — D1, D2, D3,
D5, D9, D10, D12.** What the code does that this document did not say:

- **A source entry's content hash is `md5("source|<digest>|<reader signature>")`,
  truncated to xorq's 12 hex.** It cannot come from `build_expr`, because the
  generated recipe reads the snapshot and the snapshot is named by the hash. The
  bytes and the reader options are the whole identity (`source_import.py`).
- **The generated recipe still calls `read_project_file`**, and a contextvar
  (`_SOURCE_ENTRY`) is what makes that call legal and resolves it to the entry's
  own snapshot. `result_cache._recipe_expr` sets the same contextvar when it
  reconstructs a source entry. The path in the recipe is provenance; nothing
  opens it.
- **pyarrow writes the snapshot; polars only parses a CSV.** `result_cache/`
  holds one shape of parquet file, so the import writes through the settings
  every computed snapshot uses (`materialize._PARQUET_OPTIONS`, via
  `materialize.write_pinned_parquet`), in row groups of
  `ORDERED_COPY_ROW_GROUP_ROWS`. A parquet source needs no parser: pyarrow reads
  it with `ParquetFile.iter_batches`, which also means it keeps the types the
  file has (polars rewrote a `date32` as a timestamp, a `time32[ms]` as a
  `time64[ns]` and a map as a list of structs, #197). A CSV is parsed by polars,
  which is the only reader that holds the file's row order and applies the schema
  DSL and inference ladder of ADR-005, and its batches go to the same writer
  through `collect_batches`, so nothing collects the whole frame.
- **A newer polars would not have helped.** Checked 2026-09-22: 1.40.1 is
  installed, 1.44.2 is the latest on PyPI, and neither exposes the parquet
  format version or a page-index option — `sink_parquet` writes format 1.0 and
  the request is pola-rs/polars#12752, still open. `use_pyarrow=True` is just
  pyarrow doing the write. So there is no polars upgrade that reaches the pinned
  layout, and writing the file twice to re-encode it was rejected.
- **The pinned layout costs size on high-cardinality numeric columns.** On the
  1.5M-row `tests/big_parquet.py` fixture (`id` sequential int64, `g` 200
  values, `v` random float) the snapshot goes from 16,188,593 bytes (polars) to
  25,495,419 (pyarrow), +57%, with the same 13 row groups of 122,880 rows.
  Measured cause: pyarrow dictionary-encodes by default, one 122,880-row chunk
  of int64 is ~983 KB, just under its 1 MB `dictionary_pagesize_limit`, so the
  dictionary never overflows to PLAIN and the file carries both. The same data
  is 16,205,749 bytes with `use_dictionary=False`, and 17,827,724 with
  1,048,576-row groups (where the limit does kick in). That is a question about
  `_PARQUET_OPTIONS` and about whether the two row-group sizes should be one,
  which is ADR-009's to answer for every snapshot at once; nothing here writes a
  source snapshot differently from a computed one to avoid it. Also measured:
  the page index is present either way — polars writes one without being asked —
  so the format version and the encoder are what actually differed.
- **`SNAPSHOT_FORMAT_VERSION` stays 1**, as it did for the same change to the
  ordered copies in #215, even though the bytes of a source snapshot do change.
  A source entry imported before this change records the digest of the
  polars-written file, so re-creating its snapshot now writes different bytes
  and the heal is recorded as unfaithful, attributed to the recipe rather than to
  the format. Bumping the version would attribute that one case correctly and
  would misattribute every computed entry, whose layout did not change; per the
  project rule the corpus is rebuilt instead, so there is no such entry.
- **Provenance lives in one manifest field**, `manifest.provenance`
  (`{alias, version, path, digest, suffix, reader, imported_at}`), and its
  presence is what makes an entry a source entry. `manifest.sources` stays empty
  for one, so `catalog_state._live_source_digests` had to learn to keep a clone
  alive from `provenance.digest` as well — otherwise a reset retires the only
  copy of the imported bytes.
- **`entry_staleness` skips axis 2 for a source entry** rather than reporting it
  unknown. A small piece of D6, forced: every source entry would otherwise carry
  a permanent "source axis unknown".
- **`data/` stops being special.** An import takes any path. A relative path
  resolves against the working directory, and the replay CLI expands
  `${TALLYMAN_PROJECT_ROOT}` in storyboard arguments so `demo/storyboard.json`
  can name a file shipped with the project.
- **Open question 1 is answered "keep both".** The imported bytes stay in
  `data/.cas/<digest><suffix>` beside the ordered snapshot, because ADR-005's
  suggestion-and-retry contract has to re-read the file as imported and the
  outside path may be gone. Every import therefore stores the data twice, and
  the second copy now earns its keep twice over: it is also what makes the
  snapshot cache rather than data (D1). Dropping the clone would not halve the
  storage, it would move the same bytes into the pinned column.
- **`tallyman_read_csv` is refused alongside `read_project_file`.** D2 names only
  the latter, but both open a file the catalog does not own, and leaving the CSV
  reader open would be a hole in the rule.
- A source entry keeps a real `xorq_build/`, so `load_entry_expr`, the diff and
  the viewer treat it as an ordinary entry. Nothing reads it on the normal path:
  a worthy entry whose snapshot exists is served by a bare read of that file.

**Deliberate debt, now paid.** `TALLYMAN_LEGACY_FILE_READS=1` disabled D2's
refusal for the whole suite so stage 1 could land before the call-site rewrite.
Stage 2 deletes the variable, the `tests/conftest.py` line that set it and the
branch in `io.py` that read it. There is no way to author a raw file read.

**Stage 2 (PR #218, 2026-09-23): D6, D8 and the call-site rewrite.** What the
code does that this document did not say:

- **`manifest.sources` is deleted, not narrowed.** D6 says the field reverts to
  being the retention closure; in the event it has no readers left at all, since
  a source version is an entry and the closure is the DAG. `dependents.sources_of`
  goes with it, and `io._note_parent_records` — the function whose folding of a
  parent's digests into its child is the direct cause of the defect under
  Problem — is deleted outright.
- **`StaleReason.axis` survives as a field that is always `"alias"`.** Deleting
  it would change the shape of every staleness reason in the API and the UI for
  no gain; a one-value field reads the same and says the axis is no longer a
  choice.
- **The fixture split is `orders_parquet` and `orders_src`.** The first is the
  file, for a test about the bytes; the second imports it and returns the alias,
  for a test that needs a recipe. The alias is `orders_src` rather than `orders`
  because several tests already create a catalog alias called `orders`, and a
  name is one kind or the other and never both.
- **An import must not depend on which project is active.** `update_and_depend(path,
  alias, project=X)` failed whenever `X` was not the active project: the generated
  recipe's `read_project_file` resolved the ambient project while `_SOURCE_ENTRY`
  named `X`, so the importer's own recipe hit the refusal written for authored
  recipes. The read follows the entry being minted. Every call site passed before
  the rewrite because the `project` fixture also activates its project; the cache
  lab, which warms xorq in a project of its own without activating it, is what
  exposed it.
- **Two test expectations changed because the behaviour is now right**, not to
  keep them green. The page-load profiler measures three entries where it
  measured two, because an imported source version is an entry. And the reset
  round-trip has to import its second file *after* the first checkpoint: import
  it before, and its source entry survives the reset back, so no clone is left
  referenced only by a retired entry and the case ADR-007 D13 is about stops
  being exercised at all. Arranged correctly, that test is the proof that a
  backward reset parks a clone in the bullpen rather than unlinking it — the
  failure mode that deleting `manifest.sources` could otherwise have caused
  silently.

## Open questions

1. ~~**Does a source version keep its raw bytes as well as its snapshot?**~~
   **Answered 2026-09-22: keep both.** A CSV imported with the wrong schema
   would otherwise be fixable only from the outside file, which may be gone,
   and ADR-005's suggestion-and-retry contract assumes the bytes can be
   re-read. Keeping the clone also decides D1's other half: the snapshot is
   re-creatable from it, so it is cache in the sense of ADR-007 D13 rather than
   irreplaceable data. Every import stores the data twice; the clone is
   copy-on-write where the filesystem offers it, so the initial cost is near
   zero, but the snapshot is a real second materialization. See the
   implementation notes for the measured sizes.
2. **Directories and multi-file datasets.** A dataset that arrives as
   `orders/part-0000.parquet`, `part-0001.parquet`, … rather than one file:
   does `update_and_depend("~/exports/orders/", "orders")` import the directory
   as one version, concatenating the parts in a defined order, or is import
   strictly one file to one alias? The snapshot is one parquet either way, so
   this is a question about the reader and about how part order is fixed, not
   about the store. Defer until a real dataset needs it; refuse a directory
   with a clear error until then.
