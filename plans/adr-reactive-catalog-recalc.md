# ADR: Reactive catalog — recalc & staleness over the dependency DAG

- **Status:** Research snapshot (2026-06-14). Intent fixed; several design
  questions resolved; key questions (source-identity mode, cascade/transaction
  semantics, archive overlay) still open. **No code yet** — this records where
  the design conversation landed.
- **Context:** A tallyman project has a few raw sources and many derived
  expressions. Today, edits to a raw source or updates to an upstream entry do
  not reach the dependents — they go silently stale. We want an on-demand
  "recalc" (and/or a scan-on-load that flags staleness) without giving up the
  catalog's reproducible, recoverable, git-backed nature.
- **Affected code (today):** `src/tallyman_xorq/io.py` (`from_catalog`),
  `src/tallyman_xorq/lineage.py` (`catalog_parents` / `catalog_dag`),
  `src/tallyman_xorq/source_identity.py`, `src/tallyman_core/aliases.py`,
  `src/tallyman_companion/app.py` (checkpoint middleware).
- **Related:** buckaroo-data/tallyman#48 (catalog-consistency bug, a
  prerequisite for the recovery model), `plans/adr-source-identity-content-hash.md`,
  `plans/adr-result-cache-cost-rubric.md`.
- **xorq evidence:** read at xorq 0.3.29 (commit `1f46392e`), under
  `~/xorq/python/xorq`.

## Intent

The author's framing, paraphrased: a notebook is a handful of raw sources and
many dependent expressions. Two properties make derived work go stale without
anyone noticing.

1. **Alias dependencies are by-value.** `from_catalog("A")` resolves alias A to
   its *current* hash at build time and embeds a read of that version's
   `result.parquet` (io.py:87). When A is revised, B keeps reading A's old
   version. B depends on "the value A had when B was built," not on "alias A."

2. **xorq invalidates lazily, with no propagation.** A cache key is recomputed
   only when an expression executes, by walking to its Read nodes; there is no
   "this source changed → invalidate its dependents" mechanism anywhere.
   Combined with tallyman's deliberate choice of `SnapshotStrategy` (path-only
   keys), an in-place source edit is invisible to identity.

The goal is to recompute the dependents of a changed source or an updated entry
on demand, while keeping each entry a reproducible, recoverable artifact.

**Guiding principle (stated 2026-06-14):** use xorq natively wherever possible;
patch in a tallyman-specific solution only when (1) xorq offers no way to do the
thing, or (2) xorq's way is too slow for reasonable interactive use.

## What xorq does natively

- **Cache strategies.** `ModificationTimeStrategy` keys a cache on the source's
  `(mtime, size, inode)` (`caching/strategy.py:89`, `common/utils/defer_utils.py:86`),
  so a changed file misses and recomputes. `SnapshotStrategy` keys on the path
  string only (`caching/strategy.py:49`), deliberately ignoring source content.
  tallyman's entry `content_hash` and `result_cache` use Snapshot.
- **Invalidation is pull-based, at `execute()`.** `cache.set_default` recomputes
  the key by walking to the Read nodes; there is no reverse-dependency index,
  watcher, or propagation (confirmed across `caching/`).
- **Provenance tagging.** `HashingTag` (`expr/relations.py:120`) tags a node
  with catalog provenance — `CatalogTag.SOURCE` / `TRANSFORM` plus `entry_name`
  (`catalog/bind.py:9`, `:118`, `:145`) — and is recoverable with `walk_nodes`.
  `composed_from` entry metadata records entry→entry edges (`catalog/catalog.py:1281`).
- **Catalog composition.** `Catalog.load` / `bind` / `compose` compose entries,
  but every path calls eager `load_expr()` and inlines the upstream recipe
  (`catalog/bind.py:115`), wrapping it in `RemoteTable` + `HashingTag`.
- **The catalog is git-backed.** `<project>/artifacts/catalog/` is a git repo;
  `xorq catalog add` zips builds; tallyman's `reset_to` sits on top of it.

## What tallyman does today

- `from_catalog` reads the upstream entry's materialized `result.parquet`
  (io.py:87) as an *anonymous* `deferred_read_parquet`. This keeps each entry's
  graph shallow. The reason on record: long xorq expression graphs are expensive
  just to **build/tokenize**, independent of execution, so the parquet read is a
  deliberate truncation boundary. The cost: it severs xorq's native dependency
  graph — xorq sees a parquet file, not "entry A."
- Because the edge is invisible to xorq, tallyman re-derives the cross-entry DAG
  by regex-matching `result.parquet` paths inside each `expr.yaml`
  (`catalog_parents`, lineage.py:53–64).
- Because Snapshot keys are content-blind, tallyman re-adds source-content
  sensitivity in `source_identity` (modes `off` / `salt` / `cas`), `off` by
  default.

## Questions answered

1. **Dependency semantics — follow-alias, pin-hash.** `from_catalog("alias")`
   should *follow* the alias head (go stale when it advances);
   `from_catalog("<hash>")` stays pinned. Requires recording the read-intent
   (alias vs hash) at build time, which is not stored today (io.py collapses
   both to a path).
2. **Binding through renames — follow.** Track the upstream through renames via
   the alias history chain; no stable alias-id exists (aliases.py keys purely on
   name). Unalias/delete prompts for what to do with dependents.
3. **Multi-parent ("poisoned") children — offer unbind.** A child that reads a
   deleted parent *and* a live one can be **unbound** from the deleted parent:
   inline that parent's contribution as a retained snapshot, keep the live
   edges. The snapshot becomes source-of-truth (retained in `.cas`), not
   evictable cache.
4. **Delete — freeze by default, cascade by option, always notify.** Author's
   instinct is git-style cleanliness: deleting an alias means the concept was
   flawed, so its descendants are suspect; recovery is real because the catalog
   is git-backed + bullpen. Recommended refinements: model deletion as
   **archive** (hide, retain bytes, restorable); **derive** "hidden because an
   ancestor is archived" rather than stamping `archived` onto each dependent;
   **preview the cone** before any cascade.
5. **Recovery model.** The catalog *is* git-backed (corrected mid-discussion —
   an earlier claim that it wasn't was wrong). git tracks the small state (alias
   pointers, xorq catalog zips); the heavy `entries/<hash>/` dirs are untracked
   and recovered via the **bullpen** during `reset_to`. Recompute-from-recipe is
   the recovery story, so recipes must be durable — see #48.
6. **Chaining mechanism — a tagged read of the materialized result.** Replace
   the anonymous parquet read with
   `deferred_read_parquet(<entry>/result.parquet).hashing_tag(CatalogTag.SOURCE, entry_name=…)`.
   This records the dependency edge natively (so `catalog_parents` path-matching
   becomes a `walk_nodes(HashingTag)`) while keeping the shallow, cheap-to-build
   graph the parquet boundary was protecting.
7. **Native-first mapping (the principle applied):**
   - record entry→entry edge → **xorq native** (`HashingTag`); drop the
     path-matching in lineage.py.
   - within-expression recompute → **xorq native** (`execute()`).
   - reference an entry *as a dependency* → **patch**: xorq's `Catalog.load`
     deep-loads (too slow, criterion 2), so compose xorq's own
     `deferred_read_parquet` + `HashingTag` instead of using `Catalog.load`.
   - cross-entry invalidation propagation → **patch** (criterion 1; xorq has none).
   - source-change detection → **patch** (criterion 1; xorq's only native option,
     `ModificationTimeStrategy`, conflicts with the content-addressed identity
     tallyman relies on).

## Verified findings

- **The tagged-read spine works.**
  `deferred_read_parquet(<entry>/result.parquet).hashing_tag(CatalogTag.SOURCE, entry_name=…)`
  built in 0.019s, stayed shallow (one Read of the result), `head(3).execute()`
  returned real data in 0.20s off a 263MB result, and the edge was recovered via
  `walk_nodes(HashingTag)` as `('catalog-source', 'af917591da4b')`
  (af917591da4b = the `camera_select_columns` entry).
- **xorq composition deep-loads.** `_resolve_source` for a `CatalogEntry` calls
  eager `load_expr()` then wraps the result in `RemoteTable` (bind.py:115–118);
  `compose` / `bind` inline upstream recipes via `replace_unbound`. So
  referencing an entry through xorq's catalog API rebuilds its full graph — the
  cost the parquet boundary was avoiding. (Slowness *at depth* is structural plus
  author-reported; not yet timed on a deep chain.)
- **#48 — tallyman breaks its own xorq catalog.** tallyman commits
  `aliases.json` / `alias_history.json` into the xorq catalog repo
  (aliases.py:30–35 + the checkpoint middleware at app.py:545–566), which
  violates xorq's `Catalog.assert_consistency` (catalog.py:768–785). After the
  first entry, every `xorq catalog add` fails with an opaque empty error, so
  17 of 18 entry recipes never reach git — undermining the recompute-from-recipe
  recovery model above. Filed as buckaroo-data/tallyman#48.

## Open questions

- **Source-identity mode (cas vs salt vs off).** cas is the only mode that both
  detects drift *and* recovers original bytes faithfully (each read is pinned to
  a content-addressed clone); salt detects drift but recovers against the
  *current* file (unfaithful); off does neither. Should cas become the default
  for recalc-enabled projects, given its disk cost and that `.cas` is durable
  source-of-truth, not cache?
- **Read-intent recording.** Mechanism for tagging a `from_catalog` read as
  alias-follow vs hash-pin at build time.
- **Cascade failure / transaction semantics.** When a mid-DAG recompute fails:
  stop, skip-and-continue, or all-or-nothing.
- **Archive overlay — derived vs stored** (recommended derived; to confirm).
- **Trigger model.** Button vs scan-on-load vs auto-cascade-on-revise (leaning
  button + scan; explicit over implicit).
- **Determinism.** Re-running `expr.py` with any nondeterminism yields a new
  hash and therefore false staleness; how strict to be (see
  `plans/adr-source-identity-content-hash.md`).
- **Deep-chain build cost.** Time xorq eager `load_expr` against the tagged-read
  reference at depth 2/3/4, to settle the "too slow" criterion with a number
  rather than a code reading.

## Not in scope here

No implementation. #48 is a prerequisite for the recovery model. The
per-expression cache-metadata tab and the catalog-sidebar tweaks are separate
work in progress on another branch.
