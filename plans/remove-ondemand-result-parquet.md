# Removing the on-demand `result.parquet` layer (as merged in #104)

## Status

Landed on `main` in #104 (merge `ea06845`, 2026-06-17). This document records the
change as it actually shipped. It supersedes the forward-looking plan that was
proposed in #101 (closed unmerged): #102 merged between that plan and the
implementation and re-introduced part of the layer, so the work grew an
"unwind #102" arm, and three of the plan's prescriptions were wrong on contact
and were corrected during implementation. Those deltas are called out in
[What the #101 plan got wrong](#what-the-101-plan-got-wrong) rather than buried.

## Goal

Delete every "materialise a per-entry `result.parquet` on demand" path. xorq's
`ParquetSnapshotCache`, which `rewrite_for_build` bakes at build time for
expensive entries, is the single materialised copy. Cheap entries recompute on
read (a pushdown over a columnar source, ~free), as since #73. After #104 no
code reads or writes `<entry>/result.parquet`, and every consumer — viewer,
paginated reads, diffs, post-processing — reads `cached_result_expr`.

This finishes the arc #73/#71/#90 started: #73 stopped writing `result.parquet`
at build but kept `ensure_result` to regenerate it lazily; #90/#71 moved the
viewer and paginated reads onto `cached_result_expr`.

## Scope (A)

Kept the cheap/expensive classifier (`classify_build` / `cache_worthy`); removed
only the `result.parquet` layer. Expensive entries resolve through their baked
`.cache()` snapshot; cheap entries recompute. The alternative — `.cache()` every
entry and drop the classifier — was rejected: it re-introduces the
materialisation #73 deliberately removed and burns storage on cheap row-wise
entries. `rewrite_for_build`, the classifier, and the source-read cache are
untouched.

## The layer that was removed

`ensure_result(project, hash)` was the on-demand writer — it wrote
`<entry>/result.parquet` via `cached_result_expr(...).to_parquet()` (or
`load_entry(...).to_parquet()` under salt). It was the only thing that created
the file. `cached_result_expr` already returned a single-backend, cache-resolving
expression for every entry (`result_cache.py:233`): a bare
`deferred_read_parquet(snapshot_path)` for expensive entries, the recomputed
recipe for cheap ones. `baked_snapshot_path(project, hash)` (`result_cache.py:211`)
returns the expensive entry's baked snapshot file, or `None`. Both were kept; the
removal pointed everything at them.

### #102, which landed between the plan and the implementation

#102 (`perf/71-viewer-serve-result-read`) merged ~4 minutes after the #101 plan
was frozen and re-introduced an on-demand `result.parquet` build on the viewer
path: `ensure_result_build` / `ensure_viewer_expanded_build`, with `ensure_session`
paging over the materialised parquet, plus the `.xorq_result_build` /
`.xorq_result_build_expanded` path helpers. Per maintainer direction #104 removed
that too — it is the same on-demand layer wearing a different hat. See
[Unwinding #102](#unwinding-102).

(Unrelated: #88's nondeterminism build-lint landed separately via #93, *not* part
of this work, even though all three were in flight the same evening.)

## The three consumers, repointed

| Site | Was | Now |
|------|-----|-----|
| `post_processing.py` | `ensure_result` → `pq.read_table(path).to_pandas()` → `xo.memtable` | `cached_result_expr(project, hash).execute()` → `xo.memtable(df, name="_post_processing_run")` |
| `companion/diff.build_diff_expr` | `ensure_result` ×2 → `deferred_read_parquet(path)` ×2 | `cached_result_expr(project, a_hash)` / `(b_hash)` passed straight into `build_compare_expr` (drops a double-materialisation) |
| `xorq/diff.full_diff` | dispatched on `backend` (`pandas`/`polars`/`xorq`), materialising parquets via `_ensure_entry_parquet` for the file-reading backends | xorq-only: requires `a_expr`/`b_expr`, raises `ValueError` if either is missing; the `pandas`/`polars` branches and `_ensure_entry_parquet` are gone |

`post_processing` reads the same expression the viewer's `api_data` executes, so
cheap / expensive / self-heal behave identically. `full_diff`'s two live callers
(`mcp/server.py`, `companion/app.py`) already passed `xorq` exprs from
`cached_result_expr`; the `backend="xorq"` kwarg was dropped at both. The
`buckaroo.compare` re-exports (`stats_diff`/`head_diff`/`key_diff` and the
`_polars`/`_xorq` variants) were kept — a separate compat surface other code and
tests import; `full_diff` simply no longer dispatches to the non-xorq ones.

## Cache-admin surfaces (`companion/app.py`)

The `CacheEntry` / `ResultCache` JSON contract is unchanged, so `types.ts`,
`CachePage.tsx`'s table, and `Header.tsx` needed no structural edit.

- **disk-usage** — dropped the `results` bucket and its loop; baked snapshots are
  already counted under `compute_cache`. The only consumer, `Header.tsx`, reads
  `disk_usage.formatted.total`, so the payload change is frontend-safe.
- **cache inspector** `/api/result_cache` — lists the baked snapshots that exist
  on disk *right now* (the rows the delete button can actually evict).
  `manifest.cache_bytes` (#87) is a cheap pre-filter: `None` for a cheap entry, so
  skip it without deriving a path. For an expensive entry, resolve
  `baked_snapshot_path` and skip it when the file is gone, then take `size` from
  the live file. `row_count` is coerced to `int(m.row_count or 0)` (the contract
  types it `number`, and `CachePage` calls `.toLocaleString()` with no null
  guard); `created` is reformatted from the ISO-8601 `created_at` to the table's
  existing `"%Y-%m-%d %H:%M:%S"`.
- **delete** `/api/result_cache/{hash}` — unlinks `baked_snapshot_path` and 404s
  when it is `None`/missing (a cheap entry, or one already evicted). It then calls
  `cached_result_expr.cache_clear()` so a warm `lru_cache` entry doesn't keep a
  `deferred_read` of the just-unlinked file; the next read re-reconstructs and the
  evicted snapshot re-bakes.

Frontend stale-copy sweep (non-functional): `CachePage.tsx` strings reworded
around "baked snapshot" instead of `result.parquet`; `CatalogPage.tsx` zero-rows
fallback → "no rows"; `NotebookPage.tsx` → "no preview for {hash}". All were keyed
on unrelated state, so no behavior changed.

## Path / manifest / build plumbing

- `paths.py` — deleted `ENTRY_RESULT_FILENAME` and `entry_result_path`, removed
  the former from `ENTRY_CACHE_NAMES`, and dropped the `.xorq_result_build*`
  helpers #102 had added.
- `tallyman_core/__init__.py` — dropped both symbols from the imports and
  `__all__`.
- `manifest.py` — removed the dead `result_path` field. Nothing in `src/` read
  it, and pydantic v2 ignores unknown keys, so old manifests still validate; no
  read-side migration.
- `build.py` — kept `migrate_drop_result_parquet`, but broadened it. The marker
  is bumped to `.migrated_no_ondemand_result_parquet` so an install that ran the
  #73 marker re-sweeps once, and the sweep deletes a *tuple* of legacy names —
  `_LEGACY_RESULT_NAMES = ("result.parquet", ".xorq_result_build",
  ".xorq_result_build_expanded")` — `unlink`ing files and `rmtree`ing the #71/#102
  build dirs. Idempotent and best-effort.
- `buckaroo_lifecycle.py` — `_entry_parquet_exists` → `_entry_exists`, returning
  `entry_build_dir(project, hash).is_dir()` (the build dir is the entry's
  existence proof now).
- `scripts/perf_report.py` — the per-entry `result.parquet` size probe (always
  `None` after the removal) repointed to `res.cache_bytes`.
- Docstring/comment sweeps in `mcp/server.py`, `io.py`, `lineage.py`,
  `parent_capture.py`, `test_companion.py`.

## Unwinding #102

#102's viewer-serve-from-`result.parquet` work was removed: `ensure_result_build`,
`ensure_viewer_expanded_build`, and the `.xorq_result_build*` path helpers are
gone. `ensure_session` (`buckaroo_lifecycle.py`) posts the entry's expanded
recipe build dir (`xorq_build/`) to Buckaroo's `/load_expr` again — a cheap entry
recomputes on read (pushdown), and a cache-worthy entry's recipe replays onto its
baked `.cache()` snapshot (a read of the snapshot, not a re-run of the
Aggregate/Join/Sort). Before posting, it calls `cached_result_expr(project, hash)`
to materialise/self-heal the snapshot (idempotent, lru-cached, ~free when warm) so
a cold compute cache doesn't render an empty grid — the same heal the paginated
read relies on. No per-entry `result.parquet` is read.

## Salt mode

The salt read branch in `cached_result_expr` was deleted with no replacement.
`rewrite_for_build` returns the expression unchanged under salt (it skips cache
injection), so a salted entry never has a `CachedNode` at its top,
`_cached_node_path` is `None`, and reads fall through the existing paths: a cheap
salt entry via `cheap-recompute`, an expensive one via `worthy-recompute-fallback`.
Both are single-backend recompute expressions, content-honest by construction.
The net semantic change: salt-mode reads recompute every time instead of freezing
to a once-written parquet — acceptable, since salt is opt-in
(`TALLYMAN_SOURCE_IDENTITY=salt`), default-off, and experimental.

## Error semantics

An entry that is genuinely broken (snapshot evicted *and* recipe can't
recompute, e.g. its source was deleted) now surfaces whatever
`cached_result_expr(...).execute()` raises, the same recompute error the viewer
and diff already surface. Post-processing heals like diff instead of erroring on
an evicted expensive entry. The trade is a less tidy error string for the
truly-unrecoverable case.

## What the #101 plan got wrong

The plan was accurate on its core mechanics, but three prescriptions were wrong
against the code and were corrected here:

1. **Cache inspector** — the plan said to drive the listing purely from
   `manifest.cache_bytes` and explicitly forbade per-entry `baked_snapshot_path`
   reconstruction as "too slow." But the repointed delete unlinks the snapshot
   without clearing `cache_bytes`, so a `cache_bytes`-only listing shows a phantom
   row after a delete (stale size, freed bytes still in the total, a delete button
   that 404s). The shipped inspector keeps `cache_bytes` as a pre-filter, then
   stats the snapshot via `baked_snapshot_path` and skips it when the file is gone.
2. **Delete self-heal** — the plan said evicting the snapshot "self-heals on the
   next read." That is false for a warm process: `cached_result_expr` is
   `@lru_cache`, and its evicted-self-heal only fires on a cache miss, so a warm
   entry dereferences the just-unlinked file ("At least one path is required").
   The delete endpoint had to add `cached_result_expr.cache_clear()`.
3. **`test_reset_to_revision`** — the plan said to change its `result.parquet`
   stand-in "since it's no longer in `ENTRY_CACHE_NAMES`." But prune/restore copies
   the whole entry dir regardless of `ENTRY_CACHE_NAMES`, so the stand-in still
   round-trips. The test was left unchanged.

The plan also under-named some required edits, which the implementation supplied:
the `.xorq_result_build*` dirs in the migration sweep, the dead `logging`/`log`
binding in `xorq/diff.py`, `scripts/perf_report.py`, and the #102 build helpers
(which post-dated the plan). The plan's "Error-semantics" note additionally cited
two before-state error strings that did not exist (`PostProcessingRunError("no
result.parquet found")` was never in `src/`, and the `404 "result.parquet not
found"` string lived only in the delete endpoint, not a read path); the corrected
note above drops them.

## Tests

Followed the repo's TDD test-vs-fix split: a failing-tests commit (post-processing,
`build_diff_expr`/promote-diff, `full_diff`, and a structural assertion that
`ensure_result` / `ensure_result_build` / `ensure_viewer_expanded_build` are gone)
watched red on CI, then the fixes. The two inspector corrections above each landed
failing-first as well (`test_cache_pane_drops_evicted_snapshot`,
`test_cache_delete_then_read_self_heals_warm_cache`).

Test changes: `test_result_cache.py` self-heal tests retargeted to
`cached_result_expr(...).execute()`; `test_diff.py` fixtures and backends moved to
the expr/xorq path; `test_cache_inspector.py` repointed to baked snapshots with the
phantom-row and warm-cache cases; `test_cache_lab.py` migrated onto
`cached_result_expr` with `view_timings` redefined as snapshot cold-vs-warm;
`test_paths.py` / `test_manifest.py` dropped the removed symbols and assertions;
`test_buckaroo_multi_project.py` switched its prune fixture to the build dir;
`test_post_processing.py` / `test_promote_diff.py` / `test_buckaroo.py` gained the
no-`result.parquet` assertions; `test_build.py` covers the broadened migration
sweep; `test_disk_usage.py` / `test_perf_integration.py` dropped the `results`
bucket.

## Out of scope

The cheap/expensive classifier and the `.cache()` injection in `rewrite_for_build`
(unchanged under scope A); the source-read cache (`read_csv`/`read_json` `.cache()`
nodes); and the Buckaroo stat caches and diff stat caches — all orthogonal.
