# Remove the on-demand `result.parquet` layer; rely on xorq `.cache()`

## Goal

Delete every "materialise a per-entry `result.parquet` on demand" path. The
xorq `ParquetSnapshotCache` that `rewrite_for_build` already bakes at build time
(for expensive entries) becomes the single materialised copy. Cheap entries
recompute on read, as they do today (#73). No code reads or writes
`<entry>/result.parquet` after this.

This finishes the arc #73/#71/#90 started: #73 stopped writing `result.parquet`
at build but kept `ensure_result` to regenerate it lazily; #90 and #71 moved the
viewer and paginated reads onto `cached_result_expr`. What remains is the
on-demand writer and its three real consumers, plus the path plumbing and the
admin surfaces that still name the file.

## Scope decision (recommended: A)

**A — keep the cheap/expensive split, remove only the `result.parquet` layer
(recommended).** Expensive entries resolve through their baked `.cache()`
snapshot; cheap entries recompute. This matches the #73 design (cheap recompute
is a pushdown over a columnar source, ~free) and has the smallest blast radius.
The rest of this plan assumes A.

**B — `.cache()` every entry, drop `classify_build`/`cache_worthy`.** Every entry
would then have a snapshot path, simplifying consumers that want a file. Rejected
as the default: it re-introduces materialisation #73 deliberately removed, burns
storage on cheap row-wise entries, and is a much larger change. Only pursue if
you specifically want a guaranteed on-disk parquet for every entry.

If B is wanted instead, say so before implementation — it changes
`rewrite_for_build`, `cached_result_expr`, and the manifest fields, not just the
consumer call sites below.

## Decisions resolved (grilling)

- **Scope: A** — remove only the `result.parquet` layer; keep the classifier.
- **pandas/polars diff backends: drop** — `full_diff` is xorq-only (§4).
- **Cache inspector + delete: repoint** — inspector is manifest-driven
  (`cache_bytes`); delete unlinks `baked_snapshot_path` (§5). Frontend untouched.
- **Cache lab: migrate** onto `cached_result_expr`, re-baselining its metrics;
  `ensure_result` fully deleted (§12).
- **Lab `view_timings`: snapshot cold-vs-warm** is the redefined metric (§12).
- **Salt mode: delete the branch** — falls through existing recompute paths (§1).

Codebase-confirmed (no longer open): all three `full_diff` callers pass exprs;
`marimo_export` is not a consumer; pydantic ignores the dropped `result_path`
key; the self-heal contract lives in `cached_result_expr`; `Header.tsx` reads
only `disk_usage.formatted.total`; `CacheEntry` maps to manifest fields; no
file-download route serves `result.parquet`.

## Current state (what we're deleting)

- `result_cache.ensure_result(project, hash)` — the on-demand writer
  (`result_cache.py:309`). Writes `<entry>/result.parquet` via
  `cached_result_expr(...).to_parquet()`, or `load_entry(...).to_parquet()` in
  salt mode. The only thing that creates the file.
- `cached_result_expr` already returns a single-backend, cache-resolving
  expression for every entry: a `deferred_read_parquet(snapshot_path)` for
  expensive entries (the baked `.cache()` file), the recomputed recipe for cheap
  ones (`result_cache.py:233`). This is the mechanism everything should use.
- `baked_snapshot_path(project, hash)` (`result_cache.py:211`) already returns
  the expensive entry's baked snapshot file (or `None`). Reuse it for the admin
  surfaces.

### The three real consumers of `ensure_result`

| Site | What it does today | Convert to |
|------|--------------------|------------|
| `post_processing.py:240` | `ensure_result` → `pq.read_table(path).to_pandas()` → `xo.memtable` | `cached_result_expr(project, hash).execute()` → `xo.memtable(df, ...)` |
| `companion/diff.py:227-230` (`build_diff_expr`) | `ensure_result` ×2 → `xo.deferred_read_parquet(path)` ×2 | `cached_result_expr(project, a_hash)` / `(b_hash)` directly |
| `xorq/diff.py:94-114,156-158` (`_ensure_entry_parquet` + no-expr fallback) | materialise both entries, read parquet paths | remove; xorq backend requires the passed `a_expr`/`b_expr` (both live callers already pass `cached_result_expr`) |

The diff helpers (`stats_diff_xorq`, `head_diff_xorq`, `key_diff_xorq`) already
accept an expression — the live diff path passes `a_expr`/`b_expr` and never
touches a file. The fallback that materialised parquets is dead in production
(MCP `server.py:739` and companion `app.py:793` both pass exprs).

### Path/admin plumbing that names the file

- `paths.py:88` `ENTRY_RESULT_FILENAME`, `paths.py:188` `entry_result_path()`,
  and its membership in `ENTRY_CACHE_NAMES` (`paths.py:105`).
- `manifest.py:36` `result_path` field (dead — nothing in `src/` reads it).
- `app.py` disk-usage `results` bucket (`158-163`), cache inspector
  `/api/result_cache` (`1015-1077`), delete endpoint `/api/result_cache/{hash}`
  (`1079-1094`), and the `#73` startup migration call (`295-304`).
- `buckaroo_lifecycle.py:60-66` `_entry_parquet_exists` (existence check).
- `build.py:504` `migrate_drop_result_parquet` (legacy sweep — keep, see below).
- Docstrings/comments in `mcp/server.py:735,1116`, `io.py`, `lineage.py`,
  `parent_capture.py` (text only).

There is **no** file-download route serving `result.parquet` (no `FileResponse`
/ `StreamingResponse` anywhere). The "downloads" consumer named in several
docstrings is stale; nothing to convert there beyond fixing the text.

## Changes, by file

### 1. `src/tallyman_xorq/result_cache.py`
- Delete `ensure_result` (309-343).
- **Just delete the salt branch (284-285)** — no special replacement needed. With
  it gone, salt reads fall through the existing paths: a cheap salt entry →
  `cheap-recompute` (`_recipe_expr`); an expensive salt entry →
  `rewrite_for_build(raw)` returns it *uncached* (salt skips injection) →
  `_cached_node_path` is `None` → the existing `worthy-recompute-fallback` returns
  `raw`. Both are single-backend recompute exprs; no `result.parquet`, no new code.
- Update the module docstring (8-19) and the `cached_result_expr` docstring
  (260-262) to drop the `ensure_result` / `result.parquet` references.
- Keep `baked_snapshot_path`, `classify_build`, `cache_worthy`, `_cached_node_path`.

### 2. `src/tallyman_core/post_processing.py`
- Drop the `ensure_result` import (230) and the `pq.read_table` step (248).
- `df = cached_result_expr(project, content_hash).execute()` then
  `xo.memtable(df, name="_post_processing_run")`. This is the same expression the
  viewer's `api_data` executes, so cheap/expensive/self-heal all behave
  identically. Keep `pyarrow`/`xorq` import-guard for the dependency error.
- Update the function docstring (212) — no longer "materialises the result".

### 3. `src/tallyman_companion/diff.py` (`build_diff_expr`)
- Replace 224-230 with `cached_result_expr(project, a_hash)` /
  `(project, b_hash)`; pass straight into `build_compare_expr`. Both sides root
  on the default backend (cached_result_expr's contract), so the join stays
  single-backend. Removes a double-materialisation the old path did.

### 4. `src/tallyman_xorq/diff.py` — make `full_diff` xorq-only (drop pandas/polars)
Both live callers (`server.py:739`, `app.py:793`) pass `backend="xorq"` with
`a_expr`/`b_expr`; only tests hit the `pandas`/`polars` branches. Decision: drop
those backends.
- Remove the `elif backend == "polars"` (165-173) and `else`/pandas (174-180)
  branches; `full_diff` keeps only the xorq composition.
- Drop the `backend` parameter (default was `"pandas"`); `full_diff` always
  composes `a_expr` ⋈ `b_expr`. Require both exprs — raise a clear `ValueError`
  if either is missing, rather than silently materialising.
- Remove `_ensure_entry_parquet` (94-114) and the no-expr fallback (156-158).
- Remove now-unused imports: `pandas as pd` (15), `pyarrow.parquet as pq` (16),
  and `ENTRY_RESULT_FILENAME` (29). Keep `ENTRY_SCHEMA_FILENAME`.
- Keep the `buckaroo.compare` re-exports (`stats_diff`/`head_diff`/`key_diff`
  and the `_polars`/`_xorq` variants, 17-46) — they're a separate backward-compat
  surface that other code/tests import; `full_diff` simply no longer dispatches
  to the non-xorq ones.
- Update the call sites to drop the now-removed `backend="xorq"` kwarg
  (`server.py:744`, `app.py:798`).

### 5. `src/tallyman_companion/app.py`
- **disk-usage** (`131-175`): delete the `results` bucket and its loop (158-163);
  baked snapshots are already counted under `compute_cache` (171-174). Drop the
  `results` payload key and update the docstring (134). **Frontend-safe**: the
  only consumer, `Header.tsx:23`, reads `disk_usage.formatted.total` and nothing
  else, so removing the `results` sub-bucket needs no TS change.
- **cache inspector** `/api/result_cache` (1015-1077): drive it from the
  **manifest, not a file walk** — read `manifest.cache_bytes` (the baked snapshot
  size, recorded at build per #87, `build.py:430`), `row_count`, and `created_at`
  per entry. No per-entry recipe reconstruction (`baked_snapshot_path` re-imports
  the recipe — too slow to call across every entry). An entry with
  `cache_bytes` set lists as cached; cheap/`None` entries list as `size: 0`
  (pre-#87 expensive entries also read `None` — acceptable staleness). Keep the
  `CacheEntry` JSON shape exactly (hash, size, size_formatted, row_count, created,
  alias, version, is_current, prompt) so `CachePage.tsx`/`types.ts` are untouched.
  Drop the `ENTRY_RESULT_FILENAME` import (25).
- **delete endpoint** `/api/result_cache/{hash}` (1079-1094): unlink
  `baked_snapshot_path(project, hash)` instead of `entry_result_path(...)`; 404
  when it returns `None`/missing (cheap entry or already evicted). Evicting the
  snapshot self-heals on the next read via `cached_result_expr`. Drop the
  `entry_result_path` import (31).
- **startup migration** (295-304): keep the call but see build.py below
  (re-sweep). Update the comment.

The inspector/delete repoint keeps the cache page working against the xorq
`.cache()` snapshots; the `CacheEntry`/`ResultCache` JSON contract is unchanged,
so `CachePage.tsx` and `types.ts` are untouched. The inspector reads the manifest
(cheap); only the single-entry delete reconstructs via `baked_snapshot_path`
(acceptable per-request cost). One copy nit: `CachePage.tsx:24`'s confirm string
("Delete result.parquet for …", "re-materialises on next run") is now inaccurate
— it deletes a baked snapshot. Optional copy tweak, non-functional.

### 6. `src/tallyman_companion/buckaroo_lifecycle.py`
- `_entry_parquet_exists` (60-66): the build dir is the entry's existence proof
  now — return `entry_build_dir(project, content_hash).is_dir()`. Drop the
  `entry_result_path` import (54). Rename to `_entry_exists` for honesty
  (update the one caller in `_load_session_file`).

### 7. `src/tallyman_core/paths.py`
- Delete `ENTRY_RESULT_FILENAME` (88) and `entry_result_path` (188-190); remove
  `ENTRY_RESULT_FILENAME` from `ENTRY_CACHE_NAMES` (105). Rewrite the comment
  block (82-91) to state result materialisation lives only in the compute cache.

### 8. `src/tallyman_core/__init__.py`
- Drop `entry_result_path` and `ENTRY_RESULT_FILENAME` from the imports/`__all__`
  (44, 106, 114, 32).

### 9. `src/tallyman_core/manifest.py`
- Remove the `result_path` field (36) and the `ENTRY_RESULT_FILENAME` import (11).
  Pydantic v2 ignores unknown keys by default, so old manifests carrying
  `result_path` still `model_validate` cleanly — no migration needed for reads.

### 10. `src/tallyman_xorq/build.py` (legacy sweep — keep)
- `migrate_drop_result_parquet` stays: it deletes pre-existing `result.parquet`
  files (both pre-#73 build artifacts and any `ensure_result` wrote since). After
  removing the public constant, inline a module-private literal
  (`_LEGACY_RESULT_PARQUET = "result.parquet"`) instead of importing
  `ENTRY_RESULT_FILENAME` (514, 523).
- Bump the marker (`_MIGRATION_MARKER`) name, e.g.
  `.migrated_no_ondemand_result_parquet`, so installs that already ran the #73
  marker re-run the sweep once to clear lazily-created files. Idempotent and
  best-effort, as today.

### 11. Docstring/comment-only fixes
- `mcp/server.py:735-736,1116`, `io.py:68-78`, `lineage.py`, `parent_capture.py`:
  correct any text implying `result.parquet`/`ensure_result` still materialises a
  per-entry file. No logic change.

## Salt mode

Salt source-identity mode bakes no cache (`rewrite_for_build` returns the
expression unchanged under salt). Today the salt branch routes reads through a
materialised `result.parquet` purely to get a single-backend `deferred_read_parquet`
and dodge the path-only snapshot-key collision. Deleting the branch makes salt
entries recompute through the existing `cheap-recompute` / `worthy-recompute-
fallback` paths (both single-backend `_recipe_expr`), which is content-honest by
construction. Net semantic change: salt-mode reads recompute every time instead of
freezing to a once-written parquet — acceptable, since salt is opt-in
(`TALLYMAN_SOURCE_IDENTITY=salt`), default-off, and an experimental cache-lab
strategy. Salt is barely covered (one ref in `tests/`), so add a smoke test:
two same-path/different-salt entries stay distinct, and a salted entry diffs +
post-processes with no `result.parquet` written.

## Error-semantics change (note, not a decision)

A genuinely broken entry (snapshot evicted **and** recipe can't recompute, e.g.
source deleted) used to surface as `HTTPException(404, "result.parquet not
found")` / `PostProcessingRunError("no result.parquet found")`. Post-change it
surfaces whatever `cached_result_expr(...).execute()` raises (a recompute error).
This is the #73 self-heal asymmetry the demo notes flagged — post-processing now
heals like diff instead of erroring on an evicted expensive entry. The trade is a
less tidy error string for the truly-unrecoverable case; acceptable.

## Test plan (TDD — failing tests first, per repo rule)

**Commit 1 — failing tests (bundle all, push, watch CI go red):**
- `post_processing`: run `run_post_processing` against a cheap entry **and** an
  expensive entry; assert the returned preview/row_count is correct **and**
  `not (entry_dir/​"result.parquet").exists()` afterward. Fails today because
  `ensure_result` writes the file.
- `build_diff_expr` / promote-diff: build two versions, call `build_diff_expr`
  (or hit `/api/promote_diff`); assert the diff expr executes correctly with
  `not result.parquet.exists()` for either entry afterward. Fails today.
- `full_diff` xorq path: assert it requires exprs and writes no parquet (extend
  `test_diff.py:370` which already asserts no parquet for the expr path).
- Structural: `assert not hasattr(tallyman_xorq.result_cache, "ensure_result")`.
  Fails today.

**Commit 2 — the fixes** (sections 1-11). Push, watch CI go green.

**Commit 3 — test cleanup / repoint** (passes immediately, separate from the
failing-tests commit per the test-vs-fix rule):
- `test_manifest.py:27` — drop the `result_path` assertion.
- `test_cache_inspector.py` — repoint expectations to baked snapshots.
- `test_reset_to_revision.py:557-594` and `test_buckaroo_multi_project.py:86-119`
  — these write `result.parquet` as a stand-in artifact; switch the fixtures to a
  surviving artifact (build dir / manifest) or assert it is **not** restored,
  since it's no longer in `ENTRY_CACHE_NAMES`/an artifact.
- `test_disk_usage.py` / `test_perf_integration.py` — drop `results`-bucket and
  `ENTRY_RESULT_FILENAME` references.
- `test_result_cache.py` — delete the plain `ensure_result` tests; **retarget the
  two self-heal tests** (`..._expensive_parent_chain_on_cold_cache`,
  `..._multi_parent_expensive_join_on_cold_cache`) to
  `cached_result_expr(project, child_h).execute()` and assert `len(df) ==
  expected_rows`. Coverage is intact — those tests already exercise the recipe-
  reconstruction self-heal that lives in `cached_result_expr` (their docstrings
  say so); `ensure_result` only forwarded to it. Keep the cheap/expensive "no
  build-time parquet" asserts.
- `test_diff.py` — its `ensure_result`-based fixtures (278-346) build a parquet to
  feed the diff; switch them to `cached_result_expr(...)` exprs. Tests calling
  `full_diff(..., backend="pandas"/"polars")` or omitting `backend` move to the
  xorq path with `a_expr`/`b_expr=cached_result_expr(...)`; delete coverage that
  only exercised the dropped file-reading branches.
- **Cache lab** (`test_cache_lab.py`) — migrate per §12 below.

Run the default suite locally before each push (`uv run pytest`). Because this
touches `result_cache`, **also run the deselected suites** it's the designated
probe for: `uv run pytest -m cache_lab`, `-m perf`, `-m perf_tier_a`, `-m
integration` (they're excluded from the default path via `addopts`, but
`test_cache_lab` / `test_perf_*` reference `result.parquet` directly). Several
default-path tests already assert `not result.parquet.exists()` (viewer/diff) and
keep passing untouched.

## 12. Cache-lab migration (`test_cache_lab.py`)

The lab is the off/cas/salt cache-strategy benchmark. It uses `ensure_result` not
incidentally but as a measurement subject, so migrate it onto `cached_result_expr`
(decision: keep the lab, re-baseline its metrics on the real production read path).

- **`view_timings()`** — redefine cold/warm as **snapshot cold vs warm** (decision):
  - cold: `shutil.rmtree(compute_cache_dir(project))` + `cached_result_expr.cache_clear()`,
    then `cached_result_expr(project, hash).execute()` — forces recompute (and a
    re-bake for an expensive entry).
  - warm: a second `execute()` — reads the baked snapshot (expensive) or recomputes
    (cheap, which has no snapshot, so warm ≈ cold — itself a meaningful result that
    validates the #73 "don't cache cheap" thesis).
  - Drop the `ensure_result`/`parquet_read_s` fields; keep `cached_expr_*`. This
    measures what the viewer/`api_data` actually does now.
- **`disk()`** (215, 226-244) — drop the `result.parquet` rglob and the
  `entry_result_bytes` key; `compute_cache_bytes` already captures baked snapshots.
- **`assert_stored_matches_recompute()`** — replace `pd.read_parquet(ensure_result(
  ...))` with `cached_result_expr(...).execute()` (what the cache serves) vs the
  existing `load_entry(...).execute()` (fresh recompute). Keep `check_dtype=False`
  — it already absorbs the parquet-round-trip-vs-direct-execute dtype drift, which
  matters more now that one side no longer round-trips through parquet.
- **The ~20 `pd.read_parquet(ensure_result(p, h))` read sites** → `cached_result_expr(
  p, h).execute()`. Mechanical. If any value assertion is parquet-dtype-sensitive
  (e.g. `Int64` vs `int64`, tz handling), wrap that one site in
  `.to_parquet(tmp)`/`pd.read_parquet(tmp)` rather than weakening the assertion.
- Drop the `ensure_result` import (70); keep `cached_result_expr`, `load_entry`.
- The `build()` helper's `rp = entry_dir/​"result.parquet"` size probe (≈244) no
  longer finds a file — drop it or repoint to `manifest.cache_bytes`.

## End-to-end verification (after fixes)

1. `uv run pytest` green locally, **plus** `uv run pytest -m cache_lab` and `-m
   "perf or perf_tier_a or integration"` (the deselected result_cache probes).
2. Restart the companion (`/restart-tallyman`), open a project with at least one
   expensive and one cheap entry.
3. Viewer page for both entries renders rows (cheap recompute + expensive
   snapshot read). Confirm no `entry/result.parquet` appears on disk:
   `find <project>/artifacts/catalog/entries -name result.parquet` is empty.
4. Run a post-processing preview against the cheap entry — succeeds, still no
   `result.parquet`.
5. Diff two versions in the UI and promote it — succeeds, no `result.parquet`.
6. `GET /{project}/api/disk_usage` returns no `results` key; `compute_cache`
   reflects the baked snapshots.
7. Confirm the bumped migration marker swept any pre-existing `result.parquet`
   on first boot.

## Out of scope
- The cheap/expensive worthiness classifier (`classify_build`/`_is_worthy_expr`)
  and the `.cache()` injection in `rewrite_for_build` — unchanged under plan A.
- The source-read cache (`read_csv`/`read_json` `.cache()` nodes) — orthogonal,
  unchanged.
- Buckaroo stat caches and diff stat caches — orthogonal.
