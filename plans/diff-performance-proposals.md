# Diff view: where the time goes, and proposals to cut it

Status: analysis and proposals (2026-10-03). Section 4a is implemented in #309; nothing else here is.
Measurements were taken in-process with a scratch script that replays
`api_diff_data` stage by stage and then replays what Buckaroo's
`LoadExprHandler` does with the posted build (the same proxy
`tests/test_perf_integration.py::measure_diff` uses). Machine: 14 cores,
48 GB. Companion and Buckaroo were running on another project at the time.

## 1. What the live diff route does today

`GET /{project}/api/diff_data/{alias}/{va}/{vb}` (`app.py:1203`) is one
synchronous request. The React page (`DiffPage.tsx`) renders "computing diff…"
and nothing else until it returns. In order:

1. `cached_result_expr` for both sides. Heals a missing snapshot under the
   request (5.3 s and 3.3 GB for the two parking_2016 snapshots).
2. `diff_keys`: cached `primary_key.json`, or a detection search with a 1 s
   budget of query time. A timeout is a 504 and caches nothing, so the next
   open fails the same way.
3. `full_diff` (`tallyman_xorq/diff.py`): eight executions under the
   execution lock. Two per-column aggregates with `nunique` on every column
   (`stats_diff_xorq`), two `limit(10)` and two `count()` (`head_diff_xorq`),
   one outer-join aggregate and one outer-join `limit(50)` (`key_diff_xorq`).
   When Buckaroo is up, the page uses three numbers from all of this
   (`matched`, `only_before`, `only_after`). The stats table, the head HTML
   and the 50-row keyed preview are only rendered in the no-Buckaroo
   fallback.
4. `_build_compare_expr`: builds the outer join with `membership`, `_v2`,
   `_eq`, `_pct_delta`, `_abs_delta` columns and compiles it to a YAML build
   under the OS temp dir (`build_expr`, 0.7 s regardless of size).
5. `POST /load_expr` to Buckaroo, flat 30 s timeout. Inside that handler
   Buckaroo constructs the dataflow, which runs the whole summary-stat
   pipeline (one batch aggregate, then one histogram query per column) and
   three `count()`s, all over the unmaterialized join. The HTTP response is
   written after the stats finish, so step 5 is where the page waits.
6. Response. Only now does the browser open the WebSocket. Every page, sort
   and search afterwards re-executes the join (`_window_to_parquet` is
   `join.limit(n, offset)`, with no `ORDER BY` unless the user sorted).

## 2. Measured stage costs

Three real pairs. `trips` sides are cheap entries (a filter over the 3 M-row
source, re-run per query); `parking_2016` sides are worthy (snapshots).

| stage | by_hour V6→V7, 24 rows | trips V5→V6, 2.47 M × 19 cols | parking_2016 V3→V4, 2.12 M × 45 cols |
|---|---|---|---|
| cached_result_expr ×2 (cold) | 0.61 s (healed) | 0.21 s | 0.15 s (after a 5.3 s heal) |
| diff_keys | 0.00 (cached) | 0.00 (inherited) | **timeout at 1.03 s → 504** |
| stats_diff_xorq | 0.18 | 0.58 | 1.34 |
| head_diff_xorq | 0.01 | 0.07 | 0.03 |
| key_diff_xorq | 0.02 | 0.25 | 0.42 |
| build_compare_expr | 0.01 | 0.07 | 0.04 |
| build_expr (YAML compile) | 0.75 | 0.68 | 0.71 |
| compare columns | | 86 (from 19) | 159 (from 45) |
| Buckaroo load_expr: stats + counts, cold | 0.80 | **16.08** | **27.98** |
| stat queries run (cache snapshots written) | 23 | 83 | 132 |
| same, warm stat cache | 0.24 | 2.08 | 4.74 |
| first page / sorted page / search page | 0.01 / 0.02 / 0.01 | 0.15 / 0.30 / 0.17 | 0.36 / 0.68 / 0.82 |
| **time to first pixel today** | ~1.6 s | **~18 s** | 504, or **~31 s** with a key (over the 30 s POST timeout) |

Parking rows were obtained by forcing `keys=['summons_number']`; the live
route never gets that far.

What the same two pairs cost when the join is materialized once and Buckaroo
is handed a view build of the file (the proposal in section 4):

| stage | trips | parking_2016 |
|---|---|---|
| materialize join → parquet, single-partition writer | 2.39 s (58.6 MB file, +2.3 GB RSS) | 5.85 s (111.6 MB file, +2.9 GB RSS) |
| same, multi-partition `to_pyarrow_batches` | 2.23 s | 5.48 s |
| view build of the parquet | 0.05 | 0.06 |
| Buckaroo stats + counts over the parquet, cold | **4.40** | **8.58** |
| first page / sorted page | 0.03 / 0.11 | 0.03 / 0.30 |
| membership counts (one group-by on the file) | 0.02 | 0.02 |
| **time to first pixel** | **~7 s** | **~15 s** |

## 3. Why it is slow, ranked by measured share

1. **Buckaroo's stat pipeline runs over an unmaterialized outer join.** 83
   and 132 queries, each a full outer join of two multi-million-row sides.
   16 s and 28 s cold; 2 to 5 s warm, because the three `count()`s still
   re-run the join. This is 85 to 90 % of the wait, it sits inside the
   `/load_expr` POST, and `diff_data` waits for the POST. At 45 columns the
   stats alone run up against the 30 s timeout, so the page falls back to
   static tables after waiting the full 30 s. (#188, #202.)
2. **Column multiplication.** The compare expression is 3.5 to 4.5 times as
   wide as the entry. The hidden before-value `{col}` columns and the
   `{col}_eq` and `membership` sentinels get full stats (nunique plus a
   histogram query each) although no stat of theirs is ever displayed. The
   display klasses only read `min`/`max` of the delta columns.
3. **`full_diff` computes what the Buckaroo page discards.** 0.9 to 1.8 s and
   1 to 2 GB transient RSS for a stats table, head HTML and a 50-row preview
   that the page only renders when Buckaroo is absent.
4. **Primary-key detection fails deterministically on wide tables.** For
   parking_2016 the two preliminary scans (count plus 44 `nunique` in one
   query: 0.50 s; distinct over all 44 columns: 0.40 s) consume 0.9 s of the
   1.0 s budget before the search starts. The real key, `summons_number`, is
   an all-unique int64 not named `*id`/`*pk`, so it is in the third
   candidate group, behind every string composite. It can never be reached
   inside the budget, and the timeout caches nothing. Any entry whose
   preliminary scans take about a second has no diff grid at all.
5. **Fixed floor.** `build_expr` of the join is 0.7 s and Buckaroo's dataflow
   construction is 0.8 s even for 24 rows: 1.6 s minimum for a trivial diff.
6. **One blocking request, no progressive render.** Schema and code diffs
   are file reads and could be on screen in milliseconds. Instead the page
   shows a spinner until the slowest step completes, and a 504 or timeout
   throws away everything computed before it.
7. **Per-interaction cost and unstable paging.** Each page, sort or search is
   a fresh join: 0.15 to 0.8 s at 2 M rows, which is why `searchDebounceMs`
   is 3000. Unsorted pages have no `ORDER BY` over a hash join, so two pages
   are not guaranteed to be consistent slices of one ordering.
8. **Cheap sides compound it.** Both `trips` sides are cheap entries, so
   every one of the 80-plus queries also re-scans the 3 M-row source and
   re-applies the filter on both sides before joining.

## 4. The proposal: materialize the join once, hand Buckaroo a file

This is the "promote to catalog every time, but unnamed" idea, and it is the
design #188 already records (ADR-007's former D10). The measurements say it
works: the join runs once instead of 80 to 130 times, Buckaroo's cold stats
drop from 16 s to 4.4 s and from 28 s to 8.6 s, and pages go from hundreds of
milliseconds to tens. Two ways to build it, and they can be staged.

### 4a. Ad-hoc diff snapshot (smaller change, test this first)

- Write the join to `compute_cache/diff_cache/<a12>-<b12>-<id12>.parquet`
  through the existing snapshot writer (`materialize._stream_to_parquet` on
  the single-partition backend). Both inputs are content-addressed, so the
  file never goes stale and needs no digest check. It gets `__row_order`
  and the canonical sort for free.
- Hand Buckaroo a view build of that file (the `ensure_view_build` pattern),
  with the same "special instructions" the live route already sends:
  `column_config_overrides`, `project_root` = `diff_extras`,
  `cache_storage_path`, plus `row_order_column`. `_build_compare_expr`'s
  YAML compile of the join and its temp build dir go away (0.7 s saved).
- `matched` / `only_before` / `only_after` become one `group_by(membership)`
  over the file (0.02 s). `full_diff`'s eight executions are no longer
  needed on the Buckaroo path (section 5, C1).
- Promote can stay as it is at first. A later step can let the promoted
  entry's `materialize` reuse the file when the recipe's `(a, b, keys)` match.
- Disk: one file per `(a, b, keys)`, 59 and 112 MB here, roughly the two
  sides' snapshots combined. Show it on the Cache page beside
  `diff_stat_cache/` as reclaimable.

As implemented in #309 (`tallyman_companion/diff_snapshot.py`):

- `<id12>` is a digest of the rows each side reads and the join keys
  (`diff_snapshot.diff_identity`, from `buckaroo_lifecycle.diff_data_id`), not of the two content hashes alone. An
  unfaithful heal rewrites a snapshot under the same hash, and a cheap entry
  reads its parents' snapshots, so hashes alone would serve old rows. The
  Buckaroo session id carries the same digest, `diff-<a12>-<b12>-<id12>`.
- The file goes through `materialize.write_expr_snapshot`: single-partition
  connection, snapshot format, atomic replace. It takes neither the execution
  lock nor the project lock, because the join runs on a connection of its own
  and nothing else reads the file's name.
- The file ends in `__row_order`, as every file tallyman writes does. The
  compare grid hides it. No canonical sort is added: a page of one file is a
  stable slice. `row_order_column` is not sent (see section 5, F).
- On the Buckaroo path the route skips `full_diff`'s queries. The code and
  schema diffs are file reads and the three counts are one group-by over the
  file. If the file or the session fails, or Buckaroo is down, or there is no
  key, the route runs `full_diff` as before.
- Nothing collects old files. They are cache and a reset leaves them alone.

### 4b. Full #188: an unnamed ephemeral entry

Same materialization, but the diff is a real entry (recipe
`build_diff_expr(a, b, keys)`) living under `compute_cache/ephemeral_entries/`,
and promote is "move into `entries/`, set the alias, checkpoint". One object
for live and promoted diffs; the stat cache, view build and session id come
from the entry machinery. What #188's review flagged still applies:

- The build runs the query twice for the reproducibility check (ADR-009 D6).
  For a diff of two immutable snapshots that doubles the dominant cost for
  no information; the diff build should skip the check.
- The build takes the project lock (ADR-007 D11) and the companion's
  `promote_diff` builds on the event loop (#190). An ephemeral build must run
  in a thread, and the UI needs a "building diff" state.
- `zip_pending_entries` zips every complete dir under `entries/` at
  checkpoint, so ephemeral dirs cannot live there; `ensure_materialized` and
  the Cache page look entries up under `entries/` and need a second root.

Recommendation: 4a is a day's work and captures the measured win; 4b is the
right end state but touches the build, lock and checkpoint paths.

### Memory, the open question

Materializing the join raised the companion's RSS by 2.3 GB (trips) and
2.9 GB (parking_2016), peaking at 6.4 GB in a process that had already done
the rest of the diff. The unmaterialized path is not cheaper: Buckaroo's
process reached 4.1 GB on the same pair after one search, and it pays on
every query. What is untested is a 10 M-row pair such as
`reactive-parking2/parking_2017` V2→V3 (10.8 M × 43, both sides cheap). The
experiment that decides it: run the materialization stage alone on that
pair, watch RSS, and note whether DataFusion's hash join fits in memory. If
it does not, the fallback is a sort-merge join (both snapshots are already
canonically sorted; when the sort's leading column is the key the merge
streams) or a join chunked by key range. Not run here, because the companion
was up and a swap storm would have hit it.

## 5. Other proposals, independent of section 4 and compounding with it

**B. Cut Buckaroo's stat work on the diff.**

- B1. Pass `skip_stat_columns` for the hidden before-value `{col}` columns,
  every `{col}_eq`, and `membership`. Buckaroo supports it today, and the
  handler comment names "a diff reusing each source column's stats" as its
  purpose. This removes roughly half of the per-column histogram queries.
  Check that a skipped column still carries the structural entry the
  styling layer needs (the pipeline pre-populates name, dtype and length).
- B2. Supply `init_sd` for `{col}` and `{col}_v2` from the source entries'
  stats. After the outer join those columns equal the source column plus
  nulls for unmatched keys, so only `null_count` differs, by `only_after`
  or `only_before`. Needs a way to read a session's `sd` back out of
  Buckaroo (the on-disk stat cache holds query-keyed result parquets, not an
  `sd`), so this is a Buckaroo-side addition.
- B3. Narrow the compare expression to columns that changed. Needs a
  per-column digest that does not depend on row order; the current
  per-column digests follow the canonical sort, so a change in one column
  reorders rows and changes every digest. Worth it only if an
  order-insensitive digest exists or is cheap to add (`scripts/spike_multiset_digest.py`).

**C. Stop computing what is discarded; render progressively.**

- C1. On the Buckaroo path skip `stats_diff_xorq`, `head_diff_xorq` and the
  keyed preview. Take the three membership counts from the materialized
  file (0.02 s) or from Buckaroo's stats of the `membership` column. Saves
  0.9 to 1.8 s and 1 to 2 GB transient.
- C2. Split the route the way the entry page already is (#133):
  `diff_data` returns versions, schema diff and code diff with zero
  executions, and a new `/api/diff_session/{alias}/{va}/{vb}` blocks the way
  `/api/session/{hash}` does, returning a typed status
  (`building`/`ok`/`timeout`/`error`/`no_key`). The page shows the header
  at once and a spinner where the grid goes.
- C3. Send `telemetry_url` on the diff load so the `firstpull.*` spans land
  in `telemetry.jsonl` (today no diff load is instrumented, which is why
  this had to be measured by hand), and derive the POST timeout from row
  count as `_load_timeout` does for entries instead of a flat 30 s.
- C4. Publish `diff_keys_resolved`, `diff_materialized`, `diff_session_ready`
  on the existing SSE channel so the spinner can say which step it is on.

**D. Do the work at write time.**

- D1. After a head advance (where `_auto_recalc_after_head_advance` runs),
  start a background job that resolves the new version's key and
  materializes the diff of the latest two versions into 4a's cache. The
  default diff route is exactly that pair, so most opens become cache hits.
- D2. Resolve the primary key during the build. The build already scans the
  data twice; the key costs one more aggregate, and a key detected at
  import time on a source entry is inherited by every row-preserving
  revision for free.

**E. Make key detection stop failing.**

- E1. Accept any single column with `distinct == n` as the key before
  trying string composites, regardless of type or name. `summons_number`
  would be found right after the first 0.5 s aggregate.
- E2. Charge the budget to the search only, not to the two preliminary
  scans, or scale it with row count, or run detection off the request
  path (D2) with a generous budget. Cache a timeout with its reason so the
  UI can ask for a key instead of answering 504 on every open.
- E3. Let the caller name the key: a `keys` parameter on `catalog_diff` and
  `catalog_promote_diff`, and a key picker on the diff page when detection
  fails.

**F. Paging and search after 4a.** The file has `__row_order`, so pass
`row_order_column` (effective once buckaroo#974 lands) and let sorted pages
push down to parquet; `searchDebounceMs` can come down from 3000.
