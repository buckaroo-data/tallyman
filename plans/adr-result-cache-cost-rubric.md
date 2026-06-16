# ADR: Result-cache admission and eviction by measured cost vs size

- **Status:** Proposed (2026-06-10); reconciled with PR #74 (merged 2026-06-16).
  #74 shipped the *structural* `cache_worthy` test as the live admission rule and
  moved the materialised result from `result_cache/` into the per-project
  `compute_cache` (with `result.parquet` now an on-demand artifact). This ADR's
  measured value-per-byte model is still the target; the path to it is now
  shadow-then-flip, not remove-now — see Decision §2.
- **Context ticket:** buckaroo-data/tallyman#30 (measured cost/size cache rubric; supersedes #12, feeds #10 and #21)
- **Affected code:** `src/tallyman_xorq/result_cache.py` (`classify_build`, `cache_worthy`, `cached_result_expr`, `ensure_result`), `src/tallyman_xorq/build.py` (`build_and_persist`, manifest write), `src/tallyman_core/manifest.py`
- **Related ADR:** `plans/adr-source-identity-content-hash.md` (why `content_hash` is a stable cache key)

## Problem

`cache_worthy` decides whether to materialise an entry's result by scanning the
serialized xorq build for ops (`classify_build` in `result_cache.py:51`): cache
if the expression contains a Join / Aggregate / Sort / Window / UDF or reads a
non-parquet source, otherwise treat it as cheap and skip the extra copy.

This is a *structural* test, and it misclassifies a common authoring workflow.
The motivating case: a trips ⋈ stations join (~2.5M rows, 100s of MB) followed
by *iterative computed-column additions* — add a distance metric, tweak it, add
another derived column. Every one of those descendant entries inherits the
upstream `Join` in its op graph, so `classify_build` marks all of them
"worthy". Each iteration writes a 100+ MB cache for a result that is trivial to
recompute (a row-wise scalar pass over an already-cheap input). The cache fills
with low-value copies.

The structural test cannot tell *"expensive to produce"* from *"contains an
expensive op upstream but is cheap to recompute"*. It also cannot tell a
big-cheap result from a small-expensive one — exactly the distinction that
matters. Concretely, two results in the same lineage:

- the 2.5M-row computed-column intermediate: ~150 MB, recompute ≈ re-reading the
  source (I/O-bound on the same bytes) — caching saves ~nothing;
- the ~6.4k-row route aggregate at the end: <1 MB, recompute requires rescanning
  2.5M rows + join + distance + aggregate — caching saves almost all of it.

Profiling the live path (see the buckaroo xorq-search investigation) showed
per-view cost is dominated by re-compiling the large expression DAG to SQL on
every execute; the win from caching a *reduced* result is that the viewed/diffed
expression collapses to `read_parquet ⋈ read_parquet`, cheap to both compile and
execute. The win exists only for results that reduce data; it is absent for
passthroughs whose output is as large as their input.

## Decision

Replace the structural admission test with a measured cost-vs-size model, and
move the admit/skip decision off a per-entry gate and onto a budget with
cost-aware eviction.

1. **Measure at build, always.** Every entry is executed on add (we never add an
   expression without executing it; unbound expressions are out of scope), so
   the cost is already available. Record both `execute_seconds` (already in
   `manifest.json`) and a new `compile_seconds` — the expr→backend-plan step
   (the `to_sqlglot`/sqlize path), timed separately from execution. Record the
   materialised result's size in bytes.

2. **Admit permissively; do not gate per entry — reached by shadow-then-flip.**
   The target is no structural or cost-floor admission test: any built result is a
   cache candidate, ranked by value-per-byte. But #74 shipped the structural
   `classify_build` / `cache_worthy` test as the live admission rule, so rather
   than remove it blind, keep it live and compute the measured value-per-byte *in
   shadow* alongside it (the admission-decision telemetry — staged plan deliverable
   0). Watch the disagreement set — entries `cache_worthy` admits that the cost
   model rates near-zero (the 2.5M intermediate) — and flip to measured-only when
   the data confirms it. This still supersedes #12 (no point robustifying a
   classifier we are removing); it just removes it on evidence, not on first
   principles.

3. **Rank by value-per-byte.** A cache's value is what reading it back saves
   over recomputing: `value ≈ recompute_cost − cache_read_cost`, divided by
   bytes occupied. This declines to ~0 for cheap row-wise passthroughs whose
   output ≈ input size (the 2.5M intermediate) and is high for reducing /
   aggregating expressions (the 6.4k aggregate). `recompute_cost` is
   `compile_seconds + execute_seconds`; `cache_read_cost` is estimated from the
   result's byte size.

4. **Bound by a budget relative to raw data.** Cap the project's total result
   cache at K× the size of `data/` (start at 2×, matching #10). When the cache
   exceeds the budget, evict lowest value-per-byte first. Evicted results
   self-heal: `ensure_result` / `cached_result_expr` recompute from the build on
   the next miss. This is the eviction half of #10; admission and eviction now
   share one cost model.

5. **Key on `content_hash`, never mtime.** The cache key is the entry's
   `content_hash`, frozen at build time (`build.py:271`) and stable across
   reset-to and reload. No modification-time comparison appears on the
   correctness path. (`content_hash` being a *content*-stable identifier is the
   subject of the related ADR.)

Sub-expression / "cache at the divergence point" caching is explicitly **out of
scope**. In the motivating lineage the divergence point is the 2.5M-row join —
big and cheap to recompute — so caching there is the opposite of what the cost
model wants. v0 caches per-entry results only.

## Alternatives considered

- **Keep the structural classifier (status quo).** Mis-caches the
  iterative-computed-column workflow (Join upstream ⇒ every descendant worthy)
  and cannot separate big-cheap from small-expensive. This is the bug.
- **Make the structural classifier robust (#12).** Fixes silent
  misclassification from regex drift but keeps the structural model, so it still
  caches the 2.5M intermediates. Necessary only if we stay structural; this ADR
  removes that need.
- **Cache at the divergence / lowest-common-ancestor sub-expression.** Would
  materialise the large shared intermediate (2.5M-row join) once and let both
  metric variants read it. Rejected: that intermediate is precisely what we do
  *not* want cached — it is large and cheap to recompute, and the variants
  diverge *on* it (haversine vs manhattan), not below it.
- **Per-entry cost-floor admission gate.** Redundant once a budget plus
  value-per-byte eviction exists; a permissive admit + eviction expresses the
  same intent without a second threshold to tune.

## Consequences

- Storage is bounded by a multiple of raw data size instead of growing with edit
  count. The iterative-column workflow stops bloating the cache.
- route_distance diffs stay fast: both 6.4k endpoints are small and
  high-value, so they are cached, and the diff is `read ⋈ read`.
- Requires new instrumentation: `compile_seconds` in the build path and result
  byte-size in the manifest (the admission-decision telemetry — staged plan
  deliverable 0). Accurate cache accounting is a prerequisite for eviction — and
  post-#74 the `/api/disk_usage` pill points at the now-empty `result_cache/`
  rather than the `compute_cache` where baked results actually live (the #74
  disk-usage follow-up), so it must be corrected to count `compute_cache`.
- Eviction depends on correct recompute self-healing, which already exists — with
  one soundness hole #74 widened: an entry with *execution* nondeterminism
  (`.sample()`, `ibis.now()`, unordered `.limit()`, impure UDF) recomputes to
  *different* bytes than were evicted, silently, because its `content_hash` is
  structural and can't see the difference. Such an entry must be retained, not
  evicted-and-recomputed. Tracked as #83.
- The `value ≈ recompute_cost − cache_read_cost` estimate is a heuristic; the
  read-cost term is approximated from bytes rather than measured. Acceptable
  because misranking only costs a recompute, never correctness.
- Scope is `result_cache` only. Buckaroo's own stat caches
  (`.buckaroo_stat_cache`, `diff_stat_cache`) are out of scope; tallyman will
  only hint to Buckaroo (separate sketch).
