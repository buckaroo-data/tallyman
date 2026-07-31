# ADR: Reads load the frozen build — the #163 read-path decisions

- **Status:** Accepted (2026-07-30) — decided in the grilling session over
  `docs/system-contract.md`. Supersedes the read-path behavior introduced by
  #74/#75 (recipe re-import on read). All follow-up questions resolved
  (D9–D12, and the D5 amendment); implemented on PR #167.
- **Context ticket:** buckaroo-data/tallyman#163 (read path reconstructs from
  `expr.py`; a content hash does not name a fixed result). Normative design:
  `docs/system-contract.md`. Bug-class survey:
  `plans/cache-soundness-audit.md`.
- **Affected code:** `src/tallyman_xorq/result_cache.py` (canonical read,
  facade internals, verify), `src/tallyman_xorq/io.py`
  (`tracked_expr_from_alias` / `pinned_expr_from_alias` chaining),
  `src/tallyman_xorq/portable.py` (deep cache-dir rewrite),
  `src/tallyman_core/manifest.py` (`snapshot_key` field),
  `src/tallyman_xorq/staleness.py` (verify sweep),
  `src/tallyman_companion/app.py` + `diff.py` (composition rebind),
  `src/tallyman_companion/buckaroo_lifecycle.py` (pre-heal removal).
- **Related ADRs:** `plans/adr-source-identity-content-hash.md` (CAS — why
  content lives in the path), `plans/adr-result-digest-canonical-ordering.md`
  (the digest whose ordering claim D5 re-scopes).
- **Evidence:** #163's verification on the `taxi2` corpus (frozen builds
  execute to distinct correct results; composition after rebinding works;
  warm and cold runs byte-identical);
  `tests/test_lineage_faithful_reads.py` — eight invariant tests red on CI
  (PR #167) against `main`; and the #171 probe (3 distinct digests over 3
  heals at 250k rows) that forced the D5 amendment.

## Problem

An entry has two representations: the recipe (`expr.py`, binds by name) and
the frozen build (`xorq_build/`, binds by value). On `main`, every in-process
read re-executes the recipe, so alias names re-resolve to their live heads at
read time and a historical entry silently materializes today's data under its
recorded hash. The full statement of the intended semantics is
`docs/system-contract.md`; this ADR records the implementation decisions for
restoring them.

## Decisions

### D1. One PR, one rebuild

The read-path fix, the chaining change (D4), and the canonical-sort bake (D5)
land together on PR #167, flipping the eight red tests green in a single
change, followed by one corpus rebuild (chaining and the sort both change
content hashes). *Rejected:* staged landing (read-path first, hash-changing
PRs after) — smaller diffs and easier bisection, but two rebuild events and an
interim state where builds are read but not yet self-contained.

### D2. Keep the facade; replace the internals

`cached_result_expr(project, hash)` and `baked_snapshot_path(project, hash)`
keep their names and signatures; their internals become the canonical read —
`ensure_expanded_build` → `load_expr(expanded, cache_dir=…)` → tallyman's
deep cache-dir rewrite — with the snapshot path derived from the *loaded*
expression (the same derivation the build uses). The `cache_dir` cold seam
lives on a lower-level read function the facade wraps. The in-process LRU
stays, now sound: its `(project, content_hash)` key finally determines its
value. *Rejected:* a new `read_entry()` API with ~14 call-site migrations —
cleaner name, same semantics, larger diff for no behavioral gain.

### D3. Rebind composition onto the default backend, with a one-group guard

Composing loaded builds (diff, chaining) collapses backends via
`replace_sources` onto xorq's process default backend. The contract's
"one backend per distinct content profile" is enforced as an assertion:
group profiles by `tokenize(profile minus idx)`; if more than one distinct
group ever appears, fail loudly rather than misbind. *Rejected:* a fresh con
per composition (setup cost, no sharing) and a full per-profile registry
(machinery with exactly one occupant — every profile in every catalog today
is content-identical `xorq_datafusion`).

### D4. Chaining inlines the parent's cache node

`tracked_expr_from_alias` composes the parent's loaded build — cache node
included — rebound onto the default backend. Child builds become
self-contained and self-healing: a cold read regenerates ancestor snapshots
through ordinary cache mechanics. The `ensure_session` pre-heal (a
discarded-result call whose only purpose was forcing ancestor snapshots onto
disk before Buckaroo's replay) is deleted, along with the #75 stripping.
Cost: builds now contain *nested* cache nodes, which xorq's shallow
load-time cache-dir rewrite cannot reach — tallyman ships a deep rewrite
over xorq's `walk_nodes`/`replace_nodes` (the traversal that descends into
`CachedNode.parent`-style opaque fields). Child hashes change; the corpus
rebuilds. *Rejected:* keep stripping — no nesting and no rebuild, but builds
stay non-self-contained and the pre-heal choreography stays load-bearing
forever.

### D5. The bake is canonically ordered: sort-by-all-columns (amended)

**Amended 2026-07-30, same day, on probe evidence.** As originally decided,
the order-insensitivity claim for parquet-sourced worthy entries was
withdrawn rather than fixed: digests documented as order-sensitive, heal-drift
warnings advisory, #171 + provocation tests capturing the residual — with
sort-by-all-columns and a multiset digest noted as the escape hatches "if the
advisory fires in practice."

It fires on every heal. The #171 probe (250k rows, above DataFusion's 1 MiB
`repartition_file_min_size`) produced three distinct digests over three heals,
none matching the build. At that rate the advisory posture is untenable: D10
would wipe stat caches on every heal and D12 would pin and badge every large
parquet-sourced entry, drowning the signal both exist to carry.

So the escape clause is exercised: `rewrite_for_build` imposes a canonical
total order under the result-cache node before every bake. Key priority: the
author's own top-level `order_by` keys stay primary (served row order is part
of what they asked for), then `original_row_order` (the canonical CSV file
order), then the remaining sortable columns in schema order; nested/geospatial
columns are skipped as keys (rows still tied after all sortable columns are
byte-identical, so residual ties can't change the file's bytes). The sort node
lands in the build, so the build's bake and every later heal execute the same
ordered plan — the probe test now pins byte-equality across heals instead of
probing for drift. `result_digest` keeps its file-hash definition and is sound
for every worthy entry, CSV- and parquet-sourced alike. *Rejected:* the
multiset digest (order insensitivity by definition, but verify must read every
row and the digest stops being a hash of the artifact). Cost accepted: a sort
on every bake/heal — D4 already forces the corpus rebuild this rides on.

### D6. A missing or unloadable build is a hard error

Serving results for an entry whose `xorq_build/` is absent or fails to load
raises, naming the entry and the remedy (rebuild). There is no automatic
recipe fallback — not even env-gated. Recipe re-execution survives in
exactly two places: minting (build/revise/recalc) and the
structural-nondeterminism diagnostic. This amends the contract's original
"fallback, loudly" clause: a warning on a background read is how #163-class
behavior stays invisible, and the fallback still carried the audit's W1
project-binding bug.

### D7. Verification runs in production and is loud

`verify_result_faithful` re-anchors to "executing this entry's frozen build
reproduces its recorded digest." Wiring: every self-heal verifies before the
result is served, with failures written durably to `errors.jsonl` and pushed
over SSE so the UI can badge the entry; `catalog_scan_staleness` gains an
opt-in `verify_results=True` corpus sweep. *Rejected:* verify-on-every-read
(a full file hash on the hottest path, guarding against drift only heals can
introduce) and log-only surfacing (invisible by construction).

### D8. The manifest records the snapshot key; reads assert it

The build writes the baked snapshot's cache key into the manifest; the
canonical read asserts its own derivation matches and raises on mismatch.
With build and read now sharing one derivation route the original
disagreement mechanism is gone; the recorded key is a tripwire that turns
any future divergence (an xorq tokenization change under an upgrade, a
rewrite drift) into an immediate, attributable failure instead of a silent
wrong-file read.

## Consequences

- **Retired:** read-time recipe re-import and its pins (`_RECON_SOURCES`
  stays only for the diagnostic path), the #74 cycle-walk's read-time role,
  #162's proposed `_RECON_PARENTS` (never built), the #75 stripping, the
  `ensure_session` pre-heal.
- **Corpus rebuild** required at landing (D4 changes child hashes); per
  project policy there is no migration path. `diff_stat_cache/` is wiped and
  Buckaroo bounced at the same time (its current contents were computed over
  #163's self-join diffs).
- **Buckaroo's caches become sound again:** with the canonical sort (D5,
  amended) and verify enforced, a faithful heal is byte-identical for every
  worthy entry — CSV- and parquet-sourced alike — so expression-keyed stat
  caches over stable paths are honest. The unfaithful residual (a genuinely
  nondeterministic recompute: an impure UDF, source drift under `off`) is the
  D10/D12 path, not a routine occurrence.
- **The audit's W5 (MCP-process invalidation) largely dissolves:** cached
  values become pure functions of immutable builds, so cross-process
  staleness reduces to the existence checks the read already performs.
- `docs/system-contract.md` is amended where these decisions sharpen it
  (fallback clause → hard error; open questions 1, 3, and 5 resolved).
  `docs/caching.md` / `expression-lifecycle.md` / `architecture.md` get
  corrected when the fix lands, as planned.

## Post-acceptance decisions (grilling round 3, 2026-07-30)

The four items originally left open here were resolved in the final round:

### D9. No cheap-entry digests

Cheap entries record no `result_digest`. With reads loading builds, a cheap
entry's bytes are the deterministic pushdown of a frozen graph over
content-pinned clones — no snapshot to drift, no second manufacture point —
so a digest would cost a canonical materialization at build and at every
verify to audit a hole that no longer exists. Resolves the contract's open
question 4.

### D10. An unfaithful heal wipes the entry's Buckaroo state

When a heal fails verification, the fix PR wipes that entry's
`.buckaroo_stat_cache` and evicts its Buckaroo session, so stale stats never
render beside fresh rows while buckaroo#955/#956 await a wheel bump.
Faithful heals need no wipe: enforced verification makes them
byte-identical, which is exactly the assumption Buckaroo's caches key on.

### D11. Second red-test commit before the fix

PR #167's failing suite is extended with the newly-decided behaviors before
the fix commit: hard error on a missing build (D6), a cold load of a
chained child's build healing the parent snapshot with no pre-heal (D4),
and the D10 wipe. The #171 provocation probes ride in a separate commit —
they may pass on `main` (whether the reorder fires at test scale is the
empirical question they exist to answer), so per the failing-tests
discipline they don't belong in the red commit. Tests that cannot fail
first (the D8 snapshot-key assert needs the new manifest field) ride with
the fix commit.

### D12. Unfaithful entries are pinned and badged

An entry whose heal fails verification has bytes that are not regenerable:
it is marked non-evictable (its snapshot is retained, not treated as
reclaimable cache) and surfaced with a UI badge via the D7 error record —
the #83 position. Full bullpen/eviction wiring is deferred; the marking and
badge land with the fix. Resolves the contract's open question 7.

Two defaults adopted without ceremony: `diff_stat_cache/` joins
`_invalidate_reset_caches` and the reset prune (size caps deferred),
resolving the contract's open question 8; and #166 (`pinned_expr_from_alias`
version references) lands as its own PR after the fix.
