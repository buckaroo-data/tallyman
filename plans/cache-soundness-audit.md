# Cache-soundness audit: #163's bug class across tallyman and Buckaroo

- **Status:** Research note (2026-07-30). Companion to `docs/system-contract.md`
  (the proposed contract) — this is the inventory of where the codebase
  currently violates it *beyond* the instances #163 already lists.
- **Template:** the #163 disease — a cache or code path whose key claims
  immutability while its value depends on mutable state; read-time resolution
  of a live name on behalf of a frozen artifact; self-heal that manufactures
  data without verification; one question answered by two disagreeing paths.
- **Method:** two exploration passes (tallyman `src/`; buckaroo **0.15.4**
  wheel in the tallyman venv, byte-identical to `~/buckaroo` @ `3313534a` —
  note `~/code/buckaroo` has uncommitted server edits that tallyman does NOT
  run). Findings marked **[verified]** were re-checked directly in source;
  the rest are careful code reads with citations, not yet reproduced live.
- **Do not re-file #163's known set** (recipe re-import, live-head alias
  resolution, `_resolve_result_plan`/`_build_compare_expr` LRUs,
  `diff_stat_cache` contents, `_loaded_diff_sessions`). Everything below is
  additional.

## Tallyman — wrong-data findings

### W1. Recipes bind to the *active project* at read time [verified]

`read_project_file(rel_path, project=None)` → `resolve_project(project)`
(`io.py:59`); same in `tracked_expr_from_alias` (`io.py:680`) and
`pinned_expr_from_alias` (`io.py:718`); `build_diff_expr` — the recipe body of
every promoted diff entry — calls a bare `resolve_project()`
(`companion/diff.py:226`). `_recipe_expr` execs `expr.py` with no project
binding (`result_cache.py:184-236`); only `${TALLYMAN_PROJECT_ROOT}` string
substitution carries project identity, and a relative
`read_project_file("x.parquet")` has no placeholder. The #115 digest pin even
disables itself cross-project: `_reconstructing_source_digest` returns `None`
on project mismatch (`io.py:630-647`) and falls through to the live path.

**Predicate:** any read where `resolve_project() != <project argument>` — two
browser tabs on different projects, a tab left open across a switch, any MCP
call after an external switch (see W8). Same relative path exists in the
active project → the *other project's rows* served under this project's URL;
else `ProjectDataNotFound`. Reachable from `/api/data`, the `ensure_session`
pre-heal, `/api/diff_data`, and every MCP reconstruction consumer.

This is #163's shape on a second axis: alias heads were one mutable name in
the recipe; the project is another. The contract's I4 ("names resolve exactly
once") must cover both — and does, once reads load builds, because the build's
expanded paths are absolute. The *fallback* recipe path keeps this hole.

### W2. `_resolve_result_plan` memoizes the active project into its value

Key `(project, content_hash)` (`result_cache.py:440`), but the `"recompute"`
branch stores an expression built under whatever project was active at fill
time (W1). Neither switch path clears it: `api_projects_switch`
(`app.py:1790-1808`) and MCP `project_switch` (`server.py:1561-1565`) clear
nothing. Fill `(A, H)` while B is active, switch back to A: every read of
`(A, H)` serves B-flavored results for the process lifetime (the recompute
branch has no exists-check to trip a heal).

### W3. `tallyman_read_csv` sits entirely outside source identity [verified]

- Intermediate keyed `md5(abs_path | schema | scan_kwargs)` — content-blind by
  documented intent (`io.py:515-527`); refreshed by an mtime sidecar via
  **in-place `os.replace` of the same path** (`io.py:575-578`).
- Never calls `si.note_source` (only `read_project_file` does, `io.py:76,81`)
  → `manifest.sources = None` for every CSV-rooted entry.

Three consequences: **(a)** edit the CSV, resubmit the same recipe → same
`Read` path → same `content_hash` → the build dedups to the stale entry
(`build.py:447`), exactly the `test_append_invalidates` failure `cas` was
built to close — and `salt` can't rescue it (`build.py:441` gates on
`and sources`, which is empty). **(b)** after re-materialization, every
historical entry rooted in that CSV reads the new intermediate — retroactive
mutation under fixed hashes, unverified (W6). **(c)** staleness is blind:
`sources_of` → `None` → the source axis lands in `unknown_axes`
(`staleness.py:96-98`), so scans, recalc, and the UI report CSV-rooted entries
permanently clean. CSV is the *mandated* ingest path (raw `deferred_read_csv`
is a build error), and no test covers a CSV content change.

Also the root of a real cross-project hash collision: the intermediate lives
in `TALLYMAN_HOME/csv_ordered/` (project-independent by design,
`io.py:499-512`), so the same recipe over the same CSV in two projects builds
the **identical content hash** — which arms B1 below.

### W4. Primary-key inheritance resolves lineage by alphabetical accident

`_parent_hash` calls `previous_version(project, hash)` with no alias hint
(`primary_key.py:61-69`); `version_of_hash`'s fallback scans aliases in dict
order, and the file is written sorted, so a hash in ≥2 alias histories
inherits from the **alphabetically-first** alias's lineage (`aliases.py:101-117`).
The inherited key is validated only by column-name membership, never
uniqueness (`primary_key.py:98`). A non-unique inherited key becomes the diff
join key → outer-join fan-out and mislabeled add/delete/change rows.
Multi-history hashes are ordinary (`catalog_alias` points new aliases at
existing hashes; `catalog_rename` reorders the scan). `primary_key.json` is
gitignored and restored verbatim from the bullpen, so `reset_to` cannot roll
it back.

### W5. The MCP process never hears any invalidation

`reset_to` clears the plan LRU in its own process; the companion clears via
`/internal/notify`; the MCP server only POSTs *out* — nothing posts in. A
long-lived MCP process that memoized a cheap entry's plan keeps serving the
old alias resolution after any browser/CLI reset. (Under the contract this
memo becomes pure and the issue reduces to existence-staleness, but the
process-boundary gap is worth naming in the fix.)

### W6. Digest checked only inside the heal branch; delete leaves stat caches

The self-heal compares against `result_digest` only when *this process* healed
(`result_cache.py:547-569`); a snapshot already on disk, or healed by a peer,
is never checked. `verify_result_faithful` has no caller in `src/`.
`DELETE /api/result_cache/{hash}` unlinks the snapshot but clears neither
`.buckaroo_stat_cache` nor the live Buckaroo session (`app.py:1509-1536`) —
after a heal to different bytes, the grid's stats describe the pre-delete
bytes (see Buckaroo #1: nothing on the Buckaroo side will notice either).

### W7. Row counts are split-brained

`api_data`'s `total` and `api_entry_detail`'s `total_rows` come from the
manifest; the rows come from a live execute (`app.py:884-893`, `:968`);
`schema_diff`'s before/after counts are build-time while diff stats are live
(`diff.py:114-129`). Any W1/W3/W6 drift shows header N over grid M.

### W8. The project-change guardrail is dead code

`_tag_project` seeds `_mcp_active_project` on first call and thereafter
compares a constant to itself (`server.py:104-122`), so the design-decision-14
"active project changed under you" warning can never fire — the exact
condition that arms W1 goes unreported.

## Tallyman — secondary findings

- **B1. Buckaroo session map is cross-project on a false premise.**
  `_sessions` keys by content hash only, justified "bytes identical by
  construction" (`buckaroo_lifecycle.py:528-531`) — W3 breaks the premise via
  `csv_ordered`. When two projects share H: B's grid uses A's session, A's
  stat-cache dir, A's telemetry; `reload_project_sessions(B)` filters on the
  *recorded* project and skips H, so B's klass changes never reload B's grid.
- **B3. Buckaroo never finds project klasses [verified].** Tallyman passes
  `project_root = artifacts_dir(project)` (`buckaroo_lifecycle.py:598`);
  Buckaroo scans `<project_root>/stats/*.py` and returns silently on a missing
  dir (`xorq_loading.py:154-155` in the wheel); tallyman writes stats to
  `catalog_dir/stats` = `artifacts/catalog/stats` (`paths.py:245-252`). The
  `/reload_expr` POST carries no body to compensate. Project-authored summary
  stats and grid post-processing klasses appear to be invisible to the grid
  always. (The post-processing *tab* uses tallyman's own server-side runner,
  which is why the feature can look alive. Needs one end-to-end check:
  `catalog_add_summary_stat` → does the stat render in the grid?)
- **B5.** `ensure_expanded_build` trusts the `.complete` marker without
  checking the directory exists (`portable.py:105-107`); `ENTRY_CACHE_NAMES`
  names the dir but not the sibling marker, so cache-filtering copiers can
  preserve the marker and drop the dir → unhealable empty grid.
- **B4.** `BuckarooManager._expanded_dirs` is never written — dead
  bookkeeping (`buckaroo_lifecycle.py:147`).
- **R1.** Display klasses live outside the catalog repo and the tracked
  surface but their MCP tools checkpoint — `reset_to` claims to undo a step
  that still renders. Also written non-atomically.
- **R2.** `_force_source_rehash` mutates `os.environ` process-wide from
  threadpool routes (`staleness.py:62-72`); two overlapping scans can drop
  the rehash flag mid-loop and pass off a same-stat in-place edit as fresh.
- **R3.** `source_digests.json` read-modify-write is non-atomic and
  two-process (companion + MCP); a torn file degrades to full rehash (perf
  only).
- **R4.** `.cas` `.tmp` leftovers are never GC'd (`gc_cas` matches on stem).
- **Reset survivors:** across every reset path, these carry stale state by
  construction — MCP-process LRUs (W5), `primary_key.json` (W4),
  `.buckaroo_stat_cache` and `.xorq_build_expanded` (restored verbatim from
  bullpen), `diff_stat_cache/`, `csv_ordered/`, `/tmp/tallyman_diff_builds`.
  Project **switch** invalidates nothing at all, in either process.

## Buckaroo (0.15.4) — "is buckaroo doing something stupid with caching?"

Yes — the worst instance is #163 verbatim, and it sits directly under
tallyman's grid.

1. **Summary-stats snapshot cache is path-keyed over mutable bytes.**
   Buckaroo caches per-column stats in a xorq `ParquetSnapshotCache`
   (`xorq_loading.py:43-45`) whose key is the stat query's *expression token*
   — structure + leaf path strings, no content, no mtime; xorq documents the
   strategy as "source data changes do not invalidate cached results." The
   lookup is filename-existence (`xorq_stat_pipeline.py:197-209`), and the
   local docstring's claim that the key is "content-addressed on `query`" is
   false. **Predicate:** same build dir + same stat-cache dir loaded twice
   with any reachable parquet's bytes changed at a stable path between loads
   — exactly what a tallyman snapshot self-heal produces (stable expanded
   build path, stable `entry_stat_cache_dir`) — serves the previous dataset's
   min/max/nunique/histograms beside the new rows, counted as a cache *hit*.
   Under the tallyman contract this is masked while I1 holds upstream (bytes
   never change under a hash) but is armed the moment any heal drifts:
   tallyman's failure containment ends at the `/load_expr` boundary.
2. **The JS row cache's data-identity signal is never armed.** Client row
   caches partition on `outside_df_params`, whose one data-identity field,
   `dataframe_id`, is passed by no caller in server mode
   (`BuckarooView.tsx:287-299`). On `/reload_expr` (which tallyman issues on
   every klass hot-reload) the server resets session state to defaults and
   rebroadcasts, the recomputed params are byte-identical, no purge fires,
   and `SmartRowCache` serves the previous expression's rows from memory
   under new stats. One prop (a server load-generation counter) closes it.
3. **`/reload_expr` silently drops config.** The rebuilt dataflow loses
   `cache_storage_path`, `column_config_overrides`, `extra_grid_config`
   (`handlers.py:807-808`) — after any reload, stat caching is off and the
   caller's column config is gone. Tallyman reloads on every stat/klass
   change.
4. **`_expr_count_cache`** (`xorq_buckaroo.py:44-70`): a WeakKeyDictionary
   keyed by immutable *expressions* caching data-dependent *counts*; while a
   session holds the expr the entry never dies, so a byte change under a
   stable path serves stale `total_rows`/scroll bounds. Same key-vs-value
   confusion as #1 in miniature.
5. **Warm-session short-circuit compares a path string** (`handlers.py:451-470`):
   same `session` + same `build_dir` path with rewritten contents → old
   expression served. Dormant for tallyman (it never reuses session ids) but
   it is the same disease server-side.
6. **Pandas-path caches** (not on tallyman's xorq path): the series-stat LRU
   stashes a hash on the Series object and never rehashes after in-place
   mutation, and equality is a wraparound *sum* of row hashes (permutations
   collide) (`utils.py:85-123`); the file-metadata cache validates by
   `current_mtime <= cached_mtime` (same-mtime swap and backwards-mtime both
   pass); failed column hashes all merge into one row keyed `0`
   (`paf_column_executor.py:225-231`).
7. **Sound, notably:** the `(id(df), id(klasses))` dedup — the previous
   object is still strongly referenced while the new one is constructed, so
   id-reuse is unreachable; unsound only under in-place mutation, which the
   xorq path can't do.

## Reading the pattern

Every wrong-data finding above is one of three shapes: (1) a key that omits a
real dependency (path-keyed stats over mutable bytes; content-blind CSV keys;
hash-only session maps crossing projects), (2) a name resolved after binding
time (active project in recipes; alphabetical alias lineage; live heads —
#163 itself), or (3) a regenerated artifact served unverified (self-heal,
`recon_cas_path`'s live fallback). The contract in `docs/system-contract.md`
(I1–I5) eliminates shape 2 and 3 for the alias axis by construction; this
audit shows the same rules must also be applied to the **project** binding
(W1/W2/W8), the **CSV ingest** identity (W3), and the **Buckaroo handoff**
(whose caches assume I1 holds upstream — fix #1/#2 there or wipe stat caches
whenever a heal writes).

## Triage order

1. **W3** — put CSV content into the identity (digest into `_ordered_csv_key`,
   `note_source` from `tallyman_read_csv`): fixes stale-dedup, retroactive
   mutation, staleness blindness, and disarms B1's cross-project collision.
2. **W1/W2/W8** — thread `project` explicitly into recipe execution (or a
   contextvar pinned for the exec), clear plan LRUs on project switch, revive
   the change warning. Fold into the #163 read-path fix, which removes the
   primary consumer of the broken binding.
3. **B3** — one end-to-end check, then point `project_root` at
   `catalog_dir(project)`.
4. **Buckaroo #1/#2/#3** — fold a content signal into the stat-cache key (or
   have tallyman wipe stat caches on any heal-write), pass a load-generation
   `dataframe_id`, persist config across `/reload_expr`.
5. **W4** — pass the alias hint into `_parent_hash`; validate inherited keys
   for uniqueness.
6. **W6** — verify digests on read (not just own-process heal); make the
   result-cache DELETE also clear stat caches + sessions.
