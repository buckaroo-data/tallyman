# Plan: native `tallyman_core.catalog` — drop xorq's catalog package

Status: proposed (2026-06-16). Refined through a parallel code investigation and
a grilling pass; the decisions below are settled unless marked open.

## Problem

tallyman drives xorq's `catalog/` package by subprocess from
`tallyman_core/xorq_catalog.py` (`uv run xorq catalog add / add-alias /
remove-alias`, all `--no-sync`). That package is the source of the recurring
catalog pain:

- **`assert_consistency` runs on every add, independent of `--no-sync`** (xorq
  `catalog.py:335`, and unconditionally in `CatalogEntry.__attrs_post_init__`
  `1212-1213`). It asserts the tracked tree equals exactly `catalog.yaml` + each
  entry's zip/metadata + alias symlinks, so any *extra tracked file* makes it
  raise. This is #48, and the reason every piece of tallyman state had to be
  smuggled into `catalog.yaml` keys instead of living in its own files.
- **The add subprocess commits outside tallyman's flock**, racing
  `checkpoint_catalog`'s `.git/index.lock` (`catalog_state.py:383-388` already
  apologizes for it and silently returns `None` when it loses).
- **#48 is live on disk**: `first-project` has four build dirs but only one
  tracked recipe zip — three adds silently no-opped. Reset on that corpus
  already fails `_assert_catalog_consistent`. A rebuild is required, which the
  single-user no-migration rule (project `CLAUDE.md`) already permits.

xorq splits into three layers; only the innermost hurts. We cut **(1)** and keep
**(2)/(3)**:
1. **Catalog repo manager** — `xorq/catalog/` (~7.7k LOC), driven by subprocess.
2. **Serializer** — `xorq/ibis_yaml/` (`build_expr`/`load_expr`).
3. **Engine** — `xorq.vendor.ibis` + datafusion + `xorq.expr`.

## Scope: two cuts, one PR

### Cut A — the WRITE cut
Delete `xorq_catalog.py`; stand up `tallyman_core/catalog.py` as the native
content-addressed recipe store; and **decompose `catalog.yaml`** so every piece
of mutable catalog state becomes its own git-tracked file. The #48 unlock
(tracking arbitrary files) is what makes the decomposition possible.

### Cut B — the RUNTIME cut
`import xorq.api` transitively loads the entire `xorq.catalog` package
(`xorq/api.py:65 import xorq.catalog.api as catalog`; verified: bare
`import xorq` loads zero catalog modules, `import xorq.api` loads 12). Replace
tallyman's ~9 `import xorq.api as xo` sites with narrow catalog-free imports plus
a datafusion `connect()` shim.

These must land in the same PR (ordered commits): if only Cut A lands the package
stays resident; if only Cut B lands the `uv run xorq catalog` child still runs it.

## Ownership: `tallyman_core.catalog` and the xorq boundary

This is not a vendored copy of xorq's `catalog.py`. tallyman already owns the
catalog *model*; the only remaining xorq tendril is the `xorq_catalog.py`
subprocess. `tallyman_core/catalog.py` is the store, written natively. We **drop**
xorq's `assert_consistency`, three-way merge, and zip contract — none carry
forward.

After this PR, tallyman code touches xorq's catalog **never**:

| Touchpoint today | After |
|---|---|
| `xorq catalog add` writes the zip | stdlib writer in `tallyman_core/catalog.py` |
| xorq commits zip + metadata + alias symlinks | tallyman's checkpoint is the only committer |
| `assert_consistency` on every add | never invoked against our repo |
| three-way merge | already dead (no remote, `--no-sync`); stays dead |
| zip internal format / `REQUIRED_ARCHIVE_NAMES` | irrelevant — nothing reads the zip back; format is ours |
| xorq's `entries`/`aliases` `catalog.yaml` keys | not written; nothing reads them |
| old on-disk corpus | rebuilt, never read |

The honest boundary: this severs the *catalog*, not the *engine*. Entry contents
are still xorq build artifacts (`build_expr`/`load_expr` produce the dir we zip;
`vendor.ibis` runs it), all catalog-free. The one asterisk on "never": a
`from_catalog` reconstruction re-execs a recipe whose `import xorq.api as xo`
*passively loads* (never calls) the catalog package — see Decision D4.

## Confirmed facts the design rests on

1. **Nothing reads the zip back.** The only consumer of `entries/<hash>.zip` is
   `_tracked_recipe_hashes` (`catalog_state.py:433`), which reads *names* via
   `git ls-files`. `load_expr` consumes the expanded build *dir*, never the
   archive. So the zip format is entirely ours.
2. **Two load paths read different files.** `from_catalog`/chaining
   (`cached_result_expr` → `_recipe_expr`, `result_cache.py:148`) re-imports
   `entry_dir/expr.py`; the Buckaroo viewer (`load_entry`) reads
   `entry_dir/xorq_build/`. Both `expr.py` and `xorq_build/` must therefore be in
   the durable artifact — today's zip has only `xorq_build/`, so the durability
   invariant currently guards the wrong file (Decision D3 fixes this).
3. **`run_git` uses `os.posix_spawn`, not `subprocess.run`** (`git_util.py`), so
   the clean "no subprocess" guard test is: monkeypatch `subprocess.run` to
   raise, run a full cycle, assert no raise.
4. **tallyman never merges** (no remote, `--no-sync` everywhere). No merge
   surface to port.
5. **`pyproject.toml` can't be narrowed** — `xorq.catalog` ships in the single
   `xorq==0.3.26` wheel. "Drop" means stop importing/calling, enforced by a test.
6. **`.xorq_build_expanded/` is derived + machine-local** (`portable.py`): the
   placeholder-expanded copy of `xorq_build/`, regenerated on load, holding this
   machine's absolute paths. Never archive or track it.

## The design (end state)

### Tracked tree
The catalog repo gets a `.gitignore` (there is none today; only an explicit
pathspec kept staging honest). Ignore the heavy/derived artifacts:

```
entries/*/            # untracked build dirs (recipe lives in entries/*.zip)
bullpen/
result_cache/
compute_cache/
diff_stat_cache/
.checkpoint.lock
```

Then `git add -A` stages exactly the meaningful state, and the tracked tree is:

```
entries/<hash>.zip            # immutable recipe, one blob per entry
aliases.jsonl                 # one line per alias
notebook.jsonl                # one line per cell
entries.jsonl                 # pointer list: untracked build dirs (bullpen reconcile)
compute_cache.jsonl           # pointer list: untracked compute-cache warm set
chart_specs/<hash>.vl.json    # one Vega-Lite spec per entry
display_configs/<hash>.json   # one Buckaroo/display config per entry
prompts/<hash>.jsonl          # append-only authoring-prompt history per entry
post_processing/<name>.py     # project-global functions (moved under the repo)
stats/<name>.py               # project-global functions (moved under the repo)
```

No YAML anywhere. The former `catalog.yaml` is gone; its residual pointer
bookkeeping is `entries.jsonl` + `compute_cache.jsonl`.

Because `git add -A` is allow-by-default, the `.gitignore` denylist alone does
not keep the tracked tree honest — `assert_catalog_consistent`'s path allowlist
(Module boundary, #52) does. (The `metadata/` `*.zip.metadata.yaml` sidecars and
the `aliases/` symlink tree that `git add -A` would otherwise stage are xorq
`catalog add` subprocess output; they stop being written once Cut A removes the
subprocess, and a from-scratch rebuild never recreates them.)

### The recipe store (immutable)
- `entries/<hash>.zip` archives an **allowlist** of the entry dir:
  `{expr.py, xorq_build/, schema.json, manifest.json}`. Allowlist, not
  "everything except," so a future derived artifact dropped in `entry_dir` can't
  leak into the durable zip. Excludes `.xorq_build_expanded/` (fact 6),
  `result.parquet` (the untracked heavy result), and `prompts.jsonl` — the last
  is append-mutable (it grows on a re-run of an existing hash with no rebuild,
  `build.py:335`), so a content-addressed zip member would churn the blob in git
  history on every re-run; it becomes a tracked decomposition file instead (see
  below).
- Members stored under arcname `<hash>/<relpath>` (single top-level prefix, so a
  future restore can reuse it). **Named by the salted `content_hash`** when
  source-identity salt mode is on, not `build_path.name` — otherwise
  `_tracked_recipe_hashes` (zip names) diverges from `entries.jsonl` pointers.
- **Deterministic archive** (`date_time=(1980,1,1,0,0,0)`,
  `external_attr=0o644<<16`, sorted iteration, `ZIP_DEFLATED` at a pinned
  `compresslevel`) so the blob doesn't churn on identical rebuilds. (Single
  machine, so cross-zlib reproducibility is moot; pinning the level keeps the
  blob byte-stable across same-machine rebuilds regardless of zlib defaults.)
- **Drop the wheel + requirements.txt** xorq injected (~178KB → ~6KB per entry);
  nothing reads them, and the payoff is git *history* (every checkpoint re-stages
  the zip).
- **The checkpoint owns zip creation, not the build.** The build persists a
  complete `entries/<hash>/` on success and cleans up the dir if this call
  created it and the build failed (no partial dirs, no orphan zips). The
  checkpoint, under `_project_lock`, zips any entry dir lacking a tracked zip,
  then `git add -A` + commit. This is the sole git transaction and sole zip
  writer: no out-of-lock writes, no loose orphans, a committed pointer always has
  its recipe in the same commit, and it self-heals (a skipped/failed checkpoint
  leaves a complete gitignored dir the next checkpoint picks up).

### The decomposition (mutable state → tracked files)
The recipe is immutable; charts, styling, post-processing, stats, aliases, the
notebook, and the per-entry authoring-prompt history are **mutable** — edited
(or, for prompts, appended to) on an existing entry without rebuilding, so they
must not live inside the content-addressed recipe zip. Today they round-trip
through `catalog.yaml` (`capture_tallyman_state` reads live files into keys;
`materialize` writes them back) only because `assert_consistency` forbade
tracking extra files. With xorq gone:
- The mutators already write live files (`set_chart`, `set_display_config`, the
  `.py` writers, `set_alias`, the notebook editors). Those files become **tracked
  directly**; checkpoint commits them via `git add -A`; `reset_to`'s
  `git reset --hard` restores them.
- **`capture`/`materialize` for these six sections is deleted.** `capture`
  shrinks to writing `entries.jsonl` + `compute_cache.jsonl`.
- Each mutation is already one checkpoint commit (dispatch-boundary), so the
  revision granularity is unchanged — but a chart revision becomes a legible
  `git diff` to its file instead of opaque `catalog.yaml` churn.
- `aliases.py`'s `_read`/`_write` switch from `catalog_state` keys to
  `aliases.jsonl` (one line per alias: `{"alias","latest","history":[...]}`);
  every other alias function rides on `_read`/`_write` untouched, and the only
  reader off `read_tallyman_state` is `aliases.py` itself (verified).
- `post_processing/` and `stats/` move from `artifacts/<name>` (outside the repo)
  to under `catalog/` so they can be tracked. Layout change, trivially a rebuild.
- **Prompt history is a seventh tracked file, with a different origin.** Unlike
  the six above it never round-tripped through `catalog.yaml` — it has always
  been a loose `entry_dir/prompts.jsonl` (written by `build.py:35`; read back by
  the entry-detail API at `app.py:580` → the React "N prompts seen for this hash"
  disclosure). Because `entries/*/` is now gitignored, that loose file would be
  lost on `reset_to`/clone-restore, and `manifest.json` preserves only the
  *first* build's prompt — so the re-run history the disclosure exists to show
  would be gone. Relocate the `build.py` writer and the `read_prompts` reader
  from `entry_dir` to a tracked root-level `prompts/<hash>.jsonl` (mirroring
  `chart_specs/`). Layout change, trivially a rebuild. `capture`/`materialize`
  do not touch it — it was never in `catalog.yaml`, so the six-section deletion
  above is unaffected.

Atomic reset is preserved: `git reset --hard <step>` restores all tracked files
at once regardless of how many — spreading state across files doesn't weaken it.

### Cut B — catalog-free imports
Replace `import xorq.api as xo` with narrow imports (all verified
symbol-identical and catalog-free): `xorq.ibis_yaml.compiler` (build/load),
`xorq.expr.api` (`deferred_read_*`, `memtable`, `to_sql`), `xorq.caching`
(`ParquetSnapshotCache`). The one non-mechanical swap is **no-arg `xo.connect()`**
(used at `source_cache.py:133,151`), which is bound to `xorq.api.connect` and
absent from `xorq.expr.api`. Add a tallyman datafusion connect shim in
`tallyman_xorq` (`Backend(); do_connect(None)`; returns the identical `Backend`),
with a determinism-equivalence test (the shim-built `ParquetSnapshotCache` must
produce byte-identical content-addressed snapshots, since source-cache keys feed
content-addressing). Sites (8 runtime imports): `source_cache.py`, `io.py`,
`build.py:419`, `result_cache.py`, `post_processing.py`, `summary_stats.py`,
`app.py`, `diff.py`. The 9th grep hit, `tallyman_mcp/server.py:219`, is the
codegen-guidance docstring teaching recipe authors the `import xorq.api as xo`
namespace — it is *not* an executed import; left as-is per D4 (the recipe
authoring surface is a separate follow-up).

Definition of done is **not** a `sys.modules` assertion (user recipes keep
`import xorq.api`, D4). It is: (a) monkeypatch-`subprocess.run`-to-raise guard
over a full cycle, and (b) a static grep that `src/` has no
`import xorq.catalog` / `from xorq.catalog` / `xorq catalog` argv.

## Decisions (resolved in the grilling pass)

- **D1 — zip timing:** checkpoint owns zip creation; build owns only the complete
  entry dir. (Collapses the orphan-zip / `--allow-empty`-masking / partial-failure
  risks.)
- **D2 — `catalog_registered`:** means "entry dir persisted" (a local fact at
  build time); durability is the next checkpoint's job and always succeeds because
  the dir is complete. Rewrite the `#48` "not durable" log accordingly.
- **D3 — archive scope:** the recipe allowlist `{expr.py, xorq_build/,
  schema.json, manifest.json}` as one immutable zip (fixes the wrong-artifact
  durability gap from fact 2). `prompts.jsonl` is deliberately *not* a member: it
  is append-mutable, runtime-read provenance, so it joins the decomposition as a
  tracked `prompts/<hash>.jsonl` rather than churning the content-addressed blob.
- **D4 — recipe imports:** keep user recipes on `import xorq.api as xo`;
  catalog-free guarantee scoped to `src/`. Real recipes call `xo.connect()`/
  `xo.desc`, so switching the authoring surface needs its own codegen-validation
  pass and is a separate follow-up.
- **Decomposition scope:** all six `catalog.yaml`-smuggled mutable sections
  (charts, display, post-processing, stats, aliases, notebook) become tracked
  files, plus a seventh — the per-entry prompt history, relocated from the
  now-gitignored entry dir to a tracked `prompts/<hash>.jsonl`; `catalog.yaml`
  reduces to the two JSONL pointer files. One PR, ordered commits.
- **Formats:** JSONL for list-shaped state (aliases, notebook cells, pointer
  lists); one pretty-printed JSON object per file for nested blobs (chart specs,
  display configs); `.py` for scripts.

## Module boundary

- **`tallyman_core/catalog.py` (new — the store):** `write_recipe_zip` /
  zip-an-entry-dir, `tracked_recipe_hashes`, `assert_catalog_consistent` (#52 —
  enforces the tracked surface as a path **allowlist** (deny-by-default): every
  `git ls-files` path must fnmatch the tracked-surface set or it raises, so a
  stray dir that `git add -A` would otherwise stage is rejected instead of
  committed silently), the `entries.jsonl`/`compute_cache.jsonl` pointer
  bookkeeping, the bullpen reconciliation (`prune_entries`/`restore_from_bullpen`),
  and the `.gitignore`/tracked-surface definition.
- **`catalog_state.py` (stays — the versioning engine):** `_project_lock`,
  `ensure_catalog_repo`, `checkpoint_catalog` (capture pointers → zip pending via
  `catalog.py` → `git add -A` → commit → tag), `reset_to`, `list_revisions`,
  `current_step`, genesis. Slims substantially.
- Call direction is one-way (`catalog_state` → `catalog`); `assert_catalog_consistent`
  takes the pointer set as an argument to avoid a cycle. Don't rename
  `catalog_state.py` (test churn) — slim it in place.
- **Keep `entry_hashes` + the consistency check for now.** Once we own the tracked
  zips, the entry set is derivable from `git ls-files entries/*.zip`, making
  `entry_hashes` and the two-views check redundant — but that collapse touches the
  bullpen reconciliation and is a deliberate separate look.

## Risk register

| # | Risk | Sev | Status / mitigation |
|---|---|---|---|
| 1 | `git add -A` is allow-by-default; a `.gitignore` denylist silently tracks any unlisted artifact (build dirs, `result.parquet`, future strays) | High | `.gitignore` (`entries/*/` etc.) covers today's heavy artifacts, but the durable guard is `assert_catalog_consistent` enforcing the tracked surface as a path **allowlist** (deny-by-default). Regression test asserts no build-dir file — and no unlisted path — is tracked. |
| 2 | `sys.modules`-based "dropped" assertion is unsatisfiable (recipes load `xorq.api`) | High | Definition of done = subprocess guard + `src/` static grep, not `sys.modules`. |
| 3 | Naive `xorq.api`→`xorq.expr.api` swap breaks no-arg `xo.connect()` | High | Datafusion connect shim + determinism-equivalence test. |
| 4 | `test_catalog_xorq_integration.py` imports `xorq.catalog.catalog.Catalog` | High | Rewrite in the fix commits: drop `_open_xorq_catalog`/alias-symlink tests; keep the `git ls-files` durability guards. |
| 5 | Salt-mode zip must be named by the salted hash | High | Name `entries/<salted_hash>.zip`; test under `TALLYMAN_SOURCE_IDENTITY=salt`. |
| 6 | Commit timing inverts (inline → checkpoint); tests that build with no checkpoint and assert tracked fail | Med | Canonical failing-first test; update `test_xorq_catalog_add_registers_after_genesis` to checkpoint first. |
| 7 | Orphan loose zip / `--allow-empty` masks a pointer-without-recipe | Med | Retired by D1 (checkpoint is the sole zip writer over complete dirs). |
| 8 | In-process git reintroduces the macOS fork-from-threads SIGSEGV | Med | All native catalog git via `git_util.run_git` (posix_spawn); never GitPython. `test_no_bare_subprocess_git_in_src` enforces. |
| 9 | `test_capture_seeds_xorq_keys` / decomposed-section capture tests pin dropped behavior | Low | Delete/rewrite in the fix commits alongside the decomposition. |
| 10 | Existing corpus is #48-inconsistent; two zip formats coexist | Low | Rebuild from scratch (project rule). |

## Implementation sequence (ordered commits, TDD)

All new failing tests bundle into one commit, seen red on CI, before the fixes.

1. **Failing-first commit.** `test_*_uses_no_subprocess` (build/alias paths,
   monkeypatch `subprocess.run`), `test_zip_staged_not_committed_until_checkpoint`,
   `test_n_builds_track_n_zips` (#48 blind spot), `test_salt_zip_named_by_salted_hash`,
   `test_build_dir_files_not_staged`, `test_failed_build_no_orphan_pointer`,
   `test_prompt_history_survives_reset` (re-run appends a prompt, checkpoint,
   `reset_to`, assert all prompts present — fails today because the loose
   `entry_dir/prompts.jsonl` is gitignored), `test_unlisted_path_not_tracked`
   (stage a stray file, assert `assert_catalog_consistent` raises). Confirm
   `run_git` is posix_spawn (it is) before relying on the monkeypatch.
2. **Native recipe store** — `tallyman_core/catalog.py` `write_recipe_zip`;
   `build.py` persists the complete dir + cleans up on failure; delete
   `xorq_catalog.py`.
3. **Checkpoint owns zipping** — `.gitignore`; `checkpoint_catalog` zips pending +
   `git add -A`; `prune_entries`/consistency move into `catalog.py`, with
   `assert_catalog_consistent` extended to enforce the tracked-path allowlist.
4. **Decompose `catalog.yaml`** — aliases → `aliases.jsonl`; charts/display/pp/stats
   tracked directly; notebook → `notebook.jsonl`; pointers → `entries.jsonl` +
   `compute_cache.jsonl`; delete `capture`/`materialize` for the six sections;
   relocate the `prompts.jsonl` writer/reader (`build.py`, `read_prompts`,
   `app.py`) from `entry_dir` to tracked `prompts/<hash>.jsonl`; move
   `post_processing/`+`stats/` under the repo.
5. **Cut B** — narrow catalog-free imports + connect shim across the 8 modules.
6. **Test-cleanup commit** — rewrite/delete the xorq-coupled integration tests;
   re-target `test_failed_registration_is_observable` to a native failure; drop the
   xorq rationale from `test_catalog_repo_branch_is_main`.
7. **Rebuild + verify** — rebuild the corpus; `git ls-files` shows only the tracked
   tree above; genesis→create×2→reset roundtrip passes; full suite green locally
   before push.

## Scope boundaries / deferred

- No `pyproject.toml` change (fact 5). No merge/sync surface (fact 4). No xorq
  `assert_consistency`/`BuildZip`/`test_zip` contract in the native store (fact 1).
- No clone-restore / reconstruct-from-zip implementation; D3 keeps the zip *able*
  to support it (whole recipe + `<hash>/` prefix).
- No on-disk migration; rebuild the corpus.
- Recipe authoring-surface switch (D4) is a separate, codegen-validated follow-up.
- Collapsing `entry_hashes` into `git ls-files` is a separate look.

## Open / not yet grilled

- **Rebuild operational path.** What you actually run to rebuild a corpus
  (re-execute stored `expr.py` via `build_and_persist` in a loop vs re-drive the
  MCP/codegen session) and whether a `tallyman rebuild` affordance is warranted —
  `first-project` is ~119MB, `parking_ticket_analysis_1m` ~10 entries, so a
  from-scratch rebuild is minutes-to-tens-of-minutes per large project. Needs a CLI
  check before settling.
- Genesis baseline behavior post-decomposition; exact `.gitignore`; final
  failing-first test list once the above is settled.
