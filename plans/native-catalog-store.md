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
(`xorq/api.py:62 import xorq.catalog.api as catalog`; verified: bare
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
notebook.jsonl                # one line per cell (moved under the repo)
entries.jsonl                 # pointer list: untracked build dirs (bullpen reconcile)
compute_cache.jsonl           # pointer list: untracked compute-cache warm set
chart_specs/<hash>.vl.json    # one Vega-Lite spec per entry
display_configs/<hash>.json   # one Buckaroo/display config per entry
prompts/<hash>.jsonl          # append-only authoring-prompt history per entry
post_processing/<name>.py     # project-global functions (moved under the repo)
stats/<name>.py               # project-global functions (moved under the repo)
pyproject.toml                # per-repo env spec — the reproducibility unit
uv.lock                       # pinned lock; recreate one venv per repo to rebuild
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
  below). The allowlist-not-denylist membership (and the three exclusions) is
  pinned by `test_recipe_zip_members_are_allowlist`; `test_recipe_zip_contains_expr_py`
  guards that `expr.py` is present (D3).
- Members stored under arcname `<hash>/<relpath>` (single top-level prefix, so a
  future restore can reuse it). **Named by the salted `content_hash`** when
  source-identity salt mode is on, not `build_path.name` — otherwise
  `_tracked_recipe_hashes` (zip names) diverges from `entries.jsonl` pointers.
- **Deterministic archive** (`date_time=(1980,1,1,0,0,0)`,
  `external_attr=0o644<<16`, sorted iteration, `ZIP_DEFLATED` at a pinned
  `compresslevel`) so the blob doesn't churn on identical rebuilds. (Single
  machine, so cross-zlib reproducibility is moot; pinning the level keeps the
  blob byte-stable across same-machine rebuilds regardless of zlib defaults.)
  Pinned by `test_recipe_zip_byte_stable_across_rebuilds` — without it a defaulted
  `ZipInfo` mtime embeds the current time and silently re-churns the git blob.
- **Drop the wheel + requirements.txt** the `xorq catalog add` subprocess injects
  (~178KB → ~6KB per entry). These are xorq's *per-expression* reproducibility
  mechanism — `WheelPackager`/`PackagedBuilder`/`PackagedRunner`
  (`xorq/ibis_yaml/packager.py`) bundle a built wheel + pinned `requirements.txt` so
  `xorq run` can re-execute an entry in its *own* isolated `uv` env. tallyman does
  not use that path: it builds every expression in-process via
  `build_expr`/`load_expr` (`build.py:317`), and captures reproducibility at
  **catalog-repo granularity** — a single tracked `pyproject.toml` + `uv.lock` (see
  Tracked tree), one venv per project, recreated to rebuild the corpus. A
  per-expression `uv` env is the slow, expensive thing tallyman avoids; per-expression
  venvs are a bridge to cross later if ever wanted. The in-process build output
  (`xorq_build/`) never held the wheel anyway (the `build_expr` file set is
  `expr.yaml`/SQL/`deferred_reads.yaml`/`profiles.yaml`); only the catalog zip did,
  and nothing reads it back (fact 1). So the drop forfeits xorq's per-entry env
  reconstruction — **unused here** — not tallyman's reproducibility, which is the
  tracked per-repo lock. Separately: because the zip
  is deterministic and content-addressed, git stores each blob once and a
  `git add -A` over an unchanged zip is a no-op that adds no history — so the saving
  is a smaller durable blob and working tree per entry, not avoided per-checkpoint
  churn (there is none once the zip is byte-stable).
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
  `aliases.jsonl` (one line per alias: `{"alias","latest","history":[...]}`); the
  read-only alias functions ride on `_read`/`_write` untouched, and the only reader
  off `read_tallyman_state` is `aliases.py` itself (verified). But `aliases.py` also
  has a *module-level* `from tallyman_core import xorq_catalog as _xcat`
  (`aliases.py:24`) and
  `set_alias`/`remove_alias` still call `_xcat.add_alias`/`remove_alias`
  (`:129`,`:155`) — the xorq subprocess. Both must be stripped in the *same* commit
  that deletes `xorq_catalog.py`: `aliases.py` is its only remaining caller once
  `build.py`'s recipe path is rewritten, and because the import is module-level,
  deleting `xorq_catalog.py` without it makes `aliases.py` — and the whole package —
  un-importable.
- `post_processing/`, `stats/`, and the notebook move from outside the repo
  (`artifacts/post_processing`, `artifacts/stats`, and `project_dir/notebooks/`
  respectively) to under `catalog/` so they can be tracked. `chart_specs/` and
  `display_configs/` already live under `catalog/` (`charts.py`/`display_configs.py`
  write to `catalog_dir`), so they need no move — only `post_processing/`, `stats/`,
  and the notebook do. Layout change, trivially a rebuild.
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
(used at `source_cache.py:133,152`). `xorq.expr.api` is *not* a drop-in here: it
does export a `connect`, but it is ibis's `connect(resource, **kwargs)`
(re-exported via `from xorq.vendor.ibis.expr.api import *`), a different function
requiring a positional `resource` — not the no-arg datafusion `xorq.api.connect`
(verified: `xorq.expr.api.connect is xorq.api.connect` → `False`). So a naive swap
resolves the symbol (no `ImportError`) and then fails at *call* time on the missing
arg, or silently builds the wrong backend. Add a tallyman datafusion connect shim in
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
`import xorq.api`, D4). It is three named tests: (a) `test_uses_no_subprocess_build`
+ `test_uses_no_subprocess_alias` — a `subprocess.run` call-count spy over a full
cycle asserting zero *xorq* invocations (the build path must assert
`spy.call_count == 0`, not "did not raise": `build.py:477-478` swallows the
exception; scope the spy past the legitimate `cp -c` clone at
`source_identity.py:111`); (b) `test_src_has_no_xorq_catalog_argv` — a static grep
that `src/` has no `import xorq.catalog` / `from xorq.catalog` and no split-token
`xorq … catalog … add` argv (skipping the `tallyman_mcp/server.py:219` codegen
docstring); and (c) `test_connect_shim_snapshot_is_byte_identical` — the datafusion
shim preserves byte-identical content-addressed snapshots.

## Decisions (resolved in the grilling pass)

- **D1 — zip timing:** checkpoint owns zip creation; build owns only the complete
  entry dir. (Collapses the orphan-zip / `--allow-empty`-masking / partial-failure
  risks.)
- **D2 — `catalog_registered`:** means "entry dir persisted" (a local fact at
  build time); durability is the next checkpoint's job and always succeeds because
  the dir is complete. Rewrite the `#48` "not durable" log accordingly.
- **D3 — archive scope:** the recipe allowlist `{expr.py, xorq_build/,
  schema.json, manifest.json}` as one immutable zip (fixes the wrong-artifact
  durability gap from fact 2). **`expr.py` is durable, not regenerated from
  `xorq_build/`.** The `from_catalog` chaining path re-execs it so a cheap entry's
  recompute expression roots on the *shared* in-process backend and two entries
  `union`/`join` on one backend (#75). Reconstructing the way xorq's own catalog
  does — `load_expr` per entry — gives each its *own* backend and raises
  `Multiple backends found` (verified: two `load_expr` results won't `union`; two
  re-exec'd sources do). xorq never hits this because its catalog runs one entry at
  a time and never composes; tallyman's chaining is the feature that needs the
  shared backend. Two source-regeneration routes were tested and rejected: xorq's
  decompiler is ibis's generic one and drops the read path (emits non-compiling,
  path-less source); `expr.unbind()` composes but likewise strips the read path and
  can't execute without re-registering each source by name. Both reimplement — more
  fragilely — what the one already-written `expr.py` (`build.py:367`) gives for
  free, so it stays. (Expensive entries don't actually need it for data — they read
  a baked snapshot and use `expr.py` only to *locate* it — but the cheap-entry
  recompute case does.) `prompts.jsonl` is deliberately *not* a member: it is
  append-mutable, runtime-read provenance, so it joins the decomposition as a
  tracked `prompts/<hash>.jsonl` rather than churning the content-addressed blob.
  The shared-backend half is already covered in the fast suite
  (`test_result_cache.py:214,232`); the **durability** half — that `expr.py`
  round-trips via the zip once `entries/*/` is gitignored — is *not*, so
  `test_two_entries_union_after_reset` (commit 1b, no `integration`/`cache_lab`/`perf`
  marker, since CI deselects those) is the gating test, with the contrapositive
  `test_two_load_expr_results_cannot_union` pinning why `expr.py` must be durable.
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
| 3 | `xo.connect()` survives the `xorq.api`→`xorq.expr.api` swap as ibis's `connect(resource, …)` (present, not absent), so it fails at *call* time / builds the wrong backend rather than at import — easy to miss | High | Datafusion connect shim + `test_connect_shim_snapshot_is_byte_identical` (commit 1b) — was prose-only, now a named failing-first test. |
| 4 | `test_catalog_xorq_integration.py` imports `xorq.catalog.catalog.Catalog` **at module level** (`:45` also imports the to-be-deleted `xorq_catalog`) | High | Delete/rewrite in the **step-2** commit (same one that deletes `xorq_catalog.py`), not step 6 — a module-level import of the deleted module collection-errors the whole suite from step 2 on. Drop `_open_xorq_catalog`/alias-symlink tests; keep the `git ls-files` durability guards. |
| 5 | Salt-mode zip must be named by the salted hash | High | Name `entries/<salted_hash>.zip`; test under `TALLYMAN_SOURCE_IDENTITY=salt`. |
| 6 | Commit timing inverts (inline → checkpoint); tests that build with no checkpoint and assert tracked fail | Med | `test_xorq_catalog_add_registers_after_genesis` is a **fix-induced break**, not failing-first: edit to checkpoint-then-assert in the step-6 cleanup commit. |
| 7 | Orphan loose zip / `--allow-empty` masks a pointer-without-recipe | Med | Retired by D1 (checkpoint is the sole zip writer over complete dirs). |
| 8 | In-process git reintroduces the macOS fork-from-threads SIGSEGV | Med | All native catalog git via `git_util.run_git` (posix_spawn); never GitPython. `test_no_bare_subprocess_git_in_src` enforces (note: it matches bare `git` argv only — the `xorq catalog` argv guard is the separate `test_src_has_no_xorq_catalog_argv`). |
| 9 | `test_capture_seeds_xorq_keys` / decomposed-section capture tests pin dropped behavior | Low | **Delete** (not rewrite) in the step-6 cleanup commit alongside the decomposition. |
| 10 | Existing corpus is #48-inconsistent; two zip formats coexist | Low | Rebuild from scratch (project rule). |
| 11 | Deterministic-archive claim untested — a defaulted `ZipInfo` mtime silently re-churns the content-addressed git blob on every rebuild | High | `test_recipe_zip_byte_stable_across_rebuilds` (commit 1b): build the same code twice, assert byte-identical archives. |
| 12 | Zip-member allowlist is the "only guard against leak" but lives only as D3 prose; a denylist impl that skips today's three known names passes existence checks yet leaks a future artifact | High | `test_recipe_zip_members_are_allowlist` (commit 1b): assert `namelist()` equals exactly the four members; reject an unknown `foo.bin`. (Distinct surface from `test_unlisted_path_not_tracked`, which guards the git-tracked repo root, not the zip bytes.) |
| 13 | D3 half (b) — `expr.py`-via-zip durability for chaining — untested in the gating suite; the only `reset`+`from_catalog` file is marker-deselected (`pyproject.toml:77`), so a `load_expr`-per-entry refactor would pass the fast suite and silently break chaining | High | `test_two_entries_union_after_reset` + `test_recipe_zip_contains_expr_py` (commit 1b), carrying **no** `integration`/`cache_lab`/`perf` marker. |
| 14 | `display_configs` is the one decomposed section with no survives-reset test today; its only coverage is the capture/materialize round-trip tests step 4 deletes | Med | `test_display_config_survives_reset` (commit 1b) — new reset test so net coverage doesn't regress. |
| 15 | Commit-1 failing-first is internally inconsistent with the TDD rule: some named tests fail by collection error (`test_unlisted_path_not_tracked` → missing API), false-green (`test_uses_no_subprocess` build path swallowed at `build.py:477-478`; `test_prompt_history_survives_reset` survives via bullpen until step-3 `.gitignore`) | High | Split into commit 1a (assertion-red today) and 1b (lands API/`.gitignore` stubs in-commit); build-path guard asserts `spy.call_count == 0`, scoped to the xorq argv (exempt the `cp -c` clone at `source_identity.py:111`). |

## Implementation sequence (ordered commits, TDD)

Every new test must be seen red on CI **by a real assertion**, never by a
collection/import error (the repo TDD rule excludes those). That forces the
failing-first work into two commits, not one: some assertions go red against
today's inline-xorq tree, but others reference symbols
(`assert_catalog_consistent`, `write_recipe_zip`) or a `.gitignore` that only
later commits create. Commit 1a holds the first kind; commit 1b lands thin stubs
of those symbols + the `.gitignore` in the *same* commit as its tests, so the
assertion is what's red, not the import. Confirm `run_git` is posix_spawn (it is)
before relying on any `subprocess.run` monkeypatch.

1a. **Failing-first, assertion-red against today's tree** (use existing public
   surfaces only — `build_and_persist`, `checkpoint_catalog`, `git ls-files` —
   never the not-yet-created `tallyman_core.catalog` API, or these collect-error):
   - `test_uses_no_subprocess_alias` — monkeypatch `subprocess.run` to raise;
     `set_alias` raises today (`aliases.py:129` → `_xcat.add_alias`, no try/except).
   - `test_uses_no_subprocess_build` — assert a **call-count spy** records zero
     *xorq* `subprocess.run` invocations. Must NOT assert "did not raise":
     `build.py:477-478` swallows the exception into `catalog_registered=False`, so a
     raising stub false-greens. Scope the spy to the xorq argv so it doesn't trip on
     the legitimate `cp -c` CoW clone at `source_identity.py:111`.
   - `test_zip_staged_not_committed_until_checkpoint` (D1 — committed inline today).
   - `test_n_builds_track_n_zips` (#48 blind spot — 4 build dirs, 1 tracked zip today).
   - `test_salt_zip_named_by_salted_hash` (Risk #5 — xorq names by `build_path.name`).
   - `test_build_dir_files_not_staged` (Risk #1 — no `.gitignore` today).
   - `test_failed_build_no_orphan_pointer` (D1/Risk #7).

1b. **Failing-first, assertion-red once this commit's stubs land.** Land
   stub `tallyman_core/catalog.py` (`assert_catalog_consistent`, `write_recipe_zip`
   as no-op/identity) **and** the `.gitignore` (`entries/*/` etc.) in this commit so
   the tests below fail by assertion:
   - `test_unlisted_path_not_tracked` — stage a stray file, assert
     `assert_catalog_consistent` raises (stub returns without raising → red).
   - `test_recipe_zip_byte_stable_across_rebuilds` — build the same code twice
     (`res2.content_hash == res1.content_hash`, the `test_portable.py:134-145`
     idempotent pattern), zip each entry dir, assert the two archives are
     byte-identical. Guards the deterministic-archive claim — a defaulted `ZipInfo`
     mtime silently re-churns the git blob on every rebuild and nothing else catches it.
   - `test_recipe_zip_members_are_allowlist` — drop strays
     (`.xorq_build_expanded/x`, `result.parquet`, `prompts.jsonl`, an unknown
     `foo.bin`) into a real entry dir, run `write_recipe_zip`, assert
     `set(namelist())` under the `<hash>/` prefix equals exactly
     `{expr.py, xorq_build/**, schema.json, manifest.json}`. Pins allowlist-not-denylist:
     a denylist skipping only today's three known names passes existence checks but
     leaks `foo.bin`.
   - `test_prompt_history_survives_reset` — re-run appends a prompt
     (`build.py:335`), checkpoint, `reset_to`, assert all prompts present. Red only
     once `entries/*/` is gitignored (pre-staged in this commit); the loose
     `entry_dir/prompts.jsonl` is then lost on reset. (Verify the bullpen copytree
     does not otherwise preserve it before relying on this.)
   - `test_two_entries_union_after_reset` — two `from_catalog` entries `union` after a
     `reset_to` that restores `expr.py` from the zip only (D3 half (b)). **Carries no
     `integration`/`cache_lab`/`perf` marker** — CI runs bare `uv run pytest` and
     deselects those (`pyproject.toml:77`), so a marked test would share the existing
     blind spot. Optionally add the contrapositive `test_two_load_expr_results_cannot_union`
     to pin why `expr.py` is load-bearing.
   - `test_recipe_zip_contains_expr_py` — assert `expr.py` is a zip member (D3).
   - `test_connect_shim_snapshot_is_byte_identical` — the shim-built and
     `xo.connect()`-built `ParquetSnapshotCache` must produce byte-identical
     content-addressed snapshots (Cut B / Risk #3; the no-arg `xo.connect()` sites are
     `src/tallyman_xorq/source_cache.py:133,152`). The central Cut-B correctness claim.
   - `test_src_has_no_xorq_catalog_argv` — static grep over `src/` for
     `import xorq.catalog` / `from xorq.catalog` **and** the split-token catalog argv
     (`_xorq(project, "catalog", "add", …)` in `xorq_catalog.py:31`, so a literal
     `"xorq catalog"` string match misses it). Model on
     `test_no_bare_subprocess_git_in_src` (`test_reset_to_revision.py:81-89`); add a
     sanctioned skip for the `tallyman_mcp/server.py:219` codegen docstring (D4).
   - `test_display_config_survives_reset` — edit a display config on an existing hash,
     checkpoint, `reset_to`, assert restored. The one decomposed section with no reset
     coverage today (`test_reset_roundtrip_over_every_kind`,
     `test_reset_to_revision.py:215-232`, covers pp/stats/charts/notebook, not display).
2. **Native recipe store** — `tallyman_core/catalog.py` `write_recipe_zip`;
   `build.py` persists the complete dir + cleans up on failure; delete
   `xorq_catalog.py` *and*, in the same commit, strip every module-level coupling to
   it so the tree stays importable (commit 1a's suite can't run red against an
   un-importable tree). Three module-level imports of the deleted module must go in
   this commit, not later:
   - `aliases.py:24` `from tallyman_core import xorq_catalog` + the
     `_xcat.add_alias`/`remove_alias` calls in `set_alias`/`remove_alias`
     (`:129`,`:155`).
   - `tests/test_catalog_xorq_integration.py:45` `from tallyman_core import
     xorq_catalog as xc` (and `:99` `from xorq.catalog.catalog import Catalog`). The
     plan otherwise defers this file's rewrite to step 6, but its import is
     module-level: the moment `xorq_catalog.py` is deleted the **whole suite** goes
     red by collection error, not assertion. Delete/rewrite it here (drop the
     `_open_xorq_catalog`/alias-symlink tests; keep the `git ls-files` durability
     guards), so steps 2–5 stay green locally.
3. **Checkpoint owns zipping** — `.gitignore`; `checkpoint_catalog` zips pending +
   `git add -A`; `prune_entries`/consistency move into `catalog.py`, with
   `assert_catalog_consistent` extended to enforce the tracked-path allowlist.
4. **Decompose `catalog.yaml`** — aliases → `aliases.jsonl`; charts/display/pp/stats
   tracked directly; notebook → `notebook.jsonl`; pointers → `entries.jsonl` +
   `compute_cache.jsonl`; delete `capture`/`materialize` for the six sections;
   relocate the `prompts.jsonl` writer/reader (`build.py`, `read_prompts`,
   `app.py`) from `entry_dir` to tracked `prompts/<hash>.jsonl`; relocate
   `post_processing/`, `stats/`, and the notebook (`project_dir/notebooks/`) under
   the repo.
5. **Cut B** — narrow catalog-free imports + connect shim across the 8 modules.
6. **Test-cleanup commit** — the remaining edits are **fix-induced breaks** (an
   existing test goes red *because* the fix landed), not failing-first tests; they
   belong with the fixes, never bundled into commit 1:
   - `test_failed_registration_is_observable` — **delete**, don't re-target. Its
     premise (a swallowed best-effort `catalog_registered` bool) vanishes with the
     registration path; the stray-file→raise behavior it gestures at is exactly what
     `test_unlisted_path_not_tracked` now covers, so re-targeting would just duplicate.
   - `test_capture_seeds_xorq_keys` — **delete** (not "rewrite"): it asserts `capture`
     seeds empty `entries`/`aliases` keys in `catalog.yaml` so `xorq catalog add`
     won't `KeyError`; with `catalog.yaml` and the subprocess both gone the invariant
     ceases to exist. Breaks when step 4 stops writing `catalog.yaml`.
   - `test_xorq_catalog_add_registers_after_genesis` — goes red on the fix branch
     because the zip is now written at checkpoint, not inline at build. Edit to
     checkpoint-then-assert (passes only post-fix), so it lives here, not in
     failing-first. Its "best-effort xorq registration must land" docstring is dead.
   - `test_catalog_repo_branch_is_main` — docstring-only edit (drop the xorq
     rationale; the `main`-branch assertion stays valid for our native repo).
   - The capture/materialize round-trip tests in `test_display_configs.py` break when
     step 4 deletes those primitives; this commit removes them, and the new
     `test_display_config_survives_reset` (commit 1b) keeps net reset coverage from
     regressing.
7. **Rebuild + verify** — rebuild the corpus; `git ls-files` shows only the tracked
   tree above; genesis→create×2→reset roundtrip passes; full suite green locally
   before push.

## Scope boundaries / deferred

- No change to *tallyman's own* `pyproject.toml`/xorq pin (fact 5 — `xorq.catalog`
  can't be narrowed out of the single wheel). The `pyproject.toml` + `uv.lock` added
  to the tracked tree are a *separate*, per-catalog-repo env lock (the reproducibility
  unit), not a change to tallyman's deps. No merge/sync surface (fact 4). No xorq
  `assert_consistency`/`BuildZip`/`test_zip` contract in the native store (fact 1).
- No clone-restore / reconstruct-from-zip implementation; D3 keeps the zip *able*
  to support it (whole recipe + `<hash>/` prefix).
- No on-disk migration; rebuild the corpus.
- Recipe authoring-surface switch (D4) is a separate, codegen-validated follow-up.
- Collapsing `entry_hashes` into `git ls-files` is a separate look.
- Runtime per-project venv isolation (build each project *inside* its own venv,
  recreated from the tracked lock, instead of the shared in-process env) is a larger
  architecture change — tracked in #106. This plan settles the reproducibility
  *record* (the tracked `pyproject.toml` + `uv.lock`) only; the in-process build stays.

## Open / not yet grilled

- **Rebuild operational path.** The *env* half is settled: recreate the project's
  one venv from the tracked `uv.lock`, then rebuild every expression in it (one venv
  per catalog repo, not per expression). Still open: what you actually run to
  re-execute (re-run stored `expr.py` via `build_and_persist` in a loop vs re-drive
  the MCP/codegen session), whether a `tallyman rebuild` affordance is warranted
  (`first-project` ~119MB, `parking_ticket_analysis_1m` ~10 entries → minutes-to-tens
  per large project), and **when `pyproject.toml`/`uv.lock` get snapshotted/refreshed**
  (snapshot from the active env at genesis, plus a refresh when a recipe needs a new
  dep). Needs a CLI check before settling. Whatever the mechanism, its verification
  should assert **execution-equivalence** (rebuilt entries execute to identical
  data), not just a structural roundtrip — xorq's `test_rebuild_produces_consistent_target`
  exists because a rebuild can match on schema/recipe/hash while silently returning
  wrong rows.
- Genesis baseline behavior post-decomposition and the exact `.gitignore` contents.
  (The failing-first list is now settled above as commits 1a/1b.)
