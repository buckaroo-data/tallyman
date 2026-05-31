# xorq codegen across model tiers — Haiku vs Sonnet vs Opus

Generated 2026-05-30. The same 5 DS demo-script step-intents were fed to each model with the real `catalog_run` docstring + dataset schema as context (the live MCP condition). Each model wrote the xorq/`process()` code; every snippet was then executed through the real `build_and_persist` / `run_post_processing` / `write_post_processing` pipeline in isolated per-model projects.

## xorq expression codegen — steps that built successfully

| script | haiku | sonnet | opus |
|---|---|---|---|
| timeseries (stocks) | 0/2 | 2/2 | 2/2 |
| hypothesis (wine) | 1/4 | 4/4 | 4/4 |
| survival (titanic) | 6/6 | 6/6 | 6/6 |
| regression (diamonds) | 3/3 | 3/3 | 3/3 |
| clustering (penguins) | 3/5 | 3/5 | 5/5 |
| **TOTAL** | **13/20** | **18/20** | **20/20** |

## post-processing / summary-stat functions — ran or committed successfully

| script | haiku | sonnet | opus |
|---|---|---|---|
| timeseries (stocks) | 0/2 | 2/2 | 0/3 |
| hypothesis (wine) | 0/3 | 2/3 | 0/3 |
| survival (titanic) | 2/2 | 2/2 | 0/2 |
| regression (diamonds) | 2/2 | 0/2 | 0/2 |
| clustering (penguins) | 0/2 | 1/2 | 2/2 |
| **TOTAL** | **4/11** | **7/11** | **2/12** |

## Reading the two tables

**xorq codegen improves monotonically with tier.** Haiku hallucinates ibis APIs (`ibis.to_date`, `ibis.re_replace`, `ibis.struct(mean=…)`), which kills whole scripts at step 2. Sonnet and Opus reach for the correct ones (`col.as_timestamp("%b %d %Y")`, `ibis.cases(...)`, `ibis.window(group_by=, order_by=, preceding=)`). Opus built every expression (20/20); the only thing Sonnet missed was the clustering "materialize k-means as a catalog entry" step (see below).

**Post-processing inverts the ranking, and it comes down to one decision: import or not.** The sandbox (`post_processing.py`) strips `__import__`, so any `process()` that does `import scipy/sklearn/numpy/pandas` fails. Opus, which writes the best xorq, also most confidently trusts the docstring's "returns a pandas DataFrame" line and imports third-party libs — so it scores *worst* on post-processing (2/12). Sonnet hedges: pure-ibis stats wrapped in `try/except`, scoring best (7/11). Haiku is mixed (4/11), occasionally avoiding imports by luck.

## Per-model character

- **Haiku** — fine on clean group-by / aggregate / filter / mutate / cases / union; falls over by inventing plausible-but-nonexistent APIs the moment a task needs date parsing, structs, or windows it hasn't seen. Silently punts hard stats into non-answers (a `memtable` of placeholder zeros for Cohen's d; a constant column instead of a z-test).
- **Sonnet** — strong, defensive, and meta-aware. Expressed skewness / kurtosis / Jarque-Bera in pure ibis via raw power moments; wrote `try/except` guards specifically to survive the commit-time dry-run; in one wine step deliberately submitted a failing version *and* a guarded version to demonstrate the validator bug. Downside: it used its tool access to inspect the filesystem and hardcoded `project="haiku-ts"` pins (stripped before execution; see methodology).
- **Opus** — best xorq codegen (20/20) and the single most sophisticated artifact: its clustering `process()` recognized the sandbox block, the missing sklearn dependency, *and* the dry-run validator, then implemented a deterministic pure-ibis nearest-centroid assignment with a dry-run guard, and re-materialized the same logic as a plain ibis expression. But in the stats scripts it imported scipy/numpy freely and hit the sandbox, and it misattributed a validator failure to "imports" when the cause was the dummy `{a,b}` schema.

## Confirmed app bugs (now multi-model — higher confidence)

1. **Commit-time dry-run validator rejects realistic functions.** `validate_post_processing_source` runs `process()` against a fixed `memtable({'a':[...], 'b':[...]})`, so any function referencing real column names is rejected at `catalog_add_post_processing` even when `catalog_run_post_processing` previewed it correctly against real data. Hit Sonnet (wine, references `wine_type`) and Opus (regression — pure ibis, no imports, references `carat_bin`, still rejected: `XorqTypeError: Column 'carat_bin' is not found … Existing columns: 'a','b'`). Workaround that worked: Sonnet's `if expected_cols_absent: return expr` guard. This breaks the commit path for almost any real post-processor.
2. **Import sandbox is opaque.** `import scipy/sklearn/numpy/pandas` inside `process()` fails with `ImportError('__import__ not found')` — cryptic, and identical in the preview and validate paths. Hit all three models. Either widen the allowlist (numpy/scipy/pandas) or emit a clear message pointing at `expr.execute()` + DataFrame methods.
3. **Structural trap: "materialize k-means as a catalog entry."** A catalog expression can't run sklearn. Haiku wrote `import sklearn` in the expression (`No module named 'sklearn'`); Sonnet read a non-existent `penguins_labeled.parquet` and cascaded; only Opus routed around it (pure-ibis centroids).
4. (from the Haiku run) a pure `memtable` expression crashes the build with `IsADirectoryError: …/memtables`; and `xorq catalog add failed … artifacts/catalog` warns on fresh projects (non-fatal).

## Methodology notes

- "medium effort": the workflow harness lets me pick the model per agent but exposes no separate reasoning-effort knob for subagents, so all three ran straightforwardly (single-shot generation, no execution-feedback retry loop).
- Sonnet's `project="…"` pins were stripped before execution (they pointed at the wrong project — an artifact of Sonnet inspecting the filesystem, which Haiku/Opus did not do). Imports were left untouched and allowed to fail honestly.
- The `⚠` gotcha flags in the raw logs are a regex scan and over-flag from comments (e.g. Opus's "no `.describe()`" comment). Execution status is the source of truth, not the flags.
- Scratch projects left on disk: `son-*` and `opus-*` (and `haiku-*` from the prior run). Active project restored to `test-1`.
