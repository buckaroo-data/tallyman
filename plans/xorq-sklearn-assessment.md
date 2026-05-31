# Does xorq's deferred-sklearn layer help? (re: xorq-labs/xorq-gallery-sklearn)

Generated 2026-05-30. Question: the codegen tests showed modeling finales failing — `import sklearn`
blocked by the post-processing sandbox, and "fit a model in a catalog expression" structurally impossible.
Does xorq's sklearn integration (as showcased by the gallery) help?

## Short answer
Yes, substantially — for the **modeling** scripts (regression, clustering, classification), with one
one-line dependency fix and two honest limits. It does **not** help the hypothesis-testing (scipy) script.

## What xorq 0.3.26 ships
`xorq.ml` / `xorq.expr.ml` (also on `xorq.api`) expose a deferred sklearn layer:
`deferred_fit_predict_sklearn`, `deferred_fit_transform_sklearn`, `deferred_cross_val_score`,
`deferred_sklearn_metric`, `train_test_splits`, `calc_split_column`, and `Pipeline` / `Step` /
`FittedPipeline`. A fit becomes part of the expression graph; the trained model is serialized
(cloudpickle) into the build. The gallery (`xorq-labs/xorq-gallery-sklearn`) is a pure xorq *catalog*
(`catalog.yaml` + `entries/*.zip`) of pre-built sklearn-pipeline expressions — no notebooks; the
"examples" are the serialized expressions.

Canonical pattern (from xorq's own `test_fit_lib.py`):
```python
import xorq.expr.datatypes as dt
from xorq.ml import deferred_fit_predict_sklearn
from sklearn.linear_model import LinearRegression
deferred_lr = deferred_fit_predict_sklearn(cls=LinearRegression, return_type=dt.float64)
instance = deferred_lr(t, target, features)
expr = t.mutate(instance.deferred_other.on_expr(t))   # predictions added; fit runs at materialization
```

## Proof (executed through the real `build_and_persist`, ephemeral scikit-learn via `uv run --with`)
- **LinearRegression** `price ~ [carat,depth,table,x,y,z]` on diamonds → ✓ built, 53,940 rows, real
  continuous predictions (`346.9, -71.5, 126.4, …`), 0.11s.
- **KMeans k=3** on the 4 penguin measurements (nulls dropped) → ✓ built, 342 rows, integer cluster labels.
Both produced normal content-hashed catalog entries.

## Why it helps
1. **Dissolves the structural trap.** "Fit a model / materialize clusters" — the step all three models
   failed at — is now a plain `catalog_create` entry. The model is a first-class catalog artifact:
   content-hashed, lineage-tracked, diffable, chart-able.
2. **Sidesteps the post-processing sandbox.** The fit lives in the expression, and `catalog_create`/`run`
   code executes in a normal module scope (full builtins) — not the restricted `process()` namespace that
   strips `__import__`. So `from sklearn… import …` is allowed where it matters.
3. **Real DS methodology is expressible:** `train_test_splits` (hash-bucketed, seeded),
   `deferred_sklearn_metric` (R²/accuracy on expressions after predictions), `deferred_cross_val_score`,
   and `Pipeline`/`Step` for multi-stage sklearn pipelines as a single deferred expression.

## The gating fix
scikit-learn is **not installed**; `xorq.ml` lazy-imports it, so every fit currently fails with
`No module named 'sklearn'` (the same wall the Haiku/Opus runs hit). It's xorq's `examples` extra:
`scikit-learn>=1.4,<2.0; extra=='examples'`. Fix: `uv add "xorq[examples]==0.3.26"` (or `uv add scikit-learn`).
One line; not an architectural blocker.

## Two honest limits
- **Doesn't touch the hypothesis-testing script.** xorq.ml covers sklearn estimators/transformers. A Welch
  t-test / Jarque-Bera is `scipy.stats`, not sklearn — still blocked by the post-processing sandbox (bug #2
  stands), or must be hand-expressed in pure ibis (as Sonnet did).
- **The models won't discover it unaided.** None of Haiku/Sonnet/Opus used `xorq.ml` — they reached for raw
  `import sklearn` (blocked) or hand-rolled (Opus's nearest-centroid trick). The `catalog_run` docstring
  never mentions it. To make the capability land, add a MODELING section to the docstring teaching
  `from xorq.ml import deferred_fit_predict_sklearn`.

## Minor wart
The auto-generated prediction column name is ugly: `_predict_<hash>(carat, depth, …)`. Want a rename helper
or document wrapping the predict column with `.name("price_pred")`.

## What this changes for the 5 demo scripts
- **regression (diamonds)** and **clustering (penguins)** get real modeling finales on the `catalog_create`
  path instead of the doomed `process()`+sklearn steps.
- Revives the **supervised classification** use case the critic had dropped: `train_test_splits` +
  `deferred_fit_predict_sklearn(LogisticRegression)` + `deferred_sklearn_metric(accuracy)` is now a clean
  end-to-end catalog workflow.
- **hypothesis-testing (wine)** is unchanged — its scipy needs are a separate problem.
