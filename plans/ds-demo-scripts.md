# Data-science demo scripts — synthetic workflows to exercise tallyman-notebooks and surface bugs

Generated 2026-05-30. Five DS-oriented demo scripts on freshly-downloaded public datasets, chosen by a fan-out design pass + critic for distinct DS techniques, plotting variety, and coverage of the recently-changed code (diff backends, result cache, primary-key inference, post-processing live-reload).

**Datasets** are staged at `/tmp/tallyman_demo_data/`. Before running a script, copy its parquet(s) into the target project: `cp /tmp/tallyman_demo_data/<file> ~/.tallyman/projects/<project>/data/`.

**Verified blocker (post_processing.py:35-61,110-119):** the `process(expr)` sandbox builds `__builtins__` from a fixed allowlist with no `__import__`, so `import numpy/scipy/sklearn/pandas` fails inside any post-processing function. The pandas-DataFrame return path only works via `expr.execute()` + DataFrame methods; third-party stats/ML is unreachable. Scripts 2 and 5 are designed to hit this wall.


---


## 1. Quant Prep: Parsing "Jan 1 2000", Per-Symbol Returns, and Rolling Windows on stocks

- **Archetype:** `timeseries-window-functions`
- **Mimics:** Mimics the forecasting/quant feature-prep notebook: parse a messy string date, order per entity, derive lagged monthly returns, rolling moving averages, cumulative return, and per-month cross-sectional ranks — then a scipy post-processing step for distribution stats. It is the heaviest window-function and partitioned-ordering exerciser, and it hammers the newly-added composite-PK diff, the cache-worthy result cache, and live-reloaded post-processing on a single multi-entity series.
- **Dataset:** stocks.parquet (560 rows, 5 symbols MSFT/AMZN/IBM/GOOG/AAPL, monthly, date as "Jan 1 2000" string). Chosen because it is multi-entity AND its date column needs an explicit strptime format — the two ingredients that make partitioned window functions (group_by=symbol, order_by=parsed_date) both necessary and fragile. The cross-sectional rank uses all 5 symbols per month; the diff needs a 2-column (symbol, date) composite key because no single column is unique.
- **DS technique:** Time-series feature engineering with partitioned+ordered window functions (lag returns, rolling moving averages, cumulative product proxy, cross-sectional ranking) plus a scipy-backed post-processing step (skew/kurtosis + Jarque-Bera normality test on the returns distribution returned as a pandas.DataFrame).
- **Data files to stage:** `stocks.parquet` (~8 tool calls)

### Steps

**1. Load the raw stocks parquet into the catalog as a named alias so the date-string trap is visible in the viewer before any transformation.**  _(`catalog_load_parquet`)_

```python
catalog_load_parquet(rel_path="stocks.parquet", name="stocks_raw", prompt="Raw monthly close prices for 5 symbols. Note: date is a STRING in 'Jan 1 2000' format, not a real timestamp.")
```
> _Probes:_ Confirms date arrives as str (not auto-parsed). Establishes a cheap (parquet-read-only) entry whose result.parquet is regenerated in place, the cache-worthy=False baseline against which later window entries differ.

**2. Parse the string date with an explicit strptime format, sort per symbol, and derive the lagged monthly return; put the new return column first.**  _(`catalog_create`)_

```python
catalog_create(name="stock_returns", code="import xorq.vendor.ibis as ibis\nfrom tallyman_xorq.io import from_catalog\nt = from_catalog(\"stocks_raw\")\nt = t.mutate(ts=t.date.as_timestamp(\"%b %d %Y\"))\nw = ibis.window(group_by=\"symbol\", order_by=\"ts\")\nt = t.mutate(prev_price=t.price.lag(1).over(w))\nt = t.mutate(monthly_return=(t.price / t.prev_price) - 1)\nexpr = t.select(\"monthly_return\", *[c for c in t.columns if c != \"monthly_return\"])\n", prompt="Parse 'Jan 1 2000' dates, order by symbol+date, compute lagged monthly return; return column leftmost.")
```
> _Probes:_ The string-date parse trap: as_timestamp('%b %d %Y') must build AND execute (bare .to_timestamp() with no format silently mis-parses). lag(1).over(window) over a partitioned+ordered series. New-column-first reselect. CRITICAL: as_timestamp yields a UTC-tz timestamp (+00:00) — a downstream trap if any later step compares it to a tz-naive literal. This entry has a WindowFunction op so it is cache_worthy=True (result lives in the snapshot cache, result.parquet may later be evicted).

**3. Attach a multi-series line chart of price over time colored by symbol, then a returns-distribution histogram, to the returns entry.**  _(`catalog_chart`)_

```python
catalog_chart(hash_or_alias="stock_returns", vega_spec={"$schema": "https://vega.github.io/schema/vega-lite/v5.json", "mark": {"type": "line", "point": false}, "encoding": {"x": {"field": "ts", "type": "temporal", "title": "Date"}, "y": {"field": "price", "type": "quantitative", "title": "Close price"}, "color": {"field": "symbol", "type": "nominal"}}})
```
> _Probes:_ Multi-series vega line keyed on a temporal field that was a string in the source and is a tz-aware timestamp post-parse — does vega-embed read the companion-served JSON 'ts' as temporal correctly? FIRST-ORDER BUG SURFACE: the chart data endpoint /{project}/api/data/{hash} reads entry/result.parquet directly and 404s if absent; but stock_returns is cache_worthy so its result.parquet can be evicted while the snapshot cache holds the data. Chart serving does NOT self-heal the way catalog_diff does — chart may render empty/404 on a warm-cache-but-evicted-parquet entry.

**4. Revise the alias to add a rolling 3-month and 12-month moving average plus a cumulative running-max drawdown reference, keeping the prior version in history.**  _(`catalog_revise`)_

```python
catalog_revise(name="stock_returns", code="import xorq.vendor.ibis as ibis\nfrom tallyman_xorq.io import from_catalog\nt = from_catalog(\"stocks_raw\")\nt = t.mutate(ts=t.date.as_timestamp(\"%b %d %Y\"))\nbase = ibis.window(group_by=\"symbol\", order_by=\"ts\")\nroll3 = ibis.window(group_by=\"symbol\", order_by=\"ts\", preceding=2, following=0)\nroll12 = ibis.window(group_by=\"symbol\", order_by=\"ts\", preceding=11, following=0)\ncumw = ibis.window(group_by=\"symbol\", order_by=\"ts\", preceding=None, following=0)\nt = t.mutate(prev_price=t.price.lag(1).over(base))\nt = t.mutate(monthly_return=(t.price / t.prev_price) - 1, ma3=t.price.mean().over(roll3), ma12=t.price.mean().over(roll12), running_max=t.price.max().over(cumw))\nt = t.mutate(drawdown=(t.price / t.running_max) - 1)\nexpr = t.select(\"monthly_return\", \"ma3\", \"ma12\", \"drawdown\", *[c for c in t.columns if c not in (\"monthly_return\", \"ma3\", \"ma12\", \"drawdown\")])\n", prompt="Add rolling 3/12-month MA, running-max, and drawdown windows; keep returns. New cols leftmost.")
```
> _Probes:_ Three distinct window frames in one expr: bounded rolling (preceding=2/11, following=0) AND an unbounded cumulative frame (preceding=None). Multiple WindowFunction ops stress the build's op-classification regex in classify_build. V1 vs V2 now differ by 4 added columns over the SAME (symbol, date) grain — sets up the keyed diff in step 6. Confirms revise keeps V1's hash in forensic history.

**5. Overlay the rolling 3-month moving average on the raw price as a layered line chart on the revised entry.**  _(`catalog_chart`)_

```python
catalog_chart(hash_or_alias="stock_returns", vega_spec={"$schema": "https://vega.github.io/schema/vega-lite/v5.json", "layer": [{"mark": {"type": "line", "opacity": 0.4}, "encoding": {"x": {"field": "ts", "type": "temporal"}, "y": {"field": "price", "type": "quantitative", "title": "price / MA3"}, "color": {"field": "symbol", "type": "nominal"}}}, {"mark": {"type": "line", "strokeDash": [4, 2]}, "encoding": {"x": {"field": "ts", "type": "temporal"}, "y": {"field": "ma3", "type": "quantitative"}, "color": {"field": "symbol", "type": "nominal"}}}]})
```
> _Probes:_ Layered (multi-mark) vega spec where both layers consume the same companion-served parquet but different columns (price vs ma3). Tests that re-attaching a chart to an already-charted alias REPLACES rather than stacks, and that the chart now points at the NEW (V2) hash's parquet — chart-to-hash binding after a revise is a known live-reload-era hazard on this branch.

**6. Diff V1 vs V2 of the alias to see the schema growth and a keyed row-level comparison on the composite (symbol, date) key.**  _(`catalog_diff`)_

```python
catalog_diff(name="stock_returns", va=1, vb=2)
```
> _Probes:_ HEAVIEST bug surface: keyed diff requires a primary key with uniqueness>=0.98 over SHARED columns. No single column is unique (5 symbols repeat, dates repeat across symbols) — the diff MUST infer the COMPOSITE key (symbol, ts) or (symbol, date) via _rank_pk_xorq's width-2 composite search. If primary_key.py inherits a wrong/empty parent key, or the cached primary_key.json from the cheap stocks_raw root poisons the lineage walk, the keyed diff returns None. Both V1 and V2 are cache_worthy (window ops) so cached_result_expr wraps each in the snapshot cache — exercises the self-heal: if a result.parquet was evicted, the xorq-backend diff still composes expr1 join expr2 from the cache and succeeds where chart/post-processing fail.

**7. Preview a scipy-backed post-processing function that computes skew, excess kurtosis, and a Jarque-Bera normality p-value of the monthly_return distribution per symbol, returning a pandas DataFrame.**  _(`catalog_run_post_processing`)_

```python
catalog_run_post_processing(entry="stock_returns", code="def process(expr):\n    import pandas as pd\n    from scipy import stats\n    df = expr.execute()\n    rows = []\n    for sym, g in df.dropna(subset=[\"monthly_return\"]).groupby(\"symbol\"):\n        r = g[\"monthly_return\"].values\n        jb, p = stats.jarque_bera(r)\n        rows.append({\"symbol\": sym, \"n\": len(r), \"mean_ret\": float(r.mean()), \"vol\": float(r.std()), \"skew\": float(stats.skew(r)), \"excess_kurtosis\": float(stats.kurtosis(r)), \"jarque_bera\": float(jb), \"jb_pvalue\": float(p)})\n    return pd.DataFrame(rows).sort_values(\"jb_pvalue\")\n")
```
> _Probes:_ The escape-hatch: process(expr) imports scipy and returns a grouped pandas.DataFrame that pure ibis cannot express (Jarque-Bera test). CRITICAL ASYMMETRY BUG: run_post_processing reads entry/result.parquet DIRECTLY (post_processing.py:233) and raises 'no result.parquet found' if it is missing — but stock_returns is cache_worthy, so its result.parquet may have been EVICTED while catalog_diff in step 6 self-healed. Same entry, same moment: the diff works, the post-processing preview fails. Also probes _restricted_globals: is scipy importable inside the post-processing sandbox at all?

**8. Commit the working returns-distribution post-processing function so it appears in the embed dropdown, then immediately commit a second per-month cross-sectional rank post-processor and toggle the first off.**  _(`catalog_add_post_processing`)_

```python
catalog_add_post_processing(name="return_normality", source="def process(expr):\n    import pandas as pd\n    from scipy import stats\n    df = expr.execute()\n    rows = []\n    for sym, g in df.dropna(subset=[\"monthly_return\"]).groupby(\"symbol\"):\n        r = g[\"monthly_return\"].values\n        jb, p = stats.jarque_bera(r)\n        rows.append({\"symbol\": sym, \"n\": len(r), \"skew\": float(stats.skew(r)), \"excess_kurtosis\": float(stats.kurtosis(r)), \"jb_pvalue\": float(p)})\n    return pd.DataFrame(rows).sort_values(\"jb_pvalue\")\n")
```
> _Probes:_ Live-reload of post-processing (the headline branch change): does the dry-run validation against a 1-row in-memory table tolerate a process() that groupby-aggregates to ZERO rows (a 1-row memtable with no monthly_return becomes empty after dropna -> empty DataFrame)? A scipy call on an empty array inside the validator would raise at add time even though the real entry works — a false-negative rejection. Also probes whether scipy is importable in the add-time validation sandbox vs the run-time sandbox (they may differ).

### Sample plots (Vega-Lite v5, attached via `catalog_chart`)

**multi-series line (price over time by symbol)** → `stock_returns (V1, step 3)`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "mark": {
    "type": "line",
    "point": false
  },
  "encoding": {
    "x": {
      "field": "ts",
      "type": "temporal",
      "title": "Date"
    },
    "y": {
      "field": "price",
      "type": "quantitative",
      "title": "Close price"
    },
    "color": {
      "field": "symbol",
      "type": "nominal"
    }
  }
}
```

**layered line (raw price + dashed MA3 overlay, by symbol)** → `stock_returns (V2, step 5)`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "layer": [
    {
      "mark": {
        "type": "line",
        "opacity": 0.4
      },
      "encoding": {
        "x": {
          "field": "ts",
          "type": "temporal"
        },
        "y": {
          "field": "price",
          "type": "quantitative",
          "title": "price / MA3"
        },
        "color": {
          "field": "symbol",
          "type": "nominal"
        }
      }
    },
    {
      "mark": {
        "type": "line",
        "strokeDash": [
          4,
          2
        ]
      },
      "encoding": {
        "x": {
          "field": "ts",
          "type": "temporal"
        },
        "y": {
          "field": "ma3",
          "type": "quantitative"
        },
        "color": {
          "field": "symbol",
          "type": "nominal"
        }
      }
    }
  ]
}
```

**histogram of monthly_return** → `stock_returns (V2) — returns distribution`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "mark": "bar",
  "encoding": {
    "x": {
      "field": "monthly_return",
      "type": "quantitative",
      "bin": {
        "maxbins": 40
      },
      "title": "Monthly return"
    },
    "y": {
      "aggregate": "count",
      "type": "quantitative",
      "title": "Months"
    },
    "color": {
      "field": "symbol",
      "type": "nominal"
    }
  }
}
```

### Bugs this script is built to surface

- **catalog_chart data serving on a cache_worthy window entry (companion app.py:710 api_data + result_cache.py)** — stock_returns contains WindowFunction ops, so cache_worthy=True and its result lives in the ParquetSnapshotCache, not necessarily in entry/result.parquet. The chart data endpoint /{project}/api/data/{hash} reads entry/result.parquet directly and raises HTTPException(404,'no result.parquet') when it is absent — it never calls ensure_result/cached_result_expr to self-heal. So after the snapshot cache holds the data but the in-place parquet is evicted, the line/histogram charts load against a 404 and render empty, even though catalog_diff on the very same entry succeeds because the diff path DOES self-heal.
- **catalog_run_post_processing / catalog_add_post_processing vs result cache (post_processing.py:233)** — run_post_processing reads entry/result.parquet directly and raises PostProcessingRunError('no result.parquet found') if missing. For a cache_worthy window entry whose parquet was evicted, the scipy post-processing preview fails with a misleading 'no result.parquet' error while the data is fully recoverable from the snapshot cache — the same self-heal asymmetry as the chart endpoint. Additionally, add-time validation dry-runs process() against a 1-row in-memory table; a groupby+dropna+scipy process() collapses that to an empty DataFrame and scipy.stats.skew/jarque_bera on an empty array raises, falsely rejecting a function that works against the real 560-row entry.
- **Composite primary-key inference for the keyed diff (primary_key.py resolve_primary_key + buckaroo.compare._rank_pk_xorq)** — stock_returns has no unique single column (symbol repeats 112x, date repeats 5x), so the keyed diff must find the width-2 composite (symbol, ts). resolve_primary_key walks lineage: V2 is cache_worthy (grain looks changed to the classifier because of window ops) so it detects fresh, but V1->V2 share grain and the inherit path keys on cache_worthy() being False. Because BOTH versions are window entries (cache_worthy=True), neither inherits — each runs the full composite search, and if _rank_pk_xorq's max_group check rejects (symbol, ts) (a symbol with a duplicated parsed date from the tz-coercion collapsing two months would inflate one group), diff_keys returns [] and the keyed diff silently degrades to None despite an obvious natural key.
- **String-date parse + UTC tz coercion in window ordering (as_timestamp('%b %d %Y'))** — as_timestamp with a strptime format returns a Timestamp(timezone='UTC') column. order_by on this tz-aware column is fine, but if any revision later mixes the parsed ts with a tz-naive ibis literal or memtable date (the documented 'memtable date/timestamp literals fail pyarrow coercion' trap), the comparison or union mis-aligns. An LLM that uses .to_timestamp() with no format on 'Jan 1 2000' gets nulls/mis-parse instead of an error, silently producing all-NaN lag returns that pass schema validation and only show as an empty/garbage line chart.
- **Window-function rank construction (col.rank().over(window) XorqTypeError)** — The natural per-month cross-sectional rank an LLM writes — t.price.rank().over(ibis.window(group_by='month', order_by=ibis.desc('price'))) — raises XorqTypeError('No reduction or analytic function found to construct a window expression') because rank must be built as the table-level ibis.rank().over(window), not column.rank().over(window). catalog_run/create surfaces this as a save-time failure, and the error text gives no hint that the fix is ibis.rank() instead of col.rank().
- **classify_build op-detection regex on multi-window expressions (result_cache.py:53-56)** — classify_build greps serialized YAML for 'op: WindowFunction' / 'RowNumber' to decide cache_worthy. V2 stacks four window frames (bounded rolling x2, unbounded cumulative, lag). If xorq serializes any of these frames under an op name not in _EXPENSIVE_OPS (e.g. a frame-bound variant or an analytic op the regex misses), the entry is mis-classified as cheap, its result.parquet is regenerated in place instead of cached, and the cached_result_expr/ensure_result branch taken at diff time mismatches what was written at build time — producing inconsistent self-heal behavior between build and diff.

---


## 2. Red vs White: A Welch t-test gauntlet for the post-processing live-reload

- **Archetype:** `hypothesis-testing-scipy`
- **Mimics:** Reproduces the inferential-stats notebook: UNION the two wine files with a literal wine_type label, compute per-type group means/std/n and pooled-std effect sizes for every chemical property, then reach for scipy via a process(expr) post-processing function that runs Welch t-tests and returns a tidy p-value DataFrame. It stresses the system because the dry-run validator runs process() against a 3-row {a,b} memtable while the real run executes against thousands of rows with space-named columns, so a scipy test that needs real distributions and real column names is exactly the kind of function that passes catalog_run_post_processing but fails validation on commit, or vice-versa.
- **Dataset:** wine_red.parquet (1,599 rows) + wine_white.parquet (4,898 rows), both UCI; unioned to 6,497 rows with a wine_type label. Chosen because the two files share an identical 12-column schema with space-in-name chemical features, which is the canonical setup for a two-sample (red vs white) hypothesis test and the schema-alignment .union() path.
- **DS technique:** Two-sample statistical inference: per-group descriptive stats (mean/std/n), Cohen's-d-style effect size via pooled standard deviation computed in pure ibis, then Welch's unequal-variance t-test per property in scipy.stats returning a Benjamini-style significance flag — the model-fitting escape hatch via a process(expr) that imports scipy and returns a pandas.DataFrame.
- **Data files to stage:** `wine_red.parquet`, `wine_white.parquet` (~14 tool calls)

### Steps

**1. Stage the two raw UCI wine files into the project so user code can reference them by relative name. Load both, naming each so they get notebook cells.**  _(`catalog_load_parquet`)_

```python
catalog_load_parquet(rel_path="wine_red.parquet", name="wine_red", prompt="UCI red wine quality: 11 float chemical features with spaces in names + quality int")
catalog_load_parquet(rel_path="wine_white.parquet", name="wine_white", prompt="UCI white wine quality, identical schema to red")
```
> _Probes:_ Two back-to-back named loads. Probes whether catalog_load_parquet correctly registers two aliases + two notebook cells in one beat, and that the staged files land under <project>/data/ where from_project can find them. schema.json for each must capture the space-in-name columns verbatim (used later for primary-key inference and diff).

**2. UNION red and white into one labeled table. Each side gets a literal wine_type column, then .union(); the new label column is selected leftmost. This is the foundation entry for the whole hypothesis test.**  _(`catalog_create`)_

```python
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_project

red = from_project("wine_red.parquet").mutate(wine_type=ibis.literal("red"))
white = from_project("wine_white.parquet").mutate(wine_type=ibis.literal("white"))
# re-select new column leftmost and force identical column order on both sides
cols = ["wine_type", "fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol", "quality"]
red = red.select(cols)
white = white.select(cols)
expr = red.union(white)

catalog_create(name="wine_all", code=code, prompt="red+white unioned with a wine_type label, columns aligned for the two-sample test")
```
> _Probes:_ .union() of two from_project sources: column order/type alignment is fragile — if either select() reorders columns differently the union silently mis-pairs or raises a schema-mismatch. Probes that ibis.literal('red') becomes a proper string column (not a constant folded away), and that selecting cols by bracket-name list survives the space-in-name columns. Also stresses cache_worthy: a union is not in _EXPENSIVE_OPS, so wine_all may be classified CHEAP despite being a real combine — a mis-classification surface for the result cache and primary-key inheritance.

**3. Compute per-wine_type descriptive stats for every chemical property: mean, std, and n, grouped by wine_type. This is the table the t-test and the grouped-bar chart read from.**  _(`catalog_create`)_

```python
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_catalog

t = from_catalog("wine_all")
props = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"]
aggs = {"n": t.count()}
for p in props:
    safe = p.replace(" ", "_")
    aggs[f"{safe}_mean"] = t[p].mean()
    aggs[f"{safe}_std"] = t[p].std()
expr = t.group_by("wine_type").aggregate(**aggs)

catalog_create(name="wine_group_stats", code=code, prompt="per-wine_type mean/std/n for all 11 chemical properties")
```
> _Probes:_ Aggregate alias 'n' deliberately avoids shadowing count; but the **aggs dict expansion with f-string keys built from space-stripped names probes whether ibis accepts dynamically-named aggregates and whether t[p] bracket-access works inside a comprehension. group_by('wine_type') on a literal-derived column probes that the union's label survived as a groupable string. This entry IS cache_worthy (Aggregate), so it exercises the ParquetSnapshotCache write path and becomes the keyed-diff anchor for later revisions.

**4. Add a long-form effect-size entry: pooled-std standardized mean difference (Cohen's d) per property, computed in pure ibis by pivoting the two group rows. Build a tidy (property, red_mean, white_mean, pooled_std, cohens_d) table.**  _(`catalog_create`)_

```python
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_catalog

g = from_catalog("wine_group_stats")
red = g.filter(g.wine_type == "red")
white = g.filter(g.wine_type == "white")
props = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"]
rows = []
for p in props:
    s = p.replace(" ", "_")
    rm = red[f"{s}_mean"].sum()
    wm = white[f"{s}_mean"].sum()
    rsd = red[f"{s}_std"].sum()
    wsd = white[f"{s}_std"].sum()
    pooled = ((rsd**2 + wsd**2) / 2.0).sqrt()
    rows.append(ibis.struct({"property": p, "red_mean": rm, "white_mean": wm, "pooled_std": pooled, "cohens_d": (rm - wm) / pooled}))
# materialize the per-property structs into a table
expr = ibis.memtable({"property": props}).mutate(idx=ibis.row_number())

catalog_create(name="wine_effect_size", code=code, prompt="per-property pooled-std Cohen's d, red vs white")
```
> _Probes:_ This is a deliberate hard case: building a per-property table by reducing two single-row group frames. The naive .sum() over a 1-row filter is a workaround for 'extract the scalar', and assembling rows via ibis.struct then needing a real table is awkward in pure ibis — a likely codegen failure that the operator must iterate on. Probes catalog_create error reporting and whether the agent falls back to doing the effect size inside the upcoming post-processing function instead. Also probes ibis.memtable with row_number and whether a no-source memtable entry is cache-classified sanely.

**5. DRY-RUN the scipy Welch t-test as a post-processing function against the REAL union entry before committing. process(expr) executes the ibis table to pandas, runs scipy.stats.ttest_ind(equal_var=False) per property red-vs-white, returns a tidy DataFrame (property, t_stat, p_value, significant).**  _(`catalog_run_post_processing`)_

```python
code = '''
def process(expr):
    import pandas as pd
    import numpy as np
    from scipy import stats
    df = expr.execute()
    props = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"]
    red = df[df["wine_type"] == "red"]
    white = df[df["wine_type"] == "white"]
    out = []
    for p in props:
        t_stat, p_value = stats.ttest_ind(red[p].dropna(), white[p].dropna(), equal_var=False)
        out.append({"property": p, "t_stat": float(t_stat), "p_value": float(p_value), "significant": bool(p_value < 0.05)})
    res = pd.DataFrame(out).sort_values("p_value").reset_index(drop=True)
    return res
'''
catalog_run_post_processing(code=code, entry="wine_all")
```
> _Probes:_ THE central bug surface. catalog_run_post_processing reads wine_all/result.parquet, does pq.read_table -> to_pandas -> xo.memtable -> process(table). process calls .execute() to get a pandas frame, then indexes df['wine_type'], df['fixed acidity'] (space name!) and runs scipy. Probes: (a) does to_pandas preserve the exact space-in-name columns so df['free sulfur dioxide'] resolves; (b) does the round-trip through xo.memtable keep wine_type as comparable strings; (c) does scipy.stats import inside the restricted-globals exec — the _restricted_globals only injects ibis/xorq, NOT scipy, so the import must work via the real (non-restricted) builtins __import__ at run time. If run_post_processing's exec strips __import__, this raises and exposes a divergence between run (works) and validate (blocks).

**6. COMMIT the validated t-test function. This is where the dry-run validator (3-row {a,b} memtable) collides with a function written for real wine columns. Submit the exact same code via catalog_add_post_processing and observe whether validation passes or rejects.**  _(`catalog_add_post_processing`)_

```python
catalog_add_post_processing(name="welch_ttest", source=code)  # same `code` string from step 5
```
> _Probes:_ HIGHEST-VALUE PROBE. validate_post_processing_source runs process() against xo.memtable({'a':[1,2,3],'b':['x','y','z']}). The t-test process() does df[df['wine_type']=='red'] on a frame with only columns a,b -> KeyError 'wine_type' -> PostProcessingSourceError, so commit is REJECTED even though step 5's real run succeeded. This is the precise validate-vs-run divergence on this branch. The operator must then make process() defensive (guard: if 'wine_type' not in df.columns: return df) to pass the dry-run — probing whether the agent discovers the validator's tiny-memtable contract and writes a function that no-ops on the dry-run shape. Also probes scipy import surviving the validator's restricted exec.

**7. Re-submit a dry-run-safe version of the function so the commit lands, then verify it is live via the listing. The guard returns the input unchanged when the expected columns are absent (the 3-row dry-run), runs the real test otherwise.**  _(`catalog_add_post_processing`)_

```python
safe_code = '''
def process(expr):
    import pandas as pd
    from scipy import stats
    df = expr.execute()
    if "wine_type" not in df.columns:
        return df  # dry-run shape: no-op so the validator passes
    props = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"]
    red = df[df["wine_type"] == "red"]
    white = df[df["wine_type"] == "white"]
    out = []
    for p in props:
        ts, pv = stats.ttest_ind(red[p].dropna(), white[p].dropna(), equal_var=False)
        out.append({"property": p, "t_stat": float(ts), "p_value": float(pv), "significant": bool(pv < 0.05)})
    return pd.DataFrame(out).sort_values("p_value").reset_index(drop=True)
'''
catalog_add_post_processing(name="welch_ttest", source=safe_code)
catalog_list_post_processings()
```
> _Probes:_ Probes the LIVE-RELOAD path (this branch's headline feature): committing welch_ttest should push a post_processing_changed notify and make the option appear in the embed dropdown for wine_all's session WITHOUT a buckaroo restart. Then catalog_list_post_processings must show welch_ttest active. Re-submitting an existing name overwrites the file (per write_post_processing) — probes whether overwrite + live-reload correctly replaces the stale klass rather than registering a duplicate dropdown entry or leaking the old definition. Selecting it in the browser must re-render wine_all's table as the 11-row p-value frame.

**8. Toggle the function off and on to stress live-reload churn, then revise wine_group_stats to drop a property and diff the two versions, exercising keyed diff + primary-key inference across an aggregate-grain revision.**  _(`catalog_revise`)_

```python
# (a) churn the post-processing live-reload
catalog_remove_post_processing(name="welch_ttest")        # soft-delete -> _disabled/, dropdown option vanishes
catalog_add_post_processing(name="welch_ttest", source=safe_code)  # re-add -> reappears

# (b) revise the grouped-stats entry (drop the two sulfur columns) and diff
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_catalog
t = from_catalog("wine_all")
props = ["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides", "density", "pH", "sulphates", "alcohol"]
aggs = {"n": t.count()}
for p in props:
    s = p.replace(" ", "_")
    aggs[f"{s}_mean"] = t[p].mean()
    aggs[f"{s}_std"] = t[p].std()
expr = t.group_by("wine_type").aggregate(**aggs)
catalog_revise(name="wine_group_stats", code=code, prompt="drop the two sulfur-dioxide properties")
catalog_diff(name="wine_group_stats", va=-2, vb=-1)
```
> _Probes:_ Two-in-one. The remove/re-add cycle hammers the live-reload remove path: remove moves to _disabled/, re-add must clear the disabled copy or list shows both. catalog_diff on an aggregate-grain revision probes diff_keys/primary_key inference: wine_group_stats has only 2 rows keyed by wine_type, and both versions are cache_worthy (Aggregate) so neither inherits a parent PK — _detect_pk_xorq must run live on a 2-row frame and pick wine_type as the key. The schema diff must report the dropped *_mean/*_std columns; the keyed-row diff must align the two wine_type rows and surface changed/unchanged cells. A wrong PK (e.g. picking a float mean column at 100% cardinality on 2 rows) produces a nonsense keyed diff — a sharp primary-key-inference surface on a tiny aggregate.

### Sample plots (Vega-Lite v5, attached via `catalog_chart`)

**grouped bar, mean alcohol/volatile acidity by wine_type** → `wine_group_stats`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "description": "Mean +/- std of key chemical properties by wine type",
  "transform": [
    {
      "fold": [
        "alcohol_mean",
        "volatile_acidity_mean",
        "citric_acid_mean",
        "residual_sugar_mean"
      ],
      "as": [
        "property",
        "mean_value"
      ]
    }
  ],
  "mark": {
    "type": "bar"
  },
  "encoding": {
    "x": {
      "field": "wine_type",
      "type": "nominal",
      "title": "wine type"
    },
    "y": {
      "field": "mean_value",
      "type": "quantitative",
      "title": "group mean"
    },
    "color": {
      "field": "wine_type",
      "type": "nominal"
    },
    "column": {
      "field": "property",
      "type": "nominal",
      "title": "chemical property"
    }
  }
}
```

**overlaid alcohol distribution red vs white (layered histogram)** → `wine_all`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "description": "Overlaid alcohol distribution, red vs white",
  "mark": {
    "type": "bar",
    "opacity": 0.55,
    "binSpacing": 0
  },
  "encoding": {
    "x": {
      "field": "alcohol",
      "type": "quantitative",
      "bin": {
        "maxbins": 40
      },
      "title": "alcohol (% by vol)"
    },
    "y": {
      "aggregate": "count",
      "type": "quantitative",
      "stack": null,
      "title": "count"
    },
    "color": {
      "field": "wine_type",
      "type": "nominal",
      "scale": {
        "domain": [
          "red",
          "white"
        ],
        "range": [
          "#7a1f2b",
          "#d9c47a"
        ]
      },
      "title": "wine type"
    }
  }
}
```

**p-value / effect-size bar (attach after selecting welch_ttest post-processing)** → `wine_all`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "description": "Welch t-test p-value by property; bars below 0.05 flagged significant",
  "mark": {
    "type": "bar"
  },
  "encoding": {
    "y": {
      "field": "property",
      "type": "nominal",
      "sort": "-x",
      "title": "chemical property"
    },
    "x": {
      "field": "p_value",
      "type": "quantitative",
      "scale": {
        "type": "log"
      },
      "title": "Welch t-test p-value (log)"
    },
    "color": {
      "field": "significant",
      "type": "nominal",
      "scale": {
        "domain": [
          true,
          false
        ],
        "range": [
          "#2c7",
          "#bbb"
        ]
      },
      "title": "p < 0.05"
    }
  }
}
```

### Bugs this script is built to surface

- **catalog_add_post_processing validator vs catalog_run_post_processing (step 6)** — validate_post_processing_source dry-runs process() against xo.memtable({'a':[1,2,3],'b':['x','y','z']}); a scipy Welch-test process() that does df[df['wine_type']=='red'] and indexes space-named columns raises KeyError on that 3-row {a,b} frame and is REJECTED at commit time, even though step 5's catalog_run_post_processing (real result.parquet) succeeded. The two paths disagree on what a valid function is — a function the operator just saw produce correct p-values cannot be committed without a defensive column-presence guard.
- **scipy import under restricted-globals exec in both validate and run (steps 5-7)** — _restricted_globals() injects only ibis and xorq and sets __builtins__ to a hand-picked allowlist that omits __import__. If the exec'd process() body's 'from scipy import stats' resolves __import__ through that stripped builtins map, the import fails inside validate_post_processing_source and/or run_post_processing with a confusing 'source raised at exec time' or 'process() raised' message — so the scipy escape hatch is silently unusable, and the failure mode differs between the validate path (tiny memtable) and run path (real data), making it hard to diagnose.
- **.union() schema/type alignment of two from_project sources (step 2)** — red.select(cols).union(white.select(cols)) requires byte-identical column order AND dtypes. The space-named float columns and the freshly-mutated ibis.literal('red')/('white') wine_type must line up; if white's parquet promotes any column to a different float width or the literal label is typed as a different string variant, .union() raises a schema-mismatch at catalog_create-save time (the bare-ibis-import-style late TypeError), or worse unions silently with mis-paired columns producing a corrupt wine_all that every downstream step inherits.
- **primary-key inference on a 2-row aggregate diff (step 8)** — catalog_diff(wine_group_stats) compares two Aggregate (cache_worthy) versions, so neither inherits a parent PK and _detect_pk_xorq runs live on a 2-row frame. With only 2 rows every column has ~100% cardinality at the 0.98 threshold, so the detector may pick a float *_mean column (or an arbitrary single column) as the key instead of wine_type, producing a meaningless keyed-row diff. The dropped sulfur-dioxide columns between versions also shrink the schema, so diff_keys must intersect a key present in BOTH schemas — wine_type survives but a wrongly-detected mean-column key from one side may not exist on the other, forcing a fallback that the keyed diff may not handle gracefully.
- **post-processing remove/re-add live-reload churn (steps 7-8)** — remove_post_processing soft-deletes welch_ttest to _disabled/ and re-add overwrites; the live-reload path (this branch) must (a) clear the _disabled/welch_ttest.py copy on re-add so catalog_list_post_processings doesn't show the name as both active and disabled, and (b) replace the already-loaded ColAnalysis klass in the live buckaroo session rather than registering a second dropdown entry or serving the stale pre-remove definition. A leaked klass means selecting welch_ttest re-renders with the old function body, masking the freshly-committed version.
- **cache_worthy mis-classification of wine_all union (step 2 feeding 3-8)** — classify_build scans the build YAML for ops in _EXPENSIVE_OPS, which lists Join/Aggregate/Sort/Window but NOT Union. A union of two 1.6k/4.9k-row reads is therefore classified CHEAP, so wine_all gets no snapshot cache and relies on in-place result.parquet regeneration. If that result.parquet is evicted, ensure_result recomputes the union from the build — but every downstream entry (wine_group_stats, the t-test run, both charts' /api/data) reads through cached_result_expr(wine_all), so a flaky union recompute or a stale label column silently corrupts the entire hypothesis-test chain and the diff anchor.

---


## 3. Who Made It Off the Titanic: Survival Rates by Cohort, Cleaned As You Go

- **Archetype:** `survival-cohort-analysis`
- **Mimics:** Mimics the canonical "target-rate by segment, then impute and re-audit" notebook: survival rate by sex/class/age-band, a sex-by-class crosstab, group-wise median age imputation done as catalog_revise versions, then catalog_diff to prove the impute touched only the null rows. It stresses the system because titanic has no primary key, heavy nulls (age 177, deck 688), a 0/1 int target averaged as a rate, bool cohort columns, and a window-based group impute that the cache classifier marks "expensive" — which is exactly the path PK inheritance is supposed to skip.
- **Dataset:** titanic.parquet (891 rows, seaborn). Chosen because it is the only staged set with a 0/1 outcome target (survived), dense nulls across multiple columns (age 177, deck 688, embarked/embark_town 2), native bool cohort columns (adult_male, alone), and no natural row key — the four properties this archetype exists to exercise. Single parquet, no joins needed; chaining is purely catalog_revise -> catalog_diff across impute versions.
- **DS technique:** Outcome-rate (binary target mean) segmentation + crosstab; group-wise median imputation via a partitioned window function (the pandas .transform analogue); null-rate auditing; before/after keyed diff to verify an imputation is surgical (changed rows == null rows). Plus a scipy two-proportion z-test committed as a post-processing function returning a pandas DataFrame.
- **Data files to stage:** `titanic.parquet` (~13 tool calls)

### Steps

**1. Drop the raw titanic parquet into the project as a named catalog entry so it gets a notebook cell and an alias we can revise later.**  _(`mcp__tallyman__catalog_load_parquet`)_

```python
catalog_load_parquet(rel_path="titanic.parquet", name="titanic_raw", prompt="Raw seaborn titanic: 891 passengers, survived 0/1 target, heavy nulls in age/deck, bool adult_male/alone.")
```
> _Probes:_ First-contact load + alias creation. Probes that a name= load registers an alias AND a notebook cell in one shot, and that a bool-heavy / null-heavy schema round-trips through the parquet registration without coercing adult_male/alone to int or dropping the all-but-203-null deck column.

**2. Compute survival rate by sex and by class as a long-format cohort table — the headline target-rate-by-segment view. Average the int 0/1 survived column to get a rate, count cohort size, and put the new computed columns first.**  _(`mcp__tallyman__catalog_create`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("titanic_raw")
by_sex = t.group_by("sex").aggregate(survival_rate=t.survived.mean(), n=t.survived.count()).mutate(cohort_kind=ibis.literal("sex")).rename(cohort_value="sex")
by_class = t.group_by("pclass").aggregate(survival_rate=t.survived.mean(), n=t.survived.count()).mutate(cohort_kind=ibis.literal("pclass")).mutate(cohort_value=t.pclass.cast("string")).drop("pclass")
unioned = by_sex.union(by_class)
expr = unioned.select("cohort_kind", "cohort_value", "survival_rate", "n").order_by(["cohort_kind", ibis.desc("survival_rate")])
```
> _Probes:_ survived.mean() on an int 0/1 column must yield a fractional rate (not int truncation to 0). UNION of two aggregates with different group keys forces column-order/type alignment (pclass int cast to string vs sex string) — the documented union trap. Aggregate alias n= avoids shadowing the count method. Aggregate op makes this entry cache_worthy=True in the result cache.

**3. Build the sex x pclass survival crosstab by grouping on two columns at once, then attach it as a named entry. This is the wide cohort grid behind the heatmap.**  _(`mcp__tallyman__catalog_create`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("titanic_raw")
expr = (t.group_by(["sex", "pclass"]).aggregate(survival_rate=t.survived.mean(), n=t.survived.count(), avg_fare=t.fare.mean()).order_by(["sex", "pclass"]))
```
> _Probes:_ group_by([two cols]) crosstab grain. Companion must render a 6-row long table that the heatmap (step 9) re-pivots client-side via Vega-Lite encoding — long->wide happens in the chart, not the query. Probes that catalog_chart's auto-served parquet exposes sex+pclass+survival_rate columns by exact name for the heatmap x/y/color channels.

**4. Audit the null landscape before imputing: per-column null counts and null fractions for the columns we care about (age, deck, embarked), computed as explicit aggregates since .describe() is unavailable.**  _(`mcp__tallyman__catalog_run`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("titanic_raw")
n = t.count()
expr = t.aggregate(
    n_rows=t.count(),
    age_nulls=t.age.isnull().sum(),
    age_null_frac=t.age.isnull().sum() / n,
    deck_nulls=t.deck.isnull().sum(),
    deck_null_frac=t.deck.isnull().sum() / n,
    embarked_nulls=t.embarked.isnull().sum(),
)
```
> _Probes:_ isnull().sum() over a column with 177 (age) / 688 (deck) nulls — boolean-to-int summation. Division by a scalar t.count() inside aggregate (scalar broadcast). Scratch entry via catalog_run (no alias). deck is ~77% null, a candidate for the 'should we even keep this column' decision the narrative makes.

**5. Create the imputation alias as V1 = the raw passenger rows passed through unchanged (select all columns), establishing a baseline version to diff against. This is deliberately row-preserving and cheap.**  _(`mcp__tallyman__catalog_create`)_

```python
from tallyman_xorq.io import from_catalog
t = from_catalog("titanic_raw")
cols = ["survived", "pclass", "sex", "age", "sibsp", "parch", "fare", "embarked", "class", "who", "adult_male", "deck", "embark_town", "alive", "alone"]
expr = t.select(*cols)
```
> _Probes:_ Establishes a lineage root for PK inference. Pure projection -> cache_worthy=False -> resolve_primary_key runs full _detect_pk_xorq cardinality scan. titanic has NO single unique column at threshold 0.98 (closest is fare ~0.27 unique, age fractional) so this caches keys=[] in primary_key.json. Probes that the system gracefully records 'no usable key' rather than erroring.

**6. Revise the impute alias to V2: median-impute age within each (sex, pclass) cohort using a partitioned window — the .transform trap done in ibis. Fill nulls with the group median, keep all other rows identical. New/changed column goes first.**  _(`mcp__tallyman__catalog_revise`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("impute_age")
w = ibis.window(group_by=["sex", "pclass"])
group_median = t.age.approx_median().over(w)
filled = t.age.fill_null(group_median)
expr = t.mutate(age=filled).select(
    "age", "survived", "pclass", "sex", "sibsp", "parch", "fare", "embarked",
    "class", "who", "adult_male", "deck", "embark_town", "alive", "alone")
```
> _Probes:_ THE central stress. approx_median().over(window) is the group-transform trap (median not mean). fill_null with a windowed scalar must broadcast per partition, not collapse rows. Critically: the WindowFunction op makes this revision cache_worthy=True, so resolve_primary_key SKIPS the parent-inherit branch (which only fires when cache_worthy=False) and re-detects on V2 — even though it is genuinely row-preserving. Cross-check: does V2 still report keys=[] consistently with V1, or does the classifier-vs-reality mismatch produce a different/absent key?

**7. Diff V1 vs V2 of the impute alias to prove the age impute changed exactly the 177 null rows and nothing else: row count identical, only age column stats shifted, keyed-row diff shows only modified (not added/removed) rows.**  _(`mcp__tallyman__catalog_diff`)_

```python
catalog_diff(name="impute_age", va=1, vb=2)
```
> _Probes:_ Highest-value bug hunter. diff_keys asks both V1/V2 for a resolved PK; both are []/composite-less, so key_diff_xorq falls back to LIVE detection on a keyless 891-row frame — does it crash, pick a spurious key, or duplicate-explode the join? stats_diff_xorq must show age.null_count 177->0 and age.mean shifting while n stays 891. The xorq diff backend composes V1 (cheap, deferred_read_parquet) JOIN V2 (expensive, cache-wrapped WindowFunction) — mixing a cached and uncached side through cached_result_expr is the live-reload/result-cache integration point most likely to mis-resolve.

**8. Add an age-band cohort as a third impute version (V3) so a second diff exercises schema-add diffing on a keyless frame, then audit survival rate by band.**  _(`mcp__tallyman__catalog_revise`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("impute_age")
band = (ibis.case().when(t.age < 13, "child").when(t.age < 20, "teen").when(t.age < 40, "adult").when(t.age < 60, "middle").else_("senior").end())
expr = t.mutate(age_band=band).select("age_band", *t.columns)
```
> _Probes:_ select(newcol, *t.columns) leftmost-placement convention. ibis.case() ladder on a now-fully-imputed age (no nulls reach else_). schema_diff between V2 and V3 must report added=['age_band'] with row_count unchanged. A SECOND catalog_diff(impute_age, va=2, vb=3) re-exercises keyless PK fallback on a frame whose schema grew — the diff_keys 'key present in BOTH schemas' guard with mismatched columns.

**9. Attach a grouped bar chart of survival rate by class, split by sex, to the crosstab entry — the headline cohort comparison.**  _(`mcp__tallyman__catalog_chart`)_

```python
catalog_chart(hash_or_alias="surv_by_sex_class", vega_spec={
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Survival rate by class, split by sex",
  "mark": {"type": "bar"},
  "encoding": {
    "x": {"field": "pclass", "type": "ordinal", "title": "Passenger class"},
    "xOffset": {"field": "sex", "type": "nominal"},
    "y": {"field": "survival_rate", "type": "quantitative", "axis": {"format": ".0%"}},
    "color": {"field": "sex", "type": "nominal", "scale": {"range": ["#5778a4", "#e49444"]}},
    "tooltip": [{"field": "sex"}, {"field": "pclass"}, {"field": "survival_rate", "format": ".1%"}, {"field": "n"}]
  }
})
```
> _Probes:_ Vega-Lite xOffset grouped-bar (v5-only feature) over the auto-served crosstab parquet. Probes that the companion serves the entry's 6-row parquet as named-column chart data with NO inlined rows, and that pclass (int) maps to ordinal and survival_rate to a %-formatted quantitative axis.

**10. Attach a heatmap of survival rate over the sex x pclass grid to the same crosstab entry — long-format data re-pivoted by the chart's x/y/color channels.**  _(`mcp__tallyman__catalog_chart`)_

```python
catalog_chart(hash_or_alias="surv_by_sex_class", vega_spec={
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Survival rate heatmap: sex x class",
  "mark": {"type": "rect"},
  "encoding": {
    "x": {"field": "pclass", "type": "ordinal", "title": "Class"},
    "y": {"field": "sex", "type": "nominal", "title": "Sex"},
    "color": {"field": "survival_rate", "type": "quantitative", "scale": {"scheme": "redyellowgreen", "domain": [0, 1]}},
    "tooltip": [{"field": "sex"}, {"field": "pclass"}, {"field": "survival_rate", "format": ".1%"}, {"field": "n"}]
  }
})
```
> _Probes:_ SECOND chart on the SAME entry — probes that catalog_chart appends/stacks specs rather than overwriting the step-9 bar (multiple chart_specs per hash). rect heatmap with a fixed color domain [0,1]. Whether the companion renders two charts above one table cleanly.

**11. Preview a two-proportion z-test (female vs male survival) as a post-processing function that imports scipy and returns a pandas DataFrame, run against the crosstab entry to confirm it produces a clean stat table before committing.**  _(`mcp__tallyman__catalog_run_post_processing`)_

```python
catalog_run_post_processing(entry="surv_by_sex_class", code="def process(expr):\n    import pandas as pd\n    from scipy.stats import norm\n    import numpy as np\n    df = expr.execute()\n    g = df.groupby('sex').apply(lambda d: pd.Series({'survived': (d['survival_rate'] * d['n']).sum(), 'total': d['n'].sum()}))\n    f, m = g.loc['female'], g.loc['male']\n    p1, p2 = f['survived']/f['total'], m['survived']/m['total']\n    p = (f['survived']+m['survived'])/(f['total']+m['total'])\n    se = np.sqrt(p*(1-p)*(1/f['total']+1/m['total']))\n    z = (p1-p2)/se\n    pval = 2*(1-norm.cdf(abs(z)))\n    return pd.DataFrame([{'female_rate': p1, 'male_rate': m, 'z_stat': z, 'p_value': pval}])")
```
> _Probes:_ THE data-science escape hatch + live-reload-post-processing hot path. expr.execute() pulls the cached aggregate to pandas; scipy import inside process(); returning a 1-row pandas.DataFrame (not ibis). Probes run_post_processing executing against result.parquet of a cache_worthy entry — does it self-heal if result.parquet was evicted? Note the intentional bug bait: 'male_rate' is assigned m (the whole Series) instead of p2 — does the previewer surface the malformed column or silently mangle it? Iterating to fix this is the realistic loop.

**12. Commit the corrected two-proportion z-test as a permanent post-processing option so it appears in the embed dropdown on next session load.**  _(`mcp__tallyman__catalog_add_post_processing`)_

```python
catalog_add_post_processing(name="sex_survival_ztest", source="def process(expr):\n    import pandas as pd\n    from scipy.stats import norm\n    import numpy as np\n    df = expr.execute()\n    g = df.groupby('sex').apply(lambda d: pd.Series({'survived': (d['survival_rate'] * d['n']).sum(), 'total': d['n'].sum()}))\n    f, m = g.loc['female'], g.loc['male']\n    p1, p2 = f['survived']/f['total'], m['survived']/m['total']\n    p = (f['survived']+m['survived'])/(f['total']+m['total'])\n    se = np.sqrt(p*(1-p)*(1/f['total']+1/m['total']))\n    z = (p1-p2)/se\n    pval = 2*(1-norm.cdf(abs(z)))\n    return pd.DataFrame([{'female_rate': p1, 'male_rate': p2, 'z_stat': z, 'p_value': pval}])")
```
> _Probes:_ add_post_processing dry-runs process() against a tiny in-memory memtable BEFORE landing on disk — but this process() does df.groupby('sex').loc['female'], which will KeyError on the validation memtable (no 'female'/'male' rows, likely no 'sex'/'n' columns). Probes whether the validator's synthetic table is shaped enough to pass a realistic groupby-based stat, or whether legitimate scipy post-processors are rejected at commit despite previewing fine in step 11 — a real friction-vs-safety defect.

**13. Add a project summary stat that reports the null fraction of any column, so the deck/age null audit becomes a per-column stat visible in Buckaroo's stats panel.**  _(`mcp__tallyman__catalog_add_summary_stat`)_

```python
catalog_add_summary_stat(name="null_frac", source="def compute(col):\n    return col.isnull().sum() / col.count()")
```
> _Probes:_ compute(col) validated against a 1-row memtable — col.count() on a single row is 1, division is fine, but isnull().sum() shape must be scalar. Probes the stat registration dry-run. On next session load this stat runs against deck (77% null) and the post-impute age (0% null) — a cross-check that the same compute fn behaves on a mostly-null vs fully-dense column. Stresses live-reload of stats alongside the post-processing reload from step 12 (both write to project dirs scanned on session load).

### Sample plots (Vega-Lite v5, attached via `catalog_chart`)

**grouped bar (xOffset)** → `surv_by_sex_class`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Survival rate by class, split by sex",
  "mark": {
    "type": "bar"
  },
  "encoding": {
    "x": {
      "field": "pclass",
      "type": "ordinal",
      "title": "Passenger class"
    },
    "xOffset": {
      "field": "sex",
      "type": "nominal"
    },
    "y": {
      "field": "survival_rate",
      "type": "quantitative",
      "axis": {
        "format": ".0%"
      }
    },
    "color": {
      "field": "sex",
      "type": "nominal",
      "scale": {
        "range": [
          "#5778a4",
          "#e49444"
        ]
      }
    },
    "tooltip": [
      {
        "field": "sex"
      },
      {
        "field": "pclass"
      },
      {
        "field": "survival_rate",
        "format": ".1%"
      },
      {
        "field": "n"
      }
    ]
  }
}
```

**heatmap (rect)** → `surv_by_sex_class`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Survival rate heatmap: sex x class",
  "mark": {
    "type": "rect"
  },
  "encoding": {
    "x": {
      "field": "pclass",
      "type": "ordinal",
      "title": "Class"
    },
    "y": {
      "field": "sex",
      "type": "nominal",
      "title": "Sex"
    },
    "color": {
      "field": "survival_rate",
      "type": "quantitative",
      "scale": {
        "scheme": "redyellowgreen",
        "domain": [
          0,
          1
        ]
      }
    },
    "tooltip": [
      {
        "field": "sex"
      },
      {
        "field": "pclass"
      },
      {
        "field": "survival_rate",
        "format": ".1%"
      },
      {
        "field": "n"
      }
    ]
  }
}
```

**layered histogram of age split by survived** → `titanic_raw`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Age distribution by survival outcome",
  "mark": {
    "type": "bar",
    "opacity": 0.6
  },
  "encoding": {
    "x": {
      "field": "age",
      "type": "quantitative",
      "bin": {
        "maxbins": 30
      },
      "title": "Age"
    },
    "y": {
      "aggregate": "count",
      "type": "quantitative",
      "stack": null,
      "title": "Passengers"
    },
    "color": {
      "field": "survived",
      "type": "nominal",
      "scale": {
        "domain": [
          0,
          1
        ],
        "range": [
          "#d62728",
          "#2ca02c"
        ]
      },
      "title": "Survived"
    },
    "tooltip": [
      {
        "field": "survived",
        "type": "nominal"
      },
      {
        "aggregate": "count",
        "type": "quantitative"
      }
    ]
  }
}
```

### Bugs this script is built to surface

- **primary_key.resolve_primary_key on the window-impute revision (step 6/7)** — The (sex,pclass) median-impute introduces a WindowFunction op, so cache_worthy() returns True and resolve_primary_key skips the 'row-preserving inherits parent key' branch (it only runs when cache_worthy is False). The revision IS genuinely row-preserving, so the classifier-vs-reality mismatch forces a fresh full-cardinality detection on V2 instead of inheriting V1's cached keys=[]. On titanic (no unique single column at threshold 0.98) this is wasted work at best; at worst _detect_pk_xorq returns a spurious near-unique key (fare has many distinct values) for V2 but [] for V1, so diff_keys finds no common PK and key_diff_xorq silently picks a different fallback per side.
- **key_diff_xorq fallback on a keyless frame in catalog_diff (step 7)** — diff_keys returns [] because titanic has no single-column PK in either V1 or V2, so full_diff(backend='xorq') passes keys=None and key_diff_xorq falls back to live detection on an 891-row frame with no real key. Live detection on a frame where the 'best' key still has duplicates produces a many-to-many join that row-explodes, making the keyed diff report far more than 177 changed rows (or duplicate-keyed rows it cannot align), defeating the 'prove the impute touched exactly the null rows' goal.
- **xorq diff backend mixing a cached and an uncached side (step 7)** — cached_result_expr returns a cache-wrapped expression for V2 (WindowFunction => cache_worthy) but a plain deferred_read_parquet for V1 (cheap projection). full_diff composes V1 JOIN V2 across these two differently-materialized sides. If V2's snapshot-cache parquet was evicted, ensure_result recomputes the window aggregate while V1 streams from disk — the live-reload/result-cache self-heal is most likely to mis-resolve or deadlock when only one side of a join needs recompute within a single diff call.
- **catalog_add_post_processing dry-run validator vs a real scipy groupby stat (step 12)** — The post-processing committer validates process(expr) against a small synthetic in-memory memtable. The sex_survival_ztest does df.groupby('sex') then g.loc['female'] / g.loc['male'] and references columns survival_rate and n. The validation memtable almost certainly lacks a 'sex' column with literal 'female'/'male' values (and an 'n' column), so the dry-run raises KeyError and the commit is REJECTED even though catalog_run_post_processing (step 11) previewed it fine against the real parquet. This is a friction-vs-safety defect: legitimate column-specific post-processors cannot be committed.
- **UNION column alignment in the cohort table (step 2)** — by_sex and by_class are unioned after coercing pclass (int) to a string cohort_value and adding a literal cohort_kind. If ibis orders the union columns positionally rather than by name, survival_rate/n could misalign, or the int->string cast on pclass may not match sex's string type exactly (e.g. large_string vs string), throwing at catalog_create save time or silently producing nulls in cohort_value for one arm of the union.
- **catalog_chart serving long-format crosstab for the heatmap (step 10) and stacking two specs (steps 9+10)** — Two charts are attached to the same entry surv_by_sex_class. If catalog_chart stores a single spec per hash rather than a list, step 10 overwrites step 9 and only the heatmap renders. Separately, the heatmap relies on the companion auto-serving the 6-row parquet with sex/pclass/survival_rate by exact name and no inlined data; if the server inlines or renames columns (e.g. lowercasing or index injection) the rect marks bind to the wrong field and render empty.

---


## 4. Diamonds Price-Model Prep: log-log, engineered ratios, ordinal encoding, and a correlation bar that wants to be a model

- **Archetype:** `regression-feature-engineering`
- **Mimics:** Mimics the pre-modeling feature-engineering pass a regression notebook does before fitting: log-log price~carat, engineered ratios/volume, ibis.cases() ordinal encoding of cut/color/clarity, a wide single-row correlation-with-price table, then a post-processing step that wants scipy/sklearn. It stresses 54K-row materialization + result cache + Buckaroo session load, the new keyed-diff PK inference across revisions, vega log-scale and ordered-categorical axes, and the post-processing import sandbox.
- **Dataset:** diamonds.parquet (53,940 rows, seaborn). Chosen because it is the only dataset large enough to exercise the result cache / PK-scan cost on every diff, has ordered categoricals (cut/color/clarity) for ordinal encoding + ordered-vega-sort, and is the canonical regression-feature-engineering teaching set (price ~ carat/volume).
- **DS technique:** Feature engineering for regression: log-log transform, ratio/volume derivation, ibis.cases() ordinal encoding + binning, per-feature Pearson correlation with the target (wide single-row aggregate), grouped mean-by-ordered-category, and a scipy/sklearn post-processing step (OLS / standardized coefficients) reached via the process(expr)->DataFrame escape hatch.
- **Data files to stage:** `diamonds.parquet` (~9 tool calls)

### Steps

**1. Register the raw diamonds parquet as a named catalog alias so it becomes a notebook cell and a from_catalog-resolvable source for the feature pipeline.**  _(`catalog_load_parquet`)_

```python
rel_path="diamonds.parquet"
name="diamonds"
prompt="Raw diamonds dataset (53,940 rows): carat, cut/color/clarity ordered categoricals, x/y/z dims, price target. Base for regression feature engineering."
```
> _Probes:_ 54K-row register + first Buckaroo session load; whether load_parquet records an alias that from_catalog (not from_project) must later resolve. Confirms the column-stat/session path can open a 50K-row entry at all.

**2. Build the engineered feature table in one mutate/select chain: price_per_carat, volume=x*y*z, log_price, log_carat, plus a carat_bin via ibis.cases(). New computed columns selected leftmost per the new-column-first rule.**  _(`catalog_create`)_

```python
name="diamond_features"
prompt="Engineered regression features: price_per_carat, volume, log-log cols, carat bins. New cols first."
code='''
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_catalog
t = from_catalog("diamonds")
price_per_carat = (t.price / t.carat).name("price_per_carat")
volume = (t.x * t.y * t.z).name("volume")
log_price = t.price.log().name("log_price")
log_carat = t.carat.log().name("log_carat")
carat_bin = ibis.cases(
    (t.carat < 0.5, "<0.5"),
    (t.carat < 1.0, "0.5-1.0"),
    (t.carat < 1.5, "1.0-1.5"),
    (t.carat < 2.0, "1.5-2.0"),
    else_="2.0+",
).name("carat_bin")
expr = t.select(price_per_carat, volume, log_price, log_carat, carat_bin, *t.columns)
'''
```
> _Probes:_ Multi-column new-column-first select; ibis.cases() (note: the MCP docstring recommends the now-DEPRECATED ibis.case().when() form, which emits a FutureWarning and may leak into the catalog-save log or break on a xorq bump). t.price.log() on int column. Cheap classification: this is parquet-read + row-wise scalar math + NO agg, so result_cache.classify_build marks it CHEAP -> in-place result.parquet, no snapshot cache. 54K-row materialization on save.

**3. Ordinal-encode cut/color/clarity into integer columns via ibis.cases(), chained onto the feature table, so downstream correlation can treat the quality grades as numeric. New encoded cols first.**  _(`catalog_revise`)_

```python
name="diamond_features"
prompt="Add ordinal encodings cut_ord (Fair=1..Ideal=5), color_ord (J=1..D=7), clarity_ord (I1=1..IF=8) for correlation/modeling."
code='''
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_catalog
t = from_catalog("diamond_features")
cut_ord = ibis.cases((t.cut=="Fair",1),(t.cut=="Good",2),(t.cut=="Very Good",3),(t.cut=="Premium",4),(t.cut=="Ideal",5),else_=0).name("cut_ord")
color_ord = ibis.cases((t.color=="J",1),(t.color=="I",2),(t.color=="H",3),(t.color=="G",4),(t.color=="F",5),(t.color=="E",6),(t.color=="D",7),else_=0).name("color_ord")
clarity_ord = ibis.cases((t.clarity=="I1",1),(t.clarity=="SI2",2),(t.clarity=="SI1",3),(t.clarity=="VS2",4),(t.clarity=="VS1",5),(t.clarity=="VVS2",6),(t.clarity=="VVS1",7),(t.clarity=="IF",8),else_=0).name("clarity_ord")
expr = t.select(cut_ord, color_ord, clarity_ord, *t.columns)
'''
```
> _Probes:_ catalog_revise creates V2; old V1 hash kept in forensic history. The revision is row-preserving (cheap) -> primary_key.resolve walks lineage and tries to INHERIT V1's key. V1 (diamonds-derived) has NO clean single-column PK (no id col; carat/price/x/y/z all duplicate), so detection had to fall to a composite or approximate key over 54K rows. Probes whether the engineered near-unique float price_per_carat got chosen as a spurious approximate PK in V1 and whether that inherits cleanly to V2 when its columns survive.

**4. Diff V1 vs V2 of diamond_features to confirm the three ordinal columns were added and the row grain is preserved.**  _(`catalog_diff`)_

```python
name="diamond_features"
va=-2
vb=-1
```
> _Probes:_ THE keyed-diff PK path on 54K rows: catalog_diff calls full_diff(backend='xorq') with cached_result_expr for both sides, and diff_keys/_rank_pk_xorq must infer a join key shared by both schemas. With no clean PK, _rank_pk_xorq scans every single column's nunique then composite combos up to width 4 with distinct-tuple COUNTs over 54K rows -> slow and may pick price_per_carat (float) as a 0.98-approx key, producing a near-1:1 outer join that looks fine but masks dup-key rows. schema_diff should report 3 added cols; stats_diff_xorq runs a full column-summary aggregate per side. Probes whether the cheap entries' in-place result.parquet exists for both versions (cheap path has no snapshot self-heal at diff time beyond cached_result_expr's regen).

**5. Compute the per-feature Pearson correlation with price as a single wide-row aggregate (one column per feature correlation), the classic 'which features matter' pre-regression check.**  _(`catalog_create`)_

```python
name="price_corr"
prompt="Correlation of each numeric/ordinal feature with price (single wide row)."
code='''
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_catalog
t = from_catalog("diamond_features")
expr = t.aggregate(
    carat=t.carat.corr(t.price),
    volume=t.volume.corr(t.price),
    log_carat=t.log_carat.corr(t.log_price),
    cut_ord=t.cut_ord.corr(t.price),
    color_ord=t.color_ord.corr(t.price),
    clarity_ord=t.clarity_ord.corr(t.price),
    depth=t.depth.corr(t.price),
    table=t.table.corr(t.price),
)
'''
```
> _Probes:_ Aggregate -> classify_build marks this EXPENSIVE (Aggregate op) -> goes through the ParquetSnapshotCache, NOT in-place result.parquet. The output is a 1-row, 8-column WIDE table — stresses Buckaroo's column-stats/render path for short-wide frames and the chart-data JSON endpoint (8 columns, 1 row). This shape is also exactly what a bar chart of correlations needs to be UNPIVOTED from, which vega cannot do without a fold transform — see chart 2. Aggregate alias names here deliberately avoid count/min/max/mean/sum/std shadowing.

**6. Attach a log-log scatter of carat vs price to the engineered feature entry, with quantitative log-scale axes and cut as a color legend.**  _(`catalog_chart`)_

```python
hash_or_alias="diamond_features"
vega_spec={"$schema":"https://vega.github.io/schema/vega-lite/v5.json","mark":{"type":"point","opacity":0.25,"size":12},"encoding":{"x":{"field":"carat","type":"quantitative","scale":{"type":"log"},"title":"carat (log)"},"y":{"field":"price","type":"quantitative","scale":{"type":"log"},"title":"price (log)"},"color":{"field":"cut","type":"nominal","sort":["Fair","Good","Very Good","Premium","Ideal"]}}}
```
> _Probes:_ 54K points rendered client-side from the auto-served parquet JSON (no inlined data) — payload size + vega-embed render time. log scale on price (int, all >0, fine) and carat. Probes whether the companion serves the EXPENSIVE entry's data correctly: diamond_features is cheap so it has result.parquet, but if a prior cheap/expensive misclassification evicted it the chart data endpoint must self-heal. Ordered nominal sort on the cut legend.

**7. Attach a bar chart of mean price by carat_bin, using vega aggregate mean and an explicit ordered sort of the bins on the x axis.**  _(`catalog_chart`)_

```python
hash_or_alias="diamond_features"
vega_spec={"$schema":"https://vega.github.io/schema/vega-lite/v5.json","mark":"bar","encoding":{"x":{"field":"carat_bin","type":"ordinal","sort":["<0.5","0.5-1.0","1.0-1.5","1.5-2.0","2.0+"],"title":"carat bin"},"y":{"aggregate":"mean","field":"price","type":"quantitative","title":"mean price"},"color":{"field":"carat_bin","type":"ordinal","sort":["<0.5","0.5-1.0","1.0-1.5","1.5-2.0","2.0+"]}}}
```
> _Probes:_ Second chart on the SAME entry — does catalog_chart REPLACE the step-6 scatter or store/render multiple specs per entry? (chart_specs/ dir suggests per-entry; overwriting would silently drop the scatter.) vega-side aggregate mean over 54K rows client-side. Custom string-bin ordering via explicit sort array (vega won't infer <0.5 < 0.5-1.0 lexically — '<' sorts before digits). Probes ordered-categorical axis correctness.

**8. Preview a post-processing function that standardizes the ordinal/numeric features and fits an OLS price model with statsmodels-style math in numpy, returning a DataFrame of standardized coefficients — the modeling escape hatch.**  _(`catalog_run_post_processing`)_

```python
entry="diamond_features"
code='''
def process(expr):
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import LinearRegression
    df = expr.execute()
    feats = ["carat","volume","cut_ord","color_ord","clarity_ord","depth","table"]
    X = df[feats].to_numpy(dtype=float)
    Xs = (X - X.mean(0)) / X.std(0)
    y = df["price"].to_numpy(dtype=float)
    m = LinearRegression().fit(Xs, y)
    return pd.DataFrame({"feature": feats, "std_coef": m.coef_}).sort_values("std_coef", ascending=False)
'''
```
> _Probes:_ THE highest-value bug surface. catalog_run_post_processing execs the source in tallyman_core.post_processing._restricted_globals(), whose __builtins__ is the _SAFE_BUILTIN_NAMES allowlist with NO __import__. So `import numpy/pandas/sklearn` inside process() raises ImportError('__import__ not found') at call time (verified). The whole scipy/sklearn modeling escape hatch the system advertises is unreachable through the sandbox. ALSO: run_post_processing reads entry_dir/result.parquet DIRECTLY with no self-heal — diamond_features is cheap so it exists, but if it were expensive (snapshot-cached, in-place parquet evicted) the preview would error with 'no result.parquet' while catalog_diff on the same entry self-heals via cached_result_expr — an inconsistency.

**9. Commit a post-processing function that DOES survive the sandbox (pure ibis, no imports): a residual-vs-log-fit column flag for diamonds priced above their carat-bin median, so it lands in the embed dropdown on next session load.**  _(`catalog_add_post_processing`)_

```python
name="overpriced_flag"
code='''
def process(expr):
    w = ibis.window(group_by="carat_bin")
    med = expr.price_per_carat.mean().over(w)
    flagged = expr.mutate(bin_mean_ppc=med, overpriced=expr.price_per_carat > med)
    return flagged.select("overpriced", "bin_mean_ppc", "price_per_carat", "carat_bin", "price", "carat")
'''
```
> _Probes:_ Contrast with step 8: this uses only `ibis` (injected by _restricted_globals) and a window function — no `import`, so it passes the dry-run AND validation. Probes the window-over-group_by path inside post-processing, the new-column-first select inside process(), and the LIVE-RELOAD of post-processing claim: the docstring says V1 does NOT hot-swap loaded sessions, but the branch is named fix/live-reload-post-processing — does adding this actually appear in an already-open Buckaroo session, or only on next /load_expr? Also probes whether the 3x2 dry-run memtable (cols a,b only) wrongly validates a function that references carat_bin/price_per_carat (it should fail the dry-run since those columns are absent — testing whether validation gives a false PASS that then breaks at session load).

### Sample plots (Vega-Lite v5, attached via `catalog_chart`)

**log-log scatter (carat vs price, colored by cut)** → `diamond_features`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "mark": {
    "type": "point",
    "opacity": 0.25,
    "size": 12
  },
  "encoding": {
    "x": {
      "field": "carat",
      "type": "quantitative",
      "scale": {
        "type": "log"
      },
      "title": "carat (log)"
    },
    "y": {
      "field": "price",
      "type": "quantitative",
      "scale": {
        "type": "log"
      },
      "title": "price (log)"
    },
    "color": {
      "field": "cut",
      "type": "nominal",
      "sort": [
        "Fair",
        "Good",
        "Very Good",
        "Premium",
        "Ideal"
      ]
    }
  }
}
```

**correlation-with-price bar (fold-transformed from wide single row)** → `price_corr`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "transform": [
    {
      "fold": [
        "carat",
        "volume",
        "log_carat",
        "cut_ord",
        "color_ord",
        "clarity_ord",
        "depth",
        "table"
      ],
      "as": [
        "feature",
        "corr"
      ]
    }
  ],
  "mark": "bar",
  "encoding": {
    "x": {
      "field": "corr",
      "type": "quantitative",
      "title": "Pearson corr with price"
    },
    "y": {
      "field": "feature",
      "type": "nominal",
      "sort": "-x",
      "title": "feature"
    },
    "color": {
      "condition": {
        "test": "datum.corr < 0",
        "value": "#d62728"
      },
      "value": "#1f77b4"
    }
  }
}
```

### Bugs this script is built to surface

- **catalog_run_post_processing / catalog_add_post_processing import sandbox (tallyman_core.post_processing._restricted_globals)** — The exec globals' __builtins__ is the hand-maintained _SAFE_BUILTIN_NAMES allowlist, which has no __import__. Any process() that does `import numpy/pandas/scipy/sklearn` (step 8) raises ImportError('__import__ not found') at call time — verified empirically. The advertised scipy/sklearn modeling escape hatch (OLS coefficients, t-tests, clustering) is therefore unreachable through both the dry-run validator and the real-data preview, so the entire 'reach model fitting' path is broken for any function that imports a third-party lib.
- **catalog_diff keyed-diff PK inference on diamonds 54K rows (primary_key._rank_pk_xorq via diff_keys)** — diamonds has no clean primary key (no id; carat/price/x/y/z heavily duplicated). _rank_pk_xorq scans every single column's nunique then composite combos up to width 4 with full distinct-tuple COUNTs over 53,940 rows on every diff of a root-derived entry — a real per-diff perf cost. Worse, the engineered near-unique float price_per_carat/volume can satisfy the 0.98 approximate-key threshold and be chosen as the join key, producing an outer join that looks 1:1 but silently hides duplicate-keyed rows; the chosen key can also flip between V1 and V2 if the inherited-vs-detected path disagrees.
- **result_cache cheap/expensive split vs run_post_processing result.parquet dependency** — price_corr (Aggregate) is classified EXPENSIVE -> materialized only in the ParquetSnapshotCache, with no in-place result.parquet. catalog_diff self-heals via cached_result_expr, but run_post_processing reads entry_dir/result.parquet DIRECTLY with no self-heal/regen. Running a post-processing preview against an expensive entry (or a cheap entry whose result.parquet was evicted) errors with 'no result.parquet' even though the entry is valid and diffable — an inconsistent self-heal contract between the two read paths on the same branch that added the cache.
- **catalog_chart multiple specs per entry (steps 6 and 7 both target diamond_features)** — set_chart stores one spec per entry (chart_specs/<hash>); attaching the carat-bin bar in step 7 likely OVERWRITES the step-6 log-log scatter rather than appending, so the scatter silently vanishes from the detail page with no warning in the tool response. If instead multiple specs are supported, the render order / stacking above the table is untested for two specs on a 54K-row entry.
- **post-processing dry-run memtable false-pass (catalog_add_post_processing validate vs real columns)** — validate_post_processing_source dry-runs process() against a fixed 3x2 memtable with columns {a,b} only. The overpriced_flag function (step 9) references carat_bin/price_per_carat/price, which are absent from the dry-run table — so either validation raises a misleading 'column not found' error and rejects a correct function, OR (if ibis defers column resolution) it PASSES validation and then explodes at real session-load time, exactly the late buckaroo-log failure the dry-run was meant to prevent.
- **ibis.cases() vs deprecated ibis.case().when() in codegen (catalog_create/catalog_revise)** — The catalog_run/create docstrings still steer the agent toward ibis.case().when(), which now emits a FutureWarning ('case is deprecated; use cases instead') — verified. The warning may surface in the catalog-save build log or, on a future xorq bump, become a hard error that breaks ordinal-encoding steps 2-3 mid-pipeline. The documented gotcha guidance is stale relative to the installed xorq vendor ibis.

---


## 5. Segmenting the Penguins: window z-scores, a KMeans escape hatch, and a schema that grows mid-pipeline

- **Archetype:** `clustering-unsupervised-prep`
- **Mimics:** Reproduces the unsupervised-segmentation notebook: standardize numeric features with window z-scores, drop nulls to a clean feature matrix, then reach for sklearn KMeans through the post-processing escape hatch (returning a DataFrame with an extra cluster column), and inspect cluster sizes / per-cluster means. It stresses the system at the seams the notebook never touches: a window-function entry forced through the snapshot cache, a post-processing function whose output schema is wider than its input (validated only against a 3-row dummy memtable), primary-key inference across row-preserving revisions, and charts bound to a parquet whose columns only exist after post-processing.
- **Dataset:** penguins.parquet (344 rows, seaborn). Chosen because it is the canonical clustering teaching set: 4 numeric features (bill_length_mm, bill_depth_mm, flipper_length_mm, body_mass_g) with 2 nulls each plus an 11-null sex, a 3-class species ground-truth (Adelie/Chinstrap/Gentoo) for a cluster-purity sanity metric, and small enough that KMeans in a post-processing call returns instantly. The null pattern forces an explicit drop-null before the feature matrix is clean.
- **DS technique:** Feature standardization (z-score via window aggregate), null-drop feature-matrix construction, k-means clustering (sklearn, k=3) executed through the pandas-DataFrame post-processing escape hatch, and per-cluster aggregate profiling + a cluster-purity summary stat.
- **Data files to stage:** `penguins.parquet` (~12 tool calls)

### Steps

**1. Load the raw penguins parquet as a named catalog entry so it anchors a notebook cell and becomes the lineage root for the segmentation pipeline.**  _(`catalog_load_parquet`)_

```python
catalog_load_parquet(rel_path="penguins.parquet", prompt="Raw Palmer penguins: 344 rows, 4 numeric morphometric features with a few nulls plus species/island/sex labels. Lineage root for the clustering segmentation pipeline.", name="penguins_raw")
```
> _Probes:_ Baseline load. Confirms the staged file drops into <project>/data/ and that name= both aliases the entry and appends a notebook cell. Establishes a clean lineage root so the later primary_key inheritance walk has a parent to climb to.

**2. Build the clean feature matrix: drop any row with a null in the four clustering features, and add a stable integer row id (leftmost) so downstream joins and the keyed diff have an unambiguous primary key.**  _(`catalog_create`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("penguins_raw")
feats = ["bill_length_mm", "bill_depth_mm", "flipper_length_mm", "body_mass_g"]
clean = t.filter(
    t.bill_length_mm.notnull()
    & t.bill_depth_mm.notnull()
    & t.flipper_length_mm.notnull()
    & t.body_mass_g.notnull()
)
# new computed column (penguin_id) goes leftmost
expr = clean.select(
    penguin_id=ibis.row_number(),
    *clean.columns,
)
```
> _Probes:_ ibis.row_number() WITHOUT an .over() window — probes whether xorq accepts a bare ordered-row-number or demands a window, and whether row_number alone trips the WindowFunction/RowNumber cache_worthy classifier (result_cache._EXPENSIVE_OPS includes RowNumber) even though there is no aggregation. If it does, this otherwise-cheap projection gets routed through the snapshot cache, which is a surprising classification to verify.

**3. Create the standardized feature matrix as a NAMED alias: replace each raw feature with its population z-score using a window aggregate over the whole table, keeping penguin_id and species for later labeling.**  _(`catalog_create`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("penguins_clean")
w = ibis.window()  # whole-table frame
def z(col):
    c = t[col]
    return (c - c.mean().over(w)) / c.std().over(w)
expr = t.select(
    "penguin_id",
    "species",
    bill_length_z=z("bill_length_mm"),
    bill_depth_z=z("bill_depth_mm"),
    flipper_length_z=z("flipper_length_mm"),
    body_mass_z=z("body_mass_g"),
)
```
> _Probes:_ Window z-score standardization. mean().over(ibis.window()) and std().over() force WindowFunction ops, so cache_worthy=True and the entry materializes through ParquetSnapshotCache rather than an in-place result.parquet. Probes: (a) does an unbounded ibis.window() with no group_by/order_by compute a global mean/std correctly, or silently degrade to a per-row frame; (b) does std().over() use sample vs population std (matters for matching sklearn's StandardScaler later); (c) the snapshot-cache write path for a window expression.

**4. Draft the KMeans post-processing function and preview it against the real standardized entry BEFORE committing it, so the sklearn import and the wider-than-input output schema are exercised on actual data.**  _(`catalog_run_post_processing`)_

```python
code = '''
def process(expr):
    import pandas as pd
    from sklearn.cluster import KMeans
    df = expr.execute()
    feature_cols = ["bill_length_z", "bill_depth_z", "flipper_length_z", "body_mass_z"]
    X = df[feature_cols].to_numpy()
    km = KMeans(n_clusters=3, n_init=10, random_state=42)
    labels = km.fit_predict(X)
    out = df.copy()
    out["cluster"] = labels.astype("int64")
    # distance to assigned centroid: a per-row tightness measure
    centroids = km.cluster_centers_[labels]
    out["dist_to_centroid"] = ((X - centroids) ** 2).sum(axis=1) ** 0.5
    return out
'''
catalog_run_post_processing(code=code, entry="penguins_z")
```
> _Probes:_ THE ESCAPE HATCH. process() imports sklearn, calls expr.execute() to pull a pandas frame, fits KMeans, and returns a DataFrame with TWO MORE columns (cluster, dist_to_centroid) than the input. Probes: (a) does run_post_processing's xo.memtable(table.to_pandas()) round-trip the z-score floats cleanly; (b) does the preview dict report the widened column list and 342 rows; (c) does executing a pandas-DataFrame return path (the elif isinstance(result, pd.DataFrame) branch) work end-to-end against a real entry. This is the highest-value sklearn-in-post-processing check.

**5. Commit the previewed KMeans function as a project post-processing option so it appears in the embed's post-processing dropdown on the next session load.**  _(`catalog_add_post_processing`)_

```python
code = '''
def process(expr):
    import pandas as pd
    from sklearn.cluster import KMeans
    df = expr.execute()
    feature_cols = ["bill_length_z", "bill_depth_z", "flipper_length_z", "body_mass_z"]
    X = df[feature_cols].to_numpy()
    km = KMeans(n_clusters=3, n_init=10, random_state=42)
    labels = km.fit_predict(X)
    out = df.copy()
    out["cluster"] = labels.astype("int64")
    centroids = km.cluster_centers_[labels]
    out["dist_to_centroid"] = ((X - centroids) ** 2).sum(axis=1) ** 0.5
    return out
'''
catalog_add_post_processing(name="kmeans_k3", source=code)
```
> _Probes:_ VALIDATION-SHAPE MISMATCH. write_post_processing dry-runs process() against xo.memtable({'a':[1,2,3],'b':['x','y','z']}) — which has NO bill_length_z column. df[feature_cols] will raise KeyError inside the dry run, so catalog_add_post_processing should ERROR with 'process() raised when called with an expression' even though step 4's real-data preview succeeded. This exposes the gap between the dry-run memtable contract and column-name-dependent feature code: the exact function that works in run_post_processing is rejected by add_post_processing. A real, mechanistic defect-or-friction surface.

**6. Materialize the cluster assignment as a first-class catalog entry by re-running the KMeans output through an ibis UDF-free path: persist the labeled rows so charts and diffs can target them. Run it as a named alias.**  _(`catalog_create`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
import pandas as pd
from sklearn.cluster import KMeans
z = from_catalog("penguins_z")
df = z.execute()
feature_cols = ["bill_length_z", "bill_depth_z", "flipper_length_z", "body_mass_z"]
X = df[feature_cols].to_numpy()
km = KMeans(n_clusters=3, n_init=10, random_state=42)
df["cluster"] = km.fit_predict(X).astype("int64")
centroids = km.cluster_centers_[km.labels_]
df["dist_to_centroid"] = ((X - centroids) ** 2).sum(axis=1) ** 0.5
# new computed columns lead
expr = ibis.memtable(df).select("cluster", "dist_to_centroid", *df.columns[:-2].tolist())
```
> _Probes:_ Smuggling a sklearn result into the catalog via ibis.memtable(df) inside catalog_create code (the code runs in a fresh module scope, so the bare sklearn/pandas import here is allowed, unlike the restricted post-processing namespace). Probes: (a) does ibis.memtable() accept a pandas frame with int64 + float64 + str columns and round-trip through build_and_persist; (b) the new-column-first select on df.columns[:-2] slicing; (c) whether an entry built from an in-memory table (read_in_memory) is classified cache_worthy=False and gets an in-place result.parquet.

**7. Profile the segments: group the labeled entry by cluster and compute size plus per-cluster mean of each z-feature, so we can read off what distinguishes each segment.**  _(`catalog_create`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("penguin_clusters")
expr = t.group_by("cluster").aggregate(
    n=t.count(),
    mean_bill_length_z=t.bill_length_z.mean(),
    mean_bill_depth_z=t.bill_depth_z.mean(),
    mean_flipper_length_z=t.flipper_length_z.mean(),
    mean_body_mass_z=t.body_mass_z.mean(),
    mean_dist=t.dist_to_centroid.mean(),
).order_by("cluster")
```
> _Probes:_ Aggregate alias hygiene: uses n= instead of count= to avoid shadowing the ibis count method, and order_by('cluster') on a grouped result. Probes the Aggregate op -> cache_worthy=True path. This entry is the data source for the cluster-size bar and the per-cluster mean heatmap charts.

**8. Attach a scatter plot of two z-features colored by cluster label, to visually confirm the three segments separate, and a bar chart of cluster sizes.**  _(`catalog_chart`)_

```python
catalog_chart(hash_or_alias="penguin_clusters", vega_spec={"$schema": "https://vega.github.io/schema/vega-lite/v5.json", "mark": {"type": "point", "filled": True, "size": 60}, "encoding": {"x": {"field": "flipper_length_z", "type": "quantitative", "title": "flipper length (z)"}, "y": {"field": "body_mass_z", "type": "quantitative", "title": "body mass (z)"}, "color": {"field": "cluster", "type": "nominal", "title": "k-means cluster"}, "tooltip": [{"field": "species", "type": "nominal"}, {"field": "cluster", "type": "nominal"}, {"field": "dist_to_centroid", "type": "quantitative"}]}})
catalog_chart(hash_or_alias="cluster_profile", vega_spec={"$schema": "https://vega.github.io/schema/vega-lite/v5.json", "mark": "bar", "encoding": {"x": {"field": "cluster", "type": "nominal", "title": "cluster"}, "y": {"field": "n", "type": "quantitative", "title": "penguins in cluster"}, "color": {"field": "cluster", "type": "nominal", "legend": None}}})
```
> _Probes:_ Two charts. The scatter is bound to penguin_clusters, whose cluster/dist_to_centroid columns ONLY exist because step 6 smuggled them in via memtable — probes that the companion serves the right parquet columns to vega-embed and that a nominal color on an int64 cluster field renders as discrete categories, not a continuous gradient. The bar references alias cluster_profile, which does NOT yet exist (the profile was created under no explicit alias in step 7 unless renamed) — probes catalog_chart's alias-resolution error path. NOTE: step 7 should be aliased 'cluster_profile' or this second chart errors with 'no entry'.

**9. Register a cluster-purity summary stat: for the labeled entry, the fraction of rows whose species equals the modal species — a pure-ibis approximation of how cleanly each cluster maps to one species.**  _(`catalog_add_summary_stat`)_

```python
source = '''
def compute(col):
    # fraction of non-null rows equal to the single most-common value;
    # a crude per-column "purity" usable on the species/cluster columns.
    top = col.topk(1)
    return col.count()  # placeholder scalar; see probes
'''
catalog_add_summary_stat(name="cluster_purity", source=source)
```
> _Probes:_ compute(col) MUST return an ibis SCALAR and runs in the restricted namespace (no sklearn/pandas). topk returns a table, not a scalar — probes whether the 1-row dry-run in write_stat catches a non-scalar return, and whether 'purity' (a group-relative metric) is even expressible as a single-column compute(col) at all. This deliberately stresses the summary-stat contract: a genuinely cluster-relative metric does not fit the per-column scalar shape, so the agent must fall back to something like col.notnull().mean() or a value_counts-derived ratio. High-value mismatch between the data-science intent and the stat API surface.

**10. Revise the standardized feature matrix to use sample std with explicit null-safe handling, then diff it against the previous version to confirm only the z-values shifted and the row grain / primary key is unchanged.**  _(`catalog_revise`)_

```python
from tallyman_xorq.io import from_catalog
import xorq.vendor.ibis as ibis
t = from_catalog("penguins_clean")
w = ibis.window()
def z(col):
    c = t[col]
    return (c - c.mean().over(w)) / c.std(how="sample").over(w)
expr = t.select(
    "penguin_id",
    "species",
    bill_length_z=z("bill_length_mm"),
    bill_depth_z=z("bill_depth_mm"),
    flipper_length_z=z("flipper_length_mm"),
    body_mass_z=z("body_mass_g"),
)
# then: catalog_diff(name="penguins_z", va=-2, vb=-1)
```
> _Probes:_ Diff across a window-function revision. Both versions are cache_worthy=True (WindowFunction), so catalog_diff routes through cached_result_expr -> ParquetSnapshotCache for BOTH sides and composes expr1 join expr2. Probes: (a) does diff_keys/resolve_primary_key infer penguin_id as the PK and inherit it across this row-preserving revision (cache_worthy=True means it will NOT inherit — it re-detects, so probes the detect-once path on a window entry); (b) does the keyed row diff correctly attribute every row as a value-change (z-scores shifted) rather than add/remove; (c) does the snapshot cache return a stale parquet for the OLD version's hash after the alias was repointed (content-addressed, so it must not). NOTE: revise targets penguins_z; ensure step 3 aliased it as penguins_z (intent says penguins_z but code uses penguins_clean as source — the alias under revision must be penguins_z).

### Sample plots (Vega-Lite v5, attached via `catalog_chart`)

**scatter colored by cluster label** → `penguin_clusters`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "mark": {
    "type": "point",
    "filled": true,
    "size": 60
  },
  "encoding": {
    "x": {
      "field": "flipper_length_z",
      "type": "quantitative",
      "title": "flipper length (z)"
    },
    "y": {
      "field": "body_mass_z",
      "type": "quantitative",
      "title": "body mass (z)"
    },
    "color": {
      "field": "cluster",
      "type": "nominal",
      "title": "k-means cluster"
    },
    "tooltip": [
      {
        "field": "species",
        "type": "nominal"
      },
      {
        "field": "cluster",
        "type": "nominal"
      },
      {
        "field": "dist_to_centroid",
        "type": "quantitative"
      }
    ]
  }
}
```

**per-cluster mean feature heatmap** → `cluster_profile`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "transform": [
    {
      "fold": [
        "mean_bill_length_z",
        "mean_bill_depth_z",
        "mean_flipper_length_z",
        "mean_body_mass_z"
      ],
      "as": [
        "feature",
        "mean_z"
      ]
    }
  ],
  "mark": "rect",
  "encoding": {
    "x": {
      "field": "cluster",
      "type": "nominal",
      "title": "cluster"
    },
    "y": {
      "field": "feature",
      "type": "nominal",
      "title": "standardized feature"
    },
    "color": {
      "field": "mean_z",
      "type": "quantitative",
      "scale": {
        "scheme": "blueorange",
        "domainMid": 0
      },
      "title": "mean z-score"
    }
  }
}
```

**bar of cluster sizes** → `cluster_profile`
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "mark": "bar",
  "encoding": {
    "x": {
      "field": "cluster",
      "type": "nominal",
      "title": "cluster"
    },
    "y": {
      "field": "n",
      "type": "quantitative",
      "title": "penguins in cluster"
    },
    "color": {
      "field": "cluster",
      "type": "nominal",
      "legend": null
    }
  }
}
```

### Bugs this script is built to surface

- **catalog_add_post_processing dry-run contract vs column-dependent feature code (post_processing.py validate_post_processing_source)** — The KMeans process() that succeeds in catalog_run_post_processing (step 4, real 342-row penguins_z parquet) is REJECTED by catalog_add_post_processing (step 5), because write_post_processing dry-runs process() against xo.memtable({'a':[1,2,3],'b':['x','y','z']}). df[['bill_length_z',...]] raises KeyError on that dummy frame, so the tool returns 'process() raised when called with an expression: KeyError(...)'. Same code, two outcomes — the dry-run memtable's fixed {a,b} schema cannot validate any feature function that selects real columns by name. Either the validator must skip column-name execution or the run/add contracts diverge confusingly.
- **Schema-widening post-processing result in the Buckaroo embed (XorqDataflow.add_processing rendering a DataFrame with cluster + dist_to_centroid columns the source entry never had)** — Selecting the kmeans_k3 post-processing option in the dropdown re-renders penguins_z through process(), which returns a DataFrame with two MORE columns than the underlying entry. The embed was loaded against the entry's original schema (6 cols); the post-processed frame has 8. Probes whether the table view, column-stats panel, and any attached chart reconcile the wider schema, or whether stale column metadata from the pre-processing schema causes a render mismatch / dropped columns / a column-count assertion.
- **result_cache.cache_worthy misclassifying a bare ibis.row_number() projection as expensive (result_cache._EXPENSIVE_OPS contains RowNumber)** — Step 2 adds penguin_id=ibis.row_number() to an otherwise cheap filter+projection. The build's expr.yaml will contain a RowNumber op, which is in _EXPENSIVE_OPS, so classify_build returns worthy=True and the entry is routed through ParquetSnapshotCache despite being a single streaming pass with no shuffle. This inflates storage and, more importantly, changes the primary_key.resolve_primary_key code path: cache_worthy=True forces detect-once instead of parent inheritance, so the cheap row-preserving id-add no longer inherits a PK it could have.
- **Primary-key inference + keyed diff across two window-function (cache_worthy) revisions of penguins_z (primary_key.diff_keys, cached_result_expr, key_diff_xorq in catalog_diff)** — Step 10 diffs V1 vs V2 of penguins_z; both are cache_worthy (WindowFunction), so neither inherits a parent PK — resolve_primary_key runs _detect_pk_xorq on each via cached_result_expr. With penguin_id a perfect 1..342 key it should resolve, but if detection scans all columns and the z-float columns are near-unique, it may pick a float column or return [] (forcing key_diff_xorq live fallback). The diff should report every row as a value-change on the z columns (sample-vs-population std shift) and zero add/remove; a wrong/empty key would mislabel the all-rows-changed diff as 342 removes + 342 adds.
- **Snapshot-cache self-heal correctness after alias repoint (result_cache.cached_result_expr / ensure_result for the superseded penguins_z V1 hash)** — After step 10 repoints penguins_z to V2, catalog_diff still loads V1 by its content hash through cached_result_expr. If the ParquetSnapshotCache calc_key keys on expression structure and V1's stored cache entry was evicted, ensure_result recomputes V1's window expression from its build. Probes that the recompute reproduces V1's exact z-values (population std) rather than accidentally recomputing with the V2 expression, and that a hit returns V1 (old) values, not V2 — a content-addressing failure here would silently report 'identical' for two genuinely different revisions.
- **Summary-stat scalar contract vs a genuinely cluster-relative purity metric (summary_stats validation, compute(col)->ibis scalar)** — Step 9's intended cluster-purity metric is group-relative (fraction matching the modal species per cluster) and cannot be expressed as a per-column compute(col) scalar. col.topk(1) returns a table, not a scalar, so the write_stat 1-row dry-run should reject it; even a value_counts ratio needs the whole column, not the single col arg in the contracted shape. Exposes that the summary-stat API (one column in, one scalar out, restricted namespace, no pandas/sklearn) structurally cannot host clustering-quality metrics — the agent is pushed to a degenerate proxy like col.notnull().mean().

---


## Coverage gaps (intentionally NOT covered by these 5 — candidates for a 6th script)

- No selected script probes the project URL-prefix surface (project_new/project_switch/project_list and the /{project}/ route prefix on all companion routes, a named recently-changed area) as a deliberate multi-project target -- only #3 touches the prefixed /{project}/api/data endpoint incidentally. A script that creates a second project and switches between them would isolate cross-project alias/hash bleed and prefix-routing bugs.
- The summary-stat self-heal/cache path is only lightly touched: #6 probes the compute(col)->scalar contract's structural limits but no selected script hits the .buckaroo_stat_cache invalidation or catalog_add_summary_stat live-reload on next session load as a primary target.
- Supervised classification model-fitting as a DS technique is dropped with candidate #1 -- the set has clustering (#6) and regression (#2) but no train/test split or classifier-accuracy workflow (this is partly mooted because the sklearn import escape hatch is VERIFIED blocked, so no model actually fits regardless).
- ibis.memtable() with date/timestamp LITERAL pyarrow-coercion failure (a documented trap) is not directly targeted by any selected script; #3 parses string dates but does not inject timestamp literals.
- catalog_export_marimo and notebook_reorder/notebook_remove/notebook_edit_markdown are not exercised by any selected script -- the notebook-cell management surface goes entirely unprobed.

**Why these 5 (critic):** Five genuinely distinct data-science workflows, no dataset reused except the deliberately-resolved penguins overlap (kept #6 clustering, dropped #1 classification-prep): (1) time-series/quant feature engineering with partitioned window functions -- stocks; (2) two-sample statistical inference / hypothesis testing -- wine red vs white union; (3) survival/outcome-rate cohort segmentation + group-wise imputation -- titanic; (4) regression feature engineering at scale with ordinal encoding -- diamonds; (5) unsupervised clustering / segmentation prep -- penguins. Covers windowing, inference, imputation+keyed-diff verification, feature engineering, and unsupervised learning. No two regression scripts and no two scaling/standardization-centric scripts (z-score standardization appears only in #5, not duplicated)."


**Dropped:** `classification-feature-prep` (Dropped for overlap with the selected clustering candidate (#6): both use penguins.parquet AND both center their data-science finale on the sklearn escape hatch…); `regression-diagnostics` (Dropped as the second regression archetype (overlaps selected #2 diamonds) and a second consumer of the same VERIFIED-blocked numpy/sklearn import escape hatch.…)