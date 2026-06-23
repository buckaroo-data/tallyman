# EDA codegen run 1 — results

**Date:** 2026-05-29
**Dataset:** citibike_large (2.5M rows, Sep 2020), artifact hash `29a208d050d1`
**Parquet path used:** `/Users/paddy/.tallyman-notebooks/projects/test-1/artifacts/catalog/entries/29a208d050d1/result.parquet`

---

## Test prompts — all 15 passed

| # | Prompt | Result |
|---|---|---|
| 1 | count the number of null values in each column | pass — 1 wide row, all cols as int64 null counts |
| 2 | group rides by start_station_name and count how many rides started from each, show top 20 | pass — 20 rows |
| 3 | what's the average trip_duration by rideable_type? | pass — 2 rows |
| 4 | add a column for the hour of day from started_at, then group by hour and count rides | pass — 24 rows |
| 5 | what share of rides have a null end_station_name? | pass — 1 row, double |
| 6 | compute the mean and standard deviation of trip_duration | pass — 1 row |
| 7 | for each rideable_type, show min, max, mean, and median trip_duration | pass — used `approx_median()` |
| 8 | filter to rides longer than 60 minutes and count how many there are per rideable_type | pass — 2 rows |
| 9 | group by member_casual and rideable_type, give me count and average duration | pass — 4 rows |
| 10 | flag rides as 'long' if trip_duration > 3600 and 'short' otherwise | pass — 2.5M row mutate with `ibis.case().when().else_().end()` |
| 11 | fill missing end_station_name with 'unknown' | pass — `fillna()` works |
| 12 | for each start station, compute average trip duration — then filter to only stations with more than 100 rides | pass — 1063 rows; chaining `.filter()` on an aggregated table works (ibis HAVING equivalent) |
| 13 | compute the correlation between trip_duration and hour of day | pass — `mutate(hour=).aggregate(r=a.corr(b))` works |
| 14 | show the median trip duration for each hour of the day | pass — 24 rows |
| 15 | fill missing end_station_name using the most common station name for that rideable_type | pass — window `row_number()` + left_join + `fillna()` |

---

## Infrastructure failure (not in the plan)

`read_project_file("citibike_large")` does not resolve catalog aliases. It looks for a raw data file at `.../data/citibike_large`. To read a prior catalog entry from new code you must hardcode the artifact parquet path. There is no clean "read current version of alias X" API for use inside expressions.

---

## Predicted failure modes — verification

| # | Predicted | Actual |
|---|---|---|
| 1 | `.describe()` on ibis column | CONFIRMED — `AttributeError: 'IntegerColumn' object has no attribute 'describe'` |
| 2 | `.dt.hour` pandas accessor | CONFIRMED — `AttributeError: 'TimestampColumn' object has no attribute 'dt'` |
| 3 | `import ibis` directly | CONFIRMED — but failure is silent until catalog save time. Expression constructs fine; xorq's `ExprDumper` then rejects it with a wall-of-text `TypeError` about ibis vs xorq.vendor.ibis type mismatch. No actionable hint for an LLM. |
| 4 | `isnull().mean()` fails | WRONG — works, returns correct double percentage |
| 5 | `median()` fails, must use `approx_median()` | WRONG — both work |
| 6 | `ibis._` wildcard in `.select()` | CONFIRMED — `AttributeError: 'Table' object has no attribute 'name'` with unhelpful "Did you mean: 'rename'?" |

---

## Observations not in the plan

**Null count output format.** Prompt 1 returns a single wide row (1 × 14, all int64). Correct but awkward to display. A long-format `(column_name, null_count)` table requires a Python-level loop to build a union — not expressible as a single ibis expression. An LLM will likely need to post-process in Python to pivot the result.

**Prompt 4 subtle correctness risk.** After `t.mutate(hour=...)`, the aggregate referenced `t.ride_id.count()` using the pre-mutated table binding. It works here because `ride_id` is still present after mutate, but the mutated table is a different expression object. An LLM that doesn't capture the mutated table to a variable and uses the original `t` in downstream aggregates is silently correct in most cases but could fail if the aggregation referenced the new column.

**`import ibis` failure is the nastiest.** The error (`'expr' must be <class 'xorq.vendor.ibis.expr.types.core.Expr'>`) gives no hint about the cause. A clearer message — "Did you use `import ibis` instead of `import xorq.vendor.ibis as ibis`?" — would save significant debugging time.
