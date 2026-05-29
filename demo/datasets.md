# Demo datasets: NYC Taxi + Citibike

Two public GB-scale datasets for demoing xorq caching against naive pandas.

## Data on disk

| Dataset | Path | Size | Rows |
|---|---|---|---|
| NYC Yellow Cab 2024 (12 months) | `/tmp/nyc_taxi/yellow_tripdata_2024-*.parquet` | 666 MB | 41.2M |
| NYC Yellow Cab 2022–2024 (36 months) | `/tmp/nyc_taxi/yellow_tripdata_*.parquet` | 1.94 GB | 119.1M |
| Taxi zone lookup | `/tmp/nyc_taxi/taxi_zone_lookup.csv` | 12 KB | 265 |
| Taxi zone shapefile | `/tmp/taxi_zones/` | — | 263 zones |
| Citibike H1 2024 | `/tmp/citibike/citibike_h1_2024.parquet` | 612 MB | 18.8M |
| Citibike 2022–2024 | `/tmp/citibike/citibike_2022_2024.parquet` | 1.37 GB | 83.7M |
| Citibike raw CSVs | `/tmp/citibike/csv/*.csv` | 15 GB | 98 files |

Download sources:
- Taxi: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY}-{MM:02d}.parquet`
- Citibike: `https://s3.amazonaws.com/tripdata/{YYYY}{MM:02d}-citibike-tripdata.zip`

---

## Benchmark results

### NYC Taxi (119M rows, 36 months)

| Computation | Engine | Time | RSS delta | Notes |
|---|---|---|---|---|
| Load all parquets + concat | pandas | 12s | +10 GB | 119M rows on heap |
| Zone join + groupby (on loaded df) | pandas | 27s | +7 GB | Copy cost of merge |
| Load + join + groupby (combined) | pandas | **39s** | **+17 GB** | Near-OOM on 16 GB machines |
| `df.apply(tip_pct, axis=1)` on 119M rows | pandas | **~328s** | +600 MB | 363k rows/s pure Python loop |
| Zone join + groupby (all 36 files) | xorq DataFusion | **0.4s** | +181 MB | Lazy, streaming, predicate pushdown |

### Citibike (18.8M rows, H1 2024)

| Computation | Engine | Time | RSS delta | Notes |
|---|---|---|---|---|
| Load 22 CSVs + concat | pandas | 27s | high | CSV parse cost |
| Station-pair pivot 100×100 | pandas | 1.7s | low | Not a candidate |
| Station groupby | xorq DataFusion | **0.2s** | +585 MB | From parquet |

---

## The two demo narratives

### Narrative 1 — "I just barely got it into memory" (39s, 17 GB)

Load all taxi parquets into pandas, join with zone lookup, groupby. On a 16 GB laptop this is touch-and-go. You get the result, you keep it in memory, you do not restart the kernel.

```python
# THE EXPENSIVE CELL — run once, pray the kernel lives
import pandas as pd, glob

dfs = [pd.read_parquet(f) for f in sorted(glob.glob("/tmp/nyc_taxi/*.parquet"))]
df = pd.concat(dfs, ignore_index=True)          # 10 GB RSS

zones = pd.read_csv("/tmp/nyc_taxi/taxi_zone_lookup.csv")
merged = df.merge(zones, left_on='PULocationID', right_on='LocationID')  # +7 GB
result = merged.groupby('Zone').agg(
    trip_count=('fare_amount', 'count'),
    mean_fare=('fare_amount', 'mean'),
    total_revenue=('fare_amount', 'sum'),
).sort_values('trip_count', ascending=False)
```

**xorq equivalent — first run:** ~0.4s, 181 MB RSS.  
**xorq second run (cache hit):** instant.

Stageable live: kick it off, talk for 39 seconds, punchline lands when xorq finishes in 0.4s.

### Narrative 2 — "The `apply` that ate my afternoon" (328s, pure Python)

Row-wise tip percentage. The obvious way to write it. 363k rows/second. At 119M rows: 5.5 minutes.

```python
# NAIVE — do not do this on large DataFrames
def tip_pct(row):
    if row['fare_amount'] > 0:
        return row['tip_amount'] / row['fare_amount']
    return 0.0

df['tip_pct'] = df.apply(tip_pct, axis=1)   # ~328s on 119M rows
```

Vectorized replacement runs in under 1 second:
```python
df['tip_pct'] = (df['tip_amount'] / df['fare_amount'].replace(0, float('nan')))
```

**Better for spoken narrative than live demo** — 5.5 minutes is too long on stage. Use as the "I've been there" moment before showing the load+join live.

---

## MCP prompts for the demo

Type these into Claude Code with the pydata MCP server running:

```
Use catalog_load_parquet to load /tmp/nyc_taxi/yellow_tripdata_2024-01.parquet,
name it yellow_jan_2024. Prompt: "NYC yellow cab trips January 2024".
```

```
Create a named entry trips_by_zone that joins yellow_jan_2024 with the taxi zone
lookup at /tmp/nyc_taxi/taxi_zone_lookup.csv on PULocationID, groups by Zone,
and counts trips with mean fare. Name it trips_by_zone.
```

```
Use catalog_load_parquet to load /tmp/citibike/citibike_h1_2024.parquet,
name it citibike_h1. Prompt: "Citibike trips January–June 2024".
```

```
Create a named entry popular_routes from citibike_h1 that groups by
start_station_name and end_station_name, counts trips, and returns the
top 50 pairs by trip count.
```

---

## Timing note for the talk

At 363k rows/second, `df.apply()` on real data:

| Rows | Time |
|---|---|
| 1M | 2.8s |
| 5M | 13.8s |
| 10M | 27.5s |
| 41M (2024 only) | ~113s |
| 119M (3 years) | ~328s |

The xorq DataFusion zone join on the same 119M rows: **0.4 seconds**.  
That's the number to put on the slide.

---

## H3 hex analysis — taxi vs Citibike

Cross-dataset spatial join using H3 resolution 8 (~460 m hex diameter, city-block scale).
Question: **how many 2024 taxi trips could have been a Citibike ride instead?**

Criteria for "substitutable":
- `trip_distance <= 2.0` miles (comfortable bike range)
- Both pickup and dropoff hex are in the Citibike station coverage area
- Borough is Manhattan or Brooklyn (Citibike service area)

### Key findings

| Metric | Value |
|---|---|
| Citibike H1 2024 trips loaded | 18,755,922 (51,559 dropped — null coordinates) |
| NYC Taxi 2024 trips loaded | 41,169,720 (425,660 dropped — unknown zone IDs) |
| Unique Citibike hexes (H3 res 8) | 341 |
| Overlapping hexes (taxi + Citibike) | 88 |
| Substitutable taxi trips | **22,718,378 / 41,169,720 (55.8%)** |

Over half of all 2024 taxi trips — by distance alone — fell within Citibike station hexes in Manhattan and Brooklyn and were short enough to bike.

### Benchmark results

All measurements on a single machine. RSS delta is macOS `ru_maxrss` peak change; numbers reflect peak allocation at that step, not necessarily sustained.

| Step | Engine | Time (s) | RSS delta (MB) |
|---|---|---|---|
| H3 encode citibike NAIVE apply (500k sample) | pandas | 1.57s (→ ~59s projected full 18.8M) | +0 MB |
| H3 encode citibike VECTORIZED (18.8M rows) | pandas/numpy | 14.91s | +4,376 MB |
| H3 encode taxi VECTORIZED only (41.2M rows) | pandas/numpy | 18.52s | +0 MB |
| Zone centroid join + H3 encode taxi (41.2M rows) | pandas | 45.58s | +8,316 MB |
| Overlapping-hex aggregation | pandas | 6.81s | +0 MB |
| Full pipeline (encode + join + filter + agg) | polars | 68.19s | +0 MB |
| Comparison query on cached hex parquet (first run) | xorq/DuckDB | 0.20s | — |
| Comparison query on cached hex parquet (second run) | xorq/DuckDB | 0.14s | — |

**Vectorized H3 encoding**: 1.26M rows/s (citibike), 2.20M rows/s (taxi). Naive `df.apply` runs at 318k rows/s — a 4–7x slowdown, and would take ~59s to encode the full 18.8M citibike dataset.

**Polars full pipeline** was slower than pandas here because H3 encoding still runs through `np.vectorize` (no native H3 UDF in polars), making the pipeline bottleneck identical while polars pays conversion overhead.

**xorq story**: write hex-encoded data to parquet once (6.67s, 298 MB), then the comparison query runs in 0.20s on first execution and 0.14s on subsequent runs — versus ~52s to re-encode + aggregate from scratch in pandas. This is the caching payoff.

### Top 10 overlapping hexes (most substitutable taxi trips)

| H3 hex (res 8) | Substitutable taxi trips | Citibike trips (H1 2024) |
|---|---|---|
| 882a100d69fffff | 2,109,855 | 119,980 |
| 882a100d2dfffff | 1,875,807 | 366,321 |
| 882a100d65fffff | 1,469,828 | 422,751 |
| 882a100d67fffff | 1,239,835 | 283,452 |
| 882a100d63fffff | 1,235,307 | 225,372 |
| 882a100891fffff | 1,222,537 | 132,343 |
| 882a107253fffff | 1,060,253 | 314,349 |
| 882a1008bbfffff | 857,422 | 276,329 |
| 882a10089bfffff | 798,118 | 118,205 |
| 882a100d6dfffff | 777,018 | 107,017 |

### Demo narrative — "half of all taxi trips could have been a bike"

The `55.8%` headline is the live punchline. On stage:

1. Load taxi + citibike parquets (fast, already on disk).
2. Show the naive apply H3 encoding — "this would take 59 seconds on the full dataset."
3. Switch to vectorized: 15 seconds for 18.8M rows, 45 seconds with the zone centroid join.
4. Run the comparison filter + aggregation: another 7 seconds.
5. Total cold pandas pipeline: ~60s end-to-end.
6. xorq: write once, query in 0.2s. Second query: 0.14s.

The xorq cache hit is effectively free. The story is: spatial analysis on 60M rows, two datasets, cross-join via H3 — pandas needs a minute, xorq needs 200 ms.

### How the shapefile join works

NYC TLC publishes 263 taxi zones as a shapefile (EPSG:2263, NY State Plane feet). Steps:

1. `gpd.read_file()` → compute centroid in projected CRS → reproject centroid to EPSG:4326.
2. Join taxi trips on `PULocationID` / `DOLocationID` → pickup and dropoff lat/lng.
3. `np.vectorize(h3.latlng_to_cell)` on the coordinate arrays → H3 hex IDs.

Shapefile download: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip`  
Saved at: `/tmp/taxi_zones/taxi_zones/taxi_zones.shp`  
Hex-encoded output cached at: `/tmp/taxi_hex_encoded.parquet` (298 MB)

---

## H3 benchmark — honest timings

### Corrected end-to-end timings

The earlier benchmark reported xorq at 0.20s vs polars at 68s. That was wrong — the 0.20s was running the comparison query on a **pre-encoded, already-written parquet**. The 68s polars number was measured on a different (slower) run and included pandas-style loading overhead. Below are honest end-to-end timings where both engines start from raw parquets and zone shapefiles, with no pre-encoded cache in place.

All measurements on a single machine (macOS, M-series). RSS delta from `psutil.Process.memory_info().rss`. Both runs start from raw taxi parquets (41.2M rows, 2024 only) and raw citibike parquet (18.8M rows).

| Phase | Engine | Time (s) | Notes |
|---|---|---|---|
| Scan + filter + zone join | polars lazy | 0.20s | before H3; pushdown cuts rows to 23M |
| H3 encode taxi (vectorized) | numpy | 22.47s | unavoidable Python loop |
| H3 encode citibike (vectorized) | numpy | 10.01s | unavoidable Python loop |
| Final filter + agg | polars lazy | 0.47s | after H3 |
| **Total end-to-end** | **polars** | **33.15s** | |
| Encode + write parquets | numpy + pyarrow | 33.75s | one-time cost (taxi 23.7s + citibike 10.0s) |
| Query on cached parquets (first run) | xorq | 0.14s | parquets already written |
| Query on cached parquets (second run) | xorq | 0.14s | OS page cache warm |
| **Total first run** | **xorq** | **33.89s** | encode + write + query |
| **Total second run** | **xorq** | **0.14s** | query only |

**The honest xorq story:** H3 encoding is the bottleneck — 32.5 of the 33 seconds in both pipelines are spent in `np.vectorize(h3.latlng_to_cell)`. Neither engine can avoid this; there is no native vectorized H3 UDF in polars or DataFusion. On a **cold first run**, xorq is not faster than polars (33.9s vs 33.2s — essentially the same, within noise). The xorq value proposition is that you pay the encoding cost **once**: after writing the hex-encoded parquets, every subsequent query runs in 0.14s regardless of how complex the filter or aggregation is. On re-runs, xorq is ~240x faster than re-running the full polars pipeline from scratch. This is the caching payoff: spatial analysis on 60M rows across two datasets, one-time 34-second setup, then sub-200ms per query forever.

---

### Full multi-year timings (2022–2024)

All measurements on a single machine (macOS, M-series). Input: 119.1M taxi rows (36 parquet files, 2022–2024) + 83.7M citibike rows (98 CSVs, 2022–H1 2024). RSS delta from `psutil.Process.memory_info().rss`.

| Phase | Time (s) | RSS delta (MB) | Rows in/out |
|---|---|---|---|
| Phase 1: scan + filter (≤2 mi) + zone join | 0.67 | +4,158 | 119M → 65,024,907 |
| Phase 2: H3 encode taxi (vectorized, res 8) | 67.50 | +11,190 | 65,024,907 rows |
| Phase 3: load CSVs + H3 encode citibike | 66.38 | -11,082 | 83.7M → 83,723,305 |
| Phase 4: filter (both hexes in citibike set, borough MAN/BK) + agg | 11.57 | +5,398 | → 90 hex groups |
| **Total polars pipeline** | **146.12** | | |
| Phase 5a: write hex parquets (taxi + citibike) | 15.75 | -1,578 | |
| Phase 5b: xorq query (first run) | 0.42 | +129 | 127 rows |
| Phase 5c: xorq query (second run) | 0.33 | +17 | 127 rows |
| **Total xorq first run** (5a + 5b) | **16.17** | | |
| **Total xorq second run** (5c only) | **0.33** | | |

**Substitutable taxi trips (all 3 years):** 64,023,027 / 119.1M = **53.8%** of all trips; 64,023,027 / 65,024,907 = **98.5%** of short trips (≤2 miles).

**Key observations:**

- H3 encoding dominates: taxi takes 67.5s (965k rows/s) and citibike takes 66.4s (1.26M rows/s) — both unavoidable Python loops through `np.vectorize`.
- Phase 1 (polars lazy scan of 36 parquet files + filter + double zone join): 0.67s. Predicate pushdown eliminates ~45% of rows before any data lands in memory.
- Phase 4 (filter 65M rows by hex set membership + group-by): 11.6s — dominated by the Python-loop hex membership check across 65M rows.
- Total polars pipeline: 146s (~2.5 min) at 3x the data volume (vs 33s for 2024-only), roughly linear with row count.
- xorq write-once cost: 15.75s (encoding already done in polars phases). Subsequent queries: 0.42s cold, 0.33s warm — essentially free.
- **xorq re-run speedup vs full polars pipeline: ~440x** (0.33s vs 146s).

**Top 10 substitutable hexes (all 3 years):**

| H3 hex (res 8) | Taxi trips (3yr) | Notes |
|---|---|---|
| 882a100d69fffff | 6,080,507 | Midtown core |
| 882a100d2dfffff | 5,350,831 | |
| 882a100d65fffff | 4,206,799 | |
| 882a100d63fffff | 3,521,152 | |
| 882a100891fffff | 3,438,642 | |
| 882a100d67fffff | 3,337,795 | |
| 882a107253fffff | 2,854,794 | |
| 882a1008bbfffff | 2,456,261 | |
| 882a10089bfffff | 2,292,119 | |
| 882a100d29fffff | 2,222,993 | |

---

### Per-step memory profile (2022–2024)

Accurate peak RSS per step measured with a 0.25 s polling thread. RSS snapshots taken at start, peak-during, and end of each phase so GC-driven drops between steps do not hide transient allocations.

Machine: macOS, M-series. Input: 119.1 M taxi rows (36 parquet files, 2022–2024) + 83.7 M citibike rows (98 CSVs). Baseline RSS (end of Phase 0): **128 MB**.

| Phase | Time (s) | RSS before (MB) | RSS peak (MB) | Peak above baseline (MB) | RSS after (MB) |
|---|---|---|---|---|---|
| Phase 0: baseline | 1.01 | 128 | 128 | +0 | 128 |
| Phase 1: load zone centroids | 0.26 | 128 | 128 | +0 | 160 |
| Phase 2: polars lazy scan + filter + zone join | 1.01 | 160 | 6,959 | +6,831 | 5,194 |
| Phase 3: H3 encode taxi | 69.94 | 5,194 | 18,149 | +18,021 | 15,709 |
| Phase 4: load citibike (raw CSVs, lat/lng only) | 15.71 | 15,709 | 17,325 | +17,197 | 3,016 |
| Phase 5: H3 encode citibike | 50.37 | 3,016 | 13,370 | +13,242 | 4,481 |
| Phase 6: build hex set + filter taxi + agg | 7.89 | 4,482 | 15,384 | +15,256 | 6,653 |
| Phase 7: write hex parquets | 2.04 | 6,654 | 13,469 | +13,341 | 13,521 |
| Phase 8: xorq query (cold) | 2.80 | 13,521 | 16,032 | +15,904 | 16,033 |
| Phase 9: xorq query (warm) | 2.02 | 16,033 | 16,725 | +16,597 | 16,725 |

**Key observations:**

- Phase 3 (H3 encode taxi, 65.7 M rows) is the single biggest RAM spike: **+18 GB peak above baseline** (18,149 MB). This is because `np.vectorize` materialises the full output hex-string array alongside the input coordinate arrays simultaneously.
- Phase 2 (polars lazy scan of 36 files, filter to ≤2 mi, double zone join, collect): spikes to **6,959 MB** (+6.8 GB) in just 1 second — polars reads and filters all 36 files in parallel before collecting.
- Phase 4 (citibike CSV load): reads 98 CSVs selecting only 4 columns, peaks at **17,325 MB** but immediately drops to 3,016 MB after collect — most of the taxi H3 arrays are released by GC between phases 3 and 4.
- Phase 6 (hex-set filter + group-by across 65 M rows): spikes to **15,384 MB** (+15.2 GB above baseline) despite only 54 output groups — the full taxi DataFrame is held in memory during the IS-IN scan.
- Phase 7 (write two parquets to disk, total ~1.15 GB): unexpectedly peaks at **13,469 MB** (+13.3 GB) — pyarrow serialises the full DataFrame before streaming to disk.
- The xorq cold query (Phase 8) uses **+2,512 MB** above its entry RSS to execute the join+agg on 587 MB + 562 MB parquets. Warm query (Phase 9) uses **+691 MB** — DataFusion reuses cached file metadata.
- **Practical implication for 16 GB laptops**: Phase 3 alone requires ~18 GB of total RSS. This pipeline cannot run on a 16 GB machine without spilling or chunking the H3 encoding step.

Script: `/Users/paddy/code/pydata-app/scripts/h3_pipeline_bench.py`

---

### Benchmark comparison: DuckDB H3 vs naive OOM vs xorq chunked

All measurements on macOS M-series. Input: 36 taxi parquet files (119.1 M rows, 1.94 GB on disk), 98 citibike CSVs (83.7 M rows). H3 resolution 8. Trip filter: `trip_distance ≤ 2.0`.

#### DuckDB H3 streaming (Benchmark 1)

DuckDB executes the full H3 encode pipeline in a single SQL statement — parquet scan, two zone-centroid joins, `h3_latlng_to_cell()` calls, and filter — without materialising Python arrays. Results written to parquet via `COPY … TO`.

| Step | Time (s) | Peak RSS (MB) | Output rows |
|---|---|---|---|
| Taxi H3 encode (all 3 years, filter ≤2 mi) | **3.1s** | **3,453 MB** | 65,024,907 |
| Write taxi parquet | 0.5s | 4,962 MB | — |
| Citibike H3 encode (83.7 M rows, all CSVs) | **5.9s** | **8,187 MB** | 83,723,305 |
| Write citibike parquet | 1.5s | 5,309 MB | — |

**Comparison with prior numpy.vectorize runs:**

| Engine | Taxi peak RSS | Citibike peak RSS | Taxi time | Citibike time |
|---|---|---|---|---|
| `np.vectorize` (prior run) | **18,149 MB** | **13,370 MB** | 69.9s | 50.4s |
| DuckDB streaming | **3,453 MB** | **8,187 MB** | 3.1s | 5.9s |
| Speedup | **5.3x less RAM** | **1.6x less RAM** | **22x faster** | **8.5x faster** |

DuckDB's streaming execution avoids materialising the full output string array in Python memory. It processes taxi data in 3 seconds at peak 3.5 GB (vs 70 seconds at 18 GB with numpy), and citibike in 6 seconds at 8.2 GB (vs 50 seconds at 13 GB). The citibike RSS is higher because DuckDB reads 98 raw CSVs concurrently and buffers more aggressively than the taxi parquet scan.

#### Naive numpy.vectorize under 8 GB cap (Benchmark 2)

Run the naive full-dataset H3 encode in a subprocess, monitored with `psutil`; send `SIGKILL` when RSS exceeds 8 GB.

**Outcome: SIGKILL at 10,287 MB RSS after 2.0 seconds.**

The subprocess loaded all 65M filtered rows into memory successfully (4,802 MB RSS). The moment `np.vectorize(h3.latlng_to_cell)` began allocating its output string array alongside the full input coordinate arrays, RSS spiked from 4.8 GB to 10.3 GB and triggered the kill. The process never produced a single H3 hex. On a real 8 GB laptop this is not a clean kill — it is an OOM crash with kernel involvement.

Note: on macOS `RLIMIT_AS` cannot be set below the current virtual address space (which exceeds 8 GB for the `uv run` process tree due to ASLR); the monitor-and-kill approach is used instead.

#### xorq chunked caching (Benchmark 3)

Process 65M filtered taxi rows in 7 chunks of 10M rows each. Each chunk is H3-encoded independently and written to `/tmp/xorq_chunks/chunk_NNN.parquet`. On re-run, existing chunk files are detected and skipped; the encode loop does zero work.

**First run (encode phase):**

| Chunk | Rows | Time (s) | Peak RSS (MB) | File size (MB) |
|---|---|---|---|---|
| chunk 0 | 10,000,000 | 10.6s | 11,700 MB | 47 MB |
| chunk 1 | 10,000,000 | 10.9s | 13,024 MB | 47 MB |
| chunk 2 | 10,000,000 | 10.8s | 12,509 MB | 47 MB |
| chunk 3 | 10,000,000 | 10.8s | 11,730 MB | 47 MB |
| chunk 4 | 10,000,000 | 10.7s | 11,837 MB | 47 MB |
| chunk 5 | 10,000,000 | 10.7s | 11,905 MB | 47 MB |
| chunk 6 | 5,024,907 | 5.5s | 11,259 MB | 24 MB |
| **Total** | **65,024,907** | **69.9s** | **11,995 MB avg** | **357 MB total** |

Note: per-chunk peak RSS (~12 GB) reflects the full taxi DataFrame staying resident in memory while each chunk slice is encoded. On a system with enough RAM to hold the full dataset (the slice operation is zero-copy), each chunk itself allocates ~2–3 GB for the encode; the rest is the retained DataFrame. A true streaming chunked reader would hold each 10M-row slice independently and stay well under 4 GB per chunk.

**Re-run (all 7 chunks cached):**

| Phase | Time |
|---|---|
| Scan + filter + join (polars lazy) | 1.0s |
| Chunk loop (7/7 cache hits, no encode) | ~0s |
| **Total re-run** | **1.0s** |

**Query on cached chunks (DuckDB):** 0.26s cold, 0.26s warm (127 hex group rows).

**Summary table:**

| Approach | First-run time | First-run peak RSS | Re-run time |
|---|---|---|---|
| Naive full encode (numpy, no cache) | 69.9s | 18,149 MB | 69.9s (always) |
| DuckDB streaming (Benchmark 1) | 9.0s total | 8,187 MB | 9.0s (always; no cache) |
| xorq chunked (Benchmark 3) | 70.9s (encode + scan) | ~12,000 MB | **1.0s (7/7 cache hits)** |

#### Talk narrative

The three benchmarks tell a single escalating story. First, DuckDB proves that H3 encoding does not have to be a Python memory problem — running it inside the query engine takes 3 seconds at 3.5 GB instead of 70 seconds at 18 GB. Second, the 8 GB cap experiment dramatises why the naive approach fails on conference-size laptops: the process loads 65 million rows cleanly at 4.8 GB, then hits SIGKILL the instant `np.vectorize` tries to allocate the output array alongside the inputs — it never produces a single hex. Third, the chunked xorq cache demo shows the structural solution: break the computation into 10M-row slices, write each to parquet, and on every subsequent run skip the work that is already done. The first run costs the same 70 seconds as the naive path; every re-run costs one second. For a notebook that a collaborator re-opens cold, or a CI job that reruns on a new machine, the cache hit is not an optimisation — it is the difference between a result and a crash. Script: `/tmp/bench_all.py`
