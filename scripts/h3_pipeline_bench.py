"""
H3 pipeline benchmark — accurate per-step peak RSS measurement.
Polls RSS every 0.25s in a background thread to catch true peak.
"""
import os
import sys
import threading
import time

import geopandas as gpd
import h3
import numpy as np
import polars as pl
import psutil

proc = psutil.Process(os.getpid())


def bench(label, fn):
    peak = [proc.memory_info().rss]
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            peak[0] = max(peak[0], proc.memory_info().rss)
            time.sleep(0.25)

    rss_before = proc.memory_info().rss
    t0 = time.perf_counter()
    t = threading.Thread(target=poll, daemon=True)
    t.start()
    try:
        result = fn()
    finally:
        stop.set()
        t.join()
    elapsed = time.perf_counter() - t0
    rss_after = proc.memory_info().rss
    peak_rss = peak[0]
    MB = 1 / 1024 / 1024
    print(f"{label}:")
    print(f"  time:       {elapsed:.2f}s")
    print(f"  RSS before: {rss_before*MB:.0f} MB")
    print(f"  RSS peak:   {peak_rss*MB:.0f} MB  (+{(peak_rss-rss_before)*MB:.0f} MB)")
    print(f"  RSS after:  {rss_after*MB:.0f} MB  (net {(rss_after-rss_before)*MB:+.0f} MB)")
    print()
    sys.stdout.flush()
    return result, {
        "label": label,
        "elapsed": elapsed,
        "rss_before": rss_before,
        "rss_peak": peak_rss,
        "rss_after": rss_after,
    }


# ─── Data paths ───────────────────────────────────────────────────────────────
TAXI_GLOB = "/tmp/nyc_taxi/yellow_tripdata_*.parquet"
CITIBIKE_CSV_GLOB = "/tmp/citibike/csv/*.csv"
SHP_PATH = "/tmp/taxi_zones/taxi_zones/taxi_zones.shp"
ZONE_CSV = "/tmp/nyc_taxi/taxi_zone_lookup.csv"
OUT_TAXI = "/tmp/taxi_hexed.parquet"
OUT_CITIBIKE = "/tmp/citibike_hexed.parquet"

results = []

# ─── Phase 0: baseline ────────────────────────────────────────────────────────
_, r = bench("Phase 0: baseline", lambda: time.sleep(1))
results.append(r)
baseline_rss = r["rss_after"]

# ─── Phase 1: load zone centroids ─────────────────────────────────────────────
centroids_df = None

def phase1():
    gdf = gpd.read_file(SHP_PATH)
    gdf = gdf.to_crs("EPSG:4326")
    gdf["centroid"] = gdf.geometry.centroid
    gdf["lat"] = gdf["centroid"].y
    gdf["lng"] = gdf["centroid"].x
    return pl.from_pandas(gdf[["LocationID", "lat", "lng"]])

_, r = bench("Phase 1: load zone centroids", phase1)
centroids_df = _
results.append(r)

# ─── Phase 2: polars lazy scan + filter + zone join ───────────────────────────
taxi_df = None

def phase2():
    pu = centroids_df.rename({"lat": "pu_lat", "lng": "pu_lng", "LocationID": "PULocationID"})
    do = centroids_df.rename({"lat": "do_lat", "lng": "do_lng", "LocationID": "DOLocationID"})
    # Select only needed columns to sidestep schema heterogeneity across 36 files
    lf = (
        pl.scan_parquet(
            TAXI_GLOB,
            cast_options=pl.ScanCastOptions(
                integer_cast=["upcast", "allow-float"],
                missing_struct_fields="insert",
            ),
            missing_columns="insert",
            extra_columns="ignore",
        )
        .select(["trip_distance", "PULocationID", "DOLocationID"])
        .filter(pl.col("trip_distance") <= 2.0)
        .join(pu.lazy(), on="PULocationID", how="left")
        .join(do.lazy(), on="DOLocationID", how="left")
    )
    df = lf.collect()
    print(f"  rows collected: {len(df):,}")
    return df

taxi_df, r = bench("Phase 2: polars lazy scan + filter + zone join", phase2)
results.append(r)

# ─── Phase 3: H3 encode taxi ──────────────────────────────────────────────────
taxi_hexed = None

def phase3():
    # Drop rows where centroid join produced nulls (unknown zone IDs)
    df = taxi_df.drop_nulls(["pu_lat", "pu_lng", "do_lat", "do_lng"])
    encode = np.vectorize(lambda lat, lng: h3.latlng_to_cell(lat, lng, 8))
    pu_lat = df["pu_lat"].to_numpy()
    pu_lng = df["pu_lng"].to_numpy()
    do_lat = df["do_lat"].to_numpy()
    do_lng = df["do_lng"].to_numpy()
    pu_hex = encode(pu_lat, pu_lng)
    do_hex = encode(do_lat, do_lng)
    return df.with_columns([
        pl.Series("pu_hex", pu_hex),
        pl.Series("do_hex", do_hex),
    ])

taxi_hexed, r = bench("Phase 3: H3 encode taxi", phase3)
results.append(r)

# ─── Phase 4: load citibike ───────────────────────────────────────────────────
citibike_df = None

def phase4():
    # citibike_2022_2024.parquet lacks lat/lng — use raw CSVs
    df = (
        pl.scan_csv(CITIBIKE_CSV_GLOB, infer_schema_length=1000)
        .select(["start_lat", "start_lng", "end_lat", "end_lng"])
        .drop_nulls(["start_lat", "start_lng"])
        .collect()
    )
    print(f"  rows collected: {len(df):,}")
    return df

citibike_df, r = bench("Phase 4: load citibike (raw CSVs, lat/lng only)", phase4)
results.append(r)

# ─── Phase 5: H3 encode citibike ──────────────────────────────────────────────
citibike_hexed = None

def phase5():
    encode = np.vectorize(lambda lat, lng: h3.latlng_to_cell(lat, lng, 8))
    start_lat = citibike_df["start_lat"].to_numpy()
    start_lng = citibike_df["start_lng"].to_numpy()
    hex_arr = encode(start_lat, start_lng)
    return citibike_df.with_columns([pl.Series("start_hex", hex_arr)])

citibike_hexed, r = bench("Phase 5: H3 encode citibike", phase5)
results.append(r)

# ─── Phase 6: build hex set + filter taxi + agg ───────────────────────────────
agg_df = None

def phase6():
    bike_hexes = set(citibike_hexed["start_hex"].to_list())
    zone_lookup = pl.read_csv(ZONE_CSV)
    # keep only Manhattan pickup
    manhattan_zones = (
        zone_lookup.filter(pl.col("Borough") == "Manhattan")["LocationID"].to_list()
    )
    df = (
        taxi_hexed
        .filter(pl.col("pu_hex").is_in(bike_hexes))
        .filter(pl.col("PULocationID").is_in(manhattan_zones))
        .group_by("pu_hex")
        .agg(pl.len().alias("trip_count"))
        .sort("trip_count", descending=True)
    )
    print(f"  hex buckets: {len(df):,}")
    return df

agg_df, r = bench("Phase 6: build hex set + filter taxi + agg", phase6)
results.append(r)

# ─── Phase 7: write hex parquets ──────────────────────────────────────────────
def phase7():
    taxi_hexed.write_parquet(OUT_TAXI)
    citibike_hexed.write_parquet(OUT_CITIBIKE)
    taxi_size = os.path.getsize(OUT_TAXI) / 1024 / 1024
    bike_size = os.path.getsize(OUT_CITIBIKE) / 1024 / 1024
    print(f"  taxi_hexed.parquet:    {taxi_size:.1f} MB")
    print(f"  citibike_hexed.parquet: {bike_size:.1f} MB")

_, r = bench("Phase 7: write hex parquets", phase7)
results.append(r)

# ─── Phase 8: xorq query (cold) ───────────────────────────────────────────────
xorq_result = None

def _make_xorq_query():
    from xorq.api import connect
    from xorq.vendor.ibis import _
    con = connect()
    taxi_tbl = con.read_parquet(OUT_TAXI, table_name="taxi_hexed")
    bike_tbl = con.read_parquet(OUT_CITIBIKE, table_name="citibike_hexed")
    bike_hexes = bike_tbl.select(bike_tbl.start_hex.name("pu_hex")).distinct()
    joined = taxi_tbl.join(bike_hexes, "pu_hex")
    q = (
        joined
        .group_by("pu_hex")
        .agg(joined.pu_hex.count().name("n"))
        .order_by(_.n.desc())
    )
    return q.execute()

def phase8():
    return _make_xorq_query()

xorq_result, r = bench("Phase 8: xorq query (cold)", phase8)
results.append(r)
print(f"  result rows: {len(xorq_result):,}")
print()

# ─── Phase 9: xorq query (warm) ───────────────────────────────────────────────
def phase9():
    return _make_xorq_query()

_, r = bench("Phase 9: xorq query (warm)", phase9)
results.append(r)

# ─── Summary table ────────────────────────────────────────────────────────────
MB = 1 / 1024 / 1024

print("=" * 80)
print("SUMMARY TABLE")
print("=" * 80)
header = f"{'Phase':<45} {'Time(s)':>8} {'Before MB':>10} {'Peak MB':>9} {'+Baseline':>10} {'After MB':>9}"
print(header)
print("-" * len(header))
for r in results:
    above_baseline = (r["rss_peak"] - baseline_rss) * MB
    print(
        f"{r['label']:<45} {r['elapsed']:>8.2f} {r['rss_before']*MB:>10.0f} "
        f"{r['rss_peak']*MB:>9.0f} {above_baseline:>+10.0f} {r['rss_after']*MB:>9.0f}"
    )
print()
print(f"Baseline RSS (end of Phase 0): {baseline_rss*MB:.0f} MB")
