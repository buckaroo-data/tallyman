"""Cache lab: adversarial integration scenarios for result-cache strategies.

NOT on the default test path (deselected via the ``cache_lab`` marker, like
``integration``). Run it when touching result_cache / build identity /
reset-to, to see how a candidate strategy *actually behaves*:

    uv run pytest -m cache_lab tests/test_cache_lab.py -v -s

Knobs:
    CACHE_LAB_TRIPS   trip row count (default 150_000; the motivating bug is
                      ~2_500_000 — expect minutes at that scale)
    CACHE_LAB_CHAIN   mutate-chain depth for the compile-heavy scenario
                      (default 32; datafusion's SQL parser hits
                      RecursionLimitExceeded around depth ~48, which is its
                      own finding about how deep a user's feature chain can
                      get before the backend refuses outright)

Philosophy: hard asserts are reserved for invariants ANY acceptable strategy
must satisfy (changed source content must change identity; stored results
must equal recomputation; reset must not corrupt surviving entries). A
failing hard assert is a finding about the strategy under test, not test
rot — notably, test_append_invalidates FAILS on xorq 0.3.26 as shipped:
the build hash is tokenized under SnapshotStrategy, whose
snapshot_normalize_read keys reads on *path identity only*, so neither
file stat nor content reaches content_hash and an in-place source change
dedups to the stale entry (see the cache-rubric / source-identity ADRs;
this also means a deferred_read_parquet normalize_method override cannot
fix identity — the snapshot hasher never consults it). Every
strategy-DIFFERENTIATING behavior — does identity survive an inode change,
does a stat-preserving swap go stale, how many bytes does an edit cost — is
*recorded*, not asserted, into a JSON report so runs on different branches
can be diffed:

    tests/cache_lab_reports/<git-sha>-<timestamp>.json

Scenario map (each is independent; run one with -k):
    treadmill            iterative computed-column edits over a join (#30's bug)
    agg_views            reducing aggregates: cold/warm view latency + sizes
    touch_identity       byte-identical source rewrite (new inode/mtime)
    append_invalidates   real source change MUST fork identity (hard)
    stat_swap            content swap preserving (mtime, size, inode)
    selfheal_after_edit  evicted result regenerated after the source changed
    reset_roundtrip      reset-to back/forward across built entries + caches
    parent_regen         from_catalog chain after parent result regeneration
    wide_compile         cheap to execute, expensive to compile (deep DAG)
    heavy_execute        expensive to execute, cheap to compile (skewed self-join)
    sizing               results whose size defies structural intuition
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tallyman_core import catalog_state as cs
from tallyman_core import paths
from tallyman_core.paths import data_dir, ensure_project, entry_dir, set_active_project
from tallyman_xorq.build import build_and_persist, load_entry
from tallyman_xorq.result_cache import cached_result_expr, ensure_result
from tests.cache_lab_data import (
    N_STATIONS,
    capacity_frame,
    events_frame,
    station_frame,
    trips_frame,
    weather_frame,
    write_plain_parquet,
)

pytestmark = pytest.mark.cache_lab

TRIPS_ROWS = int(os.environ.get("CACHE_LAB_TRIPS", "150000"))
CHAIN_DEPTH = int(os.environ.get("CACHE_LAB_CHAIN", "32"))
IDENTITY_MODE = os.environ.get("TALLYMAN_SOURCE_IDENTITY", "off")
REPORT_DIR = Path(__file__).resolve().parent / "cache_lab_reports"


# ---------------------------------------------------------------------------
# Report plumbing
# ---------------------------------------------------------------------------


class LabReport:
    def __init__(self) -> None:
        self.scenarios: dict[str, dict] = {}
        self.started = datetime.now(timezone.utc).isoformat(timespec="seconds")

    def scenario(self, name: str) -> dict:
        return self.scenarios.setdefault(name, {})

    def write(self) -> Path:
        rev = (
            subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
            or "nogit"
        )
        from importlib.metadata import version

        doc = {
            "meta": {
                "started": self.started,
                "git_rev": rev,
                "trips_rows": TRIPS_ROWS,
                "chain_depth": CHAIN_DEPTH,
                "platform": platform.platform(),
                "xorq": version("xorq"),
                "xorq_dasher": version("xorq-dasher"),
                "source_identity_mode": IDENTITY_MODE,
            },
            "scenarios": self.scenarios,
        }
        REPORT_DIR.mkdir(exist_ok=True)
        out = REPORT_DIR / f"{rev}-{IDENTITY_MODE}-{self.started.replace(':', '')}.json"
        out.write_text(json.dumps(doc, indent=2, default=str))
        print(f"\n=== cache lab report: {out} ===")
        for name, rec in self.scenarios.items():
            print(f"\n[{name}]")
            for k, v in rec.items():
                if isinstance(v, float):
                    v = round(v, 4)
                print(f"  {k}: {v}")
        return out


@pytest.fixture(scope="session")
def lab_report():
    rep = LabReport()
    yield rep
    if rep.scenarios:
        rep.write()


# ---------------------------------------------------------------------------
# Data staging + per-scenario project
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def staged_data(tmp_path_factory) -> Path:
    d = tmp_path_factory.mktemp("cache_lab_data")
    rng = np.random.default_rng(7)
    stations = station_frame(rng)
    stations.to_parquet(d / "stations.parquet")
    trips_frame(TRIPS_ROWS, stations, rng).to_parquet(d / "trips.parquet")
    events_frame(stations, rng).to_parquet(d / "events.parquet")
    weather_frame(rng).to_parquet(d / "weather.parquet")
    write_plain_parquet(capacity_frame(stations), d / "capacity.parquet")
    return d


_WARMED = False


def _warm_xorq_once(staged_data: Path) -> None:
    """One throwaway build so first-import/datafusion-init cost (~15s) lands
    in a scratch project instead of polluting the first scenario's timings."""
    global _WARMED
    if _WARMED:
        return
    ensure_project("lab-warmup")
    dd = data_dir("lab-warmup")
    dd.mkdir(parents=True, exist_ok=True)
    shutil.copy2(staged_data / "stations.parquet", dd / "stations.parquet")
    build_and_persist(
        "lab-warmup",
        _prelude("lab-warmup") + 'expr = from_project("stations.parquet", project=_P).select("station_id")\n',
    )
    _WARMED = True


@pytest.fixture
def lab(staged_data: Path, isolated_home: Path, request, lab_report):
    """A fresh project seeded with copies of the staged parquets."""
    _warm_xorq_once(staged_data)
    name = request.node.name.removeprefix("test_").replace("_", "-")[:32].strip("-")
    ensure_project(name)
    set_active_project(name)
    dd = data_dir(name)
    dd.mkdir(parents=True, exist_ok=True)
    for f in staged_data.iterdir():
        shutil.copy2(f, dd / f.name)
    rec = lab_report.scenario(request.node.name.removeprefix("test_"))

    class Lab:
        project = name
        record = rec
        data = dd

    return Lab


# ---------------------------------------------------------------------------
# Measurement helpers
# ---------------------------------------------------------------------------


def du(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def disk(project: str) -> dict:
    entries = paths.entries_dir(project)
    results = sum(f.stat().st_size for f in entries.rglob("result.parquet")) if entries.exists() else 0
    data = du(paths.data_dir(project))
    caches = results + du(paths.compute_cache_dir(project))
    return {
        "data_bytes": data,
        "entry_result_bytes": results,
        "entry_total_bytes": du(entries),
        "compute_cache_bytes": du(paths.compute_cache_dir(project)),
        "bullpen_bytes": du(paths.bullpen_dir(project)),
        "cache_to_data_ratio": round(caches / data, 3) if data else None,
    }


def build(project: str, code: str) -> dict:
    """build_and_persist with wall timing and size accounting.

    compile_proxy_s is wall minus execute: it bundles xorq's build/serialize,
    load, schema work, and catalog registration. Noisy in absolute terms but
    measured identically for every entry, so ratios are comparable within and
    across runs.
    """
    t0 = time.monotonic()
    res = build_and_persist(project, code)
    wall = time.monotonic() - t0
    rp = entry_dir(project, res.content_hash) / "result.parquet"
    return {
        "hash": res.content_hash,
        "rows": res.row_count,
        "wall_s": round(wall, 3),
        "execute_s": res.execute_seconds,
        "compile_proxy_s": round(wall - res.execute_seconds, 3),
        "result_bytes": rp.stat().st_size if rp.exists() else 0,
    }


def view_timings(project: str, content_hash: str) -> dict:
    """Cold/warm view costs through the production read path."""
    t0 = time.monotonic()
    p1 = ensure_result(project, content_hash)
    cold = time.monotonic() - t0
    t0 = time.monotonic()
    p2 = ensure_result(project, content_hash)
    warm = time.monotonic() - t0
    assert p1 == p2
    t0 = time.monotonic()
    pd.read_parquet(p1)
    read = time.monotonic() - t0
    t0 = time.monotonic()
    n = cached_result_expr(project, content_hash).count().execute()
    expr_count = time.monotonic() - t0
    return {
        "view_cold_s": round(cold, 3),
        "view_warm_s": round(warm, 3),
        "parquet_read_s": round(read, 3),
        "cached_expr_count_s": round(expr_count, 3),
        "cached_expr_rows": int(n),
    }


def assert_stored_matches_recompute(project: str, content_hash: str, sort_keys: list[str], cols: list[str]):
    """Invariant for ANY strategy: what the cache serves == what recompute produces."""
    fresh = load_entry(project, content_hash).execute()
    stored = pd.read_parquet(ensure_result(project, content_hash))
    f = fresh[cols].sort_values(sort_keys).reset_index(drop=True)
    s = stored[cols].sort_values(sort_keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(f, s, check_dtype=False, rtol=1e-5, atol=1e-8)


def md5_file(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# User-plausible expression code (strings executed by build_and_persist)
# ---------------------------------------------------------------------------


def _prelude(project: str) -> str:
    return (
        f"from tallyman_xorq.io import from_project, from_catalog\nimport xorq.vendor.ibis as ibis\n_P = {project!r}\n"
    )


def treadmill_code(project: str, edit: int) -> str:
    """The #30 workflow: a join, then cumulative computed-column edits.

    Each edit restates the whole cell (how users actually iterate), so every
    version's op graph contains the upstream Join. Distance is plain
    equirectangular km — deliberately not the Manhattan-grid metric the demo
    catalog already has.
    """
    parts = [
        _prelude(project),
        'trips = from_project("trips.parquet", project=_P)\n'
        'stations = from_project("stations.parquet", project=_P)\n'
        "t = trips.filter(trips.start_station_id != trips.end_station_id)\n"
        "j = t.join(stations, t.start_station_id == stations.station_id)\n"
        "j = j.mutate(trip_minutes=(j.ended_at.cast('int64') - j.started_at.cast('int64')) / 60_000_000)\n",
    ]
    if edit >= 2:
        parts.append(
            "dy = (j.end_lat - j.start_lat) * 111.32\n"
            "dx = (j.end_lng - j.start_lng) * 85.0\n"
            "j = j.mutate(straight_km=(dx * dx + dy * dy).sqrt())\n"
            "j = j.mutate(speed_kmh=j.straight_km / (j.trip_minutes / 60))\n"
        )
    if edit >= 3:
        parts.append("j = j.mutate(speed_kmh=ibis.ifelse(j.trip_minutes > 240, ibis.null(), j.speed_kmh))\n")
    if edit >= 4:
        parts.append(
            "dow = j.started_at.day_of_week.index()\n"
            "j = j.mutate(is_weekend=(dow >= 5), hour=j.started_at.hour())\n"
            "j = j.mutate(is_rush=((j.hour >= 7) & (j.hour <= 9)) | ((j.hour >= 16) & (j.hour <= 18)))\n"
        )
    if edit >= 5:
        parts.append(
            "unlock = ibis.cases((j.member_casual == 'member', 0.0), else_=1.50)\n"
            "per_min = ibis.cases(\n"
            "    (j.rideable_type == 'electric_bike', 0.36),\n"
            "    (j.member_casual == 'member', 0.0),\n"
            "    else_=0.26,\n"
            ")\n"
            "j = j.mutate(fare_estimate=unlock + per_min * j.trip_minutes)\n"
        )
    if edit >= 6:
        parts.append("j = j.filter((j.speed_kmh < 45) & (j.trip_minutes < 240))\n")
    parts.append("expr = j\n")
    return "".join(parts)


def flow_imbalance_code(project: str) -> str:
    """Rebalancing pressure per station — the classic bikeshare ops question.

    Two aggregates joined, re-aggregated, joined to station metadata: heavy
    lineage, tiny (~600 row) result. The high-value cache archetype.
    """
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        'stations = from_project("stations.parquet", project=_P)\n'
        "t = trips.mutate(day=trips.started_at.truncate('D'))\n"
        "dep = t.group_by(['start_station_id', 'day']).aggregate(departures=t.ride_id.count())\n"
        "arr = t.group_by(['end_station_id', 'day']).aggregate(arrivals=t.ride_id.count())\n"
        "f = dep.join(arr, [dep.start_station_id == arr.end_station_id, dep.day == arr.day])\n"
        "f = f.mutate(net=f.departures - f.arrivals)\n"
        "g = f.group_by('start_station_id').aggregate(mean_net=f.net.mean(), days_seen=f.net.count())\n"
        "g = g.join(stations, g.start_station_id == stations.station_id)\n"
        "expr = g.select('start_station_id', 'station_name', 'capacity', 'mean_net', 'days_seen',\n"
        "                pressure=g.mean_net / g.capacity).order_by(ibis.desc('pressure'))\n"
    )


def weather_mix_code(project: str) -> str:
    """Trips joined to hourly weather on the truncated hour, bucketed."""
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        'wx = from_project("weather.parquet", project=_P)\n'
        "t = trips.mutate(obs=trips.started_at.truncate('h'))\n"
        "j = t.join(wx, t.obs == wx.obs_hour)\n"
        "j = j.mutate(minutes=(j.ended_at.cast('int64') - j.started_at.cast('int64')) / 60_000_000)\n"
        "bucket = ibis.cases((j.precip_mm <= 0, 'dry'), (j.precip_mm < 2.5, 'light'), else_='wet')\n"
        "j = j.mutate(precip_bucket=bucket)\n"
        "expr = j.group_by(['precip_bucket', 'member_casual']).aggregate(\n"
        "    n_trips=j.ride_id.count(), avg_minutes=j.minutes.mean())\n"
    )


def od_matrix_code(project: str, order: str = "name") -> str:
    """Origin-destination trip matrix; `order` exists for the sizing scenario."""
    tail = {
        "name": "expr = od.order_by(['start_station_name', 'end_station_name'])\n",
        "shuffle": "expr = od.order_by(((od.n_trips * 2654435761) % 1000003).name('shuffle_key'))\n",
        "none": "expr = od\n",
    }[order]
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        "t = trips.filter(trips.start_station_id != trips.end_station_id)\n"
        "t = t.mutate(minutes=(t.ended_at.cast('int64') - t.started_at.cast('int64')) / 60_000_000)\n"
        "od = t.group_by(['start_station_name', 'end_station_name']).aggregate(\n"
        "    n_trips=t.ride_id.count(), avg_minutes=t.minutes.mean())\n" + tail
    )


def member_count_code(project: str) -> str:
    """Smallest honest aggregate; used where the scenario isn't about the expr."""
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        "expr = trips.group_by('member_casual').aggregate(n=trips.ride_id.count())\n"
    )


def utilization_code(project: str) -> str:
    """Trips-per-dock against the fixed-width capacity table (swap scenario)."""
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        'cap = from_project("capacity.parquet", project=_P)\n'
        "g = trips.group_by('start_station_id').aggregate(n_starts=trips.ride_id.count())\n"
        "j = g.join(cap, g.start_station_id == cap.station_id)\n"
        "expr = j.select('start_station_id', 'n_starts', 'capacity',\n"
        "                trips_per_dock=j.n_starts / j.capacity).order_by(ibis.desc('trips_per_dock'))\n"
    )


def cleaned_trips_code(project: str) -> str:
    """Parent of a from_catalog chain: filter + projection only (no join/agg/sort)."""
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        "t = trips.filter((trips.start_station_id != trips.end_station_id)\n"
        "                 & (trips.ended_at > trips.started_at))\n"
        "expr = t.select('ride_id', 'rideable_type', 'started_at', 'ended_at',\n"
        "                'start_station_id', 'end_station_id', 'member_casual')\n"
    )


def child_of_catalog_code(project: str, parent_hash: str) -> str:
    return _prelude(project) + (
        f"base = from_catalog({parent_hash!r}, project=_P)\n"
        "base = base.mutate(minutes=(base.ended_at.cast('int64') - base.started_at.cast('int64')) / 60_000_000)\n"
        "expr = base.group_by(['start_station_id', 'rideable_type']).aggregate(\n"
        "    n=base.ride_id.count(), avg_minutes=base.minutes.mean())\n"
    )


def wide_compile_code(project: str, depth: int) -> str:
    """Cheap to execute, expensive to compile: one streaming pass over the
    rows, but a DAG with a `depth`-deep dependent mutate chain, 24 one-hot
    columns, and a fare CASE ladder. The feature-pipeline shape users build
    in a loop without thinking about plan size.
    """
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        "f = trips.mutate(minutes=(trips.ended_at.cast('int64') - trips.started_at.cast('int64')) / 60_000_000,\n"
        "                 hour=trips.started_at.hour())\n"
        "for h in range(24):\n"
        "    f = f.mutate(**{f'is_h{h:02d}': (f.hour == h).cast('int8')})\n"
        f"for i in range({depth}):\n"
        "    prev = f[f'score_{i}'] if i else f.minutes\n"
        "    f = f.mutate(**{f'score_{i + 1}': ((prev * 1.013 + (f.hour + i) % 7).abs() + 1.0).ln() + prev * 0.5})\n"
        "band = ibis.cases(*[ (f.start_station_id % 20 == k, float(k)) for k in range(19) ], else_=19.0)\n"
        "f = f.mutate(zone_band=band)\n"
        f"expr = f.select('ride_id', 'minutes', 'hour', 'zone_band', f'score_{depth}',\n"
        "                *[f'is_h{h:02d}' for h in range(24)])\n"
    )


def dock_contention_code(project: str) -> str:
    """Expensive to execute, cheap to compile: a skewed self-join.

    Pairs of distinct rides leaving the same station within 3 minutes of
    each other in week one — "how contended are my docks". The zipf head
    makes pair counts quadratic in station popularity; the DAG is ~6 nodes
    and the result is one row per station.
    """
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        "week = trips.filter(trips.started_at < ibis.timestamp('2025-09-08 00:00:00'))\n"
        "a = week.select(sid='start_station_id', ride='ride_id', ts='started_at')\n"
        "b = week.view().select(sid2='start_station_id', ride2='ride_id', ts2='started_at')\n"
        "p = a.join(b, a.sid == b.sid2)\n"
        "p = p.filter((p.ride < p.ride2) & ((p.ts.cast('int64') - p.ts2.cast('int64')).abs() < 180_000_000))\n"
        "expr = p.group_by('sid').aggregate(near_collisions=p.ride.count()).order_by(ibis.desc('near_collisions'))\n"
    )


def event_annotate_code(project: str) -> str:
    """'Annotate each trip with its start station's maintenance events' —
    reads as an enrichment, is actually a row multiplier (~8 events/station)."""
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        'ev = from_project("events.parquet", project=_P)\n'
        "j = trips.join(ev, trips.start_station_id == ev.station_id)\n"
        "expr = j.select('ride_id', 'start_station_id', 'started_at', 'member_casual',\n"
        "                'event_type', 'event_date', 'crew')\n"
    )


def groups_galore_code(project: str) -> str:
    """An Aggregate that barely reduces: group keys ≈ row identity."""
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        "t = trips.mutate(day=trips.started_at.truncate('D'), hour=trips.started_at.hour(),\n"
        "                 minutes=(trips.ended_at.cast('int64') - trips.started_at.cast('int64')) / 60_000_000)\n"
        "expr = t.group_by(['start_station_id', 'end_station_id', 'day', 'hour']).aggregate(\n"
        "    n=t.ride_id.count(), avg_minutes=t.minutes.mean())\n"
    )


def label_balloon_code(project: str) -> str:
    """Row-wise string assembly: output bytes balloon past the input's."""
    return _prelude(project) + (
        'trips = from_project("trips.parquet", project=_P)\n'
        "t = trips.mutate(minutes=(trips.ended_at.cast('int64') - trips.started_at.cast('int64')) / 60_000_000)\n"
        "t = t.mutate(route_label=t.start_station_name.concat(' -> ').concat(t.end_station_name))\n"
        "t = t.mutate(trip_summary=t.member_casual.concat(' ').concat(t.rideable_type)\n"
        "             .concat(' ride ').concat(t.ride_id).concat(' via ').concat(t.route_label))\n"
        "expr = t.select('ride_id', 'route_label', 'trip_summary', 'minutes')\n"
    )


def distinct_tiny_code(project: str) -> str:
    return _prelude(project) + (
        "trips = from_project(\"trips.parquet\", project=_P)\nexpr = trips.select('rideable_type').distinct()\n"
    )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def test_treadmill(lab):
    """#30's motivating bug, end to end: 6 cumulative edits over a 600-station
    join, each viewed once. Records what every edit cost in bytes and time."""
    edits = []
    for i in range(1, 7):
        b = build(lab.project, treadmill_code(lab.project, i))
        v = view_timings(lab.project, b["hash"])
        d = disk(lab.project)
        edits.append({**b, **v, "cache_to_data_ratio": d["cache_to_data_ratio"]})
    lab.record["edits"] = edits
    lab.record["final_disk"] = disk(lab.project)
    lab.record["distinct_hashes"] = len({e["hash"] for e in edits})

    # Invariants: edits produced distinct identities, and the final stored
    # result matches a fresh recompute (spot-checked on the fare column).
    assert lab.record["distinct_hashes"] == 6
    assert_stored_matches_recompute(
        lab.project, edits[-1]["hash"], sort_keys=["ride_id"], cols=["ride_id", "trip_minutes", "fare_estimate"]
    )
    # Pandas cross-check on one derived value: max fare for casual e-bike rides.
    stored = pd.read_parquet(ensure_result(lab.project, edits[-1]["hash"]))
    casual_ebike = stored[(stored.member_casual == "casual") & (stored.rideable_type == "electric_bike")]
    expect = 1.50 + 0.36 * casual_ebike.trip_minutes
    assert np.allclose(casual_ebike.fare_estimate, expect, rtol=1e-9)


def test_agg_views(lab):
    """Reducing aggregates: tiny results over heavy lineage. Records the
    cold/warm view gap a cache strategy is supposed to win."""
    flow = build(lab.project, flow_imbalance_code(lab.project))
    wx = build(lab.project, weather_mix_code(lab.project))
    lab.record["flow"] = {**flow, **view_timings(lab.project, flow["hash"])}
    lab.record["weather"] = {**wx, **view_timings(lab.project, wx["hash"])}
    t0 = time.monotonic()
    load_entry(lab.project, flow["hash"]).execute()
    lab.record["flow"]["full_recompute_s"] = round(time.monotonic() - t0, 3)
    lab.record["final_disk"] = disk(lab.project)

    assert flow["rows"] <= N_STATIONS
    assert wx["rows"] <= 6  # 3 precip buckets × 2 member kinds
    assert_stored_matches_recompute(
        lab.project, flow["hash"], sort_keys=["start_station_id"], cols=["start_station_id", "mean_net", "pressure"]
    )


def test_touch_identity(lab):
    """Byte-identical rewrite of the source (new inode + mtime). Strategy
    differentiator: stat-keyed identity forks, content-keyed identity holds."""
    code = od_matrix_code(lab.project)
    b1 = build(lab.project, code)

    src = lab.data / "trips.parquet"
    tmp = src.with_suffix(".rewrite")
    shutil.copyfile(src, tmp)
    os.replace(tmp, src)  # same bytes, fresh inode and mtime

    b2 = build(lab.project, code)
    lab.record.update(
        {
            "hash_before": b1["hash"],
            "hash_after": b2["hash"],
            "identity_stable_on_identical_rewrite": b1["hash"] == b2["hash"],
            "duplicate_entries_created": int(b1["hash"] != b2["hash"]),
            "build_wall_s": [b1["wall_s"], b2["wall_s"]],
            "final_disk": disk(lab.project),
        }
    )
    # Invariant either way: both entries hold the same logical result.
    a = pd.read_parquet(ensure_result(lab.project, b1["hash"]))
    b = pd.read_parquet(ensure_result(lab.project, b2["hash"]))
    pd.testing.assert_frame_equal(
        a.sort_values(["start_station_name", "end_station_name"]).reset_index(drop=True),
        b.sort_values(["start_station_name", "end_station_name"]).reset_index(drop=True),
        check_dtype=False,
        rtol=1e-9,
    )


def test_append_invalidates(lab, staged_data):
    """A real content change MUST fork identity and refresh results — hard
    invariant for any strategy."""
    code = member_count_code(lab.project)
    b1 = build(lab.project, code)
    n_before = pd.read_parquet(ensure_result(lab.project, b1["hash"]))["n"].sum()

    src = lab.data / "trips.parquet"
    extra = trips_frame(
        max(TRIPS_ROWS // 20, 500), pd.read_parquet(lab.data / "stations.parquet"), np.random.default_rng(99)
    )
    pd.concat([pd.read_parquet(src), extra], ignore_index=True).to_parquet(src)

    b2 = build(lab.project, code)
    n_after = pd.read_parquet(ensure_result(lab.project, b2["hash"]))["n"].sum()
    lab.record.update(
        {
            "hash_before": b1["hash"],
            "hash_after": b2["hash"],
            "rows_before": int(n_before),
            "rows_after": int(n_after),
        }
    )
    assert b2["hash"] != b1["hash"], "appending source rows did not change entry identity"
    assert n_after == n_before + len(extra), "rebuilt result does not reflect the appended rows"


def test_stat_swap(lab):
    """The nastiest invalidation case: replace content while preserving
    (mtime, size, inode). A stat identity keeps the old hash and silently
    serves stale bytes; a content identity forks. Recorded, not asserted."""
    code = utilization_code(lab.project)
    b1 = build(lab.project, code)
    stored_before = pd.read_parquet(ensure_result(lab.project, b1["hash"]))

    cap_path = lab.data / "capacity.parquet"
    st_before = cap_path.stat()
    df = pd.read_parquet(cap_path)
    swapped = df.assign(capacity=(df.capacity + 7).astype("int64"))
    tmp = cap_path.with_suffix(".swap")
    write_plain_parquet(swapped, tmp)
    if tmp.stat().st_size != st_before.st_size:
        tmp.unlink()
        pytest.skip("plain-parquet writer did not produce byte-length-identical files")
    with cap_path.open("r+b") as fh:  # in-place: inode preserved
        fh.write(tmp.read_bytes())
    tmp.unlink()
    os.utime(cap_path, ns=(st_before.st_atime_ns, st_before.st_mtime_ns))
    st_after = cap_path.stat()
    assert (st_after.st_mtime_ns, st_after.st_size, st_after.st_ino) == (
        st_before.st_mtime_ns,
        st_before.st_size,
        st_before.st_ino,
    ), "test rig failed to preserve the stat triple"

    b2 = build(lab.project, code)
    detected = b2["hash"] != b1["hash"]
    lab.record.update({"hash_before": b1["hash"], "hash_after": b2["hash"], "content_swap_detected": detected})
    if not detected:
        # Same identity ⇒ the dedup path returned the OLD entry. Show whether
        # its stored bytes now disagree with an honest recompute.
        fresh = load_entry(lab.project, b1["hash"]).execute()
        merged = stored_before.merge(fresh, on="start_station_id", suffixes=("_stale", "_fresh"))
        lab.record["stale_rows_served"] = int((merged.capacity_stale != merged.capacity_fresh).sum())
    else:
        fresh = pd.read_parquet(ensure_result(lab.project, b2["hash"]))
        assert (
            fresh.sort_values("start_station_id").capacity.to_numpy()
            == swapped.sort_values("station_id").capacity.to_numpy()
        ).all()


def test_selfheal_after_edit(lab):
    """An entry's materializations are evicted AFTER the user edits the
    source file in place. Self-heal recomputes from the persisted build:
    does the old entry get back the data it was built from, or silently
    absorb the new data under the old identity? A content-addressed
    snapshot (cas) stays faithful; a plain data-path read recomputes over
    the new bytes. Recorded, not asserted — it differentiates strategies."""
    code = od_matrix_code(lab.project)
    b1 = build(lab.project, code)
    before = pd.read_parquet(ensure_result(lab.project, b1["hash"]))

    src = lab.data / "trips.parquet"
    df = pd.read_parquet(src)
    df.head(len(df) // 2).to_parquet(src)  # the user halves the file in place

    rp = entry_dir(lab.project, b1["hash"]) / "result.parquet"
    if rp.exists():
        rp.unlink()
    # #73: baked results + source parses live in the compute cache; wipe it to
    # evict every materialization and force a self-heal recompute from the build.
    cc_dir = paths.compute_cache_dir(lab.project)
    if cc_dir.exists():
        shutil.rmtree(cc_dir)

    healed = pd.read_parquet(ensure_result(lab.project, b1["hash"]))
    lab.record.update(
        {
            "trips_before": int(before.n_trips.sum()),
            "trips_healed": int(healed.n_trips.sum()),
            "selfheal_faithful_after_source_edit": int(healed.n_trips.sum()) == int(before.n_trips.sum()),
        }
    )
    assert len(healed) > 0  # self-heal must at least produce a readable result


def test_reset_roundtrip(lab):
    """reset-to back and forward across built entries, with warmed caches.
    Hard invariants: reset never corrupts surviving entries and their reads
    still match recompute. Identity behavior across the reset is recorded."""
    cs.ensure_catalog_repo(lab.project)
    cs.genesis(lab.project)

    base = build(lab.project, flow_imbalance_code(lab.project))
    view_timings(lab.project, base["hash"])
    s1 = cs.checkpoint_catalog(lab.project, "lab: baseline aggregate")
    assert s1 is not None

    e2_code = treadmill_code(lab.project, 2)
    e2 = build(lab.project, e2_code)
    e5 = build(lab.project, treadmill_code(lab.project, 5))
    view_timings(lab.project, e2["hash"])
    view_timings(lab.project, e5["hash"])
    s2 = cs.checkpoint_catalog(lab.project, "lab: two treadmill edits")
    assert s2 is not None

    base_df_before = pd.read_parquet(ensure_result(lab.project, base["hash"]))
    lab.record["disk_at_step2"] = disk(lab.project)

    cs.reset_to(lab.project, s1)
    lab.record["disk_after_back_reset"] = disk(lab.project)
    lab.record["edits_pruned_on_back_reset"] = [
        not entry_dir(lab.project, e2["hash"]).exists(),
        not entry_dir(lab.project, e5["hash"]).exists(),
    ]
    assert entry_dir(lab.project, base["hash"]).exists(), "reset destroyed a baseline entry"
    base_df_after = pd.read_parquet(ensure_result(lab.project, base["hash"]))
    pd.testing.assert_frame_equal(
        base_df_before.sort_values("start_station_id").reset_index(drop=True),
        base_df_after.sort_values("start_station_id").reset_index(drop=True),
        check_dtype=False,
        rtol=1e-9,
    )

    rebuilt = build(lab.project, e2_code)  # same code, same untouched source
    lab.record["rebuild_after_reset_dedups"] = rebuilt["hash"] == e2["hash"]

    cs.reset_to(lab.project, s2)
    lab.record["disk_after_forward_reset"] = disk(lab.project)
    lab.record["edit_restored_on_forward_reset"] = entry_dir(lab.project, e5["hash"]).exists()
    if entry_dir(lab.project, e5["hash"]).exists():
        p = ensure_result(lab.project, e5["hash"])
        assert p.exists() and p.stat().st_size > 0


def test_parent_regen(lab):
    """from_catalog chain identity across parent result regeneration — the
    interaction the two ADRs have to get right together. If the regenerated
    parent parquet is not byte-identical, a content-keyed child forks; a
    stat-keyed child forks regardless (new inode)."""
    parent = build(lab.project, cleaned_trips_code(lab.project))
    child_code = child_of_catalog_code(lab.project, parent["hash"])
    c1 = build(lab.project, child_code)

    parent_rp = entry_dir(lab.project, parent["hash"]) / "result.parquet"
    md5_before = md5_file(parent_rp)
    parent_rp.unlink()
    t0 = time.monotonic()
    regenerated = ensure_result(lab.project, parent["hash"])
    lab.record["parent_regen_s"] = round(time.monotonic() - t0, 3)
    assert regenerated.exists()
    lab.record["parent_regen_byte_identical"] = md5_file(regenerated) == md5_before

    c2 = build(lab.project, child_code)
    lab.record.update(
        {
            "child_hash_before": c1["hash"],
            "child_hash_after": c2["hash"],
            "child_stable_after_parent_regen": c1["hash"] == c2["hash"],
            "final_disk": disk(lab.project),
        }
    )
    assert_stored_matches_recompute(
        lab.project,
        c2["hash"],
        sort_keys=["start_station_id", "rideable_type"],
        cols=["start_station_id", "rideable_type", "n"],
    )


def test_wide_compile(lab):
    """Cheap to execute, expensive to compile. A cost model built only on
    execute_seconds undervalues caching here; the per-view recompile is the
    real tax. Records compile-vs-execute split and the view-path costs."""
    b = build(lab.project, wide_compile_code(lab.project, CHAIN_DEPTH))
    v = view_timings(lab.project, b["hash"])
    t0 = time.monotonic()
    load_entry(lab.project, b["hash"]).count().execute()  # the no-cache view path
    uncached_view = round(time.monotonic() - t0, 3)
    lab.record.update(
        {
            **b,
            **v,
            "chain_depth": CHAIN_DEPTH,
            "uncached_view_s": uncached_view,
            "compile_to_execute_ratio": round(b["compile_proxy_s"] / max(b["execute_s"], 1e-3), 2),
            "final_disk": disk(lab.project),
        }
    )
    stored = pd.read_parquet(ensure_result(lab.project, b["hash"]))
    assert stored[f"score_{CHAIN_DEPTH}"].notna().all()
    assert int(stored[[f"is_h{h:02d}" for h in range(24)]].sum(axis=1).max()) == 1


def test_heavy_execute(lab):
    """Expensive to execute, cheap to compile: skewed self-join down to one
    row per station. The high value-per-byte archetype; also the entry whose
    recompute cost a build-time-only measurement will understate as data
    grows. Correctness pinned against a pandas reference on the same data."""
    b = build(lab.project, dock_contention_code(lab.project))
    v = view_timings(lab.project, b["hash"])
    lab.record.update(
        {**b, **v, "execute_to_compile_ratio": round(b["execute_s"] / max(b["compile_proxy_s"], 1e-3), 2)}
    )

    # Pandas reference on a bounded subset (the busiest stations) so the
    # check stays tractable when CACHE_LAB_TRIPS is cranked up.
    trips = pd.read_parquet(lab.data / "trips.parquet")
    week = trips[trips.started_at < pd.Timestamp("2025-09-08")][["start_station_id", "ride_id", "started_at"]]
    top = week.start_station_id.value_counts().index[:25]
    week = week[week.start_station_id.isin(top)]
    pairs = week.merge(week, on="start_station_id", suffixes=("_a", "_b"))
    pairs = pairs[
        (pairs.ride_id_a < pairs.ride_id_b)
        & ((pairs.started_at_a - pairs.started_at_b).abs() < pd.Timedelta(seconds=180))
    ]
    expect = pairs.groupby("start_station_id").size()
    stored = pd.read_parquet(ensure_result(lab.project, b["hash"])).set_index("sid")["near_collisions"]
    stored = stored[stored.index.isin(top)]
    pd.testing.assert_series_equal(stored.sort_index(), expect.sort_index(), check_names=False, check_dtype=False)


def test_sizing(lab):
    """Results whose size defies structural intuition. Feeds the 'unexpected
    cache sizing' axis: an Aggregate that doesn't reduce, a join that
    multiplies, strings that balloon, ordering that changes compressed bytes
    for identical logical content, and a near-zero-byte distinct."""
    trips_bytes = (lab.data / "trips.parquet").stat().st_size
    cases = {
        "groups_galore": build(lab.project, groups_galore_code(lab.project)),
        "event_annotate": build(lab.project, event_annotate_code(lab.project)),
        "label_balloon": build(lab.project, label_balloon_code(lab.project)),
        "od_sorted": build(lab.project, od_matrix_code(lab.project, order="name")),
        "od_shuffled": build(lab.project, od_matrix_code(lab.project, order="shuffle")),
        "distinct_tiny": build(lab.project, distinct_tiny_code(lab.project)),
    }
    for name, b in cases.items():
        lab.record[name] = {
            **b,
            "bytes_vs_input": round(b["result_bytes"] / trips_bytes, 3),
            "bytes_per_row": round(b["result_bytes"] / max(b["rows"], 1), 1),
        }
    lab.record["input_bytes"] = trips_bytes
    lab.record["od_order_size_ratio"] = round(
        cases["od_shuffled"]["result_bytes"] / max(cases["od_sorted"]["result_bytes"], 1), 3
    )
    lab.record["final_disk"] = disk(lab.project)

    assert cases["event_annotate"]["rows"] >= 4 * TRIPS_ROWS  # ≥4 events/station
    assert cases["distinct_tiny"]["rows"] <= 3
    assert cases["groups_galore"]["rows"] > TRIPS_ROWS * 0.3  # an Aggregate that barely reduces
    # Identical logical content, different physical order — same rows either way.
    assert cases["od_sorted"]["rows"] == cases["od_shuffled"]["rows"]
