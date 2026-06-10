"""Synthetic bikeshare-shaped data for the cache lab (see test_cache_lab.py).

Deterministic numpy generators producing citibike-like tables at a row count
chosen by the caller, so the same scenarios run as a fast smoke (20k trips) or
at bug-reproducing scale (2.5M trips). Distributions are deliberately skewed:

- station popularity is zipf-ish, so self-joins on station blow up
  quadratically at the head — the "expensive to execute" scenarios depend
  on that skew;
- trip durations are lognormal with a 0.2% "forgot to dock" tail (multi-hour
  rides), so outlier-clamping edits actually change results;
- end stations are biased toward the start neighborhood, so neighborhood
  joins are non-degenerate.

Timestamps are pinned to microsecond precision (datetime64[us]) so user-code
epoch arithmetic (`cast("int64")`) has a known unit across writers/readers.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

T0 = pd.Timestamp("2025-09-01")
DAYS = 30
N_STATIONS = 600
N_NEIGHBORHOODS = 40

_STREETS = [f"{n} St" for n in range(1, 61)]
_AVENUES = [
    "1 Ave",
    "2 Ave",
    "3 Ave",
    "Lexington Ave",
    "Park Ave",
    "Madison Ave",
    "5 Ave",
    "Broadway",
    "8 Ave",
    "9 Ave",
]
_EVENT_TYPES = ["inspection", "rebalance", "repair", "dock_swap"]


def station_frame(rng: np.random.Generator, n_stations: int = N_STATIONS) -> pd.DataFrame:
    idx = np.arange(n_stations)
    nb = idx % N_NEIGHBORHOODS
    # Cluster coordinates by neighborhood with per-station jitter.
    nb_lat = 40.65 + (rng.random(N_NEIGHBORHOODS) * 0.25)
    nb_lng = -74.05 + (rng.random(N_NEIGHBORHOODS) * 0.25)
    return pd.DataFrame(
        {
            "station_id": (3100 + idx).astype("int64"),
            "station_name": [f"{_STREETS[i % len(_STREETS)]} & {_AVENUES[(i * 7) % len(_AVENUES)]} #{i}" for i in idx],
            "neighborhood": [f"nb_{n:02d}" for n in nb],
            "lat": nb_lat[nb] + rng.normal(0, 0.004, n_stations),
            "lng": nb_lng[nb] + rng.normal(0, 0.004, n_stations),
            "capacity": rng.integers(15, 61, n_stations).astype("int64"),
        }
    )


def _popularity(n_stations: int) -> np.ndarray:
    w = (1.0 + np.arange(n_stations)) ** -0.8
    return w / w.sum()


def trips_frame(n: int, stations: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n_st = len(stations)
    pop = _popularity(n_st)
    start = rng.choice(n_st, size=n, p=pop)

    # End station: 55% biased to the start's neighborhood, else popularity draw.
    end = rng.choice(n_st, size=n, p=pop)
    same_nb = rng.random(n) < 0.55
    nb = stations["neighborhood"].to_numpy()
    members_by_nb = {name: np.flatnonzero(nb == name) for name in np.unique(nb)}
    for name, members in members_by_nb.items():
        mask = same_nb & (nb[start] == name)
        end[mask] = rng.choice(members, size=int(mask.sum()))

    day = rng.integers(0, DAYS, n)
    hour_w = np.array([1, 1, 1, 1, 2, 4, 8, 14, 18, 10, 7, 8, 9, 9, 8, 9, 12, 17, 16, 10, 7, 5, 3, 2], dtype=float)
    hour = rng.choice(24, size=n, p=hour_w / hour_w.sum())
    sec = rng.integers(0, 3600, n)
    started = T0.value // 1000 + (day * 86_400 + hour * 3_600 + sec) * 1_000_000  # microseconds

    dur_s = np.exp(rng.normal(np.log(700), 0.75, n)).clip(60, 21_600)
    forgot = rng.random(n) < 0.002  # multi-hour "forgot to dock" tail
    dur_s[forgot] *= 20
    ended = started + (dur_s * 1_000_000).astype("int64")

    st = stations.iloc[start].reset_index(drop=True)
    en = stations.iloc[end].reset_index(drop=True)
    return pd.DataFrame(
        {
            "ride_id": [f"{x:016X}" for x in rng.integers(0, 2**63, n)],
            "rideable_type": rng.choice(["classic_bike", "electric_bike", "docked_bike"], size=n, p=[0.60, 0.38, 0.02]),
            "started_at": pd.to_datetime(started, unit="us").astype("datetime64[us]"),
            "ended_at": pd.to_datetime(ended, unit="us").astype("datetime64[us]"),
            "start_station_id": st["station_id"].to_numpy(),
            "start_station_name": st["station_name"].to_numpy(),
            "end_station_id": en["station_id"].to_numpy(),
            "end_station_name": en["station_name"].to_numpy(),
            "start_lat": st["lat"].to_numpy() + rng.normal(0, 1e-4, n),
            "start_lng": st["lng"].to_numpy() + rng.normal(0, 1e-4, n),
            "end_lat": en["lat"].to_numpy() + rng.normal(0, 1e-4, n),
            "end_lng": en["lng"].to_numpy() + rng.normal(0, 1e-4, n),
            "member_casual": rng.choice(["member", "casual"], size=n, p=[0.74, 0.26]),
        }
    )


def events_frame(stations: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Maintenance events, several per station — joining trips to this multiplies rows."""
    rows = []
    for sid in stations["station_id"]:
        for _ in range(int(rng.integers(4, 13))):
            rows.append(
                {
                    "station_id": int(sid),
                    "event_type": _EVENT_TYPES[int(rng.integers(0, len(_EVENT_TYPES)))],
                    "event_day": int(rng.integers(0, DAYS)),
                    "crew": f"crew_{int(rng.integers(0, 12)):02d}",
                }
            )
    df = pd.DataFrame(rows)
    df["event_date"] = (T0 + pd.to_timedelta(df.pop("event_day"), unit="D")).astype("datetime64[us]")
    return df


def weather_frame(rng: np.random.Generator) -> pd.DataFrame:
    """One row per hour over the trip window; join key is the truncated hour."""
    hours = DAYS * 24
    h = np.arange(hours)
    temp = 18 + 6 * np.sin((h % 24 - 14) / 24 * 2 * np.pi) + rng.normal(0, 1.5, hours)
    precip = np.where(rng.random(hours) < 0.8, 0.0, rng.exponential(2.0, hours))
    return pd.DataFrame(
        {
            "obs_hour": (T0 + pd.to_timedelta(h, unit="h")).astype("datetime64[us]"),
            "temp_c": temp.round(1),
            "precip_mm": precip.round(2),
            "wind_kph": (8 + rng.exponential(6, hours)).round(1),
        }
    )


def capacity_frame(stations: pd.DataFrame) -> pd.DataFrame:
    """Fixed-width-only table for the stat-preserving-swap scenario."""
    return stations[["station_id", "capacity"]].astype("int64")


def write_plain_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write with no compression / dictionary / statistics.

    For tables of fixed-width columns this makes two files with the same
    shape but different values byte-length-identical, which is what the
    stat-preserving content-swap scenario needs to fool an
    (mtime, size, inode) identity.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="NONE", use_dictionary=False, write_statistics=False)
