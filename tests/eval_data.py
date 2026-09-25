"""Seeded data for the eval scenarios: NFL contracts and season stats shaped like the nflverse files the recorded
sessions used, plus the shoe-orders fixture and a CSV.

The NFL frames carry the real files' column names and types (``nfl_contracts.parquet`` and
``player_stats_season.parquet`` from nflverse), so recipes copied verbatim from past sessions run unchanged. They
also carry the data shapes those sessions tripped over:

- a null ``gsis_id`` on a few contracts (the grain probe in 0fcac6bb found one NULL-keyed "duplicate");
- a second, spurious ``is_active`` contract at a fraction of the value for about 10% of QBs (why 63721a56's
  ``is_active == True`` join fanned out and f3a97dc8 switched to ``max(value)``);
- several contracts signed by one player in one year, so a ``row_number`` over ``(gsis_id, year_signed)`` has ties;
- ``contract_history`` as ``list<struct>``, with practice-squad transactions to filter out (f3a97dc8 #51);
- null EPA values and float EPA sums (the float-aggregate digest cases of ADR-009).

Set ``EVAL_NFL_DIR`` to a directory holding the real two parquet files to run the scenarios on them instead (the
real contracts file is 11 MB, above the ~10 MB where unsorted paging stopped being repeatable, B1 in the eval
notes). ``EVAL_SCALE`` multiplies the synthetic row counts.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

TEAMS = [
    "ARI",
    "ATL",
    "BAL",
    "BUF",
    "CAR",
    "CHI",
    "CIN",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GB",
    "HOU",
    "IND",
    "JAX",
    "KC",
    "LV",
    "LAC",
    "LA",
    "MIA",
    "MIN",
    "NE",
    "NO",
    "NYG",
    "NYJ",
    "PHI",
    "PIT",
    "SF",
    "SEA",
    "TB",
    "TEN",
    "WAS",
]
POSITIONS = ["QB", "RB", "WR", "TE", "OL", "DL", "LB", "CB", "S", "K"]
POSITION_WEIGHTS = [0.14, 0.1, 0.16, 0.08, 0.16, 0.12, 0.08, 0.08, 0.06, 0.02]
CONTRACT_TYPES = ["Veteran", "Rookie", "Extension", "Practice", "Futures"]

_HISTORY_TYPE = pa.list_(
    pa.struct(
        [
            ("team", pa.string()),
            ("contract_type", pa.string()),
            ("status", pa.string()),
            ("year_signed", pa.int32()),
            ("yrs", pa.int32()),
            ("total", pa.float64()),
            ("apy", pa.float64()),
            ("guarantees", pa.float64()),
        ]
    )
)
_SEASON_HISTORY_TYPE = pa.list_(
    pa.struct(
        [
            ("year", pa.string()),
            ("team", pa.string()),
            ("base_salary", pa.float64()),
            ("cap_number", pa.float64()),
        ]
    )
)


def scale() -> float:
    return float(os.environ.get("EVAL_SCALE", "1"))


def real_nfl_dir() -> Path | None:
    d = os.environ.get("EVAL_NFL_DIR")
    return Path(d).expanduser() if d else None


def _players(rng: np.random.Generator, n: int) -> list[dict]:
    first = [
        "Pat",
        "Josh",
        "Lamar",
        "Joe",
        "Tua",
        "Kyler",
        "Dak",
        "Jalen",
        "Aaron",
        "Justin",
        "Trevor",
        "Bo",
        "Brock",
        "Sam",
        "Baker",
        "Geno",
        "Kirk",
        "Derek",
        "Jared",
        "Russell",
    ]
    last = [
        "Mahomes",
        "Allen",
        "Jackson",
        "Burrow",
        "Tagovailoa",
        "Murray",
        "Prescott",
        "Hurts",
        "Rodgers",
        "Herbert",
        "Lawrence",
        "Nix",
        "Purdy",
        "Darnold",
        "Mayfield",
        "Smith",
        "Cousins",
        "Carr",
        "Goff",
        "Wilson",
        "Brown",
        "Watson",
        "Love",
        "Stroud",
        "Young",
        "Williams",
        "Daniels",
        "Maye",
        "Levis",
        "Fields",
    ]
    out = []
    for i in range(n):
        draft_year = int(rng.integers(1998, 2025))
        out.append(
            {
                "player": f"{first[i % len(first)]} {last[(i * 7) % len(last)]}{'' if i < 600 else f' {i}'}",
                "position": str(rng.choice(POSITIONS, p=POSITION_WEIGHTS)),
                "gsis_id": None if rng.random() < 0.02 else f"00-00{30000 + i:05d}",
                "otc_id": 1000 + i,
                "draft_year": draft_year,
                "draft_round": int(rng.integers(1, 8)),
                "draft_overall": int(rng.integers(1, 260)),
                "draft_team": str(rng.choice(TEAMS)),
                "college": str(
                    rng.choice(["Alabama", "Ohio State", "LSU", "Oklahoma", "USC", "Wyoming", "Texas Tech"])
                ),
                "height": f"6'{int(rng.integers(0, 6))}\"",
                "weight": str(int(rng.integers(180, 330))),
                "date_of_birth": f"{draft_year - 22}-0{int(rng.integers(1, 9))}-1{int(rng.integers(0, 9))}",
            }
        )
    return out


def contracts_table(seed: int = 7) -> pa.Table:
    rng = np.random.default_rng(seed)
    players = _players(rng, int(600 * scale()))
    rows = []
    for p in players:
        n = int(rng.integers(1, 6))
        year = p["draft_year"]
        history = []
        for k in range(n):
            practice = k == 0 and rng.random() < 0.15
            years = 1 if practice else int(rng.integers(1, 8))
            is_qb = p["position"] == "QB"
            value = 0.0 if rng.random() < 0.02 else round(float(rng.gamma(2.0, 30.0 if is_qb else 8.0)), 3)
            if practice:
                value = round(float(rng.uniform(0.1, 0.9)), 3)
            ctype = "Practice" if practice else ("Rookie" if k == 0 else str(rng.choice(CONTRACT_TYPES[:3])))
            history.append(
                {
                    "team": str(rng.choice(TEAMS)),
                    "contract_type": ctype,
                    "status": "signed",
                    "year_signed": year,
                    "yrs": years,
                    "total": value,
                    "apy": round(value / years, 3),
                    "guarantees": round(value * float(rng.uniform(0.1, 0.9)), 3),
                }
            )
            if rng.random() > 0.2:  # a second deal in the same year now and then (row_number ties)
                year = min(year + int(rng.integers(1, 5)), 2026)
        for k, h in enumerate(history):
            rows.append(
                {
                    **p,
                    "team": h["team"],
                    "is_active": k == len(history) - 1,
                    "year_signed": h["year_signed"],
                    "years": h["yrs"],
                    "value": h["total"],
                    "apy": h["apy"],
                    "guaranteed": h["guarantees"],
                    "contract_history": history,
                }
            )
        if p["position"] == "QB" and rng.random() < 0.10:  # the spurious low-value active contract
            h = history[-1]
            rows.append(
                {
                    **p,
                    "team": str(rng.choice(TEAMS)),
                    "is_active": True,
                    "year_signed": h["year_signed"],
                    "years": 1,
                    "value": round(h["total"] * 0.05, 3),
                    "apy": round(h["total"] * 0.05, 3),
                    "guaranteed": 0.0,
                    "contract_history": history,
                }
            )
    cols = {
        "player": pa.array([r["player"] for r in rows], pa.string()),
        "position": pa.array([r["position"] for r in rows], pa.string()),
        "team": pa.array([r["team"] for r in rows], pa.string()),
        "is_active": pa.array([r["is_active"] for r in rows], pa.bool_()),
        "year_signed": pa.array([r["year_signed"] for r in rows], pa.int32()),
        "years": pa.array([r["years"] for r in rows], pa.int32()),
        "value": pa.array([r["value"] for r in rows], pa.float64()),
        "apy": pa.array([r["apy"] for r in rows], pa.float64()),
        "guaranteed": pa.array([r["guaranteed"] for r in rows], pa.float64()),
        "apy_cap_pct": pa.array([round(r["apy"] / 250.0, 4) for r in rows], pa.float64()),
        "inflated_value": pa.array([round(r["value"] * 1.12, 3) for r in rows], pa.float64()),
        "inflated_apy": pa.array([round(r["apy"] * 1.12, 3) for r in rows], pa.float64()),
        "inflated_guaranteed": pa.array([round(r["guaranteed"] * 1.12, 3) for r in rows], pa.float64()),
        "player_page": pa.array([f"https://example.invalid/p/{r['otc_id']}" for r in rows], pa.string()),
        "otc_id": pa.array([r["otc_id"] for r in rows], pa.int32()),
        "gsis_id": pa.array([r["gsis_id"] for r in rows], pa.string()),
        "height": pa.array([r["height"] for r in rows], pa.string()),
        "weight": pa.array([r["weight"] for r in rows], pa.string()),
        "college": pa.array([r["college"] for r in rows], pa.string()),
        "draft_year": pa.array([r["draft_year"] for r in rows], pa.int32()),
        "draft_round": pa.array([r["draft_round"] for r in rows], pa.int32()),
        "draft_overall": pa.array([r["draft_overall"] for r in rows], pa.int32()),
        "draft_team": pa.array([r["draft_team"] for r in rows], pa.string()),
        "date_of_birth": pa.array([r["date_of_birth"] for r in rows], pa.string()),
        "season_history": pa.array(
            [
                [
                    {"year": str(h["year_signed"]), "team": h["team"], "base_salary": h["apy"], "cap_number": h["apy"]}
                    for h in r["contract_history"][:2]
                ]
                for r in rows
            ],
            _SEASON_HISTORY_TYPE,
        ),
        "contract_history": pa.array([r["contract_history"] for r in rows], _HISTORY_TYPE),
    }
    return pa.table(cols)


def player_stats_table(contracts: pa.Table, seed: int = 11) -> pa.Table:
    rng = np.random.default_rng(seed)
    players = {}
    for gid, name, pos, draft in zip(
        contracts["gsis_id"].to_pylist(),
        contracts["player"].to_pylist(),
        contracts["position"].to_pylist(),
        contracts["draft_year"].to_pylist(),
    ):
        if gid is not None:
            players.setdefault(gid, (name, pos, draft))
    rows = []
    for gid, (name, pos, draft) in players.items():
        first = max(draft, 1999)
        for season in range(first, min(first + int(rng.integers(1, 12)), 2026)):
            for season_type in ("REG", "POST") if rng.random() < 0.3 else ("REG",):
                qb = pos == "QB"
                attempts = int(rng.integers(0, 650)) if qb else int(rng.integers(0, 3))
                carries = int(rng.integers(0, 90)) if qb else int(rng.integers(0, 250) if pos == "RB" else 0)
                rows.append(
                    {
                        "season": season,
                        "season_type": season_type,
                        "player_id": gid,
                        "player_name": name.split()[0][0] + "." + name.split()[-1],
                        "player_display_name": name,
                        "position": pos,
                        "position_group": pos,
                        "games": int(rng.integers(1, 18)),
                        "recent_team": str(rng.choice(TEAMS)),
                        "completions": int(attempts * rng.uniform(0.5, 0.7)),
                        "attempts": attempts,
                        "passing_yards": float(attempts * rng.uniform(5, 9)),
                        "passing_tds": int(attempts / 25),
                        "interceptions": float(int(attempts / 45)),
                        "carries": carries,
                        "rushing_yards": float(carries * rng.uniform(2, 6)),
                        # ~5% nulls, and non-terminating binary fractions, so float sums depend on summation order
                        "passing_epa": None if rng.random() < 0.05 else float(rng.normal(0.05, 0.2) * attempts / 3),
                        "rushing_epa": None if rng.random() < 0.05 else float(rng.normal(0.0, 0.3) * carries / 3),
                    }
                )
    ints = {"season", "games", "completions", "attempts", "passing_tds", "carries"}
    names = list(rows[0])
    return pa.table({n: pa.array([r[n] for r in rows], pa.int32() if n in ints else None) for n in names})


def write_nfl(
    data_dir: Path, *, contracts_name: str = "nfl_contracts.parquet", stats_name: str = "player_stats_season.parquet"
) -> dict[str, Path]:
    """Put the two NFL files in a project's data dir: the real ones when ``EVAL_NFL_DIR`` is set, else synthetic."""
    data_dir.mkdir(parents=True, exist_ok=True)
    out = {"contracts": data_dir / contracts_name, "stats": data_dir / stats_name}
    real = real_nfl_dir()
    if real is not None:
        shutil.copyfile(real / "nfl_contracts.parquet", out["contracts"])
        shutil.copyfile(real / "player_stats_season.parquet", out["stats"])
        return out
    contracts = contracts_table()
    pq.write_table(contracts, out["contracts"])
    pq.write_table(player_stats_table(contracts), out["stats"])
    return out


def write_orders(data_dir: Path, name: str = "orders.parquet", *, n_rows: int = 2000, seed: int = 0) -> Path:
    from tallyman_cli.fixtures import write_shoe_orders

    return write_shoe_orders(data_dir / name, n_rows=int(n_rows * scale()), seed=seed)


def write_returns_csv(data_dir: Path, name: str = "returns.csv", *, n_rows: int = 600, seed: int = 3) -> Path:
    """Returns against the shoe orders, as a CSV (the ``tallyman_read_csv`` path and its ordered copy)."""
    import pandas as pd

    rng = np.random.default_rng(seed)
    n = int(n_rows * scale())
    df = pd.DataFrame(
        {
            "order_id": rng.integers(1, 2000, n),
            "reason": rng.choice(["size", "damaged", "late", "changed mind"], n),
            "refund": np.round(rng.uniform(5, 220, n), 2),
        }
    )
    p = data_dir / name
    df.to_csv(p, index=False)
    return p
