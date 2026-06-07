from __future__ import annotations

import random
from pathlib import Path

import pandas as pd

REGIONS = ["NE", "MW", "S", "W"]
CATEGORIES = ["sneakers", "boots", "sandals", "running", "dress"]


def write_shoe_orders(path: Path, n_rows: int = 2000, seed: int = 42) -> Path:
    rng = random.Random(seed)
    rows = []
    for i in range(n_rows):
        cat = rng.choice(CATEGORIES)
        rows.append(
            {
                "order_id": i + 1,
                "region": rng.choice(REGIONS),
                "category": cat,
                "price": round(rng.uniform(20.0, 220.0), 2),
                "qty": rng.randint(1, 4),
            }
        )
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    return path
