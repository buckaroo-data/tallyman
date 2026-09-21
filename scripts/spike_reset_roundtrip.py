"""ADR-007 evidence (D13, D14): a reset back and then forward, played through on today's code.

Three kinds of file sit behind an entry, and ``reset_to`` treats them differently:

- the entry directory: moved to the bullpen, copied back by a forward reset;
- the snapshots under ``compute_cache/``: the same, driven by a git-tracked list of the files that existed;
- the content-addressed clone of the source under ``data/.cas/``: DELETED by ``gc_cas``, not moved.

Step s1 has an entry over ``orders.parquet``. Step s2 adds a cheap entry and a worthy entry over ``extra.parquet``.
The script resets to s1, resets forward to s2, and reads both s2 entries. ``extra.parquet`` is never touched. It then
empties the cache, which is the cold state of ADR-007 D7, and reads the worthy entry again so that it has to heal.

Runs in a scratch ``TALLYMAN_HOME``.

    uv run python scripts/spike_reset_roundtrip.py
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_reset_roundtrip_"))
os.environ["TALLYMAN_HOME"] = str(HOME)
os.environ["XORQ_CACHE_DIR"] = str(HOME / "_global_xorq")  # must be set before xorq is imported

import pandas as pd  # noqa: E402

from tallyman_core import catalog_state as cs  # noqa: E402
from tallyman_core import data_dir, ensure_project, set_active_project  # noqa: E402
from tallyman_core.paths import compute_cache_dir  # noqa: E402
from tallyman_xorq import build_and_persist  # noqa: E402
from tallyman_xorq.result_cache import cached_result_expr  # noqa: E402

PROJECT = "spike"


def recipe(source: str, tail: str = "") -> str:
    return (
        "from tallyman_xorq.io import read_project_file\n"
        f"t = read_project_file({source!r}, project={PROJECT!r})\n"
        f"expr = t{tail}\n"
    )


def clones() -> list[str]:
    cas = data_dir(PROJECT) / ".cas"
    return sorted(p.name[:8] for p in cas.iterdir()) if cas.is_dir() else []


def read(label: str, content_hash: str) -> None:
    cached_result_expr.cache_clear()
    try:
        print(f"   {label}: ok, {len(cached_result_expr(PROJECT, content_hash).execute())} rows")
    except Exception as exc:  # noqa: BLE001 - the spike reports whatever is raised
        print(f"   {label}: raises {type(exc).__name__}: {str(exc)[:80]}")


def main() -> None:
    ensure_project(PROJECT)
    set_active_project(PROJECT)
    cs.ensure_catalog_repo(PROJECT)
    data = data_dir(PROJECT)
    data.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"region": ["a", "b", "a"], "price": [1.0, 2.0, 3.0]}).to_parquet(data / "orders.parquet")
    pd.DataFrame({"k": ["x", "y", "x", "z"], "v": [1.0, 2.0, 3.0, 4.0]}).to_parquet(data / "extra.parquet")

    build_and_persist(PROJECT, recipe("orders.parquet"))
    s1 = cs.checkpoint_catalog(PROJECT, "s1")
    cheap = build_and_persist(PROJECT, recipe("extra.parquet", ".filter(t.v > 1)"))
    worthy = build_and_persist(PROJECT, recipe("extra.parquet", ".group_by('k').aggregate(s=t.v.sum())"))
    s2 = cs.checkpoint_catalog(PROJECT, "s2")

    print(f"at s2, clones: {clones()}")
    read("cheap entry", cheap.content_hash)
    read("worthy entry", worthy.content_hash)

    cs.reset_to(PROJECT, s1)
    print(f"after the reset to s1, clones: {clones()}")
    cs.reset_to(PROJECT, s2)
    print(f"after the reset forward to s2, clones: {clones()}; extra.parquet is unchanged on disk")
    read("cheap entry, which reads the clone on every read", cheap.content_hash)
    read("worthy entry, whose snapshot came back from the bullpen", worthy.content_hash)

    for snapshot in compute_cache_dir(PROJECT).rglob("*.parquet"):
        snapshot.unlink()
    print("with the cache emptied, the worthy entry has to heal from its build:")
    read("worthy entry", worthy.content_hash)


if __name__ == "__main__":
    main()
