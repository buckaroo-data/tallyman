"""Rebuild the canonical `parking_ticket_analysis` catalog from scratch.

The V0 demo catalog was built before the #48/#52 fixes, so 17 of its 18 entry
recipes never reached git (#48) and the durable-recipe invariant is broken. Rather
than repair it, this script rebuilds it from raw sources through the now-fixed
machinery (#48 closed by PR #57, #52 by PR #58), producing a clean catalog whose
recipes are all committed and whose two views (tracked `.zip` set vs
`entry_hashes`) agree.

It doubles as the Stage 2 perf corpus generator (see
`plans/staged-catalog-perf-reactive.md`):

- full corpus    — `--rows 0`     (the real ~10M+ row sources)
- truncated copy — `--rows 1000000` (1M rows; identical DAG, fast execution)

Truncation happens at the raw-source read: each source parquet is copied into the
project's `data/` dir capped at N rows, so the build/tokenize graph is identical
to the full corpus and only execution cost drops.

The recipe sequence below is the alias-tracked structure of the original catalog
(9 aliases, 14 versioned builds across 4 dependency tiers); the three
exploration-orphan entries that were never part of an alias chain are
intentionally not reproduced. Exact content hashes are not expected to match the
original — logical structure (DAG shape, revision count) is what matters.

Usage:
    uv run python scripts/rebuild_parking_catalog.py \
        --src /Users/paddy/code/parking_speeding_data \
        --project parking_ticket_analysis \
        --rows 1000000              # omit or 0 for the full dataset

By default the project is (re)built under $TALLYMAN_HOME (~/.tallyman-notebooks).
Pass --home to target an isolated tree (e.g. a perf-corpus home).
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

# The three raw sources the recipes read via `from_project(...)`.
SOURCES = (
    "camera_select_columns.parquet",
    "camera_violations.parquet",
    "parking_plt_only4.parquet",
)


@dataclass(frozen=True)
class Step:
    alias: str
    kind: str  # "create" or "revise"
    code: str


# The build sequence, in dependency order. Each alias's full create->revise chain
# is built before any dependent reads it, so `from_catalog(alias)` resolves to the
# intended head. Codes are the recipes recovered from the original entry dirs.
STEPS: tuple[Step, ...] = (
    # --- Tier 0: raw sources (from_project) ---
    Step(
        "camera_select_columns",
        "create",
        "from tallyman_xorq.io import from_project\n"
        "expr = from_project('camera_select_columns.parquet')\n",
    ),
    Step(
        "camera_select_columns",
        "revise",
        "from tallyman_xorq.io import from_project\n"
        "import xorq.vendor.ibis as ibis\n"
        "def parse_issue_date(t):\n"
        "    return (\n"
        "        ibis.case()\n"
        "        .when(t.issue_date.contains('T'), t.issue_date.cast('timestamp').date())\n"
        "        .when(t.issue_date.left(2).cast('int64') > 12, t.issue_date.to_date('%d/%m/%Y'))\n"
        "        .else_(t.issue_date.to_date('%m/%d/%Y'))\n"
        "        .end()\n"
        "    )\n"
        "expr = from_project('camera_select_columns.parquet').mutate(issue_date=parse_issue_date)\n",
    ),
    Step(
        "camera_violations",
        "create",
        "from tallyman_xorq.io import from_project\n"
        "expr = from_project('camera_violations.parquet')\n",
    ),
    Step(
        "camera_violations",
        "revise",
        "from tallyman_xorq.io import from_project\n"
        "expr = from_project('camera_violations.parquet').limit(10_000_000)\n",
    ),
    Step(
        "camera_violations",
        "revise",
        "from tallyman_xorq.io import from_project\n"
        "expr = from_project('camera_violations.parquet').limit(5_000_000)\n",
    ),
    Step(
        "parking_plt_only4",
        "create",
        "from tallyman_xorq.io import from_project\n"
        "expr = from_project('parking_plt_only4.parquet')\n",
    ),
    # --- Tier 1: depend on tier-0 aliases ---
    Step(
        "all_violations",
        "create",
        "from tallyman_xorq.io import from_catalog\n"
        "import xorq.vendor.ibis as ibis\n"
        "camera = from_catalog('camera_select_columns').select(\n"
        "    plate='plate', state='state', issue_date='issue_date', violation='violation',\n"
        ")\n"
        "parking = from_catalog('parking_plt_only4').select(\n"
        "    plate='plate_id', state='registration_state', issue_date='issue_date',\n"
        "    violation=ibis.literal('parking'),\n"
        ")\n"
        "expr = ibis.union(camera, parking)\n",
    ),
    Step(
        "camera_violations_by_plate",
        "create",
        "from tallyman_xorq.io import from_catalog\n"
        "t = from_catalog('camera_select_columns')\n"
        "expr = (\n"
        "    t.group_by('plate')\n"
        "    .aggregate(\n"
        "        state=t.state.max(),\n"
        "        license_type=t.license_type.max(),\n"
        "        count=t.plate.count(),\n"
        "    )\n"
        ")\n",
    ),
    # --- Tier 2: depend on all_violations ---
    Step(
        "tickets_per_day",
        "create",
        "from tallyman_xorq.io import from_catalog\n"
        "import xorq.vendor.ibis as ibis\n"
        "t = from_catalog('all_violations')\n"
        "is_parking = t.violation == 'parking'\n"
        "is_speeding = t.violation != 'parking'\n"
        "daily = (\n"
        "    t.group_by('issue_date')\n"
        "    .aggregate(\n"
        "        parking_count=is_parking.cast('int64').sum(),\n"
        "        speeding_count=is_speeding.cast('int64').sum(),\n"
        "    )\n"
        ")\n"
        "speeding_firsts = (\n"
        "    t.filter(is_speeding).group_by('plate').aggregate(first_date=t.issue_date.min())\n"
        ")\n"
        "first_speeding_per_day = (\n"
        "    speeding_firsts.group_by('first_date')\n"
        "    .aggregate(first_time_speeding_count=speeding_firsts.plate.count())\n"
        ")\n"
        "parking_firsts = (\n"
        "    t.filter(is_parking).group_by('plate').aggregate(first_date=t.issue_date.min())\n"
        ")\n"
        "first_parking_per_day = (\n"
        "    parking_firsts.group_by('first_date')\n"
        "    .aggregate(first_time_parking_count=parking_firsts.plate.count())\n"
        ")\n"
        "expr = (\n"
        "    daily\n"
        "    .left_join(first_speeding_per_day, daily.issue_date == first_speeding_per_day.first_date)\n"
        "    .left_join(first_parking_per_day, daily.issue_date == first_parking_per_day.first_date)\n"
        "    .select(\n"
        "        'issue_date', 'parking_count', 'speeding_count',\n"
        "        'first_time_speeding_count', 'first_time_parking_count',\n"
        "    )\n"
        ")\n",
    ),
    Step(
        "tickets_per_day",
        "revise",
        "from tallyman_xorq.io import from_catalog\n"
        "import xorq.vendor.ibis as ibis\n"
        "t = from_catalog('all_violations').filter(lambda r: r.issue_date.year() > 2013)\n"
        "is_parking = t.violation == 'parking'\n"
        "is_speeding = t.violation != 'parking'\n"
        "daily = (\n"
        "    t.group_by('issue_date')\n"
        "    .aggregate(\n"
        "        parking_count=is_parking.cast('int64').sum(),\n"
        "        speeding_count=is_speeding.cast('int64').sum(),\n"
        "    )\n"
        ")\n"
        "speeding_firsts = (\n"
        "    t.filter(is_speeding).group_by('plate').aggregate(first_date=t.issue_date.min())\n"
        ")\n"
        "first_speeding_per_day = (\n"
        "    speeding_firsts.group_by('first_date')\n"
        "    .aggregate(first_time_speeding_count=speeding_firsts.plate.count())\n"
        ")\n"
        "parking_firsts = (\n"
        "    t.filter(is_parking).group_by('plate').aggregate(first_date=t.issue_date.min())\n"
        ")\n"
        "first_parking_per_day = (\n"
        "    parking_firsts.group_by('first_date')\n"
        "    .aggregate(first_time_parking_count=parking_firsts.plate.count())\n"
        ")\n"
        "expr = (\n"
        "    daily\n"
        "    .left_join(first_speeding_per_day, daily.issue_date == first_speeding_per_day.first_date)\n"
        "    .left_join(first_parking_per_day, daily.issue_date == first_parking_per_day.first_date)\n"
        "    .select(\n"
        "        'issue_date', 'parking_count', 'speeding_count',\n"
        "        'first_time_speeding_count', 'first_time_parking_count',\n"
        "    )\n"
        ")\n",
    ),
    Step(
        "habitual_speeder_stats",
        "create",
        "from tallyman_xorq.io import from_catalog\n"
        "import xorq.vendor.ibis as ibis\n"
        "t = from_catalog('all_violations')\n"
        "is_parking = t.violation == 'parking'\n"
        "is_speeding = t.violation != 'parking'\n"
        "parking_date = ibis.case().when(is_parking, t.issue_date).else_(ibis.null()).end()\n"
        "speeding_date = ibis.case().when(is_speeding, t.issue_date).else_(ibis.null()).end()\n"
        "expr = (\n"
        "    t.group_by('plate')\n"
        "    .aggregate(\n"
        "        parking_count=is_parking.cast('int64').sum(),\n"
        "        speeding_count=is_speeding.cast('int64').sum(),\n"
        "        first_ticket=t.issue_date.min(),\n"
        "        last_ticket=t.issue_date.max(),\n"
        "        last_parking_date=parking_date.max(),\n"
        "        last_speeding_date=speeding_date.max(),\n"
        "    )\n"
        "    .filter(lambda r: (r.parking_count + r.speeding_count) >= 5)\n"
        ")\n",
    ),
    Step(
        "habitual_speeder_stats",
        "revise",
        "from tallyman_xorq.io import from_catalog\n"
        "import xorq.vendor.ibis as ibis\n"
        "t = from_catalog('all_violations')\n"
        "is_parking = t.violation == 'parking'\n"
        "is_speeding = t.violation != 'parking'\n"
        "parking_date = ibis.case().when(is_parking, t.issue_date).else_(ibis.null()).end()\n"
        "speeding_date = ibis.case().when(is_speeding, t.issue_date).else_(ibis.null()).end()\n"
        "agg = (\n"
        "    t.group_by('plate')\n"
        "    .aggregate(\n"
        "        parking_count=is_parking.cast('int64').sum(),\n"
        "        speeding_count=is_speeding.cast('int64').sum(),\n"
        "        first_ticket=t.issue_date.min(),\n"
        "        last_ticket=t.issue_date.max(),\n"
        "        last_parking_date=parking_date.max(),\n"
        "        last_speeding_date=speeding_date.max(),\n"
        "    )\n"
        "    .filter(lambda r: (r.parking_count + r.speeding_count) >= 5)\n"
        ")\n"
        "expr = agg.mutate(\n"
        "    active_days=agg.last_ticket.delta(agg.first_ticket, 'day'),\n"
        "    last_speeding_to_last_parking=agg.last_parking_date.delta(agg.last_speeding_date, 'day'),\n"
        ").drop('first_ticket', 'last_ticket', 'last_parking_date')\n",
    ),
    # --- Tier 3: depends on habitual_speeder_stats ---
    Step(
        "probable_ghost_plate",
        "create",
        "from tallyman_xorq.io import from_catalog\n"
        "t = from_catalog('habitual_speeder_stats')\n"
        "filtered = t.filter(\n"
        "    (t.speeding_count >= 5) & (t.last_speeding_to_last_parking > 365 * 3)\n"
        ")\n"
        "expr = filtered.group_by('last_speeding_date').aggregate(\n"
        "    count=filtered.plate.count(),\n"
        "    mean_speeding_tickets=filtered.speeding_count.mean(),\n"
        "    median_speeding_tickets=filtered.speeding_count.approx_median(),\n"
        ")\n",
    ),
    # --- Tier 4: depends on tickets_per_day + probable_ghost_plate ---
    Step(
        "tickets_and_ghosts",
        "create",
        "from tallyman_xorq.io import from_catalog\n"
        "tpd = from_catalog('tickets_per_day')\n"
        "pgp = (\n"
        "    from_catalog('probable_ghost_plate')\n"
        "    .filter(lambda r: r.last_speeding_date.year() > 2013)\n"
        "    .rename(issue_date='last_speeding_date', ghost_plate_count='count')\n"
        "    .select('issue_date', 'ghost_plate_count')\n"
        ")\n"
        "expr = (\n"
        "    tpd\n"
        "    .left_join(pgp, 'issue_date')\n"
        "    .select(\n"
        "        'issue_date', 'parking_count', 'speeding_count',\n"
        "        'first_time_speeding_count', 'first_time_parking_count', 'ghost_plate_count',\n"
        "    )\n"
        ")\n",
    ),
)


def _stage_sources(src_dir: Path, project: str, rows: int) -> None:
    """Place the raw sources into <project>/data/, truncated to `rows` if > 0."""
    from tallyman_core.paths import data_dir

    dst = data_dir(project)
    dst.mkdir(parents=True, exist_ok=True)
    for name in SOURCES:
        src = src_dir / name
        if not src.is_file():
            sys.exit(f"source not found: {src}")
        target = dst / name
        if rows and rows > 0:
            import pyarrow.parquet as pq

            table = pq.read_table(src)
            pq.write_table(table.slice(0, rows), target)
            print(f"  staged {name}: {min(rows, table.num_rows):,} rows (truncated)")
        else:
            shutil.copy2(src, target)
            print(f"  staged {name}: full copy")


def _verify(project: str) -> None:
    """Assert the rebuilt catalog is clean: opens, and every pointer is durable."""
    from xorq.catalog.catalog import Catalog

    from tallyman_core.catalog_state import _read_raw
    from tallyman_core.paths import catalog_dir

    cat_dir = catalog_dir(project)
    Catalog.from_kwargs(path=str(cat_dir), init=False)  # raises if inconsistent

    import subprocess

    tracked = subprocess.run(
        ["git", "-C", str(cat_dir), "ls-files", "entries/*.zip"],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    tracked_hashes = {Path(p).stem for p in tracked}
    pointers = set(_read_raw(project).get("entry_hashes", []))
    missing = pointers - tracked_hashes
    if missing:
        sys.exit(f"pointers with no durable recipe: {sorted(missing)}")
    print(f"  catalog opens; {len(tracked_hashes)} recipes committed, all pointers durable")


def rebuild(src_dir: Path, project: str, rows: int) -> None:
    from tallyman_core.aliases import set_alias
    from tallyman_core.catalog_state import checkpoint_catalog, ensure_catalog_repo, genesis
    from tallyman_core.paths import ensure_project, project_dir
    from tallyman_xorq.build import build_and_persist

    pdir = project_dir(project)
    if pdir.exists():
        print(f"removing existing project tree at {pdir}")
        shutil.rmtree(pdir)

    ensure_project(project)
    ensure_catalog_repo(project)
    genesis(project)
    # from_project/from_catalog inside recipe code resolve the active project.
    os.environ["TALLYMAN_PROJECT"] = project

    print(f"staging sources (rows={'full' if not rows else rows}):")
    _stage_sources(src_dir, project, rows)

    print(f"building {len(STEPS)} entries:")
    for i, step in enumerate(STEPS, 1):
        result = build_and_persist(project=project, code=step.code, prompt=f"rebuild: {step.alias}")
        set_alias(project, step.alias, result.content_hash, expect_exists=(step.kind == "revise"))
        checkpoint_catalog(project, f"rebuild: {step.kind} {step.alias}")
        flag = "" if result.catalog_registered else "  !! NOT registered in xorq catalog"
        print(f"  [{i:>2}/{len(STEPS)}] {step.kind:<6} {step.alias:<28} {result.content_hash} "
              f"({result.row_count:,} rows, {result.execute_seconds:.2f}s){flag}")

    print("verifying:")
    _verify(project)
    print("done.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", type=Path, default=Path("/Users/paddy/code/parking_speeding_data"),
                    help="directory holding the raw source parquets")
    ap.add_argument("--project", default="parking_ticket_analysis", help="project name to (re)build")
    ap.add_argument("--rows", type=int, default=0,
                    help="truncate each source to N rows (0 = full dataset)")
    ap.add_argument("--home", type=Path, default=None,
                    help="override TALLYMAN_HOME (isolated project tree)")
    args = ap.parse_args()

    if args.home:
        os.environ["TALLYMAN_HOME"] = str(args.home)

    rebuild(args.src, args.project, args.rows)


if __name__ == "__main__":
    main()
