"""Rebuild the `nfl-salaries` catalog from scratch.

The project was built interactively in a chat session: pull QB contract and
season-stats data from nflverse, join them into a per-contract EPA-vs-cost
table, add money-formatted display styling, and chart the result. This
script reproduces that end state directly through the build machinery
(`build_and_persist`) instead of replaying the interactive back-and-forth —
the intermediate revisions were mostly bug fixes (nullif direction, duplicate
contract rows, a datafusion duplicate-expression-name planner error) whose
lessons are captured as comments in the recipes below, not separate steps.

Data sources (nflverse-data GitHub releases, no auth required):
- contracts:     https://github.com/nflverse/nflverse-data/releases/download/contracts/historical_contracts.parquet
- player_stats:  https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_season.parquet

Usage:
    uv run python scripts/rebuild_nfl_salaries_catalog.py [--project nfl-salaries]

By default the project is (re)built under $TALLYMAN_HOME (~/.tallyman-notebooks).
Pass --home to target an isolated tree.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

SOURCES = {
    "nfl_contracts.parquet": (
        "https://github.com/nflverse/nflverse-data/releases/download/contracts/historical_contracts.parquet"
    ),
    "player_stats_season.parquet": (
        "https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_season.parquet"
    ),
}

MONEY_MILLIONS_STYLING = '''\
MONEY_COLS = {"value", "guaranteed", "inflated_value", "inflated_guaranteed"}
# Plain years, not quantities — "2,024" reads as a count, not the year 2024.
NO_GROUPING_COLS = {"year", "season", "year_signed"}


class MoneyMillionsStyling(DefaultMainStyling):
    df_display_name = "main"

    @classmethod
    def style_column(kls, col, column_metadata):
        base_config = super().style_column(col, column_metadata)
        # `col` is buckaroo's internal wire-code (e.g. "g"), not the real
        # column name — the real name lives in column_metadata['orig_col_name'].
        orig_name = column_metadata.get("orig_col_name", col)
        if orig_name in MONEY_COLS:
            base_config["displayer_args"] = {"displayer": "compact_number"}
        elif orig_name in NO_GROUPING_COLS:
            # The 'obj' displayer does a plain val.toString() — no thousands
            # grouping — unlike the 'float'/'integer' displayers, which have
            # no way to turn grouping off.
            base_config["displayer_args"] = {"displayer": "obj"}
        return base_config
'''

STOCK_MAIN_STYLING = '''\
class StockMainStyling(DefaultMainStyling):
    df_display_name = "stock_main"
'''

# The final scatter chart on qb_contracts: EPA/$1M vs signing year, outliers
# clamped to ±45 so a small number of extreme contracts (e.g. Deshaun Watson)
# don't compress the rest of the distribution.
QB_CONTRACTS_CHART_SPEC = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "title": "QB contract value: EPA per $1M vs. signing year (outliers capped at ±45)",
    "width": "container",
    "height": 420,
    "transform": [
        {"filter": "datum.epa_per_value_million != null"},
        {"calculate": "clamp(datum.epa_per_value_million, -45, 45)", "as": "epa_per_value_million_clamped"},
    ],
    "layer": [
        {
            "mark": {"type": "rule", "color": "#999", "strokeDash": [4, 4]},
            "encoding": {"y": {"datum": 0}},
        },
        {
            "mark": {"type": "circle", "size": 70, "opacity": 0.65},
            "encoding": {
                "x": {
                    "field": "year_signed",
                    "type": "quantitative",
                    "title": "Signing year",
                    "scale": {"domain": [1995, 2027]},
                    "axis": {"format": "d", "grid": False},
                },
                "y": {
                    "field": "epa_per_value_million_clamped",
                    "type": "quantitative",
                    "title": "EPA per $1M of contract value",
                    "scale": {"domain": [-45, 45]},
                    "axis": {"values": [-40, -20, 0, 20, 40]},
                },
                "color": {
                    "field": "is_rookie_contract",
                    "type": "nominal",
                    "title": "Contract type",
                    "scale": {"domain": [True, False], "range": ["#f58518", "#4c78a8"]},
                    "legend": {"labelExpr": "datum.label === 'true' ? 'Rookie deal' : 'Veteran deal'"},
                },
                "tooltip": [
                    {"field": "player", "type": "nominal", "title": "Player"},
                    {"field": "year_signed", "type": "quantitative", "title": "Signing year", "format": "d"},
                    {"field": "team", "type": "nominal", "title": "Team"},
                    {"field": "epa_per_value_million", "type": "quantitative", "title": "EPA/$1M (true value)", "format": ".1f"},
                    {"field": "total_epa_during_contract", "type": "quantitative", "title": "Total EPA", "format": ".1f"},
                    {"field": "value", "type": "quantitative", "title": "Contract value ($)", "format": "$,.0f"},
                    {"field": "contract_years", "type": "quantitative", "title": "Contract years"},
                ],
            },
        },
    ],
}


@dataclass(frozen=True)
class Step:
    alias: str
    code: str
    prompt: str


# Build sequence, in dependency order. Each alias is built as a single
# `create` at its final logical state (the code below is exactly what's
# committed in the live project's entries), so `tracked_expr_from_alias(alias)`
# resolves to the intended head for downstream steps.
STEPS: tuple[Step, ...] = (
    Step(
        "contracts",
        "from tallyman_xorq.io import read_project_file\n"
        "expr = read_project_file('nfl_contracts.parquet')\n"
        "expr = expr.mutate(\n"
        "    value=expr.value * 1_000_000,\n"
        "    guaranteed=expr.guaranteed * 1_000_000,\n"
        "    inflated_value=expr.inflated_value * 1_000_000,\n"
        "    inflated_guaranteed=expr.inflated_guaranteed * 1_000_000,\n"
        ")\n",
        "OTC contract data, with value/guaranteed/inflated_* rescaled from "
        "millions to raw dollars so the compact_number displayer can render "
        "them as $X.XM.",
    ),
    Step(
        "player_stats_season",
        "from tallyman_xorq.io import read_project_file\n"
        "expr = read_project_file('player_stats_season.parquet')\n",
        "nflverse per-season player stats, unmodified.",
    ),
    # --- QB-only per-season table: EPA vs. current-contract APY ---
    Step(
        "qb_epa",
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        "import xorq.vendor.ibis as ibis\n"
        "\n"
        "stats = tracked_expr_from_alias(\"player_stats_season\")\n"
        "qb = stats.filter(\n"
        "    (stats.position == \"QB\") & (stats.season_type == \"REG\") & ((stats.attempts + stats.carries) >= 200)\n"
        ")\n"
        "\n"
        "contracts = tracked_expr_from_alias(\"contracts\")\n"
        "# Add sequence number for each contract per player, ordered by year_signed.\n"
        "w_seq = ibis.window(\n"
        "    group_by=contracts.gsis_id,\n"
        "    order_by=ibis.asc(contracts.year_signed),\n"
        ")\n"
        "contracts_with_sequence = contracts.mutate(\n"
        "    contract_sequence=ibis.row_number().over(w_seq)\n"
        ")\n"
        "\n"
        "# Join: for each season, find contracts signed on or before that season.\n"
        "joined = qb.join(\n"
        "    contracts_with_sequence,\n"
        "    (qb.player_id == contracts_with_sequence.gsis_id)\n"
        "    & (contracts_with_sequence.year_signed <= qb.season),\n"
        "    how=\"left\",\n"
        ")\n"
        "\n"
        "# For each (player_id, season), keep the contract with the highest year_signed (most recent).\n"
        "w_most_recent = ibis.window(\n"
        "    group_by=[joined.player_id, joined.season],\n"
        "    order_by=ibis.desc(joined.year_signed),\n"
        ")\n"
        "most_recent_contract = (\n"
        "    joined.mutate(rn=ibis.row_number().over(w_most_recent))\n"
        "    .filter(ibis._.rn == 0)\n"
        ")\n"
        "\n"
        "total_epa = ibis.coalesce(most_recent_contract.passing_epa, 0) + ibis.coalesce(most_recent_contract.rushing_epa, 0)\n"
        "expr = most_recent_contract.select(\n"
        "    player_display_name=most_recent_contract.player_display_name,\n"
        "    season=most_recent_contract.season,\n"
        "    recent_team=most_recent_contract.recent_team,\n"
        "    contract_team=most_recent_contract.team,\n"
        "    year_signed=most_recent_contract.year_signed,\n"
        "    apy=most_recent_contract.apy,\n"
        "    passing_epa=most_recent_contract.passing_epa,\n"
        "    rushing_epa=most_recent_contract.rushing_epa,\n"
        "    games_started=most_recent_contract.games,\n"
        "    position=most_recent_contract.position,\n"
        "    player_id=most_recent_contract.player_id,\n"
        "    contract_sequence=most_recent_contract.contract_sequence,\n"
        "    epa_per_apy_million=total_epa / (most_recent_contract.apy / 1_000_000),\n"
        ").order_by(ibis.desc(\"season\"), ibis.desc(total_epa))\n",
        "QB-only table: season EPA (>= 200 attempts+carries) joined to each player's "
        "most recent contract (signed on or before that season), with epa_per_apy_million "
        "as the bang-for-buck measure.",
    ),
    # --- Per-contract table: whole-deal EPA vs. cost, with the scatter chart ---
    Step(
        "qb_contracts",
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        "import xorq.vendor.ibis as ibis\n"
        "\n"
        "contracts = tracked_expr_from_alias(\"contracts\")\n"
        "qb_contracts_raw = contracts.filter((contracts.position == \"QB\") & (contracts.value > 0))\n"
        "\n"
        "id_w = ibis.window(order_by=[qb_contracts_raw.gsis_id, qb_contracts_raw.year_signed])\n"
        "qb_contracts = qb_contracts_raw.mutate(\n"
        "    contract_id=ibis.row_number().over(id_w),\n"
        "    contract_end_year=qb_contracts_raw.year_signed + qb_contracts_raw.years - 1,\n"
        "    career_year_at_signing=qb_contracts_raw.year_signed - qb_contracts_raw.draft_year + 1,\n"
        "    is_rookie_contract=qb_contracts_raw.year_signed == qb_contracts_raw.draft_year,\n"
        ")\n"
        "\n"
        "stats = tracked_expr_from_alias(\"player_stats_season\")\n"
        "qb_stats = stats.filter((stats.position == \"QB\") & (stats.season_type == \"REG\"))\n"
        "\n"
        "joined = qb_contracts.join(\n"
        "    qb_stats,\n"
        "    (qb_contracts.gsis_id == qb_stats.player_id)\n"
        "    & (qb_stats.season >= qb_contracts.year_signed)\n"
        "    & (qb_stats.season <= qb_contracts.contract_end_year),\n"
        "    how=\"left\",\n"
        ")\n"
        "\n"
        "# Independent sums of the same formula (rather than reusing one summed\n"
        "# column twice downstream) — referencing the same materialized aggregate\n"
        "# column twice in one projection previously broke xorq's datafusion planner\n"
        "# with a duplicate-expression-name error; recomputing avoids that.\n"
        "epa_totals = joined.group_by(joined.contract_id).aggregate(\n"
        "    total_epa_during_contract=(joined.passing_epa + joined.rushing_epa).sum(),\n"
        "    total_epa_during_contract_for_ratio=(joined.passing_epa + joined.rushing_epa).sum(),\n"
        "    seasons_in_data=joined.season.count(),\n"
        "    seasons_in_data_for_ratio=joined.season.count(),\n"
        "    games_during_contract=joined.games.sum(),\n"
        "    games_during_contract_for_ratio=joined.games.sum(),\n"
        "    attempts_during_contract=joined.attempts.sum(),\n"
        ")\n"
        "\n"
        "with_epa = qb_contracts.join(epa_totals, qb_contracts.contract_id == epa_totals.contract_id)\n"
        "\n"
        "expr = with_epa.select(\n"
        "    player=with_epa.player,\n"
        "    epa_per_value_million=with_epa.total_epa_during_contract_for_ratio / (with_epa.value / 1_000_000),\n"
        "    # Money earmarked for the seasons that have already happened (apy * elapsed\n"
        "    # seasons), divided by games actually played — not the FULL contract value,\n"
        "    # which would unfairly punish a healthy player early in a long deal for\n"
        "    # years of money he hasn't been paid or missed yet. apy is in millions\n"
        "    # (unlike value/guaranteed, which are raw dollars), hence the *1e6.\n"
        "    # Null when a QB never played a game under the deal, rather than a\n"
        "    # divide-by-zero.\n"
        "    cost_per_game_played=(with_epa.apy * 1_000_000 * with_epa.seasons_in_data_for_ratio)\n"
        "    / with_epa.games_during_contract_for_ratio.nullif(0),\n"
        "    total_epa_during_contract=with_epa.total_epa_during_contract,\n"
        "    career_year_at_signing=with_epa.career_year_at_signing,\n"
        "    is_rookie_contract=with_epa.is_rookie_contract,\n"
        "    draft_overall=with_epa.draft_overall,\n"
        "    year_signed=with_epa.year_signed,\n"
        "    contract_years=with_epa.years,\n"
        "    team=with_epa.team,\n"
        "    value=with_epa.value,\n"
        "    apy=with_epa.apy,\n"
        "    guaranteed=with_epa.guaranteed,\n"
        "    seasons_in_data=with_epa.seasons_in_data,\n"
        "    games_during_contract=with_epa.games_during_contract,\n"
        "    attempts_during_contract=with_epa.attempts_during_contract,\n"
        "    draft_year=with_epa.draft_year,\n"
        "    college=with_epa.college,\n"
        "    gsis_id=with_epa.gsis_id,\n"
        ").order_by(ibis.desc(\"epa_per_value_million\"))\n",
        "Rate entire QB contracts (not per-season) against EPA production over the "
        "life of the deal: one row per contract, with career_year_at_signing, "
        "is_rookie_contract, and cost_per_game_played (money earmarked for elapsed "
        "seasons / games actually played, not the full contract value).",
    ),
)


def _download_sources(project: str) -> None:
    """Download the two nflverse-data parquet releases into <project>/data/."""
    from tallyman_core.paths import data_dir

    dst = data_dir(project)
    dst.mkdir(parents=True, exist_ok=True)
    for name, url in SOURCES.items():
        target = dst / name
        print(f"  downloading {name} from {url}")
        urllib.request.urlretrieve(url, target)
        print(f"    staged {name}: {target.stat().st_size:,} bytes")


def _write_display_and_chart(project: str) -> None:
    from tallyman_core.charts import set_chart
    from tallyman_core.display_klasses import write_display_klass

    write_display_klass(project, "money_millions_styling", MONEY_MILLIONS_STYLING)
    write_display_klass(project, "stock_main_styling", STOCK_MAIN_STYLING)

    from tallyman_core.aliases import get_alias

    qb_contracts_hash = get_alias(project, "qb_contracts")
    set_chart(project, qb_contracts_hash, QB_CONTRACTS_CHART_SPEC)
    print(f"  chart attached to qb_contracts ({qb_contracts_hash})")


def _verify(project: str) -> None:
    """Assert every alias resolves to a durably-committed entry with a recipe.

    Not xorq's own `Catalog.assert_consistency` — that check treats any repo
    file outside its own entries/aliases bookkeeping as an inconsistency, and
    already fails this way on the live nfl-salaries project today because
    chart_specs/*.vl.json is committed alongside catalog.yaml but unknown to
    it. That's a pre-existing gap between tallyman's chart feature and xorq's
    catalog invariant, not something this rebuild introduces.
    """
    import subprocess

    from tallyman_core.aliases import get_alias, load_aliases
    from tallyman_core.catalog_state import read_tallyman_state
    from tallyman_core.paths import catalog_dir

    cat_dir = catalog_dir(project)

    tracked = subprocess.run(
        ["git", "-C", str(cat_dir), "ls-files", "entries/*.zip"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    tracked_hashes = {Path(p).stem for p in tracked}
    pointers = set(read_tallyman_state(project).get("entry_hashes", []))
    missing = pointers - tracked_hashes
    if missing:
        sys.exit(f"pointers with no durable recipe: {sorted(missing)}")

    for step in STEPS:
        content_hash = get_alias(project, step.alias)
        if content_hash is None:
            sys.exit(f"alias {step.alias!r} did not resolve after rebuild")
        entry_dir = cat_dir / "entries" / content_hash
        for required in ("expr.py", "manifest.json", "schema.json"):
            if not (entry_dir / required).is_file():
                sys.exit(f"entry {content_hash} ({step.alias}) missing {required}")

    aliases = load_aliases(project)
    chart_path = cat_dir / "chart_specs" / f"{aliases['qb_contracts']}.vl.json"
    if not chart_path.is_file():
        sys.exit(f"expected chart spec at {chart_path}")

    print(f"  {len(tracked_hashes)} recipes committed, all pointers durable")
    print(f"  {len(STEPS)} aliases resolve with expr.py/manifest.json/schema.json present")
    print(f"  chart spec present for qb_contracts ({aliases['qb_contracts']})")


def rebuild(project: str) -> None:
    from tallyman_core.aliases import set_alias
    from tallyman_core.catalog_state import checkpoint_catalog, ensure_catalog_repo, genesis
    from tallyman_core.paths import ensure_project, project_dir, set_active_project
    from tallyman_xorq.build import build_and_persist

    pdir = project_dir(project)
    if pdir.exists():
        print(f"removing existing project tree at {pdir}")
        shutil.rmtree(pdir)

    ensure_project(project)
    ensure_catalog_repo(project)
    genesis(project)
    # read_project_file/tracked_expr_from_alias inside recipe code resolve the active
    # project with no explicit arg, and resolve_project() reads the active_project
    # file *before* TALLYMAN_PROJECT. Write the file so resolution is deterministic.
    set_active_project(project)

    print("downloading sources:")
    _download_sources(project)

    print(f"building {len(STEPS)} entries:")
    for i, step in enumerate(STEPS, 1):
        result = build_and_persist(project=project, code=step.code, prompt=step.prompt)
        set_alias(project, step.alias, result.content_hash, expect_exists=False)
        if checkpoint_catalog(project, f"rebuild: create {step.alias}") is None:
            print(f"  !! checkpoint failed for {step.alias} ({result.content_hash}); recipe may not be durable")
        flag = "  !! NOT registered in xorq catalog" if result.catalog_registered is False else ""
        print(
            f"  [{i:>2}/{len(STEPS)}] {step.alias:<20} {result.content_hash} "
            f"({result.row_count:,} rows, {result.execute_seconds:.2f}s){flag}"
        )

    print("writing display styling and chart:")
    _write_display_and_chart(project)
    # chart_specs/ lives inside the catalog git repo (unlike display/, which
    # doesn't); check it in or assert_consistency sees an untracked path.
    if checkpoint_catalog(project, "rebuild: attach qb_contracts chart") is None:
        print("  !! checkpoint failed for chart attachment")

    print("verifying:")
    _verify(project)
    print("done.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", default="nfl-salaries", help="project name to (re)build")
    ap.add_argument("--home", type=Path, default=None, help="override TALLYMAN_HOME (isolated project tree)")
    args = ap.parse_args()

    if args.home:
        os.environ["TALLYMAN_HOME"] = str(args.home)

    rebuild(args.project)


if __name__ == "__main__":
    main()
