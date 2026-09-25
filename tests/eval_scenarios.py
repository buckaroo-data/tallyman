"""Eval scenarios: multi-step tallyman sessions replayed from recorded ones, with the world disturbed in between.

Each scenario is a function of a ``Session`` (tests/eval_harness.py). Its steps are the MCP calls a recorded Claude
session made, with the recipe code copied verbatim and the user's prompt as the ``prompt`` argument, interleaved
with what the user did around them: opening entries (the Buckaroo grid), a reset to an earlier step in the middle,
editing a source file, emptying the cache, restarting the companion. The oracle checks the project after each
step; every scenario ends with ``Session.finish()`` (restart, warm sweep, cold sweep, verify, fresh process).

Origins (Claude Code transcripts under ``~/.claude/projects/``):

- ``f3a97dc8`` (-Users-paddy-tallyman): the nfl-salaries session. 51 MCP calls, 19 errors: contracts, money
  styling, the QB EPA table and its revisions, the planner's duplicate-name error, qb_contracts, the chart.
- ``63721a56`` and ``0fcac6bb`` (-Users-paddy-code-tallyman-nfl-demo): the scripted demo prompt, a first encounter
  in a fresh project (files not in data/ yet, grain probes, the post-processing sandbox, the display prompt).
- ``6180b849`` (-Users-paddy-tallyman): a revise of contracts whose recalc left qb_epa ``noop`` while stale.
- ``96f1817c`` (-Users-paddy-tallyman): "stalled loading on /diff/contracts/1/2, then 'already borrowed'".
- The adversarial reviews of the cache redesign (8c95f33c, c4906858, 10c2da22, 675b2a67 and the PR #189 review in
  memory): 3-way joins, sorts that are not the last step, tied sorts under a limit, unnest and windows classed
  cheap, raw parquet reads, the reset roundtrip that lost a clone, the pin lost across a reset, and polars
  changing source column types in the ordered copy.

``known`` maps a finding code to the open issue that explains it; the test reports those as known and does not fail
on them, and says so when a known one stops reproducing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from tests.eval_data import write_nfl, write_orders, write_returns_csv
from tests.eval_harness import Session


@dataclass
class Scenario:
    name: str
    fn: Callable[[Session], None]
    origin: str
    summary: str
    options: dict = field(default_factory=dict)  # Session keyword arguments (auto_recalc, sweep, ...)
    known: dict[str, str] = field(default_factory=dict)


SCENARIOS: dict[str, Scenario] = {}


def scenario(*, origin: str, summary: str, known: dict[str, str] | None = None, **options):
    def register(fn):
        SCENARIOS[fn.__name__] = Scenario(fn.__name__, fn, origin, summary, options, known or {})
        return fn

    return register


def _data_dir(s: Session):
    from tallyman_core import data_dir

    return data_dir(s.project)


# ---------------------------------------------------------------------------
# Recipes, verbatim from the recorded sessions (session id and call number in each comment)
# ---------------------------------------------------------------------------

# f3a97dc8 #5 — "Value, Guaranteed, inflated_value, inflated guaranteed are numeric millions columns"
CONTRACTS_RESCALE = """from tallyman_xorq.io import read_project_file
expr = read_project_file('nfl_contracts.parquet')
expr = expr.mutate(
    value=expr.value * 1_000_000,
    guaranteed=expr.guaranteed * 1_000_000,
    inflated_value=expr.inflated_value * 1_000_000,
    inflated_guaranteed=expr.inflated_guaranteed * 1_000_000,
)
"""

# A second rescale, the same shape, so an alias with followers advances again late in a scenario.
CONTRACTS_RESCALE_APY = CONTRACTS_RESCALE.replace(
    "    inflated_guaranteed=expr.inflated_guaranteed * 1_000_000,\n",
    "    inflated_guaranteed=expr.inflated_guaranteed * 1_000_000,\n    inflated_apy=expr.inflated_apy * 1_000_000,\n",
)

# f3a97dc8 #6 — keyed on `col`, buckaroo's wire code: silently matched nothing
DISPLAY_COL_KEYED = """MONEY_COLS = {"value", "guaranteed", "inflated_value", "inflated_guaranteed"}


class MoneyMillionsStyling(DefaultMainStyling):
    df_display_name = "main"

    @classmethod
    def style_column(kls, col, column_metadata):
        base_config = super().style_column(col, column_metadata)
        if col in MONEY_COLS:
            base_config["displayer_args"] = {"displayer": "compact_number"}
        return base_config
"""

# f3a97dc8 #9 — the fix: match on orig_col_name
DISPLAY_ORIG_COL = """MONEY_COLS = {"value", "guaranteed", "inflated_value", "inflated_guaranteed"}


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
        return base_config
"""

# f3a97dc8 #10
STOCK_MAIN = 'class StockMainStyling(DefaultMainStyling):\n    df_display_name = "stock_main"\n'

# 0fcac6bb #85 — the display prompt's final klass
DISPLAY_MONEY_AND_YEAR = """class MoneyAndYearStyling(DefaultMainStyling):
    df_display_name = "main"
    requires_summary = ["histogram", "is_numeric", "dtype", "_type", "min", "max"]

    PLAIN_INT_COLS = {"year_signed", "season"}
    MONEY_KEYWORDS = ("apy", "value", "guarantee", "salary", "bonus", "cash", "cap_number", "amount_earned",
                      "price", "cost")
    MONEY_EXCLUDE = ("pct", "percent", "ratio", "_per_", "rate")

    @classmethod
    def _contains_any(kls, name, needles):
        # the display-klass sandbox's builtins whitelist has no any()/all()
        for needle in needles:
            if needle in name:
                return True
        return False

    @classmethod
    def style_column(kls, col, column_metadata):
        base_config = super().style_column(col, column_metadata)
        real_name = str(column_metadata.get('orig_col_name', col)).lower()
        t = column_metadata.get('_type')

        if real_name in kls.PLAIN_INT_COLS and t in ('integer', 'float'):
            base_config['displayer_args'] = {'displayer': 'obj'}
            return base_config

        is_moneyish = (
            kls._contains_any(real_name, kls.MONEY_KEYWORDS)
            and not kls._contains_any(real_name, kls.MONEY_EXCLUDE)
        )
        if is_moneyish and t in ('integer', 'float', 'decimal'):
            max_val = column_metadata.get('max') or 0
            min_val = column_metadata.get('min') or 0
            magnitude = max(abs(max_val), abs(min_val))
            if magnitude >= 1000:
                base_config['displayer_args'] = {'displayer': 'compact_number', 'prefix': '$'}
            else:
                base_config['displayer_args'] = {
                    'displayer': 'float',
                    'min_fraction_digits': 0,
                    'max_fraction_digits': 1,
                    'prefix': '$',
                    'suffix': 'M',
                }
        return base_config
"""

_QEC_HEAD = """from tallyman_xorq.io import tracked_expr_from_alias
import xorq.vendor.ibis as ibis

stats = tracked_expr_from_alias("player_stats_season")
"""

_QEC_SELECT = """        season=qb.season,
        player=qb.player_display_name,
        team=qb.recent_team,
        games=qb.games,
        attempts=qb.attempts,
        passing_epa=qb.passing_epa,
        rushing_epa=qb.rushing_epa,
        total_epa=qb.total_epa,
        contract_team=current_contract.contract_team,
        year_signed=current_contract.year_signed,
        contract_years=current_contract.contract_years,
        value=current_contract.value,
        apy=current_contract.apy,
        guaranteed=current_contract.guaranteed,
        is_active=current_contract.is_active,
"""

_CURRENT_CONTRACT_SELECT = """    .select(
        gsis_id=contracts.gsis_id,
        contract_team=contracts.team,
        year_signed=contracts.year_signed,
        contract_years=contracts.years,
        value=contracts.value,
        apy=contracts.apy,
        guaranteed=contracts.guaranteed,
        is_active=contracts.is_active,
    )
"""

# f3a97dc8 #13 — "make a new QB only table with EPA, year, team, contract details" (2,350 rows: join fan-out)
QEC_V1 = (
    _QEC_HEAD
    + """qb = stats.filter((stats.position == "QB") & (stats.season_type == "REG")).mutate(
    total_epa=stats.passing_epa + stats.rushing_epa,
)

contracts = tracked_expr_from_alias("contracts")
latest_year = contracts.group_by("gsis_id").aggregate(
    latest_year_signed=contracts.year_signed.max()
)
current_contract = (
    contracts.join(
        latest_year,
        (contracts.gsis_id == latest_year.gsis_id)
        & (contracts.year_signed == latest_year.latest_year_signed),
    )
"""
    + _CURRENT_CONTRACT_SELECT
    + """)

expr = (
    qb.join(current_contract, qb.player_id == current_contract.gsis_id, how="left")
    .select(
"""
    + _QEC_SELECT
    + """    )
    .order_by(ibis.desc("season"), ibis.desc("total_epa"))
)
"""
)

# f3a97dc8 #14 — one contract per player by a row_number window (2,003 rows)
QEC_V2 = (
    _QEC_HEAD
    + """qb = stats.filter((stats.position == "QB") & (stats.season_type == "REG")).mutate(
    total_epa=stats.passing_epa + stats.rushing_epa,
)

contracts = tracked_expr_from_alias("contracts")
w = ibis.window(
    group_by=contracts.gsis_id,
    order_by=[ibis.desc(contracts.is_active), ibis.desc(contracts.value)],
)
current_contract = (
    contracts.mutate(rn=ibis.row_number().over(w))
    .filter(ibis._.rn == 0)
"""
    + _CURRENT_CONTRACT_SELECT
    + """)

expr = (
    qb.join(current_contract, qb.player_id == current_contract.gsis_id, how="left")
    .select(
"""
    + _QEC_SELECT
    + """    )
    .order_by(ibis.desc("season"), ibis.desc("total_epa"))
)
"""
)

_QEC_MAXVALUE_CONTRACT = (
    """contracts = tracked_expr_from_alias("contracts")
w = ibis.window(
    group_by=contracts.gsis_id,
    order_by=[ibis.desc(contracts.value), ibis.desc(contracts.year_signed)],
)
current_contract = (
    contracts.mutate(rn=ibis.row_number().over(w))
    .filter(ibis._.rn == 0)
"""
    + _CURRENT_CONTRACT_SELECT
    + """)
"""
)

# f3a97dc8 #16 — "ok now compute value per qb salary million"
QEC_V3 = (
    _QEC_HEAD
    + """qb = stats.filter((stats.position == "QB") & (stats.season_type == "REG")).mutate(
    total_epa=stats.passing_epa + stats.rushing_epa,
)

"""
    + _QEC_MAXVALUE_CONTRACT
    + """
joined = qb.join(current_contract, qb.player_id == current_contract.gsis_id, how="left")

expr = (
    joined.select(
        season=joined.season,
        player=joined.player_display_name,
        team=joined.recent_team,
        games=joined.games,
        attempts=joined.attempts,
        passing_epa=joined.passing_epa,
        rushing_epa=joined.rushing_epa,
        total_epa=joined.total_epa,
        contract_team=joined.contract_team,
        year_signed=joined.year_signed,
        contract_years=joined.contract_years,
        value=joined.value,
        apy=joined.apy,
        guaranteed=joined.guaranteed,
        is_active=joined.is_active,
    )
    # apy is already in millions (only value/guaranteed were rescaled to raw dollars).
    .mutate(epa_per_apy_million=ibis._.total_epa / ibis._.apy)
    .order_by(ibis.desc("season"), ibis.desc("total_epa"))
)
"""
)

# f3a97dc8 #17 — "put epa per apy million as the 3rd column": failed in the session with the datafusion planner's
# "Projections require unique expression names" (a mutate before the join, re-selected after it)
QEC_DUP_NAMES = (
    _QEC_HEAD
    + """qb = stats.filter((stats.position == "QB") & (stats.season_type == "REG")).mutate(
    total_epa=stats.passing_epa + stats.rushing_epa,
)

"""
    + _QEC_MAXVALUE_CONTRACT
    + """
joined = qb.join(current_contract, qb.player_id == current_contract.gsis_id, how="left").mutate(
    # apy is already in millions (only value/guaranteed were rescaled to raw dollars).
    epa_per_apy_million=ibis._.total_epa / ibis._.apy
)

expr = joined.select(
    season=joined.season,
    player=joined.player_display_name,
    epa_per_apy_million=joined.epa_per_apy_million,
    team=joined.recent_team,
    games=joined.games,
    attempts=joined.attempts,
    passing_epa=joined.passing_epa,
    rushing_epa=joined.rushing_epa,
    total_epa=joined.total_epa,
    contract_team=joined.contract_team,
    year_signed=joined.year_signed,
    contract_years=joined.contract_years,
    value=joined.value,
    apy=joined.apy,
    guaranteed=joined.guaranteed,
    is_active=joined.is_active,
).order_by(ibis.desc("season"), ibis.desc("total_epa"))
"""
)

# f3a97dc8 #26 — attempts >= 200, "make the 4th column salary, and the 5th total epa" (901 rows)
QEC_SALARY = (
    _QEC_HEAD
    + """qb = stats.filter(
    (stats.position == "QB") & (stats.season_type == "REG") & (stats.attempts >= 200)
)

"""
    + _QEC_MAXVALUE_CONTRACT
    + """
joined = qb.join(current_contract, qb.player_id == current_contract.gsis_id, how="left")

expr = joined.select(
    season=joined.season,
    player=joined.player_display_name,
    # apy is already in millions (only value/guaranteed were rescaled to raw dollars).
    epa_per_apy_million=(joined.passing_epa + joined.rushing_epa) / joined.apy,
    salary=joined.apy,
    total_epa=joined.passing_epa + joined.rushing_epa,
    team=joined.recent_team,
    games=joined.games,
    attempts=joined.attempts,
    passing_epa=joined.passing_epa,
    rushing_epa=joined.rushing_epa,
    contract_team=joined.contract_team,
    year_signed=joined.year_signed,
    contract_years=joined.contract_years,
    value=joined.value,
    guaranteed=joined.guaranteed,
    is_active=joined.is_active,
).order_by(ibis.desc("season"), ibis.desc("total_epa"))
"""
)

_QC_BODY = """from tallyman_xorq.io import tracked_expr_from_alias
import xorq.vendor.ibis as ibis

contracts = tracked_expr_from_alias("contracts")
qb_contracts_raw = contracts.filter((contracts.position == "QB") & (contracts.value > 0))

id_w = ibis.window(order_by=[qb_contracts_raw.gsis_id, qb_contracts_raw.year_signed])
qb_contracts = qb_contracts_raw.mutate(
    contract_id=ibis.row_number().over(id_w),
    contract_end_year=qb_contracts_raw.year_signed + qb_contracts_raw.{YEARS} - 1,
    career_year_at_signing=qb_contracts_raw.year_signed - qb_contracts_raw.draft_year + 1,
    is_rookie_contract=qb_contracts_raw.year_signed == qb_contracts_raw.draft_year,
)

stats = tracked_expr_from_alias("player_stats_season")
qb_stats = stats.filter((stats.position == "QB") & (stats.season_type == "REG"))

joined = qb_contracts.join(
    qb_stats,
    (qb_contracts.gsis_id == qb_stats.player_id)
    & (qb_stats.season >= qb_contracts.year_signed)
    & (qb_stats.season <= qb_contracts.contract_end_year),
    how="left",
)

epa_totals = joined.group_by(joined.contract_id).aggregate(
    total_epa_during_contract=(joined.passing_epa + joined.rushing_epa).sum(),
    total_epa_during_contract_for_ratio=(joined.passing_epa + joined.rushing_epa).sum(),
    seasons_in_data=joined.season.count(),
    games_during_contract=joined.games.sum(),
    games_during_contract_for_ratio=joined.games.sum(),
    attempts_during_contract=joined.attempts.sum(),
)

with_epa = qb_contracts.join(epa_totals, qb_contracts.contract_id == epa_totals.contract_id)

expr = with_epa.select(
    player=with_epa.player,
    epa_per_value_million=with_epa.total_epa_during_contract_for_ratio / (with_epa.value / 1_000_000),
{COST}    total_epa_during_contract=with_epa.total_epa_during_contract,
    career_year_at_signing=with_epa.career_year_at_signing,
    is_rookie_contract=with_epa.is_rookie_contract,
    draft_overall=with_epa.draft_overall,
    year_signed=with_epa.year_signed,
    contract_years=with_epa.years,
    team=with_epa.team,
    value=with_epa.value,
    apy=with_epa.apy,
    guaranteed=with_epa.guaranteed,
    seasons_in_data=with_epa.seasons_in_data,
    games_during_contract=with_epa.games_during_contract,
    attempts_during_contract=with_epa.attempts_during_contract,
    draft_year=with_epa.draft_year,
    college=with_epa.college,
    gsis_id=with_epa.gsis_id,
).order_by(ibis.desc("epa_per_value_million"))
"""

# f3a97dc8 #27 — `contract_years` is not a column of contracts (it is `years`)
QC_TYPO = _QC_BODY.replace("{YEARS}", "contract_years").replace("{COST}", "")
# f3a97dc8 #28
QC_V1 = _QC_BODY.replace("{YEARS}", "years").replace("{COST}", "")
# f3a97dc8 #32 — nullif given a predicate
QC_NULLIF_BAD = _QC_BODY.replace("{YEARS}", "years").replace(
    "{COST}",
    "    cost_per_game_played=(with_epa.value / with_epa.games_during_contract_for_ratio)"
    ".nullif(ibis._.games_during_contract_for_ratio == 0),\n",
)
# f3a97dc8 #33
QC_NULLIF_GOOD = _QC_BODY.replace("{YEARS}", "years").replace(
    "{COST}", "    cost_per_game_played=with_epa.value / with_epa.games_during_contract_for_ratio.nullif(0),\n"
)

# f3a97dc8 #30 — a layered scatter with the clamp transform that rendered white on a column that is mostly null
QC_CHART = {
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "transform": [{"calculate": "clamp(datum.epa_per_value_million, -45, 45)", "as": "epa_clamped"}],
    "layer": [
        {"mark": {"type": "rule", "color": "#888"}, "encoding": {"y": {"datum": 0}}},
        {
            "mark": {"type": "circle", "opacity": 0.6},
            "encoding": {
                "x": {"field": "year_signed", "type": "quantitative", "scale": {"domain": [1995, 2027]}},
                "y": {"field": "epa_clamped", "type": "quantitative"},
                "tooltip": [{"field": "player"}, {"field": "team"}],
            },
        },
    ],
}

# f3a97dc8 #37 — "exclude all practice-squad/futures/reserve transactions": ArrayFilter has no compile rule
ARRAY_FILTER_PROBE = """from tallyman_xorq.io import tracked_expr_from_alias
import xorq.vendor.ibis as ibis

contracts = tracked_expr_from_alias("contracts")
t = contracts.filter(contracts.gsis_id == "00-0030001")
matched = t.mutate(
    matched_type=t.contract_history.filter(lambda e: e["apy"] == t.apy)[0]["contract_type"]
)
expr = matched.select("year_signed", "apy", "matched_type")
"""

# f3a97dc8 #39 — the unnest probe it switched to (value-level unnest: a row-multiplying op, so a worthy entry)
UNNEST_PROBE = """from tallyman_xorq.io import tracked_expr_from_alias

contracts = tracked_expr_from_alias("contracts")
qb = contracts.filter(contracts.position == "QB")
exploded = qb.select("gsis_id", "year_signed", "apy", entry=qb.contract_history.unnest())
expr = exploded.mutate(entry_type=exploded.entry["contract_type"], entry_apy=exploded.entry["apy"]).drop("entry")
"""

_QB_EPA_HEAD = """from tallyman_xorq.io import tracked_expr_from_alias
import xorq.vendor.ibis as ibis

stats = tracked_expr_from_alias("player_stats")
contracts = tracked_expr_from_alias("contracts")
"""

_QB_EPA_JOIN = """
current_contracts = contracts.filter(contracts.is_active == True).select(
    "gsis_id",
    contract_team=contracts.team,
    year_signed=contracts.year_signed,
    apy=contracts.apy,
)

joined = qb_stats.join(
    current_contracts,
    qb_stats.player_id == current_contracts.gsis_id,
    how="inner",
)
"""

# 63721a56 #57 — the scripted demo prompt's qb_epa (with its /(apy/1e6) units bug)
QB_EPA_V1 = (
    _QB_EPA_HEAD
    + """
qb_stats = stats.filter(
    (stats.season_type == "REG")
    & (stats.position == "QB")
    & (stats.attempts >= 200)
)
"""
    + _QB_EPA_JOIN
    + """
expr = joined.mutate(
    epa_per_apy_million=(
        ibis.coalesce(joined.passing_epa, 0) + ibis.coalesce(joined.rushing_epa, 0)
    )
    / (joined.apy / 1_000_000)
).select(
    "player_id",
    "player_display_name",
    "season",
    "position",
    "recent_team",
    "passing_epa",
    "rushing_epa",
    "contract_team",
    "year_signed",
    "apy",
    "epa_per_apy_million",
)
"""
)

# 63721a56 #58 — "for qb_epa the first columns should be Player_name, season, recent team, apy, apy_per_million"
QB_EPA_REORDER = QB_EPA_V1.replace(
    """    "player_id",
    "player_display_name",
    "season",
    "position",
    "recent_team",
    "passing_epa",
    "rushing_epa",
    "contract_team",
    "year_signed",
    "apy",
    "epa_per_apy_million",
""",
    """    "player_display_name",
    "season",
    "recent_team",
    "apy",
    "epa_per_apy_million",
    "player_id",
    "position",
    "passing_epa",
    "rushing_epa",
    "contract_team",
    "year_signed",
""",
)

# 63721a56 #60 — "we need to filter qb_epa by players who have played 200 downs"
QB_EPA_200_DOWNS = QB_EPA_REORDER.replace(
    "    & (stats.attempts >= 200)\n", "    & ((stats.attempts + stats.carries) >= 200)\n"
)

# 63721a56 #61 — "add snaps and games started as columns"
QB_EPA_GAMES = QB_EPA_200_DOWNS.replace(
    """    "epa_per_apy_million",
    "player_id",
    "position",
    "passing_epa",
    "rushing_epa",
    "contract_team",
    "year_signed",
)""",
    """    "epa_per_apy_million",
    games_started=joined.games,
    position=joined.position,
    passing_epa=joined.passing_epa,
    rushing_epa=joined.rushing_epa,
    contract_team=joined.contract_team,
    year_signed=joined.year_signed,
    player_id=joined.player_id,
)""",
)

# 63721a56: "how is patrick mahomes year signed 2026" — rows whose attached contract was signed after the season
QB_EPA_FUTURE_CONTRACTS = """from tallyman_xorq.io import tracked_expr_from_alias

q = tracked_expr_from_alias("qb_epa")
expr = q.filter(q.year_signed > q.season)
"""

# 6180b849 — a second-level follower of qb_epa
QB_BANG_FOR_BUCK = """from tallyman_xorq.io import tracked_expr_from_alias

q = tracked_expr_from_alias("qb_epa")
expr = q.group_by("player_display_name").aggregate(
    seasons=q.count(),
    mean_epa_per_apy=q.epa_per_apy_million.mean(),
    total_apy=q.apy.sum(),
)
"""

# 0fcac6bb #65, #66 — grain probes
GRAIN_PLAYER_SEASON = """from tallyman_xorq.io import tracked_expr_from_alias
import xorq.vendor.ibis as ibis

stats = tracked_expr_from_alias("player_stats")
qb = stats.filter(
    (stats.season_type == "REG") &
    (stats.position == "QB") &
    ((stats.attempts + stats.carries) >= 200)
)
dupes = qb.group_by(["player_id", "season"]).aggregate(n=qb.count()).filter(ibis._.n > 1)
expr = dupes
"""
GRAIN_ACTIVE = """from tallyman_xorq.io import tracked_expr_from_alias
import xorq.vendor.ibis as ibis

contracts = tracked_expr_from_alias("contracts")
active = contracts.filter(contracts.is_active == True)
dupes = active.group_by("gsis_id").aggregate(n=active.count()).filter(ibis._.n > 1)
expr = dupes
"""

# 0fcac6bb #70, #71, #72 — post-processing drafts (#71 imports pandas: the sandbox refuses it)
PP_ACTIVE_DUPES = """def process(expr):
    df = expr.execute()
    active = df[df["is_active"] == True]
    dupe_ids = active.groupby("gsis_id").size()
    dupe_ids = dupe_ids[dupe_ids > 1]
    return active[active["gsis_id"].isin(dupe_ids.index)][
        ["player","position","team","gsis_id","year_signed","apy","otc_id"]]
"""
PP_IMPORTS_PANDAS = """def process(expr):
    df = expr.execute()
    active = df[(df["is_active"] == True) & (df["gsis_id"].notna())]
    total = len(active)
    distinct = active["gsis_id"].nunique()
    import pandas as pd
    return pd.DataFrame([{"total_active_with_gsis": total, "distinct_gsis": distinct}])
"""
PP_COUNTS = """def process(expr):
    df = expr.execute()
    active = df[(df["is_active"] == True) & (df["gsis_id"].notna())]
    total = len(active)
    distinct = active["gsis_id"].nunique()
    out = active.head(1)[["player"]].copy()
    out["total_active_with_gsis"] = total
    out["distinct_gsis"] = distinct
    return out
"""
PP_ACTIVE_ONLY = "def process(expr):\n    return expr.filter(expr.is_active == True)\n"


ORDERS = "from tallyman_xorq.io import read_project_file\nt = read_project_file('orders.parquet')\n"
BY_REGION = ORDERS + "expr = t.group_by('region').aggregate(total=t.price.sum(), n=t.count())\n"

# The unnest probe over the file rather than the alias, for a scenario that has no contracts entry to follow.
UNNEST_FROM_FILE = UNNEST_PROBE.replace(
    'from tallyman_xorq.io import tracked_expr_from_alias\n\ncontracts = tracked_expr_from_alias("contracts")',
    "from tallyman_xorq.io import read_project_file\n\ncontracts = read_project_file('nfl_contracts.parquet')",
)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


@scenario(
    origin="f3a97dc8",
    summary="The nfl-salaries session: contracts, money styling, the QB EPA table through "
    "its revisions and the planner error, qb_contracts with a chart; a reset back to mid-session, a forward "
    "reset from the CLI, then a contracts revise that cascades into both followers.",
)
def nfl_salaries_session(s: Session) -> None:
    write_nfl(_data_dir(s))
    s.note("f3a97dc8: 'lets start a new tallyman project. can you find me nfl salary information in a dataset?'")
    s.mcp(
        "catalog_load_parquet",
        rel_path="nfl_contracts.parquet",
        name="contracts",
        prompt="nfl contracts (nflverse historical_contracts)",
    )
    s.open("contracts")
    s.mcp("catalog_list_display_klasses")
    s.mcp(
        "catalog_revise",
        name="contracts",
        code=CONTRACTS_RESCALE,
        prompt="Value, Guaranteed, inflated_value, inflated guaranteed are numeric millions columns. displayed as "
        "$34.4M, only one decimal point",
    )
    s.mcp("catalog_add_display_klass", name="money_millions_styling", source=DISPLAY_COL_KEYED)
    s.open("contracts", label="open contracts: 'it's not displaying right'")
    s.mcp("catalog_remove_display_klass", name="money_millions_styling")
    s.mcp("catalog_add_display_klass", name="money_millions_styling", source=DISPLAY_ORIG_COL)
    s.mcp("catalog_add_display_klass", name="stock_main_styling", source=STOCK_MAIN)
    s.mcp("catalog_list")

    s.note("'make a new QB only table with EPA, year, team, contract details'")
    s.mcp(
        "catalog_load_parquet",
        rel_path="player_stats_season.parquet",
        name="player_stats_season",
        prompt="nflverse player stats, season level",
    )
    s.mcp(
        "catalog_create",
        name="qb_epa_contracts",
        code=QEC_V1,
        prompt="a new QB only table with EPA, year, team, contract details",
    )
    s.open("qb_epa_contracts")
    s.mcp("catalog_revise", name="qb_epa_contracts", code=QEC_V2, prompt="one contract per player")
    s.mcp("catalog_revise", name="qb_epa_contracts", code=QEC_V3, prompt="ok now compute value per qb salary million")
    s.diff("qb_epa_contracts")
    s.label_step("before_reorder")
    s.mcp(
        "catalog_revise",
        name="qb_epa_contracts",
        code=QEC_DUP_NAMES,
        expect_error=True,
        prompt="put epa per apy million as the 3rd column after year and name",
    )
    s.mcp(
        "catalog_revise",
        name="qb_epa_contracts",
        code=QEC_SALARY,
        prompt="filter to 200 attempts; make the 4th column salary, and the 5th total epa",
    )
    s.open("qb_epa_contracts")

    s.note("'ok make a new table qb contracts. we want to rate the entire contract vs the epa production'")
    s.mcp(
        "catalog_create",
        name="qb_contracts",
        code=QC_TYPO,
        expect_error=True,
        expect_message="years",
        prompt="rate the entire contract vs the epa production over that contract",
    )
    s.mcp(
        "catalog_create",
        name="qb_contracts",
        code=QC_V1,
        prompt="rate the entire contract vs the epa production over that contract",
    )
    s.mcp("catalog_chart", hash_or_alias="qb_contracts", vega_spec=QC_CHART)
    s.mcp("catalog_chart_errors", hash_or_alias="qb_contracts")
    s.mcp(
        "catalog_revise",
        name="qb_contracts",
        code=QC_NULLIF_BAD,
        expect_error=True,
        expect_message="nullif",
        prompt="do 1. cost per game played",
    )
    out = s.mcp("catalog_revise", name="qb_contracts", code=QC_NULLIF_GOOD, prompt="do 1. cost per game played")
    s.expect(
        "chart" in (out.get("carried_over") or []),
        "the chart did not follow qb_contracts to its new version",
        carried_over=out.get("carried_over"),
    )
    s.open("qb_contracts")
    s.label_step("after_qb_contracts")

    s.note("the user scrubs the timeline back to before the column reorder, looks around, and scrubs forward")
    s.reset("before_reorder", via="api")
    s.open_project()
    s.open("qb_epa_contracts", label="open qb_epa_contracts at V3")
    s.page("qb_epa_contracts", offset=100)
    s.diff("qb_epa_contracts")
    s.http("GET", f"/{s.project}/api/entry/qb_contracts", expect=(404,), label="qb_contracts is gone after the reset")
    s.reset("after_qb_contracts", via="cli")
    s.open("qb_contracts")
    s.open("qb_epa_contracts")

    s.note("a later revise of contracts: auto-recalc must carry qb_epa_contracts and qb_contracts in one revision")
    out = s.mcp("catalog_revise", name="contracts", code=CONTRACTS_RESCALE_APY, prompt="inflated_apy in dollars too")
    recalc = out.get("recalc") or {}
    s.expect(
        recalc.get("status") == "ok" and len(recalc.get("remap") or {}) >= 2,
        "the contracts revise did not re-point both followers",
        recalc=_brief(recalc),
    )
    s.open("qb_epa_contracts")
    s.open("qb_contracts")
    s.diff("qb_contracts")
    s.mcp("catalog_scan_staleness", verify_results=True)
    s.finish()


@scenario(
    origin="63721a56, 0fcac6bb",
    summary="The demo prompt in a fresh project: a load before the files are in "
    "data/, grain probes, the post-processing sandbox, qb_epa and three revisions, the display prompt; a CLI "
    "reset back to V1, a new revision on the branch that re-creates a retired version's hash, the "
    "'contract signed in the future' child, a snapshot eviction under the child, a restart.",
)
def nfl_demo_first_encounter(s: Session) -> None:
    s.mcp("project_list")
    s.mcp("catalog_list")
    s.mcp(
        "catalog_load_parquet",
        rel_path="nfl_contracts.parquet",
        name="contracts",
        expect_error=True,
        expect_message="data",
        prompt="Load nfl_contracts.parquet as contracts",
    )
    write_nfl(_data_dir(s))
    s.mcp(
        "catalog_load_parquet",
        rel_path="nfl_contracts.parquet",
        name="contracts",
        prompt="Load nfl_contracts.parquet as contracts",
    )
    s.mcp(
        "catalog_load_parquet",
        rel_path="player_stats_season.parquet",
        name="player_stats",
        prompt="Load player_stats_season.parquet as player_stats",
    )
    s.mcp("catalog_run", code=GRAIN_PLAYER_SEASON, prompt="grain check: one row per QB per season?")
    s.mcp("catalog_run", code=GRAIN_ACTIVE, prompt="grain check: one active contract per player?")
    s.mcp("catalog_run_post_processing", entry="contracts", code=PP_ACTIVE_DUPES)
    s.mcp(
        "catalog_run_post_processing",
        entry="contracts",
        code=PP_IMPORTS_PANDAS,
        expect_error=True,
        expect_message="import",
    )
    s.mcp("catalog_run_post_processing", entry="contracts", code=PP_COUNTS)
    s.mcp(
        "catalog_create",
        name="qb_epa",
        code=QB_EPA_V1,
        prompt="one row per QB per regular season (attempts >= 200), passing EPA, rushing EPA, current contract",
    )
    s.open("qb_epa")
    s.label_step("qb_epa_v1")
    s.mcp(
        "catalog_revise",
        name="qb_epa",
        code=QB_EPA_REORDER,
        prompt="for qb_epa the first columns should be Player_name, season, recent team, apy, apy_per_million",
    )
    s.mcp(
        "catalog_revise",
        name="qb_epa",
        code=QB_EPA_200_DOWNS,
        prompt="we need to filter qb_epa by players who have played 200 downs",
    )
    s.mcp("catalog_revise", name="qb_epa", code=QB_EPA_GAMES, prompt="add snaps and games started as columns")
    s.diff("qb_epa")
    s.diff("qb_epa", 1, 4, label="diff qb_epa V1 -> V4")
    s.mcp("catalog_diff", name="qb_epa")
    s.mcp("catalog_add_display_klass", name="money_and_year_styling", source=DISPLAY_MONEY_AND_YEAR)
    s.mcp("catalog_add_post_processing", name="active_only", source=PP_ACTIVE_ONLY)
    s.open("qb_epa")
    s.open("contracts")

    s.note(
        "reset from the CLI to qb_epa V1; then a revision on the new branch re-derives V3's code, whose entry "
        "now sits in the bullpen"
    )
    s.reset("qb_epa_v1", via="cli")
    s.open("qb_epa", label="open qb_epa at V1")
    s.mcp("catalog_list_post_processings")
    out = s.mcp(
        "catalog_revise",
        name="qb_epa",
        code=QB_EPA_200_DOWNS,
        prompt="we need to filter qb_epa by players who have played 200 downs",
    )
    s.expect(out.get("version") == 2, "the revise after the reset did not become V2", version=out.get("version"))
    s.open("qb_epa")
    s.diff("qb_epa")

    s.note("'how is patrick mahomes year signed 2026' — a cheap child over the worthy qb_epa's snapshot")
    s.mcp(
        "catalog_create",
        name="qb_epa_future_contracts",
        code=QB_EPA_FUTURE_CONTRACTS,
        prompt="rows whose current contract was signed after that season",
    )
    s.open("qb_epa_future_contracts")
    s.evict("snapshots")
    s.open("qb_epa_future_contracts", label="open the child with its parent's snapshot deleted")
    s.restart()
    s.open_project()
    s.reset("qb_epa_v1", via="api", label="reset to qb_epa_v1 again (the branch's steps are retired)")
    s.finish()


@scenario(
    origin="6180b849",
    summary="Manual recalc: a two-level follower chain, a contracts revise with auto-recalc "
    "off, scan and recalc; a reset back, the same revise again and a recalc that must mint the same hashes; a "
    "source edit on disk and a source-axis recalc; old versions must keep serving their rows.",
    auto_recalc=False,
)
def recalc_after_rescale(s: Session) -> None:
    write_nfl(_data_dir(s))
    s.mcp("catalog_load_parquet", rel_path="nfl_contracts.parquet", name="contracts", prompt="contracts")
    s.mcp("catalog_load_parquet", rel_path="player_stats_season.parquet", name="player_stats", prompt="stats")
    s.mcp("catalog_create", name="qb_epa", code=QB_EPA_V1, prompt="qb epa per apy")
    s.mcp("catalog_create", name="qb_bang_for_buck", code=QB_BANG_FOR_BUCK, prompt="bang for buck per QB")
    s.open("qb_epa")
    s.open("qb_bang_for_buck")
    s.label_step("before_rescale")

    s.mcp("catalog_revise", name="contracts", code=CONTRACTS_RESCALE, prompt="dollars, not millions")
    scan = s.mcp("catalog_scan_staleness")
    from tallyman_core import get_alias

    qb_epa_v1 = get_alias(s.project, "qb_epa")
    s.expect(
        qb_epa_v1 in (scan.get("stale") or []), "qb_epa is not stale after contracts advanced", stale=scan.get("stale")
    )
    s.open_project()
    s.mcp("catalog_recalc", dry_run=True)
    first = s.mcp("catalog_recalc", dry_run=False)
    s.expect(get_alias(s.project, "qb_epa") != qb_epa_v1, "recalc did not re-point qb_epa", report=_brief(first))
    s.expect(
        len(first.get("remap") or {}) == 2,
        "recalc should rebuild qb_epa and qb_bang_for_buck",
        remap=first.get("remap"),
    )
    s.open("qb_epa")
    s.open("qb_bang_for_buck")

    s.note(
        "reset back before the rescale, then do it again: content addressing says the recalc lands on the same "
        "hashes as the first time"
    )
    s.reset("before_rescale", via="api")
    s.mcp("catalog_scan_staleness")
    s.mcp("catalog_revise", name="contracts", code=CONTRACTS_RESCALE, prompt="dollars, not millions (again)")
    second = s.mcp("catalog_recalc", dry_run=False)
    s.expect(
        sorted((second.get("remap") or {}).values()) == sorted((first.get("remap") or {}).values()),
        "the recalc after the reset minted different hashes",
        first=first.get("remap"),
        second=second.get("remap"),
    )
    s.open("qb_bang_for_buck")

    s.note("the user overwrites player_stats_season.parquet: the source axis goes stale")
    s.edit_source("player_stats_season.parquet", lambda df: df.assign(passing_epa=df.passing_epa * 1.01))
    scan = s.mcp("catalog_scan_staleness")
    s.expect(
        get_alias(s.project, "player_stats") in (scan.get("stale") or []),
        "player_stats is not stale after its file changed",
        stale=scan.get("stale"),
    )
    s.mcp("catalog_recalc", dry_run=False)
    s.open("qb_epa")
    s.page("qb_epa-v1", label="qb_epa V1 still serves its own rows")
    s.page("qb_epa-v2")
    s.diff("player_stats")
    s.diff("qb_bang_for_buck")
    s.finish()


@scenario(
    origin="spike_reset_roundtrip.py, 675b2a67",
    summary="Reset back and forward across worthy, cheap and "
    "chained entries over two sources, with grids open: entries of the later step disappear and come back, "
    "a cold cache under a chained child, Buckaroo forgetting sessions, a reset to genesis and back.",
)
def reset_roundtrip_with_grids(s: Session) -> None:
    write_orders(_data_dir(s))
    write_orders(_data_dir(s), "extra.parquet", seed=5)
    s.mcp("catalog_create", name="by_region", code=BY_REGION, prompt="orders by region")
    s.open("by_region")
    s.label_step("s1")
    extra = "from tallyman_xorq.io import read_project_file\nt = read_project_file('extra.parquet')\n"
    s.mcp(
        "catalog_create",
        name="extra_boots",
        code=extra + "expr = t.filter(t.category == 'boots')\n",
        prompt="boots in extra",
    )
    s.mcp(
        "catalog_create",
        name="extra_by_cat",
        prompt="extra by category",
        code=extra + "expr = t.group_by('category').aggregate(n=t.count(), revenue=(t.price * t.qty).sum())\n",
    )
    s.mcp(
        "catalog_create",
        name="extra_top",
        prompt="categories with more than 300 orders",
        code="from tallyman_xorq.io import tracked_expr_from_alias\nc = tracked_expr_from_alias('extra_by_cat')\n"
        "expr = c.filter(c.n > 300)\n",
    )
    for name in ("extra_boots", "extra_by_cat", "extra_top"):
        s.open(name)
    s.label_step("s2")

    s.reset("s1", via="api")
    s.open_project()
    s.open("by_region")
    s.http("GET", f"/{s.project}/api/entry/extra_by_cat", expect=(404,), label="extra_by_cat is gone at s1")
    s.reset("s2", via="cli")
    for name in ("extra_boots", "extra_by_cat", "extra_top"):
        s.open(name)
    s.evict("all")
    s.open("extra_top", label="open the chained child from an empty cache")
    s.buckaroo_forgets()
    s.open("extra_by_cat", label="open after Buckaroo dropped its sessions")
    s.reset(0, via="api", label="reset to genesis")
    s.open_project()
    s.reset("s2", via="api")
    s.restart(buckaroo=True)
    s.open("extra_top")
    s.finish()


@scenario(
    origin="PR #184 reviews (E1, C1), #168",
    summary="A CSV source through tallyman_read_csv, a worthy float "
    "aggregate and a join over it; the CSV edited on disk, scan and recalc; the diff promoted to an entry and "
    "read from an empty cache; a reset back to before the edit, with the live file still edited.",
)
def csv_edit_and_promoted_diff(s: Session) -> None:
    d = _data_dir(s)
    write_orders(d)
    write_returns_csv(d)
    s.mcp("catalog_load_parquet", rel_path="orders.parquet", name="orders", prompt="orders")
    s.mcp(
        "catalog_create",
        name="returns",
        prompt="returns",
        code=f"from tallyman_xorq.io import tallyman_read_csv\nexpr = tallyman_read_csv({str(d / 'returns.csv')!r})\n",
    )
    s.mcp(
        "catalog_create",
        name="refunds_by_reason",
        prompt="refunds by reason",
        code="from tallyman_xorq.io import tracked_expr_from_alias\nr = tracked_expr_from_alias('returns')\n"
        "expr = r.group_by('reason').aggregate(n=r.count(), refunded=r.refund.sum(), mean=r.refund.mean())\n",
    )
    s.mcp(
        "catalog_create",
        name="returns_by_region",
        prompt="returns joined to orders, by region",
        code="from tallyman_xorq.io import tracked_expr_from_alias\no = tracked_expr_from_alias('orders')\n"
        "r = tracked_expr_from_alias('returns')\nj = o.join(r, 'order_id', how='left')\n"
        "expr = j.group_by('region').aggregate(returned=j.refund.count(), refunds=j.refund.sum())\n",
    )
    for name in ("returns", "refunds_by_reason", "returns_by_region"):
        s.open(name)
    s.label_step("before_edit")

    s.edit_source("returns.csv", lambda df: df.assign(refund=df.refund.round(0)))
    scan = s.mcp("catalog_scan_staleness")
    from tallyman_core import get_alias

    s.expect(
        get_alias(s.project, "returns") in (scan.get("stale") or []),
        "returns is not stale after the edit",
        stale=scan.get("stale"),
    )
    s.mcp("catalog_recalc", dry_run=False)
    s.open("refunds_by_reason")
    s.diff("refunds_by_reason")
    s.page("returns-v1", label="returns V1 still serves the rows before the edit")
    s.mcp("catalog_promote_diff", name="refunds_by_reason", alias="refunds_change")
    s.open("refunds_change")
    s.evict("all")
    s.open("refunds_change", label="open the promoted diff from an empty cache")
    s.reset("before_edit", via="api")
    s.open("refunds_by_reason", label="refunds_by_reason V1 after the reset (the live CSV is still edited)")
    s.mcp("catalog_scan_staleness")
    s.finish()


@scenario(
    origin="96f1817c, #118, #176, #190",
    summary="Several tabs at once: concurrent pages of three entries "
    "and the diff of a key-less wide entry, then the same while a build runs on another thread.",
    known={"already_borrowed": "#118", "concurrent_failure": "#118", "stall": "#190"},
)
def concurrency_under_load(s: Session) -> None:
    write_nfl(_data_dir(s))
    s.mcp("catalog_load_parquet", rel_path="nfl_contracts.parquet", name="contracts", prompt="contracts")
    s.mcp("catalog_load_parquet", rel_path="player_stats_season.parquet", name="player_stats", prompt="stats")
    s.mcp("catalog_create", name="qb_epa", code=QB_EPA_V1, prompt="qb epa")
    s.mcp("catalog_revise", name="contracts", code=CONTRACTS_RESCALE, prompt="dollars")
    s.diff("contracts", label="diff contracts V1 -> V2 (no primary key: the search is time-boxed)")
    s.hammer(["contracts", "qb_epa", "player_stats"], threads=8, per_thread=6, with_diff="contracts")
    s.hammer(
        ["contracts", "qb_epa"],
        threads=6,
        per_thread=4,
        with_build=QB_EPA_GAMES,
        label="pages while a build holds the project lock",
    )
    s.open("qb_epa")
    s.finish()


@scenario(
    origin="PR #184 second review, f3a97dc8 errors, ADR-008",
    summary="The authoring rules the redesign added, "
    "each hit the way an LLM hits it: a self-referencing revise, a bare-alias pin, .cache(), a raw parquet "
    "read, a three-way join without the drop, a sort that is not the last step, a tied sort under a limit, a "
    "window and an unnest, ArrayFilter, float aggregates; then a reset and the cold closing checks.",
)
def authoring_edge_cases(s: Session) -> None:
    write_orders(_data_dir(s))
    write_nfl(_data_dir(s))
    base = s.mcp("catalog_create", name="by_region", code=BY_REGION, prompt="by region")
    s.label_step("base")
    track = "from tallyman_xorq.io import tracked_expr_from_alias, pinned_expr_from_alias\n"
    s.mcp(
        "catalog_revise",
        name="by_region",
        expect_error=True,
        expect_message="pinned_expr_from_alias",
        code=track + "t = tracked_expr_from_alias('by_region')\nexpr = t.filter(t.n > 10)\n",
        prompt="only regions with more than 10 orders (#135: self reference)",
    )
    s.mcp(
        "catalog_revise",
        name="by_region",
        prompt="only regions with more than 10 orders",
        code=track + f"t = pinned_expr_from_alias({base.get('hash')!r})\nexpr = t.filter(t.n > 10)\n",
    )
    s.mcp(
        "catalog_run",
        expect_error=True,
        expect_message="-v",
        code=track + "t = pinned_expr_from_alias('by_region')\nexpr = t\n",
        prompt="#166: bare alias pin",
    )
    s.mcp(
        "catalog_run",
        expect_error=True,
        expect_message="cache",
        code=ORDERS + "expr = t.group_by('region').aggregate(n=t.count()).cache()\n",
        prompt=".cache()",
    )
    s.mcp(
        "catalog_run",
        expect_error=True,
        expect_message="read_project_file",
        prompt="raw parquet read",
        code=f"import xorq.api as xo\nexpr = xo.deferred_read_parquet({str(_data_dir(s) / 'orders.parquet')!r})\n",
    )

    s.mcp("catalog_load_parquet", rel_path="orders.parquet", name="orders", prompt="orders")
    # cheap selects keep __row_order: a cheap entry that drops it is a build error (ADR-008)
    s.mcp(
        "catalog_create",
        name="orders_a",
        code=track + "o = tracked_expr_from_alias('orders')\nexpr = o.select('order_id', 'region', '__row_order')\n",
        prompt="a",
    )
    s.mcp(
        "catalog_create",
        name="orders_b",
        code=track + "o = tracked_expr_from_alias('orders')\nexpr = o.select('order_id', 'price', '__row_order')\n",
        prompt="b",
    )
    three = (
        track + "a = tracked_expr_from_alias('orders_a')\nb = tracked_expr_from_alias('orders_b')\n"
        "c = tracked_expr_from_alias('orders')\n"
    )
    s.mcp(
        "catalog_run",
        expect_error=True,
        expect_message="drop",
        prompt="three-way join without the drop",
        code=three + "expr = a.join(b, 'order_id').join(c.select('order_id', 'qty', '__row_order'), 'order_id')\n",
    )
    s.mcp(
        "catalog_create",
        name="three_way",
        prompt="three-way join with the drop",
        code=three + "expr = a.join(b.drop('__row_order'), 'order_id').join(c.select('order_id', 'qty'), 'order_id')\n",
    )

    s.mcp(
        "catalog_create",
        name="priciest_first",
        prompt="sort by price, then add a column (B3)",
        code=ORDERS + "expr = t.order_by(t.price.desc()).mutate(double_qty=t.qty * 2)\n",
    )
    rows = s.page("priciest_first")
    prices = [r["price"] for r in rows]
    s.expect(
        prices == sorted(prices, reverse=True), "a sort that is not the last step was not honoured", head=prices[:10]
    )
    out = s.mcp(
        "catalog_create",
        name="first_by_region",
        prompt="a tied sort feeding a limit (B4)",
        code=ORDERS + "expr = t.order_by('region').limit(50)\n",
    )
    s.expect(out.get("reproducible") is not False, "a tied sort under a limit was not made reproducible")
    ranked = ORDERS + (
        "import xorq.vendor.ibis as ibis\n"
        "w = ibis.window(group_by='region', order_by='price')\n"
        "expr = t.mutate(rank=ibis.row_number().over(w))\n"
    )
    for name, code in (("ranked", ranked), ("history_rows", UNNEST_FROM_FILE)):
        s.mcp("catalog_create", name=name, code=code, prompt=f"{name} (B5: a window or an unnest must be worthy)")
        s.check(f"{name} is worthy", _expect_worthy(name))

    s.mcp("catalog_load_parquet", rel_path="nfl_contracts.parquet", name="contracts", prompt="contracts")
    s.mcp("catalog_load_parquet", rel_path="player_stats_season.parquet", name="player_stats", prompt="stats")
    s.mcp(
        "catalog_run",
        code=ARRAY_FILTER_PROBE,
        expect_error=True,
        prompt="exclude practice-squad transactions (ArrayFilter)",
    )
    s.mcp(
        "catalog_create",
        name="epa_by_position",
        prompt="float aggregates (D1)",
        code=track + "p = tracked_expr_from_alias('player_stats')\n"
        "expr = p.group_by('position').aggregate(epa=p.passing_epa.sum(), mean=p.rushing_epa.mean())\n",
    )
    s.mcp(
        "catalog_create",
        name="epa_total",
        prompt="an ungrouped float sum (D2)",
        code=track + "p = tracked_expr_from_alias('player_stats')\n"
        "expr = p.aggregate(epa=p.passing_epa.sum(), n=p.count())\n",
    )
    s.open("epa_by_position")
    s.reset("base", via="api")
    s.open_project()
    s.finish()


@scenario(
    origin="PR #189 review (memory pr-189-review-2026-09-21)",
    summary="A non-reproducible entry is pinned; a "
    "reset back makes its snapshot an unpinned orphan the Cache page deletes; a reset forward then heals it "
    "to different rows.",
)
def pin_across_reset(s: Session) -> None:
    write_orders(_data_dir(s))
    s.mcp("catalog_create", name="by_region", code=BY_REGION, prompt="by region")
    s.label_step("base")
    out = s.mcp(
        "catalog_create",
        name="jittered",
        prompt="a random jitter per region",
        code=ORDERS + "import xorq.vendor.ibis as ibis\n"
        "expr = t.mutate(r=ibis.random()).group_by('region').aggregate(noise=ibis._.r.sum())\n",
    )
    h = out.get("hash")
    s.expect(out.get("reproducible") is False, "an ibis.random() entry was not reported non-reproducible")
    s.open("jittered")
    s.label_step("jittered")

    def pinned(ss, rec):
        rows = ss.client.get(f"/{ss.project}/api/result_cache").json().get("entries", [])
        row = next((r for r in rows if r["hash"] == h), None)
        if row is None or not row.get("pinned"):
            ss.flag("scenario_expectation", "error", f"the Cache page lists {h} as {row}", rec)

    s.check("the Cache page shows the snapshot pinned", pinned)
    s.http(
        "DELETE",
        f"/{s.project}/api/result_cache/{h}",
        expect=(409,),
        mutating=False,
        label="the Cache page refuses to delete a pinned snapshot",
    )
    s.reset("base", via="api")
    s.check("after a reset back, the snapshot is still pinned", pinned)
    s.http(
        "DELETE",
        f"/{s.project}/api/result_cache/{h}",
        expect=(409, 404),
        mutating=False,
        label="the Cache page still refuses after the reset",
    )
    s.reset("jittered", via="api")
    s.open("jittered", label="open jittered after the reset forward")
    s.finish()


@scenario(
    origin="PR #189 review (memory pr-189-review-2026-09-21)",
    summary="Parquet sources whose column types "
    "polars rewrites in the ordered copy (date64, map, time32, fixed_size_binary), and a decimal256 that makes "
    "polars panic; each loaded, opened, and compared with the type the source file has.",
)
def source_types_survive_ingest(s: Session) -> None:
    import datetime
    import decimal

    import pyarrow as pa
    import pyarrow.parquet as pq

    d = _data_dir(s)
    d.mkdir(parents=True, exist_ok=True)
    n = 40
    typed = pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "day": pa.array([datetime.date(2024, 1, 1 + i % 28) for i in range(n)], pa.date64()),
            "attrs": pa.array([[("k", i)] for i in range(n)], pa.map_(pa.string(), pa.int64())),
            "at": pa.array([datetime.time(9, i % 60) for i in range(n)], pa.time32("s")),
            "code": pa.array([bytes([i % 256]) * 4 for i in range(n)], pa.binary(4)),
            "amount": pa.array([decimal.Decimal(i) / 4 for i in range(n)], pa.decimal128(12, 2)),
        }
    )
    pq.write_table(typed, d / "typed.parquet")
    pq.write_table(
        pa.table({"big": pa.array([decimal.Decimal(i) for i in range(n)], pa.decimal256(40, 0))}),
        d / "wide_decimal.parquet",
    )

    s.mcp("catalog_load_parquet", rel_path="typed.parquet", name="typed", prompt="typed columns")
    s.open("typed")

    def same_types(ss, rec):
        import xorq.api as xo

        from tallyman_xorq.result_cache import cached_result_expr

        source = xo.deferred_read_parquet(str(d / "typed.parquet")).schema()
        entry = cached_result_expr(ss.project, ss.resolve("typed")).schema()
        for col in source.names:
            if str(source[col]) != str(entry[col]):
                ss.flag(
                    "source_type_changed",
                    "error",
                    f"typed.{col}: {source[col]} in the file, {entry[col]} in the entry",
                    rec,
                )

    s.check("an entry sees the source file's column types", same_types)
    s.mcp(
        "catalog_create",
        name="typed_filtered",
        prompt="a cheap filter over typed",
        code="from tallyman_xorq.io import read_project_file\nt = read_project_file('typed.parquet')\n"
        "expr = t.filter(t.id > 3)\n",
    )
    s.open("typed_filtered")
    s.label_step("typed")
    # Loading it may succeed or fail with a readable error; a polars panic escaping the tool is the defect.
    s.mcp(
        "catalog_load_parquet",
        rel_path="wide_decimal.parquet",
        name="wide_decimal",
        prompt="decimal256",
        expect_error=True,
        expect_message="decimal",
    )
    s.reset("typed", via="api")
    s.finish()


@scenario(
    origin="demo/storyboard.json",
    summary="The demo storyboard replayed through the MCP tools, with a reset "
    "to the first version of shoe_sales mid-way and the revise replayed onto it.",
)
def demo_storyboard(s: Session) -> None:
    import json
    from pathlib import Path

    sb = json.loads((Path(__file__).resolve().parents[1] / "demo" / "storyboard.json").read_text())
    write_orders(_data_dir(s))
    s.replay(sb, stop_before=3)
    s.open("shoe_sales")
    s.label_step("shoe_sales_v1")
    s.replay(sb, start_at=3)
    s.open("shoe_sales")
    s.diff("shoe_sales")
    s.reset("shoe_sales_v1", via="api")
    s.open("shoe_sales", label="shoe_sales V1 after the reset (its chart belonged to V2)")
    s.replay(sb, start_at=3, stop_before=5)
    s.open("shoe_sales")
    s.finish()


def _expect_worthy(name: str):
    def check(s: Session, rec) -> None:
        if not s.oracle._manifest(s.resolve(name)).get("cache_worthy"):
            s.flag("scenario_expectation", "error", f"{name} was classed cheap", rec)

    return check


def _brief(report: dict) -> dict:
    return {k: report.get(k) for k in ("status", "remap", "checkpoint_step", "error") if k in report}
