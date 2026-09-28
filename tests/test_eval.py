"""Eval suite: multi-step tallyman sessions checked after every step (tests/eval_harness.py, tests/eval_oracle.py).

NOT on the default test path (deselected via the ``eval`` marker, like ``cache_lab``). Each scenario in
``tests/eval_scenarios.py`` replays a recorded session's MCP calls with resets, grid opens, source edits, cache
wipes and restarts between them, and the oracle checks the whole project after each step.

    uv run pytest -m eval tests/test_eval.py -v -s
    uv run pytest -m eval tests/test_eval.py -k reset_roundtrip -s

Knobs:
    EVAL_SWEEP          when the full sweep runs: ``mutations`` (default, after every step that can change state),
                        ``every`` (after every step), ``manual`` (only at Session.sweep and finish)
    EVAL_BUCKAROO_0156  1: simulate today's grid, which ignores the row-order hint (buckaroo#974), so a grid/API
                        first-window mismatch is a warning instead of an error
    EVAL_NFL_DIR        a directory with the real nfl_contracts.parquet and player_stats_season.parquet
    EVAL_SCALE          multiplies the synthetic row counts (default 1)
    EVAL_STALL_SECONDS  the per-request stall budget (default 20)

A scenario fails on any ``error`` finding whose code is not in its ``known`` map. Every run writes one JSON report
per scenario (steps, findings, notes, notifies, the ledger) to

    tests/eval_reports/<git-sha>-<timestamp>/<scenario>.json

and prints a summary of the findings grouped by code.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.eval_harness import Session
from tests.eval_scenarios import SCENARIOS

pytestmark = pytest.mark.eval

REPORTS = Path(__file__).parent / "eval_reports"


def _run_dir() -> Path:
    sha = (
        subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=Path(__file__).parent
        ).stdout.strip()
        or "nogit"
    )
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S")
    return REPORTS / f"{sha}-{stamp}"


RUN_DIR = _run_dir()


def _summary(name: str, report: dict, known: dict[str, str]) -> str:
    by_check: dict[str, list[dict]] = defaultdict(list)
    for f in report["findings"]:
        by_check[f["check"]].append(f)
    steps = {s["index"]: s["label"] for s in report["steps"]}
    lines = [f"{name}: {len(report['steps'])} steps, {report['seconds']}s, {len(report['findings'])} findings"]
    for check, fs in sorted(by_check.items(), key=lambda kv: (kv[1][0]["severity"] != "error", kv[0])):
        tag = f" (known: {known[check]})" if check in known else ""
        lines.append(f"  {fs[0]['severity']:5} {check} x{len(fs)}{tag}")
        for f in fs[:3]:
            lines.append(f"      [{f['step']}] {steps.get(f['step'], '')}: {f['message'][:220]}")
    for check in report["known_not_reproduced"]:
        lines.append(f"  note  known issue {check} ({known[check]}) did not reproduce")
    return "\n".join(lines)


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_scenario(name: str, tmp_path: Path, monkeypatch) -> None:
    sc = SCENARIOS[name]
    opts = dict(sc.options)
    opts.setdefault("sweep", os.environ.get("EVAL_SWEEP", "mutations"))
    opts.setdefault("stall_seconds", float(os.environ.get("EVAL_STALL_SECONDS", "20")))
    if os.environ.get("EVAL_BUCKAROO_0156") == "1":
        opts["honor_row_order_hint"] = False

    session = Session(tmp_path, scenario=name, monkeypatch=monkeypatch, **opts)
    with session as s:
        s.note(f"origin: {sc.origin}. {sc.summary}")
        try:
            sc.fn(s)
        finally:
            report = s.report()

    hit = {f.check for f in session.findings}
    report["known"] = sc.known
    report["known_not_reproduced"] = sorted(set(sc.known) - hit)
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    (RUN_DIR / f"{name}.json").write_text(json.dumps(report, indent=2, default=str))

    summary = _summary(name, report, sc.known)
    print("\n" + summary + f"\n  report: {RUN_DIR / (name + '.json')}")
    errors = [f for f in session.findings if f.severity == "error" and f.check not in sc.known]
    assert not errors, summary
