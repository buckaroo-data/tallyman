"""``scripts/profile_pageload.py`` runs against a corpus this branch builds.

The script borrows its helpers from the Tier-B harness, which CI never runs (it is marked ``perf`` and needs the real
parking corpus), so nothing caught the script when those helpers or the on-disk layout changed. These tests build a
two-entry corpus in the test's home and run the script as a user would, reading the report it prints.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tallyman_mcp.server import catalog_create
from tallyman_xorq.result_cache import cached_result_expr

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "profile_pageload.py"


def _hash(res: dict) -> str:
    assert "error" not in res, res
    return res["hash"]


@pytest.fixture
def corpus(project, orders_src, isolated_home, monkeypatch) -> dict[str, tuple[str, int]]:
    """A cheap root entry and a worthy aggregate over the same source alias: alias -> (hash, rows)."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    orders = _hash(
        catalog_create(
            "orders",
            f"""
from tallyman_xorq.io import tracked_expr_from_alias
expr = tracked_expr_from_alias({orders_src!r})
""",
        )
    )
    agg = _hash(
        catalog_create(
            "agg",
            f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
""",
        )
    )
    # The source version is an ordinary entry (ADR-011 D1), so the corpus is three entries, not two:
    # the profiler measures the imported rows the same way it measures anything built from them.
    from tallyman_core.aliases import get_alias

    return {
        alias: (h, int(cached_result_expr(project, h).count().execute()))
        for alias, h in (("orders", orders), ("agg", agg), (orders_src, get_alias(project, orders_src)))
    }


def _run(project: str, home: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "TALLYMAN_PERF_HOME": str(home)}
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args, "--project", project],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def _report_rows(stdout: str) -> dict[str, list[str]]:
    """The report's table, keyed on the entry cell (``<alias> `<hash8>```)."""
    rows = {}
    for line in stdout.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if line.startswith("| ") and "`" in cells[0]:
            rows[cells[0]] = cells[1:]
    return rows


def test_the_profiler_measures_every_entry_of_a_corpus(project, isolated_home, corpus):
    """``--all`` finds both entries, names them by alias, and pages each one without an error."""
    proc = _run(project, isolated_home, "--all")
    assert proc.returncode == 0, proc.stderr
    assert "ERROR" not in proc.stderr, proc.stderr
    rows = _report_rows(proc.stdout)
    assert set(rows) == {f"{alias} `{h[:8]}`" for alias, (h, _) in corpus.items()}, proc.stdout
    for alias, (h, n) in corpus.items():
        assert rows[f"{alias} `{h[:8]}`"][0] == f"{n:,}"


def test_the_profiler_resolves_an_entry_by_its_alias(project, isolated_home, corpus):
    """An alias on the command line picks that entry, and ``--profile`` attributes its time."""
    h, n = corpus["agg"]
    proc = _run(project, isolated_home, "agg", "--profile")
    assert proc.returncode == 0, proc.stderr
    assert "ERROR" not in proc.stderr, proc.stderr
    rows = _report_rows(proc.stdout)
    assert list(rows) == [f"agg `{h[:8]}`"], proc.stdout
    assert rows[f"agg `{h[:8]}`"][0] == f"{n:,}"
