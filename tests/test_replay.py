"""T-19: `tallyman replay` against a JSON storyboard."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from tallyman_cli.main import cli
from tallyman_core import get_alias, history_for, notebook


def _storyboard(project: str, parquet: Path) -> dict:
    return {
        "project": project,
        "steps": [
            {
                "tool": "catalog_load_parquet",
                "args": {"rel_path": "orders.parquet", "name": "orders", "prompt": "raw"},
                "narration": "load the parquet",
            },
            {
                "tool": "catalog_create",
                "args": {
                    "name": "by_region",
                    "code": "from tallyman_xorq.io import read_project_file\nt = read_project_file('orders.parquet')\nexpr = t.group_by('region').aggregate(n=t.count())\n",  # noqa: E501
                    "prompt": "by region",
                },
            },
            {
                "tool": "catalog_revise",
                "args": {
                    "name": "by_region",
                    "code": "from tallyman_xorq.io import read_project_file\nt = read_project_file('orders.parquet')\nf = t.filter(t.category == 'boots')\nexpr = f.group_by('region').aggregate(n=f.count())\n",  # noqa: E501
                    "prompt": "boots only",
                },
            },
        ],
    }


def test_replay_runs_all_steps(project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path, monkeypatch):
    sb_file = tmp_path / "demo.json"
    sb_file.write_text(json.dumps(_storyboard(project, orders_parquet)))

    runner = CliRunner()
    result = runner.invoke(cli, ["replay", str(sb_file)])
    assert result.exit_code == 0, result.output

    # All three tool calls landed.
    assert get_alias(project, "orders") is not None
    assert get_alias(project, "by_region") is not None
    assert len(history_for(project, "by_region")) == 2
    # Notebook auto-appended both named entries.
    cells = notebook.load(project)["cells"]
    aliases = [c["alias"] for c in cells]
    assert "orders" in aliases and "by_region" in aliases


def test_replay_stops_on_error_by_default(project: str, orders_parquet: Path, tmp_path: Path, monkeypatch):
    sb = {
        "project": project,
        "steps": [
            {"tool": "catalog_run", "args": {"code": "expr = nope", "prompt": "broken"}},
            {"tool": "catalog_load_parquet", "args": {"rel_path": "orders.parquet"}},
        ],
    }
    sb_file = tmp_path / "demo.json"
    sb_file.write_text(json.dumps(sb))
    runner = CliRunner()
    result = runner.invoke(cli, ["replay", str(sb_file)])
    assert result.exit_code != 0
    assert "step failed" in result.output


def test_replay_continue_on_error(project: str, orders_parquet: Path, tmp_path: Path, monkeypatch):
    sb = {
        "project": project,
        "steps": [
            {"tool": "catalog_run", "args": {"code": "expr = nope"}},
            {"tool": "catalog_load_parquet", "args": {"rel_path": "orders.parquet"}},
        ],
    }
    sb_file = tmp_path / "demo.json"
    sb_file.write_text(json.dumps(sb))
    runner = CliRunner()
    result = runner.invoke(cli, ["replay", str(sb_file), "--continue-on-error"])
    assert result.exit_code == 0
    assert "1 failure" in result.output


def test_replay_skip_field(project: str, orders_parquet: Path, tmp_path: Path, monkeypatch):
    sb = {
        "project": project,
        "steps": [
            {"tool": "catalog_load_parquet", "args": {"rel_path": "orders.parquet"}},
            {"tool": "catalog_run", "args": {"code": "won't run"}, "skip": True},
        ],
    }
    sb_file = tmp_path / "demo.json"
    sb_file.write_text(json.dumps(sb))
    runner = CliRunner()
    result = runner.invoke(cli, ["replay", str(sb_file)])
    assert result.exit_code == 0
    assert "(skipped" in result.output


def test_replay_unknown_tool(project: str, tmp_path: Path):
    sb = {"project": project, "steps": [{"tool": "no_such_tool", "args": {}}]}
    sb_file = tmp_path / "demo.json"
    sb_file.write_text(json.dumps(sb))
    runner = CliRunner()
    result = runner.invoke(cli, ["replay", str(sb_file)])
    assert result.exit_code != 0
    assert "unknown tool" in result.output.lower()


def test_canonical_demo_storyboard_replays_to_completion(project: str, orders_parquet: Path, monkeypatch):
    """Sanity-check the committed `demo/storyboard.json`. Project name is
    overridden so we hit the isolated test fixture rather than the real
    `spike` project."""
    repo_root = Path(__file__).resolve().parent.parent
    sb_path = repo_root / "demo" / "storyboard.json"
    assert sb_path.exists(), "demo/storyboard.json is missing"
    runner = CliRunner()
    result = runner.invoke(cli, ["replay", str(sb_path), "--project", project])
    assert result.exit_code == 0, result.output
    assert get_alias(project, "shoe_sales") is not None
    # Two revisions of shoe_sales (V1 + V2) per the storyboard.
    assert len(history_for(project, "shoe_sales")) == 2
