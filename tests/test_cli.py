"""CLI behaviors that don't fit elsewhere."""
from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from pydata_cli.main import cli
from pydata_core import project_dir


def test_init_creates_project(isolated_home: Path):
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "alpha", "--no-fixture"])
    assert result.exit_code == 0
    assert (project_dir("alpha") / "artifacts" / "catalog" / "entries").is_dir()


def test_init_rejects_existing_project(isolated_home: Path):
    runner = CliRunner()
    runner.invoke(cli, ["init", "alpha", "--no-fixture"])
    result = runner.invoke(cli, ["init", "alpha", "--no-fixture"])
    assert result.exit_code != 0
    assert "already exists" in result.output
    assert "--force" in result.output


def test_init_force_re_initialises(isolated_home: Path):
    runner = CliRunner()
    runner.invoke(cli, ["init", "alpha", "--no-fixture"])
    result = runner.invoke(cli, ["init", "alpha", "--no-fixture", "--force"])
    assert result.exit_code == 0
    assert "re-initialised" in result.output
