"""Tests for `pydata pack`: tar a project dir into a portable .tgz."""

from __future__ import annotations

import tarfile
from pathlib import Path

from click.testing import CliRunner
from fastapi.testclient import TestClient

from pydata_cli.main import cli
from pydata_companion import create_app
from pydata_core import set_alias
from pydata_xorq import build_and_persist


def _build_one(project: str, orders_parquet: Path) -> str:
    code = f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    res = build_and_persist(project, code, prompt="by region")
    return res.content_hash


def test_pack_creates_tgz(project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path):
    h = _build_one(project, orders_parquet)
    set_alias(project, "by_region", h)

    out_path = tmp_path / "spike.tgz"
    runner = CliRunner()
    result = runner.invoke(cli, ["pack", project, "-o", str(out_path)])
    assert result.exit_code == 0, result.output
    assert out_path.exists()

    with tarfile.open(out_path) as tar:
        names = tar.getnames()
    assert f"{project}/artifacts/catalog/aliases.json" in names
    assert f"{project}/data/orders.parquet" in names
    assert any(f"{project}/artifacts/catalog/entries/{h}/" in n for n in names)


def test_pack_skips_buckaroo_sessions(project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path):
    # Write a stale sessions file that mustn't ship.
    from pydata_core import catalog_dir

    _build_one(project, orders_parquet)
    sessions_file = catalog_dir(project) / "buckaroo_sessions.json"
    sessions_file.write_text("{}")
    assert sessions_file.exists()

    out_path = tmp_path / "spike.tgz"
    runner = CliRunner()
    result = runner.invoke(cli, ["pack", project, "-o", str(out_path)])
    assert result.exit_code == 0
    with tarfile.open(out_path) as tar:
        names = tar.getnames()
    assert not any("buckaroo_sessions.json" in n for n in names)


def test_pack_unknown_project(isolated_home: Path, tmp_path: Path):
    out_path = tmp_path / "bad.tgz"
    runner = CliRunner()
    result = runner.invoke(cli, ["pack", "no-such-project", "-o", str(out_path)])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_pack_round_trip_serves(project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path, monkeypatch):
    """The full handoff loop: pack → extract to a random location →
    point PYDATA_PROJECT_PATH at the extract → serve read-only and verify
    every alias resolves."""
    h = _build_one(project, orders_parquet)
    set_alias(project, "by_region", h)

    out_path = tmp_path / "bundle.tgz"
    runner = CliRunner()
    runner.invoke(cli, ["pack", project, "-o", str(out_path)])

    handoff = tmp_path / "handoff"
    handoff.mkdir()
    with tarfile.open(out_path) as tar:
        tar.extractall(handoff, filter="data")
    extracted_project = handoff / project
    assert extracted_project.is_dir()

    # Point the companion at the extracted dir, NOT the original PYDATA_HOME.
    monkeypatch.setenv("PYDATA_HOME", str(tmp_path / "empty-home"))
    monkeypatch.setenv("PYDATA_PROJECT", project)
    monkeypatch.setenv("PYDATA_PROJECT_PATH", str(extracted_project))

    app = create_app(project, read_only=True)
    c = TestClient(app)
    r = c.get("/catalog")
    assert r.status_code == 200
    assert "by_region" in r.text
    r = c.get(f"/catalog/{h}")
    assert r.status_code == 200
