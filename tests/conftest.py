from __future__ import annotations

import importlib
from pathlib import Path

import pytest

import pydata_core.paths as paths_mod
from pydata_cli.fixtures import write_shoe_orders


@pytest.fixture
def isolated_home(tmp_path: Path, monkeypatch) -> Path:
    """Point PYDATA_HOME at a tmp dir for the duration of the test."""
    monkeypatch.setenv("PYDATA_HOME", str(tmp_path))
    monkeypatch.setattr(paths_mod, "PYDATA_HOME", tmp_path)
    return tmp_path


@pytest.fixture
def project(isolated_home: Path) -> str:
    name = "test"
    paths_mod.ensure_project(name)
    return name


@pytest.fixture
def orders_parquet(project: str) -> Path:
    """Generate a deterministic shoe-orders fixture under project/data/."""
    return write_shoe_orders(paths_mod.data_dir(project) / "orders.parquet", n_rows=200, seed=0)


@pytest.fixture
def fresh_companion_app(project: str):
    """Reload pydata_companion.app so it picks up the patched PYDATA_HOME."""
    from pydata_companion import app as app_mod

    importlib.reload(app_mod)
    return app_mod.create_app(project)
