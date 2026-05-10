from __future__ import annotations

from pathlib import Path

import pytest

from pydata_cli.fixtures import write_shoe_orders
from pydata_core import data_dir, ensure_project


@pytest.fixture
def isolated_home(tmp_path: Path, monkeypatch) -> Path:
    """Point PYDATA_HOME at a tmp dir for the duration of the test."""
    monkeypatch.setenv("PYDATA_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def project(isolated_home: Path) -> str:
    name = "test"
    ensure_project(name)
    return name


@pytest.fixture
def orders_parquet(project: str) -> Path:
    """Generate a deterministic shoe-orders fixture under project/data/."""
    return write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=0)


@pytest.fixture
def fresh_companion_app(project: str):
    """Create a companion app bound to the current isolated project."""
    from pydata_companion import create_app

    return create_app(project)
