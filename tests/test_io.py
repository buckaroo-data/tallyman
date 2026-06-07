from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_xorq import ProjectDataNotFound, from_project, project_path


def test_project_path_resolves(orders_parquet: Path, project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = project_path("orders.parquet")
    assert p == orders_parquet
    assert p.is_file()


def test_project_path_missing(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    with pytest.raises(ProjectDataNotFound):
        project_path("nope.parquet")


def test_project_path_traversal_blocked(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    with pytest.raises(ProjectDataNotFound):
        project_path("../../etc/passwd")


def test_from_project_returns_xorq_expr(orders_parquet: Path, project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    expr = from_project("orders.parquet")
    schema = expr.schema()
    assert "region" in schema.names
    assert "price" in schema.names
