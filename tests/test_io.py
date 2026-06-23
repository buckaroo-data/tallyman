from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_xorq import ProjectDataNotFound, read_project_file, project_path


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


def test_read_project_file_returns_xorq_expr(orders_parquet: Path, project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    expr = read_project_file("orders.parquet")
    schema = expr.schema()
    assert "region" in schema.names
    assert "price" in schema.names
