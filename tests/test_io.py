from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_xorq import ProjectDataNotFound, project_path, read_project_file


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


def test_tracked_expr_from_alias_rejects_a_hash(orders_parquet: Path, project: str, monkeypatch):
    """docs/reactive-recalc.md: ``tracked_expr_from_alias`` accepts an alias only
    — "pass it a hash and it raises, because a hash has no head to follow". The
    hash form must not silently degrade into an untracked/pinned read."""
    from tallyman_xorq import build_and_persist
    from tallyman_xorq.io import tracked_expr_from_alias

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = 'from tallyman_xorq.io import read_project_file\nexpr = read_project_file("orders.parquet")\n'
    content_hash = build_and_persist(project, code).content_hash

    with pytest.raises(Exception):
        tracked_expr_from_alias(content_hash, project=project)
