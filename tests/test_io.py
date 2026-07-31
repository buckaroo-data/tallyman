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


# ---------------------------------------------------------------------------
# #166 — pinned_expr_from_alias requires a hash or "<alias>-v<N>", never a
# bare alias (a bare alias silently pins whatever the head happened to be
# when the recipe was built; the recipe text under-determines the entry).
# ---------------------------------------------------------------------------


def _parent_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "category", "price")
"""


def _parent_v2_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.filter(t.category == "boots").select("region", "category", "price")
"""


def _pin_child_code(ref: str) -> str:
    return f"""
from tallyman_xorq.io import pinned_expr_from_alias
t = pinned_expr_from_alias({ref!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _two_versions(project: str, monkeypatch) -> tuple[str, str]:
    """An alias "trips" with two revisions; returns (v1_hash, v2_hash)."""
    from tallyman_core.aliases import get_alias, history_for
    from tallyman_mcp.server import catalog_create, catalog_revise

    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    v1 = catalog_create("trips", _parent_code(project))["hash"]
    catalog_revise("trips", _parent_v2_code(project))
    v2 = get_alias(project, "trips")
    assert history_for(project, "trips") == [v1, v2]
    return v1, v2


def test_pinned_bare_alias_rejected(orders_parquet: Path, project: str, monkeypatch):
    """A bare alias reads like a pin but resolves to the build-time head — the
    recipe text under-determines the entry. The error must name both accepted
    forms so the author can self-correct."""
    from tallyman_mcp.server import catalog_create

    _two_versions(project, monkeypatch)
    res = catalog_create("child", _pin_child_code("trips"))
    assert "error" in res, res
    assert "-v" in res["error"] and "hash" in res["error"], res["error"]


def test_pinned_version_ref_resolves_history(orders_parquet: Path, project: str, monkeypatch):
    """"trips-v1" denotes the first revision forever, regardless of when the
    recipe is built — the pin is self-describing."""
    from tallyman_core.manifest import read_manifest
    from tallyman_core.paths import entry_dir
    from tallyman_mcp.server import catalog_create

    v1, v2 = _two_versions(project, monkeypatch)
    res = catalog_create("child", _pin_child_code("trips-v1"))
    assert "error" not in res, res
    parents = read_manifest(entry_dir(project, res["hash"])).parents
    assert parents and parents[0].hash == v1 and parents[0].follow is False
    assert parents[0].hash != v2


def test_pinned_version_ref_out_of_range_names_count(orders_parquet: Path, project: str, monkeypatch):
    """A version past the history's end must say how many versions exist, not
    report a generic not-found."""
    from tallyman_mcp.server import catalog_create

    _two_versions(project, monkeypatch)
    res = catalog_create("child", _pin_child_code("trips-v9"))
    assert "error" in res, res
    assert "2 version" in res["error"], res["error"]


def test_alias_name_matching_version_syntax_rejected(orders_parquet: Path, project: str, monkeypatch):
    """An alias literally named "foo-v2" would collide with version-reference
    syntax; reject it at creation and at rename (#166's cheapest closure),
    steering a parallel-take to the "-o<N>" (option) convention."""
    from tallyman_mcp.server import catalog_create, catalog_rename

    res = catalog_create("orders-v2", _parent_code(project))
    assert "error" in res, res
    assert "orders-o2" in res["error"], res["error"]

    ok = catalog_create("orders", _parent_code(project))
    assert "error" not in ok, ok
    renamed = catalog_rename("orders", "orders-v3")
    assert "error" in renamed, renamed
    assert "orders-o3" in renamed["error"], renamed["error"]
