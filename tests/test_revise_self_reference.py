from __future__ import annotations

from tallyman_mcp.server import catalog_create, catalog_revise

# #135: a revision of alias X must not reference X *by name*
# (tracked_expr_from_alias("X") / pinned_expr_from_alias("X")) — that makes the
# code an opaque self-reference up a chain of identically-named versions instead
# of a self-contained recipe you can read in one view. catalog_revise rejects it
# and points at the two good shapes: inline the source (source-shaped) or pin the
# previous version by its content hash (expensive). A by-hash reference stays
# legal.


def _src(project: str, cols: str = '"region", "price"') -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select({cols})
"""


def _self_alias(project: str, alias: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({alias!r}, project={project!r})
expr = t.mutate(region2=t.region)
"""


def _pin_by_hash(project: str, prev_hash: str) -> str:
    return f"""
from tallyman_xorq.io import pinned_expr_from_alias
t = pinned_expr_from_alias({prev_hash!r}, project={project!r})
expr = t.mutate(region2=t.region)
"""


def test_revise_rejects_self_alias_reference(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    v1 = catalog_create("parking", _src(project))
    out = catalog_revise("parking", _self_alias(project, "parking"))
    assert "error" in out, out
    assert "parking" in out["error"]
    # The alias must not have advanced to the self-referential version.
    from tallyman_core import get_alias

    assert get_alias(project, "parking") == v1["hash"]


def test_revise_allows_pin_by_hash_of_prior_version(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    v1 = catalog_create("parking", _src(project))
    out = catalog_revise("parking", _pin_by_hash(project, v1["hash"]))
    assert "error" not in out, out
    assert out.get("version") == 2


def test_revise_allows_inlined_source(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("parking", _src(project))
    out = catalog_revise("parking", _src(project, cols='"region"'))  # inline, no alias ref
    assert "error" not in out, out
    assert out.get("version") == 2
