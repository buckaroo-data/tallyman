from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from pydata_core import set_alias
from pydata_xorq import build_and_persist
from pydata_xorq.layout import layered_positions


def _agg_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _from_catalog_code(project: str, parent: str) -> str:
    return f"""
from pydata_xorq.io import from_catalog
t = from_catalog({parent!r}, project={project!r})
expr = t.order_by("total")
"""


def test_layered_positions_simple_chain():
    nodes = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    edges = [["a", "b"], ["b", "c"]]
    out = layered_positions(nodes, edges, root="a", x_step=100, y_step=50, margin=10)
    positions = out["positions"]
    assert positions["a"][0] < positions["b"][0] < positions["c"][0]
    assert out["width"] > 0 and out["height"] > 0


def test_layered_positions_dict_edges():
    nodes = [{"id": "a"}, {"id": "b"}]
    edges = [{"from": "a", "to": "b"}]
    out = layered_positions(nodes, edges, root="a", edge_from="from", edge_to="to")
    assert "a" in out["positions"]
    assert "b" in out["positions"]


def test_layered_positions_handles_disconnected_node():
    nodes = [{"id": "a"}, {"id": "b"}, {"id": "orphan"}]
    edges = [["a", "b"]]
    out = layered_positions(nodes, edges, root="a")
    assert "orphan" in out["positions"]


def test_lineage_overview_empty(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.get("/lineage")
    assert r.status_code == 200
    assert "no entries in this project yet" in r.text


def test_lineage_overview_with_chain(fresh_companion_app, project: str, orders_parquet: Path):
    parent = build_and_persist(project, _agg_code(project), prompt="parent")
    set_alias(project, "by_region", parent.content_hash)
    child = build_and_persist(project, _from_catalog_code(project, "by_region"))

    c = TestClient(fresh_companion_app)
    r = c.get("/lineage")
    assert r.status_code == 200
    # SVG nodes for both hashes should appear.
    assert parent.content_hash in r.text or "by_region" in r.text
    assert child.content_hash[:10] in r.text
    # At least one edge should render.
    assert "<line class=\"edge\"" in r.text


def test_lineage_entry_view_renders_internal_dag(fresh_companion_app, project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/lineage/{res.content_hash}")
    assert r.status_code == 200
    assert "Aggregate" in r.text
    assert "Read" in r.text


def test_lineage_entry_404(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    assert c.get("/lineage/deadbeef").status_code == 404


def test_api_catalog_dag_returns_nodes_and_edges(fresh_companion_app, project: str, orders_parquet: Path):
    parent = build_and_persist(project, _agg_code(project))
    child = build_and_persist(project, _from_catalog_code(project, parent.content_hash))
    c = TestClient(fresh_companion_app)
    r = c.get("/api/catalog_dag")
    assert r.status_code == 200
    body = r.json()
    assert len(body["nodes"]) >= 2
    assert {"from": parent.content_hash, "to": child.content_hash} in body["edges"]


def test_api_lineage_returns_internal(fresh_companion_app, project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/api/lineage/{res.content_hash}")
    assert r.status_code == 200
    body = r.json()
    assert "internal" in body
    assert body["internal"]["nodes"]


def test_lineage_entry_page_includes_column_trees(
    fresh_companion_app, project: str, orders_parquet: Path
):
    """T-17: the /lineage/<hash> page now surfaces per-column trees."""
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/lineage/{res.content_hash}")
    assert r.status_code == 200
    assert "column lineage" in r.text
    # All three output columns from the agg expression should be present.
    for col in ("region", "total", "n"):
        assert f"<code>{col}</code>" in r.text
