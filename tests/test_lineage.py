from __future__ import annotations

import json
from pathlib import Path

from tallyman_xorq import (
    build_and_persist,
    catalog_dag,
    catalog_parents,
    read_data_sources,
    read_internal_lineage,
)


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _from_catalog_code(project: str, parent_alias_or_hash: str) -> str:
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent_alias_or_hash!r}, project={project!r})
expr = t.order_by("total")
"""


def test_internal_lineage_has_nodes_and_edges(project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    lin = read_internal_lineage(project, res.content_hash)
    assert lin["nodes"], "expected at least one lineage node"
    # The root should be an Aggregate (the top operation in our test query).
    node_by_id = {n["id"]: n for n in lin["nodes"]}
    assert node_by_id[lin["root"]]["type"] == "Aggregate"
    # There must be a Read node downstream (the data source).
    assert any(n["type"] == "Read" for n in lin["nodes"])
    # Edges connect existing nodes.
    for e in lin["edges"]:
        assert e[0] in node_by_id and e[1] in node_by_id


def test_read_data_sources_lists_input_paths(project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    sources = read_data_sources(project, res.content_hash)
    assert sources, "expected at least one source"
    # Persisted form is the portable placeholder, not the absolute path.
    assert all("${TALLYMAN_PROJECT_ROOT}" in s for s in sources)
    assert any(s.endswith("data/orders.parquet") for s in sources)


def test_no_catalog_parents_for_a_root_entry(project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    assert catalog_parents(project, res.content_hash) == []


def test_from_catalog_records_parent_edge(project: str, orders_parquet: Path):
    # #84: from_catalog composes the parent's *expression* rather than reading a
    # path to the parent's result.parquet, so the edge no longer survives in
    # expr.yaml. The resolved parent hash is recorded in manifest.parents at build
    # time instead, and catalog_parents reads it from there.
    parent = build_and_persist(project, _agg_code(project), prompt="parent")
    child = build_and_persist(project, _from_catalog_code(project, parent.content_hash), prompt="child")
    assert catalog_parents(project, child.content_hash) == [parent.content_hash]


def test_parent_ref_pins_on_a_hash_argument(project: str, orders_parquet: Path):
    # from_catalog("<hash>") is a pin: ref is the literal hash, follow=False.
    from tallyman_core import entry_manifest_path

    parent = build_and_persist(project, _agg_code(project), prompt="parent")
    child = build_and_persist(project, _from_catalog_code(project, parent.content_hash), prompt="child")
    meta = json.loads(entry_manifest_path(project, child.content_hash).read_text())
    assert meta["parents"] == [
        {"hash": parent.content_hash, "ref": parent.content_hash, "follow": False}
    ]


def test_parent_ref_follows_on_an_alias_argument(project: str, orders_parquet: Path):
    # from_catalog("alias") is a follow: ref is the alias name, follow=True, and
    # the recorded hash is the alias's resolved head.
    from tallyman_core import entry_manifest_path
    from tallyman_core.aliases import set_alias

    parent = build_and_persist(project, _agg_code(project), prompt="parent")
    set_alias(project, "orders_by_region", parent.content_hash, expect_exists=False)
    child = build_and_persist(project, _from_catalog_code(project, "orders_by_region"), prompt="child")
    meta = json.loads(entry_manifest_path(project, child.content_hash).read_text())
    assert meta["parents"] == [
        {"hash": parent.content_hash, "ref": "orders_by_region", "follow": True}
    ]
    assert catalog_parents(project, child.content_hash) == [parent.content_hash]


def test_only_direct_parents_recorded_not_grandparents(project: str, orders_parquet: Path):
    # A→B→C: building C reconstructs B's recipe, which re-runs B's from_catalog(A)
    # during import. That transitive resolution must NOT leak A into C's parents —
    # only B, C's direct parent, is recorded.
    a = build_and_persist(project, _agg_code(project), prompt="a")
    b = build_and_persist(project, _from_catalog_code(project, a.content_hash), prompt="b")
    c = build_and_persist(project, _from_catalog_code(project, b.content_hash), prompt="c")
    assert catalog_parents(project, c.content_hash) == [b.content_hash]


def test_catalog_dag_has_inter_entry_edges(project: str, orders_parquet: Path):
    # #84: every entry is a node, and a chained entry surfaces a parent→child edge.
    parent = build_and_persist(project, _agg_code(project))
    child = build_and_persist(project, _from_catalog_code(project, parent.content_hash))

    dag = catalog_dag(project)
    hashes = {n["hash"] for n in dag["nodes"]}
    assert parent.content_hash in hashes
    assert child.content_hash in hashes
    assert {"from": parent.content_hash, "to": child.content_hash} in dag["edges"]


def test_column_lineage_returns_per_column_trees(project: str, orders_parquet: Path):
    from tallyman_xorq import column_lineage

    res = build_and_persist(project, _agg_code(project))
    cols = column_lineage(project, res.content_hash)
    # The agg expression produces region/total/n.
    assert set(cols.keys()) == {"region", "total", "n"}
    # Each tree is a non-empty ASCII string that mentions the source op.
    for col, tree in cols.items():
        assert tree
        assert "Read" in tree or "Field" in tree


def test_column_lineage_traces_through_filter(project: str, orders_parquet: Path):
    from tallyman_xorq import column_lineage

    code = f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
f = t.filter(t.category == "boots")
expr = f.group_by("region").aggregate(total=f.price.sum())
"""
    res = build_and_persist(project, code)
    cols = column_lineage(project, res.content_hash)
    # "total" should trace back through the filter and a Field:price.
    total = cols["total"]
    assert "Filter" in total
    assert "Field:price" in total


def test_column_lineage_missing_hash_returns_empty(project: str):
    from tallyman_xorq import column_lineage

    assert column_lineage(project, "deadbeef") == {}
