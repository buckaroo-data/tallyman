from __future__ import annotations

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir
from tallyman_core.aliases import get_alias
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.staleness import entry_staleness, scan


def _base_code(project: str) -> str:  # from_project root over orders.parquet
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _base_code_v2(project: str) -> str:  # a different graph → a new content hash
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select("region", "price").mutate(extra=1)
"""


def _child_code(parent: str) -> str:  # from_catalog child (alias name or literal hash)
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _agg_child_code(parent: str) -> str:  # expensive (Aggregate) child → bakes a snapshot
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.group_by("region").aggregate(total=t.price.sum())
"""


def _const_child_code(parent: str) -> str:  # cheap child, schema-independent
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.mutate(flag=1)
"""


def _hash(result: dict) -> str:
    assert "hash" in result, result
    return result["hash"]


def test_followed_alias_advance_marks_child_stale(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_v1 = _hash(catalog_create("base", _base_code(project)))
    child_hash = _hash(catalog_create("child", _child_code("base")))

    # The followed edge still matches the alias head — not stale yet.
    assert entry_staleness(project, child_hash).stale is False

    _hash(catalog_revise("base", _base_code_v2(project)))
    base_v2 = get_alias(project, "base")
    assert base_v2 != base_v1

    v = entry_staleness(project, child_hash)
    assert v.stale is True
    alias_reasons = [r for r in v.reasons if r.axis == "alias"]
    assert len(alias_reasons) == 1
    assert (alias_reasons[0].ref, alias_reasons[0].was, alias_reasons[0].now) == (
        "base",
        base_v1,
        base_v2,
    )


def test_hash_pinned_parent_is_not_stale_when_alias_advances(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_v1 = _hash(catalog_create("base", _base_code(project)))
    # A literal-hash argument is recorded follow=False (a pin), so the alias
    # advancing must not make this child stale.
    child_hash = _hash(catalog_create("child", _child_code(base_v1)))

    _hash(catalog_revise("base", _base_code_v2(project)))
    assert get_alias(project, "base") != base_v1

    v = entry_staleness(project, child_hash)
    assert v.stale is False
    assert [r for r in v.reasons if r.axis == "alias"] == []


def test_source_edit_marks_entry_stale_under_cas(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_hash = _hash(catalog_create("base", _base_code(project)))
    assert entry_staleness(project, base_hash).stale is False

    # Rewrite the source in place with different data (new bytes, new mtime).
    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=1)

    v = entry_staleness(project, base_hash)
    assert v.stale is True
    source_reasons = [r for r in v.reasons if r.axis == "source"]
    assert len(source_reasons) == 1
    assert source_reasons[0].ref == "orders.parquet"


def test_off_mode_reports_source_axis_unknown_not_fresh(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    base_hash = _hash(catalog_create("base", _base_code(project)))

    v = entry_staleness(project, base_hash)
    assert v.stale is False
    assert "source" in v.unknown_axes

    # off mode records no source digests, so it cannot see drift — unknown, not stale.
    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=1)
    v2 = entry_staleness(project, base_hash)
    assert v2.stale is False
    assert "source" in v2.unknown_axes


def test_scan_distinguishes_direct_from_transitive_staleness(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a_v1 = _hash(catalog_create("a", _base_code(project)))
    # b is an expensive (Aggregate) intermediate: reconstructing it reads its
    # baked snapshot rather than re-running its recipe, so its from_catalog("a")
    # edge does not leak into c. (A cheap intermediate re-runs inline, so c would
    # follow "a" directly and there would be no transitive-only node.)
    b = _hash(catalog_create("b", _agg_child_code("a")))
    c = _hash(catalog_create("c", _const_child_code("b")))

    # Advance alias "a". b follows "a" so it is DIRECTLY stale; c follows "b"
    # (unchanged), so it is only TRANSITIVELY stale via its ancestor b.
    _hash(catalog_revise("a", _base_code_v2(project)))
    assert get_alias(project, "a") != a_v1

    verdicts = scan(project)
    assert verdicts[b].stale is True
    assert verdicts[b].transitively_stale is False
    assert verdicts[c].stale is False
    assert verdicts[c].transitively_stale is True
