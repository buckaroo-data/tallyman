from __future__ import annotations

import pytest

from tallyman_core.manifest import Manifest, ParentRef, write_manifest
from tallyman_core.paths import entry_dir
from tallyman_xorq.dependents import (
    DependencyCycleError,
    build_dag,
    dependents_index,
    descendant_cone,
    parents_of,
    sources_of,
)


def _write_entry(project: str, content_hash: str, parents=None, sources=None) -> str:
    """Write a minimal entry (manifest.json only) with the given parent edges.

    The dependents reader walks ``manifest.parents`` and never executes a
    recipe, so a hand-written manifest is a faithful and fast graph fixture —
    no xorq build required. ``parents`` is a list of ``(hash, ref, follow)``;
    ``sources`` is the recorded ``{rel_path: digest}`` map (``None`` = built
    under identity ``off``, the default).
    """
    d = entry_dir(project, content_hash)
    d.mkdir(parents=True, exist_ok=True)
    refs = [ParentRef(hash=h, ref=r, follow=f) for (h, r, f) in (parents or [])]
    write_manifest(d, Manifest(content_hash=content_hash, project=project, parents=refs or None, sources=sources))
    return content_hash


def test_parents_of_root_is_empty(project):
    _write_entry(project, "root")
    assert parents_of(project, "root") == []


def test_parents_of_returns_recorded_edges(project):
    _write_entry(project, "root")
    _write_entry(project, "child", [("root", "root", True)])
    edges = parents_of(project, "child")
    assert [(e.hash, e.ref, e.follow) for e in edges] == [("root", "root", True)]


def test_sources_of_none_when_unrecorded(project):
    # Built under identity off (sources never recorded) — None, not {}, so the
    # staleness source axis reports "unknown" rather than silently "fresh".
    _write_entry(project, "root")
    assert sources_of(project, "root") is None


def test_sources_of_empty_dict_is_preserved(project):
    # Identity on, but the recipe read no raw files: {} is distinct from None.
    _write_entry(project, "noraw", sources={})
    assert sources_of(project, "noraw") == {}


def test_sources_of_returns_recorded_digests(project):
    _write_entry(project, "leaf", sources={"orders.parquet": "abc123"})
    assert sources_of(project, "leaf") == {"orders.parquet": "abc123"}


def test_build_dag_maps_every_live_entry(project):
    _write_entry(project, "a")
    _write_entry(project, "b", [("a", "a", True)])
    dag = build_dag(project)
    assert set(dag) == {"a", "b"}
    assert dag["a"] == []
    assert [e.hash for e in dag["b"]] == ["a"]


def test_dependents_index_inverts_the_dag(project):
    _write_entry(project, "a")
    _write_entry(project, "b", [("a", "a", True)])
    _write_entry(project, "c", [("a", "a", True)])
    idx = dependents_index(project)
    assert idx == {"a": {"b", "c"}}


def test_descendant_cone_three_level_chain_is_topologically_ordered(project):
    _write_entry(project, "a")
    _write_entry(project, "b", [("a", "a", True)])
    _write_entry(project, "c", [("b", "b", True)])
    assert descendant_cone(project, ["a"]) == ["a", "b", "c"]


def test_descendant_cone_diamond_orders_parents_before_children(project):
    _write_entry(project, "a")
    _write_entry(project, "b", [("a", "a", True)])
    _write_entry(project, "c", [("a", "a", True)])
    _write_entry(project, "d", [("b", "b", True), ("c", "c", True)])
    cone = descendant_cone(project, ["a"])
    assert set(cone) == {"a", "b", "c", "d"}
    assert cone[0] == "a"
    assert cone[-1] == "d"
    assert cone.index("b") < cone.index("d")
    assert cone.index("c") < cone.index("d")


def test_descendant_cone_includes_both_pinned_and_followed_children(project):
    _write_entry(project, "a")
    _write_entry(project, "followed", [("a", "a", True)])
    _write_entry(project, "pinned", [("a", "a", False)])
    cone = descendant_cone(project, ["a"])
    assert set(cone) == {"a", "followed", "pinned"}


def test_descendant_cone_detects_a_cycle_instead_of_hanging(project):
    _write_entry(project, "a", [("b", "b", True)])
    _write_entry(project, "b", [("a", "a", True)])
    with pytest.raises(DependencyCycleError):
        descendant_cone(project, ["a"])


def test_descendant_cone_of_a_leaf_is_just_the_leaf(project):
    _write_entry(project, "solo")
    assert descendant_cone(project, ["solo"]) == ["solo"]
