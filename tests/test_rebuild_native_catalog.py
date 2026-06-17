"""Round-trip test for ``scripts/rebuild_native_catalog.py``.

Builds a small native catalog (an alias with a revision, a from_catalog child,
a chart, prompt history), rebuilds it through the script's importable core, and
asserts the rebuild re-execs every recipe, reconstructs the decomposed surface,
and leaves a consistent native store. A same-path rebuild is hash-stable (the
build pipeline is deterministic), so the entry set, alias history, and chart key
are preserved exactly.

The script is loaded via importlib (scripts/ is not a package), mirroring how
test_perf_integration.py loads scripts/perf_report.py.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from tallyman_core import aliases as al
from tallyman_core import catalog
from tallyman_core import catalog_state as cs
from tallyman_core.charts import get_chart
from tallyman_xorq import build_and_persist, read_prompts
from tallyman_xorq.result_cache import cached_result_expr

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_rebuild():
    path = _REPO_ROOT / "scripts" / "rebuild_native_catalog.py"
    spec = importlib.util.spec_from_file_location("rebuild_native_catalog", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = mod  # so @dataclass can resolve cls.__module__ under importlib load
    spec.loader.exec_module(mod)
    return mod


def _agg(parquet: Path, *, avg: bool = False) -> str:
    extra = ", avg=t.price.mean()" if avg else ""
    return (
        "import xorq.api as xo\n"
        f"t = xo.deferred_read_parquet({str(parquet)!r})\n"
        f"expr = t.group_by('region').aggregate(n=t.count(){extra})\n"
    )


def _child(alias: str, project: str) -> str:
    return (
        "from tallyman_xorq import from_catalog\n"
        f"expr = from_catalog({alias!r}, {project!r}).filter(lambda r: r.n > 0)\n"
    )


def _build_corpus(project: str, parquet: Path) -> dict:
    cs.genesis(project)
    a = build_and_persist(project, _agg(parquet), prompt="agg by region")
    al.set_alias(project, "regions", a.content_hash)
    a2 = build_and_persist(project, _agg(parquet, avg=True), prompt="add avg")
    al.set_alias(project, "regions", a2.content_hash)  # revision: history [a, a2]
    b = build_and_persist(project, _child("regions", project), prompt="filter regions")
    from tallyman_core.charts import set_chart

    set_chart(project, a2.content_hash, {"mark": "bar", "encoding": {"x": {"field": "region"}}})
    cs.checkpoint_catalog(project, "corpus")
    return {"a": a.content_hash, "a2": a2.content_hash, "b": b.content_hash}


def test_rebuild_preserves_entries_aliases_charts_prompts(project, orders_parquet):
    rb = _load_rebuild()
    before = _build_corpus(project, orders_parquet)

    remap = rb.rebuild_project(project, log=lambda *a: None)

    # Same-path rebuild is hash-stable: every entry rebuilt to its own hash.
    assert set(remap) == set(before.values())
    assert all(remap[h] == h for h in before.values()), remap

    # Native store is internally consistent after the rebuild.
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    assert pointers == set(before.values())
    catalog.assert_catalog_consistent(project, pointers)

    # Alias + its full revision history survived the round-trip.
    assert al.get_alias(project, "regions") == before["a2"]
    assert al.history_for(project, "regions") == [before["a"], before["a2"]]

    # Chart re-attached to the (preserved) hash; prompts carried across.
    assert get_chart(project, before["a2"]) is not None
    assert [p["prompt"] for p in read_prompts(project, before["a2"])] == ["add avg"]

    # Every rebuilt entry — including the from_catalog child — reloads.
    for h in before.values():
        assert len(cached_result_expr(project, h).execute()) >= 0


def test_rebuild_dry_run_writes_nothing(project, orders_parquet):
    rb = _load_rebuild()
    before = _build_corpus(project, orders_parquet)
    head_before = cs.list_revisions(project)

    remap = rb.rebuild_project(project, dry_run=True, log=lambda *a: None)

    assert remap == {}  # nothing rebuilt
    # Catalog untouched: same entries, same revision timeline.
    assert set(cs.read_tallyman_state(project)["entry_hashes"]) == set(before.values())
    assert cs.list_revisions(project) == head_before


def test_rebuild_toposort_orders_parents_before_children(project, orders_parquet):
    rb = _load_rebuild()
    recipes = {
        "child1": "from tallyman_xorq import from_catalog\nexpr = from_catalog('base')\n",
        "base000child": "import xorq.api as xo\nexpr = xo.memtable({'a': [1]})\n",
    }
    # "base" alias points at the root hash; the child must sort after it.
    order = rb.toposort(recipes, {"base": "base000child"})
    assert order.index("base000child") < order.index("child1")
