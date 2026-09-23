"""Round-trip test for ``scripts/rebuild_native_catalog.py``.

Builds a small native catalog (an alias with a revision, a tracked_expr_from_alias child,
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
        "from tallyman_xorq.io import read_project_file\n"
        f"t = read_project_file({parquet.name!r})\n"
        f"expr = t.group_by('region').aggregate(n=t.count(){extra})\n"
    )


def _child(alias: str, project: str) -> str:
    return (
        "from tallyman_xorq import tracked_expr_from_alias\n"
        f"expr = tracked_expr_from_alias({alias!r}, {project!r}).filter(lambda r: r.n > 0)\n"
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

    # Every original entry was rebuilt.
    assert set(remap) == set(before.values())
    # A same-path rebuild is hash-stable for the recipe-deterministic roots. The
    # tracked_expr_from_alias child bakes its aggregate parent's snapshot, whose bytes are
    # not datafusion-order-deterministic, so its hash may drift — the rebuild
    # remaps it (and re-points aliases) rather than relying on exact preservation.
    assert remap[before["a"]] == before["a"]
    assert remap[before["a2"]] == before["a2"]
    new_hashes = set(remap.values())

    # Native store is internally consistent after the rebuild.
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    assert pointers == new_hashes
    catalog.assert_catalog_consistent(project, pointers)

    # Alias + its full revision history survived the round-trip (stable roots).
    assert al.get_alias(project, "regions") == before["a2"]
    assert al.history_for(project, "regions") == [before["a"], before["a2"]]

    # Chart re-attached to the (preserved) hash; prompts carried across.
    assert get_chart(project, before["a2"]) is not None
    assert [p["prompt"] for p in read_prompts(project, before["a2"])] == ["add avg"]

    # Every rebuilt entry — including the tracked_expr_from_alias child — reloads.
    for h in new_hashes:
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
        "child1": "from tallyman_xorq import tracked_expr_from_alias\nexpr = tracked_expr_from_alias('base')\n",
        "base000child": "import xorq.api as xo\nexpr = xo.memtable({'a': [1]})\n",
    }
    # "base" alias points at the root hash; the child must sort after it.
    order = rb.toposort(recipes, {"base": "base000child"})
    assert order.index("base000child") < order.index("child1")


def test_toposort_self_chaining_revise_resolves_to_prior_revision(project):
    """A revision that chains off its own alias (the documented self-chaining
    revise, #74) depends on the PRIOR revision, not itself — otherwise the
    dependency graph has a false self-cycle and the topo sort cannot proceed."""
    rb = _load_rebuild()
    recipes = {
        "v1aaaaaaaaaa": "import xorq.api as xo\nexpr = xo.memtable({'x': [1]})\n",
        "v2bbbbbbbbbb": "from tallyman_xorq import tracked_expr_from_alias\nexpr = tracked_expr_from_alias('a')\n",
    }
    aliases = {"a": "v2bbbbbbbbbb"}  # latest is v2
    history = {"a": ["v1aaaaaaaaaa", "v2bbbbbbbbbb"]}
    # v2's self-alias-ref resolves to v1 (its build-time parent), not itself.
    assert rb.parse_deps(recipes["v2bbbbbbbbbb"], "v2bbbbbbbbbb", aliases, history, set(recipes)) == {"v1aaaaaaaaaa"}
    assert rb.toposort(recipes, aliases, history) == ["v1aaaaaaaaaa", "v2bbbbbbbbbb"]


def test_read_old_catalog_reads_catalog_yaml_era(project):
    """Most of the real corpus is catalog.yaml-era: aliases live in
    alias_map/alias_history and charts/pp/stats/notebook are embedded in
    catalog.yaml (no aliases.json, no chart_specs files). read_old_catalog must
    read all of it, not just the aliases.json layout."""
    import yaml

    from tallyman_core.paths import catalog_dir, ensure_project, entry_dir

    rb = _load_rebuild()
    ensure_project(project)
    cd = catalog_dir(project)
    cd.mkdir(parents=True, exist_ok=True)
    for h in ("aaaa11112222", "bbbb33334444"):
        ed = entry_dir(project, h)
        ed.mkdir(parents=True)
        (ed / "expr.py").write_text("expr = 1\n")
        (ed / "manifest.json").write_text("{}")
    (cd / "catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "alias_map": {"thing": "bbbb33334444"},
                "alias_history": {"thing": ["aaaa11112222", "bbbb33334444"]},
                "charts": [{"content_hash": "bbbb33334444", "spec": {"mark": "bar"}}],
                "post_processing": [{"name": "pp1", "source": "def process(t):\n    return t\n"}],
                "stats": [{"name": "st1", "source": "def compute(c):\n    return c.count()\n"}],
                "notebook": {"cells": [{"cell_id": "x", "alias": "thing", "markdown": "note"}]},
            }
        )
    )
    oc = rb.read_old_catalog(project)
    assert oc.aliases == {"thing": "bbbb33334444"}
    assert oc.history == {"thing": ["aaaa11112222", "bbbb33334444"]}
    assert "bbbb33334444" in oc.charts
    assert oc.post_processing.get("pp1")
    assert oc.stats.get("st1")
    assert [c["alias"] for c in oc.notebook_cells] == ["thing"]


def test_rebuild_replays_a_source_entry_as_an_import(project, tmp_path):
    """A source entry has no recipe to re-exec, so the rebuild imports it again (ADR-011).

    ``build_and_persist`` cannot rebuild one: the generated recipe reads the entry's own snapshot, which
    the rebuild has just wiped, and a raw read is a build error anywhere else. The replay therefore goes
    through ``update_and_depend``, from the provenance path when it still holds the imported bytes and
    from the clone in ``data/.cas`` when it does not. The hash is a function of the bytes and the reader
    options, so it survives the round-trip unchanged even though the outside file is gone.
    """
    import pandas as pd

    from tallyman_core.aliases import SOURCE_KIND, alias_kind, history_for
    from tallyman_core.manifest import read_manifest
    from tallyman_core.paths import entry_dir
    from tallyman_xorq.source_import import update_and_depend

    rb = _load_rebuild()
    cs.genesis(project)
    outside = tmp_path / "outside"
    outside.mkdir(parents=True, exist_ok=True)
    src = outside / "orders.parquet"
    pd.DataFrame({"region": ["n", "s", "n"], "price": [1.0, 2.0, 3.0]}).to_parquet(src)

    v1 = update_and_depend(src, "orders", project=project)
    child = build_and_persist(
        project,
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        f"t = tracked_expr_from_alias('orders', {project!r})\n"
        "expr = t.group_by('region').aggregate(n=t.count())\n",
        prompt="count by region",
    )
    al.set_alias(project, "regions", child.content_hash)
    cs.checkpoint_catalog(project, "corpus")
    src.unlink()  # the outside file is provenance; the clone is what the rebuild reads

    remap = rb.rebuild_project(project, log=lambda *a: None)

    assert set(remap) == {v1["hash"], child.content_hash}
    assert remap[v1["hash"]] == v1["hash"], "a source entry's hash is its bytes and its reader options"
    assert alias_kind(project, "orders") == SOURCE_KIND
    assert history_for(project, "orders") == [v1["hash"]]
    provenance = read_manifest(entry_dir(project, v1["hash"])).provenance
    assert provenance is not None and provenance.alias == "orders" and provenance.version == 1
    assert len(cached_result_expr(project, remap[child.content_hash]).execute()) == 2

    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    assert pointers == set(remap.values())
    catalog.assert_catalog_consistent(project, pointers)
