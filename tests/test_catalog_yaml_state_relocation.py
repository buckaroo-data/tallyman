"""#49 — tallyman authored state must not live in the xorq-managed catalog.yaml.

xorq rebuilds catalog.yaml from ``entries`` + ``aliases`` only when it resolves a
pull merge conflict (``_resolve_yaml_conflict_if_any``, xorq/catalog/catalog.py),
dropping every other key. Today ``capture_tallyman_state`` writes all eight
tallyman keys (``post_processing``, ``stats``, ``charts``, ``display_configs``,
``notebook``, ``entry_hashes``, ``result_cache``, ``compute_cache``) into that
file, so the first conflicting pull silently erases the authored project.

The fix moves those keys into a tallyman-owned store outside the xorq catalog
repo, leaving catalog.yaml carrying only what xorq manages. These tests pin three
properties of that relocation; all three fail today, where the state still lives
in catalog.yaml. The reset engine's own roundtrip invariants stay in
``test_reset_to_revision.py`` — this file is only about *where* the state lives.
"""

from __future__ import annotations

import subprocess

import yaml

from tallyman_core import catalog_state as cs
from tallyman_core import charts, notebook, paths
from tallyman_core import post_processing as pp
from tallyman_core import summary_stats as ss

PP_SRC = "def process(expr):\n    return expr\n"
ST_SRC = "def compute(col):\n    return col.count()\n"
SPEC = {"mark": "bar", "encoding": {"x": {"field": "region"}}}

_TALLYMAN_KEYS = {
    "post_processing",
    "stats",
    "charts",
    "display_configs",
    "notebook",
    "entry_hashes",
    "result_cache",
    "compute_cache",
}


def _seed_state(project: str) -> None:
    pp.write_post_processing(project, "pp1", PP_SRC)
    ss.write_stat(project, "st1", ST_SRC)
    charts.set_chart(project, "deadbeef", SPEC)
    notebook.append(project, "alias1", markdown="hello")


def _catalog_yaml(project: str) -> dict:
    return yaml.safe_load((paths.catalog_dir(project) / "catalog.yaml").read_text()) or {}


def test_capture_leaves_no_tallyman_keys_in_catalog_yaml(project):
    """catalog.yaml must carry only xorq's keys after a capture. Today capture
    writes all eight tallyman keys there, which is exactly what xorq's merge
    reconstruction drops (#49)."""
    _seed_state(project)
    cs.capture_tallyman_state(project)

    raw = _catalog_yaml(project)
    leaked = _TALLYMAN_KEYS & set(raw)
    assert not leaked, f"tallyman keys still written to catalog.yaml: {sorted(leaked)}"
    assert set(raw) <= {"entries", "aliases"}, f"catalog.yaml carries non-xorq keys: {sorted(raw)}"


def test_state_store_lives_outside_the_xorq_catalog_repo(project):
    """The durable authored state must live outside the xorq catalog repo, so a
    pull/merge in that repo cannot reach it. Today it is the git-tracked
    catalog.yaml *inside* the repo."""
    _seed_state(project)
    cs.checkpoint_catalog(project, "step 1")

    cd = paths.catalog_dir(project)
    tracked = subprocess.run(["git", "-C", str(cd), "ls-files"], capture_output=True, text=True).stdout.split()
    # catalog.yaml is tracked, but must no longer carry authored state.
    assert _TALLYMAN_KEYS.isdisjoint(set(_catalog_yaml(project)))

    store = paths.tallyman_state_dir(project)
    assert store.is_dir(), "tallyman state store was not created"
    assert store != cd and cd not in store.parents, f"store {store} is inside the xorq repo {cd}"
    # Nothing tracked in the xorq repo lives under the store.
    assert not any(str(store) in line for line in tracked)


def test_state_survives_catalog_yaml_reconstruction(project):
    """The #49 failure, end to end. After a checkpoint, simulate xorq's merge
    resolution — overwrite catalog.yaml with ``entries`` + ``aliases`` only — and
    the authored state must still read back. Today ``read_tallyman_state`` reads
    catalog.yaml, so the reconstruction wipes pp/stats/charts/notebook."""
    _seed_state(project)
    cs.checkpoint_catalog(project, "step 1")

    # Byte-for-byte what _resolve_yaml_conflict_if_any does to catalog.yaml.
    cy = paths.catalog_dir(project) / "catalog.yaml"
    raw = yaml.safe_load(cy.read_text()) or {}
    cy.write_text(yaml.safe_dump({"entries": raw.get("entries", []), "aliases": raw.get("aliases", [])}))

    state = cs.read_tallyman_state(project)
    assert any(e["name"] == "pp1" for e in state["post_processing"]), "post_processing lost"
    assert any(e["name"] == "st1" for e in state["stats"]), "stats lost"
    assert any(c["content_hash"] == "deadbeef" for c in state["charts"]), "charts lost"
    assert state["notebook"]["cells"][0]["alias"] == "alias1", "notebook lost"
