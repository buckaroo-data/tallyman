"""reset-to-revision: the catalog.yaml state-carrier engine.

These tests pin the four invariants #33 kept re-breaking (see
plans/adr-reset-to-revision.md): fork-safe git, commit faithfulness,
non-destructive materialize, untracked-artifact reconciliation — plus the
per-project compute cache that gives the demo its honest cold add.

Tests shell out to git directly to assert on the catalog repo; that is the
sanctioned exception to the no-bare-subprocess-git guard (which scans src/).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from pydata_core import catalog_state as cs
from pydata_core import charts, git_util, notebook, paths
from pydata_core import post_processing as pp
from pydata_core import summary_stats as ss
from pydata_xorq import build_and_persist

PP_SRC = "def process(expr):\n    return expr\n"
ST_SRC = "def compute(col):\n    return col.count()\n"
SPEC = {"mark": "bar", "encoding": {"x": {"field": "region"}}}


def _agg_code(parquet_path: Path) -> str:
    return (
        "import xorq.api as xo\n"
        f"t = xo.deferred_read_parquet({str(parquet_path)!r})\n"
        "expr = t.group_by('region').aggregate(total=t.price.sum(), n=t.count())\n"
    )


def _git(project: str, *args: str) -> str:
    cd = paths.catalog_dir(project)
    return subprocess.run(["git", "-C", str(cd), *args], capture_output=True, text=True).stdout.strip()


def _commit_count(project: str) -> int:
    out = _git(project, "rev-list", "--count", "HEAD")
    return int(out) if out else 0


# ---------------------------------------------------------------------------
# Invariant 1 — fork-safe git (the guard, static)
# ---------------------------------------------------------------------------


def test_no_bare_subprocess_git_in_src():
    """No src/ module shells out to bare `git` via subprocess (#25 / G1).

    The macOS fork-from-multithreaded crash won't reproduce on Linux CI, so a
    static check is the only enforcement. All runtime git must go through
    git_util.run_git (os.posix_spawn).
    """
    src = Path(__file__).resolve().parent.parent / "src"
    pattern = re.compile(r"""subprocess\.\w+\(\s*\[\s*["']git["']""")
    offenders = [
        f"{p.relative_to(src)}:{i}"
        for p in src.rglob("*.py")
        if p.name != "git_util.py"  # the sanctioned primitive; names the pattern in its docs
        for i, line in enumerate(p.read_text().splitlines(), 1)
        if pattern.search(line)
    ]
    assert not offenders, f"bare subprocess git calls: {offenders}"


def test_run_git_returns_code_out_err(project):
    cs.ensure_catalog_repo(project)
    rc, out, err = git_util.run_git(["rev-parse", "--git-dir"], cwd=paths.catalog_dir(project))
    assert rc == 0
    assert ".git" in out


# ---------------------------------------------------------------------------
# Invariant 3 — non-destructive materialize (G3)
# ---------------------------------------------------------------------------


def test_capture_materialize_roundtrip(project):
    pp.write_post_processing(project, "pp1", PP_SRC)
    ss.write_stat(project, "st1", ST_SRC)
    charts.set_chart(project, "deadbeef", SPEC)
    notebook.append(project, "alias1", markdown="hello")

    state = cs.capture_pydata_state(project)
    assert any(e["name"] == "pp1" for e in state["post_processing"])
    assert any(e["name"] == "st1" for e in state["stats"])
    assert any(c["content_hash"] == "deadbeef" for c in state["charts"])
    assert state["notebook"]["cells"][0]["alias"] == "alias1"

    # Drift the on-disk tree, then materialize back from catalog.yaml.
    pp.write_post_processing(project, "pp2_added_later", PP_SRC)
    charts.set_chart(project, "cafef00d", SPEC)
    cs.materialize(project)

    names = {e["name"] for e in pp.list_post_processings(project)}
    assert names == {"pp1"}  # pp2 reconciled away
    assert charts.list_charts(project) == ["deadbeef"]  # cafef00d gone
    assert {c["alias"] for c in notebook.load(project)["cells"]} == {"alias1"}


def test_materialize_absent_key_preserves_files(project):
    """A key absent from catalog.yaml means 'not recorded' — never wipe it."""
    pp.write_post_processing(project, "legacy", PP_SRC)
    # catalog.yaml has no post_processing key written.
    cs.materialize(project)
    assert any(e["name"] == "legacy" for e in pp.list_post_processings(project))


def test_materialize_present_empty_clears(project):
    """A present-but-empty list is a deliberate 'none at this step'."""
    pp.write_post_processing(project, "doomed", PP_SRC)
    cs.write_pydata_state(project, post_processing=[])
    cs.materialize(project)
    assert pp.list_post_processings(project) == []


# ---------------------------------------------------------------------------
# Invariant 4 — untracked-artifact reconciliation (prune to pointers)
# ---------------------------------------------------------------------------


def test_prune_entries_to_recorded(project):
    for h in ("aaaa", "bbbb"):
        paths.entry_dir(project, h).mkdir(parents=True)
    cs.write_pydata_state(project, entry_hashes=["aaaa"])
    assert cs.prune_entries(project) == 1
    assert paths.entry_dir(project, "aaaa").exists()
    assert not paths.entry_dir(project, "bbbb").exists()


def test_prune_entries_noop_when_key_absent(project):
    paths.entry_dir(project, "aaaa").mkdir(parents=True)
    assert cs.prune_entries(project) == 0  # absent key → never delete
    assert paths.entry_dir(project, "aaaa").exists()


def test_prune_compute_cache_to_recorded(project):
    cdir = paths.compute_cache_dir(project)
    cdir.mkdir(parents=True)
    (cdir / "keep.parquet").write_bytes(b"x")
    (cdir / "drop.parquet").write_bytes(b"y")
    cs.write_pydata_state(project, compute_cache=["keep.parquet"])
    assert cs.prune_compute_cache(project) == 1
    assert {p.name for p in cdir.iterdir()} == {"keep.parquet"}


# ---------------------------------------------------------------------------
# Invariant 2 — commit faithfulness (one commit per op, clean tree)
# ---------------------------------------------------------------------------


def test_checkpoint_one_commit_and_clean_tree(project):
    cs.ensure_catalog_repo(project)
    base = _commit_count(project)
    pp.write_post_processing(project, "pp1", PP_SRC)
    notebook.append(project, "alias1", markdown="note")
    cs.checkpoint_catalog(project, "pydata: add pp1")
    assert _commit_count(project) == base + 1  # exactly one, not three
    dirty = _git(project, "status", "--porcelain", "--untracked-files=no")
    assert dirty == "", f"tracked tree not clean after checkpoint: {dirty!r}"


def test_reset_roundtrip_over_every_kind(project):
    cs.ensure_catalog_repo(project)
    pp.write_post_processing(project, "pp1", PP_SRC)
    ss.write_stat(project, "st1", ST_SRC)
    notebook.append(project, "alias1", markdown="hello")
    s1 = cs.checkpoint_catalog(project, "step 1")

    pp.write_post_processing(project, "pp2", PP_SRC)
    charts.set_chart(project, "abc123", SPEC)
    notebook.append(project, "alias2", markdown="world")
    cs.checkpoint_catalog(project, "step 2")

    cs.reset_to(project, s1)

    assert {e["name"] for e in pp.list_post_processings(project)} == {"pp1"}
    assert {e["name"] for e in ss.list_stats(project)} == {"st1"}
    assert charts.list_charts(project) == []  # the kind #33 forgot
    assert {c["alias"] for c in notebook.load(project)["cells"]} == {"alias1"}


# ---------------------------------------------------------------------------
# Per-project compute cache + honest cold add
# ---------------------------------------------------------------------------


def test_compute_cache_is_per_project(project, orders_parquet):
    # W4 routes xorq's compute cache to a per-project dir, not the global
    # ~/.cache/xorq. A build creates compute_cache_dir under the project; the
    # guarantee is the *location* (a plain aggregate may cache nothing).
    build_and_persist(project, _agg_code(orders_parquet))
    cdir = paths.compute_cache_dir(project)
    assert cdir.is_dir()
    assert str(cdir).startswith(str(paths.project_dir(project)))


def test_reset_prunes_compute_cache_to_warm_set(project):
    # Caches warm lazily when entries are viewed, not at build time — so seed
    # the warm-set directly and assert the reset prunes it back to the step's
    # recorded set (a file added after the checkpoint is gone → recomputes cold).
    cs.ensure_catalog_repo(project)
    cc = paths.compute_cache_dir(project)
    cc.mkdir(parents=True)
    (cc / "baseline.parquet").write_bytes(b"a")
    s1 = cs.checkpoint_catalog(project, "baseline")
    assert set(cs.read_pydata_state(project)["compute_cache"]) == {"baseline.parquet"}

    (cc / "added.parquet").write_bytes(b"b")
    cs.checkpoint_catalog(project, "added later")
    assert {p.name for p in cc.iterdir()} == {"baseline.parquet", "added.parquet"}

    cs.reset_to(project, s1)
    assert {p.name for p in cc.iterdir()} == {"baseline.parquet"}  # added pruned → cold


# ---------------------------------------------------------------------------
# W5 — dispatch-boundary checkpoint hook (opt-out)
# ---------------------------------------------------------------------------


def _steps(project: str) -> set[str]:
    return set(_git(project, "tag", "-l", "step-*").split())


def test_every_mcp_tool_is_checkpoint_wrapped():
    """Opt-out, structurally: every @mcp.tool() registration passes through the
    checkpoint wrapper, so a tool added next month is covered by default and
    only the explicit read-only/lifecycle denylist suppresses the checkpoint."""
    from pydata_mcp import server

    assert server.mcp.tool.__name__ == "_checkpointing_tool"  # registration is wrapped
    mutating = {
        "catalog_run",
        "catalog_load_parquet",
        "catalog_create",
        "catalog_revise",
        "catalog_alias",
        "catalog_rename",
        "catalog_unalias",
        "notebook_reorder",
        "notebook_remove",
        "notebook_edit_markdown",
        "catalog_chart",
        "catalog_add_summary_stat",
        "catalog_remove_summary_stat",
        "catalog_add_post_processing",
        "catalog_remove_post_processing",
    }
    assert mutating <= server._CHECKPOINTED_TOOLS
    assert server._NO_CHECKPOINT.isdisjoint(mutating)


def test_mcp_mutating_tool_checkpoints_once(project):
    from pydata_mcp.server import catalog_add_post_processing, catalog_list

    base = _steps(project)
    res = catalog_add_post_processing(name="pp_hook", source=PP_SRC)
    assert "error" not in res
    after = _steps(project)
    assert len(after) == len(base) + 1  # exactly one revision for the op

    catalog_list()  # read-only: denylisted, no revision
    assert _steps(project) == after


def test_mcp_multi_mutator_tool_is_one_step(project, orders_parquet):
    from pydata_mcp.server import catalog_create

    base = _steps(project)
    res = catalog_create(name="agg", code=_agg_code(orders_parquet))
    assert "error" not in res
    assert len(_steps(project)) == len(base) + 1  # build+alias+notebook → one step


def test_mcp_failing_tool_does_not_checkpoint(project):
    from pydata_mcp.server import catalog_add_post_processing

    base = _steps(project)
    res = catalog_add_post_processing(name="bad", source="not python ((")
    assert "error" in res
    assert _steps(project) == base


def test_companion_mutation_checkpoints_once(fresh_companion_app, project):
    from fastapi.testclient import TestClient

    cell = notebook.append(project, "alias1", markdown="hi")
    c = TestClient(fresh_companion_app)
    base = _steps(project)
    r = c.put(f"/{project}/api/markdown/{cell['cell_id']}", json={"markdown": "edited"})
    assert r.status_code == 200
    assert len(_steps(project)) == len(base) + 1

    after = _steps(project)
    assert c.get(f"/{project}/api/notebook").status_code == 200  # GET: never
    assert c.delete(f"/{project}/api/errors").status_code == 200  # denylisted clear
    assert _steps(project) == after


# ---------------------------------------------------------------------------
# W6 — reset surface (companion endpoint + CLI)
# ---------------------------------------------------------------------------


def test_companion_reset_endpoint(fresh_companion_app, project):
    from fastapi.testclient import TestClient

    cs.ensure_catalog_repo(project)
    notebook.append(project, "alias1", markdown="hello")
    s1 = cs.checkpoint_catalog(project, "step one")
    notebook.append(project, "alias2", markdown="world")
    cs.checkpoint_catalog(project, "step two")

    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/reset", json={"ref": s1})
    assert r.status_code == 200, r.text
    assert r.json()["step"] == s1
    assert {x["alias"] for x in notebook.load(project)["cells"]} == {"alias1"}

    assert c.post(f"/{project}/api/reset", json={"ref": 999}).status_code == 404


def test_cli_genesis_revisions_label_and_reset(isolated_home):
    from click.testing import CliRunner

    from pydata_cli.main import cli

    runner = CliRunner()
    assert runner.invoke(cli, ["init", "alpha", "--no-fixture"]).exit_code == 0
    assert "step-000" in _git("alpha", "tag", "-l", "step-*")  # genesis baseline

    pp.write_post_processing("alpha", "pp1", PP_SRC)
    cs.checkpoint_catalog("alpha", "pydata: add pp1")

    listed = runner.invoke(cli, ["revisions", "--project", "alpha"])
    assert listed.exit_code == 0, listed.output
    assert "step-000" in listed.output and "step-001" in listed.output

    labelled = runner.invoke(cli, ["revisions", "label", "0", "baseline", "--project", "alpha"])
    assert labelled.exit_code == 0, labelled.output

    reset = runner.invoke(cli, ["reset-to", "baseline", "--project", "alpha"])
    assert reset.exit_code == 0, reset.output
    assert pp.list_post_processings("alpha") == []  # back to the empty genesis


def test_genesis_on_companion_project_new(fresh_companion_app, project):
    from fastapi.testclient import TestClient

    c = TestClient(fresh_companion_app)
    r = c.post("/api/projects/new", json={"name": "fresh"})
    assert r.status_code == 200, r.text
    assert "step-000" in _git("fresh", "tag", "-l", "step-*")


def test_cross_process_checkpoint(project, isolated_home):
    """Two processes checkpoint concurrently; the per-project flock serializes
    them so both land — no lost revision, no torn `git index.lock` race. Real
    processes, not threads: flock is held per-process, which is the whole point
    (the companion and the MCP server are separate processes)."""
    import os
    import subprocess as sp
    import sys

    cs.ensure_catalog_repo(project)
    code = f"from pydata_core.catalog_state import checkpoint_catalog; checkpoint_catalog({project!r}, 'from child')"
    env = {**os.environ, "PYDATA_HOME": str(isolated_home)}
    procs = [sp.Popen([sys.executable, "-c", code], env=env, stdout=sp.DEVNULL, stderr=sp.DEVNULL) for _ in range(2)]
    for p in procs:
        assert p.wait(timeout=120) == 0
    assert _steps(project) == {"step-000", "step-001"}  # both landed, distinct steps
    assert int(_git(project, "rev-list", "--count", "HEAD")) == 2
