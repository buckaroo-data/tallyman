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
import time
from pathlib import Path

import pytest

from tallyman_core import catalog_state as cs
from tallyman_core import charts, git_util, notebook, paths
from tallyman_core import post_processing as pp
from tallyman_core import summary_stats as ss
from tallyman_xorq import build_and_persist

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


def _stage_recipe_zip(project: str, content_hash: str) -> None:
    """Stage a synthetic durable recipe (``entries/<hash>.zip``) so the next
    ``checkpoint_catalog`` commits it alongside ``catalog.yaml``.

    reset_to's consistency check (#52) requires every ``entry_hashes`` pointer to
    have a git-tracked recipe; a synthetic build dir alone leaves the two views
    disagreeing. This mirrors what a real ``xorq catalog add`` commits, without
    running xorq."""
    zp = paths.entries_dir(project) / f"{content_hash}.zip"
    zp.parent.mkdir(parents=True, exist_ok=True)
    zp.write_bytes(b"PK\x05\x06" + b"\x00" * 18)  # empty-zip EOCD: plausible recipe bytes
    _git(project, "add", str(zp.relative_to(paths.catalog_dir(project))))


# ---------------------------------------------------------------------------
# Invariant 1 — fork-safe git (the guard, static)
# ---------------------------------------------------------------------------


def test_no_bare_subprocess_git_in_src():
    """No src/ module shells out to bare `git` via subprocess (#25 / G1).

    The macOS fork-from-multithreaded crash won't reproduce on Linux CI, so a
    static check is the only enforcement. All runtime git must go through
    git_util.run_git (os.posix_spawn).

    Matches against the whole file text (``\\s`` spans newlines), so a call
    ruff wraps across lines is still caught, and any module alias
    (``import subprocess as sp``) is covered by matching the attribute name,
    not the literal ``subprocess.``.
    """
    src = Path(__file__).resolve().parent.parent / "src"
    pattern = re.compile(r"""\b\w+\.(?:run|Popen|call|check_call|check_output)\(\s*\[\s*["']git["']""")
    offenders = []
    for p in src.rglob("*.py"):
        if p.name == "git_util.py":  # the sanctioned primitive; names the pattern in its docs
            continue
        text = p.read_text()
        offenders += [f"{p.relative_to(src)}:{text.count('\n', 0, m.start()) + 1}" for m in pattern.finditer(text)]
    assert not offenders, f"bare subprocess git calls: {offenders}"


def test_run_git_returns_code_out_err(project):
    cs.ensure_catalog_repo(project)
    rc, out, err = git_util.run_git(["rev-parse", "--git-dir"], cwd=paths.catalog_dir(project))
    assert rc == 0
    assert ".git" in out


# Invariant 3 (non-destructive materialize) is retired by the native store: the
# six mutable sections are tracked files now, restored by `git reset --hard`
# directly, so there is no capture/materialize catalog.yaml round-trip. The
# survives-reset behavior is covered by test_reset_roundtrip_over_every_kind
# (below) and test_native_catalog_store.py's per-section reset tests.


# ---------------------------------------------------------------------------
# Invariant 4 — untracked-artifact reconciliation (prune to pointers)
# ---------------------------------------------------------------------------


def test_prune_entries_to_recorded(project):
    for h in ("aaaa", "bbbb"):
        paths.entry_dir(project, h).mkdir(parents=True)
    cs.write_tallyman_state(project, entry_hashes=["aaaa"])
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
    cs.write_tallyman_state(project, compute_cache=["keep.parquet"])
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
    cs.checkpoint_catalog(project, "tallyman: add pp1")
    assert _commit_count(project) == base + 1  # exactly one, not three
    dirty = _git(project, "status", "--porcelain", "--untracked-files=no")
    assert dirty == "", f"tracked tree not clean after checkpoint: {dirty!r}"


def test_checkpoint_commit_failure_does_not_move_step_tag(project):
    """A failed checkpoint commit must not retag: tagging anyway lands step-N
    on step-(N-1)'s commit, and list_revisions keys rows by commit, so the
    earlier step silently vanishes from the timeline. The realistic trigger
    exists — xorq's own `catalog add` commits to the same repo outside the
    checkpoint flock, so index.lock contention is a matter of timing."""
    cs.ensure_catalog_repo(project)
    notebook.append(project, "alias1", markdown="hello")
    s1 = cs.checkpoint_catalog(project, "step one")
    assert s1 is not None
    before = _steps(project)

    # Wedge the repo the way a concurrent writer does: a stale index.lock.
    lock = paths.catalog_dir(project) / ".git" / "index.lock"
    lock.write_bytes(b"")
    try:
        notebook.append(project, "alias2", markdown="world")
        assert cs.checkpoint_catalog(project, "step two") is None  # did not land
    finally:
        lock.unlink()

    assert _steps(project) == before  # no tag created, none clobbered
    revs = cs.list_revisions(project)
    assert any(r["step"] == s1 and r["current"] for r in revs)  # s1 still on the timeline, at HEAD


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
    assert set(cs.read_tallyman_state(project)["compute_cache"]) == {"baseline.parquet"}

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
    from tallyman_mcp import server

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
    from tallyman_mcp.server import catalog_add_post_processing, catalog_list

    base = _steps(project)
    res = catalog_add_post_processing(name="pp_hook", source=PP_SRC)
    assert "error" not in res
    after = _steps(project)
    assert len(after) == len(base) + 1  # exactly one revision for the op

    catalog_list()  # read-only: denylisted, no revision
    assert _steps(project) == after


def test_mcp_multi_mutator_tool_is_one_step(project, orders_parquet):
    from tallyman_mcp.server import catalog_create

    base = _steps(project)
    res = catalog_create(name="agg", code=_agg_code(orders_parquet))
    assert "error" not in res
    assert len(_steps(project)) == len(base) + 1  # build+alias+notebook → one step


def test_mcp_failing_tool_does_not_checkpoint(project):
    from tallyman_mcp.server import catalog_add_post_processing

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
    before = _steps(project)
    r = c.post(f"/{project}/api/reset", json={"ref": s1})
    assert r.status_code == 200, r.text
    assert r.json()["step"] == s1
    assert {x["alias"] for x in notebook.load(project)["cells"]} == {"alias1"}
    assert _steps(project) == before  # /api/reset is middleware-exempt: moves current, appends nothing

    assert c.post(f"/{project}/api/reset", json={"ref": 999}).status_code == 404


def test_cli_genesis_revisions_label_and_reset(isolated_home):
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    runner = CliRunner()
    assert runner.invoke(cli, ["init", "alpha", "--no-fixture"]).exit_code == 0
    assert "step-000" in _git("alpha", "tag", "-l", "step-*")  # genesis baseline

    pp.write_post_processing("alpha", "pp1", PP_SRC)
    cs.checkpoint_catalog("alpha", "tallyman: add pp1")

    listed = runner.invoke(cli, ["revisions", "--project", "alpha"])
    assert listed.exit_code == 0, listed.output
    assert "step-000" in listed.output and "step-001" in listed.output

    labelled = runner.invoke(cli, ["revisions", "label", "0", "baseline", "--project", "alpha"])
    assert labelled.exit_code == 0, labelled.output

    reset = runner.invoke(cli, ["reset-to", "baseline", "--project", "alpha"])
    assert reset.exit_code == 0, reset.output
    assert pp.list_post_processings("alpha") == []  # back to the empty genesis


def test_cli_reset_notify_names_the_reset_project(isolated_home, monkeypatch):
    """`reset-to --project` must tell the companion *which* project was reset.
    The notify payload otherwise falls back to the companion's active project,
    so resetting a non-active project would reload the wrong sessions and
    leave the reset project's buckaroo sessions stale."""
    import httpx
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    sent = {}
    monkeypatch.setattr(httpx, "post", lambda url, json=None, timeout=None: sent.update(url=url, json=json))

    runner = CliRunner()
    assert runner.invoke(cli, ["init", "beta", "--no-fixture"]).exit_code == 0
    res = runner.invoke(cli, ["reset-to", "0", "--project", "beta"])
    assert res.exit_code == 0, res.output
    assert sent["json"] == {"kind": "project_reset", "project": "beta"}


def test_notify_honors_explicit_project(fresh_companion_app, project):
    """/internal/notify reloads sessions for the project named in the payload,
    falling back to the active project only when none is given."""
    from fastapi.testclient import TestClient

    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "project_reset", "project": "beta"})
    assert r.status_code == 200, r.text
    assert r.json()["project"] == "beta"

    r2 = c.post("/internal/notify", json={"kind": "project_reset"})  # no project: active
    assert r2.status_code == 200
    assert r2.json()["project"] == project


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
    code = f"from tallyman_core.catalog_state import checkpoint_catalog; checkpoint_catalog({project!r}, 'from child')"
    env = {**os.environ, "TALLYMAN_HOME": str(isolated_home)}
    procs = [sp.Popen([sys.executable, "-c", code], env=env, stdout=sp.DEVNULL, stderr=sp.DEVNULL) for _ in range(2)]
    for p in procs:
        assert p.wait(timeout=120) == 0
    assert _steps(project) == {"step-000", "step-001"}  # both landed, distinct steps
    assert int(_git(project, "rev-list", "--count", "HEAD")) == 2


# ---------------------------------------------------------------------------
# Hardening — branch pin, run_git timeout, ref/label validation, view-time cache
# ---------------------------------------------------------------------------


def test_catalog_repo_branch_is_main(project, monkeypatch):
    """The catalog repo pins its branch to `main` rather than inheriting the
    host's init.defaultBranch (master on a stock machine and on CI), so the
    repo's layout is deterministic and independent of host git config."""
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", "/dev/null")
    cs.ensure_catalog_repo(project)
    rc, out, _ = git_util.run_git(["symbolic-ref", "HEAD"], cwd=paths.catalog_dir(project))
    assert rc == 0
    assert out == "refs/heads/main"


def test_run_git_timeout_kills_hung_git(project):
    """run_git promises a timeout; a wedged git must come back as a signal
    death (negative rc), not block the caller — which may hold the project
    flock — indefinitely."""
    cs.ensure_catalog_repo(project)
    t0 = time.monotonic()
    rc, _, _ = git_util.run_git(["-c", "alias.zzz=!sleep 5", "zzz"], cwd=paths.catalog_dir(project), timeout=0.5)
    assert rc < 0  # killed, not exited
    assert time.monotonic() - t0 < 3


def test_reset_to_accepts_only_step_tags_and_labels(project):
    """reset_to's ref is operator input (HTTP payload / CLI arg). Only a plain
    existing tag may pass to git — option-like strings, revision navigation,
    and bare refs must be rejected, not silently resolved."""
    cs.ensure_catalog_repo(project)
    notebook.append(project, "alias1", markdown="hello")
    s1 = cs.checkpoint_catalog(project, "step one")
    notebook.append(project, "alias2", markdown="world")
    cs.checkpoint_catalog(project, "step two")

    for ref in ("--quiet", ":/step", f"step-{s1:03d}~0", "HEAD"):
        with pytest.raises(RuntimeError):
            cs.reset_to(project, ref)

    cs.reset_to(project, s1)  # the sanctioned forms still work
    assert cs.current_step(project) == s1


def test_label_step_rejects_unsafe_names(project):
    """Labels become git args and live in the tag namespace: option-like names
    (`-d` deletes tags), step-shaped names (clobber real steps via `tag -f`),
    and all-digit names (unreachable — digit refs parse as step numbers) must
    all be rejected before they reach git."""
    cs.ensure_catalog_repo(project)
    s = cs.checkpoint_catalog(project, "step")
    for bad in ("-d", "step-001", "42", "so bad"):
        with pytest.raises(RuntimeError):
            cs.label_step(project, s, bad)
    tags = set(_git(project, "tag", "-l").split())
    assert tags == {f"step-{s:03d}"}  # nothing created, deleted, or clobbered


def test_load_entry_uses_per_project_compute_cache(project, monkeypatch):
    """#45: caches warm lazily at view time, and view time goes through
    load_entry — so load_entry must use the per-project compute cache, not the
    global ~/.cache/xorq. Otherwise the recorded warm-set is vacuous and
    reset's prune cannot make a re-added expression compute cold."""
    from tallyman_xorq import build as build_mod

    (paths.entry_dir(project, "cafe0001") / "xorq_build").mkdir(parents=True)
    seen = {}

    def spy(build_dir, project_root, cache_dir=None):
        seen["cache_dir"] = Path(cache_dir)
        return "expr"

    monkeypatch.setattr("tallyman_xorq.portable.load_expr_portable", spy)
    assert build_mod.load_entry(project, "cafe0001") == "expr"
    assert seen["cache_dir"] == paths.compute_cache_dir(project)


# ---------------------------------------------------------------------------
# Cache bullpen — evictions are retired, not destroyed; forward reset restores
# ---------------------------------------------------------------------------


def test_prune_retires_to_bullpen_not_delete(project):
    """Reset evictions move to the bullpen: the live tree stays honest (a
    re-add finds nothing and computes cold) while the artifact survives for a
    forward reset to restore."""
    cs.ensure_catalog_repo(project)
    paths.entry_dir(project, "aaaa").mkdir(parents=True)
    (paths.entry_dir(project, "aaaa") / "result.parquet").write_bytes(b"r")
    cc = paths.compute_cache_dir(project)
    cc.mkdir(parents=True)
    (cc / "warm.parquet").write_bytes(b"w")

    cs.write_tallyman_state(project, entry_hashes=[], compute_cache=[])
    assert cs.prune_entries(project) == 1
    assert cs.prune_compute_cache(project) == 1

    bp = paths.bullpen_dir(project)
    assert (bp / "entries" / "aaaa" / "result.parquet").read_bytes() == b"r"
    assert (bp / "compute_cache" / "warm.parquet").read_bytes() == b"w"
    assert not paths.entry_dir(project, "aaaa").exists()  # live tree is cold


def test_scrub_back_then_forward_restores_from_bullpen(project):
    """The rehearsal gesture: reset back (evict to bullpen), reset forward
    (the step's recorded artifacts come back from the bullpen instead of
    recomputing), and the bullpen keeps its copies so the loop can repeat."""
    cs.ensure_catalog_repo(project)
    notebook.append(project, "alias1", markdown="hello")
    s1 = cs.checkpoint_catalog(project, "baseline")

    ed = paths.entry_dir(project, "bbbb")
    ed.mkdir(parents=True)
    (ed / "manifest.json").write_text("{}")  # a complete entry — capture only records manifest-bearing dirs
    (ed / "result.parquet").write_bytes(b"big")
    _stage_recipe_zip(project, "bbbb")  # durable recipe, so the step's two views agree (#52)
    cc = paths.compute_cache_dir(project)
    cc.mkdir(parents=True)
    (cc / "later.parquet").write_bytes(b"warm")
    s2 = cs.checkpoint_catalog(project, "added entry")

    cs.reset_to(project, s1)
    assert not ed.exists()  # evicted from the live tree...
    assert (paths.bullpen_dir(project) / "entries" / "bbbb").is_dir()  # ...into the bullpen

    cs.reset_to(project, s2)
    assert (ed / "result.parquet").read_bytes() == b"big"  # restored, not recomputed
    assert (cc / "later.parquet").read_bytes() == b"warm"
    assert (paths.bullpen_dir(project) / "entries" / "bbbb").is_dir()  # bullpen retains its copy


# ---------------------------------------------------------------------------
# reset ↔ xorq-catalog consistency — a pointer with no durable recipe must not
# pass silently through the bullpen (#52)
# ---------------------------------------------------------------------------


def test_reset_surfaces_pointer_without_durable_recipe(project):
    """reset_to must not report success when a restored step's entry_hashes
    pointer names an entry whose durable xorq recipe (entries/<hash>.zip) was
    never committed (#52).

    Defense-in-depth: the normal capture path no longer *creates* this divergence
    — a manifest-less mid-build dir is not recorded as a pointer (see
    test_native_catalog_store.py::test_checkpoint_skips_manifestless_entry_dir).
    But should a committed step's ``entry_hashes`` pointer name an entry whose
    durable recipe zip is absent anyway (manual corruption, or a future code path
    that bypasses the capture filter), reset must still surface the divergence —
    the consistency guard is the backstop — not mask it and return success. So
    inject the divergent step directly rather than via capture."""
    cs.ensure_catalog_repo(project)
    s0 = cs.checkpoint_catalog(project, "baseline")  # no entries yet

    # Commit a step whose pointer has no durable recipe zip, bypassing the
    # capture manifest-filter (which would otherwise drop the bogus pointer).
    cs.write_tallyman_state(project, entry_hashes=["deadbeef"])
    cd = paths.catalog_dir(project)
    cs.run_git(["add", "-A"], cwd=cd)
    cs.run_git([*cs._GIT_ID, "commit", "-m", "pointer whose recipe never reached git", "--allow-empty"], cwd=cd)
    cs.run_git(["tag", "-f", "step-001"], cwd=cd)

    cs.reset_to(project, s0)  # move away from the divergent step
    with pytest.raises(RuntimeError, match="deadbeef"):
        cs.reset_to(project, 1)  # restoring it must surface the pointer-without-recipe divergence


# The xorq-catalog-coexistence section (catalog.yaml entries/aliases seeding,
# write_tallyman_state yaml-safety, and the best-effort xorq-add-after-genesis
# durability) is retired with the subprocess. The native store's
# zip-at-checkpoint durability is covered by
# test_native_catalog_store.py::test_zip_staged_not_committed_until_checkpoint
# and test_n_builds_track_n_zips.
