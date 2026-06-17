"""Native catalog store (drop xorq's catalog package) — the Cut-A/Cut-B suite.

See plans/native-catalog-store.md. These pin the contract the native
``tallyman_core.catalog`` store satisfies: the build/alias paths invoke no xorq
``catalog`` subprocess, the checkpoint (not the build) owns the deterministic,
allowlisted recipe zip, a failed build leaves no orphan pointer, and the
tracked surface is a path allowlist (deny-by-default).

Carries no ``integration``/``cache_lab``/``perf`` marker, so a bare
``uv run pytest`` (CI default, pyproject.toml:77) exercises them. The durability
guards here used to live in the xorq-specific ``test_catalog_xorq_integration.py``
(deleted with the subprocess) as slow ``@integration`` tests; they are now
in-process and fast.
"""

from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

import pytest

from tallyman_core import catalog, paths
from tallyman_core import catalog_state as cs
from tallyman_xorq import build_and_persist


def _agg_code(parquet_path: Path) -> str:
    return (
        "import xorq.api as xo\n"
        f"t = xo.deferred_read_parquet({str(parquet_path)!r})\n"
        "expr = t.group_by('region').aggregate(total=t.price.sum(), n=t.count())\n"
    )


def _agg_code_n(parquet_path: Path, agg: str) -> str:
    return (
        "import xorq.api as xo\n"
        f"t = xo.deferred_read_parquet({str(parquet_path)!r})\n"
        f"expr = t.group_by('region').aggregate({agg})\n"
    )


_AGGS = ["total=t.price.sum()", "avg=t.price.mean()", "mx=t.price.max()", "mn=t.price.min()"]


def _tracked_zip_hashes(project: str) -> set[str]:
    return catalog.tracked_recipe_hashes(project)


def _build_checkpoint(project: str, code: str, message: str) -> str:
    res = build_and_persist(project, code)
    cs.checkpoint_catalog(project, message)
    return res.content_hash


class _XorqSpy:
    """Wrap ``subprocess.run`` and count only ``uv run xorq catalog ...`` calls.

    Scoped to the xorq argv so it never trips on the ``cp -c`` CoW clone
    (source_identity.py:111) or any other subprocess. Delegates everything to
    the real ``subprocess.run`` so behaviour is unchanged.
    """

    def __init__(self, real):
        self._real = real
        self.xorq_calls = 0

    def __call__(self, argv, *a, **k):
        if isinstance(argv, (list, tuple)) and list(argv[:4]) == ["uv", "run", "xorq", "catalog"]:
            self.xorq_calls += 1
        return self._real(argv, *a, **k)


# ---------------------------------------------------------------------------
# Cut A — no xorq subprocess on the build / alias paths
# ---------------------------------------------------------------------------


def test_uses_no_subprocess_alias(project, monkeypatch):
    """set_alias must not shell out to ``xorq catalog add-alias``."""
    from tallyman_core import aliases as al

    cs.genesis(project)

    def _boom(*a, **k):
        raise AssertionError("set_alias spawned a subprocess (xorq catalog add-alias)")

    monkeypatch.setattr(subprocess, "run", _boom)
    al.set_alias(project, "myalias", "deadbeefdead", expect_exists=False)


def test_uses_no_subprocess_build(project, orders_parquet, monkeypatch):
    """A full build invokes zero ``xorq catalog`` subprocesses (call-count spy,
    not "did not raise" — the swallowed-registration path would false-green)."""
    cs.genesis(project)
    spy = _XorqSpy(subprocess.run)
    monkeypatch.setattr(subprocess, "run", spy)
    build_and_persist(project, _agg_code(orders_parquet))
    assert spy.xorq_calls == 0, f"build spawned {spy.xorq_calls} xorq catalog subprocess(es)"


def test_src_has_no_xorq_catalog_argv():
    """No src/ module imports ``xorq.catalog`` or builds a ``xorq catalog`` argv.

    A static guard (the macOS-only fork crash and the catalog package both evade
    a runtime check). Matches ``import/from xorq.catalog`` and the split-token
    ``"xorq", "catalog"`` argv (so a literal ``"xorq catalog"`` search misses
    it). Modelled on ``test_no_bare_subprocess_git_in_src``.
    """
    import re

    src = Path(__file__).resolve().parent.parent / "src"
    imp = re.compile(r"(?:from|import)\s+xorq\.catalog\b")
    argv = re.compile(r"""["']xorq["']\s*,\s*["']catalog["']""")
    offenders = []
    for p in src.rglob("*.py"):
        text = p.read_text()
        offenders += [f"{p.relative_to(src)} (import)" for _ in imp.finditer(text)]
        offenders += [f"{p.relative_to(src)} (argv)" for _ in argv.finditer(text)]
    # The MCP codegen docstring (server.py) teaches `import xorq.api as xo` to
    # recipe authors — not a catalog import, not the argv — so it never matches.
    assert not offenders, f"xorq.catalog import/argv in src/: {offenders}"


# ---------------------------------------------------------------------------
# Cut A — the recipe zip: allowlist, determinism, expr.py durability
# ---------------------------------------------------------------------------


def test_recipe_zip_members_are_allowlist(project, orders_parquet, tmp_path):
    """write_recipe_zip archives exactly the allowlist — never a stray.

    Allowlist-not-denylist: a denylist skipping only today's known strays would
    pass existence checks yet leak a future artifact (here ``foo.bin``).
    """
    res = build_and_persist(project, _agg_code(orders_parquet), prompt="seed")
    ed = paths.entry_dir(project, res.content_hash)
    # strays that must NOT leak into the durable zip (prompts.jsonl is relocated
    # to a tracked prompts/<hash>.jsonl now; drop one here to prove the zip
    # excludes it even if a stale copy lingers in the entry dir)
    (ed / "result.parquet").write_bytes(b"heavy")
    (ed / "foo.bin").write_bytes(b"\x00")
    (ed / "prompts.jsonl").write_text('{"prompt": "x"}\n')
    (ed / ".xorq_build_expanded").mkdir(exist_ok=True)
    (ed / ".xorq_build_expanded" / "x.yaml").write_text("x")

    dest = tmp_path / "recipe.zip"
    catalog.write_recipe_zip(ed, dest, res.content_hash)
    names = set(zipfile.ZipFile(dest).namelist())
    prefix = f"{res.content_hash}/"
    assert names and all(n.startswith(prefix) for n in names), "every member under one <hash>/ prefix"
    rels = {n[len(prefix) :] for n in names}
    top = {r for r in rels if "/" not in r}
    tree = {r for r in rels if "/" in r}
    assert top == {"expr.py", "schema.json", "manifest.json"}, top
    assert all(r.startswith("xorq_build/") for r in tree), tree
    assert not any(s in r for r in rels for s in ("foo.bin", "result.parquet", "prompts.jsonl", "xorq_build_expanded"))


def test_recipe_zip_byte_stable_across_rebuilds(project, orders_parquet, tmp_path):
    """The archive is byte-identical across rebuilds — a defaulted ZipInfo mtime
    would embed the wall clock and silently re-churn the content-addressed git
    blob on every rebuild. Bumping file mtimes between the two zips is what a
    naive (file-mtime) writer leaks and a deterministic one absorbs."""
    import os
    import time

    res = build_and_persist(project, _agg_code(orders_parquet))
    ed = paths.entry_dir(project, res.content_hash)
    z1 = tmp_path / "a.zip"
    catalog.write_recipe_zip(ed, z1, res.content_hash)

    bumped = time.time() + 10_000
    for p in ed.rglob("*"):
        if p.is_file():
            os.utime(p, (bumped, bumped))
    z2 = tmp_path / "b.zip"
    catalog.write_recipe_zip(ed, z2, res.content_hash)

    assert z1.read_bytes() == z2.read_bytes()


def test_recipe_zip_contains_expr_py(project, orders_parquet, tmp_path):
    """expr.py is a durable zip member (D3): the from_catalog chaining path
    re-execs it so a cheap entry's recompute roots on the shared in-process
    backend, and two entries union/join on one backend (#75)."""
    res = build_and_persist(project, _agg_code(orders_parquet))
    ed = paths.entry_dir(project, res.content_hash)
    dest = tmp_path / "recipe.zip"
    catalog.write_recipe_zip(ed, dest, res.content_hash)
    assert f"{res.content_hash}/expr.py" in zipfile.ZipFile(dest).namelist()


def test_salt_zip_named_by_salted_hash(project, monkeypatch):
    """Under source-identity salt mode the durable zip is named by the salted
    content_hash (= the entry dir name), not xorq's path-identity build hash, so
    the tracked zip set and the entry pointers don't diverge (Risk #5)."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "salt")
    from tallyman_cli.fixtures import write_shoe_orders

    write_shoe_orders(paths.data_dir(project) / "orders.parquet", n_rows=120, seed=1)
    code = (
        "from tallyman_xorq.io import from_project\n"
        "t = from_project('orders.parquet')\n"
        "expr = t.group_by('region').aggregate(n=t.count())\n"
    )
    cs.genesis(project)
    res = build_and_persist(project, code)
    import json

    manifest = json.loads((paths.entry_dir(project, res.content_hash) / "manifest.json").read_text())
    assert manifest.get("sources"), "salt mode should have captured a source digest"
    cs.checkpoint_catalog(project, "create")
    assert res.content_hash in _tracked_zip_hashes(project)


def test_failed_build_no_orphan_pointer(project, orders_parquet, monkeypatch):
    """A build that fails after creating its entry dir leaves no dir behind, so
    the next checkpoint records no pointer without a durable recipe (D1/Risk #7).
    """
    import xorq.ibis_yaml.compiler as compiler

    cs.genesis(project)

    def _boom_load(*a, **k):
        raise RuntimeError("induced load failure after target.mkdir")

    monkeypatch.setattr(compiler, "load_expr", _boom_load)
    with pytest.raises(Exception):
        build_and_persist(project, _agg_code(orders_parquet))
    monkeypatch.undo()

    ed = paths.entries_dir(project)
    leftover = [c.name for c in ed.iterdir() if c.is_dir()] if ed.exists() else []
    assert not leftover, f"failed build left orphan entry dir(s): {leftover}"


# ---------------------------------------------------------------------------
# Cut A — the checkpoint owns zipping; the tracked surface is an allowlist
# ---------------------------------------------------------------------------


def test_zip_staged_not_committed_until_checkpoint(project, orders_parquet):
    """The durable ``entries/<hash>.zip`` is written by the checkpoint, not the
    build (D1)."""
    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet))
    assert res.content_hash not in _tracked_zip_hashes(project), "zip must not exist before a checkpoint"
    cs.checkpoint_catalog(project, "create")
    assert res.content_hash in _tracked_zip_hashes(project), "checkpoint did not commit the recipe zip"


def test_n_builds_track_n_zips(project, orders_parquet):
    """N distinct builds, each checkpointed, leave N durable tracked zips (the
    #48 failure mode was N build dirs but fewer tracked recipes)."""
    cs.genesis(project)
    hashes = [_build_checkpoint(project, _agg_code_n(orders_parquet, _AGGS[i]), f"c{i}") for i in range(4)]
    tracked = _tracked_zip_hashes(project)
    missing = [h for h in hashes if h not in tracked]
    assert not missing, f"entries with no durable tracked zip: {missing} (tracked: {tracked})"


def test_build_dir_files_not_staged(project, orders_parquet):
    """The untracked build dir (``entries/<hash>/xorq_build/...``) is never
    git-tracked under ``git add -A`` — the .gitignore + allowlist keep it out
    (Risk #1)."""
    cs.genesis(project)
    build_and_persist(project, _agg_code(orders_parquet))
    cs.checkpoint_catalog(project, "create")
    rc, out, _ = cs.run_git(["ls-files"], cwd=paths.catalog_dir(project))
    staged = [p for p in out.split() if "/xorq_build/" in p or p.endswith("/expr.py") or p.endswith("/manifest.json")]
    assert not staged, f"build-dir files were staged: {staged}"


def test_unlisted_path_not_tracked(project, orders_parquet):
    """A tracked path outside the allowed surface fails the consistency check —
    the durable guard against ``git add -A`` silently committing a stray."""
    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet))
    cs.checkpoint_catalog(project, "create")
    cd = paths.catalog_dir(project)
    (cd / "stray.txt").write_text("not part of the tracked surface\n")
    cs.run_git(["add", "stray.txt"], cwd=cd)
    with pytest.raises(RuntimeError, match="outside the allowed surface"):
        catalog.assert_catalog_consistent(project, {res.content_hash})


# ---------------------------------------------------------------------------
# Cut A — reset keeps the durable recipe set and the pointers in agreement
# ---------------------------------------------------------------------------


def test_reset_keeps_pointer_zip_agreement(project, orders_parquet):
    """A reset to an earlier step leaves the tracked recipe set equal to the
    entry_hashes pointers (the consistency check passes, #52)."""
    cs.genesis(project)
    hashes = [_build_checkpoint(project, _agg_code_n(orders_parquet, _AGGS[i]), f"c{i}") for i in range(2)]
    cs.reset_to(project, 1)  # back to the one-entry step
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    assert pointers == _tracked_zip_hashes(project) == {hashes[0]}


def test_back_then_forward_restores_recipe(project, orders_parquet):
    """Reset back then forward restores entry 2's durable recipe zip, not merely
    its bullpen dir copy."""
    cs.genesis(project)
    hashes = [_build_checkpoint(project, _agg_code_n(orders_parquet, _AGGS[i]), f"c{i}") for i in range(2)]
    cs.reset_to(project, 1)
    cs.reset_to(project, 2)
    assert hashes[1] in _tracked_zip_hashes(project)


# ---------------------------------------------------------------------------
# Decomposition — mutable state is tracked files that roll back with git reset
# ---------------------------------------------------------------------------


def test_prompt_history_survives_reset(project, orders_parquet):
    """The per-entry prompt history is a tracked ``prompts/<hash>.jsonl`` now, so
    a re-run's appended prompt rolls back with the tree on reset (it used to be a
    loose, untracked ``entries/<hash>/prompts.jsonl`` git reset never touched)."""
    from tallyman_xorq import read_prompts

    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet), prompt="first")
    h = res.content_hash
    cs.checkpoint_catalog(project, "c1")
    build_and_persist(project, _agg_code(orders_parquet), prompt="second")  # re-run, same hash, appends
    cs.checkpoint_catalog(project, "c2")
    assert [p["prompt"] for p in read_prompts(project, h)] == ["first", "second"]
    cs.reset_to(project, 1)  # only "first" had been recorded at step 1
    assert [p["prompt"] for p in read_prompts(project, h)] == ["first"]
    cs.reset_to(project, 2)
    assert [p["prompt"] for p in read_prompts(project, h)] == ["first", "second"]


def test_display_config_survives_reset(project, orders_parquet):
    """A display config (tracked ``display_configs/<hash>.json``) rolls back with
    git reset — the one decomposed section with no survives-reset coverage before
    (its old capture/materialize round-trip tests are deleted with this cut)."""
    from tallyman_core import display_configs as dc

    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet))
    h = res.content_hash
    cs.checkpoint_catalog(project, "c1")  # step 1: entry, no display config
    dc.set_display_config(project, h, {"pinned_rows": [1, 2]})
    cs.checkpoint_catalog(project, "c2")  # step 2: display config set
    assert dc.get_display_config(project, h) == {"pinned_rows": [1, 2]}
    cs.reset_to(project, 1)
    assert dc.get_display_config(project, h) is None
    cs.reset_to(project, 2)
    assert dc.get_display_config(project, h) == {"pinned_rows": [1, 2]}
