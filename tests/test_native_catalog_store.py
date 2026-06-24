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
    """expr.py is a durable zip member (D3): the tracked_expr_from_alias chaining path
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
        "from tallyman_xorq.io import read_project_file\n"
        "t = read_project_file('orders.parquet')\n"
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
    # No monkeypatch.undo(): it shares the instance that isolated_home used to set
    # TALLYMAN_HOME, so undo() would point entries_dir at the real home and make
    # this check vacuous. monkeypatch auto-reverts at teardown.

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
# Cut B — the catalog-free datafusion connect shim
# ---------------------------------------------------------------------------


def test_connect_shim_matches_xorq_api_connect(project, orders_parquet, monkeypatch):
    """The catalog-free connect shim is equivalent to xorq.api.connect(): it
    builds the same datafusion backend, so a ParquetSnapshotCache(source=shim)
    derives the SAME content-addressed snapshot key and bakes the SAME result
    data. A naive swap to xorq.expr.api.connect (ibis's connect(resource,
    **kwargs)) would resolve the symbol then build the wrong backend or fail at
    call time (Risk #3) — this is the central Cut-B correctness claim.

    Not raw-byte equality: datafusion's group-by output order (and parquet
    metadata) isn't deterministic, so even one backend re-baking the same expr
    yields different bytes. The content-addressed *key* and the *data* are what
    must match.
    """
    import pandas as pd
    import xorq.api as xo

    from tallyman_xorq.result_cache import _cached_node_path, _recipe_expr
    from tallyman_xorq.source_cache import rewrite_for_build

    res = build_and_persist(project, _agg_code(orders_parquet))  # bakes via the shim
    raw = _recipe_expr(project, res.content_hash)
    shim_path = _cached_node_path(rewrite_for_build(raw, project))
    assert shim_path is not None and shim_path.exists(), "the shim must bake a real snapshot"
    shim_df = pd.read_parquet(shim_path).sort_values("region").reset_index(drop=True)

    # rewrite_for_build does `from tallyman_xorq.backend import connect` at call
    # time, so patching the shim's source reroutes the next derive/bake.
    monkeypatch.setattr("tallyman_xorq.backend.connect", xo.connect)
    api_expr = rewrite_for_build(raw, project)
    assert _cached_node_path(api_expr) == shim_path, "shim and xorq.api.connect must share the snapshot key"
    shim_path.unlink()  # evict, then re-bake via xorq.api.connect
    api_expr.count().execute()
    api_df = pd.read_parquet(shim_path).sort_values("region").reset_index(drop=True)
    pd.testing.assert_frame_equal(shim_df, api_df)


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


def test_tracked_tree_is_the_decomposed_surface(project, orders_parquet):
    """End to end: after building an entry and touching every decomposed section,
    the git-tracked tree is *exactly* the native decomposed surface — no
    catalog.yaml, no build dirs, no xorq metadata/symlinks. Pins the whole cut."""
    from tallyman_core import aliases as al
    from tallyman_core import charts, notebook
    from tallyman_core import display_configs as dc
    from tallyman_core import post_processing as pp
    from tallyman_core import summary_stats as ss

    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet), prompt="first")
    h = res.content_hash
    al.set_alias(project, "by_region", h, expect_exists=False)
    charts.set_chart(project, h, {"mark": "bar"})
    dc.set_display_config(project, h, {"pinned_rows": [1]})
    pp.write_post_processing(project, "pp1", "def process(expr):\n    return expr\n")
    ss.write_stat(project, "st1", "def compute(col):\n    return col.count()\n")
    notebook.append(project, "by_region", markdown="hello")
    cs.checkpoint_catalog(project, "create")

    rc, out, _ = cs.run_git(["ls-files"], cwd=paths.catalog_dir(project))
    tracked = set(out.split())
    assert tracked == {
        ".gitignore",
        "aliases.jsonl",
        "notebook.jsonl",
        "entries.jsonl",
        "compute_cache.jsonl",
        f"entries/{h}.zip",
        f"chart_specs/{h}.vl.json",
        f"display_configs/{h}.json",
        "post_processing/pp1.py",
        "stats/st1.py",
        f"prompts/{h}.jsonl",
    }, tracked
    assert "catalog.yaml" not in tracked and not any("/xorq_build/" in t for t in tracked)


# ---------------------------------------------------------------------------
# tracked-surface integrity (bug-hunt regressions)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rel",
    [
        "post_processing/sub/leak.py",
        "chart_specs/deep/leak.vl.json",
        "prompts/_disabled/leak.jsonl",
        "stats/nested/leak.py",
        "display_configs/nested/leak.json",
    ],
)
def test_nested_stray_under_allowlisted_prefix_is_rejected(project, orders_parquet, rel):
    """A stray nested *under* an allowlisted dir prefix must be rejected.

    ``fnmatch``'s ``*`` spans ``/``, so ``post_processing/sub/leak.py`` wrongly
    matched the ``post_processing/*.py`` pattern and slipped past the
    deny-by-default guard (none of these paths are gitignored either). The
    allowlist must be path-segment-aware.
    """
    cs.genesis(project)
    build_and_persist(project, _agg_code(orders_parquet))
    cs.checkpoint_catalog(project, "create")
    cd = paths.catalog_dir(project)
    p = cd / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("LEAK\n")
    cs.run_git(["add", "-A"], cwd=cd)
    _, out, _ = cs.run_git(["ls-files"], cwd=cd)
    assert rel in out.split()  # `git add -A` staged it (not gitignored)
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    with pytest.raises(RuntimeError, match="outside the allowed surface"):
        catalog.assert_catalog_consistent(project, pointers)


def test_legit_disabled_subdir_member_still_passes(project, orders_parquet):
    """The segment-aware allowlist must not over-reject the legitimate two-level
    members it already tracks (``post_processing/_disabled/*.py``, &c.)."""
    cs.genesis(project)
    build_and_persist(project, _agg_code(orders_parquet))
    cs.checkpoint_catalog(project, "create")
    cd = paths.catalog_dir(project)
    for rel in ("post_processing/_disabled/old.py", "stats/_disabled/old.py"):
        p = cd / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("def process(t):\n    return t\n")
    cs.run_git(["add", "-A"], cwd=cd)
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    catalog.assert_catalog_consistent(project, pointers)  # must NOT raise


def test_interrupted_atomic_write_tmp_not_committed(project, orders_parquet):
    """A ``*.tmp`` left by a crash mid atomic-write (``aliases._write`` /
    ``write_recipe_zip``) must never be staged by the checkpoint's ``git add -A``,
    so it cannot durably wedge a later ``reset_to`` via the tracked-surface
    allowlist."""
    from tallyman_core import aliases as al

    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet))
    al.set_alias(project, "regions", res.content_hash)
    cs.checkpoint_catalog(project, "cp1")
    cd = paths.catalog_dir(project)
    # Simulate a crash between tmp.write_text and tmp.replace in aliases._write.
    (cd / "aliases.jsonl.tmp").write_text('{"alias":"x","latest":"d","history":["d"]}\n')
    s2 = cs.checkpoint_catalog(project, "cp2")  # nothing consumes the .tmp
    _, out, _ = cs.run_git(["ls-files"], cwd=cd)
    assert "aliases.jsonl.tmp" not in out.split(), "interrupted atomic-write .tmp committed by `git add -A`"
    cs.reset_to(project, s2)  # the committed step must remain reset-able


def test_checkpoint_skips_manifestless_entry_dir(project):
    """A checkpoint firing during the mid-build window (entry dir with
    ``expr.py`` + ``schema.json`` but no ``manifest.json``) must not commit a
    pointer it has no durable recipe zip for; the committed step must remain
    ``reset_to``-able. ``capture_tallyman_state`` must apply the same manifest
    filter ``zip_pending_entries`` already uses."""
    cs.genesis(project)
    ed = paths.entry_dir(project, "deadbeef0001")
    ed.mkdir(parents=True)
    (ed / "expr.py").write_text("expr = 1\n")
    (ed / "schema.json").write_text("{}")  # NO manifest.json — a mid-build dir
    step = cs.checkpoint_catalog(project, "checkpoint over a manifest-less mid-build dir")
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    assert pointers == catalog.tracked_recipe_hashes(project)  # pointer set == durable recipe set
    catalog.assert_catalog_consistent(project, pointers)  # must not raise
    cs.reset_to(project, step)  # must not raise


# ---------------------------------------------------------------------------
# Cut A — the guard validates every decomposed hash-reference, not just
# entries.jsonl ↔ entries/*.zip (M1/L1)
# ---------------------------------------------------------------------------


def test_consistency_guard_surfaces_dangling_alias_hash(project, orders_parquet):
    """A committed ``aliases.jsonl`` whose latest/history hash names no durable
    recipe must fail the consistency guard, not reset clean (M1). A dangling
    alias head later blows up the self-chaining ``tracked_expr_from_alias`` build, so
    ``reset_to`` must refuse to restore the step rather than mask the divergence."""
    from tallyman_core import aliases as al

    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet))
    al.set_alias(project, "ghost", "ffffffffffff", expect_exists=False)  # no such entry
    cs.checkpoint_catalog(project, "dangling alias")
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    assert pointers == {res.content_hash}  # the real entry's pointer/recipe agree (guard 2 passes)
    with pytest.raises(RuntimeError, match="ffffffffffff"):
        catalog.assert_catalog_consistent(project, pointers)


def test_consistency_guard_surfaces_dangling_alias_history_hash(project, orders_parquet):
    """The alias check covers history, not only the latest head — a revised alias
    whose *older* version's recipe is missing is just as dangling (M1: 'at minimum
    cover aliases.jsonl history')."""
    from tallyman_core import aliases as al

    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet))
    al.set_alias(project, "regions", "ffffffffffff", expect_exists=False)  # v1: no recipe
    al.set_alias(project, "regions", res.content_hash)  # v2: real head; history = [ffff…, res]
    cs.checkpoint_catalog(project, "alias with a dangling history entry")
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    with pytest.raises(RuntimeError, match="ffffffffffff"):
        catalog.assert_catalog_consistent(project, pointers)


def test_consistency_guard_surfaces_orphan_chart_hash(project, orders_parquet):
    """A committed ``chart_specs/<hash>.vl.json`` whose hash names no durable
    recipe must fail the guard (L1) — the guard validates every content-addressed
    sidecar against the live recipe set, not just the recipe↔pointer pair."""
    from tallyman_core import charts

    cs.genesis(project)
    build_and_persist(project, _agg_code(orders_parquet))
    charts.set_chart(project, "ffffffffffff", {"mark": "bar"})  # no such entry
    cs.checkpoint_catalog(project, "orphan chart")
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    with pytest.raises(RuntimeError, match="ffffffffffff"):
        catalog.assert_catalog_consistent(project, pointers)


def test_consistency_guard_surfaces_orphan_display_config_hash(project, orders_parquet):
    """A committed ``display_configs/<hash>.json`` whose hash names no durable
    recipe must fail the guard (L1), the display-config twin of the orphan-chart
    case."""
    from tallyman_core import display_configs as dc

    cs.genesis(project)
    build_and_persist(project, _agg_code(orders_parquet))
    dc.set_display_config(project, "ffffffffffff", {"pinned_rows": [1]})  # no such entry
    cs.checkpoint_catalog(project, "orphan display config")
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    with pytest.raises(RuntimeError, match="ffffffffffff"):
        catalog.assert_catalog_consistent(project, pointers)


def test_failed_build_after_copy_leaves_no_orphan_dir(project, orders_parquet, monkeypatch):
    """A build that fails AFTER the entry dir is populated but past the
    load_expr/execute paths (here: ``make_portable_inplace``) must also leave no
    orphan entry dir (L4). The D1 'no partial dirs' invariant covers every
    population step, not just the two that had ad-hoc ``rmtree`` cleanup."""
    import tallyman_xorq.build as build_mod

    cs.genesis(project)

    def _boom(*a, **k):
        raise RuntimeError("induced failure after the entry dir was populated")

    monkeypatch.setattr(build_mod, "make_portable_inplace", _boom)
    with pytest.raises(Exception):
        build_and_persist(project, _agg_code(orders_parquet))
    # No monkeypatch.undo(): it would also revert isolated_home's TALLYMAN_HOME
    # setenv (same monkeypatch instance), pointing entries_dir at the real home
    # and making this check vacuous. monkeypatch auto-reverts at teardown.

    ed = paths.entries_dir(project)
    leftover = [c.name for c in ed.iterdir() if c.is_dir()] if ed.exists() else []
    assert not leftover, f"failed build left orphan entry dir(s): {leftover}"


def test_consistency_guard_surfaces_orphan_recipe(project, orders_parquet):
    """The orphan branch of guard 2 (a tracked ``entries/<hash>.zip`` with no
    pointer) must raise — the untested half of the pointer/recipe agreement check
    (L8). The missing-pointer half is covered by
    ``test_reset_to_revision.py::test_reset_surfaces_pointer_without_durable_recipe``."""
    cs.genesis(project)
    build_and_persist(project, _agg_code(orders_parquet))
    cs.checkpoint_catalog(project, "create")  # tracks entries/<hash>.zip
    with pytest.raises(RuntimeError, match="tracked recipes with no pointer"):
        catalog.assert_catalog_consistent(project, set())  # pointers empty, recipe tracked


# ---------------------------------------------------------------------------
# Decomposition — alias state rolls back with git reset (M2)
# ---------------------------------------------------------------------------


def test_alias_state_survives_reset(project, orders_parquet):
    """The alias map + history (the tracked ``aliases.jsonl`` this cut introduced)
    rolls back with ``git reset`` — the carrier whose only coverage was the deleted
    ``test_reset_rolls_back_alias_state`` (M2). Mirrors the display-config /
    prompt-history survives-reset tests."""
    from tallyman_core import aliases as al

    cs.genesis(project)
    h0 = _build_checkpoint(project, _agg_code_n(orders_parquet, _AGGS[0]), "c0")  # step 1
    al.set_alias(project, "regions", h0, expect_exists=False)
    cs.checkpoint_catalog(project, "c1")  # step 2: regions -> h0 (history [h0])
    h1 = build_and_persist(project, _agg_code_n(orders_parquet, _AGGS[1])).content_hash
    al.set_alias(project, "regions", h1)  # revise: history [h0, h1]
    al.set_alias(project, "avg", h1, expect_exists=False)
    cs.checkpoint_catalog(project, "c2")  # step 3: regions -> h1 (history [h0, h1]) + avg -> h1

    assert al.load_aliases(project) == {"regions": h1, "avg": h1}
    cs.reset_to(project, 2)
    assert al.load_aliases(project) == {"regions": h0}
    assert al.history_for(project, "regions") == [h0]
    cs.reset_to(project, 3)
    assert al.load_aliases(project) == {"regions": h1, "avg": h1}
    assert al.history_for(project, "regions") == [h0, h1]


def test_append_prompt_is_atomic_and_complete(project, orders_parquet):
    """``_append_prompt`` rewrites ``prompts/<hash>.jsonl`` via tmp + replace (C2):
    it runs outside the project lock (only the checkpoint holds it), so the file
    must stay whole JSONL across appends with no leftover ``*.tmp`` the
    checkpoint's ``git add -A`` could stage."""
    from tallyman_xorq import read_prompts

    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet), prompt="first")
    h = res.content_hash
    build_and_persist(project, _agg_code(orders_parquet), prompt="second")  # re-run, same hash, appends
    assert [r["prompt"] for r in read_prompts(project, h)] == ["first", "second"]
    assert not list(paths.prompts_path(project, h).parent.glob("*.tmp"))  # no interrupted temp left behind
