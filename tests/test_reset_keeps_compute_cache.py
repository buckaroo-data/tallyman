"""A reset leaves ``compute_cache/`` alone, and parks clones in the bullpen instead of deleting them.

Red tests for ``plans/ADR-007-tallyman-owned-materialization.md`` D14 (a reset leaves ``compute_cache/`` alone) and D13
(a file is cache only if ``ensure_materialized`` can re-create it; clones are data). The scenario is
``scripts/spike_reset_roundtrip.py``, which played it through on today's code:

- step s1 holds an entry over ``orders.parquet``;
- step s2 adds a cheap entry and a worthy entry over ``extra.parquet``, which nothing touches afterwards.

A reset to s1 and then forward to s2 used to leave the cheap entry unreadable (``At least one path is required``),
because the backward reset deleted the content-addressed clone of ``extra.parquet`` under ``data/.cas/`` (``gc_cas``
unlinks a clone that no surviving entry refers to) and the forward reset restores entry directories, not clones.
Snapshots under ``compute_cache/`` were pruned and copied back by a second, non-atomic writer, driven by the
git-tracked list ``compute_cache.jsonl``; the design retires that list and lets a snapshot that is missing be healed
and verified like any other (ADR-007 D5).

Because the snapshot stays and a create always rewrites it, a reset also has to keep the entry dir whose manifest
matches that file when the bullpen already holds an older one (#194).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from tallyman_core import catalog, data_dir, read_manifest
from tallyman_core import catalog_state as cs
from tallyman_core.paths import bullpen_dir, catalog_dir, compute_cache_dir, entry_dir
from tallyman_xorq import build_and_persist
from tallyman_xorq.materialize import snapshot_path
from tallyman_xorq.result_cache import cached_result_expr, snapshot_file_digest, verify_result_faithful


def _recipe(alias: str, tail: str = "") -> str:
    """A recipe over an imported source alias — a recipe cannot open a file (ADR-011 D2)."""
    return (
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        f"t = tracked_expr_from_alias({alias!r})\n"
        f"expr = t{tail}\n"
    )


def _random_recipe(alias: str) -> str:
    """``random()`` in a mutate: a worthy entry (an impure operation always is) whose two runs at create differ, so it
    is recorded as not reproducible (ADR-009 D6) and each create of it writes other rows."""
    return (
        "import xorq.vendor.ibis as ibis\n"
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        f"t = tracked_expr_from_alias({alias!r})\n"
        "expr = t.mutate(r=ibis.random())\n"
    )


def _import(project: str, path: Path, alias: str) -> str:
    """Import *path* as a source alias and return the digest of the bytes, which names its clone."""
    from tallyman_xorq.source_import import update_and_depend

    out = update_and_depend(path, alias, project=project)
    return read_manifest(entry_dir(project, out["hash"])).provenance.digest


def _stat_map(root: Path) -> dict[str, tuple[int, int, int]]:
    """Every file under *root* with the fields a move, a copy or a rewrite would change."""
    out = {}
    for p in sorted(root.rglob("*")):
        if p.is_file():
            st = p.stat()
            out[str(p.relative_to(root))] = (st.st_ino, st.st_mtime_ns, st.st_size)
    return out


@pytest.fixture
def two_steps(project: str, orders_parquet: Path) -> SimpleNamespace:
    cs.ensure_catalog_repo(project)
    extra = data_dir(project) / "extra.parquet"
    pd.DataFrame({"k": ["x", "y", "x", "z"], "v": [1.0, 2.0, 3.0, 4.0]}).to_parquet(extra)

    orders_digest = _import(project, orders_parquet, "orders_base_src")
    base = build_and_persist(project, _recipe("orders_base_src")).content_hash
    s1 = cs.checkpoint_catalog(project, "s1")

    # extra is imported AFTER s1, so the reset back retires its source entry too (ADR-011 D1 makes a
    # source version an ordinary entry). Its clone is then the one no surviving entry refers to, which
    # is the case D13 is about: park it in the bullpen, never unlink it.
    extra_digest = _import(project, extra, "extra_src")
    cheap = build_and_persist(project, _recipe("extra_src", ".filter(t.v > 1)")).content_hash
    worthy = build_and_persist(project, _recipe("extra_src", ".group_by('k').aggregate(s=t.v.sum())")).content_hash
    s2 = cs.checkpoint_catalog(project, "s2")
    assert s1 is not None and s2 is not None and s1 != s2

    return SimpleNamespace(
        project=project,
        base=base,
        cheap=cheap,
        worthy=worthy,
        s1=s1,
        s2=s2,
        extra_digest=extra_digest,
        orders_digest=orders_digest,
    )


def _read_all(project: str, hashes: dict[str, str]) -> None:
    for label, h in hashes.items():
        cached_result_expr.cache_clear()
        assert len(cached_result_expr(project, h).execute()) > 0, f"the {label} entry read no rows"


def test_every_entry_of_the_restored_step_reads_after_a_reset_back_and_forward(two_steps):
    """ADR-007 D13 and D14: after a reset to s1 and forward to s2, every entry of s2 reads, cheap and worthy, and
    still reads once ``compute_cache/`` is emptied, which is the cold state of ADR-007 D7. Measured on today's code
    (``scripts/spike_reset_roundtrip.py``): the cheap entry fails at once with ``At least one path is required``, and
    the worthy entry fails the same way as soon as it has to heal."""
    p = two_steps.project
    entries = {"cheap": two_steps.cheap, "worthy": two_steps.worthy}

    cs.reset_to(p, two_steps.s1)
    cs.reset_to(p, two_steps.s2)
    _read_all(p, entries)

    shutil.rmtree(compute_cache_dir(p), ignore_errors=True)
    _read_all(p, entries)


def test_a_reset_moves_and_copies_nothing_under_compute_cache(two_steps):
    """ADR-007 D14: a reset stops managing ``compute_cache/``. Snapshots are named by content hash, so a file left
    behind by a retired entry cannot be served for another entry; it is unreferenced disk until that entry comes back
    or the user deletes it. Nothing under the directory moves on the way back, and nothing is copied in on the way
    forward (the copy was a second writer of snapshot files, outside the one writer of ADR-007 D4)."""
    p = two_steps.project
    root = compute_cache_dir(p)
    before = _stat_map(root)
    assert before, "the worthy entries of the scenario left no file under compute_cache/"

    cs.reset_to(p, two_steps.s1)
    assert _stat_map(root) == before, "the reset back moved or rewrote files under compute_cache/"

    cs.reset_to(p, two_steps.s2)
    assert _stat_map(root) == before, "the reset forward copied files into compute_cache/"


def test_a_backward_reset_parks_the_clone_in_the_bullpen_and_a_forward_reset_restores_it(two_steps):
    """ADR-007 D13 and D14: a clone is the only frozen copy of the bytes an entry was built from, and it cannot be made
    again once the live file has been edited. So a reset does not unlink a clone that no surviving entry refers to; it
    moves it to the bullpen, as it does with entry directories, and a reset forward copies it back to ``data/.cas/``.
    A clone that a surviving entry still refers to stays where it is."""
    p = two_steps.project
    cas = data_dir(p) / ".cas"
    extra_clone = cas / f"{two_steps.extra_digest}.parquet"
    orders_clone = cas / f"{two_steps.orders_digest}.parquet"
    assert extra_clone.is_file() and orders_clone.is_file()

    cs.reset_to(p, two_steps.s1)

    assert not extra_clone.exists()
    assert (bullpen_dir(p) / "cas" / extra_clone.name).is_file(), "the clone was deleted, not parked in the bullpen"
    assert orders_clone.is_file(), "a clone that an entry of the step still refers to must not be parked"

    cs.reset_to(p, two_steps.s2)

    assert extra_clone.is_file(), "the reset forward did not restore the clone"


def test_a_reset_forward_restores_the_manifest_that_matches_the_snapshot_of_an_entry_added_again(
    project: str, orders_src: str
):
    """#194: an entry dir is named by its content hash, but its contents are not a function of it. The manifest records
    ``created_at`` and ``prompt``, and a recipe that is not reproducible records another ``result_digest`` each time it
    is created, while a create always rewrites the snapshot (ADR-007 D4). So when a reset retires an entry whose name
    the bullpen already holds, the live dir is the one that matches the snapshot on disk. It used to be dropped, and a
    reset forward brought back the older manifest over the newer snapshot, with nothing reporting the disagreement.

    The five steps of the issue: create the entry (step 1); reset to step 0, which parks its dir in the bullpen and
    leaves its snapshot (ADR-007 D14); add it again (step 2), which writes a new snapshot and manifest; reset to step 0
    again; reset forward to step 2.
    """
    cs.ensure_catalog_repo(project)
    s0 = cs.checkpoint_catalog(project, "s0")
    first = build_and_persist(project, _random_recipe(orders_src))
    h = first.content_hash
    assert first.reproducible is False
    first_digest = read_manifest(entry_dir(project, h)).result_digest
    assert cs.checkpoint_catalog(project, "s1") is not None

    cs.reset_to(project, s0)
    assert (bullpen_dir(project) / "entries" / h).is_dir()
    assert build_and_persist(project, _random_recipe(orders_src)).content_hash == h
    s2 = cs.checkpoint_catalog(project, "s2")
    snap = snapshot_path(project, h)
    written = snapshot_file_digest(snap)
    assert read_manifest(entry_dir(project, h)).result_digest == written
    assert written != first_digest, "the second create wrote the same rows, so the scenario tests nothing"

    cs.reset_to(project, s0)
    cs.reset_to(project, s2)

    assert snapshot_file_digest(snap) == written, "a reset changed the snapshot (ADR-007 D14)"
    restored = read_manifest(entry_dir(project, h)).result_digest
    assert restored == written, "the reset forward restored the first create's manifest over the second's snapshot"
    assert verify_result_faithful(project, h) is True


def test_no_compute_cache_pointer_file_is_written(project: str, orders_parquet: Path):
    """ADR-007 D14: ``compute_cache.jsonl``, the git-tracked list of every file that was under the directory at each
    checkpoint, is retired. Capturing it was a cost every checkpoint paid, and it grew with the cache (#22)."""
    cs.ensure_catalog_repo(project)
    _import(project, orders_parquet, "orders_base_src")
    build_and_persist(project, _recipe("orders_base_src", ".group_by('region').aggregate(n=t.count())"))

    assert cs.checkpoint_catalog(project, "one") is not None

    assert not (catalog_dir(project) / "compute_cache.jsonl").exists()


def test_the_reset_machinery_for_compute_cache_is_gone():
    """ADR-007 D14: ``prune_compute_cache`` and the ``compute_cache/`` half of ``restore_from_bullpen`` are deleted."""
    assert not hasattr(cs, "prune_compute_cache")


def test_compute_cache_pointer_file_is_not_on_the_tracked_surface():
    """ADR-007 D14: nothing writes ``compute_cache.jsonl`` any more, so the catalog's allowlist of tracked paths no
    longer names it."""
    assert "compute_cache.jsonl" not in catalog.TRACKED_SURFACE
