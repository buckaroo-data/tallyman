"""Native catalog store (drop xorq's catalog package) — the Cut-A/Cut-B suite.

See plans/native-catalog-store.md. These pin the contract the native
``tallyman_core.catalog`` store must satisfy: the build/alias paths invoke no
xorq ``catalog`` subprocess, the checkpoint (not the build) owns the durable
recipe zip, and a failed build leaves no orphan pointer.

Carries no ``integration``/``cache_lab``/``perf`` marker, so a bare
``uv run pytest`` (CI default, pyproject.toml:77) exercises them.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from tallyman_core import catalog_state as cs
from tallyman_core import paths
from tallyman_xorq import build_and_persist


def _agg_code(parquet_path: Path) -> str:
    return (
        "import xorq.api as xo\n"
        f"t = xo.deferred_read_parquet({str(parquet_path)!r})\n"
        "expr = t.group_by('region').aggregate(total=t.price.sum(), n=t.count())\n"
    )


def _tracked_zip_hashes(project: str) -> set[str]:
    rc, out, _ = cs.run_git(["ls-files", "entries"], cwd=paths.catalog_dir(project))  # type: ignore[attr-defined]
    return {ln[len("entries/") : -len(".zip")] for ln in out.split() if ln.endswith(".zip")}


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
    """set_alias must not shell out to ``xorq catalog add-alias``.

    Today aliases.py:129 calls the xorq subprocess with no try/except, so a
    raising ``subprocess.run`` propagates out of set_alias. Native aliases write
    a tracked ``aliases.jsonl`` and never spawn xorq, so this must not raise.
    """
    from tallyman_core import aliases as al

    cs.genesis(project)  # the catalog repo must exist or add_alias returns early

    def _boom(*a, **k):
        raise AssertionError("set_alias spawned a subprocess (xorq catalog add-alias)")

    monkeypatch.setattr(subprocess, "run", _boom)
    al.set_alias(project, "myalias", "deadbeefdead", expect_exists=False)


def test_uses_no_subprocess_build(project, orders_parquet, monkeypatch):
    """A full build must invoke zero ``xorq catalog`` subprocesses.

    Asserts a call-count spy of zero, NOT "did not raise": build.py:539-541
    swallows a raising registration into ``catalog_registered=False``, so a
    raising stub would false-green.
    """
    cs.genesis(project)  # so add_entry reaches the subprocess rather than returning early
    spy = _XorqSpy(subprocess.run)
    monkeypatch.setattr(subprocess, "run", spy)
    build_and_persist(project, _agg_code(orders_parquet))
    assert spy.xorq_calls == 0, f"build spawned {spy.xorq_calls} xorq catalog subprocess(es)"


# ---------------------------------------------------------------------------
# Cut A — the checkpoint owns the durable recipe zip (D1)
# ---------------------------------------------------------------------------


def test_zip_staged_not_committed_until_checkpoint(project, orders_parquet):
    """The durable ``entries/<hash>.zip`` is written by the checkpoint, not the
    build. Today xorq's ``catalog add`` commits it inline (outside the flock),
    so it is tracked before any tallyman checkpoint — D1 moves that to the
    checkpoint under the project lock.
    """
    cs.genesis(project)
    res = build_and_persist(project, _agg_code(orders_parquet))
    assert res.content_hash not in _tracked_zip_hashes(project), (
        "recipe zip was committed at build time; D1 requires the checkpoint to own it"
    )
    cs.checkpoint_catalog(project, "create")
    assert res.content_hash in _tracked_zip_hashes(project), "checkpoint did not commit the recipe zip"


def test_failed_build_no_orphan_pointer(project, orders_parquet, monkeypatch):
    """A build that fails after creating its entry dir leaves no dir behind, so
    the next checkpoint records no pointer without a durable recipe (D1/Risk #7).
    """
    import xorq.ibis_yaml.compiler as compiler

    cs.genesis(project)
    real_load = compiler.load_expr

    def _boom_load(*a, **k):
        raise RuntimeError("induced load failure after target.mkdir")

    monkeypatch.setattr(compiler, "load_expr", _boom_load)
    try:
        build_and_persist(project, _agg_code(orders_parquet))
    except Exception:
        pass
    monkeypatch.setattr(compiler, "load_expr", real_load)

    ed = paths.entries_dir(project)
    leftover = [c.name for c in ed.iterdir() if c.is_dir()] if ed.exists() else []
    assert not leftover, f"failed build left orphan entry dir(s): {leftover}"
