"""Integration tests for the git-state guard against the *real* xorq stack.

Background: xorq records git provenance by shelling out to `git rev-parse HEAD`
/ `git diff` / `git diff --cached` from `logging_utils.get_git_state`, called
inline on the catalog-write/compile path. From the long-lived, multithreaded
tallyman server those bare-name `subprocess` calls fork, and `fork()` from a
multithreaded process on macOS can die with SIGSEGV/SIGABRT (a child inherits
locks held by threads that don't exist in it). See
`plans/ADR-001-git-subprocess-threading.md` and buckaroo-data/nokernel-notebooks#25.

These tests exercise the real xorq `get_git_state` (the unedited library
function) through the in-repo guard, plus the real `build_and_persist` write
path. The ADR's three claims are tested:

- `test_*_fork_free`        — git is dispatched via `os.posix_spawn`, never fork.
- `test_guard_reflects_*`   — provenance is fresh per build (no permanent cache).
- `test_guard_degrades_*`   — a signal-killed git degrades to placeholders.

The first two FAIL against the pre-ADR implementation (it forks and caches
forever) and pass once the ADR lands.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from tallyman_xorq import build_and_persist


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------
def _git(*args: str, cwd: Path) -> None:
    subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


# The three commands xorq's get_git_state runs. A forked command never reaches
# os.posix_spawn, so "all three were spawned" is exactly "none of them forked".
_PROVENANCE_COMMANDS = (("rev-parse", "HEAD"), ("diff",), ("diff", "--cached"))


def _provenance_spawns(seen: list[list[str]]) -> list[tuple[str, ...]]:
    """From recorded posix_spawn argvs, the git sub-commands that were spawned."""
    out: list[tuple[str, ...]] = []
    for argv in seen:
        if argv and "git" in os.path.basename(str(argv[0])):
            out.append(tuple(str(a) for a in argv[1:]))
    return out


def _assert_provenance_fork_free(seen: list[list[str]], context: str) -> None:
    prov = _provenance_spawns(seen)
    forked = [cmd for cmd in _PROVENANCE_COMMANDS if cmd not in prov]
    assert not forked, (
        f"{context}: git {forked} forked instead of using os.posix_spawn — the "
        f"macOS fork-from-multithreaded hazard. posix_spawn'd git calls: {prov}"
    )


@pytest.fixture
def guard_env(monkeypatch):
    """Snapshot/restore the swapped `lu.get_git_state` + reset any guard cache.

    Resets defensively so the same test file works before and after the ADR
    drops the cache symbols (`_raw_state` / `_UNSET`).
    """
    from xorq.common.utils import logging_utils as lu

    import tallyman_xorq._git_state_guard as g

    original = lu.get_git_state
    had_cache = hasattr(g, "_raw_state")
    saved_raw = getattr(g, "_raw_state", None)
    if had_cache:
        g._raw_state = g._UNSET
    monkeypatch.delenv("TALLYMAN_DISABLE_GIT_STATE", raising=False)
    try:
        yield lu, g
    finally:
        lu.get_git_state = original
        if had_cache:
            g._raw_state = saved_raw


@pytest.fixture
def spawn_spy(monkeypatch):
    """Record every `os.posix_spawn` argv; pass-through to the real one."""
    seen: list[list[str]] = []
    real = os.posix_spawn

    def spy(path, argv, env, **kwargs):
        seen.append(list(argv))
        return real(path, argv, env, **kwargs)

    monkeypatch.setattr(os, "posix_spawn", spy)
    return seen


@pytest.fixture
def temp_git_repo(tmp_path: Path, monkeypatch) -> Path:
    """A throwaway real git repo with one commit; cwd points here."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git("init", "-q", cwd=repo)
    _git("config", "user.email", "t@example.com", cwd=repo)
    _git("config", "user.name", "tester", cwd=repo)
    (repo / "a.txt").write_text("one\n")
    _git("add", "a.txt", cwd=repo)
    _git("commit", "-q", "-m", "init", cwd=repo)
    monkeypatch.chdir(repo)
    return repo


@pytest.fixture
def segfault_git_repo(tmp_path: Path, monkeypatch) -> Path:
    """A dir that looks like a repo, with a `git` on PATH that dies via SIGSEGV.

    Mirrors the observed crash (git killed by signal -> returncode -11).
    """
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)  # makes xorq's _git_is_present() true
    bindir = tmp_path / "bin"
    bindir.mkdir()
    fake_git = bindir / "git"
    fake_git.write_text("#!/bin/sh\nkill -SEGV $$\n")
    fake_git.chmod(0o755)
    monkeypatch.chdir(repo)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    return repo


def _agg_code(src: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


# ---------------------------------------------------------------------------
# ADR claim 1 — git provenance must be dispatched fork-free (os.posix_spawn)
# ---------------------------------------------------------------------------
def test_installed_guard_dispatches_git_fork_free(guard_env, spawn_spy, temp_git_repo):
    """The real xorq get_git_state, through the guard, must use posix_spawn."""
    lu, g = guard_env
    g.install_git_state_guard()

    state = lu.get_git_state(hash_diffs=False)

    # Capture actually ran in this repo (not the degraded placeholder), so the
    # assertion below is about a real provenance call, not a no-op.
    assert state["commit"] != "unknown", state

    _assert_provenance_fork_free(spawn_spy, "guarded get_git_state")


def test_build_dispatches_git_provenance_fork_free(project, orders_src, spawn_spy):
    """End-to-end: a real catalog write, through real xorq build_expr ->
    compiler.py:507 -> lu.get_git_state, must capture provenance fork-free."""
    res = build_and_persist(project, _agg_code(orders_src), prompt="fork-safety")
    assert res.content_hash

    _assert_provenance_fork_free(spawn_spy, "build_and_persist")


# ---------------------------------------------------------------------------
# ADR claim 3 — provenance is fresh per build (no permanent one-shot cache)
# ---------------------------------------------------------------------------
def test_guard_reflects_repo_state_changes(guard_env, temp_git_repo):
    lu, g = guard_env
    g.install_git_state_guard()

    first = lu.get_git_state(hash_diffs=False)
    assert first["diff_cached"] == ""

    # Stage a new change; a subsequent build must see it.
    (temp_git_repo / "new.txt").write_text("hello\n")
    _git("add", "new.txt", cwd=temp_git_repo)

    second = lu.get_git_state(hash_diffs=False)
    assert second["diff_cached"] != first["diff_cached"], (
        "guard returned stale, permanently-cached provenance; each build must "
        "reflect current repo state (ADR: drop the one-shot cache)"
    )
    assert "new.txt" in second["diff_cached"]


# ---------------------------------------------------------------------------
# ADR claim 2 — a signal-killed git degrades to placeholders, never raises
# (regression: already holds today; locks it in across the posix_spawn refactor)
# ---------------------------------------------------------------------------
def test_guard_degrades_on_signal_death(guard_env, segfault_git_repo):
    lu, g = guard_env

    # The unguarded form xorq ships raises on signal death (the original bug).
    with pytest.raises(subprocess.CalledProcessError) as excinfo:
        subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
    assert excinfo.value.returncode == -11  # -SIGSEGV

    # The guard contains it: placeholder provenance instead of a raise.
    g.install_git_state_guard()
    state = lu.get_git_state(hash_diffs=False)
    assert state == {"commit": "unknown", "diff": "", "diff_cached": ""}


# ---------------------------------------------------------------------------
# The companion's own builds. Opening a worthy entry's grid (the view build), a diff's compare grid and the re-hash
# after an unfaithful heal each call xorq's build_expr in the companion process. Catalog writes normally run in the MCP
# process, so no build may have installed the guard in the companion before these run.
# ---------------------------------------------------------------------------
def _unguarded_get_git_state(hash_diffs=False):
    """xorq's ``get_git_state`` as it ships (``logging_utils.py:44``): a bare ``git`` through ``subprocess``, which
    forks, and a signal death raises."""
    commit, diff, diff_cached = (
        subprocess.check_output(cmd).decode().strip()
        for cmd in (["git", "rev-parse", "HEAD"], ["git", "diff"], ["git", "diff", "--cached"])
    )
    return {"commit": commit, "diff": diff, "diff_cached": diff_cached}


def _crash_forked_git(monkeypatch, tmp_path: Path) -> None:
    """Put the process in the state #267 describes: a ``git`` forked from it dies with SIGSEGV, and xorq's unguarded
    ``get_git_state`` is in place, as in a companion that has not built anything yet.

    Call it after the test's own builds, since ``build_and_persist`` installs the guard.
    """
    from xorq.common.utils import logging_utils as lu

    repo = tmp_path / "checkout"
    (repo / ".git").mkdir(parents=True)  # the companion runs from a checkout, so xorq's _git_is_present() is true
    bindir = tmp_path / "bin"
    bindir.mkdir()
    fake_git = bindir / "git"
    fake_git.write_text("#!/bin/sh\nkill -SEGV $$\n")
    fake_git.chmod(0o755)
    monkeypatch.chdir(repo)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setattr(lu, "get_git_state", _unguarded_get_git_state)


def test_a_companion_opens_a_worthy_entrys_grid_when_a_forked_git_would_crash(
    project, orders_src, tmp_path, monkeypatch
):
    from tallyman_companion import create_app
    from tallyman_companion.buckaroo_lifecycle import ensure_view_build

    h = build_and_persist(project, _agg_code(orders_src)).content_hash
    _crash_forked_git(monkeypatch, tmp_path)
    create_app(project)

    assert (ensure_view_build(project, h) / "expr.yaml").is_file()


def test_a_companion_builds_a_diffs_compare_grid_when_a_forked_git_would_crash(
    project, orders_src, tmp_path, monkeypatch
):
    from tallyman_companion import create_app
    from tallyman_companion.app import _build_compare_expr

    a = build_and_persist(project, _agg_code(orders_src)).content_hash
    b = build_and_persist(project, _agg_code(orders_src).replace("t.price.sum()", "t.price.max()")).content_hash
    _crash_forked_git(monkeypatch, tmp_path)
    create_app(project)
    _build_compare_expr.cache_clear()  # keyed by hashes that another test's project can share

    build_path, _ = _build_compare_expr(project, a, b, ("region",))
    assert (build_path / "expr.yaml").is_file()


def test_a_companion_re_hashes_a_recipe_when_a_forked_git_would_crash(project, orders_src, tmp_path, monkeypatch):
    """The re-hash decides whether an unfaithful heal is blamed on the recipe (#88). It returns None on any error, so a
    crashed git shows up as no hash, and the heal is blamed on execution."""
    from tallyman_companion import create_app
    from tallyman_xorq.result_cache import _reconstructed_hash

    h = build_and_persist(project, _agg_code(orders_src)).content_hash
    _crash_forked_git(monkeypatch, tmp_path)
    create_app(project)

    assert _reconstructed_hash(project, h) is not None
