"""Integration tests for the git-state guard against the *real* xorq stack.

Background: xorq records git provenance by shelling out to `git rev-parse HEAD`
/ `git diff` / `git diff --cached` from `logging_utils.get_git_state`, called
inline on the catalog-write/compile path. From the long-lived, multithreaded
tallyman server those bare-name `subprocess` calls fork, and `fork()` from a
multithreaded process on macOS can die with SIGSEGV/SIGABRT (a child inherits
locks held by threads that don't exist in it). See
`plans/adr-git-subprocess-threading.md` and buckaroo-data/nokernel-notebooks#25.

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


def _agg_code(parquet_path: Path) -> str:
    return f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet_path)!r})
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


def test_build_dispatches_git_provenance_fork_free(project, orders_parquet, spawn_spy):
    """End-to-end: a real catalog write, through real xorq build_expr ->
    compiler.py:507 -> lu.get_git_state, must capture provenance fork-free."""
    res = build_and_persist(project, _agg_code(orders_parquet), prompt="fork-safety")
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
