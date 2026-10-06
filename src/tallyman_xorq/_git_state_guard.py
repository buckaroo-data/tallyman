"""Crash-safe wrapper around xorq's git-provenance capture.

During expression compilation xorq records git provenance by shelling out to
``git rev-parse HEAD`` / ``git diff`` / ``git diff --cached``
(``xorq.common.utils.logging_utils.get_git_state``), invoked from the compiler
on the catalog-write path (``xorq/ibis_yaml/compiler.py``). When git is forked
from the long-lived, multithreaded tallyman server it can die with ``SIGSEGV`` (a
macOS fork-from-a-multithreaded-process hazard); ``subprocess`` then raises
``CalledProcessError`` (returncode -11), which propagates out of ``build_expr``
and fails *every* catalog write — turning a best-effort logging detail into a
hard blocker.

Provenance must never be able to fail a write, so this installs a wrapper that:

1. spawns git **fork-free** through ``git_util.run_git`` (``os.posix_spawn``,
   ``tallyman_core.spawn``) — a vfork-style spawn that does not clone the
   parent's address space or threads, so the macOS fork-from-multithreaded
   hazard cannot fire by construction (bare-name ``subprocess`` would fork:
   CPython only takes the posix_spawn path when the executable has a directory
   component, which ``"git"`` lacks); and
2. **degrades to placeholders on any failure** (signal death, missing git,
   timeout) instead of raising.

Each call captures fresh — no permanent cache — so successive builds record the
repo state at build time rather than freezing the first capture. The fix lives
here, in-repo, rather than in xorq. See
``plans/ADR-001-git-subprocess-threading.md`` and
buckaroo-data/nokernel-notebooks#25.
"""

from __future__ import annotations

import hashlib
import os

from tallyman_core.git_util import run_git

# Matches xorq's get_git_state command set (sub-commands; "git" prepended at spawn).
_GIT_COMMANDS = (
    ["rev-parse", "HEAD"],
    ["diff"],
    ["diff", "--cached"],
)


def _run_git(args: list[str], timeout: float = 10.0) -> str:
    """Run ``git <args>`` fork-free; return its stripped stdout.

    Raises on a missing git, a signal death (the *timeout* SIGKILL among them) or a
    non-zero exit, all of which ``_capture`` turns into placeholders.
    """
    rc, out, _ = run_git(args, timeout=timeout)
    if rc < 0:
        raise RuntimeError(f"git {args} killed by signal {-rc}")
    if rc != 0:
        raise RuntimeError(f"git {args} exited {rc}")
    return out


def _format(triple: tuple[str, str, str], hash_diffs: bool) -> dict:
    """Shape a (commit, diff, diff_cached) triple like xorq's get_git_state does."""
    commit, diff, diff_cached = triple
    state = {"commit": commit, "diff": diff, "diff_cached": diff_cached}
    if hash_diffs:
        for key in ("diff", "diff_cached"):
            state[f"{key}_hash"] = hashlib.md5(state.pop(key).encode()).hexdigest()
    return state


def _capture() -> tuple[str, str, str]:
    """Run the git commands fork-free; ``("unknown", "", "")`` on any failure."""
    try:
        out = [_run_git(cmd) for cmd in _GIT_COMMANDS]
        return (out[0], out[1], out[2])
    except BaseException:
        # RuntimeError (signal death like SIGSEGV / non-zero exit),
        # FileNotFoundError, OSError — none of it may reach the caller.
        return ("unknown", "", "")


def _safe_get_git_state(hash_diffs: bool = False) -> dict:
    if os.environ.get("TALLYMAN_DISABLE_GIT_STATE"):
        return _format(("unknown", "", ""), hash_diffs)
    return _format(_capture(), hash_diffs)


_safe_get_git_state._tallyman_guarded = True  # type: ignore[attr-defined]


def install_git_state_guard() -> None:
    """Idempotently replace ``logging_utils.get_git_state`` with the safe version.

    Lazy xorq import so importing tallyman_xorq doesn't pull in xorq eagerly.
    """
    from xorq.common.utils import logging_utils as lu

    if getattr(lu.get_git_state, "_tallyman_guarded", False):
        return
    lu.get_git_state = _safe_get_git_state
