#!/usr/bin/env python3
"""Standalone reproduction of the xorq git-state SIGSEGV catalog-write bug.

See buckaroo-data/nokernel-notebooks#25.

THE BUG
-------
During expression compilation, xorq records git provenance by shelling out to
``git rev-parse HEAD`` / ``git diff`` / ``git diff --cached`` in
``xorq.common.utils.logging_utils.get_git_state`` — called inline from the
compiler (``xorq/ibis_yaml/compiler.py:507``) on the catalog-write path
(``build_and_persist`` -> ``build_expr``).

That function has no error containment. When git is forked from the long-lived,
multithreaded pydata server it can die with SIGSEGV (a macOS
fork-from-a-multithreaded-process hazard); ``subprocess.check_output`` then
raises ``CalledProcessError`` (returncode -11). Because the call sits *inline*
in the build path, that exception propagates out of ``build_expr`` and fails
**every** catalog write — a best-effort logging detail becomes a hard blocker.

WHAT THIS FILE DOES
-------------------
1. Embeds ``get_git_state`` / ``_git_is_present`` **verbatim** from xorq 0.3.26
   (the function under test) plus ``simulate_catalog_write`` mirroring the
   compiler's inline call site.
2. Reproduces the failure deterministically: a fake ``git`` on PATH that kills
   itself with SIGSEGV, exactly like the observed crash — no flaky macOS repro
   needed.
3. Patches the function with the crash-safe guard (mirrors
   ``src/pydata_xorq/_git_state_guard.py``) and proves the build now succeeds.
4. If xorq is importable, repeats the proof against the **real** xorq module +
   the **real** in-repo ``install_git_state_guard`` for an end-to-end check.

Run:  ``uv run python repro_git_state_segfault.py``   (exit 0 == all asserts hold)
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import pathlib
import stat
import subprocess
import sys
import tempfile


# ---------------------------------------------------------------------------
# 1. THE FUNCTION UNDER TEST — copied verbatim from xorq 0.3.26
#    site-packages/xorq/common/utils/logging_utils.py
# ---------------------------------------------------------------------------
def _git_is_present(cwd=None):
    if cwd is None:
        cwd = pathlib.Path().absolute()
    return any(p for p in (cwd, *cwd.parents) if p.joinpath(".git").exists())


def get_git_state(hash_diffs):
    (commit, diff, diff_cached) = (
        subprocess.check_output(lst).decode().strip()
        for lst in (
            ["git", "rev-parse", "HEAD"],
            ["git", "diff"],
            ["git", "diff", "--cached"],
        )
    )
    git_state = {"commit": commit, "diff": diff, "diff_cached": diff_cached}
    if hash_diffs:
        for key in ("diff", "diff_cached"):
            git_state[f"{key}_hash"] = hashlib.md5(git_state.pop(key).encode()).hexdigest()
    return git_state


# ---------------------------------------------------------------------------
# 2. THE CALL SITE — mirrors xorq/ibis_yaml/compiler.py:507, which captures git
#    provenance INLINE while building an expression. A catalog write funnels
#    through this; if it raises, the write fails.
# ---------------------------------------------------------------------------
def simulate_catalog_write(expr):
    """Analog of build_and_persist -> build_expr. Returns the build metadata."""
    metadata = {
        "expr": expr,
        # Resolved by module-global lookup at call time, so apply_fix() below
        # can swap the implementation — exactly how the guard swaps lu.get_git_state.
        "git_state": get_git_state(hash_diffs=False) if _git_is_present() else None,
    }
    # ... real build would now serialize + materialize a parquet here ...
    return metadata


# ---------------------------------------------------------------------------
# 3. THE FIX — crash-safe replacement (mirrors src/pydata_xorq/_git_state_guard.py):
#    capture once + cache, and degrade to placeholders on ANY failure.
# ---------------------------------------------------------------------------
_UNSET = object()
_raw_state: object = _UNSET
_GIT_COMMANDS = (
    ["git", "rev-parse", "HEAD"],
    ["git", "diff"],
    ["git", "diff", "--cached"],
)


def _format(triple, hash_diffs):
    commit, diff, diff_cached = triple
    state = {"commit": commit, "diff": diff, "diff_cached": diff_cached}
    if hash_diffs:
        for key in ("diff", "diff_cached"):
            state[f"{key}_hash"] = hashlib.md5(state.pop(key).encode()).hexdigest()
    return state


def safe_get_git_state(hash_diffs=False):
    global _raw_state
    if os.environ.get("PYDATA_DISABLE_GIT_STATE"):
        return _format(("unknown", "", ""), hash_diffs)
    if _raw_state is _UNSET:
        try:
            out = [
                subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=10)
                .decode()
                .strip()
                for cmd in _GIT_COMMANDS
            ]
            _raw_state = (out[0], out[1], out[2])
        except BaseException:
            # CalledProcessError (incl. signal death), FileNotFoundError, TimeoutExpired
            _raw_state = ("unknown", "", "")
    return _format(_raw_state, hash_diffs)


def apply_fix():
    """Patch the function under test — the standalone analog of installing the guard."""
    global get_git_state, _raw_state
    _raw_state = _UNSET  # fresh capture under the patched implementation
    get_git_state = safe_get_git_state


# ---------------------------------------------------------------------------
# 4. HARNESS — a fake `git` that dies with SIGSEGV, in a dir that looks like a repo.
# ---------------------------------------------------------------------------
@contextlib.contextmanager
def segfaulting_git_environment():
    """Prepend a SIGSEGV-on-invoke `git` to PATH and cd into a fake repo."""
    orig_cwd = os.getcwd()
    orig_path = os.environ["PATH"]
    tmp = tempfile.mkdtemp(prefix="repro_gitsegv_")
    try:
        repo = pathlib.Path(tmp, "repo")
        (repo / ".git").mkdir(parents=True)  # makes _git_is_present() True
        bindir = pathlib.Path(tmp, "bin")
        bindir.mkdir()
        fake_git = bindir / "git"
        fake_git.write_text("#!/bin/sh\nkill -SEGV $$\n")  # exit via SIGSEGV (returncode -11)
        fake_git.chmod(fake_git.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

        os.chdir(repo)
        os.environ["PATH"] = f"{bindir}{os.pathsep}{orig_path}"
        yield
    finally:
        os.chdir(orig_cwd)
        os.environ["PATH"] = orig_path
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)


def _banner(title):
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------
def reproduce_bug():
    """Returns True iff the bug reproduces (catalog write dies with SIGSEGV)."""
    _banner("PHASE 1 — REPRODUCE: pristine get_git_state + segfaulting git")
    try:
        md = simulate_catalog_write(expr="<demo expr>")
        print(f"  catalog write SUCCEEDED (unexpected): {md['git_state']}")
        return False
    except subprocess.CalledProcessError as exc:
        print(f"  catalog write FAILED as expected: {type(exc).__name__}: {exc}")
        reproduced = exc.returncode == -11  # -SIGSEGV
        print(f"  git died with SIGSEGV (returncode == -11): {reproduced}")
        return reproduced
    except Exception as exc:  # noqa: BLE001
        print(f"  catalog write FAILED with unexpected error: {type(exc).__name__}: {exc}")
        return False


def verify_fixed():
    """Returns True iff, after patching, the write succeeds and degrades gracefully."""
    _banner("PHASE 2 — FIX: patch get_git_state with the crash-safe guard")
    apply_fix()
    try:
        md = simulate_catalog_write(expr="<demo expr>")
    except Exception as exc:  # noqa: BLE001
        print(f"  catalog write STILL FAILED (fix did not work): {type(exc).__name__}: {exc}")
        return False
    gs = md["git_state"]
    print(f"  catalog write SUCCEEDED. git_state = {gs}")
    ok = gs == {"commit": "unknown", "diff": "", "diff_cached": ""}
    print(f"  degraded to placeholder provenance instead of raising: {ok}")

    # hash_diffs path also degrades cleanly (md5 of "" == d41d8cd9...)
    h = safe_get_git_state(hash_diffs=True)
    empty_md5 = hashlib.md5(b"").hexdigest()
    ok_hash = h.get("diff_hash") == empty_md5 and h.get("diff_cached_hash") == empty_md5
    print(f"  hash_diffs variant degrades cleanly: {ok_hash}")
    return ok and ok_hash


def healthy_git_still_works():
    """Sanity: with a real git (outside the fake env), provenance is preserved."""
    _banner("PHASE 3 — SANITY: patched function + healthy git preserves provenance")
    global _raw_state
    _raw_state = _UNSET
    if not pathlib.Path(".git").exists() and not any(
        p.joinpath(".git").exists() for p in pathlib.Path().absolute().parents
    ):
        print("  (skipped — not inside a git repo)")
        return True
    gs = safe_get_git_state(hash_diffs=False)
    ok = isinstance(gs["commit"], str) and len(gs["commit"]) == 40
    print(f"  commit = {gs['commit']!r} | looks like a real sha: {ok}")
    return ok


def real_xorq_proof():
    """If xorq is importable, prove it against the REAL module + the in-repo guard."""
    _banner("PHASE 4 — REAL STACK: against actual xorq + src/pydata_xorq guard")
    try:
        from xorq.common.utils import logging_utils as lu
    except Exception as exc:  # noqa: BLE001
        print(f"  (skipped — xorq not importable here: {type(exc).__name__})")
        return True

    with segfaulting_git_environment():
        # Reproduce on the real function.
        try:
            lu.get_git_state(hash_diffs=False)
            print("  real xorq get_git_state SUCCEEDED (unexpected)")
            return False
        except subprocess.CalledProcessError as exc:
            print(f"  real xorq get_git_state FAILED as expected: {exc}")

        # Install the real in-repo guard and prove it.
        try:
            from pydata_xorq._git_state_guard import install_git_state_guard
        except Exception as exc:  # noqa: BLE001
            print(f"  (could not import in-repo guard: {type(exc).__name__}: {exc})")
            return False
        install_git_state_guard()
        gs = lu.get_git_state(hash_diffs=False)
        ok = gs == {"commit": "unknown", "diff": "", "diff_cached": ""}
        print(f"  after install_git_state_guard(): {gs} -> contained: {ok}")
        return ok


def main():
    reproduced = verified = sanity = real = False
    with segfaulting_git_environment():
        reproduced = reproduce_bug()
        verified = verify_fixed()
    sanity = healthy_git_still_works()
    real = real_xorq_proof()

    _banner("SUMMARY")
    rows = [
        ("bug reproduced (git SIGSEGV blocks catalog write)", reproduced),
        ("fix contains it (write succeeds, provenance -> unknown)", verified),
        ("healthy git still records the real commit", sanity),
        ("holds against the real xorq stack", real),
    ]
    for label, ok in rows:
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
    all_ok = all(ok for _, ok in rows)
    print(f"\n{'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
