"""Pydata-owned state in the xorq catalog's catalog.yaml + the reset engine.

catalog.yaml is the versioned state carrier (see plans/adr-reset-to-revision.md).
xorq owns ``entries`` and ``aliases`` in that file; pydata owns:

    post_processing: [{name, source, disabled}]
    stats:           [{name, source, disabled}]
    charts:          [{content_hash, spec}]
    notebook:        {cells: [{cell_id, alias, markdown}]}
    entry_hashes:    [content_hash, ...]      # pointer to untracked entries/<hash>/
    result_cache:    [relpath, ...]           # pointer to untracked result cache
    compute_cache:   [relpath, ...]           # pointer to untracked compute cache

The catalog git repo tracks only ``catalog.yaml`` + the two alias files. The
heavy artifacts (entries/, the caches) are content-addressed, additive, and
untracked — ``git reset`` can't roll them back, so ``reset_to`` prunes them to
the recorded pointer lists. The derived files (.py scripts, chart specs, the
notebook) live outside the repo, so they are reconstructed by ``materialize``.

A checkpoint captures the live on-disk state into catalog.yaml and commits once,
through the fork-safe git primitive (git_util) under a per-project lock. This
keeps the four invariants #33 kept re-breaking structural rather than reviewed.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import shutil
from pathlib import Path

import yaml

from pydata_core import charts, notebook
from pydata_core import post_processing as pp
from pydata_core import summary_stats as ss
from pydata_core.git_util import run_git
from pydata_core.paths import (
    catalog_dir,
    compute_cache_dir,
    entries_dir,
    notebooks_dir,
    post_processing_dir,
    result_cache_dir,
    stats_dir,
)

log = logging.getLogger(__name__)

# git-level identity flags, so commits work without a global git config.
_GIT_ID = ["-c", "user.email=pydata@local", "-c", "user.name=pydata"]
_TRACKED = ("catalog.yaml", "aliases.json", "alias_history.json")
_STEP_RE = re.compile(r"^step-(\d+)$")


# ---------------------------------------------------------------------------
# catalog.yaml read / write (read-modify-write so xorq's keys survive)
# ---------------------------------------------------------------------------


def _catalog_yaml(project: str) -> Path:
    return catalog_dir(project) / "catalog.yaml"


def _read_raw(project: str) -> dict:
    p = _catalog_yaml(project)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text()) or {}


def read_pydata_state(project: str) -> dict:
    """The pydata-owned keys, each defaulted so callers never KeyError."""
    raw = _read_raw(project)
    return {
        "post_processing": raw.get("post_processing", []),
        "stats": raw.get("stats", []),
        "charts": raw.get("charts", []),
        "notebook": raw.get("notebook", {"cells": []}),
        "entry_hashes": raw.get("entry_hashes", []),
        "result_cache": raw.get("result_cache", []),
        "compute_cache": raw.get("compute_cache", []),
    }


def write_pydata_state(project: str, **keys) -> None:
    """Merge pydata keys into catalog.yaml atomically, preserving xorq's keys."""
    p = _catalog_yaml(project)
    p.parent.mkdir(parents=True, exist_ok=True)
    raw = _read_raw(project)
    for k, v in keys.items():
        if v is not None:
            raw[k] = v
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))
    tmp.replace(p)


# ---------------------------------------------------------------------------
# capture: live on-disk state -> catalog.yaml keys
# ---------------------------------------------------------------------------


def _scan_scripts(listing: list[dict]) -> list[dict]:
    return [{"name": e["name"], "source": e["source"], "disabled": e["disabled"]} for e in listing]


def _list_cache_files(root: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def capture_pydata_state(project: str) -> dict:
    """Read the live authored state + artifact listings into catalog.yaml."""
    ed = entries_dir(project)
    state = {
        "post_processing": _scan_scripts(pp.list_post_processings(project)),
        "stats": _scan_scripts(ss.list_stats(project)),
        "charts": [{"content_hash": h, "spec": charts.get_chart(project, h)} for h in charts.list_charts(project)],
        "notebook": notebook.load(project),
        "entry_hashes": sorted(c.name for c in ed.iterdir() if c.is_dir()) if ed.exists() else [],
        "result_cache": _list_cache_files(result_cache_dir(project)),
        "compute_cache": _list_cache_files(compute_cache_dir(project)),
    }
    write_pydata_state(project, **state)
    return state


# ---------------------------------------------------------------------------
# materialize: catalog.yaml keys -> on-disk derived files (non-destructive)
# ---------------------------------------------------------------------------


def _materialize_scripts(target: Path, entries: list[dict]) -> None:
    for d in (target, target / "_disabled"):
        if d.exists():
            shutil.rmtree(d)
    target.mkdir(parents=True, exist_ok=True)
    for e in entries or []:
        dest = (target / "_disabled") if e.get("disabled") else target
        dest.mkdir(parents=True, exist_ok=True)
        src = e.get("source", "")
        (dest / f"{e['name']}.py").write_text(src if src.endswith("\n") else src + "\n")


def _materialize_charts(project: str, entries: list[dict]) -> None:
    recorded = {c["content_hash"]: c["spec"] for c in entries or []}
    for h in list(charts.list_charts(project)):
        if h not in recorded:
            charts.remove_chart(project, h)
    for h, spec in recorded.items():
        charts.set_chart(project, h, spec)


def _materialize_notebook(project: str, nb: dict) -> None:
    nbfile = notebooks_dir(project) / "default.json"
    cells = (nb or {}).get("cells") or []
    if cells:
        notebooks_dir(project).mkdir(parents=True, exist_ok=True)
        nbfile.write_text(json.dumps({"cells": cells}, indent=2))
    else:
        nbfile.unlink(missing_ok=True)


def materialize(project: str) -> None:
    """Reconstruct on-disk derived files from catalog.yaml.

    A section is rebuilt only when its key is *present*: an absent key means
    the project never recorded that state (e.g. predates these keys), so its
    on-disk files are left alone rather than wiped. A present-but-empty list is
    a deliberate "none at this step" and clears the section.
    """
    raw = _read_raw(project)
    if "post_processing" in raw:
        _materialize_scripts(post_processing_dir(project), raw["post_processing"])
    if "stats" in raw:
        _materialize_scripts(stats_dir(project), raw["stats"])
    if "charts" in raw:
        _materialize_charts(project, raw["charts"])
    if "notebook" in raw:
        _materialize_notebook(project, raw["notebook"] or {"cells": []})


# ---------------------------------------------------------------------------
# prune: reconcile untracked content-addressed artifacts to the pointer lists
# ---------------------------------------------------------------------------


def prune_entries(project: str) -> int:
    """Remove entry dirs not named by ``entry_hashes``. No-op when key absent."""
    raw = _read_raw(project)
    if "entry_hashes" not in raw:
        return 0
    valid = set(raw["entry_hashes"] or [])
    ed = entries_dir(project)
    if not ed.exists():
        return 0
    removed = 0
    for child in ed.iterdir():
        if child.is_dir() and child.name not in valid:
            shutil.rmtree(child)
            removed += 1
    return removed


def _prune_cache(project: str, root: Path, key: str) -> int:
    raw = _read_raw(project)
    if key not in raw:
        return 0
    valid = set(raw[key] or [])
    if not root.exists():
        return 0
    removed = 0
    for p in root.rglob("*"):
        if p.is_file() and str(p.relative_to(root)) not in valid:
            p.unlink()
            removed += 1
    return removed


def prune_result_cache(project: str) -> int:
    return _prune_cache(project, result_cache_dir(project), "result_cache")


def prune_compute_cache(project: str) -> int:
    return _prune_cache(project, compute_cache_dir(project), "compute_cache")


# ---------------------------------------------------------------------------
# the catalog git repo: init, checkpoint (one commit per op), reset
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _project_lock(project: str):
    """Cross-process file lock so the companion and MCP server can't race a
    checkpoint (the git index.lock race that swallowed #33 writes)."""
    cd = catalog_dir(project)
    cd.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(cd / ".checkpoint.lock"), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def ensure_catalog_repo(project: str) -> bool:
    """Idempotently git-init the catalog repo. Returns True if it exists after."""
    cd = catalog_dir(project)
    cd.mkdir(parents=True, exist_ok=True)
    if (cd / ".git").exists():
        return True
    rc, _, err = run_git(["init", "-q"], cwd=cd)
    if rc != 0:
        log.warning("catalog git init failed in %s: %s", cd, err)
        return False
    return True


def _step_tags(project: str) -> list[int]:
    rc, out, _ = run_git(["tag", "-l", "step-*"], cwd=catalog_dir(project))
    nums = []
    for t in out.split():
        m = _STEP_RE.match(t)
        if m:
            nums.append(int(m.group(1)))
    return sorted(nums)


def checkpoint_catalog(project: str, message: str, *, step: int | None = None, label: str | None = None) -> int:
    """Capture state, commit catalog.yaml + alias files once, tag the step.

    One commit per operation (not per mutator), through the fork-safe primitive,
    under the per-project lock. Returns the step number.
    """
    cd = catalog_dir(project)
    with _project_lock(project):
        ensure_catalog_repo(project)
        capture_pydata_state(project)
        existing = [f for f in _TRACKED if (cd / f).exists()]
        if existing:
            run_git(["add", *existing], cwd=cd)
        if step is None:
            tags = _step_tags(project)
            step = tags[-1] + 1 if tags else 0
        rc, _, err = run_git([*_GIT_ID, "commit", "-m", message, "--allow-empty"], cwd=cd)
        if rc != 0:
            log.warning("catalog checkpoint commit failed (step %s): %s", step, err)
        run_git(["tag", "-f", f"step-{step:03d}"], cwd=cd)
        if label:
            run_git(["tag", "-f", label], cwd=cd)
    return step


def reset_to(project: str, ref: int | str) -> None:
    """Restore the catalog to a step/label: git reset --hard, materialize the
    derived files, prune the untracked artifacts to the recorded pointers."""
    cd = catalog_dir(project)
    tag = f"step-{ref:03d}" if isinstance(ref, int) else str(ref)
    with _project_lock(project):
        rc, _, err = run_git(["reset", "--hard", tag], cwd=cd)
        if rc != 0:
            raise RuntimeError(f"catalog reset to {tag!r} failed: {err}")
        materialize(project)
        prune_entries(project)
        prune_result_cache(project)
        prune_compute_cache(project)
