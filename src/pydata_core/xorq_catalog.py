"""Thin wrappers that keep the xorq catalog git repo in sync.

All functions are best-effort: failures are logged but never propagate to the
caller so a missing or misconfigured catalog repo never breaks the main build.

Each function takes an explicit ``project`` argument — the xorq catalog
repo lives at ``<project>/artifacts/catalog/``, and the calling layer
(aliases.py, build.py) already knows which project it's mutating. Using
``resolve_project()`` here would risk writing to the wrong catalog if the
user switched projects mid-operation.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from pydata_core.paths import catalog_dir

log = logging.getLogger(__name__)


def _repo_path(project: str) -> Path:
    return catalog_dir(project)


def _xorq(project: str, *args: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "xorq", "catalog", "--path", str(_repo_path(project)), *args],
        capture_output=True,
        text=True,
        **kw,
    )


def add_entry(
    project: str,
    build_dir: Path,
    aliases: list[str] = (),
    *,
    entry_name: str | None = None,
) -> bool:
    """Add *build_dir* to the xorq catalog, optionally tagging with *aliases*.

    *entry_name* lets the caller control the name xorq uses for the entry
    (xorq derives it from the directory name).  When supplied the build dir
    is copied into a temp location first.

    Returns True on success.
    """
    repo = _repo_path(project)
    if not repo.exists():
        log.warning("xorq catalog repo not found at %s — skipping", repo)
        return False

    alias_flags: list[str] = []
    for a in aliases:
        alias_flags += ["-a", a]

    def _run(d: Path) -> bool:
        r = _xorq(project, "add", "--no-sync", str(d), *alias_flags)
        if r.returncode != 0:
            log.warning("xorq catalog add failed for %s: %s", d, r.stderr.strip())
            return False
        return True

    if entry_name and entry_name != build_dir.name:
        with tempfile.TemporaryDirectory() as tmp:
            named = Path(tmp) / entry_name
            shutil.copytree(build_dir, named)
            return _run(named)
    return _run(build_dir)


def add_alias(project: str, content_hash: str, alias: str) -> bool:
    """Point *alias* at *content_hash* in the project's xorq catalog."""
    repo = _repo_path(project)
    if not repo.exists():
        log.warning("xorq catalog repo not found at %s — skipping", repo)
        return False
    r = _xorq(project, "add-alias", content_hash, alias, "--no-sync")
    if r.returncode != 0:
        log.warning("xorq catalog add-alias failed: %s", r.stderr.strip())
        return False
    return True


def remove_alias(project: str, alias: str) -> bool:
    repo = _repo_path(project)
    if not repo.exists():
        return False
    r = _xorq(project, "remove-alias", alias, "--no-sync")
    if r.returncode != 0:
        log.warning("xorq catalog remove-alias failed: %s", r.stderr.strip())
        return False
    return True
