"""Thin wrappers that keep the xorq catalog git repo in sync.

All functions are best-effort: failures are logged but never propagate to the
caller so a missing or misconfigured catalog repo never breaks the main build.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from pydata_core.paths import catalog_repo_path

log = logging.getLogger(__name__)


def _xorq(*args: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "xorq", "catalog", "--path", str(catalog_repo_path()), *args],
        capture_output=True,
        text=True,
        **kw,
    )


def add_entry(build_dir: Path, aliases: list[str] = (), *, entry_name: str | None = None) -> bool:
    """Add *build_dir* to the xorq catalog, optionally tagging with *aliases*.

    *entry_name* lets the caller control the name xorq uses for the entry
    (xorq derives it from the directory name).  When supplied the build dir
    is copied into a temp location first.

    Returns True on success.
    """
    repo = catalog_repo_path()
    if not repo.exists():
        log.warning("xorq catalog repo not found at %s — skipping", repo)
        return False

    alias_flags: list[str] = []
    for a in aliases:
        alias_flags += ["-a", a]

    def _run(d: Path) -> bool:
        r = _xorq("add", "--no-sync", str(d), *alias_flags)
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


def add_alias(content_hash: str, alias: str) -> bool:
    """Point *alias* at *content_hash* in the xorq catalog."""
    repo = catalog_repo_path()
    if not repo.exists():
        log.warning("xorq catalog repo not found at %s — skipping", repo)
        return False
    # xorq catalog add-alias NAME ALIAS  (positional: entry name, then alias)
    r = _xorq("add-alias", content_hash, alias, "--no-sync")
    if r.returncode != 0:
        log.warning("xorq catalog add-alias failed: %s", r.stderr.strip())
        return False
    return True


def remove_alias(alias: str) -> bool:
    repo = catalog_repo_path()
    if not repo.exists():
        return False
    r = _xorq("remove-alias", alias, "--no-sync")
    if r.returncode != 0:
        log.warning("xorq catalog remove-alias failed: %s", r.stderr.strip())
        return False
    return True
