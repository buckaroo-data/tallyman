"""Git-revision stamping so each running component reports the source it's on.

tallyman has no releases yet, so the version *is* the git revision. Every
long-lived process (the companion, the MCP server) and the built JS bundle
stamps the revision it was launched or built from. That makes drift between them
visible — e.g. a SPA bundle built from a commit the running backend doesn't have
(the class of bug behind #132). The companion exposes its revision at
``GET /api/version`` and on an ``X-Tallyman-Revision`` response header; the SPA
bakes its build-time revision and warns when the two disagree.
"""

from __future__ import annotations

import functools
import subprocess
from pathlib import Path

# version.py lives at <repo>/src/tallyman_core/version.py.
_REPO_ROOT = Path(__file__).resolve().parents[2]


@functools.lru_cache(maxsize=1)
def git_revision() -> str:
    """Short git revision of the checkout this process was launched from.

    Pinned on first call (``lru_cache``) so it reflects the code the process is
    actually running, not wherever ``HEAD`` moves later in the process's life.
    ``git describe --always --dirty`` yields the abbreviated commit, suffixed
    ``-dirty`` when there are *tracked* working-tree changes (untracked files are
    ignored, so a stray scratch file doesn't flag drift). Returns ``"unknown"``
    when git or the repo isn't available — versioning must never break a start.
    """
    try:
        out = subprocess.run(
            ["git", "-C", str(_REPO_ROOT), "describe", "--always", "--dirty", "--abbrev=7"],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return out or "unknown"


def version_info() -> dict:
    """Structured version payload for ``/api/version``, headers, and logs."""
    rev = git_revision()
    return {"revision": rev, "dirty": rev.endswith("-dirty")}
