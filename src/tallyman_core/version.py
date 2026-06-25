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
from pathlib import Path

from tallyman_core.git_util import run_git

# version.py lives at <repo>/src/tallyman_core/version.py.
_REPO_ROOT = Path(__file__).resolve().parents[2]


@functools.lru_cache(maxsize=1)
def git_revision() -> str:
    """Short git revision of the checkout this process was launched from.

    Pinned on first call (``lru_cache``) so it reflects the code the process is
    actually running, not wherever ``HEAD`` moves later in the process's life.
    ``git describe --always --dirty`` yields the abbreviated commit, suffixed
    ``-dirty`` when there are *tracked* working-tree changes (untracked files are
    ignored, so a stray scratch file doesn't flag drift). Routed through
    ``git_util.run_git`` (fork-safe ``posix_spawn``, the sanctioned primitive —
    no bare ``subprocess`` git in src/). Returns ``"unknown"`` when git or the
    repo isn't available — versioning must never break a start.
    """
    try:
        rc, out, _ = run_git(
            ["describe", "--always", "--dirty", "--abbrev=7"],
            cwd=_REPO_ROOT,
            timeout=2.0,
        )
    except OSError:
        return "unknown"
    return out if (rc == 0 and out) else "unknown"


def version_info() -> dict:
    """Structured version payload for ``/api/version``, headers, and logs."""
    rev = git_revision()
    return {"revision": rev, "dirty": rev.endswith("-dirty")}
