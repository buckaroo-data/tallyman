"""Git-revision stamping, and the check that every process on a data dir runs the same source.

tallyman has no releases yet, so the version *is* the git revision. Every
long-lived process (the companion, the MCP server) and the built JS bundle
stamps the revision it was launched or built from. That makes drift between them
visible — e.g. a SPA bundle built from a commit the running backend doesn't have
(the class of bug behind #132). The companion exposes its revision at
``GET /api/version`` and on an ``X-Tallyman-Revision`` response header; the SPA
bakes its build-time revision and warns when the two disagree.

The MCP server (one per Claude Code session), the companion and the CLI are started independently, so one of them can
run code from before a pull, a checkout or an edit while another runs the new code, against the same catalogs. They
refuse to work together across revisions: the companion writes its revision into the owner record of the data dir it
claims (``server_lock``), an MCP tool refuses to run against a companion on another one, and the companion refuses a
notify from a client on another one.
"""

from __future__ import annotations

import functools
import hashlib
from pathlib import Path

from tallyman_core.git_util import run_git

# version.py lives at <repo>/src/tallyman_core/version.py.
REPO_ROOT = Path(__file__).resolve().parents[2]


def checkout_revision(root: Path | str) -> str:
    """The revision of the checkout at *root*, as it is on disk now.

    ``git describe --always --dirty`` yields the abbreviated commit, suffixed ``-dirty`` when there are *tracked*
    working-tree changes (untracked files are ignored, so a stray scratch file doesn't flag drift). The suffix is the
    same for every set of changes, so a dirty checkout also names its changes with a short hash of ``git diff HEAD``:
    ``<sha>-dirty.<diff hash>``. Two processes started from one commit with different edits then report different
    revisions. ``packages/app/vite.config.ts`` computes the same string for the SPA bundle.

    Routed through ``git_util.run_git`` (fork-safe ``posix_spawn``, the sanctioned primitive — no bare ``subprocess``
    git in src/). Returns ``"unknown"`` when git or the repo isn't available — versioning must never break a start.
    """
    try:
        rc, out, _ = run_git(["describe", "--always", "--dirty", "--abbrev=7"], cwd=root, timeout=2.0)
        if rc != 0 or not out:
            return "unknown"
        if not out.endswith("-dirty"):
            return out
        rc, diff, _ = run_git(["diff", "--no-ext-diff", "--no-color", "--binary", "HEAD"], cwd=root, timeout=5.0)
    except OSError:
        return "unknown"
    if rc != 0:
        return "unknown"
    return f"{out}.{hashlib.sha1(diff.encode()).hexdigest()[:6]}"


@functools.lru_cache(maxsize=1)
def git_revision() -> str:
    """Revision of the checkout this process was launched from (``checkout_revision``).

    Pinned on first call (``lru_cache``) so it reflects the code the process is
    actually running, not wherever ``HEAD`` moves later in the process's life.
    """
    return checkout_revision(REPO_ROOT)


def version_info() -> dict:
    """Structured version payload for ``/api/version``, headers, and logs."""
    rev = git_revision()
    return {"revision": rev, "dirty": "-dirty" in rev, "source": str(REPO_ROOT)}


def is_this_revision(revision: str | None) -> bool:
    """Whether another process reporting *revision* runs the source this one does. An unknown revision never does."""
    return revision == git_revision() and revision != "unknown"


def describe_source(revision: str | None, source: str | None) -> str:
    """``<revision> from <checkout>`` for a refusal message, noting when that checkout has since moved on.

    The note says which side is stale: a process whose checkout is no longer at the revision it started from is
    running old code.
    """
    if not revision:
        return "no revision (it was started from source older than the revision check)"
    text = f"{revision} from {source or 'an unknown checkout'}"
    if source and Path(source).is_dir():
        now = checkout_revision(source)
        if now not in (revision, "unknown"):
            text += f" (stale: that checkout is now at {now})"
    return text
