from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from tallyman_core.paths import ensure_project, errors_path

_UNBOUNDED = 1_000_000_000  # list_errors caps at `limit`; this means "all rows"


def _errors_path(project: str) -> Path:
    return errors_path(project)


def record_error(
    project: str,
    code: str,
    message: str,
    prompt: str | None = None,
    tool: str | None = None,
    hash: str | None = None,
) -> dict:
    """Append a build-failure record. Returns the recorded entry (with id).

    ``hash`` ties the failure to the catalog entry that failed to (re)build — set
    by recalc/auto-recalc so a later staleness scan can attribute a lingering
    stale entry to a known prior failure (``error_for_hash``) instead of flagging
    it UNEXPLAINED. ``errors.jsonl`` lives in ``artifacts_dir``, outside the
    catalog git repo, so a recorded failure survives ``reset_to``.
    """
    ensure_project(project)
    entry = {
        "id": uuid.uuid4().hex[:12],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "prompt": prompt,
        "code": code,
        "message": message,
        "tool": tool,
        "hash": hash,
    }
    p = _errors_path(project)
    with p.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")
    return entry


def list_errors(project: str, limit: int = 20) -> list[dict]:
    """Most recent errors first, capped at `limit`."""
    p = _errors_path(project)
    if not p.exists():
        return []
    with p.open() as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    rows.reverse()
    return rows[:limit]


def clear_errors(project: str) -> int:
    """Delete all recorded errors for a project. Returns the count removed.

    A no-op (returns 0) when no errors have been recorded. This is what backs
    a permanent dismiss — the banner re-reads from disk on every page mount,
    so a client-only hide would resurface on the next visit.
    """
    p = _errors_path(project)
    if not p.exists():
        return 0
    with p.open() as fh:
        n = sum(1 for line in fh if line.strip())
    p.unlink()
    return n


def errors_by_hash(project: str) -> dict[str, dict]:
    """Map each entry hash to its most-recent recorded error, in ONE pass over the log.

    The batch form of ``error_for_hash`` for the orphan-stale tie-back, which
    classifies many stale entries at once: read ``errors.jsonl`` a single time and
    keep the freshest error per hash, instead of re-scanning the whole log once per
    entry (the classify pass is O(orphans) lookups).
    """
    out: dict[str, dict] = {}
    for row in list_errors(project, limit=_UNBOUNDED):  # most-recent first
        h = row.get("hash")
        if h is not None and h not in out:  # first seen wins → freshest failure
            out[h] = row
    return out


def error_for_hash(project: str, content_hash: str) -> dict | None:
    """The most recent recorded error carrying ``hash == content_hash``, or None.

    Powers the orphan-stale tie-back: an entry that is still stale because a prior
    recalc/build of it failed is "explained by" that recorded error, not an
    unexplained invariant break. Most-recent-first so the freshest failure wins.
    """
    return errors_by_hash(project).get(content_hash)


def get_error(project: str, error_id: str) -> dict | None:
    p = _errors_path(project)
    if not p.exists():
        return None
    with p.open() as fh:
        for line in fh:
            row = json.loads(line)
            if row.get("id") == error_id:
                return row
    return None
