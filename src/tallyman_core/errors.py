from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from tallyman_core.paths import ensure_project, errors_path


def _errors_path(project: str) -> Path:
    return errors_path(project)


def record_error(
    project: str,
    code: str,
    message: str,
    prompt: str | None = None,
    tool: str | None = None,
) -> dict:
    """Append a build-failure record. Returns the recorded entry (with id)."""
    ensure_project(project)
    entry = {
        "id": uuid.uuid4().hex[:12],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "prompt": prompt,
        "code": code,
        "message": message,
        "tool": tool,
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
