"""Append-only project activity log (``events.jsonl``).

A linear, cross-session record of what happened in a project: MCP build runs
(success / failure, with the prompt, code, and full traceback), alias creations,
and Buckaroo grid-load attempts. Multiple Claude Code sessions write to one
project, so each ``tallyman mcp`` process stamps its own ``session`` id; every
event is a single JSON line, so concurrent appends across processes stay intact
(POSIX ``O_APPEND`` atomicity for writes under PIPE_BUF). Lives in
``artifacts_dir`` beside ``errors.jsonl`` — outside the catalog git repo, so it
survives ``reset_to``. Read by ``GET /{project}/api/log`` for the UI's
filterable, expandable log.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from tallyman_core.paths import artifacts_dir, ensure_project

# event kind -> the filter category the UI groups it under.
_CATEGORY = {
    "build_ok": "mcp",
    "build_error": "mcp",
    "alias_set": "alias",
    "buckaroo": "buckaroo",
}


def _events_path(project: str):
    return artifacts_dir(project) / "events.jsonl"


def record_event(project: str, kind: str, **fields) -> dict:
    """Append one event. ``kind`` is one of build_ok / build_error / alias_set /
    buckaroo; ``fields`` carries the kind-specific payload (session, tool, alias,
    version, hash, prompt, code, message, traceback, status, detail, *_ms …).
    Best-effort and never raises — logging must not break a build or a page."""
    ev = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "kind": kind,
        "category": _CATEGORY.get(kind, "other"),
        **fields,
    }
    try:
        ensure_project(project)
        p = _events_path(project)
        with p.open("a") as fh:
            fh.write(json.dumps(ev) + "\n")
    except OSError:
        pass
    return ev


def read_events(
    project: str,
    *,
    categories: list[str] | None = None,
    sessions: list[str] | None = None,
    limit: int = 300,
) -> list[dict]:
    """Events newest-first, optionally filtered by category and/or session.

    Tolerant of a partially-written trailing line (a concurrent append mid-read):
    a line that doesn't parse is skipped rather than failing the whole read.
    """
    p = _events_path(project)
    if not p.exists():
        return []
    cat = set(categories) if categories else None
    sess = set(sessions) if sessions else None
    out: list[dict] = []
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if cat is not None and ev.get("category") not in cat:
            continue
        if sess is not None and ev.get("session") not in sess:
            continue
        out.append(ev)
    out.sort(key=lambda e: e.get("ts", ""), reverse=True)
    return out[:limit]


def list_sessions(project: str) -> list[dict]:
    """Distinct sessions seen in the log, newest-active first, with a count and
    last-seen ts — for the UI's per-session filter."""
    seen: dict[str, dict] = {}
    for ev in read_events(project, limit=100_000):
        s = ev.get("session")
        if s is None:
            continue
        rec = seen.setdefault(s, {"session": s, "count": 0, "last_ts": ""})
        rec["count"] += 1
        if ev.get("ts", "") > rec["last_ts"]:
            rec["last_ts"] = ev["ts"]
    return sorted(seen.values(), key=lambda r: r["last_ts"], reverse=True)
