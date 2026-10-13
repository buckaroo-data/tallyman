"""What the user typed, read from the Claude Code session that runs this server.

A tool's ``prompt`` argument is whatever the model chose to send, and it sends a summary: for ``qb_epa`` it dropped the
user's "every QB should end up with exactly one contract row". Claude Code starts ``tallyman mcp`` with
``CLAUDE_CODE_SESSION_ID`` in its environment, and the session's transcript is
``<config dir>/projects/<cwd slug>/<session id>.jsonl``, so the server reads the user's words from there and the
model has no hand in them.

Neither the variable nor the transcript's format is a documented interface. When either is missing or unfamiliar,
``user_words`` returns None and the entry keeps only the model's ``prompt``.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

log = logging.getLogger("tallyman_mcp.transcript")

# The tallyman tools that change nothing. A turn that called only these (looked at the data, then asked a question) is
# part of the request the next turn answers; a turn that changed the catalog used up the messages before it.
_READ_ONLY_TOOLS = frozenset(
    {
        "catalog_chart_errors",
        "catalog_diff",
        "catalog_list",
        "catalog_list_display_klasses",
        "catalog_list_post_processings",
        "catalog_list_summary_stats",
        "catalog_peek",
        "catalog_query",
        "catalog_scan_staleness",
        "project_list",
    }
)

# Claude Code wraps a paste in these tags; the user sees the pasted text, not the tags.
_PASTE_TAG = re.compile(r'<pasted_content id="[^"]*">\n?|\n?</pasted_content id="[^"]*">')

_SESSION_ID = re.compile(r"[0-9a-fA-F-]+")


def transcript_path() -> Path | None:
    """The transcript of the Claude Code session that started this process, or None outside one."""
    session = os.environ.get("CLAUDE_CODE_SESSION_ID", "")
    if not _SESSION_ID.fullmatch(session):
        return None
    config = Path(os.environ.get("CLAUDE_CONFIG_DIR") or Path.home() / ".claude")
    found = list((config / "projects").glob(f"*/{session}.jsonl"))
    return max(found, key=lambda p: p.stat().st_mtime) if found else None


def user_words() -> str | None:
    """Every message the user typed since the last earlier turn that changed the catalog, verbatim.

    Taking only the last message would record "yes, do that" for an entry made after a question. Messages from the
    turn making this call count, so each entry made in one turn gets that turn's request.
    """
    path = transcript_path()
    if path is None:
        return None
    try:
        return _words_since_last_change(path.read_text().splitlines())
    except Exception:
        log.warning("could not read the user's words from %s", path, exc_info=True)
        return None


def _words_since_last_change(lines: list[str]) -> str | None:
    typed: list[tuple[int, str]] = []  # (line, text) of each message the user typed
    calls: dict[str, int] = {}  # tool_use id -> line, for the tallyman calls that change something
    changes: list[int] = []  # the lines of those calls that succeeded
    for n, line in enumerate(lines):
        try:
            record = json.loads(line)
        except ValueError:
            continue  # the line Claude Code is writing as we read
        content = (record.get("message") or {}).get("content")
        if record.get("isSidechain"):
            continue
        if record.get("type") == "assistant" and isinstance(content, list):
            for block in content:
                if block.get("type") == "tool_use" and _changes_catalog(block.get("name", "")):
                    calls[block.get("id")] = n
        elif record.get("type") == "user":
            if _typed(record):
                typed.append((n, _text(content)))
            elif isinstance(content, list):
                for block in content:
                    if block.get("type") == "tool_result" and block.get("tool_use_id") in calls and _ok(block):
                        changes.append(calls[block["tool_use_id"]])
    if not typed:
        return None
    turn = typed[-1][0]
    last_change = max((n for n in changes if n < turn), default=-1)
    return "\n\n".join(text for n, text in typed if n > last_change and text) or None


def _typed(record: dict) -> bool:
    """A message the user wrote: typed into Claude Code (queued, a slash command's arguments, an accepted suggestion)
    or given to ``claude -p``. Not a tool result, a task notification, command output, or a message from another
    session."""
    if record.get("isMeta"):
        return False
    return (record.get("origin") or {}).get("kind") == "human" or record.get("promptSource") == "sdk"


def _changes_catalog(tool_name: str) -> bool:
    # An MCP tool is named mcp__<server>__<tool>; the server is "tallyman" under .mcp.json, longer under a plugin.
    parts = tool_name.split("__")
    return len(parts) == 3 and parts[0] == "mcp" and "tallyman" in parts[1] and parts[2] not in _READ_ONLY_TOOLS


def _text(content) -> str:
    if isinstance(content, list):
        content = "\n".join(b.get("text", "") for b in content if b.get("type") == "text")
    return _PASTE_TAG.sub("", content or "").strip()


def _ok(result: dict) -> bool:
    """Whether a tool call succeeded: a tallyman tool replies with a JSON object, with ``error`` on failure."""
    if result.get("is_error"):
        return False
    content = result.get("content")
    if isinstance(content, list):
        content = "".join(b.get("text", "") for b in content if b.get("type") == "text")
    try:
        reply = json.loads(content or "")
    except ValueError:
        return False
    return isinstance(reply, dict) and "error" not in reply
