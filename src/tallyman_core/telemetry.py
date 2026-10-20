"""Per-grid-load telemetry spans pushed from the Buckaroo server (``telemetry.jsonl``).

The activity log (``events.jsonl``) records one *companion-visible* event per
Buckaroo grid load: status plus the ``load_ms`` / ``load_expr_ms`` timing the
companion can measure on its own side. Everything *inside* that load — the
expression load, the stats pipeline, whether the summary-stat cache was hit, the
first WebSocket row payload — happens in the Buckaroo subprocess and the widget,
where the companion is structurally blind (buckaroo-data/buckaroo#943).

Buckaroo 0.15.3+ closes that gap: pass ``telemetry_url`` in the ``/load_expr``
POST body and the server fire-and-forget POSTs one record per perf span back to
that URL as the load progresses. Each record is OTel-shaped::

    {"trace": <buckaroo session id>, "source": "server", "name": "firstpull.summary_stats",
     "t_start_ms": 1.7e12, "t_end_ms": 1.7e12, "attrs": {"cache_status": "hit", ...}}

``trace`` is the Buckaroo session id, which the companion records on the parent
``buckaroo`` grid-load event — so the Log UI joins a load's spans back to its
event. Spans for one load trickle in over time (the ``firstpull.load_expr`` span
closes when the POST returns; ``firstpull.summary_stats`` and the WS-payload spans
arrive later, once the browser connects and pulls rows), so this is a persisted
store the UI re-reads on refresh rather than a synchronous response.

Lives in ``artifacts_dir`` beside ``events.jsonl`` / ``errors.jsonl`` — outside
the catalog git repo, so it survives ``reset_to``. One JSON line per span, so
concurrent appends from interleaved loads stay intact (POSIX ``O_APPEND``
atomicity under ``PIPE_BUF``). Read by ``GET /{project}/api/telemetry`` for the
UI's per-load span waterfall.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone

from tallyman_core.paths import artifacts_dir, ensure_project

# read_spans(received_since_ms=...) reads the file backward in blocks of this size, and keeps reading until a line is
# this much older than the cutoff.
_TAIL_BLOCK_BYTES = 256 * 1024
_TAIL_SLACK_MS = 60_000

# Records this size or larger are dropped rather than appended. A span record is
# a handful of small fields; anything past this is a malformed or hostile body,
# and we don't want one bad POST to bloat the log. Generous enough that a real
# span (even with verbose attrs) never trips it.
_MAX_RECORD_BYTES = 64 * 1024


def _telemetry_path(project: str):
    return artifacts_dir(project) / "telemetry.jsonl"


def record_span(project: str, record: dict) -> dict | None:
    """Append one telemetry span record, stamped with the server's receive time.

    ``record`` is the raw payload Buckaroo POSTed: ``{trace, source, name,
    t_start_ms, t_end_ms, attrs}``. We persist it verbatim plus a ``received_at``
    ISO timestamp (the companion's wall clock — useful for ordering spans whose
    ``t_start_ms`` come from a different process's clock). Best-effort and never
    raises: telemetry must never surface as an error to the Buckaroo server that
    POSTed it. Returns the stored record, or ``None`` if it was rejected
    (oversized / unserializable) or the write failed.
    """
    try:
        ev = {"received_at": datetime.now(timezone.utc).isoformat(), **record}
        line = json.dumps(ev)
    except (TypeError, ValueError):
        return None
    if len(line) > _MAX_RECORD_BYTES:
        return None
    try:
        ensure_project(project)
        p = _telemetry_path(project)
        with p.open("a") as fh:
            fh.write(line + "\n")
    except OSError:
        return None
    return ev


def _lines_newest_first(path) -> Iterator[bytes]:
    """The file's lines from the last to the first, reading it in blocks from the end."""
    with path.open("rb") as fh:
        pos = fh.seek(0, os.SEEK_END)
        partial = b""  # the head of the block read last: a line that continues into the block before it
        while pos > 0:
            step = min(_TAIL_BLOCK_BYTES, pos)
            pos -= step
            fh.seek(pos)
            lines = (fh.read(step) + partial).split(b"\n")
            partial = lines[0]
            yield from reversed(lines[1:])
        yield partial


def _received_ms(ev: dict) -> float | None:
    try:
        return datetime.fromisoformat(ev["received_at"]).timestamp() * 1000
    except (KeyError, TypeError, ValueError):
        return None


def read_spans(
    project: str,
    *,
    trace: str | None = None,
    limit: int = 500,
    received_since_ms: float | None = None,
) -> list[dict]:
    """Spans newest-first (by ``received_at``), optionally filtered to one ``trace``.

    With ``received_since_ms`` (epoch ms), only spans received at or after it, and the file is read backward and
    left unread past that point: lines are appended in the order they are received, so once one is a minute older
    than the cutoff the rest are older still. The file only grows, so a caller that polls wants this.

    Tolerant of a partially-written trailing line (a concurrent append mid-read):
    a line that doesn't parse is skipped rather than failing the whole read.
    """
    p = _telemetry_path(project)
    if not p.exists():
        return []
    out: list[dict] = []
    if received_since_ms is None:
        lines: Iterable[str | bytes] = p.read_text().splitlines()
    else:
        lines = _lines_newest_first(p)
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        if not isinstance(ev, dict):
            continue
        if received_since_ms is not None:
            received = _received_ms(ev)
            if received is not None and received < received_since_ms - _TAIL_SLACK_MS:
                break
            if received is None or received < received_since_ms:
                continue
        if trace is not None and ev.get("trace") != trace:
            continue
        out.append(ev)
    out.sort(key=lambda e: e.get("received_at", ""), reverse=True)
    return out[:limit]
