"""Filesystem helpers shared across the tracked catalog surface.

The decomposed catalog files (aliases, notebook, charts, display configs,
post-processing/stats sources, the entry manifest + schema, prompt history) are
rewritten in place by their own mutators. A bare ``write_text`` is
``.is_file()``-true the instant it is truncated, so a crash mid-write — or the
checkpoint's ``git add -A`` firing in the write window from a separate process —
can stage a torn file a later reader chokes on. Routing every such writer
through ``atomic_write_text`` makes the rewrite a single rename: the destination
is either the old bytes or the new bytes, never a prefix.
"""

from __future__ import annotations

from pathlib import Path


def atomic_write_text(path: Path, text: str) -> Path:
    """Write *text* to *path* atomically (write a sibling ``*.tmp``, then
    ``os.replace`` it into place).

    The tmp sibling shares *path*'s directory so the replace is a
    same-filesystem rename (atomic on POSIX). ``*.tmp`` is gitignored
    (``catalog.GITIGNORE_LINES``), so an interrupted write leaves an
    uncommittable temp rather than a truncated *path*. The caller ensures the
    parent dir exists (every catalog writer already ``mkdir(parents=True)``s).
    """
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)
    return path
