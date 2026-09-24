"""The clone store: tallyman's frozen copy of every byte that was ever imported.

An import copies the file it was given to ``data/.cas/<digest><suffix>``, named by the md5 of its
content, and verifies the copy against that name before publishing it (ADR-011 D9). The clone is what
makes a source version durable: the outside path is provenance and may be gone, the snapshot under
``compute_cache/`` is cache anyone may delete, and the clone is what re-creates the snapshot and what
ADR-005's suggestion-and-retry contract re-reads when a CSV was imported under the wrong schema.

There is one mode (ADR-011 D8). ADR-002 made this switchable — ``off`` (path identity only), ``cas``
(read through the clone) and ``salt`` (mix the digest into the entry hash) — so the cache lab could
benchmark them against each other. Import always digests and always clones now, because a raw input is
an entry and an entry's identity is its bytes; there is no configuration under which a raw input is
unversioned, so the env switch, the two other modes and ``salted_hash`` are gone.

Nothing memoizes a digest either. The memo existed so a build did not re-hash an unchanged source on
every run, and it keyed on ``(mtime_ns, size, inode)``, which a same-stat in-place swap defeats. A file
is digested once now, at the moment it is imported, so the memo has nothing to save and its hole is not
worth keeping.

``gc_cas`` retires a clone no live entry needs. Liveness is the DAG: a source version's clone is alive
exactly while its entry is (``catalog_state._live_source_digests`` over ``manifest.provenance``).
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from tallyman_core import data_dir


class CloneDigestMismatch(ValueError):
    """The clone tallyman just wrote does not hash to the name it was given (ADR-011 D9)."""


class LostSourceVersion(FileNotFoundError):
    """A version tallyman promised is gone: no clone, and the live file no longer has those bytes (ADR-011 D9)."""


# ---------------------------------------------------------------------------
# Digests
# ---------------------------------------------------------------------------


def _digest_file(path: Path) -> str:
    with path.open("rb") as fh:
        return hashlib.file_digest(fh, hashlib.md5).hexdigest()


# ---------------------------------------------------------------------------
# the clone store
# ---------------------------------------------------------------------------


def _clone(src: Path, dst: Path) -> None:
    """Copy-on-write clone when the platform offers one, else a plain copy.

    NOT a hardlink: a hardlink shares the inode, so an in-place edit of the
    source would silently rewrite the "snapshot" and the digest-named file
    would lie about its content.

    macOS uses ``cp -c`` (APFS clonefile); Linux uses ``cp --reflink=auto``,
    a CoW clone on btrfs/XFS that degrades to a real copy on ext4 (a full copy
    per content version — the cost the ``.cas`` GC bounds). Both fall back to
    ``shutil.copy2`` when the platform ``cp`` is unavailable or fails.
    """
    if sys.platform == "darwin":
        proc = subprocess.run(["cp", "-c", str(src), str(dst)], capture_output=True)
        if proc.returncode == 0:
            return
    elif sys.platform.startswith("linux"):
        proc = subprocess.run(["cp", "--reflink=auto", str(src), str(dst)], capture_output=True)
        if proc.returncode == 0:
            return
    shutil.copy2(src, dst)


def cas_path(project: str, digest: str, suffix: str) -> Path:
    """Where the clone of the bytes named *digest* lives: ``data/.cas/<digest><suffix>``."""
    return data_dir(project) / ".cas" / f"{digest}{suffix}"


def ensure_cas_path(project: str, src: Path, digest: str, suffix: str | None = None) -> Path:
    """The clone of *src* named by *digest*, written if it is not there yet, and verified before it is published.

    The digest is computed before the copy, so a file edited mid-copy would otherwise leave a clone whose name lies
    about its content — and that clone is the only frozen record of what an entry was built from. The written bytes
    are re-digested and a mismatch raises ``CloneDigestMismatch`` rather than publishing the file (ADR-011 D9).

    *suffix* defaults to *src*'s own. A repair passes the one its entry recorded, since the same bytes can arrive
    from a file with another suffix (``.pq`` for ``.parquet``) and the entry names the clone it was minted with.
    """
    dst = cas_path(project, digest, src.suffix if suffix is None else suffix)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        # A unique temp name (not a fixed <digest>.tmp): two builders cloning one source at once must not share one.
        tmp = dst.with_name(f"{dst.name}.{uuid.uuid4().hex}.tmp")
        try:
            _clone(src, tmp)
            written = _digest_file(tmp)
            if written != digest:
                raise CloneDigestMismatch(
                    f"the copy of {src} hashes to {written}, not the {digest} it was named by — the file changed "
                    "while it was being copied. Nothing was written; try again once the file is settled."
                )
            os.replace(tmp, dst)
        finally:
            tmp.unlink(missing_ok=True)
    return dst


def recon_cas_path(project: str, live_src: Path, digest: str) -> Path:
    """The frozen ``.cas`` clone named by *digest*, without re-digesting *live_src*.

    Returns ``data/.cas/<digest><suffix>`` — the bytes as they were imported. The clone is normally
    already on disk: ``gc_cas`` keeps any clone a live entry's ``manifest.provenance`` names.

    If the clone is absent — manually deleted, or a fresh cross-machine catalog clone, since ``.cas``
    lives under ``data/`` outside the catalog git repo — re-materialise it from the live file IFF the
    live bytes still hash to *digest*. When the clone is gone AND the live file has drifted, the
    original bytes are unrecoverable and this raises ``LostSourceVersion`` (ADR-011 D9): a version
    tallyman promised and then lost is a failure, not a downgrade to whatever is on disk now.
    """
    cas_dir = data_dir(project) / ".cas"
    dst = cas_dir / f"{digest}{live_src.suffix}"
    if dst.exists():
        return dst
    if live_src.exists() and _digest_file(live_src) == digest:
        return ensure_cas_path(project, live_src, digest)
    raise LostSourceVersion(
        f"the bytes {digest}{live_src.suffix} are gone: their frozen clone under data/.cas is missing and "
        f"{live_src} no longer has that content. Whatever was built from them cannot be read faithfully; "
        "import the data again, or rebuild the entry from the current file."
    )


def gc_cas(project: str, live_digests: set[str], *, bullpen: Path | None = None) -> int:
    """Retire ``.cas`` clones whose digest no live entry references.

    ``live_digests`` is every surviving source version's ``manifest.provenance.digest`` — the md5 the
    clone is named by (``<digest><suffix>``). That set IS the retention closure now (ADR-011 D6): a
    clone is alive exactly while the entry whose bytes it holds is, which the DAG already records, so
    there is no separate ``manifest.sources`` map to walk. Returns the
    number of files retired; a no-op when the ``.cas`` dir is absent. ``.cas``
    lives under ``data/``, outside the catalog git repo, so a reset's
    ``git reset`` never reclaims it — this is the explicit sweep, called from
    ``reset_to`` against the post-prune live entry set.

    A clone is data (ADR-007 D13), the only frozen copy of the bytes an entry was built from once the live source is
    edited, so with ``bullpen`` given it is MOVED there instead of deleted, and a reset forward copies it back.
    Without one it is unlinked, which nothing in tallyman does any more.
    """
    cas_dir = data_dir(project) / ".cas"
    if not cas_dir.is_dir():
        return 0
    retired = 0
    for f in cas_dir.iterdir():
        if f.is_file() and f.stem not in live_digests:
            try:
                if bullpen is None:
                    f.unlink()
                else:
                    bullpen.mkdir(parents=True, exist_ok=True)
                    dest = bullpen / f.name
                    if dest.exists():
                        f.unlink()  # content-addressed: an existing copy is the same bytes
                    else:
                        shutil.move(str(f), str(dest))
                retired += 1
            except OSError:
                pass  # best-effort sweep; never fail a reset over it
    return retired
