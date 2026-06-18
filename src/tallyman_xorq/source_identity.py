"""Tallyman-side source-content identity for catalog entries.

xorq's build hash is tokenized under SnapshotStrategy, whose
``snapshot_normalize_read`` keys a Read on its *path string only* — neither
file stat nor file content reaches ``content_hash`` (verified empirically by
``tests/test_cache_lab.py::test_append_invalidates``: changing a source file
in place keeps the hash and dedups to the stale entry). xorq is a frozen
upstream for us, so content sensitivity has to be restored on our side. Two
candidate strategies, switchable at runtime so the cache lab can benchmark
them head to head:

``TALLYMAN_SOURCE_IDENTITY`` env:

- ``off`` (default): current behavior, path-identity only.
- ``cas``: ``from_project`` reads through a content-addressed clone at
  ``data/.cas/<digest><suffix>``. The path xorq hashes then *is* the content
  identity, every xorq-level key (build hash, snapshot cache keys) becomes
  content-honest for free, and because the clone is a copy-on-write snapshot
  (APFS clonefile), an old entry's recompute still reads the bytes it was
  built from even after the user edits the source in place.
- ``salt``: ``from_project`` records each source's digest during user-code
  import, and ``build_and_persist`` mixes the digests into the entry's
  ``content_hash``. Builds keep their human-readable ``data/`` paths, but
  xorq-level keys stay path-only — see ``result_cache`` for the snapshot-key
  collision this forces us to route around.

Digests are md5 over file content, memoized per ``(mtime_ns, size, inode)``
in ``artifacts/source_digests.json`` so an unchanged file is hashed once,
not once per build. The memo reintroduces a stat-keyed shortcut: a swap
that preserves all three stat fields serves the stale digest (the same hole
git's index accepts). ``TALLYMAN_SOURCE_REHASH=1`` bypasses the memo.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from tallyman_core import artifacts_dir, data_dir

# Shares tallyman.perf with result_cache so the one branch where reconstruction
# can't be content-faithful surfaces on the same namespace as the #83 self-heal
# faithfulness warning.
_log = logging.getLogger("tallyman.perf")

_MODE_ENV = "TALLYMAN_SOURCE_IDENTITY"
_REHASH_ENV = "TALLYMAN_SOURCE_REHASH"
_MODES = ("off", "cas", "salt")


def mode() -> str:
    m = os.environ.get(_MODE_ENV, "cas")
    if m not in _MODES:
        raise ValueError(f"{_MODE_ENV}={m!r}; expected one of {_MODES}")
    return m


# ---------------------------------------------------------------------------
# Digests (memoized on stat)
# ---------------------------------------------------------------------------


def _digest_file(path: Path) -> str:
    with path.open("rb") as fh:
        return hashlib.file_digest(fh, hashlib.md5).hexdigest()


def _index_path(project: str) -> Path:
    return artifacts_dir(project) / "source_digests.json"


def digest_for(project: str, path: Path) -> str:
    """Content digest of *path*, memoized per (mtime_ns, size, inode)."""
    st = path.stat()
    key = str(path)
    stat_sig = [st.st_mtime_ns, st.st_size, st.st_ino]
    idx_path = _index_path(project)
    index: dict = {}
    if not os.environ.get(_REHASH_ENV) and idx_path.exists():
        try:
            index = json.loads(idx_path.read_text())
        except (OSError, json.JSONDecodeError):
            index = {}
        hit = index.get(key)
        if hit and hit.get("stat") == stat_sig:
            return hit["digest"]
    digest = _digest_file(path)
    index[key] = {"stat": stat_sig, "digest": digest}
    try:
        idx_path.parent.mkdir(parents=True, exist_ok=True)
        idx_path.write_text(json.dumps(index, indent=2))
    except OSError:
        pass  # the memo is an optimization; never fail a build over it
    return digest


# ---------------------------------------------------------------------------
# cas: content-addressed source clones
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


def ensure_cas_path(project: str, src: Path, digest: str) -> Path:
    cas_dir = data_dir(project) / ".cas"
    cas_dir.mkdir(parents=True, exist_ok=True)
    dst = cas_dir / f"{digest}{src.suffix}"
    if not dst.exists():
        tmp = dst.with_suffix(dst.suffix + ".tmp")
        _clone(src, tmp)
        os.replace(tmp, dst)
    return dst


def recon_cas_path(project: str, live_src: Path, digest: str) -> Path:
    """The frozen ``.cas`` clone named by *digest*, for digest-pinned reconstruction (#115).

    Returns ``data/.cas/<digest><suffix>`` — the bytes the entry was built from —
    WITHOUT re-digesting the live source, so a cold read after an in-place edit serves
    what was built, not the edited file. The clone is normally already on disk: the
    ``.cas`` GC (``gc_cas``) preserves any clone a live entry's ``manifest.sources``
    references.

    If the clone is absent — manually deleted, or a fresh cross-machine catalog clone,
    since ``.cas`` lives under ``data/`` outside the catalog git repo — re-materialise
    it from the live source IFF the live bytes still hash to *digest*. The check hashes
    file *content* (``_digest_file``), never ``digest_for``, so the stat-keyed memo
    can't certify an in-place edit that preserved ``(mtime_ns, size, inode)`` as the
    original (the documented memo hole). When the clone is gone AND the live source has
    drifted, the original bytes are unrecoverable: serve the live source best-effort
    (never crash a read) but warn — this is the one branch that is NOT content-faithful.
    """
    cas_dir = data_dir(project) / ".cas"
    dst = cas_dir / f"{digest}{live_src.suffix}"
    if dst.exists():
        return dst
    if live_src.exists() and _digest_file(live_src) == digest:
        return ensure_cas_path(project, live_src, digest)
    _log.warning(
        "recon_cas_path: frozen .cas clone %s%s missing and live source %s drifted — "
        "serving live bytes; this cold read is NOT faithful to the build (#115)",
        digest,
        live_src.suffix,
        live_src,
    )
    return live_src


def gc_cas(project: str, live_digests: set[str]) -> int:
    """Delete ``.cas`` clones whose digest no live entry references.

    ``live_digests`` is the union of every live entry's ``manifest.sources``
    values — the md5 the clone is named by (``<digest><suffix>``). Returns the
    number of files removed; a no-op when the ``.cas`` dir is absent. ``.cas``
    lives under ``data/``, outside the catalog git repo, so a reset's
    ``git reset`` never reclaims it — this is the explicit reclaim, called from
    ``reset_to`` against the post-prune live entry set.
    """
    cas_dir = data_dir(project) / ".cas"
    if not cas_dir.is_dir():
        return 0
    removed = 0
    for f in cas_dir.iterdir():
        if f.is_file() and f.stem not in live_digests:
            try:
                f.unlink()
                removed += 1
            except OSError:
                pass  # best-effort reclaim; never fail a reset over GC
    return removed


# ---------------------------------------------------------------------------
# salt: collect source digests during user-code import
# ---------------------------------------------------------------------------

_collector: contextvars.ContextVar[dict[str, str] | None] = contextvars.ContextVar(
    "tallyman_source_collector", default=None
)


def begin_collect() -> contextvars.Token:
    return _collector.set({})


def note_source(rel_path: str, digest: str) -> None:
    bag = _collector.get()
    if bag is not None:
        bag[rel_path] = digest


def end_collect(token: contextvars.Token) -> dict[str, str]:
    bag = _collector.get() or {}
    _collector.reset(token)
    return dict(bag)


def salted_hash(xorq_hash: str, sources: dict[str, str]) -> str:
    """Mix source digests into the entry identity; same 12-hex shape as
    xorq's build hash so nothing downstream (dir names, URLs, aliases)
    notices the difference."""
    parts = "|".join(f"{rel}={digest}" for rel, digest in sorted(sources.items()))
    return hashlib.md5(f"{xorq_hash}|{parts}".encode()).hexdigest()[: len(xorq_hash)]
