"""The clone store (``tallyman_xorq.source_identity``) — the frozen copy of every imported byte.

An import digests the file it is given, clones it to ``data/.cas/<digest><suffix>`` and verifies the
copy before publishing it. The clone is what makes a source version durable once the outside file is
edited or deleted, and what re-creates its snapshot. These are the fast-suite pins for the parts CI
must guard: that the clone is a frozen snapshot rather than a hardlink, that the ``.cas`` GC reclaims
only what no live entry references, and that there is exactly one identity mode (ADR-011 D8).

The cache lab (``tests/test_cache_lab.py``) is the heavyweight, on-demand home for identity scenarios,
but it is marked ``cache_lab`` and never runs in CI.
"""

from __future__ import annotations

import pandas as pd

from tallyman_core import data_dir
from tallyman_xorq import source_identity as si


def _write_parquet(path, n_rows: int) -> None:
    pd.DataFrame({"a": list(range(n_rows))}).to_parquet(path)


def test_gc_cas_removes_unreferenced_digests(project):
    """gc_cas deletes only ``.cas`` clones no live entry references."""
    cas_dir = data_dir(project) / ".cas"
    cas_dir.mkdir(parents=True, exist_ok=True)
    referenced = cas_dir / "aaaaaaaa.parquet"
    orphan = cas_dir / "bbbbbbbb.parquet"
    referenced.write_bytes(b"x")
    orphan.write_bytes(b"y")

    removed = si.gc_cas(project, {"aaaaaaaa"})

    assert removed == 1
    assert referenced.exists()
    assert not orphan.exists()


def test_cas_clone_is_a_snapshot_not_a_hardlink(project):
    """ensure_cas_path freezes the bytes: editing the source must not change the clone.

    A hardlink would share the inode and let an in-place edit rewrite the
    "snapshot"; ``_clone`` uses a CoW clone (``cp -c`` / ``cp --reflink=auto``)
    or a plain copy, never a hardlink. Also guards the Linux reflink arm, which
    runs on CI.
    """
    src = data_dir(project) / "frozen.parquet"
    _write_parquet(src, 3)
    digest = si._digest_file(src)
    clone = si.ensure_cas_path(project, src, digest)

    _write_parquet(src, 5)  # edit the source in place, after cloning

    assert pd.read_parquet(clone).shape[0] == 3
    assert clone.stat().st_ino != src.stat().st_ino  # distinct inode — not a hardlink


def test_source_identity_has_one_mode():
    """ADR-011 D8: import always digests and always clones.

    There is no configuration under which a raw input is unversioned, so ``TALLYMAN_SOURCE_IDENTITY``,
    the ``off`` and ``salt`` modes and the salted hash they needed are gone — checked against the source
    tree, because a mode nothing reads is still a mode someone can set and be misled by.
    """
    import re
    from pathlib import Path

    for name in ("mode", "salted_hash", "digest_for", "begin_collect", "note_source", "end_collect"):
        assert not hasattr(si, name), f"source_identity.{name} is deleted by ADR-011 D6/D8"

    src = Path(__file__).resolve().parent.parent / "src"
    pattern = re.compile(r"TALLYMAN_SOURCE_IDENTITY|TALLYMAN_SOURCE_REHASH|source_digests\.json")
    offenders = [
        f"{p.relative_to(src)}:{p.read_text().count(chr(10), 0, m.start()) + 1}"
        for p in sorted(src.rglob("*.py"))
        for m in pattern.finditer(p.read_text())
    ]
    assert not offenders, f"the source-identity modes and their digest memo are gone: {offenders}"
