"""Source-content identity (``tallyman_xorq.source_identity``) under the cas default.

The cache lab (``tests/test_cache_lab.py``) is the heavyweight, on-demand home for
identity scenarios, but it is marked ``cache_lab`` and never runs in CI. These are
the fast-suite pins for the parts of #86 that CI must guard: that ``cas`` is the
default, that a rebuild over an edited source forks the content hash (the
``test_append_invalidates`` contract, lifted into the fast suite), and that the
``.cas`` GC reclaims clones no live entry references.

Scope note (#86): flipping the default to cas makes *build* identity content-aware
(a rebuild forks the hash). It does NOT make cold *reconstruction* content-faithful
— a cheap entry's recipe is re-run on read and re-digests the live source — so there
is deliberately no "cold read after edit returns the original bytes" assertion here.
That guarantee needs digest-pinned reconstruction, tracked separately.
"""

from __future__ import annotations

import pandas as pd

from tallyman_core import data_dir
from tallyman_xorq import build_and_persist
from tallyman_xorq import source_identity as si


def _write_parquet(path, n_rows: int) -> None:
    pd.DataFrame({"a": list(range(n_rows))}).to_parquet(path)


def _read_code(project: str, rel: str) -> str:
    return f"from tallyman_xorq.io import read_project_file\nexpr = read_project_file({rel!r}, project={project!r})\n"


def test_cas_is_default(monkeypatch):
    """With TALLYMAN_SOURCE_IDENTITY unset, identity defaults to content-addressed."""
    monkeypatch.delenv("TALLYMAN_SOURCE_IDENTITY", raising=False)
    assert si.mode() == "cas"


def test_cas_rebuild_over_edited_source_forks_hash(project, monkeypatch):
    """Editing a source in place and rebuilding must fork the content hash.

    Under the old ``off`` default the build hash keys on the path string only
    (xorq's SnapshotStrategy), so an in-place edit dedups to the stale entry —
    this is the documented ``test_append_invalidates`` failure. Under ``cas`` the
    read goes through ``data/.cas/<digest>``, so the path embeds the content and
    the hash forks.
    """
    monkeypatch.delenv("TALLYMAN_SOURCE_IDENTITY", raising=False)
    monkeypatch.setenv("TALLYMAN_SOURCE_REHASH", "1")  # never serve a stale digest
    assert si.mode() == "cas"

    src = data_dir(project) / "src.parquet"
    _write_parquet(src, 3)
    code = _read_code(project, "src.parquet")
    h1 = build_and_persist(project, code).content_hash

    _write_parquet(src, 5)  # user edits the source in place
    h2 = build_and_persist(project, code).content_hash

    assert h1 != h2, "cas: an in-place source edit must fork the content hash on rebuild"


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
    digest = si.digest_for(project, src)
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
