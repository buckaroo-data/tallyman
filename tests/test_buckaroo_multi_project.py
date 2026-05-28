"""T-46 multi-project Buckaroo: one manager serves sessions from any project.

Content hashes are globally unique by definition (content-addressed), so one
Buckaroo subprocess can serve sessions backed by xorq builds from any project.
The manager no longer captures a single project at construction time; the
project travels with each ``ensure_session(hash, project)`` call. Session
state lives in a single global file at ``~/.pydata-app/buckaroo_sessions.json``
keyed by hash, with the project recorded so a restart can find the parquet
again.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydata_companion.buckaroo_lifecycle import BuckarooManager
from pydata_core import ensure_project
from pydata_core.paths import buckaroo_sessions_path


# ---------------------------------------------------------------------------
# Constructor shape
# ---------------------------------------------------------------------------


def test_manager_constructor_no_longer_takes_project(isolated_home: Path):
    """The constructor takes no positional project argument — project lifts
    to per-``ensure_session``."""
    ensure_project("alpha")
    mgr = BuckarooManager()  # type: ignore[call-arg]
    assert mgr is not None


# ---------------------------------------------------------------------------
# Multi-project session bookkeeping
# ---------------------------------------------------------------------------


def test_ensure_session_takes_project_explicitly(isolated_home: Path, monkeypatch):
    """``ensure_session(hash, project)`` accepts the project per call;
    sessions across projects coexist in one manager."""
    ensure_project("alpha")
    ensure_project("beta")
    # Set up a fake "running" manager that returns a session id for any /load_expr.
    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    class FakeResp:
        def raise_for_status(self): ...
        def json(self):
            return {"session": "sess-A"}

    monkeypatch.setattr(mgr._client, "post", lambda *a, **kw: FakeResp())

    # An xorq_build dir must exist for ensure_session not to short-circuit.
    from pydata_core.paths import entry_dir

    for proj, h in (("alpha", "hash_a"), ("beta", "hash_b")):
        d = entry_dir(proj, h) / "xorq_build"
        d.mkdir(parents=True, exist_ok=True)
        # Minimal expr.yaml so expand_to_tmp has something to read.
        (d / "expr.yaml").write_text("dummy: yes\n")

    # First call from project alpha.
    sid_a = mgr.ensure_session("hash_a", "alpha")
    assert sid_a == "sess-A"

    # Second call from project beta — independent session lookup.
    sid_b = mgr.ensure_session("hash_b", "beta")
    assert sid_b == "sess-A"  # mocked POST always returns the same id

    # Both entries are in the in-memory map.
    assert "hash_a" in mgr._sessions
    assert "hash_b" in mgr._sessions


def test_session_file_lives_at_global_location(isolated_home: Path):
    """The on-disk session map is one file under ``~/.pydata-app/``, not under
    any specific project's catalog. Schema includes the project per hash so
    a restart-reload knows which parquet to find."""
    ensure_project("alpha")
    mgr = BuckarooManager()
    mgr._sessions["abc"] = {"session_id": "sess-1", "project": "alpha"}
    mgr._buckaroo_started_at = 123.456
    mgr._persist_sessions()

    expected = buckaroo_sessions_path()
    assert expected.exists()
    assert expected == isolated_home / "buckaroo_sessions.json"
    data = json.loads(expected.read_text())
    assert data["sessions"]["abc"]["session_id"] == "sess-1"
    assert data["sessions"]["abc"]["project"] == "alpha"

    # Fresh instance picks up the same map.
    mgr2 = BuckarooManager()
    assert mgr2._sessions["abc"]["session_id"] == "sess-1"
    assert mgr2._sessions["abc"]["project"] == "alpha"


def test_startup_prunes_entries_for_missing_parquets(isolated_home: Path):
    """If a session entry points at a project/hash whose parquet no longer
    exists on disk, the manager drops it on load. Defensive against catalog
    cleanup or project deletion happening behind our back."""
    ensure_project("alpha")
    # Write a session file with one valid entry (parquet exists) and one stale
    # entry (no such project at all).
    from pydata_core.paths import entry_dir

    valid_dir = entry_dir("alpha", "valid_hash")
    valid_dir.mkdir(parents=True, exist_ok=True)
    (valid_dir / "result.parquet").write_text("not really parquet, just exists")

    sessions_path = buckaroo_sessions_path()
    sessions_path.parent.mkdir(parents=True, exist_ok=True)
    sessions_path.write_text(
        json.dumps(
            {
                "sessions": {
                    "valid_hash": {"session_id": "sess-valid", "project": "alpha"},
                    "stale_hash": {"session_id": "sess-stale", "project": "ghost"},
                },
                "buckaroo_started_at": 1.0,
            }
        )
    )

    mgr = BuckarooManager()
    assert "valid_hash" in mgr._sessions
    assert "stale_hash" not in mgr._sessions


def test_persist_only_writes_global_file_not_per_project(isolated_home: Path):
    """Negative assertion: nothing gets written under
    ``<project>/artifacts/catalog/buckaroo_sessions.json`` anymore."""
    from pydata_core.paths import catalog_dir

    ensure_project("alpha")
    mgr = BuckarooManager()
    mgr._sessions["abc"] = {"session_id": "sess-1", "project": "alpha"}
    mgr._persist_sessions()

    legacy = catalog_dir("alpha") / "buckaroo_sessions.json"
    assert not legacy.exists()
