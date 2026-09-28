"""T-46 multi-project Buckaroo: one manager serves sessions from any project.

Content hashes are globally unique by definition (content-addressed), so one
Buckaroo subprocess can serve sessions backed by xorq builds from any project.
The manager no longer captures a single project at construction time; the
project travels with each ``ensure_session(hash, project)`` call.

Tallyman keeps no record of Buckaroo's sessions (plans/ADR-007-tallyman-owned-materialization.md D6). A session's id
is derived from the project and the hash, so the two projects' sessions never share one and there is nothing to
persist: the global ``~/.tallyman/buckaroo_sessions.json`` this file used to pin is gone.
"""

from __future__ import annotations

from pathlib import Path

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_core import data_dir, ensure_project, set_active_project
from tallyman_core.paths import artifacts_dir
from tallyman_xorq import build_and_persist
from tallyman_xorq.source_import import update_and_depend

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
# Multi-project sessions
# ---------------------------------------------------------------------------


SRC_ALIAS = "orders_src"


def _seed_orders(project: str) -> None:
    """Write the fixture into *project* and import it as a source alias.

    These tests stand up their own projects rather than using the shared ``project`` fixture, so they
    do their own import. A recipe reads the alias, never the file (ADR-011 D1).

    The project is made active first because the recipe ``source_import._recipe`` generates calls
    ``read_project_file(path)`` with no project, so the read resolves against whatever project is
    active rather than the one passed to ``update_and_depend``.
    """
    set_active_project(project)
    path = write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=50, seed=1)
    update_and_depend(path, SRC_ALIAS, project=project)


def _entry_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({SRC_ALIAS!r}, project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_ensure_session_takes_project_explicitly(isolated_home: Path, monkeypatch):
    """``ensure_session(hash, project)`` accepts the project per call; sessions of entries in different projects
    coexist in one manager, each with its own derived id, and each is loaded against its own project's files."""
    hashes = {}
    for proj in ("alpha", "beta"):
        ensure_project(proj)
        _seed_orders(proj)
        hashes[proj] = build_and_persist(proj, _entry_code(proj)).content_hash

    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    posted: list[dict] = []

    class FakeResp:
        def __init__(self, session: str):
            self._session = session

        def raise_for_status(self): ...
        def json(self):
            return {"session": self._session}

    def fake_post(url, json=None, timeout=None):
        posted.append(json)
        return FakeResp(json["session"])

    monkeypatch.setattr(mgr._client, "post", fake_post)

    sid_a = mgr.ensure_session(hashes["alpha"], "alpha")
    sid_b = mgr.ensure_session(hashes["beta"], "beta")

    # The id is a function of the project and the hash, so two projects never share a session.
    assert sid_a == mgr.session_id_for("alpha", hashes["alpha"])
    assert sid_b == mgr.session_id_for("beta", hashes["beta"])
    assert sid_a != sid_b
    # Each load names its own project's artifacts dir (where Buckaroo finds that project's klasses).
    assert [body["project_root"] for body in posted] == [str(artifacts_dir("alpha")), str(artifacts_dir("beta"))]


def test_no_session_file_is_written_anywhere(isolated_home: Path, monkeypatch):
    """Negative assertion: nothing gets written to ``~/.tallyman/buckaroo_sessions.json``, nor to a project's
    ``artifacts/catalog/buckaroo_sessions.json``, nor anywhere else, when sessions are opened (ADR-007 D6)."""
    ensure_project("alpha")
    _seed_orders("alpha")
    h = build_and_persist("alpha", _entry_code("alpha")).content_hash

    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    class FakeResp:
        def raise_for_status(self): ...
        def json(self):
            return {"session": "sess-1"}

    monkeypatch.setattr(mgr._client, "post", lambda *a, **kw: FakeResp())

    assert mgr.ensure_session(h, "alpha") == "sess-1"
    assert not list(isolated_home.rglob("buckaroo_sessions.json"))
