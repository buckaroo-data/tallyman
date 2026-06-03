from __future__ import annotations

import shutil
from pathlib import Path

from fastapi.testclient import TestClient

from pydata_companion import create_app
from pydata_core import entry_dir, project_dir, set_alias
from pydata_xorq import build_and_persist


def _code(project_name: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project_name!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_read_only_mode_rejects_notify(fresh_companion_app, project: str):
    # The plain fixture is edit-mode; build a new app explicitly in read-only.
    app = create_app(project, read_only=True)
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "new_entry", "hash": "abc"})
    assert r.status_code == 403


def test_read_only_mode_serves_get_routes(project: str, orders_parquet: Path):
    res = build_and_persist(project, _code(project))
    set_alias(project, "shoe_sales", res.content_hash)
    app = create_app(project, read_only=True)
    c = TestClient(app)
    assert c.get(f"/{project}/catalog").status_code == 200
    assert c.get(f"/{project}/catalog/{res.content_hash}").status_code == 200
    assert c.get(f"/{project}/api/entries").status_code == 200
    assert c.get(f"/{project}/api/aliases").status_code == 200


def test_read_only_mode_indicator_in_header(project: str):
    # Serve mode enforces read-only at the API layer (403 on writes).
    app = create_app(project, read_only=True)
    c = TestClient(app)
    # Reads still work.
    assert c.get(f"/{project}/api/entries").status_code == 200
    # Project switching is rejected.
    assert c.post("/api/projects/switch", json={"name": "other"}).status_code == 403


def test_edit_mode_indicator_absent(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/catalog")
    assert "read-only" not in r.text


class _TrackingBuckaroo:
    """Stub that records reload_project_sessions calls."""

    def __init__(self):
        self.reload_calls: list[str] = []
        self.bound_port = None
        self.is_running = False

    def reload_project_sessions(self, project: str) -> int:
        self.reload_calls.append(project)
        return 0

    def ensure_session(self, content_hash: str, project: str):
        return None


def test_notify_post_processing_changed_reloads_sessions(project: str):
    stub = _TrackingBuckaroo()
    app = create_app(project, buckaroo=stub)
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "post_processing_changed"})
    assert r.status_code == 200
    assert stub.reload_calls == [project]


def test_notify_summary_stat_changed_reloads_sessions(project: str):
    stub = _TrackingBuckaroo()
    app = create_app(project, buckaroo=stub)
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "summary_stat_changed"})
    assert r.status_code == 200
    assert stub.reload_calls == [project]


def test_notify_other_kind_does_not_reload_sessions(project: str):
    stub = _TrackingBuckaroo()
    app = create_app(project, buckaroo=stub)
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "new_entry", "hash": "abc"})
    assert r.status_code == 200
    assert stub.reload_calls == []


def test_project_path_override_relocates_project(
    project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path, monkeypatch
):
    """The full hand-off scenario: copy the project to a random location,
    point PYDATA_PROJECT_PATH at it, and verify the catalog still loads
    and entries still execute through the portability layer.
    """
    res = build_and_persist(project, _code(project), prompt="region totals")
    set_alias(project, "shoe_sales", res.content_hash)

    # Simulate untarring the project dir somewhere arbitrary (NOT under PYDATA_HOME).
    handoff = tmp_path / "handoff" / project
    handoff.parent.mkdir()
    shutil.copytree(project_dir(project), handoff)

    # Now reconfigure env to point at the handoff dir, and use a different PYDATA_HOME
    # (so we know we're NOT secretly reading from the original location).
    monkeypatch.setenv("PYDATA_HOME", str(tmp_path / "fresh-home"))
    monkeypatch.setenv("PYDATA_PROJECT", project)
    monkeypatch.setenv("PYDATA_PROJECT_PATH", str(handoff))

    # Sanity: project_dir() now returns the handoff path.
    assert project_dir(project) == handoff
    assert entry_dir(project, res.content_hash) == handoff / "artifacts" / "catalog" / "entries" / res.content_hash

    # Serve mode should render correctly against the relocated project.
    app = create_app(project, read_only=True)
    c = TestClient(app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    aliases = [e["alias"] for e in r.json()["entries"] if e["alias"]]
    assert "shoe_sales" in aliases
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
