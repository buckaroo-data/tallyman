from __future__ import annotations

import shutil
from pathlib import Path

from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_core import entry_dir, project_dir, set_alias
from tallyman_xorq import build_and_persist


def _code(project_name: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project_name!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_read_only_mode_rejects_notify(fresh_companion_app, project: str):
    # The plain fixture is edit-mode; build a new app explicitly in read-only.
    app = create_app(project, read_only=True)
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "new_entry", "hash": "abc"})
    assert r.status_code == 403


def test_read_only_mode_serves_get_routes(built_spa, project: str, orders_parquet: Path):
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


def test_edit_mode_indicator_absent(built_spa, fresh_companion_app, project: str):
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


def test_notify_recalc_reloads_sessions_and_invalidates_caches(project: str, monkeypatch):
    """An MCP-driven recalc reaches the companion as a ``kind="recalc"`` notify.
    The handler must reload buckaroo sessions AND invalidate the result/compare
    LRUs — the same cleanup the in-process /api/recalc route does — so the
    primary (MCP) recalc path doesn't leave the viewer serving stale state.
    """
    import tallyman_companion.app as appmod

    invalidated: list[bool] = []
    monkeypatch.setattr(appmod, "_invalidate_reset_caches", lambda: invalidated.append(True))
    stub = _TrackingBuckaroo()
    app = create_app(project, buckaroo=stub)
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "recalc", "extra": {"remap": {"old": "new"}, "step": 3}})
    assert r.status_code == 200
    assert stub.reload_calls == [project]  # buckaroo reloaded, like project_reset
    assert invalidated == [True]  # LRUs cleared, like project_reset


def test_recalc_sse_event_has_canonical_shape():
    """Both recalc emitters — the in-process /api/recalc route and the
    cross-process /internal/notify path — publish through this one helper, so
    the SSE event shape cannot drift between the two surfaces."""
    from tallyman_companion.app import _recalc_sse_event

    assert _recalc_sse_event({"a": "b"}, 7) == {"kind": "recalc", "remap": {"a": "b"}, "step": 7}
    assert _recalc_sse_event({}, None) == {"kind": "recalc", "remap": {}, "step": None}


def test_notify_recalc_publishes_normalized_event(project: str, monkeypatch):
    """The notify handler must republish a recalc in the canonical {kind, remap,
    step} shape (via ``_recalc_sse_event``), not the raw ``payload.model_dump()``
    that wraps remap under ``extra`` and drops ``step`` entirely."""
    import tallyman_companion.app as appmod

    captured: list[tuple] = []
    monkeypatch.setattr(
        appmod,
        "_recalc_sse_event",
        lambda remap, step: captured.append((remap, step)) or {"kind": "recalc", "remap": remap, "step": step},
        raising=False,
    )
    app = create_app(project)
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "recalc", "extra": {"remap": {"o": "n"}, "step": 5}})
    assert r.status_code == 200
    assert captured == [({"o": "n"}, 5)]  # remap + step pulled from extra, routed through the normalizer


def test_project_path_override_relocates_project(
    project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path, monkeypatch
):
    """The full hand-off scenario: copy the project to a random location,
    point TALLYMAN_PROJECT_PATH at it, and verify the catalog still loads
    and entries still execute through the portability layer.
    """
    res = build_and_persist(project, _code(project), prompt="region totals")
    set_alias(project, "shoe_sales", res.content_hash)

    # Simulate untarring the project dir somewhere arbitrary (NOT under TALLYMAN_HOME).
    handoff = tmp_path / "handoff" / project
    handoff.parent.mkdir()
    shutil.copytree(project_dir(project), handoff)

    # Now reconfigure env to point at the handoff dir, and use a different TALLYMAN_HOME
    # (so we know we're NOT secretly reading from the original location).
    monkeypatch.setenv("TALLYMAN_HOME", str(tmp_path / "fresh-home"))
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_PROJECT_PATH", str(handoff))

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
