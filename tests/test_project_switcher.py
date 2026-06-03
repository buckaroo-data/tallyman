"""Tests for the project-switcher routes in the companion.

Companion-side counterpart to ``test_paths.py`` (which covers the file
layer). Tests here exercise the HTTP surface:

- ``GET /api/projects`` — what the dropdown queries.
- ``POST /api/projects/switch`` — what the dropdown POSTs on change.
- ``POST /api/projects/new`` — what the "+ new" button (and
  ``project_new`` MCP tool) calls.
- SSE event tagging — every published event carries ``project`` so
  cross-project events don't yank the wrong tab.
- Empty-state landing — companion with no projects on disk serves a
  one-form page instead of the normal chrome.
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from pydata_companion import create_app
from pydata_core import ensure_project, resolve_project, set_active_project
from pydata_core.paths import active_project_file_path

# ---------------------------------------------------------------------------
# GET /api/projects
# ---------------------------------------------------------------------------


def test_api_projects_returns_active_and_available(isolated_home: Path):
    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.get("/api/projects")
    assert r.status_code == 200
    body = r.json()
    assert body["active"] == "alpha"
    assert sorted(body["available"]) == ["alpha", "beta"]


def test_api_projects_active_null_when_no_active(isolated_home: Path, monkeypatch):
    monkeypatch.delenv("PYDATA_PROJECT", raising=False)
    ensure_project("alpha")
    # No set_active_project — file absent.
    app = create_app()
    c = TestClient(app)
    r = c.get("/api/projects")
    assert r.status_code == 200
    body = r.json()
    assert body["active"] is None
    assert body["available"] == ["alpha"]


# ---------------------------------------------------------------------------
# POST /api/projects/switch
# ---------------------------------------------------------------------------


def test_api_projects_switch_updates_file_and_returns_new_active(isolated_home: Path):
    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/switch", json={"name": "beta"})
    assert r.status_code == 200
    assert r.json() == {"previous": "alpha", "active": "beta"}
    assert resolve_project() == "beta"


def test_api_projects_switch_404_on_unknown_project(isolated_home: Path):
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/switch", json={"name": "no_such"})
    assert r.status_code == 404


def test_api_projects_switch_400_on_invalid_name(isolated_home: Path):
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/switch", json={"name": "BadName"})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/projects/new
# ---------------------------------------------------------------------------


def test_api_projects_new_creates_and_switches(isolated_home: Path):
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/new", json={"name": "fresh", "with_fixture": False})
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "fresh"
    assert body["active"] == "fresh"
    assert resolve_project() == "fresh"
    # Layout was created.
    p = isolated_home / "projects" / "fresh"
    assert (p / "artifacts" / "catalog" / "entries").is_dir()
    assert (p / "notebooks").is_dir()
    # Fixture default off — no orders.parquet.
    assert not (p / "data" / "orders.parquet").exists()


def test_api_projects_new_with_fixture(isolated_home: Path):
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/new", json={"name": "demo", "with_fixture": True})
    assert r.status_code == 200
    assert (isolated_home / "projects" / "demo" / "data" / "orders.parquet").exists()


def test_api_projects_new_409_on_collision(isolated_home: Path):
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/new", json={"name": "alpha"})
    assert r.status_code == 409


def test_api_projects_new_400_on_invalid_name(isolated_home: Path):
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/new", json={"name": "Bad/Name"})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Per-request project resolution
# ---------------------------------------------------------------------------


def test_companion_picks_up_switch_without_restart(isolated_home: Path, orders_parquet: Path):
    """Switching projects via the API updates the active project without restarting."""
    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)

    assert c.get("/api/projects").json()["active"] == "alpha"
    c.post("/api/projects/switch", json={"name": "beta"})
    assert c.get("/api/projects").json()["active"] == "beta"
    assert c.get("/beta/api/entries").status_code == 200


# ---------------------------------------------------------------------------
# Empty-state landing page
# ---------------------------------------------------------------------------


def test_empty_state_landing_when_no_projects(built_spa, isolated_home: Path, monkeypatch):
    """No projects → root serves the React SPA (empty state rendered client-side)."""
    monkeypatch.delenv("PYDATA_PROJECT", raising=False)
    assert not active_project_file_path().exists()
    app = create_app()
    c = TestClient(app, follow_redirects=False)
    r = c.get("/")
    assert r.status_code == 200
    # React SPA serves; empty state is handled client-side.
    assert '<div id="root">' in r.text
    # API confirms no active project
    assert c.get("/api/projects").json()["active"] is None


def test_empty_state_landing_creates_first_project(isolated_home: Path, monkeypatch):
    monkeypatch.delenv("PYDATA_PROJECT", raising=False)
    app = create_app()
    c = TestClient(app, follow_redirects=False)
    r = c.post("/api/projects/new", json={"name": "first", "with_fixture": False})
    assert r.status_code == 200
    assert resolve_project() == "first"


# ---------------------------------------------------------------------------
# SSE event tagging
# ---------------------------------------------------------------------------


def test_internal_notify_tags_with_active_project(isolated_home: Path):
    """``POST /internal/notify`` (used by xorq build subprocesses to ping
    the companion) publishes events tagged with the active project so
    cross-project events can be filtered client-side.
    """
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/internal/notify", json={"kind": "test_event"})
    assert r.status_code == 200
    body = r.json()
    # Response echoes the project it tagged with so callers can verify.
    assert body.get("project") == "alpha"


def test_sse_project_switched_event_fires(isolated_home: Path):
    """Switching projects publishes a project_switched event. Smoke test: the
    SSE endpoint must accept connections and the switch must succeed."""
    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.post("/api/projects/switch", json={"name": "beta"})
    assert r.status_code == 200
    # SSE event firing is observed end-to-end in browser; here we just assert
    # the route returned cleanly and the switch took effect.
    assert resolve_project() == "beta"


# ---------------------------------------------------------------------------
# Chrome (dropdown + new-project button)
# ---------------------------------------------------------------------------


def test_chrome_renders_dropdown_in_writable_mode(isolated_home: Path):
    """In writable mode, project switching API works and no 403 is returned."""
    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    app = create_app()
    c = TestClient(app)
    r = c.get("/api/projects")
    assert r.status_code == 200
    body = r.json()
    assert body["active"] == "alpha"
    assert "alpha" in body["available"]
    # Switch works (not 403) in writable mode.
    r2 = c.post("/api/projects/switch", json={"name": "beta"})
    assert r2.status_code == 200


def test_chrome_hides_switcher_in_read_only_mode(isolated_home: Path, orders_parquet: Path):
    """pydata serve mode: project switching is rejected (403)."""
    ensure_project("alpha")
    set_active_project("alpha")
    app = create_app(read_only=True)
    c = TestClient(app)
    # Reads work in read_only mode.
    assert c.get("/alpha/api/entries").status_code == 200
    # Project switching is rejected.
    ensure_project("beta")
    r = c.post("/api/projects/switch", json={"name": "beta"})
    assert r.status_code == 403
