"""React SPA serving and API/SPA boundary tests.

The Jinja2 + SPA-lite approach is replaced by a React SPA. These tests verify
the new contract: UI routes serve the React index.html, API routes return JSON,
and project validation is enforced at the API layer.
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tallyman_mcp.server import catalog_create


def _agg(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_spa_catch_all_serves_index_html(built_spa, fresh_companion_app, project: str):
    """UI routes return the React SPA index.html with a #root div."""
    c = TestClient(fresh_companion_app)
    for path in [
        f"/{project}/catalog",
        f"/{project}/notebook",
    ]:
        r = c.get(path)
        assert r.status_code == 200, path
        assert '<div id="root">' in r.text, path
        assert "<!doctype html>" in r.text.lower(), path


def test_api_routes_return_json_not_spa(fresh_companion_app, project: str):
    """API routes must return JSON, not the SPA HTML."""
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    body = r.json()
    assert "entries" in body


def test_unknown_project_spa_still_200(built_spa, fresh_companion_app, project: str):
    """SPA catch-all serves for unknown project names; API returns 404."""
    c = TestClient(fresh_companion_app)
    # UI route: React app handles the unknown project gracefully
    assert c.get("/nope/catalog").status_code == 200
    # API route: validation rejects it
    assert c.get("/nope/api/entries").status_code == 404


def test_unknown_api_path_404s(fresh_companion_app, project: str):
    """Unmatched API paths must 404, not fall through to the SPA index.html.

    Otherwise a mistyped or removed endpoint returns 200 HTML and the client's
    ``response.json()`` throws an opaque parse error instead of surfacing the
    404. Guard keys on path *position* (``api`` as the first or second segment),
    not a substring, so content segments that merely contain "api" still route
    to the SPA.
    """
    c = TestClient(fresh_companion_app)
    # Project-prefixed API typo under a valid project → JSON 404, not HTML.
    r = c.get(f"/{project}/api/does_not_exist")
    assert r.status_code == 404, r.text
    assert '<div id="root">' not in r.text
    # Top-level API typo → 404.
    assert c.get("/api/does_not_exist").status_code == 404
    # A page route whose alias segment is literally "api" must still serve the
    # SPA — proves the guard matches on segment position, not substring.
    r2 = c.get(f"/{project}/diff/api/1/2")
    assert r2.status_code == 200, r2.text
    assert '<div id="root">' in r2.text


def test_entry_detail_api_returns_entry_data(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    """Entry detail data is served via JSON API, not server-rendered HTML."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_create("shoe_sales", _agg(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{out['hash']}")
    assert r.status_code == 200
    body = r.json()
    assert body["content_hash"] == out["hash"]
    assert body["alias"] == "shoe_sales"
    assert "schema" in body
    assert "code" in body
