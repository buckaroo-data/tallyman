from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from pydata_xorq import build_and_persist


def _build_one(project: str, parquet: Path) -> str:
    code = f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    return build_and_persist(project, code, prompt="by region").content_hash


def test_root_redirects(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"].endswith(f"/{project}/catalog")


def test_catalog_empty(fresh_companion_app, project: str):
    # UI routes now serve the React SPA; data checks go through the API.
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    assert r.json()["entries"] == []


def test_unknown_project_404s(fresh_companion_app, project: str):
    """Project validation happens at the API layer; UI routes serve the SPA."""
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/entries").status_code == 200  # valid project
    assert c.get("/nope/api/entries").status_code == 404         # unknown project
    assert c.get("/Bad-NAME/api/entries").status_code == 404     # invalid name


def test_api_entries_empty(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    assert r.json() == {"project": project, "entries": []}


def test_catalog_renders_after_build(fresh_companion_app, project: str, orders_parquet: Path):
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    entries = r.json()["entries"]
    hashes = [e["content_hash"] for e in entries]
    assert h in hashes
    prompts = [e["prompt"] for e in entries]
    assert "by region" in prompts


def test_entry_detail_renders_table(fresh_companion_app, project: str, orders_parquet: Path):
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{h}")
    assert r.status_code == 200
    body = r.json()
    assert body["content_hash"] == h
    col_names = [f["name"] for f in body["schema"]["fields"]]
    assert "region" in col_names

    # Data endpoint still serves rows.
    rd = c.get(f"/{project}/api/data/{h}")
    assert rd.status_code == 200
    assert rd.json()["total"] > 0


def test_entry_detail_sidebar_lists_all_entries_with_current_highlighted(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    """Entries API returns named/forensic/scratch entries with correct flags."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create, catalog_run

    catalog_create(
        "shoe_sales",
        f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
""",
    )
    scratch = catalog_run(
        f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.order_by("region")
""",
        prompt="exploratory",
    )

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    entries = r.json()["entries"]
    named = [e for e in entries if e["alias"] and e["is_current"]]
    scratches = [e for e in entries if not e["alias"]]
    assert any(e["alias"] == "shoe_sales" for e in named)
    assert any(e["content_hash"] == scratch["hash"] for e in scratches)


def test_entry_detail_shows_build_artifacts(fresh_companion_app, project: str, orders_parquet: Path):
    """Build artifacts (expr.yaml etc) appear in the entry JSON."""
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{h}")
    assert r.status_code == 200
    body = r.json()
    artifact_names = [a["name"] for a in body["build_artifacts"]]
    assert any("yaml" in n for n in artifact_names)
    # Portable build references ${PYDATA_PROJECT_ROOT}, not absolute paths.
    artifact_texts = " ".join(a["text"] for a in body["build_artifacts"])
    assert "${PYDATA_PROJECT_ROOT}" in artifact_texts


def test_entry_detail_missing_returns_404(fresh_companion_app, project: str):
    """Unknown hashes return 404 from the API (React app handles the UI gracefully)."""
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/entry/deadbeef").status_code == 404


def test_internal_notify_returns_subscriber_count(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "new_entry", "hash": "abc"})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["subscribers"] == 0  # no SSE clients connected in this test
