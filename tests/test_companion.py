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


def test_root_redirects(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"].endswith("/catalog")


def test_catalog_empty(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.get("/catalog")
    assert r.status_code == 200
    assert "no entries yet" in r.text


def test_api_entries_empty(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.get("/api/entries")
    assert r.status_code == 200
    assert r.json() == {"project": project, "entries": []}


def test_catalog_renders_after_build(fresh_companion_app, project: str, orders_parquet: Path):
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get("/catalog")
    assert r.status_code == 200
    assert h in r.text
    assert "by region" in r.text


def test_entry_detail_renders_table(fresh_companion_app, project: str, orders_parquet: Path):
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/catalog/{h}")
    assert r.status_code == 200
    assert h in r.text
    assert "region" in r.text  # column header
    assert "data-table" in r.text


def test_entry_detail_shows_build_artifacts(
    fresh_companion_app, project: str, orders_parquet: Path
):
    """T-14: build artifacts (expr.yaml etc) are visible from the UI, with
    the portable ${PYDATA_PROJECT_ROOT} placeholder still in the text."""
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/catalog/{h}")
    assert "build artifacts" in r.text
    assert "expr.yaml" in r.text
    # The portable build references ${PYDATA_PROJECT_ROOT}, not absolute paths.
    assert "${PYDATA_PROJECT_ROOT}" in r.text


def test_entry_detail_404(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.get("/catalog/deadbeef")
    assert r.status_code == 404


def test_internal_notify_returns_subscriber_count(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "new_entry", "hash": "abc"})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["subscribers"] == 0  # no SSE clients connected in this test
