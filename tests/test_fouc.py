"""Visible-load behavior: SPA-lite swap regions and embed plumbing.

These tests don't run a browser, so we can't observe the actual flash
duration. They DO assert that the HTML/CSS/JS contract the SPA-lite
controller depends on is in place: swap regions present, embed bundle
referenced, navigation handlers attached.
"""
from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from pydata_mcp.server import catalog_create


def _agg(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_catalog_has_swap_regions(fresh_companion_app):
    """Both #catalog-sidebar and #catalog-detail must be present so the
    SPA-lite swap can find them and avoid a full reload."""
    c = TestClient(fresh_companion_app)
    r = c.get("/catalog")
    assert r.status_code == 200
    assert 'id="catalog-sidebar"' in r.text
    assert 'id="catalog-detail"' in r.text


def test_entry_detail_has_swap_regions(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_create("shoe_sales", _agg(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/catalog/{out['hash']}")
    assert r.status_code == 200
    assert 'id="catalog-sidebar"' in r.text
    assert 'id="catalog-detail"' in r.text


def test_error_detail_has_swap_regions(fresh_companion_app, project: str):
    from pydata_core import record_error
    rec = record_error(project, code="x", message="m", tool="catalog_run")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/errors/{rec['id']}")
    assert 'id="catalog-sidebar"' in r.text
    assert 'id="catalog-detail"' in r.text


def test_embed_bundle_referenced(fresh_companion_app):
    """The compiled buckaroo-embed bundle + its dark-background placeholder
    CSS live in base.html so embed divs render on top of the right colour
    before the WebSocket payload lands."""
    c = TestClient(fresh_companion_app)
    r = c.get("/catalog")
    assert '/static/buckaroo-embed.js' in r.text
    assert '/static/buckaroo-embed.css' in r.text
    # CSS for .buckaroo-embed sets the dark background before the WS payload
    # arrives. Exact rule shape grew (sizing/border/overflow added alongside
    # the size-toggle work) — assert on the substring that names the colour.
    assert ".buckaroo-embed" in r.text and "#181d1f" in r.text


def test_spa_lite_js_wired_up(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.get("/catalog")
    # SPA-lite controller signatures.
    assert "spaNavigate" in r.text
    assert "history.pushState" in r.text
    # Routes the click handler activates on — the JS regex escapes / as \/.
    assert r"^\/catalog" in r.text


def test_lineage_pages_fall_through_to_real_nav(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    """`/lineage` doesn't have swap regions today, so SPA-lite is
    expected to fall through to a regular navigation. Sanity-check
    that the lineage pages still render correctly (no swap regions
    means no SPA-lite — that's correct)."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_create("shoe_sales", _agg(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/lineage/{out['hash']}")
    assert r.status_code == 200
    assert 'id="catalog-sidebar"' not in r.text
