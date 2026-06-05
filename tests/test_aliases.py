from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pydata_core import (
    AliasExists,
    AliasNotFound,
    alias_for_hash,
    get_alias,
    history_for,
    load_aliases,
    load_history,
    rename_alias,
    set_alias,
    version_of_hash,
)
from pydata_mcp.server import (
    catalog_alias,
    catalog_create,
    catalog_rename,
    catalog_revise,
    catalog_run,
)
from pydata_xorq import build_and_persist


def _agg_code(project_name: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project_name!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def _filter_code(project_name: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project_name!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(n=filtered.count())
"""


# ---------------------------------------------------------------------------
# storage layer
# ---------------------------------------------------------------------------


def test_set_alias_first_time(project: str):
    info = set_alias(project, "x", "h1")
    assert info == {"name": "x", "hash": "h1", "version": 1}
    assert load_aliases(project) == {"x": "h1"}
    assert load_history(project) == {"x": ["h1"]}


def test_set_alias_versioning(project: str):
    set_alias(project, "x", "h1")
    set_alias(project, "x", "h2")
    set_alias(project, "x", "h3")
    assert get_alias(project, "x") == "h3"
    assert history_for(project, "x") == ["h1", "h2", "h3"]


def test_set_alias_consecutive_same_hash_dedupes(project: str):
    set_alias(project, "x", "h1")
    set_alias(project, "x", "h1")
    assert history_for(project, "x") == ["h1"]


def test_set_alias_expect_exists_constraints(project: str):
    set_alias(project, "x", "h1")
    with pytest.raises(AliasNotFound):
        set_alias(project, "missing", "h", expect_exists=True)
    with pytest.raises(AliasExists):
        set_alias(project, "x", "h2", expect_exists=False)


def test_alias_for_hash_returns_current_only(project: str):
    set_alias(project, "x", "h1")
    set_alias(project, "x", "h2")
    assert alias_for_hash(project, "h2") == "x"
    assert alias_for_hash(project, "h1") is None  # not current


def test_version_of_hash_returns_one_based(project: str):
    set_alias(project, "x", "h1")
    set_alias(project, "x", "h2")
    assert version_of_hash(project, "h1") == ("x", 1)
    assert version_of_hash(project, "h2") == ("x", 2)
    assert version_of_hash(project, "missing") is None


def test_rename_alias_preserves_history(project: str):
    set_alias(project, "old", "h1")
    set_alias(project, "old", "h2")
    rename_alias(project, "old", "new")
    assert get_alias(project, "old") is None
    assert get_alias(project, "new") == "h2"
    assert history_for(project, "new") == ["h1", "h2"]


def test_rename_alias_missing(project: str):
    with pytest.raises(AliasNotFound):
        rename_alias(project, "missing", "new")


def test_rename_alias_collision(project: str):
    set_alias(project, "a", "h1")
    set_alias(project, "b", "h2")
    with pytest.raises(AliasExists):
        rename_alias(project, "a", "b")


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


def test_catalog_create_assigns_alias(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_create("shoe_sales", _agg_code(project), prompt="sales by region")
    assert "error" not in out
    assert out["alias"] == "shoe_sales"
    assert out["version"] == 1
    assert get_alias(project, "shoe_sales") == out["hash"]


def test_catalog_create_rejects_duplicate(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_create("shoe_sales", _agg_code(project))
    assert "error" in out
    assert "already exists" in out["error"]


def test_catalog_revise_bumps_version(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    v1 = catalog_create("shoe_sales", _agg_code(project))
    v2 = catalog_revise("shoe_sales", _filter_code(project))
    assert v1["hash"] != v2["hash"]
    assert v2["version"] == 2
    assert history_for(project, "shoe_sales") == [v1["hash"], v2["hash"]]
    assert get_alias(project, "shoe_sales") == v2["hash"]


def test_catalog_revise_rejects_missing_alias(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_revise("nonexistent", _agg_code(project))
    assert "error" in out
    assert "does not exist" in out["error"]


def test_catalog_alias_promotes_scratch(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    scratch = catalog_run(_agg_code(project), prompt="exploratory")
    out = catalog_alias(scratch["hash"], "shoe_sales")
    assert "error" not in out
    assert out["alias"] == "shoe_sales"
    assert get_alias(project, "shoe_sales") == scratch["hash"]


def test_catalog_alias_rejects_missing_hash(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_alias("deadbeef", "shoe_sales")
    assert "error" in out


def test_catalog_alias_rejects_taken_name(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    scratch = catalog_run(_agg_code(project))
    catalog_alias(scratch["hash"], "shoe_sales")
    other = catalog_run(_filter_code(project))
    out = catalog_alias(other["hash"], "shoe_sales")
    assert "error" in out


def test_catalog_rename(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("foo", _agg_code(project))
    out = catalog_rename("foo", "bar")
    assert out["old"] == "foo"
    assert out["new"] == "bar"
    assert get_alias(project, "foo") is None
    assert get_alias(project, "bar") is not None


# ---------------------------------------------------------------------------
# url field — every entry-creating tool must return a viewer URL
# ---------------------------------------------------------------------------


def test_catalog_run_returns_url(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_run(_agg_code(project), prompt="url check")
    assert "url" in out
    assert out["url"].endswith(f"/catalog/{out['hash']}")
    assert project in out["url"]


def test_catalog_create_returns_url(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_create("shoe_sales", _agg_code(project), prompt="url check")
    assert "url" in out
    assert out["url"].endswith(f"/catalog/{out['hash']}")
    assert project in out["url"]


def test_catalog_revise_returns_url(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_revise("shoe_sales", _filter_code(project), prompt="url check")
    assert "url" in out
    assert out["url"].endswith(f"/catalog/{out['hash']}")
    assert project in out["url"]


def test_catalog_alias_returns_url(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    scratch = catalog_run(_agg_code(project))
    out = catalog_alias(scratch["hash"], "shoe_sales")
    assert "url" in out
    assert out["url"].endswith(f"/catalog/{scratch['hash']}")
    assert project in out["url"]


# ---------------------------------------------------------------------------
# companion rendering
# ---------------------------------------------------------------------------


def test_catalog_renders_named_with_vchip(fresh_companion_app, project: str, orders_parquet: Path):
    build_and_persist(project, _agg_code(project))
    set_alias(project, "shoe_sales", build_and_persist(project, _filter_code(project)).content_hash)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    entries = {e["alias"]: e for e in r.json()["entries"] if e["alias"]}
    assert "shoe_sales" in entries
    assert entries["shoe_sales"]["version"] == 1


def test_catalog_includes_forensic_versions(fresh_companion_app, project: str, orders_parquet: Path):
    v1 = build_and_persist(project, _agg_code(project))
    set_alias(project, "shoe_sales", v1.content_hash)
    v2 = build_and_persist(project, _filter_code(project))
    set_alias(project, "shoe_sales", v2.content_hash)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    entries = r.json()["entries"]
    # V2 is current, V1 is forensic
    forensic = [e for e in entries if e["alias"] == "shoe_sales" and not e["is_current"]]
    current = [e for e in entries if e["alias"] == "shoe_sales" and e["is_current"]]
    assert len(forensic) == 1
    assert len(current) == 1
    assert current[0]["version"] == 2


def test_entry_detail_resolves_alias_lookup(fresh_companion_app, project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    set_alias(project, "shoe_sales", res.content_hash)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/shoe_sales")
    assert r.status_code == 200
    body = r.json()
    assert body["content_hash"] == res.content_hash
    assert body["alias"] == "shoe_sales"


def test_entry_detail_shows_forensic_history(fresh_companion_app, project: str, orders_parquet: Path):
    v1 = build_and_persist(project, _agg_code(project))
    set_alias(project, "shoe_sales", v1.content_hash)
    v2 = build_and_persist(project, _filter_code(project))
    set_alias(project, "shoe_sales", v2.content_hash)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{v2.content_hash}")
    assert r.status_code == 200
    body = r.json()
    hist_hashes = [h["hash"] for h in body["forensic_history"]]
    assert v1.content_hash in hist_hashes
    assert len(body["forensic_history"]) == 2


def test_catalog_renders_section_headers(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    """T-13: entries API groups named/forensic/scratch with correct flags."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create, catalog_revise, catalog_run

    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))  # V1 becomes forensic
    catalog_run(_filter_code(project), prompt="scratch")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    entries = r.json()["entries"]
    named_current = [e for e in entries if e["alias"] and e["is_current"]]
    named_forensic = [e for e in entries if e["alias"] and not e["is_current"]]
    assert len(named_current) >= 1
    assert len(named_forensic) >= 1


def test_api_aliases_returns_history(fresh_companion_app, project: str):
    set_alias(project, "a", "h1")
    set_alias(project, "a", "h2")
    set_alias(project, "b", "x1")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/aliases")
    assert r.status_code == 200
    body = r.json()
    names = {a["name"]: a for a in body["aliases"]}
    assert names["a"]["hash"] == "h2"
    assert names["a"]["history"] == ["h1", "h2"]
    assert names["b"]["hash"] == "x1"
