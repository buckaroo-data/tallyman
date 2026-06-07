from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_core.aliases import history_for
from tallyman_core.paths import entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries

# The cache pane is a React page (CachePage) backed by JSON: GET
# /{project}/api/result_cache lists materialised parquets, DELETE
# /{project}/api/result_cache/{hash} evicts one. These tests exercise that
# contract; the rendering itself is the SPA's job.


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _filter_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
f = t.filter(t.category == "boots")
expr = f.group_by("region").aggregate(total=f.price.sum(), n=f.count())
"""


def test_cache_pane_lists_entries(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/result_cache")
    assert r.status_code == 200
    body = r.json()
    aliases = [e["alias"] for e in body["entries"] if e["alias"]]
    assert "shoe_sales" in aliases  # alias surfaced on the row
    # every materialised entry shows up in the listing
    listed = {e["hash"] for e in body["entries"]}
    for e in list_entries(project):
        assert e["content_hash"] in listed
    # rows carry size + created metadata for the table
    assert all("size_formatted" in e and "created" in e for e in body["entries"])
    assert "total_formatted" in body


def test_cache_delete_removes_only_parquet(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    edir = entry_dir(project, h)
    pq_path = edir / "result.parquet"
    assert pq_path.exists()
    # stat cache + code that must survive a parquet delete
    stat_cache = edir / ".buckaroo_stat_cache"
    stat_cache.mkdir(exist_ok=True)
    (stat_cache / "marker").write_text("keep me")
    assert (edir / "expr.py").exists()

    c = TestClient(fresh_companion_app)
    r = c.delete(f"/{project}/api/result_cache/{h}")
    assert r.status_code == 200
    assert r.json()["hash"] == h

    assert not pq_path.exists()  # parquet gone
    assert (stat_cache / "marker").read_text() == "keep me"  # stat cache kept
    assert (edir / "expr.py").exists()  # code kept
    assert (edir / "manifest.json").exists()  # entry kept
    assert h in history_for(project, "shoe_sales")  # alias history intact


def test_cache_delete_missing_parquet_404(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    c = TestClient(fresh_companion_app)
    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 200
    # second delete: parquet already gone
    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 404


def test_cache_delete_blocked_in_read_only(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    pq_path = entry_dir(project, h) / "result.parquet"
    assert pq_path.exists()

    c = TestClient(create_app(project, read_only=True))
    # serve mode: the parquet must not be deletable.
    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 403
    assert pq_path.exists()  # untouched


def test_cache_delete_rejects_non_hex_hash(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(fresh_companion_app)
    # A content hash is lowercase hex (it names an entry dir). A junk value can
    # only ever resolve outside the entries tree, so reject it as a bad request
    # rather than letting it fall through to the "no result.parquet" 404 — that
    # 404 reads as "valid entry, already evicted", which a malformed hash isn't.
    for bad in ("NOPE-not-a-hash", "Deadbeef", "abc def"):
        r = c.delete(f"/{project}/api/result_cache/{bad}")
        assert r.status_code == 400, f"{bad!r} should be a 400, got {r.status_code}"
