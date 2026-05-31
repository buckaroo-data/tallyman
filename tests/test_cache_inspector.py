from __future__ import annotations

from fastapi.testclient import TestClient

from pydata_companion import create_app
from pydata_core.aliases import history_for
from pydata_core.paths import entry_dir
from pydata_mcp.server import catalog_create, catalog_revise
from pydata_xorq.build import list_entries


def _agg_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _filter_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
f = t.filter(t.category == "boots")
expr = f.group_by("region").aggregate(total=f.price.sum(), n=f.count())
"""


def test_cache_pane_lists_entries(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/cache")
    assert r.status_code == 200
    body = r.text
    assert "Parquet result cache" in body
    assert "shoe_sales" in body  # alias-name column
    assert "Created" in body and "Size" in body
    # every materialised entry's short hash appears
    for e in list_entries(project):
        assert e["content_hash"][:12] in body


def test_cache_delete_removes_only_parquet(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
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
    r = c.post(f"/{project}/api/cache/delete/{h}")
    assert r.status_code == 200
    assert r.json()["deleted"] == h

    assert not pq_path.exists()  # parquet gone
    assert (stat_cache / "marker").read_text() == "keep me"  # stat cache kept
    assert (edir / "expr.py").exists()  # code kept
    assert (edir / "manifest.json").exists()  # entry kept
    assert h in history_for(project, "shoe_sales")  # alias history intact


def test_cache_delete_missing_parquet_404(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    c = TestClient(fresh_companion_app)
    assert c.post(f"/{project}/api/cache/delete/{h}").status_code == 200
    # second delete: parquet already gone
    assert c.post(f"/{project}/api/cache/delete/{h}").status_code == 404


def test_cache_delete_blocked_in_read_only(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    pq_path = entry_dir(project, h) / "result.parquet"
    assert pq_path.exists()

    c = TestClient(create_app(project, read_only=True))
    # serve mode: the parquet must not be deletable...
    assert c.post(f"/{project}/api/cache/delete/{h}").status_code == 403
    assert pq_path.exists()  # untouched
    # ...and no clickable delete button may render in the pane. (The
    # deleteCache() JS helper and .del-btn CSS live in the always-emitted
    # script/style blocks; the per-row onclick invocation is the gated affordance.)
    body = c.get(f"/{project}/cache").text
    assert "Parquet result cache" in body
    assert 'onclick="deleteCache(' not in body


def test_cache_delete_rejects_non_hex_hash(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    c = TestClient(fresh_companion_app)
    # A content hash is lowercase hex (it names an entry dir). A junk value can
    # only ever resolve outside the entries tree, so reject it as a bad request
    # rather than letting it fall through to the "no result.parquet" 404 — that
    # 404 reads as "valid entry, already evicted", which a malformed hash isn't.
    # (Dot-segments like ".." are normalised away by the client before routing,
    # so the guard's job is the charset check, not path-traversal defence.)
    for bad in ("NOPE-not-a-hash", "Deadbeef", "abc def"):
        r = c.post(f"/{project}/api/cache/delete/{bad}")
        assert r.status_code == 400, f"{bad!r} should be a 400, got {r.status_code}"
