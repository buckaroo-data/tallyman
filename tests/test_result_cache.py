from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_core.paths import entry_dir, result_cache_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.result_cache import cache_worthy, classify_build, ensure_result


def _agg_code(project: str) -> str:  # Aggregate → expensive → cache-worthy
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _project_code(project: str) -> str:  # parquet read + projection → cheap
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _hash_of(project: str) -> str:
    return list_entries(project)[0]["content_hash"]


def test_classifier_skips_cheap_caches_expensive(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)
    assert cache_worthy(project, agg_h) is True
    assert "Aggregate" in classify_build(entry_dir(project, agg_h) / "xorq_build")["why"]

    catalog_create("proj", _project_code(project))
    proj_h = _hash_of(project)
    assert cache_worthy(project, proj_h) is False
    assert "cheap" in classify_build(entry_dir(project, proj_h) / "xorq_build")["why"]


def test_ensure_result_cheap_regenerates_in_place(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    rp = entry_dir(project, h) / "result.parquet"
    assert rp.exists()
    rp.unlink()  # evict
    out = ensure_result(project, h)  # should self-heal in place
    assert out == rp and out.exists()  # cheap → no extra cache dir
    assert not result_cache_dir(project).exists() or not any(result_cache_dir(project).glob("**/*.parquet"))


def test_ensure_result_expensive_uses_snapshot_cache(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    out = ensure_result(project, h)
    # materialised under the project's result_cache dir, not the entry dir
    assert out.exists()
    assert result_cache_dir(project) in out.parents
    out.unlink()  # delete the cache file
    out2 = ensure_result(project, h)  # self-heals via recompute
    assert out2.exists()


def test_diff_route_survives_evicted_parquet(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    catalog_revise("proj", _project_code(project).replace('select("region", "price")', 'select("region")'))
    # evict the older version's parquet — previously this blanked the diff
    older = entry_dir(project, list_entries(project)[-1]["content_hash"]) / "result.parquet"
    if older.exists():
        older.unlink()
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/proj/1/2")
    assert r.status_code == 200
    diff = r.json()["diff"]
    assert "stats" in diff or "keyed" in diff
