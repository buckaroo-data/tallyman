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


def test_cheap_entry_writes_no_result_parquet_at_build(project, orders_parquet, monkeypatch):
    # #73: a cheap entry materialises nothing at build — no result.parquet, and
    # nothing under the (retired) result_cache dir.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    rp = entry_dir(project, h) / "result.parquet"
    assert not rp.exists()
    assert not result_cache_dir(project).exists() or not any(result_cache_dir(project).glob("**/*.parquet"))
    # ensure_result regenerates it on demand, in place, and a deleted one self-heals.
    out = ensure_result(project, h)
    assert out == rp and out.exists()
    out.unlink()
    assert ensure_result(project, h).exists()


def test_expensive_entry_bakes_result_cache(project, orders_parquet, monkeypatch):
    # #73: an expensive entry bakes a top-level result-cache node into its build
    # (so every loader reads the cached result), materialised in the compute
    # cache — not a build-time entry result.parquet, not the retired result_cache dir.
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.build import load_entry
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    assert not (entry_dir(project, h) / "result.parquet").exists()
    assert not result_cache_dir(project).exists() or not any(result_cache_dir(project).glob("**/*.parquet"))
    # The build carries a baked CachedNode, so loading it (and cached_result_expr)
    # yields a cache-resolving expression rather than the bare recipe.
    assert type(load_entry(project, h).op()).__name__ == "CachedNode"
    assert type(cached_result_expr(project, h).op()).__name__ == "CachedNode"
    assert any(compute_cache_dir(project).rglob("result_cache/*.parquet"))
    # ensure_result still hands back a parquet path on demand, self-healing.
    out = ensure_result(project, h)
    assert out.exists()
    out.unlink()
    assert ensure_result(project, h).exists()


def test_cheap_entry_cached_result_expr_is_not_a_cache_node(project, orders_parquet, monkeypatch):
    # A cheap entry recomputes on read — its expression is not a CachedNode.
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    assert type(cached_result_expr(project, h).op()).__name__ != "CachedNode"


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
