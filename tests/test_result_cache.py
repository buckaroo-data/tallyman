from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_core.paths import catalog_dir, entry_dir
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


def _parent_code(project: str, col: str) -> str:  # single-column rename → distinct, unionable
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select(k=t.{col})
"""


def _union_code(parent_a: str, parent_b: str) -> str:  # combine two from_catalog parents
    return f"""
from tallyman_xorq.io import from_catalog
a = from_catalog({parent_a!r}).select(k="k")
b = from_catalog({parent_b!r}).select(k="k")
expr = a.union(b)
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
    # The result_cache/ dir was retired in #73 and must never be re-created.
    assert not (catalog_dir(project) / "result_cache").exists()
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
    # The result_cache/ dir was retired in #73 — baked results live in the compute cache.
    assert not (catalog_dir(project) / "result_cache").exists()
    # The build still carries a baked CachedNode (load_entry surfaces it)...
    assert type(load_entry(project, h).op()).__name__ == "CachedNode"
    # ...but cached_result_expr reads that baked snapshot as a single-backend
    # deferred read, not the cache node itself: a CachedNode carries its own
    # storage connection, which would make a composed union/join span two
    # backends (#75). A bare Read on the default backend composes cleanly.
    ce = cached_result_expr(project, h)
    assert type(ce.op()).__name__ == "Read"
    assert len(ce._find_backends()[0]) == 1
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


def test_multi_parent_from_catalog_shares_one_backend(project, orders_parquet, monkeypatch):
    # #75: combining two from_catalog parents in one expression must resolve to a
    # single backend. #73's expression-level from_catalog loaded each parent into
    # its own backend, so a union/join raised "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import from_catalog

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))
    catalog_create("pb", _parent_code(project, "category"))

    a = from_catalog("pa").select(k="k")
    b = from_catalog("pb").select(k="k")
    # Must not raise XorqError: Multiple backends found for this expression.
    ibis.union(a, b)._find_backend()


def test_multi_parent_from_catalog_union_builds(project, orders_parquet, monkeypatch):
    # #75: an entry that unions two from_catalog parents must build end-to-end.
    # On the #73 branch this aborted with BuildError: Multiple backends found.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))
    catalog_create("pb", _parent_code(project, "category"))

    res = catalog_create("ab", _union_code("pa", "pb"))
    assert "error" not in res, res
    # 200 source rows per parent → 400 unioned.
    assert res["row_count"] == 400


def _mix_code(project: str, parent: str) -> str:  # from_project + from_catalog in one expression
    return f"""
from tallyman_xorq.io import from_project, from_catalog
fp = from_project("orders.parquet", project={project!r}).select(k="region")
fc = from_catalog({parent!r}).select(k="k")
expr = fp.union(fc)
"""


def test_from_project_and_from_catalog_mix_shares_one_backend(project, orders_parquet, monkeypatch):
    # #75: a from_project read combined with a from_catalog parent in one
    # expression must resolve to a single backend. #73 rooted from_project on the
    # default backend but loaded from_catalog into its own, so the mix raised
    # "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import from_catalog, from_project

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))

    fp = from_project("orders.parquet", project=project).select(k="region")
    fc = from_catalog("pa").select(k="k")
    ibis.union(fp, fc)._find_backend()  # must not raise


def test_from_project_and_from_catalog_mix_builds(project, orders_parquet, monkeypatch):
    # #75: an entry mixing from_project and from_catalog must build end-to-end.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))

    res = catalog_create("mix", _mix_code(project, "pa"))
    assert "error" not in res, res
    # 200 from_project rows + 200 from_catalog rows.
    assert res["row_count"] == 400


def test_expensive_from_catalog_mix_shares_one_backend(project, orders_parquet, monkeypatch):
    # #75: an expensive parent resolves to a deferred read of its baked snapshot —
    # a bare Read on the default backend — so mixing it with a from_project read
    # also stays on one backend. A baked CachedNode (the #73 form) would have
    # dragged its own storage backend in and re-raised "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import from_catalog, from_project

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))  # Aggregate → expensive → baked snapshot

    ft = from_project("orders.parquet", project=project)
    fp = ft.group_by("region").aggregate(total=ft.price.sum(), n=ft.count())
    fc = from_catalog("agg")  # region, total, n — same schema as fp
    ibis.union(fp, fc)._find_backend()  # must not raise


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
