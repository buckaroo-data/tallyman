from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_core.paths import catalog_dir, entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.result_cache import cache_worthy, classify_build


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


def _self_chain_code(alias: str) -> str:  # the documented revise pattern: chain off own alias
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({alias!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _chain_off_expensive_code(parent: str) -> str:  # cheap child chaining off ONE expensive parent
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.mutate(total2=t.total * 2)
"""


def _agg_avg_code(project: str) -> str:  # a second Aggregate → expensive, joins on region
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(avg_price=t.price.mean())
"""


def _join_two_expensive_code(parent_a: str, parent_b: str) -> str:  # multi-parent: join two expensive parents
    return f"""
from tallyman_xorq.io import from_catalog
a = from_catalog({parent_a!r})
b = from_catalog({parent_b!r})
expr = a.join(b, "region", how="left")
"""


def _scalar_udf_code(project: str) -> str:  # scalar UDF only (no Aggregate/Join/Sort) → worthy via UDF (#81)
    return f"""
from tallyman_xorq.io import from_project
from xorq.expr.udf import make_pandas_udf
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.vendor.ibis import schema as ibis_schema

t = from_project("orders.parquet", project={project!r})


def plusone(df):
    return df["qty"] + 1


_udf = make_pandas_udf(plusone, ibis_schema({{"qty": dt.int64}}), dt.int64, name="plusone")
expr = t.mutate(qty_plus=_udf.on_expr(t))
"""


def _call_under_tight_recursion_guard(fn, *args):
    """Run *fn* with the recursion limit pinned just above the current depth.

    A genuine infinite self-recursion then raises ``RecursionError`` after a
    handful of frames (tens of MB) instead of growing the expression tree until
    the box swaps to death — so a *failing* run of this test is CI-safe rather
    than a machine-locking ~50GB RSS spike. A correct (bounded) reconstruction
    fits well within the margin.
    """
    import sys

    depth = 0
    frame = sys._getframe()
    while frame is not None:
        depth += 1
        frame = frame.f_back
    old = sys.getrecursionlimit()
    sys.setrecursionlimit(depth + 80)
    try:
        return fn(*args)
    finally:
        sys.setrecursionlimit(old)


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


def test_cheap_entry_writes_no_result_parquet(project, orders_parquet, monkeypatch):
    # A cheap entry materialises nothing — no result.parquet, ever (not at build,
    # not on read), and nothing under the (retired) result_cache dir.
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    rp = entry_dir(project, h) / "result.parquet"
    assert not rp.exists()
    # The result_cache/ dir was retired in #73 and must never be re-created.
    assert not (catalog_dir(project) / "result_cache").exists()
    # Reading the entry recomputes the recipe — still no result.parquet on disk.
    df = cached_result_expr(project, h).execute()
    assert len(df) > 0
    assert not rp.exists()


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
    # Reading executes against the baked snapshot; no per-entry result.parquet.
    assert len(ce.execute()) > 0
    assert not (entry_dir(project, h) / "result.parquet").exists()


def test_scalar_udf_is_worthy_in_lockstep_with_classify_build(project, orders_parquet, monkeypatch):
    # #81: a scalar UDF used without any accompanying expensive op must be judged
    # worthy by BOTH predicates. classify_build greps the serialized YAML (where the
    # node is `op: ScalarUDF`) and has always returned worthy=True; _is_worthy_expr
    # walked the live graph testing `"UDF" in type(node).__name__`, but a
    # make_pandas_udf node is classed after the user function (here `plusone`), so
    # the substring missed the ScalarUDF base and the live predicate returned False —
    # the two "change one, change both" predicates fell out of lockstep. They must agree.
    from tallyman_xorq.result_cache import _recipe_expr
    from tallyman_xorq.source_cache import _is_worthy_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("udf", _scalar_udf_code(project))
    h = _hash_of(project)
    assert "udf:ScalarUDF" in classify_build(entry_dir(project, h) / "xorq_build")["why"]
    assert cache_worthy(project, h) is True
    assert _is_worthy_expr(_recipe_expr(project, h)) is True


def test_scalar_udf_only_entry_bakes_result_cache(project, orders_parquet, monkeypatch):
    # #81 net effect: because _is_worthy_expr returned False for a scalar-UDF-only
    # expression, rewrite_for_build baked no top-level result cache, so every viewer
    # / diff / from_catalog read recomputed the UDF over the whole DAG (the
    # worthy-recompute-fallback kept results correct but uncached — a silent perf
    # regression). With the live predicate fixed, the entry bakes a CachedNode like
    # any other expensive entry and cached_result_expr dereferences a materialised
    # single-backend snapshot. Mirrors test_expensive_entry_bakes_result_cache.
    #
    # NB: xorq's make_pandas_udf mints a fresh class per reconstruction, so the
    # snapshot's structural key drifts across processes — the build-time bake is not
    # reused cross-process and a cold read self-heals its own snapshot. That deeper
    # key-stability gap is tracked separately; here we assert the lockstep fix and
    # that a read dereferences a materialised snapshot.
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.build import load_entry
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("udf", _scalar_udf_code(project))
    h = _hash_of(project)

    # The build bakes a top-level CachedNode (pre-fix the top op was a bare Project).
    assert type(load_entry(project, h).op()).__name__ == "CachedNode"

    # cached_result_expr dereferences a single-backend snapshot, self-healing it on
    # this cold read; afterward a baked result_cache parquet exists on disk.
    ce = cached_result_expr(project, h)
    assert type(ce.op()).__name__ == "Read"
    assert len(ce._find_backends()[0]) == 1
    assert "qty_plus" in ce.columns
    assert len(ce.execute()) > 0
    assert any(compute_cache_dir(project).rglob("result_cache/*.parquet"))


def test_cheap_entry_cached_result_expr_is_not_a_cache_node(project, orders_parquet, monkeypatch):
    # A cheap entry recomputes on read — its expression is not a CachedNode.
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    assert type(cached_result_expr(project, h).op()).__name__ != "CachedNode"


def test_cold_read_logs_path_and_wall_time(project, orders_parquet, monkeypatch, caplog):
    # #87: each cold cached_result_expr read is tagged on tallyman.perf with the
    # #74 path it took (baked-read / cheap-recompute / evicted-self-heal) plus
    # wall-clock, so a Join-descendant that is structurally worthy but cost-cheap
    # is visible from the read path, not just lifecycle counts.
    import logging

    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)
    catalog_create("proj", _project_code(project))
    proj_h = _hash_of(project)

    def perf_tags(call):
        cached_result_expr.cache_clear()  # force a cold read (the LRU memoises warm)
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="tallyman.perf"):
            call()
        return [r.getMessage() for r in caplog.records if r.name == "tallyman.perf"]

    # Expensive entry with a live baked snapshot → baked-read.
    msgs = perf_tags(lambda: cached_result_expr(project, agg_h))
    assert any("baked-read" in m and agg_h in m for m in msgs)

    # Cheap entry → cheap-recompute.
    msgs = perf_tags(lambda: cached_result_expr(project, proj_h))
    assert any("cheap-recompute" in m and proj_h in m for m in msgs)

    # An evicted snapshot self-heals (recompute once, then read) on the next read.
    p = baked_snapshot_path(project, agg_h)
    assert p is not None and p.exists()
    p.unlink()
    msgs = perf_tags(lambda: cached_result_expr(project, agg_h))
    assert any("evicted-self-heal" in m and agg_h in m for m in msgs)


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


def test_diff_route_survives_cold_cache(fresh_companion_app, project, orders_parquet, monkeypatch):
    # The diff route composes both entries' cache-resolving expressions; on a cold
    # compute cache it must recompute/self-heal rather than blank the diff (there
    # is no per-entry result.parquet to evict any more — the cache is the snapshot).
    import shutil

    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    catalog_revise("proj", _project_code(project).replace('select("region", "price")', 'select("region")'))
    # Cold cache: wipe the compute cache and clear the reconstruction lru.
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/proj/1/2")
    assert r.status_code == 200
    diff = r.json()["diff"]
    assert "stats" in diff or "keyed" in diff


def test_cached_result_expr_self_heals_expensive_parent_chain_on_cold_cache(project, orders_parquet, monkeypatch):
    """#73/#74: reading an entry that ``from_catalog``s an expensive parent must
    self-heal an evicted parent snapshot, not error.

    An expensive parent bakes its result into the per-project compute cache; a
    child that chains off it serialises a *bare* ``deferred_read_parquet`` of
    that snapshot (no recipe, no source to recompute from — #75 strips the
    parent's ``CachedNode`` so the composed expression stays on one backend). On
    a cold compute cache (fresh clone / write-isolated overlay) that read
    resolves to zero files and ``load_entry`` raises ``ValueError: At least one
    path is required``. ``cached_result_expr`` instead reconstructs via the recipe
    — re-resolving ``from_catalog`` and recomputing the parent — so the read
    self-heals.
    """
    import shutil

    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))  # expensive parent (Aggregate)
    res = catalog_create("chain", _chain_off_expensive_code("agg"))  # cheap mutate child off it
    assert "error" not in res, res
    expected_rows = res["row_count"]
    child_h = _hash_of(project)

    # Cold start, as a fresh clone / write-isolated overlay / restarted app has:
    # evict every baked snapshot (the parent's included), so the build's bare
    # snapshot read dangles, AND clear the reconstruction lru — the in-process
    # build above warmed it with the parent's pre-eviction snapshot read, which a
    # fresh process would not carry (the perf overlay where this surfaced is cold).
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()

    df = cached_result_expr(project, child_h).execute()  # pre-fix: ValueError "At least one path is required"
    assert len(df) == expected_rows


def test_cached_result_expr_self_heals_multi_parent_expensive_join_on_cold_cache(project, orders_parquet, monkeypatch):
    """#73/#75: a multi-parent entry joining two expensive ``from_catalog`` parents
    must self-heal *both* evicted parent snapshots on a cold cache.

    This is the ``tickets_and_ghosts`` shape: a join of two expensive parents.
    The child is itself expensive (Join), so its build is a top-level
    ``CachedNode`` whose parent joins two *bare* snapshot reads. On a cold cache
    the child's cache node recomputes — but the two parent reads dangle (both
    snapshots evicted) → ``ValueError: At least one path is required``. The read
    reconstructs via the recipe, recomputing each parent in turn.
    """
    import shutil

    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg_a", _agg_code(project))  # expensive parent 1 → (region, total, n)
    catalog_create("agg_b", _agg_avg_code(project))  # expensive parent 2 → (region, avg_price)
    res = catalog_create("joined", _join_two_expensive_code("agg_a", "agg_b"))
    assert "error" not in res, res
    expected_rows = res["row_count"]
    child_h = _hash_of(project)

    # Cold start (fresh clone / overlay / restart): evict both parent snapshots
    # and the child's, and clear the reconstruction lru the in-process build
    # warmed — so every snapshot in the chain must self-heal from the recipe.
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()

    df = cached_result_expr(project, child_h).execute()  # pre-fix: ValueError "At least one path is required"
    assert len(df) == expected_rows


def test_revise_in_place_self_reference_terminates(project, orders_parquet, monkeypatch):
    """A revise-in-place recipe that reads ``from_catalog`` of its OWN alias must
    not self-recurse forever when later reconstructed.

    ``catalog_revise`` repoints the alias to the new head *after* building, so a
    recipe that chains off ``from_catalog(name)`` (the pattern its own docstring
    recommends) resolves to the *previous* revision at build time but to *itself*
    afterwards. #73 made ``cached_result_expr`` re-import the raw recipe and
    re-resolve ``from_catalog`` against the live head, so post-revise
    reconstruction (diff, further chaining) recursed without bound — a ~50GB RSS
    spike that locked a 48GB machine. The fix resolves a self-referential
    ``from_catalog`` to the build-time parent revision.
    """
    from tallyman_core.aliases import get_alias
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("tpd", _project_code(project))  # v1: region, price
    v1 = get_alias(project, "tpd")
    catalog_revise("tpd", _self_chain_code("tpd"))  # v2: from_catalog('tpd').mutate(doubled=price*2)
    v2 = get_alias(project, "tpd")
    assert v2 != v1

    # Pre-fix: infinite recursion (guarded so the failure is a fast RecursionError,
    # not an OOM). Post-fix: resolves from_catalog('tpd') to v1 and returns.
    expr = _call_under_tight_recursion_guard(cached_result_expr, project, v2)

    # Correct reconstruction == v1's rows plus the mutated column.
    assert "doubled" in expr.schema().names
    assert expr.execute().shape[0] == cached_result_expr(project, v1).execute().shape[0]


def test_recipe_reconstruction_does_not_leak_sys_modules(project, orders_parquet, monkeypatch):
    # #73 made entry reconstruction (cached_result_expr -> _recipe_expr ->
    # _import_script) a per-READ operation. _import_script registers the recipe
    # module in sys.modules under a unique uuid name; left there, every cold read
    # leaks a module object that pins its whole expression graph. Reconstructing
    # the same entry repeatedly must not accumulate sys.modules entries.
    import sys

    from tallyman_xorq.result_cache import _recipe_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    before = [m for m in sys.modules if m.startswith("tallyman_expr_")]
    for _ in range(5):
        _recipe_expr(project, h)  # not lru-cached: re-imports every call
    after = [m for m in sys.modules if m.startswith("tallyman_expr_")]
    assert len(after) == len(before), f"leaked {len(after) - len(before)} recipe modules"


def test_worthiness_disagreement_falls_back_to_recompute(project, orders_parquet, monkeypatch):
    # classify_build (serialized) and _is_worthy_expr (live) are meant to agree,
    # but cached_result_expr guards a disagreement: if cache_worthy is True yet
    # rewrite_for_build bakes no top-level CachedNode, it must return the
    # recompute expression rather than dereference a cache that was never baked.
    # Force the disagreement by pinning cache_worthy True over a cheap entry
    # (which bakes no result cache) and assert the read does not raise.
    import tallyman_xorq.result_cache as rc

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    monkeypatch.setattr(rc, "cache_worthy", lambda *a, **k: True)
    rc.cached_result_expr.cache_clear()
    expr = rc.cached_result_expr(project, h)  # must not raise
    assert type(expr.op()).__name__ != "CachedNode"


def test_ensure_result_is_removed():
    # The on-demand result.parquet writer is deleted entirely: every consumer
    # now reads cached_result_expr (cheap recompute / expensive baked snapshot),
    # so nothing materialises a per-entry result.parquet on demand. The #102
    # viewer-from-result.parquet build helpers go with it — the viewer pages over
    # the baked snapshot (expensive) or recomputes (cheap), never a result.parquet.
    import tallyman_xorq.result_cache as rc

    assert not hasattr(rc, "ensure_result")
    assert not hasattr(rc, "ensure_result_build")
    assert not hasattr(rc, "ensure_viewer_expanded_build")


def test_cached_result_expr_self_heals_after_warm_then_evict(project, orders_parquet, monkeypatch):
    """An expensive entry's baked snapshot evicted *after* a warm read must still
    self-heal on the next read, not dangle on a stale ``deferred_read_parquet``.

    ``cached_result_expr`` memoised the snapshot existence check, so once an
    expensive entry was warm in the LRU, evicting its snapshot left the cached
    expression pointing at a now-missing path — the next ``.execute()`` raised
    ``ValueError: At least one path is required``. The existence check + self-heal
    must run on every call, not only on a cold LRU miss.
    """
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)

    # Warm the LRU via a real read, exactly as the public from_catalog API does.
    expected = len(cached_result_expr(project, agg_h).execute())

    # Evict the baked snapshot while the LRU stays WARM (no cache_clear).
    p = baked_snapshot_path(project, agg_h)
    assert p is not None and p.exists()
    p.unlink()

    # Re-read the SAME entry: must self-heal, not dangle on the stale deferred read.
    df = cached_result_expr(project, agg_h).execute()
    assert len(df) == expected
