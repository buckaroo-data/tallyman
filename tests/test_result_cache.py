from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_core.paths import catalog_dir, entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.result_cache import cache_worthy, classify_build


def _agg_code(project: str) -> str:  # Aggregate → expensive → cache-worthy
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _project_code(project: str) -> str:  # parquet read + projection → cheap
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _parent_code(project: str, col: str) -> str:  # single-column rename → distinct, unionable
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select(k=t.{col})
"""


def _union_code(parent_a: str, parent_b: str) -> str:  # combine two tracked_expr_from_alias parents
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
a = tracked_expr_from_alias({parent_a!r}).select(k="k")
b = tracked_expr_from_alias({parent_b!r}).select(k="k")
expr = a.union(b)
"""


def _self_chain_code(alias: str) -> str:  # the documented revise pattern: chain off own alias
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({alias!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _chain_off_expensive_code(parent: str) -> str:  # cheap child chaining off ONE expensive parent
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.mutate(total2=t.total * 2)
"""


def _agg_avg_code(project: str) -> str:  # a second Aggregate → expensive, joins on region
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(avg_price=t.price.mean())
"""


def _join_two_expensive_code(parent_a: str, parent_b: str) -> str:  # multi-parent: join two expensive parents
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
a = tracked_expr_from_alias({parent_a!r})
b = tracked_expr_from_alias({parent_b!r})
expr = a.join(b, "region", how="left")
"""


def _scalar_udf_code(project: str) -> str:  # scalar UDF only (no Aggregate/Join/Sort) → worthy via UDF (#81)
    return f"""
from tallyman_xorq.io import read_project_file
from xorq.expr.udf import make_pandas_udf
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.vendor.ibis import schema as ibis_schema

t = read_project_file("orders.parquet", project={project!r})


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
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    assert not (entry_dir(project, h) / "result.parquet").exists()
    # The result_cache/ dir was retired in #73 — baked results live in the compute cache.
    assert not (catalog_dir(project) / "result_cache").exists()
    # The entry bakes a result-cache snapshot (located by the same rewrite the build used)...
    assert baked_snapshot_path(project, h) is not None
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
    # / diff / tracked_expr_from_alias read recomputed the UDF over the whole DAG (the
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
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("udf", _scalar_udf_code(project))
    h = _hash_of(project)

    # The entry bakes a result-cache snapshot (pre-fix the top op was a bare
    # Project, so nothing was baked).
    assert baked_snapshot_path(project, h) is not None

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


def test_multi_parent_tracked_expr_from_alias_shares_one_backend(project, orders_parquet, monkeypatch):
    # #75: combining two tracked_expr_from_alias parents in one expression must resolve to a
    # single backend. #73's expression-level tracked_expr_from_alias loaded each parent into
    # its own backend, so a union/join raised "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import tracked_expr_from_alias

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))
    catalog_create("pb", _parent_code(project, "category"))

    a = tracked_expr_from_alias("pa").select(k="k")
    b = tracked_expr_from_alias("pb").select(k="k")
    # Must not raise XorqError: Multiple backends found for this expression.
    ibis.union(a, b)._find_backend()


def test_multi_parent_tracked_expr_from_alias_union_builds(project, orders_parquet, monkeypatch):
    # #75: an entry that unions two tracked_expr_from_alias parents must build end-to-end.
    # On the #73 branch this aborted with BuildError: Multiple backends found.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))
    catalog_create("pb", _parent_code(project, "category"))

    res = catalog_create("ab", _union_code("pa", "pb"))
    assert "error" not in res, res
    # 200 source rows per parent → 400 unioned.
    assert res["row_count"] == 400


def _mix_code(project: str, parent: str) -> str:  # read_project_file + tracked_expr_from_alias in one expression
    return f"""
from tallyman_xorq.io import read_project_file, tracked_expr_from_alias
fp = read_project_file("orders.parquet", project={project!r}).select(k="region")
fc = tracked_expr_from_alias({parent!r}).select(k="k")
expr = fp.union(fc)
"""


def test_read_project_file_and_tracked_expr_from_alias_mix_shares_one_backend(project, orders_parquet, monkeypatch):
    # #75: a read_project_file read combined with a tracked_expr_from_alias parent in one
    # expression must resolve to a single backend. #73 rooted read_project_file on the
    # default backend but loaded tracked_expr_from_alias into its own, so the mix raised
    # "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import read_project_file, tracked_expr_from_alias

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))

    fp = read_project_file("orders.parquet", project=project).select(k="region")
    fc = tracked_expr_from_alias("pa").select(k="k")
    ibis.union(fp, fc)._find_backend()  # must not raise


def test_read_project_file_and_tracked_expr_from_alias_mix_builds(project, orders_parquet, monkeypatch):
    # #75: an entry mixing read_project_file and tracked_expr_from_alias must build end-to-end.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))

    res = catalog_create("mix", _mix_code(project, "pa"))
    assert "error" not in res, res
    # 200 read_project_file rows + 200 tracked_expr_from_alias rows.
    assert res["row_count"] == 400


def test_expensive_tracked_expr_from_alias_mix_shares_one_backend(project, orders_parquet, monkeypatch):
    # #75: an expensive parent resolves to a deferred read of its baked snapshot —
    # a bare Read on the default backend — so mixing it with a read_project_file read
    # also stays on one backend. A baked CachedNode (the #73 form) would have
    # dragged its own storage backend in and re-raised "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import read_project_file, tracked_expr_from_alias

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))  # Aggregate → expensive → baked snapshot

    ft = read_project_file("orders.parquet", project=project)
    fp = ft.group_by("region").aggregate(total=ft.price.sum(), n=ft.count())
    fc = tracked_expr_from_alias("agg")  # region, total, n — same schema as fp
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
    """#73/#74: reading an entry that ``tracked_expr_from_alias``s an expensive parent must
    self-heal an evicted parent snapshot, not error.

    An expensive parent bakes its result into the per-project compute cache; a
    child that chains off it serialises a *bare* ``deferred_read_parquet`` of
    that snapshot (no recipe, no source to recompute from — #75 strips the
    parent's ``CachedNode`` so the composed expression stays on one backend). On
    a cold compute cache (fresh clone / write-isolated overlay) that bare read
    resolves to zero files (``ValueError: At least one path is required``).
    ``cached_result_expr`` reconstructs via the recipe instead — re-resolving
    ``tracked_expr_from_alias`` and recomputing the parent — so the read self-heals.
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
    """#73/#75: a multi-parent entry joining two expensive ``tracked_expr_from_alias`` parents
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
    """A revise-in-place recipe that reads ``tracked_expr_from_alias`` of its OWN alias must
    not self-recurse forever when later reconstructed.

    A head whose recipe chains off ``tracked_expr_from_alias(name)`` (once the
    documented revise pattern; now rejected at the tool boundary by
    ``catalog_revise``, #135, but still reachable for historical heads / indirect
    cycles, so built directly below) resolves to the *previous* revision at build
    time but to *itself* afterwards. #73 made ``cached_result_expr`` re-import the raw recipe and
    re-resolve ``tracked_expr_from_alias`` against the live head, so post-revise
    reconstruction (diff, further chaining) recursed without bound — a ~50GB RSS
    spike that locked a 48GB machine. The fix resolves a self-referential
    ``tracked_expr_from_alias`` to the build-time parent revision.
    """
    from tallyman_core.aliases import get_alias, set_alias
    from tallyman_xorq import build_and_persist
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("tpd", _project_code(project))  # v1: region, price
    v1 = get_alias(project, "tpd")
    # catalog_revise now rejects a self-referential recipe (#135), so build the v2
    # head directly to still exercise the #73 anti-recursion engine — historical
    # self-ref heads and indirect cycles can still reach cached_result_expr.
    res = build_and_persist(project=project, code=_self_chain_code("tpd"))
    set_alias(project, "tpd", res.content_hash, expect_exists=True)
    v2 = get_alias(project, "tpd")
    assert v2 != v1

    # Pre-fix: infinite recursion (guarded so the failure is a fast RecursionError,
    # not an OOM). Post-fix: resolves tracked_expr_from_alias('tpd') to v1 and returns.
    expr = _call_under_tight_recursion_guard(cached_result_expr, project, v2)

    # Correct reconstruction == v1's rows plus the mutated column.
    assert "doubled" in expr.schema().names
    assert expr.execute().shape[0] == cached_result_expr(project, v1).execute().shape[0]


def test_resolve_noncyclic_hash_scopes_step_back_to_requested_alias(project):
    # The resolver derives a lineage hint from `requested`:
    #   alias_hint = requested if get_alias(...) is not None else None  (#85)
    # so a hash that lives in two histories steps back through the alias
    # tracked_expr_from_alias was actually invoked on, not whichever sorts first. This
    # pins that derivation directly — test_revise_in_place_self_reference_
    # terminates only exercises a single-alias chain where the hint is moot.
    from tallyman_core.aliases import set_alias
    from tallyman_xorq.result_cache import _RECONSTRUCTING, _resolve_noncyclic_hash

    # Shared head "hS" in two histories; _write persists alphabetically, so an
    # unscoped (dict-order) step-back always resolves through "alpha".
    #   alpha: [a0, hS]      -> step back from hS == a0
    #   zeta:  [z0, z1, hS]  -> step back from hS == z1
    set_alias(project, "alpha", "a0")
    set_alias(project, "alpha", "hS")
    set_alias(project, "zeta", "z0")
    set_alias(project, "zeta", "z1")
    set_alias(project, "zeta", "hS")

    # Mark only hS in-flight so the resolver takes exactly one hop; the returned
    # parent reveals which lineage was followed.
    token = _RECONSTRUCTING.set(frozenset({(project, "hS")}))
    try:
        # requested names an alias -> hint scopes the step-back to that lineage.
        assert _resolve_noncyclic_hash(project, "zeta", "hS") == "z1"
        assert _resolve_noncyclic_hash(project, "alpha", "hS") == "a0"
        # requested is a literal hash pin (no such alias) -> hint is None and the
        # resolver keeps the dict-order fallback ("alpha"), proving the hash-pin
        # branch of the derivation.
        assert _resolve_noncyclic_hash(project, "hS", "hS") == "a0"
    finally:
        _RECONSTRUCTING.reset(token)


def test_resolve_noncyclic_hash_multi_hop_stays_in_requested_lineage(project):
    # The hint is computed once and reused for every hop. A multi-hop walk that
    # stays inside the requested alias's history must keep following it: each
    # parent is still in that history, so the hint never goes stale within the
    # lineage. Without scoping, the second hop would resolve through the
    # alphabetically-first alias and pick the wrong parent (#85, #74).
    from tallyman_core.aliases import set_alias
    from tallyman_xorq.result_cache import _RECONSTRUCTING, _resolve_noncyclic_hash

    #   alpha: [ax, z1]          -> dict-order step back from z1 == ax (the trap)
    #   zeta:  [z0, z1, z2]      -> scoped step back: z2 -> z1 -> z0
    set_alias(project, "alpha", "ax")
    set_alias(project, "alpha", "z1")
    set_alias(project, "zeta", "z0")
    set_alias(project, "zeta", "z1")
    set_alias(project, "zeta", "z2")

    # Both z2 and its parent z1 are in-flight, forcing two hops.
    token = _RECONSTRUCTING.set(frozenset({(project, "z2"), (project, "z1")}))
    try:
        # z2 -> z1 -> z0, all within "zeta". A stale hint at the z1 hop would
        # resolve through "alpha" and return "ax".
        assert _resolve_noncyclic_hash(project, "zeta", "z2") == "z0"
    finally:
        _RECONSTRUCTING.reset(token)


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


def _overwrite_orders(project: str, seed: int) -> None:
    """Rewrite the orders source with different values (same schema, same rows).

    A read_project_file recompute under ``off`` identity reads the live path, so this
    makes a previously-built entry's recompute drift from its build-time bytes —
    the execution-nondeterminism stand-in #83's faithfulness check must catch.
    """
    from tallyman_cli.fixtures import write_shoe_orders
    from tallyman_core import data_dir

    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=seed)


def test_build_records_stable_result_digest(project, orders_parquet, monkeypatch):
    # #83: every build records a result_digest — a content digest of the executed
    # bytes, the second identity axis the structural content_hash can't see. It is
    # recorded for cheap and expensive entries alike and is stable: the same recipe
    # over the same source digests identically run-to-run.
    from tallyman_core import entry_dir, read_manifest
    from tallyman_xorq.result_cache import cached_result_expr, result_digest

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)

    for name, code in (("proj", _project_code(project)), ("agg", _agg_code(project))):
        catalog_create(name, code)
        h = _hash_of(project)
        digest = read_manifest(entry_dir(project, h)).result_digest
        assert digest, f"{name} entry recorded no result_digest"
        # Recomputing the same entry's result hashes to the recorded digest.
        cached_result_expr.cache_clear()
        assert result_digest(cached_result_expr(project, h)) == digest


def test_verify_result_faithful_true_for_deterministic_entry(project, orders_parquet, monkeypatch):
    # #83: a deterministic entry's served bytes match its build-time digest, so the
    # faithfulness predicate is True. None would mean "no digest recorded"; False
    # would mean drift — neither is right for a clean entry.
    from tallyman_xorq.result_cache import verify_result_faithful

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    assert verify_result_faithful(project, h) is True


def test_verify_result_faithful_detects_recompute_drift(project, orders_parquet, monkeypatch):
    # #83: the soundness hole. A cheap entry recomputes on read; if its source
    # drifts under it, the recompute serves different bytes under the SAME
    # content_hash — invisible to the structural hash. The result_digest catches
    # it: verify_result_faithful flips True -> False once the bytes change.
    from tallyman_xorq.result_cache import cached_result_expr, verify_result_faithful

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    assert verify_result_faithful(project, h) is True

    _overwrite_orders(project, seed=1)  # recompute now reads different bytes
    cached_result_expr.cache_clear()
    assert verify_result_faithful(project, h) is False


def test_self_heal_warns_on_unfaithful_recompute(project, orders_parquet, monkeypatch, caplog):
    # #83: eviction's load-bearing assumption is that an evicted snapshot recomputes
    # to what was evicted. For an entry whose recompute drifts, that is false — the
    # snapshot self-heals to DIFFERENT bytes, silently. The faithfulness check fires
    # a tallyman.perf warning when a self-healed snapshot's digest != the recorded
    # one, and the read still succeeds (the check is advisory, never fatal).
    import logging

    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))  # expensive → baked snapshot
    h = _hash_of(project)

    # Drift the source, then evict the snapshot so the next read self-heals from the
    # now-different source rather than returning the frozen build-time bytes.
    _overwrite_orders(project, seed=1)
    p = baked_snapshot_path(project, h)
    assert p is not None and p.exists()
    p.unlink()
    cached_result_expr.cache_clear()

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
        df = cached_result_expr(project, h).execute()  # self-heals, must not raise
    assert len(df) > 0
    msgs = [r.getMessage() for r in caplog.records if r.name == "tallyman.perf"]
    # Source drift keeps the recipe graph fixed (same expr.py, same read), so the
    # drift is attributed to the execution axis (#83), not the structural one (#88).
    assert any("UNFAITHFUL" in m and h in m and "execution (#83)" in m for m in msgs), msgs


def test_self_heal_warning_degrades_to_execution_when_classify_build_raises(
    project, orders_parquet, monkeypatch, caplog
):
    # #88 part 2, best-effort contract: the structural-vs-execution attribution must
    # never break the read. recipe_is_structurally_nondeterministic reads the build's
    # serialized ops via classify_build (Path.glob + read_text); on a missing or
    # unreadable build dir that raises, and the call sits inside the self-heal block
    # OUTSIDE its surrounding try/except, so an unguarded raise propagates out of
    # cached_result_expr and kills the read it was only meant to annotate. The
    # predicate must swallow that and degrade to False (not-structural), so the drift
    # is still labeled execution (#83) and the read still succeeds.
    import logging

    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))  # expensive → baked snapshot
    h = _hash_of(project)

    # Warm the plan memo with a working classify_build FIRST: _resolve_result_plan
    # calls cache_worthy → classify_build too, so memoising the plan here means the
    # self-heal read below skips that call and the only classify_build left on the
    # path is the predicate's at line 360 — the one statement under test.
    cached_result_expr.cache_clear()
    cached_result_expr(project, h).execute()

    # Drift the source and evict the snapshot so the next read self-heals to bytes
    # that differ from the recorded digest, firing the faithfulness warning.
    _overwrite_orders(project, seed=1)
    p = baked_snapshot_path(project, h)
    assert p is not None and p.exists()
    p.unlink()

    # Now make the predicate's classify_build raise. The plan stays memoised (no
    # cache_clear), so cache_worthy is not re-invoked; only recipe_is_structurally_
    # nondeterministic hits this on the self-heal warning path.
    def _raise(*_a, **_k):
        raise OSError("build_dir inaccessible")

    monkeypatch.setattr("tallyman_xorq.result_cache.classify_build", _raise)

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
        df = cached_result_expr(project, h).execute()  # must NOT raise despite the fault
    assert len(df) > 0
    msgs = [r.getMessage() for r in caplog.records if r.name == "tallyman.perf"]
    # Degraded to not-structural → execution axis, never a false structural claim.
    assert any("UNFAITHFUL" in m and h in m and "execution (#83)" in m for m in msgs), msgs
    assert not any("structural (#88)" in m for m in msgs), msgs


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

    # Warm the LRU via a real read, exactly as the public tracked_expr_from_alias API does.
    expected = len(cached_result_expr(project, agg_h).execute())

    # Evict the baked snapshot while the LRU stays WARM (no cache_clear).
    p = baked_snapshot_path(project, agg_h)
    assert p is not None and p.exists()
    p.unlink()

    # Re-read the SAME entry: must self-heal, not dangle on the stale deferred read.
    df = cached_result_expr(project, agg_h).execute()
    assert len(df) == expected


def test_concurrent_cold_heal_is_single_flighted(project, orders_parquet, monkeypatch):
    """Concurrent cold reads of the same expensive entry must single-flight the
    snapshot heal (#79).

    ``cached_result_expr`` heals an evicted baked snapshot by executing the shared
    (lru-memoised) ``baked`` op. Without a per-``(project, content_hash)`` guard, two
    concurrent cold readers both run that materialisation: in-process they collide on
    the shared op (DataFusion ``"Already borrowed"``); across processes they collide
    on xorq ParquetStorage's fixed ``<key>.parquet.tmp`` (``FileNotFoundError`` on the
    loser's rename). Either surfaces as a 500 from ``api_data`` and, swallowed by
    ``ensure_session``, an empty grid. The heal must run once; the rest wait, re-check,
    and read the materialised snapshot.

    The returned expression is built but not executed inside the workers — this
    isolates the *materialisation* race (what #79 is about) from concurrent
    ``.execute()`` on the shared single DataFusion backend, which raises
    ``"Already borrowed"`` for any concurrent read (warm or cold) and is a separate
    backend-thread-safety concern.
    """
    import threading

    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)

    # Warm the plan so concurrent readers share the one baked op, and learn the result.
    expected = len(cached_result_expr(project, agg_h).execute())

    def run_round():
        # Evict the snapshot, then fire two barrier-synced readers that each trigger the
        # heal (building the expr, not executing it). Any raised exception is the
        # materialisation race; the snapshot must end up materialised exactly once.
        p = baked_snapshot_path(project, agg_h)
        assert p is not None and p.exists()
        p.unlink()

        errors = []
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()  # maximise overlap on the heal
            try:
                cached_result_expr(project, agg_h)  # triggers the heal; expr is not executed here
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return errors, p

    # A few rounds: the collision is deterministic (shared op + shared tmp), but rounds
    # widen the window so a one-off scheduling fluke can't green a still-racy heal.
    for _ in range(3):
        errors, p = run_round()
        assert not errors, f"concurrent cold heal raced: {errors!r}"
        assert p.exists()  # materialised exactly once, intact

    # The healed snapshot reads back correctly (executed sequentially — concurrent
    # .execute() on the shared backend is out of scope for this materialisation test).
    assert len(cached_result_expr(project, agg_h).execute()) == expected


def test_cross_process_heal_reads_landed_snapshot_else_reraises(project, orders_parquet, monkeypatch):
    """The cross-process arm of the heal: a peer *process* can win xorq
    ParquetStorage's fixed ``<key>.parquet.tmp`` rename, so this process's
    ``baked.count().execute()`` raises ``FileNotFoundError``/``ValueError`` (#79).
    The ``except`` gates on the snapshot's on-disk presence, not the exception type:
    if the peer's snapshot landed, read it; otherwise the failure is real and must
    propagate rather than dangle on a missing-path ``deferred_read_parquet``.

    Two interpreters can't share one in-process backend here, so this stubs
    ``_resolve_result_plan`` to hand back a baked op whose ``execute()`` raises after
    (landed) the snapshot reappeared or (not-landed) it stayed gone — exactly the two
    outcomes the arm distinguishes. The in-process collision is covered separately by
    ``test_concurrent_cold_heal_is_single_flighted``.
    """
    import pytest

    import tallyman_xorq.result_cache as rc
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)

    # Warm + learn the result, and keep a real valid parquet to "land" in the peer case.
    expected = len(cached_result_expr(project, agg_h).execute())
    p = baked_snapshot_path(project, agg_h)
    assert p is not None and p.exists()
    snapshot_bytes = p.read_bytes()

    class _FakeBaked:
        def __init__(self, on_execute):
            self._on_execute = on_execute

        def count(self):
            return self

        def execute(self):
            self._on_execute()

    def _stub_plan(on_execute):
        # Stand in for the lru-memoised plan so the heal hits our controlled execute()
        # against the REAL evicted path — the only thing the except arm reads.
        monkeypatch.setattr(rc, "_resolve_result_plan", lambda _proj, _h: ("baked", _FakeBaked(on_execute), p))

    # Landed: the peer's rename completed (snapshot back on disk) but our execute still
    # raised on the vanished shared tmp — swallow it and read the materialised file.
    # Both caught exception types must behave identically; the gate is path presence.
    landed_excs = [
        FileNotFoundError("peer won the shared .parquet.tmp rename"),
        ValueError("snapshot tmp vanished mid-read"),
    ]
    for exc in landed_excs:
        p.unlink()

        def _land_then_raise(_exc=exc):
            p.write_bytes(snapshot_bytes)
            raise _exc

        _stub_plan(_land_then_raise)
        assert len(cached_result_expr(project, agg_h).execute()) == expected

    # Not landed: no peer materialised the snapshot, so the failure is real and must
    # propagate rather than hand back a dangling read of a missing path.
    p.unlink()

    def _just_raise():
        raise FileNotFoundError("shared .parquet.tmp gone, snapshot never landed")

    _stub_plan(_just_raise)
    with pytest.raises(FileNotFoundError):
        cached_result_expr(project, agg_h)
    assert not p.exists()


# ---------------------------------------------------------------------------
# #115: digest-pinned reconstruction — a cold read must serve the bytes the entry
# was BUILT from (the frozen data/.cas/<digest> clone), not a re-digest of the
# live source after it is edited in place. #86 made *build* identity content-aware
# (a rebuild forks the hash) but left cold *reconstruction* re-running the recipe's
# read_project_file over the live file. These pin the closure for both #74 hazards: a
# cheap entry's cold read, and an expensive entry self-healing an evicted snapshot.
# ---------------------------------------------------------------------------


def _write_ints(path, values) -> None:
    import pandas as pd

    pd.DataFrame({"x": list(values)}).to_parquet(path)


def _src_read_code(project: str, rel: str) -> str:  # cheap: a bare project read
    return f"from tallyman_xorq.io import read_project_file\nexpr = read_project_file({rel!r}, project={project!r})\n"


def _src_group_code(project: str, rel: str) -> str:  # expensive: group_by → Aggregate → baked snapshot
    # order_by('x') pins a deterministic row order so the #83 result_digest is stable
    # build-to-self-heal (an unordered aggregate would reshuffle and read as drift).
    return (
        "from tallyman_xorq.io import read_project_file\n"
        f"t = read_project_file({rel!r}, project={project!r})\n"
        "expr = t.group_by('x').aggregate(c=t.count()).order_by('x')\n"
    )


def test_cas_cold_read_after_inplace_edit_serves_built_bytes(project, monkeypatch):
    """#115: a cheap entry's cold read serves the rows it was built from, even after
    the source is edited in place.

    Pre-#115 the cold read re-imports the recipe and re-runs read_project_file, which under
    cas re-digests the now-edited live file and clones it under a NEW digest — serving
    the edited rows under the entry's ORIGINAL content_hash. Digest-pinned
    reconstruction resolves to the frozen .cas clone the entry recorded instead.
    """
    from tallyman_core import data_dir
    from tallyman_xorq import build_and_persist
    from tallyman_xorq import source_identity as si
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")  # several sibling tests force off
    assert si.mode() == "cas"

    src = data_dir(project) / "src.parquet"
    _write_ints(src, [0, 1, 2])
    h = build_and_persist(project, _src_read_code(project, "src.parquet")).content_hash

    # Edit the source in place AFTER building. The 3→5 row edit changes the file
    # size, so the stat-keyed digest memo invalidates without TALLYMAN_SOURCE_REHASH.
    _write_ints(src, [0, 1, 2, 3, 4])
    cached_result_expr.cache_clear()  # force a genuinely COLD read (re-import the recipe)

    df = cached_result_expr(project, h).execute()
    # Assert on VALUES, not just the count, so a fix that read some other 3-row file
    # can't false-green: only the frozen clone holds exactly {0,1,2} now.
    assert sorted(df["x"].tolist()) == [0, 1, 2]


def test_cas_expensive_self_heal_after_edit_serves_built_bytes(project, monkeypatch, caplog):
    """#115: an expensive entry self-healing an evicted snapshot recomputes from the
    FROZEN source, not the edited live file.

    The self-heal re-imports the recipe (cold _resolve_result_plan) and re-executes
    the baked op; pre-#115 that recompute read the edited source, so the snapshot
    self-healed to different bytes under the original content_hash — exactly the #83
    UNFAITHFUL signal. Pinned reconstruction reads the frozen clone, so the heal is
    faithful and the warning does not fire.
    """
    import logging

    from tallyman_core import data_dir
    from tallyman_xorq import build_and_persist
    from tallyman_xorq import source_identity as si
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    assert si.mode() == "cas"

    src = data_dir(project) / "src.parquet"
    _write_ints(src, [0, 1, 2])
    h = build_and_persist(project, _src_group_code(project, "src.parquet")).content_hash

    # Capture the snapshot path BEFORE editing. Post-edit, baked_snapshot_path
    # re-imports the recipe and (on current code) re-digests the edited source, so it
    # returns a NEW, non-existent path — unlinking that would error in setup, not test
    # the fix.
    p = baked_snapshot_path(project, h)
    assert p is not None and p.exists()

    _write_ints(src, [0, 1, 2, 3, 4])  # edit the source in place
    p.unlink()  # evict the baked snapshot → the next read must self-heal
    cached_result_expr.cache_clear()

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
        df = cached_result_expr(project, h).execute()

    # group_by('x') over the frozen {0,1,2} → 3 groups; the edited {0,1,2,3,4} would
    # give 5. The self-heal must recompute from the frozen clone.
    assert sorted(df["x"].tolist()) == [0, 1, 2]
    assert p.exists()  # the heal rewrote the ORIGINAL-keyed snapshot, not a drifted one
    # The #83 faithfulness check must NOT fire: the recompute matches the build digest
    # because it read the frozen bytes (pre-fix it self-healed to edited bytes and
    # logged UNFAITHFUL).
    msgs = [r.getMessage() for r in caplog.records if r.name == "tallyman.perf"]
    assert not any("UNFAITHFUL" in m for m in msgs), msgs


def test_cas_nested_chain_reconstruction_pins_each_entrys_sources(project, monkeypatch):
    """#115: nested reconstruction restores the per-entry source map across the chain.

    B unions tracked_expr_from_alias(A) with its own read_project_file(b). A cold read of B
    reconstructs A — whose read_project_file must resolve to A's recorded a.parquet — then
    reads B's own recorded b.parquet. The frozen-source contextvar is overwritten for
    A's reconstruction and restored for B's remaining reads, so each read_project_file
    pins ITS OWN built bytes even after both sources are edited in place.
    """
    from tallyman_core import data_dir
    from tallyman_xorq import source_identity as si
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    assert si.mode() == "cas"

    srca = data_dir(project) / "a.parquet"
    srcb = data_dir(project) / "b.parquet"
    _write_ints(srca, [0, 1, 2])
    _write_ints(srcb, [10, 11])

    catalog_create("pa", _src_read_code(project, "a.parquet"))  # cheap parent over a.parquet
    b_code = (
        "from tallyman_xorq.io import read_project_file, tracked_expr_from_alias\n"
        "a = tracked_expr_from_alias('pa')\n"
        f"b = read_project_file('b.parquet', project={project!r})\n"
        "expr = a.union(b)\n"
    )
    res = catalog_create("pb", b_code)
    assert "error" not in res, res
    b_h = _hash_of(project)

    _write_ints(srca, [0, 1, 2, 3, 4])  # edit BOTH sources in place after building
    _write_ints(srcb, [10, 11, 12])
    cached_result_expr.cache_clear()

    df = cached_result_expr(project, b_h).execute()
    # Frozen a = {0,1,2} ∪ frozen b = {10,11}. A leaked contextvar would serve the
    # edited {0,1,2,3,4} and/or {10,11,12}.
    assert sorted(df["x"].tolist()) == [0, 1, 2, 10, 11]


def test_cas_child_records_reconstructed_parent_frozen_digest(project, monkeypatch):
    """#115: building a child records the parent's FROZEN source digest, keeping the
    parent's .cas clone alive for GC.

    Building a child reconstructs the parent's recipe, re-running the parent's
    read_project_file. That must record the parent's frozen digest (the bytes actually
    read) into the child's manifest.sources — not a re-digest of the edited live file
    — so gc_cas keeps the frozen clone the child's reconstruction depends on.
    """
    from tallyman_core import data_dir, entry_dir, read_manifest
    from tallyman_xorq import source_identity as si

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    assert si.mode() == "cas"

    src = data_dir(project) / "src.parquet"
    _write_ints(src, [0, 1, 2])
    catalog_create("pp", _src_read_code(project, "src.parquet"))
    p_h = _hash_of(project)
    p_digest = read_manifest(entry_dir(project, p_h)).sources["src.parquet"]

    _write_ints(src, [0, 1, 2, 3, 4])  # edit in place, then build a child off the parent
    cc_code = (
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        "t = tracked_expr_from_alias('pp')\nexpr = t.mutate(y=t.x * 2)\n"
    )
    res = catalog_create("cc", cc_code)
    assert "error" not in res, res
    c_h = _hash_of(project)

    child_sources = read_manifest(entry_dir(project, c_h)).sources or {}
    assert child_sources.get("src.parquet") == p_digest
    # Sanity: the frozen digest the child pinned is NOT the edited live digest.
    assert si.digest_for(project, src) != p_digest


def test_off_built_entry_reads_under_cas_default_without_crash(project, monkeypatch):
    """#115 graceful degradation: an entry built under mode=off (no recorded sources)
    read under the cas default falls through to a live read, not a crash.

    The digest-pinned hook is a no-op when manifest.sources is None — there is no
    frozen clone to pin — so reconstruction reads the live source as before. (Per the
    single-user rebuild-always policy, rebuilding such an entry under cas is the path
    to pinning; this only pins the no-crash contract.)
    """
    from tallyman_core import data_dir, entry_dir, read_manifest
    from tallyman_xorq import build_and_persist
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = data_dir(project) / "src.parquet"
    _write_ints(src, [0, 1, 2])

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    h = build_and_persist(project, _src_read_code(project, "src.parquet")).content_hash
    assert read_manifest(entry_dir(project, h)).sources is None  # off records no sources

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")  # flip to the default
    cached_result_expr.cache_clear()
    df = cached_result_expr(project, h).execute()  # must not raise; reads the live source
    assert sorted(df["x"].tolist()) == [0, 1, 2]


def test_reset_to_clears_result_plan_memo(project, orders_parquet, monkeypatch):
    """reset_to retires the on-disk compute cache to the target revision's warm
    set; it must also invalidate the in-process ``_resolve_result_plan`` memo,
    which holds reconstructed plans — including the baked snapshot paths the
    prune can delete (#80 / #96). A reactive reset layer needs this cache
    invalidated alongside the prune, not left to ``cached_result_expr``'s
    per-call self-heal: ``_build_compare_expr`` bakes a cached snapshot path with
    no per-call existence re-check, so a stale plan compounds downstream.
    """
    from tallyman_core import catalog_state as cs
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)
    step = cs.current_step(project)

    cached_result_expr(project, agg_h).execute()  # warm the memo
    assert cached_result_expr.cache_info().currsize >= 1

    cs.reset_to(project, step)
    assert cached_result_expr.cache_info().currsize == 0


def test_reset_endpoint_clears_compare_expr_memo(fresh_companion_app, project, orders_parquet, monkeypatch):
    """The diff compare-expr LRU (``_build_compare_expr``) caches a serialized
    build that bakes in each entry's baked snapshot path and — unlike
    ``cached_result_expr`` — never re-checks ``path.exists()`` on a hit. A reset
    that prunes a snapshot would otherwise leave that cached build pointing at a
    deleted parquet, so ``/api/reset`` must clear it alongside the result-plan
    memo (#80).
    """
    from fastapi.testclient import TestClient

    from tallyman_companion.app import _build_compare_expr
    from tallyman_core import catalog_state as cs
    from tallyman_core.aliases import history_for

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    step = cs.current_step(project)
    catalog_revise("shoe_sales", _agg_avg_code(project))
    a_h, b_h = history_for(project, "shoe_sales")

    _build_compare_expr(project, a_h, b_h, ("region",))  # warm the compare memo
    assert _build_compare_expr.cache_info().currsize == 1

    c = TestClient(fresh_companion_app)
    r = c.post(f"/{project}/api/reset", json={"ref": step})
    assert r.status_code == 200, r.text
    assert _build_compare_expr.cache_info().currsize == 0


def test_notify_project_reset_evicts_compare_build_over_pruned_snapshot(
    fresh_companion_app, project, orders_parquet, monkeypatch
):
    """Load-bearing #80/#96 regression — stronger than ``currsize == 0``: it
    proves the cross-process ``project_reset`` notify evicts a compare build that
    would otherwise *execute over a deleted snapshot*, not merely that the LRU is
    emptied.

    ``reset-to`` is normally run out of process (the ``tallyman reset-to`` CLI):
    the CLI runs ``reset_to`` — which prunes snapshots — in its own short-lived
    process, then signals the long-lived companion via ``POST /internal/notify
    {kind: project_reset}``. ``_build_compare_expr`` froze each entry's baked
    snapshot path into a serialized build with no per-call ``exists()`` recheck,
    so the companion's warm build now points at a parquet the prune deleted.
    Executing that stale build raises ``ValueError: At least one path is
    required`` (the #73/#74 dangling-read symptom). ``reset_to`` itself clears
    only ``cached_result_expr`` (which self-heals anyway), never this
    companion-layer LRU — so without the notify-path clear the gap is real and
    the next diff view serves the broken build to buckaroo.

    The flow mirrors production: ``cs.reset_to`` stands in for the CLI process's
    prune (it does NOT reach the companion LRU), then the ``/internal/notify``
    POST is the signal that must close the gap.
    """
    import pytest
    from fastapi.testclient import TestClient
    from xorq.ibis_yaml.compiler import load_expr

    from tallyman_companion.app import _build_compare_expr
    from tallyman_core import catalog_state as cs
    from tallyman_core.aliases import history_for
    from tallyman_xorq.result_cache import baked_snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    step = cs.current_step(project)  # only v1 is warm at this step
    catalog_revise("shoe_sales", _agg_avg_code(project))
    a_h, b_h = history_for(project, "shoe_sales")

    build_path, _ = _build_compare_expr(project, a_h, b_h, ("region",))  # warm + freeze b_h's path
    snap_b = baked_snapshot_path(project, b_h)  # capture BEFORE the prune deletes it
    assert snap_b is not None and snap_b.exists()

    # Out-of-process CLI work: reset_to prunes (retires b_h's snapshot) but does
    # not reach the companion's _build_compare_expr LRU — the stale build stays.
    cs.reset_to(project, step)
    assert not snap_b.exists()  # the prune deleted b_h's snapshot
    assert _build_compare_expr.cache_info().currsize == 1  # stale build survives reset_to

    # The stale build is genuinely broken: it executes over the deleted parquet.
    with pytest.raises(ValueError, match="At least one path is required"):
        load_expr(str(build_path)).execute()

    # The notify signal closes the gap — it must evict that broken build so the
    # next /api/diff_data can't POST a dangling build_dir to buckaroo.
    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "project_reset", "project": project})
    assert r.status_code == 200, r.text
    assert _build_compare_expr.cache_info().currsize == 0


def test_notify_project_reset_clears_result_plan_memo(fresh_companion_app, project, orders_parquet, monkeypatch):
    """Parity with the in-server reset: the cross-process ``project_reset`` notify
    clears the companion's ``cached_result_expr`` memo too. Hygiene rather than
    correctness — ``cached_result_expr`` self-heals on every read — but both reset
    paths share one invalidation helper, so the notify path clears it alongside
    the compare-expr LRU (#80).
    """
    from fastapi.testclient import TestClient

    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)

    cached_result_expr(project, agg_h).execute()  # warm the memo
    assert cached_result_expr.cache_info().currsize >= 1

    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "project_reset", "project": project})
    assert r.status_code == 200, r.text
    assert cached_result_expr.cache_info().currsize == 0


def test_result_digest_is_framing_independent_and_order_sensitive(project, orders_parquet, monkeypatch):
    # #83: the digest hashes row tuples in order, not the arrow IPC framing, so two
    # reads that batch differently agree — but it IS sensitive to row order, because
    # an unordered query that reshuffles produces different materialised bytes, which
    # is real drift, not a false positive.
    from tallyman_xorq.result_cache import _digest_init, _digest_update, cached_result_expr, result_digest

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)

    expr = cached_result_expr(project, h)
    base = result_digest(expr)

    # Framing independence: digest the SAME rows re-batched at two different chunk
    # sizes and assert equality — the digest must hash row tuples, not IPC framing.
    tbl = expr.to_pyarrow()

    def _digest_at(chunk):
        hh = _digest_init(expr.schema())
        for b in tbl.to_batches(max_chunksize=chunk):
            _digest_update(hh, b)
        return hh.hexdigest()

    assert _digest_at(7) == _digest_at(50) == base

    # Order sensitivity: reversing the row order changes the digest (real drift —
    # an unordered reshuffle materialises different bytes, not a false positive).
    first_col = list(expr.schema().names)[0]
    reordered = expr.order_by([expr[first_col].desc()])
    assert result_digest(reordered) != base


def _literal_nondeterministic_code(project: str) -> str:
    # A Python-level nondeterministic literal (random.random()) baked at author
    # time: re-importing expr.py re-evaluates it, so the reconstructed graph — and
    # its content_hash — moves each time. NOT an ibis op, so the build-time op lint
    # (#88 part 1) can't see it; the structural runtime detector (#88 part 2) must.
    return (
        "import random\n"
        "from tallyman_xorq.io import read_project_file\n"
        f"t = read_project_file('orders.parquet', project={project!r})\n"
        "expr = t.group_by('region').aggregate(total=t.price.sum()).mutate(nonce=random.random())\n"
    )


def test_structural_nondeterminism_detected_for_baked_literal(project, orders_parquet, monkeypatch):
    # #88 part 2: the structural case the op lint is blind to. Two reconstructions of
    # a recipe that bakes a random literal at author time hash differently, so the
    # entry's structural content_hash is itself unstable — distinct from an
    # execution-nondeterministic recipe whose graph is fixed.
    from tallyman_xorq.result_cache import recipe_is_structurally_nondeterministic

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("nd", _literal_nondeterministic_code(project))
    h = _hash_of(project)
    assert recipe_is_structurally_nondeterministic(project, h) is True


def test_deterministic_recipe_is_not_structurally_nondeterministic(project, orders_parquet, monkeypatch):
    # #88 part 2: a deterministic recipe reconstructs to the same graph hash both
    # times, so the structural detector does not false-positive on a clean entry —
    # its drift, if any, is execution-level (#83), not structural.
    from tallyman_xorq.result_cache import recipe_is_structurally_nondeterministic

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    assert recipe_is_structurally_nondeterministic(project, h) is False


def test_deterministic_udf_entry_is_not_structurally_nondeterministic(project, orders_parquet, monkeypatch):
    # #88 part 2: a deterministic UDF recipe must NOT be flagged structural. xorq's
    # make_pandas_udf mints a fresh class per reconstruction (see
    # test_scalar_udf_is_worthy_in_lockstep_with_classify_build), so two reconstructions
    # of even a pure UDF recipe hash differently — graph-hash instability from the
    # class mint, NOT an author-time baked literal. Attributing that as structural is
    # wrong: impure-UDF nondeterminism is execution-level (#83). The detector must
    # exclude UDF entries so a UDF self-heal drift is labeled execution, not structural.
    from tallyman_xorq.result_cache import recipe_is_structurally_nondeterministic

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("udf", _scalar_udf_code(project))
    h = _hash_of(project)
    assert recipe_is_structurally_nondeterministic(project, h) is False


def test_self_heal_warning_labels_structural_for_baked_literal(project, orders_parquet, monkeypatch, caplog):
    # #88 part 2, end to end: an entry that bakes a random literal reconstructs to a
    # graph whose snapshot key differs from the build-time one, so its very first cold
    # read self-heals to bytes that differ from the recorded digest. The faithfulness
    # warning must fire AND attribute the drift to the structural axis (#88), not
    # execution — the literal genuinely moves the graph hash between imports.
    import logging

    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("nd", _literal_nondeterministic_code(project))  # expensive (Aggregate) → baked
    h = _hash_of(project)
    cached_result_expr.cache_clear()

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
        df = cached_result_expr(project, h).execute()  # reconstructs a new graph → self-heals
    assert len(df) > 0
    msgs = [r.getMessage() for r in caplog.records if r.name == "tallyman.perf"]
    assert any("UNFAITHFUL" in m and h in m and "structural (#88)" in m for m in msgs), msgs
