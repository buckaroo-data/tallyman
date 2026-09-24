from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tallyman_core.paths import catalog_dir, entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.result_cache import cache_worthy

ORDERS_SRC = "orders_src"  # the source alias conftest's ``orders_src`` fixture imports the shoe-orders file under


def _agg_code(project: str) -> str:  # Aggregate → expensive → cache-worthy
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _project_code(project: str) -> str:  # source read + projection → cheap (keeps __row_order, ADR-008 D3)
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r})
expr = t.select("region", "price", "__row_order")
"""


def _parent_code(project: str, col: str) -> str:  # single-column rename → distinct, unionable
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r})
expr = t.select("__row_order", k=t.{col})
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
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r})
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
from tallyman_xorq.io import tracked_expr_from_alias
from xorq.expr.udf import make_pandas_udf
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.vendor.ibis import schema as ibis_schema

t = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r})


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
    """The most recently written built entry.

    An import is an entry too (ADR-011 D1), so skip the ones whose manifest records one: every project
    here carries the ``orders_src`` version, and a test always means the entry it just created.
    """
    for manifest in list_entries(project):
        if not manifest.get("provenance"):
            return manifest["content_hash"]
    raise AssertionError(f"{project!r} has no built entry, only imported sources")


def test_classifier_skips_cheap_caches_expensive(project, orders_src, monkeypatch):
    # The verdict is decided once, at build, on the live expression (ADR-008 D4) and read from the manifest ever after.
    from tallyman_core import read_manifest

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)
    assert cache_worthy(project, agg_h) is True
    assert "Aggregate" in read_manifest(entry_dir(project, agg_h)).cache_worthy_why

    catalog_create("proj", _project_code(project))
    proj_h = _hash_of(project)
    assert cache_worthy(project, proj_h) is False
    assert "cheap" in read_manifest(entry_dir(project, proj_h)).cache_worthy_why


@pytest.mark.parametrize("snapshot_on_disk", [True, False], ids=["snapshot-on-disk", "no-snapshot"])
@pytest.mark.parametrize("kind", ["worthy", "cheap"])
def test_cache_worthy_refuses_an_entry_with_no_manifest(project, orders_src, monkeypatch, kind, snapshot_on_disk):
    """#204: the verdict is the manifest's (ADR-008 D4), and a directory without a manifest is not an entry (ADR-007
    D6), so ``cache_worthy`` refuses one and names the rebuild. It used to answer with whether a file sat at the
    snapshot path: a worthy entry that had lost both was reported cheap, and a cheap entry with a file at that path
    was reported worthy."""
    import shutil

    from tallyman_core.aliases import get_alias
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create(kind, _agg_code(project) if kind == "worthy" else _project_code(project))
    h = _hash_of(project)
    snap = snapshot_path(project, h)
    if snapshot_on_disk and not snap.exists():
        shutil.copy(snapshot_path(project, get_alias(project, ORDERS_SRC)), snap)
    if not snapshot_on_disk:
        snap.unlink(missing_ok=True)
    (entry_dir(project, h) / "manifest.json").unlink()

    with pytest.raises(BuildError, match="has no manifest.json") as info:
        cache_worthy(project, h)
    assert "expr.py" in str(info.value), "the refusal names the recipe to run again"


def test_cache_worthy_refuses_a_hash_whose_entry_dir_is_gone(project, orders_src, monkeypatch):
    """#204: a reset that retires an entry parks its directory in the bullpen and leaves its snapshot on disk
    (ADR-007 D14). The file is still there, but the hash names no entry of this catalog, so nothing reads it as one.
    ``cache_worthy`` used to report it worthy because the file existed, and ``cached_result_expr`` served it."""
    import shutil

    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import snapshot_path
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    shutil.rmtree(entry_dir(project, h))
    assert snapshot_path(project, h).is_file()

    for read in (cache_worthy, cached_result_expr):
        with pytest.raises(BuildError, match=f"no entry {h}"):
            read(project, h)


def test_cache_worthy_is_the_manifests_verdict_whatever_is_at_the_snapshot_path(project, orders_src, monkeypatch):
    """#204, the complete-entry half: a worthy entry whose snapshot was deleted is still worthy, and a cheap entry with
    a file at its snapshot path is still cheap. The file says nothing about the verdict (ADR-008 D4)."""
    import shutil

    from tallyman_core.aliases import get_alias
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    worthy = _hash_of(project)
    catalog_create("proj", _project_code(project))
    cheap = _hash_of(project)

    snapshot_path(project, worthy).unlink()
    shutil.copy(snapshot_path(project, get_alias(project, ORDERS_SRC)), snapshot_path(project, cheap))

    assert cache_worthy(project, worthy) is True
    assert cache_worthy(project, cheap) is False


def test_cheap_entry_writes_no_result_parquet(project, orders_src, monkeypatch):
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


def test_expensive_entry_bakes_result_cache(project, orders_src, monkeypatch):
    # An expensive entry is materialised when it is created (ADR-007 D4): its snapshot is written to the compute cache
    # — not a build-time entry result.parquet, not the retired catalog-level result_cache dir.
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    assert not (entry_dir(project, h) / "result.parquet").exists()
    # The result_cache/ dir was retired in #73 — snapshots live in the compute cache.
    assert not (catalog_dir(project) / "result_cache").exists()
    # The entry has a snapshot (its path is a function of the content hash)...
    assert baked_snapshot_path(project, h) is not None
    # ...and cached_result_expr reads that snapshot as a single-backend deferred read: a bare Read on the default
    # backend composes cleanly into a union/join (#75), where a xorq cache node carried its own storage backend.
    ce = cached_result_expr(project, h)
    assert type(ce.op()).__name__ == "Read"
    assert len(ce._find_backends()[0]) == 1
    assert any(compute_cache_dir(project).rglob("result_cache/*.parquet"))
    # Reading executes against the baked snapshot; no per-entry result.parquet.
    assert len(ce.execute()) > 0
    assert not (entry_dir(project, h) / "result.parquet").exists()


def test_scalar_udf_is_worthy(project, orders_src, monkeypatch):
    # #81: a scalar UDF used without any accompanying expensive op must be judged
    # worthy. A make_pandas_udf node is classed after the user's function (here
    # `plusone`), so its live leaf name carries no "UDF"; it is matched by base class
    # (its MRO holds ScalarUDF). Two predicates used to have to be kept in lockstep
    # (the YAML regex and the live walk); ADR-008 D4 retired the regex, so the one
    # classifier runs on the live expression, and its recorded verdict names the UDF.
    from tallyman_core import read_manifest
    from tallyman_xorq.result_cache import _recipe_expr
    from tallyman_xorq.worthiness import classify_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("udf", _scalar_udf_code(project))
    h = _hash_of(project)
    assert "udf:" in read_manifest(entry_dir(project, h)).cache_worthy_why
    assert cache_worthy(project, h) is True
    assert classify_expr(_recipe_expr(project, h)).worthy is True


def test_scalar_udf_only_entry_bakes_result_cache(project, orders_src, monkeypatch):
    # #81 net effect: a scalar-UDF-only expression that was judged cheap wrote no snapshot, so every viewer / diff /
    # tracked_expr_from_alias read recomputed the UDF over the whole DAG (a silent perf regression). With the UDF
    # judged worthy the entry is materialised like any other expensive entry and cached_result_expr reads a
    # single-backend snapshot. Mirrors test_expensive_entry_bakes_result_cache.
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("udf", _scalar_udf_code(project))
    h = _hash_of(project)

    # The entry has a snapshot (a UDF judged cheap wrote none).
    assert baked_snapshot_path(project, h) is not None

    # cached_result_expr reads a single-backend snapshot; a snapshot parquet exists on disk.
    ce = cached_result_expr(project, h)
    assert type(ce.op()).__name__ == "Read"
    assert len(ce._find_backends()[0]) == 1
    assert "qty_plus" in ce.columns
    assert len(ce.execute()) > 0
    assert any(compute_cache_dir(project).rglob("result_cache/*.parquet"))


def test_cheap_entry_cached_result_expr_is_not_a_cache_node(project, orders_src, monkeypatch):
    # A cheap entry recomputes on read — its expression is not a CachedNode.
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    assert type(cached_result_expr(project, h).op()).__name__ != "CachedNode"


def test_cold_read_logs_path_and_wall_time(project, orders_src, monkeypatch, caplog):
    # #87: each cold read is visible on tallyman.perf with wall-clock, so a Join-descendant that is structurally worthy
    # but cost-cheap is visible from the read path, not just lifecycle counts. A worthy entry whose snapshot exists is
    # served without loading its build (ADR-007 D2), so it logs no cold read; a cheap entry's cold read loads its
    # plan; a deleted snapshot's heal is logged.
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

    # Worthy entry with a live snapshot → a bare read, no build loaded, nothing to log.
    msgs = perf_tags(lambda: cached_result_expr(project, agg_h))
    assert not any("cold read" in m and agg_h in m for m in msgs)

    # Cheap entry → its plan is loaded, with wall-clock.
    msgs = perf_tags(lambda: cached_result_expr(project, proj_h))
    assert any("cold read" in m and proj_h in m and "wall_ms" in m for m in msgs)

    # A deleted snapshot is made again (run once, then read) on the next read, and the heal is logged.
    p = baked_snapshot_path(project, agg_h)
    assert p is not None and p.exists()
    p.unlink()
    msgs = perf_tags(lambda: cached_result_expr(project, agg_h))
    assert any("healed" in m and agg_h in m for m in msgs)


def test_multi_parent_tracked_expr_from_alias_shares_one_backend(project, orders_src, monkeypatch):
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


def test_multi_parent_tracked_expr_from_alias_union_builds(project, orders_src, monkeypatch):
    # #75: an entry that unions two tracked_expr_from_alias parents must build end-to-end.
    # On the #73 branch this aborted with BuildError: Multiple backends found.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))
    catalog_create("pb", _parent_code(project, "category"))

    res = catalog_create("ab", _union_code("pa", "pb"))
    assert "error" not in res, res
    # 200 source rows per parent → 400 unioned.
    assert res["row_count"] == 400


def _mix_code(project: str, parent: str) -> str:  # a source parent + a cheap parent in one expression
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
fp = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r}).select(k="region")
fc = tracked_expr_from_alias({parent!r}).select(k="k")
expr = fp.union(fc)
"""


def test_source_and_cheap_parent_mix_shares_one_backend(project, orders_src, monkeypatch):
    # #75: the two ways a parent resolves must meet on one backend. A source version is worthy, so it
    # comes back as a bare read of its snapshot; a cheap parent comes back as its own frozen graph.
    # #73 rooted the raw read on the default backend but loaded a parent into its own, so the mix
    # raised "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import tracked_expr_from_alias

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))

    fp = tracked_expr_from_alias(orders_src, project=project).select(k="region")
    fc = tracked_expr_from_alias("pa").select(k="k")
    ibis.union(fp, fc)._find_backend()  # must not raise


def test_source_and_cheap_parent_mix_builds(project, orders_src, monkeypatch):
    # #75: an entry mixing a source parent and a cheap parent must build end-to-end.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("pa", _parent_code(project, "region"))

    res = catalog_create("mix", _mix_code(project, "pa"))
    assert "error" not in res, res
    # 200 rows from the source version + 200 from the cheap parent over it.
    assert res["row_count"] == 400


def test_expensive_tracked_expr_from_alias_mix_shares_one_backend(project, orders_src, monkeypatch):
    # #75: an expensive parent resolves to a deferred read of its baked snapshot —
    # a bare Read on the default backend — so mixing it with a fresh aggregate over
    # the source stays on one backend. A baked CachedNode (the #73 form) would have
    # dragged its own storage backend in and re-raised "Multiple backends found".
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import tracked_expr_from_alias

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))  # Aggregate → expensive → baked snapshot

    ft = tracked_expr_from_alias(orders_src, project=project)
    fp = ft.group_by("region").aggregate(total=ft.price.sum(), n=ft.count())
    fc = tracked_expr_from_alias("agg").drop("__row_order")  # region, total, n — same schema as fp
    ibis.union(fp, fc)._find_backend()  # must not raise


def test_diff_route_survives_cold_cache(fresh_companion_app, project, orders_src, monkeypatch):
    # The diff route composes both entries' reads; on a cold compute cache it must make the files they read exist (the
    # source version's snapshot) rather than blank the diff (there is no per-entry result.parquet to evict any more).
    import shutil

    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    catalog_revise("proj", _project_code(project).replace('select("region", "price", ', 'select("region", '))
    # Cold cache: wipe the compute cache and clear the reconstruction lru.
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/diff_data/proj/1/2")
    assert r.status_code == 200
    diff = r.json()["diff"]
    assert "stats" in diff or "keyed" in diff


def test_cached_result_expr_self_heals_expensive_parent_chain_on_cold_cache(project, orders_src, monkeypatch):
    """#73/#74: reading an entry that ``tracked_expr_from_alias``s an expensive parent must
    make an evicted parent snapshot exist again, not error.

    An expensive parent is materialised into the per-project compute cache; a child that chains off it serialises a
    *bare* ``deferred_read_parquet`` of that snapshot (ADR-007 D3). On a cold compute cache (fresh clone /
    write-isolated overlay) that bare read would resolve to zero files (``ValueError: At least one path is
    required``). ``cached_result_expr`` calls ``ensure_materialized`` first (ADR-007 D5): it recurses on the hash in
    the parent's snapshot path, rewrites the parent from its own frozen build, verifies it, and only then hands back
    the child's plan.
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


def test_cached_result_expr_self_heals_multi_parent_expensive_join_on_cold_cache(project, orders_src, monkeypatch):
    """#73/#75: a multi-parent entry joining two expensive ``tracked_expr_from_alias`` parents
    must make *both* evicted parent snapshots exist again on a cold cache.

    This is the ``tickets_and_ghosts`` shape: a join of two expensive parents.
    The child is itself expensive (Join), so its plan joins two *bare* snapshot reads. On a cold cache the child's
    snapshot is missing and so are both parents' → ``ValueError: At least one path is required`` if anything ran
    first. ``ensure_materialized`` collects the two files the child's plan reads and rewrites each from its own frozen
    build before the child is written.
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


def test_revise_in_place_self_reference_terminates(project, orders_src, monkeypatch):
    """A revise-in-place recipe that reads ``tracked_expr_from_alias`` of its OWN alias must
    not self-recurse forever when later reconstructed (the recipe re-import that remains).

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

    # Correct read == v1's rows plus the mutated column.
    assert "doubled" in expr.schema().names
    assert expr.execute().shape[0] == cached_result_expr(project, v1).execute().shape[0]

    # A read loads the frozen build and never re-imports the recipe (#163), so the recursion can only be reached
    # through the recipe re-import that survives as a diagnostic (recipe_is_structurally_nondeterministic): that is
    # where a self-referential recipe must still resolve to the build-time parent.
    from tallyman_xorq.result_cache import _recipe_expr

    recipe_expr = _call_under_tight_recursion_guard(_recipe_expr, project, v2)
    assert "doubled" in recipe_expr.schema().names


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


def test_recipe_reconstruction_does_not_leak_sys_modules(project, orders_src, monkeypatch):
    # Recipe reconstruction (_recipe_expr -> _import_script) survives as a diagnostic (the structural-nondeterminism
    # check, which re-imports the recipe twice); it was a per-READ operation under #73. _import_script registers the
    # recipe module in sys.modules under a unique uuid name; left there, every reconstruction leaks a module object
    # that pins its whole expression graph. Reconstructing the same entry repeatedly must not accumulate sys.modules
    # entries.
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


def test_ensure_result_is_removed():
    # The on-demand result.parquet writer is deleted entirely: every consumer
    # now reads cached_result_expr (cheap plan / expensive snapshot),
    # so nothing materialises a per-entry result.parquet on demand. The #102
    # viewer-from-result.parquet build helpers go with it — the viewer pages over
    # the snapshot (expensive) or a small plan over files that exist (cheap), never a result.parquet.
    import tallyman_xorq.result_cache as rc

    assert not hasattr(rc, "ensure_result")
    assert not hasattr(rc, "ensure_result_build")
    assert not hasattr(rc, "ensure_viewer_expanded_build")


def test_build_records_stable_result_digest(project, orders_src, monkeypatch):
    # ADR-004-result-digest-canonical-ordering, as redefined by ADR-009 D2: worthy entries record a result_digest, the
    # content digest (arrow-sha256) of the snapshot read back, stable run-to-run because the snapshot is written in a
    # canonical order on a single-partition connection. Cheap entries record no digest — they have no snapshot to hash.
    from tallyman_core import entry_dir, read_manifest
    from tallyman_xorq.result_cache import baked_snapshot_path, snapshot_file_digest

    monkeypatch.setenv("TALLYMAN_PROJECT", project)

    # Worthy entry (Aggregate): must record a digest equal to the snapshot's content digest.
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    digest = read_manifest(entry_dir(project, h)).result_digest
    assert digest, "worthy entry recorded no result_digest"
    assert digest.startswith("arrow-sha256:"), digest
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    assert snapshot_file_digest(snap) == digest, "recorded digest does not match the snapshot's content digest"

    # Cheap entry (projection only): must record NO digest.
    catalog_create("proj", _project_code(project))
    h2 = _hash_of(project)
    cheap_digest = read_manifest(entry_dir(project, h2)).result_digest
    assert not cheap_digest, f"cheap entry unexpectedly recorded result_digest: {cheap_digest!r}"


def test_verify_result_faithful_true_for_deterministic_entry(project, orders_src, monkeypatch):
    # ADR-004-result-digest-canonical-ordering: for a worthy (snapshot-writing) entry,
    # verify_result_faithful compares the snapshot's content digest to the recorded
    # digest and returns True for a clean, deterministic build. A cheap entry records
    # no digest, so verify_result_faithful returns None for it.
    from tallyman_xorq.result_cache import verify_result_faithful

    monkeypatch.setenv("TALLYMAN_PROJECT", project)

    # Worthy entry: digest recorded at build; verify_result_faithful checks the file.
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    assert verify_result_faithful(project, h) is True

    # Cheap entry: no digest recorded; verify_result_faithful returns None.
    catalog_create("proj", _project_code(project))
    h2 = _hash_of(project)
    assert verify_result_faithful(project, h2) is None


def test_verify_result_faithful_detects_snapshot_drift(project, orders_src, monkeypatch):
    # ADR-004-result-digest-canonical-ordering: verify_result_faithful compares the
    # snapshot's content digest against the recorded digest. For a worthy entry
    # whose snapshot no longer holds the rows it was built with (one value differs),
    # it returns False. A cheap entry always returns None (no digest recorded).
    import pyarrow as pa
    import pyarrow.parquet as pq

    from tallyman_xorq.result_cache import baked_snapshot_path, verify_result_faithful

    monkeypatch.setenv("TALLYMAN_PROJECT", project)

    # Worthy entry: starts faithful.
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    assert verify_result_faithful(project, h) is True

    # Rewrite the snapshot with every count changed — still a valid parquet file, but not the built rows.
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    table = pq.read_table(snap)
    n = table.schema.get_field_index("n")
    counts = pa.array([v + 1 for v in table["n"].to_pylist()], type=table.schema.field("n").type)
    drifted = table.set_column(n, "n", counts)
    pq.write_table(drifted, snap)
    assert verify_result_faithful(project, h) is False

    # Cheap entry: always None — it has no snapshot, so there is nothing to compare.
    catalog_create("proj", _project_code(project))
    h2 = _hash_of(project)
    assert verify_result_faithful(project, h2) is None


def test_self_heal_warns_on_unfaithful_recompute(project, orders_src, monkeypatch, caplog):
    # #83: eviction's load-bearing assumption is that an evicted snapshot recomputes
    # to what was evicted. For an entry whose recompute differs from the digest
    # recorded at build, that is false — the snapshot self-heals to DIFFERENT rows,
    # silently. The faithfulness check fires a tallyman.perf warning when a healed
    # snapshot's digest != the recorded one, and the read still succeeds (the check
    # is advisory, never fatal).
    #
    # Source drift cannot provoke this at all: an entry reads a source version's snapshot, editing the
    # imported file changes nothing, and importing the edited bytes mints a new version (ADR-011 D1,
    # tests/test_source_import.py::test_editing_the_outside_file_after_import_changes_nothing). The
    # mismatch is provoked directly, by recording a digest the frozen build cannot reproduce.
    import json
    import logging

    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))  # expensive → snapshot
    h = _hash_of(project)

    mpath = entry_dir(project, h) / "manifest.json"
    doc = json.loads(mpath.read_text())
    doc["result_digest"] = "arrow-sha256:" + "0" * 64
    mpath.write_text(json.dumps(doc))
    p = baked_snapshot_path(project, h)
    assert p is not None and p.exists()
    p.unlink()
    cached_result_expr.cache_clear()

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
        df = cached_result_expr(project, h).execute()  # self-heals, must not raise
    assert len(df) > 0
    msgs = [r.getMessage() for r in caplog.records if r.name == "tallyman.perf"]
    # The recipe graph is fixed (same expr.py, same read) and the engine versions match the manifest's, so the
    # drift is attributed to the execution axis (#83), not the structural one (#88) or an engine change.
    assert any("UNFAITHFUL" in m and h in m and "execution (#83)" in m for m in msgs), msgs


def test_structural_attribution_degrades_to_not_structural_when_the_manifest_is_unreadable(
    project, orders_src, monkeypatch
):
    # #88 part 2, best-effort contract: the structural-vs-execution attribution must
    # never break the read it only annotates. recipe_is_structurally_nondeterministic
    # reads the entry's recorded worthiness reason from its manifest (the UDF exclusion
    # below keys on it); on a missing or torn manifest that read raises, and the call
    # sits on the self-heal warning path, so an unguarded raise would propagate out of
    # cached_result_expr and kill the read it was only meant to annotate. The predicate
    # must swallow that and degrade to False (not-structural), so a drift is still
    # labeled execution (#83) and the read still succeeds.
    from tallyman_xorq.result_cache import recipe_is_structurally_nondeterministic

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)

    assert recipe_is_structurally_nondeterministic(project, "deadbeefdead") is False  # no such entry
    (entry_dir(project, h) / "manifest.json").write_text("{torn")  # an interrupted write
    assert recipe_is_structurally_nondeterministic(project, h) is False


def test_cached_result_expr_self_heals_after_warm_then_evict(project, orders_src, monkeypatch):
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


def test_concurrent_cold_heal_is_single_flighted(project, orders_src, monkeypatch):
    """Concurrent cold reads of the same expensive entry must single-flight the
    snapshot heal (#79).

    A heal is ``materialize`` under the project's write lock with the existence check repeated inside the lock
    (ADR-007 D4, D11): the first reader writes the file, and the rest wait, find it there and read it. Without that,
    two readers both run the materialisation: in-process they collide on the shared connection (DataFusion
    ``"Already borrowed"``), and across processes they overwrite each other's output. Either surfaces as a 500 from
    ``api_data`` and, swallowed by ``ensure_session``, an empty grid. The heal must run once per eviction.

    The returned expression is built but not executed inside the workers — this
    isolates the *materialisation* race (what #79 is about) from concurrent
    ``.execute()`` on the shared single DataFusion backend, which raises
    ``"Already borrowed"`` for any concurrent read (warm or cold) and is a separate
    backend-thread-safety concern (#118).
    """
    import threading

    import tallyman_xorq.materialize as materialize_module
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    agg_h = _hash_of(project)

    # Warm the plan and learn the result.
    expected = len(cached_result_expr(project, agg_h).execute())

    materializations: list[str] = []
    real_materialize = materialize_module.materialize

    def counting_materialize(*args, **kwargs):
        materializations.append(args[1])
        return real_materialize(*args, **kwargs)

    monkeypatch.setattr(materialize_module, "materialize", counting_materialize)

    def run_round():
        # Evict the snapshot, then fire barrier-synced readers that each trigger the heal (building the expr, not
        # executing it). Any raised exception is the materialisation race; the snapshot must end up written once.
        p = baked_snapshot_path(project, agg_h)
        assert p is not None and p.exists()
        p.unlink()

        errors = []
        barrier = threading.Barrier(4)

        def worker():
            barrier.wait()  # maximise overlap on the heal
            try:
                cached_result_expr(project, agg_h)  # triggers the heal; expr is not executed here
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return errors, p

    # A few rounds widen the window so a one-off scheduling fluke can't green a still-racy heal.
    for round_no in range(1, 4):
        errors, p = run_round()
        assert not errors, f"concurrent cold heal raced: {errors!r}"
        assert p.exists()  # materialised, intact
        assert len(materializations) == round_no, materializations  # once per eviction, not once per reader

    # The healed snapshot reads back correctly (executed sequentially — concurrent
    # .execute() on the shared backend is out of scope for this materialisation test).
    assert len(cached_result_expr(project, agg_h).execute()) == expected


def test_reset_to_clears_result_plan_memo(project, orders_src, monkeypatch):
    """reset_to changes which entries exist (it retires and restores entry dirs, and leaves compute_cache alone,
    ADR-007 D14), so it must also invalidate the in-process ``_resolve_result_plan`` memo, which holds loaded builds
    (#80 / #96). A reactive reset layer needs this cache invalidated alongside the entry sweep, not left to
    ``cached_result_expr``'s per-call ``ensure_materialized``: ``_build_compare_expr`` bakes a snapshot path with no
    per-call existence re-check, so a stale plan compounds downstream.

    A worthy entry whose snapshot exists is served without loading its build, so the memo is warmed through a cheap
    entry, whose plan is loaded on its first read.
    """
    from tallyman_core import catalog_state as cs
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    proj_h = _hash_of(project)
    step = cs.current_step(project)

    cached_result_expr(project, proj_h).execute()  # warm the memo
    assert cached_result_expr.cache_info().currsize >= 1

    cs.reset_to(project, step)
    assert cached_result_expr.cache_info().currsize == 0


def test_reset_endpoint_clears_compare_expr_memo(fresh_companion_app, project, orders_src, monkeypatch):
    """The diff compare-expr LRU (``_build_compare_expr``) caches a serialized
    build that bakes in each entry's snapshot path and — unlike
    ``cached_result_expr`` — never re-checks ``path.exists()`` on a hit. A reset
    changes which entries exist, and a cached compare build over entries it retired is stale,
    so ``/api/reset`` must clear it alongside the result-plan
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


def test_notify_project_reset_evicts_the_compare_build(fresh_companion_app, project, orders_src, monkeypatch):
    """The cross-process ``project_reset`` notify evicts the companion's compare build.

    ``reset-to`` is normally run out of process (the ``tallyman reset-to`` CLI):
    the CLI runs ``reset_to`` in its own short-lived process, then signals the
    long-lived companion via ``POST /internal/notify {kind: project_reset}``.
    ``_build_compare_expr`` froze each entry's snapshot path into a serialized build
    with no per-call ``exists()`` recheck, and ``reset_to`` itself clears only
    ``cached_result_expr``, never this companion-layer LRU — so without the
    notify-path clear the compare build of entries the reset retired stays warm and the next diff view
    serves it to buckaroo (#80/#96).

    The dangling-read half of that regression is gone: a reset no longer deletes snapshots (ADR-007 D14), so the
    stale build still executes over the files that exist. That is why this is hygiene rather than the
    correctness gate it was, and why it pins the memo, not an error.

    The flow mirrors production: ``cs.reset_to`` stands in for the CLI process's
    reset (it does NOT reach the companion LRU), then the ``/internal/notify``
    POST is the signal that must clear it.
    """
    from fastapi.testclient import TestClient
    from xorq.ibis_yaml.compiler import load_expr

    from tallyman_companion.app import _build_compare_expr
    from tallyman_core import catalog_state as cs
    from tallyman_core.aliases import history_for
    from tallyman_xorq.result_cache import baked_snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    step = cs.current_step(project)  # only v1 exists at this step
    catalog_revise("shoe_sales", _agg_avg_code(project))
    a_h, b_h = history_for(project, "shoe_sales")

    build_path, _ = _build_compare_expr(project, a_h, b_h, ("region",))  # warm + freeze b_h's path
    snap_b = baked_snapshot_path(project, b_h)
    assert snap_b is not None and snap_b.exists()

    # Out-of-process CLI work: reset_to retires v2's entry but does not reach the companion's _build_compare_expr LRU,
    # so the stale build stays. It still reads files that exist: the reset left compute_cache alone.
    cs.reset_to(project, step)
    assert snap_b.exists()
    assert _build_compare_expr.cache_info().currsize == 1  # stale build survives reset_to
    assert len(load_expr(str(build_path)).execute()) > 0

    # The notify signal clears it, so the next /api/diff_data builds afresh.
    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "project_reset", "project": project})
    assert r.status_code == 200, r.text
    assert _build_compare_expr.cache_info().currsize == 0


def test_notify_project_reset_clears_result_plan_memo(fresh_companion_app, project, orders_src, monkeypatch):
    """Parity with the in-server reset: the cross-process ``project_reset`` notify
    clears the companion's ``cached_result_expr`` memo too. Hygiene rather than
    correctness — ``cached_result_expr`` makes its files exist on every read — but both reset
    paths share one invalidation helper, so the notify path clears it alongside
    the compare-expr LRU (#80). The memo is warmed through a cheap entry, whose plan is loaded on its first read.
    """
    from fastapi.testclient import TestClient

    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    proj_h = _hash_of(project)

    cached_result_expr(project, proj_h).execute()  # warm the memo
    assert cached_result_expr.cache_info().currsize >= 1

    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "project_reset", "project": project})
    assert r.status_code == 200, r.text
    assert cached_result_expr.cache_info().currsize == 0


def test_result_digest_is_snapshot_content_digest(project, orders_src, monkeypatch):
    # ADR-009 D2 (amending ADR-004-result-digest-canonical-ordering): the result_digest for a worthy entry is the
    # content digest of the snapshot read back (arrow-sha256:<hex>), not a hash of the file's bytes (which moves with
    # the writer's version, the codec and the row-group size) and not a per-row repr() hash. It equals
    # snapshot_file_digest(baked_snapshot_path(...)) and is stable because the snapshot is written in a canonical
    # order.
    from tallyman_core import entry_dir, read_manifest
    from tallyman_xorq.result_cache import baked_snapshot_path, snapshot_file_digest

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)

    recorded = read_manifest(entry_dir(project, h)).result_digest
    assert recorded, "worthy entry must record a result_digest"
    assert recorded.startswith("arrow-sha256:"), recorded

    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    assert snapshot_file_digest(snap) == recorded


def _literal_nondeterministic_code(project: str) -> str:
    # A Python-level nondeterministic literal (random.random()) baked at author
    # time: re-importing expr.py re-evaluates it, so the reconstructed graph — and
    # its content_hash — moves each time. NOT an ibis op, so the build-time op lint
    # (#88 part 1) can't see it; the structural runtime detector (#88 part 2) must.
    # (Within one build the literal is fixed, so the create-time check that runs the
    # frozen build twice, ADR-009 D6, does not flag it either.)
    return (
        "import random\n"
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        f"t = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r})\n"
        "expr = t.group_by('region').aggregate(total=t.price.sum()).mutate(nonce=random.random())\n"
    )


def test_structural_nondeterminism_detected_for_baked_literal(project, orders_src, monkeypatch):
    # #88 part 2: the structural case the op lint is blind to. Two reconstructions of
    # a recipe that bakes a random literal at author time hash differently, so the
    # entry's structural content_hash is itself unstable — distinct from an
    # execution-nondeterministic recipe whose graph is fixed.
    from tallyman_xorq.result_cache import recipe_is_structurally_nondeterministic

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("nd", _literal_nondeterministic_code(project))
    h = _hash_of(project)
    assert recipe_is_structurally_nondeterministic(project, h) is True


def test_deterministic_recipe_is_not_structurally_nondeterministic(project, orders_src, monkeypatch):
    # #88 part 2: a deterministic recipe reconstructs to the same graph hash both
    # times, so the structural detector does not false-positive on a clean entry —
    # its drift, if any, is execution-level (#83), not structural.
    from tallyman_xorq.result_cache import recipe_is_structurally_nondeterministic

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("agg", _agg_code(project))
    h = _hash_of(project)
    assert recipe_is_structurally_nondeterministic(project, h) is False


def test_deterministic_udf_entry_is_not_structurally_nondeterministic(project, orders_src, monkeypatch):
    # #88 part 2: a deterministic UDF recipe must NOT be flagged structural. xorq's
    # make_pandas_udf mints a fresh class per reconstruction (see
    # test_scalar_udf_is_worthy), so two reconstructions
    # of even a pure UDF recipe hash differently — graph-hash instability from the
    # class mint, NOT an author-time baked literal. Attributing that as structural is
    # wrong: impure-UDF nondeterminism is execution-level (#83). The detector must
    # exclude UDF entries so a UDF self-heal drift is labeled execution, not structural.
    from tallyman_xorq.result_cache import recipe_is_structurally_nondeterministic

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("udf", _scalar_udf_code(project))
    h = _hash_of(project)
    assert recipe_is_structurally_nondeterministic(project, h) is False


def test_baked_literal_entry_reads_faithfully_from_frozen_build(project, orders_src, monkeypatch, caplog):
    # Inversion of the pre-#163 behavior this test used to pin. On main, reading
    # an entry whose recipe bakes a random literal re-imported expr.py, re-rolled
    # the literal, derived a *different* snapshot key, and self-healed to bytes
    # that weren't the recorded ones — so the read had to fire an UNFAITHFUL
    # warning with the structural (#88) attribution. Under the contract the read
    # loads the frozen build, where the literal is fixed forever: the original
    # snapshot resolves, nothing heals, no warning fires, and the entry is
    # byte-faithful (I1) despite the structurally nondeterministic recipe. The
    # nondeterminism now surfaces only where it belongs — re-minting — and the
    # detector test above still covers the diagnostic.
    import logging

    from tallyman_xorq.result_cache import cached_result_expr, verify_result_faithful

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("nd", _literal_nondeterministic_code(project))  # expensive (Aggregate) → baked
    h = _hash_of(project)
    cached_result_expr.cache_clear()

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
        df = cached_result_expr(project, h).execute()
    assert len(df) > 0
    assert verify_result_faithful(project, h) is True
    msgs = [r.getMessage() for r in caplog.records if r.name == "tallyman.perf"]
    assert not any("UNFAITHFUL" in m for m in msgs), msgs


# ---------------------------------------------------------------------------
# ADR-004-result-digest-canonical-ordering — new contract tests
# ---------------------------------------------------------------------------


def test_cheap_entry_records_no_digest(project, orders_src, monkeypatch):
    # ADR-004-result-digest-canonical-ordering: a cheap entry (row-preserving over one file)
    # writes no snapshot, so there is nothing to hash. The manifest must record no
    # result_digest (None / falsy).
    from tallyman_core import entry_dir, read_manifest

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("proj", _project_code(project))
    h = _hash_of(project)
    assert not read_manifest(entry_dir(project, h)).result_digest, (
        "cheap entry must record no result_digest"
    )



def _ordered_agg_code(project: str) -> str:
    # Aggregate + explicit order_by: the output is fully deterministic run-to-run.
    # __row_order is present in the raw read but is aggregated away, so the
    # canonical sort has no inherited order to lead with; the explicit order_by is the
    # author's own key, which is kept (ADR-008 D10) — Sort adds a Sort op making it
    # expensive/worthy regardless.
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({ORDERS_SRC!r}, project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count()).order_by("region")
"""


def test_result_digest_stable_run_to_run(project, orders_src, monkeypatch):
    # ADR-004-result-digest-canonical-ordering: the result_digest for a worthy entry
    # with a deterministic (explicitly ordered) result is stable across two independent
    # materialisations. Uses an explicitly-ordered aggregate (Sort op) so the rows are
    # deterministic and the content digest should be identical on self-heal.
    from tallyman_core import entry_dir, read_manifest
    from tallyman_xorq.result_cache import baked_snapshot_path, snapshot_file_digest

    monkeypatch.setenv("TALLYMAN_PROJECT", project)

    catalog_create("agg_ord", _ordered_agg_code(project))
    h = _hash_of(project)
    digest1 = read_manifest(entry_dir(project, h)).result_digest
    assert digest1, "worthy entry must record a result_digest"

    # Delete the snapshot and self-heal to get a second materialisation.
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    snap.unlink()

    from tallyman_xorq.result_cache import cached_result_expr

    cached_result_expr.cache_clear()
    cached_result_expr(project, h).execute()  # forces self-heal → rematerialises snapshot

    assert snap.exists(), "self-heal must rematerialise the snapshot"
    digest2 = snapshot_file_digest(snap)
    assert digest2 == digest1, (
        f"result_digest changed between materialisations: {digest1!r} vs {digest2!r}"
    )
