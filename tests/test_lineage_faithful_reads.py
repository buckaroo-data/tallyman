"""Lineage-faithful reads (#163): a content hash names a fixed result.

These tests encode the invariants of ``docs/system-contract.md`` against the
#163 failure shape — two revisions of one recipe that are distinct entries
only because their recorded parent moved — and are EXPECTED TO FAIL on
current main (TDD: they must be seen failing on CI before the fix lands).

The shape mirrors the taxi2 ``by_hour`` case from the issue:

    trips     v1 = full orders            (a1)
    by_region v1 = child recipe           (b1, parents=[{trips, a1}])
    trips     v2 = boots-only rows        (a2)   revise → auto-recalc mints
    by_region v2 = same recipe bytes      (b2, parents=[{trips, a2}])

After the cascade the ``trips`` head is a2, so b1 is a historical entry whose
recorded parent no longer sits at its alias head. Each test asserts a form of
contract invariant I1: materializing b1 yields b1's bytes — regardless of
where the alias head is now (I4), which consumer asks (I3/I5), or what state
the caches are in (I2).
"""

from __future__ import annotations

import json
import shutil

import pytest
from xorq.ibis_yaml.compiler import load_expr

from tallyman_companion.diff import build_diff_expr
from tallyman_core.aliases import get_alias
from tallyman_core.manifest import read_manifest
from tallyman_core.paths import (
    compute_cache_dir,
    entry_build_dir,
    entry_dir,
    entry_expanded_build_dir,
    entry_stat_cache_dir,
    project_dir,
)
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.portable import ensure_expanded_build
from tallyman_xorq.result_cache import (
    baked_snapshot_path,
    cached_result_expr,
    verify_result_faithful,
)

# ---------------------------------------------------------------------------
# recipe helpers (mirror test_auto_recalc.py)
# ---------------------------------------------------------------------------


def _parent_v1_code(project: str) -> str:  # full orders
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "category", "price")
"""


def _parent_v2_code(project: str) -> str:  # same source, boots only — fewer rows
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.filter(t.category == "boots").select("region", "category", "price")
"""


def _cheap_child_code() -> str:  # row-preserving → no snapshot, recomputes on read
    return """
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("trips")
expr = t.mutate(doubled=t.price * 2)
"""


def _worthy_child_code() -> str:  # Aggregate → bakes a result snapshot at build
    return """
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("trips")
expr = t.group_by("region").aggregate(n=t.count(), total=t.price.sum())
"""


def _lineage(project: str, monkeypatch, child_code: str) -> tuple[str, str]:
    """Build the #163 shape; return (child_v1_hash, child_v2_hash)."""
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)  # default-ON cascade
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a1 = catalog_create("trips", _parent_v1_code(project))["hash"]
    b1 = catalog_create("by_region", child_code)["hash"]
    catalog_revise("trips", _parent_v2_code(project))
    b2 = get_alias(project, "by_region")
    # Sanity on the setup itself (not the invariant under test): the cascade
    # minted a distinct child version off the new head, and b1's binding to a1
    # is durably recorded.
    assert b2 is not None and b2 != b1
    assert get_alias(project, "trips") != a1
    m1 = read_manifest(entry_dir(project, b1))
    assert m1.parents and m1.parents[0].hash == a1
    return b1, b2


# ---------------------------------------------------------------------------
# the invariant tests — each expected to fail on current main
# ---------------------------------------------------------------------------


def test_cheap_historical_read_uses_recorded_parent(project, orders_parquet, monkeypatch):
    """I1/I4: a cheap entry rematerializes from its frozen lineage, not the live head.

    b1 was built over the full parent (recorded row_count). On main, reading b1
    re-imports its recipe and ``tracked_expr_from_alias`` resolves ``trips`` to
    the *current* head, so the count comes back boots-only.
    """
    b1, _b2 = _lineage(project, monkeypatch, _cheap_child_code())
    recorded_rows = read_manifest(entry_dir(project, b1)).row_count
    live_rows = int(cached_result_expr(project, b1).count().execute())
    assert live_rows == recorded_rows


def test_worthy_revisions_resolve_distinct_snapshots(project, orders_parquet, monkeypatch):
    """I1: two distinct entries must never resolve to one snapshot file.

    On main both revisions' recipes reconstruct against the same live head, so
    both derive the same snapshot key — the "both reconstruct onto the same
    file" collapse from #163.
    """
    b1, b2 = _lineage(project, monkeypatch, _worthy_child_code())
    p1 = baked_snapshot_path(project, b1)
    p2 = baked_snapshot_path(project, b2)
    assert p1 is not None and p2 is not None
    assert p1 != p2


def test_untouched_historical_entry_verifies_faithful(project, orders_parquet, monkeypatch):
    """I4-verification: an entry nobody touched must reproduce its recorded digest.

    ``verify_result_faithful`` must mean "this entry's build reproduces its
    result_digest". On main it locates the snapshot through today's alias
    state, so an ordinary parent advance makes every non-head revision report
    unfaithful (or resolve to a sibling's snapshot).
    """
    b1, _b2 = _lineage(project, monkeypatch, _worthy_child_code())
    assert verify_result_faithful(project, b1) is True


def test_diff_of_two_revisions_is_not_a_self_join(project, orders_parquet, monkeypatch):
    """I5: the diff consumer must see the two revisions' real, distinct results.

    v2's parent filters to boots, so per-region counts genuinely differ. On
    main both diff sides reconstruct against the same head and the outer join
    compares a result to itself — every delta exactly zero (#162's symptom).
    """
    b1, b2 = _lineage(project, monkeypatch, _worthy_child_code())
    df = build_diff_expr(b1, b2, keys=["region"]).execute()
    deltas = df["n_abs_delta"].fillna(0)
    assert (deltas != 0).any()


def test_cold_cache_read_reproduces_build_bytes(project, orders_parquet, monkeypatch):
    """I2: wiping every cache may change latency only — never the bytes served.

    With the compute cache emptied, reading b1 self-heals its snapshot. The
    heal must re-execute b1's *frozen* computation and land bytes matching the
    recorded digest. On main it executes the live-head reconstruction and
    manufactures today's bytes under b1's historical hash.
    """
    b1, _b2 = _lineage(project, monkeypatch, _worthy_child_code())
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()
    cached_result_expr(project, b1)  # cold read — forces the self-heal
    assert verify_result_faithful(project, b1) is True


# ---------------------------------------------------------------------------
# second wave — behaviors decided in plans/adr-read-path-loads-builds.md
# (D4, D6, D10); also expected to fail on current main
# ---------------------------------------------------------------------------


def _worthy_parent_code(project: str) -> str:  # Aggregate at the parent → worthy parent
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count(), total=t.price.sum())
"""


def test_missing_build_is_hard_error(project, orders_parquet, monkeypatch):
    """ADR D6: an entry whose xorq_build/ is gone must raise, not reconstruct.

    On main the read path never touches the build — it re-imports expr.py —
    so deleting xorq_build/ goes unnoticed and the read silently succeeds.
    The error must name the entry so the remedy (rebuild) is actionable.
    """
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    h = catalog_create("plain", _parent_v1_code(project))["hash"]
    shutil.rmtree(entry_build_dir(project, h))
    cached_result_expr.cache_clear()
    with pytest.raises(Exception, match=h[:12]):
        cached_result_expr(project, h)


def test_chained_child_build_heals_parent_without_preheal(project, orders_parquet, monkeypatch):
    """ADR D4: a child's build is self-contained — a cold load heals ancestors.

    Simulates exactly what Buckaroo does (expand the child's build, load it,
    execute) with every snapshot evicted and NO pre-heal call. On main the
    child's build holds a bare read of the worthy parent's snapshot — a file
    reference with no regeneration knowledge — so the cold load reads a
    missing file. Under the contract the parent's cache node travels in the
    child's build and heals through ordinary cache mechanics.
    """
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    catalog_create("trips", _worthy_parent_code(project))
    child_code = """
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("trips")
expr = t.mutate(doubled=t.total * 2)
"""
    child = catalog_create("by_region", child_code)["hash"]
    recorded = read_manifest(entry_dir(project, child)).row_count
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()
    expanded = ensure_expanded_build(
        entry_build_dir(project, child),
        project_dir(project),
        entry_expanded_build_dir(project, child),
    )
    loaded = load_expr(expanded, cache_dir=compute_cache_dir(project))
    assert int(loaded.count().execute()) == recorded


def test_unfaithful_heal_wipes_stat_cache(project, orders_parquet, monkeypatch):
    """ADR D10: a heal that fails verification wipes the entry's stat cache.

    Buckaroo's summary-stats cache is keyed on expression structure and paths
    (buckaroo#955), so bytes that change under a stable path leave stale stats
    rendering beside fresh rows. A verified-faithful heal is byte-identical
    and needs no wipe; an UNFAITHFUL one must clear the entry's
    .buckaroo_stat_cache. The recorded digest is tampered here so the heal is
    unfaithful by construction. On main the mismatch only logs a warning.
    """
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    h = catalog_create("by_region", _worthy_parent_code(project))["hash"]
    mpath = entry_dir(project, h) / "manifest.json"
    doc = json.loads(mpath.read_text())
    doc["result_digest"] = "0" * 64
    mpath.write_text(json.dumps(doc))
    stat_dir = entry_stat_cache_dir(project, h) / "parquet"
    stat_dir.mkdir(parents=True, exist_ok=True)
    sentinel = stat_dir / "stale-stats.parquet"
    sentinel.write_text("stale")
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()
    cached_result_expr(project, h)  # heal fires; digest cannot match the tamper
    assert not sentinel.exists()
