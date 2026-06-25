from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_core.aliases import history_for
from tallyman_core.paths import entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.result_cache import baked_snapshot_path

# The cache pane is a React page (CachePage) backed by JSON: GET
# /{project}/api/result_cache lists each entry's baked xorq .cache() snapshot,
# DELETE /{project}/api/result_cache/{hash} evicts one. These tests exercise that
# contract; the rendering itself is the SPA's job.
#
# The page is manifest-driven: an expensive entry records its snapshot size
# (manifest.cache_bytes, #87) at build, so it lists with no extra setup; a cheap
# entry bakes no snapshot and is absent. Deleting unlinks the baked snapshot,
# which self-heals (re-bakes) on the next read.


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _filter_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
f = t.filter(t.category == "boots")
expr = f.group_by("region").aggregate(total=f.price.sum(), n=f.count())
"""


def _cheap_code(project: str) -> str:  # projection → cheap, bakes no snapshot
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _child_code(project: str, parent: str) -> str:  # tracked_expr_from_alias → records a parent edge
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r}, project={project!r})
expr = t.mutate(double=t.total * 2)
"""


def test_cache_pane_lists_expensive_not_cheap(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))  # expensive → baked snapshot
    catalog_revise("shoe_sales", _filter_code(project))  # still expensive
    cheap = catalog_create("cheap_one", _cheap_code(project))  # projection → no snapshot
    cheap_h = cheap["hash"]

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/result_cache")
    assert r.status_code == 200
    body = r.json()
    listed = {e["hash"] for e in body["entries"]}

    # Both expensive shoe_sales versions list with their baked-snapshot size.
    for e in list_entries(project):
        if e["content_hash"] == cheap_h:
            continue
        assert e["content_hash"] in listed
    aliases = [e["alias"] for e in body["entries"] if e["alias"]]
    assert "shoe_sales" in aliases  # alias surfaced on the row
    # rows carry size + created + a non-null int row_count for the table
    for e in body["entries"]:
        assert "size_formatted" in e and "created" in e
        assert isinstance(e["row_count"], int)
    assert "total_formatted" in body

    # The cheap entry bakes no snapshot (cache_bytes is None) → absent, so its
    # always-failing delete button never appears on the page.
    assert cheap_h not in listed


def test_cache_pane_drops_evicted_snapshot(fresh_companion_app, project, orders_parquet, monkeypatch):
    # The page reflects what's on disk. After a snapshot is deleted — and not yet
    # re-viewed, so nothing re-baked it — the entry must drop out of the listing,
    # not linger as a phantom "cached" row. Keying the listing on manifest.cache_bytes
    # alone (recorded at build, untouched by delete) kept showing the row with a
    # stale size and an "on disk" total that counts bytes already freed; a second
    # delete then 404s on a row the page is still showing.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))  # expensive → baked snapshot
    h = list_entries(project)[0]["content_hash"]
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()

    c = TestClient(fresh_companion_app)
    before = c.get(f"/{project}/api/result_cache").json()
    assert h in {e["hash"] for e in before["entries"]}
    row_size = next(e["size"] for e in before["entries"] if e["hash"] == h)

    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 200
    assert not snap.exists()

    # Listing again (no read in between, so nothing re-baked the snapshot) must
    # drop the row and not count its freed bytes in the total.
    after = c.get(f"/{project}/api/result_cache").json()
    assert h not in {e["hash"] for e in after["entries"]}
    assert after["total"] == before["total"] - row_size


def test_cache_delete_removes_only_snapshot(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))  # expensive → baked snapshot
    h = list_entries(project)[0]["content_hash"]
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    edir = entry_dir(project, h)
    # stat cache + code that must survive a snapshot delete
    stat_cache = edir / ".buckaroo_stat_cache"
    stat_cache.mkdir(exist_ok=True)
    (stat_cache / "marker").write_text("keep me")
    assert (edir / "expr.py").exists()

    c = TestClient(fresh_companion_app)
    r = c.delete(f"/{project}/api/result_cache/{h}")
    assert r.status_code == 200
    assert r.json()["hash"] == h

    assert not snap.exists()  # baked snapshot gone
    assert (stat_cache / "marker").read_text() == "keep me"  # stat cache kept
    assert (edir / "expr.py").exists()  # code kept
    assert (edir / "manifest.json").exists()  # entry kept
    assert h in history_for(project, "shoe_sales")  # alias history intact


def test_cache_delete_then_read_self_heals_warm_cache(fresh_companion_app, project, orders_parquet, monkeypatch):
    # Deleting a snapshot must self-heal on the next read even when the read path's
    # cached_result_expr was already warm for that entry. Since ee0a90a the read
    # wrapper re-checks path.exists() on every call and self-heals regardless of the
    # warm memo (see test_cached_result_expr_self_heals_after_warm_then_evict), so
    # the delete endpoint's cache_clear() is now defense-in-depth, not what averts
    # the "At least one path is required" 500. This pins the end-to-end promise
    # through the endpoint.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.result_cache import cached_result_expr

    catalog_create("shoe_sales", _agg_code(project))  # expensive → baked snapshot
    h = list_entries(project)[0]["content_hash"]
    # Warm the read-path lru the way a prior viewer load (api_data) would.
    assert cached_result_expr(project, h).count().execute() > 0

    c = TestClient(fresh_companion_app)
    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 200

    # Next read must re-bake the evicted snapshot, not raise on the deleted file.
    assert cached_result_expr(project, h).count().execute() > 0
    assert baked_snapshot_path(project, h).exists()  # re-baked


def test_cache_delete_missing_snapshot_404(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    c = TestClient(fresh_companion_app)
    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 200
    # second delete: baked snapshot already evicted
    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 404


def test_cache_delete_blocked_in_read_only(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()

    c = TestClient(create_app(project, read_only=True))
    # serve mode: the snapshot must not be deletable.
    assert c.delete(f"/{project}/api/result_cache/{h}").status_code == 403
    assert snap.exists()  # untouched


def test_cache_delete_rejects_non_hex_hash(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(fresh_companion_app)
    # A content hash is lowercase hex (it names an entry dir). A junk value can
    # only ever resolve outside the entries tree, so reject it as a bad request
    # rather than letting it fall through to the "no baked snapshot" 404 — that
    # 404 reads as "valid entry, already evicted", which a malformed hash isn't.
    for bad in ("NOPE-not-a-hash", "Deadbeef", "abc def"):
        r = c.delete(f"/{project}/api/result_cache/{bad}")
        assert r.status_code == 400, f"{bad!r} should be a 400, got {r.status_code}"


# GET /{project}/api/entry_cache/{hash} is the per-expression footprint behind
# the catalog-detail "metadata" tab: one entry's on-disk bytes split into
# reclaimable cache (result.parquet, the xorq snapshot, the Buckaroo stat cache)
# vs. the build artifact, plus any diff stat caches it participates in. Distinct
# from /api/result_cache (the project-wide listing) — this one is per-entry and
# resolves an alias to its current head. Restored alongside the metadata tab
# (#132); the rendering is the SPA's job, this pins the JSON contract.


def test_entry_cache_expensive_breakdown(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))  # expensive → baked snapshot
    h = list_entries(project)[0]["content_hash"]
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()  # precondition: it baked

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry_cache/{h}")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["content_hash"] == h
    assert body["cache_worthy"] is True

    comps = {comp["key"]: comp for comp in body["components"]}
    # The build artifact is always present and explicitly not reclaimable.
    assert comps["build"]["kind"] == "artifact"
    assert comps["build"]["reclaimable"] is False
    assert comps["build"]["bytes"] > 0
    # An expensive entry's snapshot is applicable and sized from the baked file.
    snap_comp = comps["snapshot_cache"]
    assert snap_comp["applicable"] is True
    assert snap_comp["exists"] is True
    assert snap_comp["bytes"] > 0
    # Totals reconcile across the three buckets.
    assert body["total_bytes"] == body["cache_bytes"] + body["artifact_bytes"] + body["diff_cache_bytes"]
    assert body["cache_bytes"] >= snap_comp["bytes"]
    assert body["total_formatted"]


def test_entry_cache_cheap_marks_snapshot_inapplicable(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    cheap = catalog_create("cheap_one", _cheap_code(project))  # projection → no snapshot
    h = cheap["hash"]

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry_cache/{h}")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["cache_worthy"] is False
    snap_comp = next(comp for comp in body["components"] if comp["key"] == "snapshot_cache")
    assert snap_comp["applicable"] is False  # no snapshot copy for a cheap entry
    # The build artifact is still present and sized.
    assert any(comp["key"] == "build" and comp["bytes"] > 0 for comp in body["components"])


def test_entry_cache_resolves_alias(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]

    c = TestClient(fresh_companion_app)
    by_alias = c.get(f"/{project}/api/entry_cache/shoe_sales")
    assert by_alias.status_code == 200, by_alias.text
    assert by_alias.json()["content_hash"] == h  # alias resolved to its current head


def test_entry_cache_unknown_hash_404_malformed_400(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(fresh_companion_app)
    # Well-formed hex, no such entry → 404 (not the SPA index.html fallthrough).
    assert c.get(f"/{project}/api/entry_cache/deadbeef").status_code == 404
    # Non-hex, non-alias → 400, matching the other alias-or-hash entry routes.
    assert c.get(f"/{project}/api/entry_cache/NOPE-not-a-hash").status_code == 400


def test_entry_cache_lineage_and_enrichment(fresh_companion_app, project, orders_parquet, monkeypatch):
    # #134: the metadata tab links the entry's parents and dependent children and
    # carries source-size / last-modified / cheap-vs-expensive enrichment.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    base = catalog_create("base", _agg_code(project))  # root: reads orders.parquet
    child = catalog_create("base_child", _child_code(project, "base"))  # tracked edge → base
    base_h = base["hash"]
    child_h = child["hash"]

    c = TestClient(fresh_companion_app)

    # The child links up to its parent (by hash, with the alias resolved).
    child_meta = c.get(f"/{project}/api/entry_cache/{child_h}").json()
    assert base_h in {p["hash"] for p in child_meta["parents"]}
    assert any(p["alias"] == "base" for p in child_meta["parents"])

    # The parent links down to its dependent child (reverse index).
    base_meta = c.get(f"/{project}/api/entry_cache/{base_h}").json()
    assert child_h in {ch["hash"] for ch in base_meta["children"]}

    # Enrichment fields are present and typed.
    assert isinstance(base_meta["source_tracked"], bool)
    assert isinstance(base_meta["sources"], list)
    assert "cache_worthy_why" in base_meta
    assert "modified_at" in base_meta
    assert "source_formatted" in base_meta
