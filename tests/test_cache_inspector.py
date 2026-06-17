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


def _cheap_code(project: str) -> str:  # projection → cheap, bakes no snapshot
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select("region", "price")
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
    # cached_result_expr was already warm for that entry — the companion memoises a
    # `deferred_read_parquet` of the snapshot, so without clearing that lru the next
    # read dereferences the just-deleted file and 500s ("At least one path is
    # required") instead of re-baking. The delete endpoint promises a self-heal.
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
