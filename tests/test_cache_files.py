"""Files are written only because something is about to read them, and deleted only by an explicit user action.

Red tests for ``plans/ADR-007-tallyman-owned-materialization.md`` D12 (files are deleted only by an explicit user
action), D5 (the verify sweep is not a caller of ``ensure_materialized``) and D14 (the Cache page needs a row for a
file whose entry is not in the catalog), and for ``plans/ADR-009-digest-stability.md`` D6 (an entry whose recipe is not
reproducible has its file pinned, and the Cache page's delete skips it and says why).

The Cache page is ``GET /{project}/api/result_cache`` and ``DELETE /{project}/api/result_cache/{hash}``.
"""

from __future__ import annotations

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_core import entry_dir, read_manifest
from tallyman_core.errors import list_errors, record_error
from tallyman_core.paths import compute_cache_dir
from tallyman_xorq import build_and_persist
from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr, snapshot_file_digest


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _second_agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.group_by("category").aggregate(n=t.count())
"""


def _nonreproducible_code(project: str) -> str:
    """A recipe whose UDF returns other values on every call: a worthy entry (a UDF always is) that cannot be
    reproduced, which ADR-009 D6 finds when the entry is created by running its query twice."""
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
from xorq.expr.udf import make_pandas_udf
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.vendor.ibis import schema as ibis_schema

t = tracked_expr_from_alias("orders_src", project={project!r})


def jitter(df):
    import random

    return df["qty"] + random.random()


_udf = make_pandas_udf(jitter, ibis_schema({{"qty": dt.int64}}), dt.float64, name="jitter")
expr = t.mutate(qty_jitter=_udf.on_expr(t))
"""


def _cache_rows(client: TestClient, project: str) -> dict[str, dict]:
    r = client.get(f"/{project}/api/result_cache")
    assert r.status_code == 200, r.text
    return {row["hash"]: row for row in r.json()["entries"]}


# ---------------------------------------------------------------------------
# nothing writes a file speculatively (ADR-007 D12, D5)
# ---------------------------------------------------------------------------


def test_startup_warm_up_writes_no_file(project, orders_src):
    """ADR-007 D12: the warm-up used to call ``cached_result_expr`` for every entry until a 3 s budget was spent,
    which heals a deleted snapshot. That undoes the Cache page's delete button, and one large heal blocks startup for
    as long as it takes. With ``compute_cache/`` emptied and no request made, starting the app leaves it empty."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)
    cached_result_expr.cache_clear()

    with TestClient(create_app(project)):  # entering runs the startup handlers, including the warm-up
        pass

    written = sorted(str(p) for p in compute_cache_dir(project).rglob("*") if p.is_file())
    assert written == [], "starting the app wrote files under compute_cache/"
    assert not snap.exists()


def test_verify_sweep_reports_a_missing_snapshot_as_absent_and_writes_nothing(project, orders_src):
    """ADR-007 D5 and D12: the verify sweep checks the files that exist and reports each entry that recorded a digest
    as faithful, unfaithful or absent. It is not a caller of ``ensure_materialized``: a sweep that rewrote every
    deleted snapshot in the project would undo the user's deletes. Every file that function writes is verified before
    it is served, so an absent file is checked at the moment it next exists."""
    from tallyman_xorq.staleness import verify_sweep

    kept = build_and_persist(project, _agg_code(project)).content_hash
    dropped = build_and_persist(project, _second_agg_code(project)).content_hash
    snap = baked_snapshot_path(project, dropped)
    assert snap is not None
    snap.unlink()
    cached_result_expr.cache_clear()

    out = verify_sweep(project)

    assert out["results"][kept] is True
    assert "absent" in out, f"the sweep does not report absent files: {sorted(out)}"
    assert sorted(out["absent"]) == [dropped]
    assert out["unfaithful"] == [] and out["errors"] == {}
    assert not snap.exists(), "the sweep rewrote a snapshot the user had deleted"


# ---------------------------------------------------------------------------
# the Cache page (ADR-007 D12, D14; ADR-009 D6)
# ---------------------------------------------------------------------------


def test_cache_page_lists_a_reproducible_entry_as_not_pinned(project, orders_src):
    """ADR-009 D6: an entry whose query gave the same content digest twice is reproducible, so its file may be
    deleted and made again. The Cache page says so for each row: ``pinned`` is False and there is no reason."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    client = TestClient(create_app(project))

    row = _cache_rows(client, project)[h]

    assert row.get("pinned") is False, row
    assert row.get("pinned_reason") is None, row
    assert not row.get("orphan")


def test_a_non_reproducible_entry_is_pinned_and_its_delete_is_refused(project, orders_src):
    """ADR-009 D6 and ADR-007 D12: an entry whose recipe is not reproducible is recorded as such when it is created,
    and its file is never deleted by tallyman, because deleting it would end the only copy of those rows. The Cache
    page's delete answers 409, says why, and leaves the file."""
    result = build_and_persist(project, _nonreproducible_code(project))
    h = result.content_hash
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    client = TestClient(create_app(project))

    row = _cache_rows(client, project)[h]
    assert row.get("pinned") is True, row
    assert row.get("pinned_reason"), row

    response = client.delete(f"/{project}/api/result_cache/{h}")

    assert response.status_code == 409, response.text
    assert "reproducible" in response.json()["detail"].lower()
    assert snap.exists(), "the delete removed the file of an entry that cannot be recreated"


def test_an_entry_with_an_unfaithful_heal_record_is_pinned(project, orders_src):
    """ADR-007 D12 and ADR-006 D12 (unfaithful entries are pinned and badged): a heal that wrote different rows than
    were built leaves an ``unfaithful_heal`` record in ``errors.jsonl``. That record is what pins the entry, so the
    Cache page's delete refuses it as it refuses an entry that was found not reproducible at creation."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    record_error(project, code="unfaithful_heal", message="self-heal produced different bytes", hash=h)
    client = TestClient(create_app(project))

    row = _cache_rows(client, project)[h]
    assert row.get("pinned") is True, row
    assert row.get("pinned_reason"), row

    assert client.delete(f"/{project}/api/result_cache/{h}").status_code == 409
    assert snap.exists()


def test_a_snapshot_whose_entry_is_not_in_the_catalog_is_listed_and_can_be_deleted(project, orders_src):
    """ADR-007 D14: a reset no longer prunes ``compute_cache/``, so the snapshot of an entry that a reset retired stays
    on disk until the user deletes it. The Cache page lists files by entry, so it needs a row for a file whose entry is
    not in the catalog, and the delete has to accept it."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    snapshots = compute_cache_dir(project) / "result_cache"
    snapshots.mkdir(parents=True, exist_ok=True)
    orphan = snapshots / "deadbeef0123.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3]}), orphan)
    client = TestClient(create_app(project))

    rows = _cache_rows(client, project)

    assert "deadbeef0123" in rows, f"the file of an entry that is not in the catalog is not listed: {sorted(rows)}"
    assert rows["deadbeef0123"].get("orphan") is True
    assert rows["deadbeef0123"].get("alias") is None
    assert not rows[h].get("orphan")

    response = client.delete(f"/{project}/api/result_cache/deadbeef0123")

    assert response.status_code == 200, response.text
    assert not orphan.exists()


def test_a_normal_snapshot_can_still_be_deleted_and_the_next_read_re_creates_and_verifies_it(project, orders_src):
    """ADR-007 D12 and D5: the delete button keeps working for an ordinary entry. The next read rewrites the file and
    checks it against the recorded digest (no ``unfaithful_heal`` record), and the entry is still not pinned."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    client = TestClient(create_app(project))

    response = client.delete(f"/{project}/api/result_cache/{h}")
    assert response.status_code == 200, response.text
    assert not snap.exists()

    cached_result_expr.cache_clear()
    assert len(cached_result_expr(project, h).execute()) > 0
    assert snap.exists()
    assert snapshot_file_digest(snap) == read_manifest(entry_dir(project, h)).result_digest
    assert not [e for e in list_errors(project) if e.get("code") == "unfaithful_heal"]
    assert _cache_rows(client, project)[h].get("pinned") is False


def test_a_source_snapshot_can_be_deleted_and_the_next_read_re_creates_it_from_the_clone(project, tmp_path):
    """ADR-011 D1, revised: a source version's snapshot is cache too.

    The clone of the imported bytes under ``data/.cas`` and the reader options on the entry are everything
    ``ensure_materialized`` needs to write the file again, which is ADR-007 D13's test for whether a file is cache.
    So the Cache page lists a source row as unpinned, the delete takes it, and the next read makes it again.
    """
    from tallyman_xorq import source_import
    from tallyman_xorq.materialize import snapshot_path

    outside = tmp_path / "outside"
    outside.mkdir(parents=True, exist_ok=True)
    src = outside / "orders.parquet"
    pq.write_table(pa.table({"region": ["e", "w", "e"], "price": [1.0, 2.0, 3.0]}), src)
    out = source_import.update_and_depend(str(src), "orders", project=project)
    snap = snapshot_path(project, out["hash"])
    assert snap.exists()
    client = TestClient(create_app(project))

    assert _cache_rows(client, project)[out["hash"]].get("pinned") is False

    response = client.delete(f"/{project}/api/result_cache/{out['hash']}")
    assert response.status_code == 200, response.text
    assert not snap.exists()

    cached_result_expr.cache_clear()
    assert len(cached_result_expr(project, out["hash"]).execute()) == 3
    assert snap.exists()
    assert snapshot_file_digest(snap) == read_manifest(entry_dir(project, out["hash"])).result_digest
    assert not [e for e in list_errors(project) if e.get("code") == "unfaithful_heal"]


def test_a_source_snapshot_whose_clone_is_gone_is_pinned_and_its_delete_is_refused(project, tmp_path):
    """The one case where a source snapshot cannot be made again: the clone of the imported bytes is gone too."""
    from tallyman_core.paths import data_dir
    from tallyman_xorq import source_import
    from tallyman_xorq.materialize import snapshot_path

    outside = tmp_path / "outside"
    outside.mkdir(parents=True, exist_ok=True)
    src = outside / "orders.parquet"
    pq.write_table(pa.table({"region": ["e", "w", "e"], "price": [1.0, 2.0, 3.0]}), src)
    out = source_import.update_and_depend(str(src), "orders", project=project)
    (data_dir(project) / ".cas" / f"{out['digest']}.parquet").unlink()
    client = TestClient(create_app(project))

    row = _cache_rows(client, project)[out["hash"]]
    assert row.get("pinned") is True, row
    assert "orders-v1" in row.get("pinned_reason", ""), row
    assert ".cas" in row.get("pinned_reason", ""), row  # pinned by the lost clone, not by being a source

    response = client.delete(f"/{project}/api/result_cache/{out['hash']}")

    assert response.status_code == 409, response.text
    assert snapshot_path(project, out["hash"]).exists()
