"""Files are written only because something is about to read them, and deleted only by an explicit user action.

Red tests for ``plans/ADR-007-tallyman-owned-materialization.md`` D12 (files are deleted only by an explicit user
action), D5 (the verify sweep is not a caller of ``ensure_materialized``) and D14 (the Cache page needs a row for a
file whose entry is not in the catalog), and for ``plans/ADR-009-digest-stability.md`` D6 (an entry whose recipe is not
reproducible has its file pinned, and the Cache page's delete skips it and says why).

A pin has to hold wherever the page offers a delete: after a reset retires the entry (#195), after the error banner is
dismissed (#196), and across a reset back and forward (#194).

The Cache page is ``GET /{project}/api/result_cache`` and ``DELETE /{project}/api/result_cache/{hash}``.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_core import catalog_state as cs
from tallyman_core import entry_dir, read_manifest
from tallyman_core.errors import list_errors, record_error
from tallyman_core.paths import compute_cache_dir, errors_path
from tallyman_xorq import build_and_persist
from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr, snapshot_file_digest


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _second_agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("category").aggregate(n=t.count())
"""


def _nonreproducible_code(project: str) -> str:
    """A recipe whose UDF returns other values on every call: a worthy entry (a UDF always is) that cannot be
    reproduced, which ADR-009 D6 finds when the entry is created by running its query twice."""
    return f"""
from tallyman_xorq.io import read_project_file
from xorq.expr.udf import make_pandas_udf
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.vendor.ibis import schema as ibis_schema

t = read_project_file("orders.parquet", project={project!r})


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


def _unfaithful_heals(project: str) -> list[str]:
    return [e["hash"] for e in list_errors(project, limit=1_000_000) if e.get("code") == "unfaithful_heal"]


def _heal_unfaithfully(project: str, content_hash: str) -> Path:
    """Make the next heal of a reproducible entry unfaithful by construction, and run it: the recorded digest is
    replaced by one no heal can match (as ``tests/test_snapshot_format.py`` does), the snapshot is deleted, and the
    entry is read. Returns the snapshot's path."""
    path = entry_dir(project, content_hash) / "manifest.json"
    doc = json.loads(path.read_text())
    doc["result_digest"] = "arrow-sha256:" + "0" * 64
    path.write_text(json.dumps(doc, indent=2))
    snap = baked_snapshot_path(project, content_hash)
    assert snap is not None and snap.exists()
    snap.unlink()
    cached_result_expr.cache_clear()

    cached_result_expr(project, content_hash)

    assert snap.exists()
    assert _unfaithful_heals(project) == [content_hash]
    return snap


# ---------------------------------------------------------------------------
# nothing writes a file speculatively (ADR-007 D12, D5)
# ---------------------------------------------------------------------------


def test_startup_warm_up_writes_no_file(project, orders_parquet):
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


def test_verify_sweep_reports_a_missing_snapshot_as_absent_and_writes_nothing(project, orders_parquet):
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


def test_cache_page_lists_a_reproducible_entry_as_not_pinned(project, orders_parquet):
    """ADR-009 D6: an entry whose query gave the same content digest twice is reproducible, so its file may be
    deleted and made again. The Cache page says so for each row: ``pinned`` is False and there is no reason."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    client = TestClient(create_app(project))

    row = _cache_rows(client, project)[h]

    assert row.get("pinned") is False, row
    assert row.get("pinned_reason") is None, row
    assert not row.get("orphan")


def test_a_non_reproducible_entry_is_pinned_and_its_delete_is_refused(project, orders_parquet):
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


def test_an_unfaithful_heal_pins_the_snapshot_and_dismissing_the_error_banner_leaves_it_pinned(project, orders_parquet):
    """ADR-007 D12 and ADR-006 D12 (unfaithful entries are pinned and badged): a heal that wrote different rows than
    were built pins the snapshot, so the Cache page's delete refuses it as it refuses an entry that was found not
    reproducible at creation. The pin used to be the ``unfaithful_heal`` record in ``errors.jsonl``, and the banner's
    dismiss deletes that file, so after a dismiss the page deleted the snapshot and the next read healed it again
    (#196). The record stays, for the banner; the pin is one of the entry's durable facts."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    snap = _heal_unfaithfully(project, h)
    client = TestClient(create_app(project))

    assert client.delete(f"/{project}/api/errors").status_code == 200  # the banner's dismiss
    assert _unfaithful_heals(project) == []

    row = _cache_rows(client, project)[h]
    assert row.get("pinned") is True, row
    assert row.get("pinned_reason"), row
    response = client.delete(f"/{project}/api/result_cache/{h}")
    assert response.status_code == 409, response.text
    assert snap.exists()


def test_the_pin_of_an_unfaithful_heal_survives_a_reset_back_and_forward(project, orders_parquet):
    """#194 and #196 together. A reset forward restores an entry's dir by copying it out of the bullpen, which keeps
    its copy, so a heal of the restored entry changes the live manifest while the parked one stays as it was. The next
    reset back has to park the live dir, or the pin recorded with it is lost and the Cache page deletes the file."""
    cs.ensure_catalog_repo(project)
    s0 = cs.checkpoint_catalog(project, "s0")
    h = build_and_persist(project, _agg_code(project)).content_hash
    s1 = cs.checkpoint_catalog(project, "s1")
    cs.reset_to(project, s0)
    cs.reset_to(project, s1)  # the entry's dir is copied back, and the bullpen keeps its copy
    snap = _heal_unfaithfully(project, h)
    client = TestClient(create_app(project))
    assert client.delete(f"/{project}/api/errors").status_code == 200  # the banner's dismiss

    for step in (s0, s1):
        cs.reset_to(project, step)

        row = _cache_rows(client, project)[h]
        assert row.get("pinned") is True, f"after the reset to step {step}: {row}"
        response = client.delete(f"/{project}/api/result_cache/{h}")
        assert response.status_code == 409, f"after the reset to step {step}: {response.text}"
        assert snap.exists()


def test_a_snapshot_whose_entry_is_not_in_the_catalog_is_listed_and_can_be_deleted(project, orders_parquet):
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


def test_a_reset_back_keeps_the_pin_of_a_non_reproducible_entrys_snapshot(project, orders_parquet):
    """#195: after a reset to an earlier step, the entry's dir is in the bullpen and its snapshot is still on disk
    (ADR-007 D14). The parked manifest still says the recipe is not reproducible, so the file stays pinned, the delete
    answers 409, and a reset forward finds the rows that were built. The row used to be an unpinned orphan whose delete
    removed the only copy, and the next read after a reset forward healed it to different rows."""
    cs.ensure_catalog_repo(project)
    s0 = cs.checkpoint_catalog(project, "s0")
    h = build_and_persist(project, _nonreproducible_code(project)).content_hash
    s1 = cs.checkpoint_catalog(project, "s1")
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    built = read_manifest(entry_dir(project, h)).result_digest
    cs.reset_to(project, s0)
    assert not entry_dir(project, h).exists() and snap.exists()
    client = TestClient(create_app(project))

    row = _cache_rows(client, project)[h]
    assert row.get("pinned") is True, row
    assert "reproducible" in (row.get("pinned_reason") or "").lower(), row
    response = client.delete(f"/{project}/api/result_cache/{h}")
    assert response.status_code == 409, response.text
    assert "reproducible" in response.json()["detail"].lower()
    assert snap.exists()

    cs.reset_to(project, s1)
    cached_result_expr(project, h)
    assert snapshot_file_digest(snap) == built
    assert _unfaithful_heals(project) == []


def test_a_snapshot_whose_entry_a_reset_retired_is_labelled_retired_not_orphan(project, orders_parquet):
    """#195: the Cache page tells a file whose entry a reset retired (its dir is parked in the bullpen, and a reset
    forward brings it back) from an orphan, a file no entry names. The retired row carries what its parked manifest
    records, and a reproducible entry's file is not pinned, so it can still be deleted."""
    cs.ensure_catalog_repo(project)
    s0 = cs.checkpoint_catalog(project, "s0")
    h = build_and_persist(project, _agg_code(project), prompt="sales by region").content_hash
    assert cs.checkpoint_catalog(project, "s1") is not None
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    cs.reset_to(project, s0)
    client = TestClient(create_app(project))

    row = _cache_rows(client, project)[h]

    assert row.get("retired") is True, row
    assert row.get("orphan") is False, row
    assert row.get("prompt") == "sales by region", row
    assert row.get("pinned") is False, row
    response = client.delete(f"/{project}/api/result_cache/{h}")
    assert response.status_code == 200, response.text
    assert not snap.exists()


def test_a_corrupt_line_in_the_error_log_breaks_neither_the_listing_nor_the_delete(project, orders_parquet):
    """#196: one line of ``errors.jsonl`` that is not JSON (a torn append, a hand edit) made the listing and the
    delete answer 500, since the pin check parsed every line of the log with no guard."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    record_error(project, code="x", message="before the torn line")
    with errors_path(project).open("a") as fh:
        fh.write('{"id": "torn", "code": \n')
    client = TestClient(create_app(project), raise_server_exceptions=False)

    listing = client.get(f"/{project}/api/result_cache")
    assert listing.status_code == 200, listing.text
    assert h in {row["hash"] for row in listing.json()["entries"]}
    response = client.delete(f"/{project}/api/result_cache/{h}")
    assert response.status_code == 200, response.text


def test_the_listing_does_not_parse_the_error_log_once_per_row(project, orders_parquet, monkeypatch):
    """#196: the pin check read and parsed the whole of ``errors.jsonl`` once for every row the Cache page lists."""
    import tallyman_core.errors as errors_module

    build_and_persist(project, _agg_code(project))
    build_and_persist(project, _second_agg_code(project))
    parses = []
    real = errors_module.list_errors

    def counting(*args, **kwargs):
        parses.append(args)
        return real(*args, **kwargs)

    monkeypatch.setattr(errors_module, "list_errors", counting)
    client = TestClient(create_app(project))

    assert len(_cache_rows(client, project)) == 2
    assert len(parses) <= 1, f"listing 2 rows parsed the error log {len(parses)} times"


def test_a_normal_snapshot_can_still_be_deleted_and_the_next_read_re_creates_and_verifies_it(project, orders_parquet):
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
