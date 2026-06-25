"""Tests for tallyman_read_csv — CSV ingest with canonical row ordering.

ADR adr-result-digest-canonical-ordering: tallyman_read_csv injects
``original_row_order`` so snapshot bytes are deterministic across builds.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_core import data_dir, entry_dir
from tallyman_mcp.server import catalog_create
from tallyman_xorq.build import list_entries


@pytest.fixture
def sample_csv(project: str) -> Path:
    """Small CSV under the project data dir with a known row ordering."""
    p = data_dir(project) / "sample.csv"
    # Deliberately write rows in an order that is NOT alphabetical by name, so
    # any test that verifies sorted-by-original_row_order can distinguish a
    # correctly-ordered snapshot from an arbitrarily-ordered one.
    p.write_text("id,name,value\n3,charlie,30\n1,alice,10\n2,bob,20\n")
    return p


def _hash_of(project: str) -> str:
    return list_entries(project)[0]["content_hash"]


def _read_csv_code(csv_path: Path) -> str:
    return f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tallyman_read_csv
schema = ibis.schema({{"id": "int64", "name": "string", "value": "int64"}})
expr = tallyman_read_csv({str(csv_path)!r}, schema=schema)
"""


def test_tallyman_read_csv_adds_original_row_order(project, sample_csv, monkeypatch):
    """tallyman_read_csv returns an expression whose schema includes original_row_order: int64."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res
    schema_fields = {f["name"]: f["type"] for f in res["schema"]["fields"]}
    assert "original_row_order" in schema_fields, (
        f"original_row_order missing from schema; got {list(schema_fields)}"
    )
    assert schema_fields["original_row_order"] == "int64"


def test_tallyman_read_csv_entry_is_worthy(project, sample_csv, monkeypatch):
    """An entry built with tallyman_read_csv is cache-worthy (RowNumber → Sort/Window op)."""
    from tallyman_xorq.result_cache import cache_worthy

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("csv_entry", _read_csv_code(sample_csv))
    h = _hash_of(project)
    assert cache_worthy(project, h) is True, (
        "tallyman_read_csv entry must be cache-worthy (RowNumber adds a window op)"
    )


def test_tallyman_read_csv_always_bakes_snapshot(project, sample_csv, monkeypatch):
    """tallyman_read_csv entries ALWAYS bake a parquet snapshot (parquet reads beat CSV re-scans).

    The RowNumber + WindowFunction ops from ibis.row_number() are in _EXPENSIVE_OPS, so there is
    no code path where a tallyman_read_csv entry ends up cheap with no snapshot.
    """
    from tallyman_xorq.result_cache import baked_snapshot_path, cache_worthy

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res
    h = _hash_of(project)

    # Must be worthy — no path where a tallyman_read_csv entry is cheap.
    assert cache_worthy(project, h) is True, "tallyman_read_csv entry must always be cache-worthy"

    # The snapshot must exist on disk immediately after the build (not lazy).
    snap = baked_snapshot_path(project, h)
    assert snap is not None, "baked_snapshot_path must return a path for a worthy entry"
    assert snap.exists(), f"snapshot must be on disk after build; expected at {snap}"


def test_tallyman_read_csv_snapshot_is_sorted(project, sample_csv, monkeypatch):
    """The baked snapshot rows are in ascending original_row_order order."""
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("csv_entry", _read_csv_code(sample_csv))
    h = _hash_of(project)

    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists(), "worthy entry must bake a snapshot"

    # Read the snapshot directly as a parquet to check physical row order.
    import pyarrow.parquet as pq

    table = pq.read_table(str(snap))
    row_orders = table.column("original_row_order").to_pylist()
    assert row_orders == sorted(row_orders), (
        f"snapshot rows not in ascending original_row_order; got {row_orders}"
    )
    # Also verify the values are 0-based and contiguous.
    assert row_orders == list(range(len(row_orders))), (
        f"expected 0-based contiguous row orders; got {row_orders}"
    )


def test_tallyman_read_csv_digest_is_stable(project, sample_csv, monkeypatch):
    """Building the same CSV entry twice yields the same result_digest.

    This tests byte-stability: both builds read the same source, inject the same
    row numbers, sort by them, and bake — so the file hash must match.
    """
    from tallyman_core import read_manifest
    from tallyman_xorq.result_cache import snapshot_file_digest, baked_snapshot_path

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)

    res1 = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res1, res1
    h = _hash_of(project)

    recorded_digest = read_manifest(entry_dir(project, h)).result_digest
    assert recorded_digest, "worthy entry must record a result_digest"

    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()

    # The recorded digest must equal the file hash.
    assert snapshot_file_digest(snap) == recorded_digest, (
        "recorded digest does not match snapshot file hash"
    )

    # Evict and self-heal to get a second materialisation.
    import shutil
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import cached_result_expr

    snap.unlink()
    cached_result_expr.cache_clear()
    cached_result_expr(project, h).execute()  # forces self-heal

    assert snap.exists(), "self-heal must rematerialise the snapshot"
    healed_digest = snapshot_file_digest(snap)
    assert healed_digest == recorded_digest, (
        f"digest changed between build and self-heal: {recorded_digest!r} vs {healed_digest!r}"
    )
