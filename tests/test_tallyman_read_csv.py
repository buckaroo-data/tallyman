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
    from tallyman_xorq.result_cache import baked_snapshot_path

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
    from tallyman_xorq.result_cache import baked_snapshot_path, snapshot_file_digest

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
    from tallyman_xorq.result_cache import cached_result_expr

    snap.unlink()
    cached_result_expr.cache_clear()
    cached_result_expr(project, h).execute()  # forces self-heal

    assert snap.exists(), "self-heal must rematerialise the snapshot"
    healed_digest = snapshot_file_digest(snap)
    assert healed_digest == recorded_digest, (
        f"digest changed between build and self-heal: {recorded_digest!r} vs {healed_digest!r}"
    )


@pytest.fixture
def repartitioned_csv(project: str) -> tuple[Path, int]:
    """A CSV large enough that datafusion repartitions the scan across threads.

    The marker column holds the source row index (0..N-1) in file order, so a
    correct ``original_row_order`` must line up with it row-for-row. The file is
    sized well over datafusion's ~10 MB ``repartition_file_min_size`` default;
    above that threshold (and with >1 core) the parallel CSV scan emits rows in
    nondeterministic arrival order, which a bare ``ibis.row_number()`` would
    capture instead of file order. Below the threshold the scan is single-
    partition and order is preserved incidentally — which is exactly why the
    3-row fixture cannot catch the bug.
    """
    n = 120_000
    pad = "x" * 160  # widen rows so N stays modest while the file clears ~10 MB
    p = data_dir(project) / "big.csv"
    lines = ["marker,pad"]
    lines += [f"{i},{pad}" for i in range(n)]
    p.write_text("\n".join(lines) + "\n")
    assert p.stat().st_size > 12_000_000, p.stat().st_size  # safely over the repartition threshold
    return p, n


def _big_read_csv_code(csv_path: Path) -> str:
    return f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tallyman_read_csv
schema = ibis.schema({{"marker": "int64", "pad": "string"}})
expr = tallyman_read_csv({str(csv_path)!r}, schema=schema)
"""


def test_tallyman_read_csv_preserves_file_order_under_repartition(project, repartitioned_csv, monkeypatch):
    """original_row_order must equal true file order even when the scan repartitions.

    Regression for the canonical-ordering bug: a bare ``ibis.row_number()`` over a
    repartitioned datafusion CSV scan numbers rows in nondeterministic arrival
    order, so ``order_by("original_row_order")`` reproduces that arbitrary order
    rather than file order. With the marker column = source row index, the baked
    snapshot's row at ``original_row_order == k`` must carry ``marker == k``.
    """
    import pyarrow.parquet as pq

    from tallyman_xorq.result_cache import baked_snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    csv_path, n = repartitioned_csv
    res = catalog_create("big_csv", _big_read_csv_code(csv_path))
    assert "error" not in res, res
    h = _hash_of(project)

    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists(), "worthy entry must bake a snapshot"

    table = pq.read_table(str(snap)).sort_by("original_row_order")
    oro = table.column("original_row_order").to_pylist()
    marker = table.column("marker").to_pylist()
    assert oro == list(range(n)), "original_row_order must be 0..N-1, contiguous"
    # The crux: row k of the source file must land at original_row_order == k.
    assert marker == list(range(n)), (
        "original_row_order does not match true file order — the scan reshuffle "
        "leaked into the row index (first divergence at "
        f"{next((i for i, m in enumerate(marker) if m != i), None)})"
    )


def test_tallyman_read_csv_digest_stable_under_repartition(project, repartitioned_csv, monkeypatch):
    """The snapshot digest is byte-stable across two independent materialisations of a repartitioned read."""
    from tallyman_core import read_manifest
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr, snapshot_file_digest

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    csv_path, _ = repartitioned_csv

    res = catalog_create("big_csv", _big_read_csv_code(csv_path))
    assert "error" not in res, res
    h = _hash_of(project)
    recorded = read_manifest(entry_dir(project, h)).result_digest
    assert recorded, "worthy entry must record a result_digest"

    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    snap.unlink()
    cached_result_expr.cache_clear()
    cached_result_expr(project, h).execute()  # self-heal → second materialisation

    assert snap.exists()
    assert snapshot_file_digest(snap) == recorded, (
        "snapshot digest drifted across materialisations of a repartitioned read"
    )


def test_tallyman_read_csv_reconstructs_after_source_deleted(project, sample_csv, monkeypatch):
    """#6: once the ordered parquet is baked, reconstruction must not touch the CSV.

    The CSV is read exactly once — at ingest. After the snapshot is baked, deleting
    (or moving) the source CSV must not break path reconstruction: baked_snapshot_path
    and verify_result_faithful re-run the recipe (-> tallyman_read_csv), and that must
    resolve the already-written intermediate instead of stat-ing the now-absent CSV.
    """
    from tallyman_xorq.result_cache import baked_snapshot_path, verify_result_faithful

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res
    h = _hash_of(project)
    assert baked_snapshot_path(project, h) is not None

    # Delete the source CSV; the baked snapshot and the ordered intermediate remain.
    sample_csv.unlink()

    # Reconstruction must still resolve the snapshot (pre-fix: FileNotFoundError on src.stat()).
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists(), "snapshot must resolve without the source CSV"
    assert verify_result_faithful(project, h) is True, "faithful check must work without the CSV"


def test_ordered_csv_intermediate_lives_outside_project_dir(project, sample_csv, monkeypatch):
    """#9: the ordered-CSV intermediate is keyed on the CSV path, not the active project.

    Placing it under a project's artifacts dir (via resolve_project(None)) means a
    reconstruction under a *different* active project looks in the wrong dir and
    re-reads the CSV. It must live in a project-independent location.
    """
    from tallyman_core import artifacts_dir
    from tallyman_core.paths import tallyman_home

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res

    shared = list((tallyman_home() / "csv_ordered").glob("*.parquet"))
    assert shared, "ordered-CSV intermediate must live under TALLYMAN_HOME/csv_ordered"
    proj_csv_ordered = artifacts_dir(project) / "csv_ordered"
    assert not proj_csv_ordered.exists(), "intermediate must NOT be under the project artifacts dir"


def test_tallyman_read_csv_forwards_reader_kwargs(project, monkeypatch):
    """#10: reader options (separator, skip_rows, ...) are forwarded to polars scan_csv.

    The rewrite dropped **kwargs; a documented call like tallyman_read_csv(path, schema,
    separator=';') must parse the alternate delimiter instead of raising TypeError.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "semi.csv"
    p.write_text("id;name\n3;charlie\n1;alice\n2;bob\n")
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tallyman_read_csv
schema = ibis.schema({{"id": "int64", "name": "string"}})
expr = tallyman_read_csv({str(p)!r}, schema=schema, separator=";")
"""
    res = catalog_create("semi", code)
    assert "error" not in res, res
    fields = {f["name"] for f in res["schema"]["fields"]}
    assert {"id", "name", "original_row_order"} <= fields, (
        f"separator=';' not forwarded — columns did not split: {fields}"
    )


# --------------------------------------------------------------------------- #
# Name collision: the CSV already carries an 'original_row_order' column.
# tallyman would add one via with_row_index, which raises a polars DuplicateError
# (not caught by the infer ladder). Instead: reuse the column when it IS the
# canonical 0..N-1 file sequence, and raise a clear, actionable error when the
# name is a coincidence carrying non-canonical data.
# --------------------------------------------------------------------------- #
def test_existing_row_order_column_reused(project, monkeypatch):
    """A CSV already carrying a valid 0..N-1 'original_row_order' column ingests
    by reusing that column, not by crashing on the row-index name collision."""
    import pyarrow.parquet as pq

    from tallyman_xorq.result_cache import baked_snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "hasorder.csv"
    # Rows deliberately NOT sorted by name; original_row_order = true 0..N-1 order.
    p.write_text("original_row_order,name\n0,charlie\n1,alice\n2,bob\n")
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(p)!r})
"""
    res = catalog_create("hasorder", code)
    assert "error" not in res, res
    h = _hash_of(project)
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    table = pq.read_table(str(snap)).sort_by("original_row_order")
    assert table.column("original_row_order").to_pylist() == [0, 1, 2]
    assert table.column("name").to_pylist() == ["charlie", "alice", "bob"]


def test_existing_row_order_column_with_explicit_schema_accepted(project, monkeypatch):
    """A CSV carrying a valid 0..N-1 'original_row_order' column plus an explicit
    schema for its DATA columns must ingest. The reserved column is tallyman's, not
    the caller's to spec, so the totality check must not demand it be named (pre-fix
    it raised 'schema is not total — it leaves column(s) [original_row_order]')."""
    import pyarrow.parquet as pq

    from tallyman_xorq.result_cache import baked_snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "orderschema.csv"
    # original_row_order present; rows deliberately NOT sorted by name.
    p.write_text("original_row_order,id,name\n0,1,charlie\n1,2,alice\n2,3,bob\n")
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(p)!r}, schema={{"id": "int64", "name": "string"}})
"""
    res = catalog_create("orderschema", code)
    assert "error" not in res, res
    h = _hash_of(project)
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    table = pq.read_table(str(snap)).sort_by("original_row_order")
    assert table.column("original_row_order").to_pylist() == [0, 1, 2]
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("name").to_pylist() == ["charlie", "alice", "bob"]


def test_existing_row_order_column_nonsequential_raises(project, monkeypatch):
    """An 'original_row_order' column that is NOT the contiguous 0..N-1 sequence
    (here 1..N) is a coincidental name clash — ingest must raise rather than
    silently trust a non-canonical order."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "badorder.csv"
    p.write_text("original_row_order,name\n1,alice\n2,bob\n3,charlie\n")
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(p)!r})
"""
    res = catalog_create("badorder", code)
    assert "error" in res
    err = res["error"]
    assert "original_row_order" in err
    assert "reserved" in err.lower() or "0.." in err or "contiguous" in err.lower()


def test_existing_row_order_column_noninteger_raises(project, monkeypatch):
    """An 'original_row_order' column whose values aren't integers cannot be the
    canonical row index — ingest must raise with the reserved-name message."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "strorder.csv"
    p.write_text("original_row_order,name\na,alice\nb,bob\n")
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(p)!r})
"""
    res = catalog_create("strorder", code)
    assert "error" in res
    err = res["error"]
    assert "original_row_order" in err
    assert "reserved" in err.lower() or "integer" in err.lower() or "0.." in err
