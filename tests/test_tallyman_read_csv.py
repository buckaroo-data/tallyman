"""Tests for tallyman_read_csv — CSV ingest with a stable ``__row_order``.

ADR-004-result-digest-canonical-ordering gave a CSV read an ``original_row_order`` column and a trailing ``order_by``
so that a snapshot's bytes are deterministic. ADR-008 (plans/ADR-008-row-order-of-reads.md) replaces both: the column
is ``__row_order`` (ADR-008 D7, amending ADR-005 INV-1), the trailing sort is gone so a CSV root is a cheap read
(ADR-008 D7, amending ADR-005 INV-2), and the ordered copy of the CSV lives in the project's
``compute_cache/ordered_sources`` (ADR-007 D13), keyed by the CSV's content (ADR-008 D2, #168).
"""
from __future__ import annotations

from pathlib import Path

import pytest
from polars.exceptions import PanicException

from tallyman_core import data_dir, entry_dir, read_manifest
from tallyman_core.paths import compute_cache_dir, tallyman_home
from tallyman_mcp.server import catalog_create
from tallyman_xorq.build import BuildError, build_and_persist, list_entries
from tallyman_xorq.ordered_copy import SourceUnavailable
from tallyman_xorq.result_cache import (
    baked_snapshot_path,
    cache_worthy,
    cached_result_expr,
    snapshot_file_digest,
    verify_result_faithful,
)


@pytest.fixture
def sample_csv(project: str) -> Path:
    """Small CSV under the project data dir with a known row ordering."""
    p = data_dir(project) / "sample.csv"
    # Deliberately write rows in an order that is NOT alphabetical by name, so
    # any test that verifies file order can distinguish a correctly-ordered
    # copy from an arbitrarily-ordered one.
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


def _ordered_copies(project: str) -> list[Path]:
    """The ordered copies of sources: ADR-007 D13 puts them under the project's compute cache."""
    return sorted((compute_cache_dir(project) / "ordered_sources").glob("*.parquet"))


def test_tallyman_read_csv_adds_row_order(project, sample_csv, monkeypatch):
    """tallyman_read_csv returns an expression whose schema ends in __row_order: int64 (ADR-008 D7)."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res
    names = [f["name"] for f in res["schema"]["fields"]]
    schema_fields = {f["name"]: f["type"] for f in res["schema"]["fields"]}
    assert names == ["id", "name", "value", "__row_order"], f"one row-order column, and it is last: {names}"
    assert schema_fields["__row_order"] == "int64"
    assert "original_row_order" not in schema_fields


def test_tallyman_read_csv_entry_is_cheap(project, sample_csv, monkeypatch):
    """A tallyman_read_csv entry is a plain read of its ordered copy, so it is cheap (ADR-008 D7).

    Before ADR-008 its trailing ``order_by`` made every entry in a CSV lineage worthy for that Sort alone, and each
    revision baked a full sorted copy.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("csv_entry", _read_csv_code(sample_csv))
    h = _hash_of(project)
    assert cache_worthy(project, h) is False, "a tallyman_read_csv root has no Sort, so it is a cheap read"
    assert read_manifest(entry_dir(project, h)).cache_worthy is False


def test_tallyman_read_csv_bakes_no_snapshot(project, sample_csv, monkeypatch):
    """A CSV root writes no snapshot: its rows are fixed by the content-keyed ordered copy (ADR-008 D7)."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res
    h = _hash_of(project)

    assert baked_snapshot_path(project, h) is None, "a cheap entry has no snapshot"
    snapshots = compute_cache_dir(project) / "result_cache"
    assert not snapshots.exists() or not list(snapshots.glob("*.parquet")), "nothing was materialized"


def test_tallyman_read_csv_ordered_copy_is_in_file_order(project, sample_csv, monkeypatch):
    """The ordered copy holds the CSV's rows in file order, numbered 0..N-1 in ``__row_order`` (ADR-008 D2)."""
    import pyarrow.parquet as pq

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("csv_entry", _read_csv_code(sample_csv))

    copies = _ordered_copies(project)
    assert len(copies) == 1, f"expected one ordered copy under compute_cache/ordered_sources, found {copies}"
    table = pq.read_table(str(copies[0]))
    assert table.column_names == ["id", "name", "value", "__row_order"]
    assert table["id"].to_pylist() == [3, 1, 2], "file order, not sorted order"
    assert table["__row_order"].to_pylist() == [0, 1, 2]


def test_a_recreated_ordered_copy_matches_its_recorded_digest(project, sample_csv, monkeypatch):
    """The manifest records the copy's content digest, and a re-created copy reproduces it (ADR-007 D13).

    Replaces the check that a CSV root's snapshot digest is stable across a heal: a CSV root is cheap now, so the
    file whose reproducibility matters is the ordered copy.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res
    h = _hash_of(project)

    records = read_manifest(entry_dir(project, h)).ordered_copies
    assert records and len(records) == 1, f"the manifest must record the ordered copy: {records}"
    ((key, record),) = records.items()
    recorded = record["content_digest"]
    assert recorded.startswith("arrow-sha256:"), recorded

    copy = compute_cache_dir(project) / "ordered_sources" / f"{key}.parquet"
    assert copy.exists() and snapshot_file_digest(copy) == recorded
    copy.unlink()
    cached_result_expr.cache_clear()
    cached_result_expr(project, h)  # opening the entry makes the copy again from the clone

    assert copy.exists(), "opening the entry must re-create its ordered copy"
    assert snapshot_file_digest(copy) == recorded, "the re-created copy must reproduce the recorded digest"


@pytest.fixture
def repartitioned_csv(project: str) -> tuple[Path, int]:
    """A CSV large enough that datafusion repartitions the scan across threads.

    The marker column holds the source row index (0..N-1) in file order, so a
    correct ``__row_order`` must line up with it row-for-row. The file is
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
    """__row_order must equal true file order even when the scan repartitions.

    Regression for the canonical-ordering bug: a bare ``ibis.row_number()`` over a
    repartitioned datafusion CSV scan numbers rows in nondeterministic arrival
    order. With the marker column = source row index, the ordered copy's row at
    ``__row_order == k`` must carry ``marker == k`` (polars numbers the rows in
    file order, ADR-008 D2).
    """
    import pyarrow.parquet as pq

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    csv_path, n = repartitioned_csv
    res = catalog_create("big_csv", _big_read_csv_code(csv_path))
    assert "error" not in res, res

    copies = _ordered_copies(project)
    assert len(copies) == 1, f"expected one ordered copy under compute_cache/ordered_sources, found {copies}"
    table = pq.read_table(str(copies[0]))
    assert table.column("__row_order").to_pylist() == list(range(n)), "__row_order must be 0..N-1, contiguous"
    marker = table.column("marker").to_pylist()
    # The crux: row k of the source file must land at __row_order == k.
    assert marker == list(range(n)), (
        "__row_order does not match true file order — the scan reshuffle "
        "leaked into the row index (first divergence at "
        f"{next((i for i, m in enumerate(marker) if m != i), None)})"
    )


def test_ordered_copy_digest_stable_under_repartition(project, repartitioned_csv, monkeypatch):
    """A re-created ordered copy of a repartitioned CSV has the digest recorded when it was first written."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    csv_path, _ = repartitioned_csv

    res = catalog_create("big_csv", _big_read_csv_code(csv_path))
    assert "error" not in res, res
    h = _hash_of(project)
    ((key, record),) = read_manifest(entry_dir(project, h)).ordered_copies.items()
    recorded = record["content_digest"]

    copy = compute_cache_dir(project) / "ordered_sources" / f"{key}.parquet"
    assert copy.exists()
    copy.unlink()
    cached_result_expr.cache_clear()
    cached_result_expr(project, h)  # re-created from the clone by the read

    assert copy.exists()
    assert snapshot_file_digest(copy) == recorded, (
        "the ordered copy's digest drifted across two independent ingests of a repartitioned CSV"
    )


def test_tallyman_read_csv_reconstructs_after_source_deleted(project, sample_csv, monkeypatch):
    """#6: once the ordered copy is written, reading the entry must not touch the CSV.

    The CSV is read exactly once — at ingest. After that, deleting (or moving) the source CSV must not break the
    entry: it is a cheap read of the ordered copy, so there is no snapshot to resolve and no digest to verify.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res
    h = _hash_of(project)
    assert baked_snapshot_path(project, h) is None, "a CSV root is a cheap entry: nothing is baked"

    # Delete the source CSV; the ordered copy remains.
    sample_csv.unlink()

    cached_result_expr.cache_clear()
    df = cached_result_expr(project, h).execute()
    assert df["id"].tolist() == [3, 1, 2], "the entry must read without the source CSV"
    assert verify_result_faithful(project, h) is None, "a cheap entry records no digest to verify"


def test_ordered_csv_copy_lives_in_the_project_compute_cache(project, sample_csv, monkeypatch):
    """The ordered copy is cache, so it lives under the project's compute_cache (ADR-007 D13).

    It used to live under TALLYMAN_HOME/csv_ordered, outside every project: never collected, not packed, and
    outside the project root, so a CSV entry's build was not portable.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = catalog_create("csv_entry", _read_csv_code(sample_csv))
    assert "error" not in res, res

    assert _ordered_copies(project), "the ordered copy must live under compute_cache/ordered_sources"
    assert not list((tallyman_home() / "csv_ordered").glob("*.parquet")), "csv_ordered under TALLYMAN_HOME is retired"


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
    assert {"id", "name", "__row_order"} <= fields, (
        f"separator=';' not forwarded — columns did not split: {fields}"
    )


def _renaming_code(csv_path: Path, with_column_names: str) -> str:
    return f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(csv_path)!r}, with_column_names={with_column_names})
"""


def _create_without_panic(name: str, code: str) -> dict:
    """``catalog_create``, with a polars ``PanicException`` turned into a test failure.

    The panic is a ``BaseException``, so it would otherwise escape both the MCP tool and the test.
    """
    try:
        return catalog_create(name, code)
    except PanicException as exc:
        pytest.fail(f"polars panicked and the PanicException escaped catalog_create: {exc}")


def test_a_function_reader_option_reaches_polars_as_given(project, sample_csv, monkeypatch):
    """#198: the first ingest hands polars the caller's own reader options, not the JSON form the manifest records.

    JSON turns a function into its repr, so ``with_column_names=lambda ...`` reached polars as a string and polars
    panicked calling it.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = _create_without_panic("upper", _renaming_code(sample_csv, "lambda cols: [c.upper() for c in cols]"))
    assert "error" not in res, res
    assert [f["name"] for f in res["schema"]["fields"]] == ["ID", "NAME", "VALUE", "__row_order"]


def test_a_deleted_copy_read_with_a_function_option_cannot_be_made_again(project, sample_csv, monkeypatch):
    """#198: the manifest records only the function's repr (``lossless: False``), so a deleted copy of the read cannot
    be made again from it, and opening the entry says so."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = _create_without_panic("upper", _renaming_code(sample_csv, "lambda cols: [c.upper() for c in cols]"))
    assert "error" not in res, res
    [copy] = _ordered_copies(project)
    assert read_manifest(entry_dir(project, res["hash"])).ordered_copies[copy.stem]["reader"]["lossless"] is False

    copy.unlink()
    cached_result_expr.cache_clear()
    with pytest.raises(SourceUnavailable, match="cannot be made again"):
        cached_result_expr(project, res["hash"])


def test_a_polars_panic_reading_a_csv_is_a_build_error(project, sample_csv):
    """#197: polars panics when a function it calls raises. The panic is a ``BaseException``, which no ``except
    Exception`` on the build or MCP path catches, so ingest turns it into a ``BuildError``."""
    # The typo `colz` is a NameError when polars calls the function.
    code = _renaming_code(sample_csv, "lambda cols: [c.upper() for c in colz]")
    try:
        with pytest.raises(BuildError, match="polars panicked"):
            build_and_persist(project, code)
    except PanicException as exc:
        pytest.fail(f"polars panicked and the PanicException escaped the build: {exc}")


# --------------------------------------------------------------------------- #
# The reserved name is now the exact string '__row_order' (ADR-008 D6). A CSV that already
# has a column of that name (a file tallyman exported) has it overwritten, with no
# validation of its values: the ordered copy numbers the rows in file order (ADR-008 D2).
# 'original_row_order' is not special any more: it is ordinary data.
# --------------------------------------------------------------------------- #
def test_existing_row_order_column_is_overwritten(project, monkeypatch):
    """A CSV that already carries a '__row_order' column ingests with it overwritten by 0..N-1 in file order,
    whatever its values were — no error, no validation, and still exactly one row-order column (last)."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "hasorder.csv"
    # Rows deliberately NOT sorted by name; the incoming __row_order values are not the file sequence.
    p.write_text("__row_order,name\n7,charlie\n3,alice\n5,bob\n")
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(p)!r})
"""
    res = catalog_create("hasorder", code)
    assert "error" not in res, res
    h = _hash_of(project)
    df = cached_result_expr(project, h).execute()
    assert list(df.columns) == ["name", "__row_order"]
    assert df["__row_order"].tolist() == [0, 1, 2]
    assert df["name"].tolist() == ["charlie", "alice", "bob"]


def test_existing_row_order_column_with_explicit_schema_accepted(project, monkeypatch):
    """A CSV carrying a '__row_order' column plus an explicit schema for its DATA columns must ingest.
    The reserved column is tallyman's, not the caller's to spec, so the totality check must not demand it be
    named."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "orderschema.csv"
    # __row_order present; rows deliberately NOT sorted by name.
    p.write_text("__row_order,id,name\n7,1,charlie\n3,2,alice\n5,3,bob\n")
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(p)!r}, schema={{"id": "int64", "name": "string"}})
"""
    res = catalog_create("orderschema", code)
    assert "error" not in res, res
    h = _hash_of(project)
    df = cached_result_expr(project, h).execute()
    assert df["__row_order"].tolist() == [0, 1, 2]
    assert df["id"].tolist() == [1, 2, 3]
    assert df["name"].tolist() == ["charlie", "alice", "bob"]


@pytest.mark.parametrize(
    ("body", "values"),
    [
        pytest.param("1,alice\n2,bob\n3,charlie\n", [1, 2, 3], id="not the 0..N-1 sequence"),
        pytest.param("a,alice\nb,bob\n", ["a", "b"], id="not integers"),
    ],
)
def test_original_row_order_is_ordinary_data(project, monkeypatch, body, values):
    """'original_row_order' is not reserved any more: ADR-008 D6 reserves only the exact name '__row_order'.

    Before ADR-008 a CSV with such a column raised unless it was the canonical 0..N-1 sequence.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "oro.csv"
    p.write_text("original_row_order,name\n" + body)
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(p)!r})
"""
    res = catalog_create("oro", code)
    assert "error" not in res, res
    df = cached_result_expr(project, _hash_of(project)).execute()
    assert df["original_row_order"].tolist() == values, "an ordinary column keeps its values"
    assert df["__row_order"].tolist() == list(range(len(values)))


@pytest.mark.parametrize(
    "schema",
    [
        (("__row_order", "int64"), ("&rest", "infer")),  # positional rename target
        (("a", "int64"), ("__row_order", "int64")),  # positional, second column
    ],
)
def test_schema_output_name_row_order_raises(project, monkeypatch, schema):
    """A schema that maps a DATA column onto the reserved '__row_order' output name collides with tallyman's
    row index: assigning to the column is not allowed (ADR-008 D6). Ingest must reject the reserved output
    name with a clear ValueError."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.io import tallyman_read_csv

    p = data_dir(project) / "renameoro.csv"
    p.write_text("a,b\n1,10\n2,20\n")
    with pytest.raises(ValueError, match="__row_order"):
        tallyman_read_csv(str(p), schema=schema)


# --------------------------------------------------------------------------- #
# Reserved-column exclusion, threaded consistently (review follow-ups).
#
# The totality check excludes tallyman's reserved '__row_order' column so
# a complete DATA-column schema is not spuriously "not total". Three surfaces
# read the header and must apply that same exclusion consistently, or the
# recovery / diagnostic / positional-binding contracts break in exactly the path
# the exclusion newly enables:
#   1. the #143 suggested-schema recovery hint must stay paste-ready,
#   2. schema-error diagnostics must not hide the reserved column, and
#   3. positional binding must not silently rebind onto the wrong data column.
# --------------------------------------------------------------------------- #
def test_suggested_schema_recovery_is_pasteable_with_reserved_column(project, monkeypatch):
    """#143 recovery contract, reserved-column path: when an explicit schema fails
    to parse a CSV that carries a '__row_order' column, the
    suggested schema in the error must be paste-ready. The whole-file suggestion
    must NOT emit a cell for the reserved column (which the caller cannot spec) —
    pasting the suggestion back would otherwise over-count the columns."""
    import ast

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.io import tallyman_read_csv

    p = data_dir(project) / "suggest_oro.csv"
    # __row_order present (as tallyman exports it); the amount column holds a float
    # that an int64 pin cannot parse, so the explicit-mode suggestion path fires.
    p.write_text("id,amount,__row_order\n1,12.5,0\n2,3.0,1\n")
    with pytest.raises(ValueError) as exc:
        tallyman_read_csv(str(p), schema=(("id", "int64"), ("amount", "int64")))
    msg = str(exc.value)
    assert "Suggested schema" in msg, msg
    assert "__row_order" not in msg, f"suggestion leaked the reserved column: {msg}"

    # The suggestion must paste back and parse.
    suggested = ast.literal_eval(msg.rsplit("schema=", 1)[-1].strip())
    expr = tallyman_read_csv(str(p), schema=suggested)
    out = {k: str(v) for k, v in expr.schema().items()}
    assert out.get("amount") == "float64", out
    assert "__row_order" in out  # the reserved column is still there, numbered by tallyman


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param((("a", "int64"), ("b", "int64"), ("c", "int64")), id="over-long positional spec"),
        # A guard: this one passes today, because the header listing in a by-name miss already names every column.
        pytest.param({"zzz": "int64", "&rest": "infer"}, id="by-name miss (a guard)"),
    ],
)
def test_schema_error_diagnostic_names_reserved_column(project, monkeypatch, schema):
    """A schema error against a CSV that carries '__row_order' must not hide
    that column. The diagnostic must not print the reserved-stripped header: a
    3-column file (a,b,__row_order) reported as 'has 2 [a, b]' is an off-by-one that the user, staring at a
    3-column file, cannot reconcile. The reserved column must appear in the message."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.io import tallyman_read_csv

    p = data_dir(project) / "diag_oro.csv"
    p.write_text("a,b,__row_order\n1,2,0\n3,4,1\n")
    with pytest.raises(ValueError) as exc:
        tallyman_read_csv(str(p), schema=schema)
    assert "__row_order" in str(exc.value), (
        f"diagnostic hides the reserved column: {exc.value}"
    )


def test_positional_schema_rejects_nontrailing_reserved_column(project, monkeypatch):
    """Positional cells bind by physical column position. A '__row_order' column that is NOT the last column
    shifts that mapping — excluding it from the middle silently rebinds later cells onto the wrong data
    column (renaming/dropping a column with no error). Ingest must reject a
    positional schema in this layout rather than silently corrupt the output."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.io import tallyman_read_csv

    p = data_dir(project) / "oro_middle.csv"
    # __row_order sits in the MIDDLE, not trailing.
    p.write_text("sku,__row_order,qty\nA,0,10\nB,1,20\n")
    with pytest.raises(ValueError) as exc:
        tallyman_read_csv(str(p), schema=(("sku", "string"), ("row", "int64")))
    msg = str(exc.value).lower()
    assert "__row_order" in msg and ("position" in msg or "last" in msg or "by-name" in msg), (
        f"expected a positional/last-column guard message; got: {exc.value}"
    )
