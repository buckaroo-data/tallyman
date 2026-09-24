"""Tests for CSV ingest — the schema DSL and a stable ``__row_order``, at import time.

ADR-004-result-digest-canonical-ordering gave a CSV read an ``original_row_order`` column and a trailing
``order_by`` so that a snapshot's bytes are deterministic. ADR-008 (plans/ADR-008-row-order-of-reads.md) replaced
both: the column is ``__row_order`` (ADR-008 D7, amending ADR-005 INV-1), and polars numbers the rows in file order
rather than a datafusion scan that may repartition (ADR-008 D2, #168).

ADR-011 (plans/ADR-011-sources-are-aliases.md) moved *when* all of that happens. ``tallyman_read_csv`` is a build
error in an authored recipe (ADR-011 D2; that refusal is pinned in tests/test_io.py). A CSV enters the catalog
through ``update_and_depend(path, alias, schema=..., **reader_options)``, which parses it with polars under the same
schema DSL, the same 100 -> 10k -> whole-file inference ladder and the same error messages, and writes ONE parquet
snapshot: the source entry's own, at ``compute_cache/result_cache/<content_hash>.parquet`` (ADR-011 D1). That
snapshot is what ``compute_cache/ordered_sources/<copy key>.parquet`` used to be, so the tests below read it, and
``manifest.result_digest`` is the digest record that ``manifest.ordered_copies`` used to hold.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_core import data_dir, entry_dir, read_manifest
from tallyman_core.paths import compute_cache_dir, tallyman_home
from tallyman_xorq.materialize import ensure_materialized, snapshot_path, snapshots_dir
from tallyman_xorq.result_cache import cached_result_expr, snapshot_file_digest, verify_result_faithful
from tallyman_xorq.source_import import update_and_depend


@pytest.fixture
def sample_csv(project: str) -> Path:
    """Small CSV under the project data dir with a known row ordering.

    ``data/`` stopped being special with ADR-011 D2 — an import takes any path — but the file has to live
    somewhere, and putting it here keeps it inside the per-test isolated home.
    """
    p = data_dir(project) / "sample.csv"
    # Deliberately write rows in an order that is NOT alphabetical by name, so
    # any test that verifies file order can distinguish a correctly-ordered
    # snapshot from an arbitrarily-ordered one.
    p.write_text("id,name,value\n3,charlie,30\n1,alice,10\n2,bob,20\n")
    return p


def _import_sample(project: str, csv_path: Path, alias: str = "sample_csv") -> dict:
    """Import *csv_path* under *alias*, with its three data columns pinned."""
    import xorq.vendor.ibis as ibis

    schema = ibis.schema({"id": "int64", "name": "string", "value": "int64"})
    return update_and_depend(csv_path, alias, project=project, schema=schema)


def _snapshot_table(project: str, content_hash: str):
    """The source entry's snapshot, read straight off disk."""
    import pyarrow.parquet as pq

    return pq.read_table(str(snapshot_path(project, content_hash)))


def test_tallyman_read_csv_adds_row_order(project, sample_csv):
    """A CSV source entry's recorded schema ends in __row_order: int64 (ADR-008 D7)."""
    out = _import_sample(project, sample_csv)
    names = [f["name"] for f in out["schema"]["fields"]]
    schema_fields = {f["name"]: f["type"] for f in out["schema"]["fields"]}
    assert names == ["id", "name", "value", "__row_order"], f"one row-order column, and it is last: {names}"
    assert schema_fields["__row_order"] == "int64"
    assert "original_row_order" not in schema_fields


def test_csv_source_snapshot_is_in_file_order(project, sample_csv):
    """The snapshot holds the CSV's rows in file order, numbered 0..N-1 in ``__row_order`` (ADR-008 D2)."""
    out = _import_sample(project, sample_csv)

    snapshots = sorted(snapshots_dir(project).glob("*.parquet"))
    assert snapshots == [snapshot_path(project, out["hash"])], f"one import, one snapshot: {snapshots}"
    table = _snapshot_table(project, out["hash"])
    assert table.column_names == ["id", "name", "value", "__row_order"]
    assert table["id"].to_pylist() == [3, 1, 2], "file order, not sorted order"
    assert table["__row_order"].to_pylist() == [0, 1, 2]


def test_a_recreated_source_snapshot_matches_its_recorded_digest(project, sample_csv):
    """The manifest records the snapshot's digest, and a re-created snapshot reproduces it (ADR-011 D1).

    The ordered copy this replaces had its own digest record in ``manifest.ordered_copies``. A source version's
    snapshot IS that copy now, so the file whose reproducibility matters is the one ``manifest.result_digest``
    already covers, and ``ensure_materialized`` is what makes it again (from the clone of the imported bytes).
    """
    out = _import_sample(project, sample_csv)
    h = out["hash"]
    recorded = read_manifest(entry_dir(project, h)).result_digest
    assert recorded.startswith("arrow-sha256:"), recorded

    snapshot = snapshot_path(project, h)
    assert snapshot.exists() and snapshot_file_digest(snapshot) == recorded
    snapshot.unlink()
    cached_result_expr.cache_clear()
    ensure_materialized(project, h)

    assert snapshot.exists(), "ensure_materialized must write the source snapshot again"
    assert snapshot_file_digest(snapshot) == recorded, "the re-created snapshot must reproduce the recorded digest"


@pytest.fixture
def repartitioned_csv(project: str) -> tuple[Path, int]:
    """A CSV large enough that a parallel scan repartitions it across threads.

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


def _import_big(project: str, csv_path: Path, alias: str = "big_csv") -> dict:
    import xorq.vendor.ibis as ibis

    schema = ibis.schema({"marker": "int64", "pad": "string"})
    return update_and_depend(csv_path, alias, project=project, schema=schema)


def test_tallyman_read_csv_preserves_file_order_under_repartition(project, repartitioned_csv):
    """__row_order must equal true file order even when a scan of this size repartitions.

    Regression for the canonical-ordering bug: a bare ``ibis.row_number()`` over a
    repartitioned datafusion CSV scan numbers rows in nondeterministic arrival
    order. With the marker column = source row index, the snapshot's row at
    ``__row_order == k`` must carry ``marker == k`` (polars numbers the rows in
    file order, ADR-008 D2).
    """
    csv_path, n = repartitioned_csv
    out = _import_big(project, csv_path)

    table = _snapshot_table(project, out["hash"])
    assert table.column("__row_order").to_pylist() == list(range(n)), "__row_order must be 0..N-1, contiguous"
    marker = table.column("marker").to_pylist()
    # The crux: row k of the source file must land at __row_order == k.
    assert marker == list(range(n)), (
        "__row_order does not match true file order — the scan reshuffle "
        "leaked into the row index (first divergence at "
        f"{next((i for i, m in enumerate(marker) if m != i), None)})"
    )


def test_source_snapshot_digest_stable_under_repartition(project, repartitioned_csv):
    """A re-created snapshot of a repartitioned CSV has the digest recorded when it was first written."""
    csv_path, _ = repartitioned_csv

    out = _import_big(project, csv_path)
    h = out["hash"]
    recorded = read_manifest(entry_dir(project, h)).result_digest

    snapshot = snapshot_path(project, h)
    assert snapshot.exists()
    snapshot.unlink()
    cached_result_expr.cache_clear()
    ensure_materialized(project, h)  # parsed again from the clone, by polars, with the recorded reader options

    assert snapshot.exists()
    assert snapshot_file_digest(snapshot) == recorded, (
        "the snapshot's digest drifted across two independent parses of a repartitioned CSV"
    )


def test_tallyman_read_csv_reconstructs_after_source_deleted(project, sample_csv):
    """#6: once the import has run, reading the entry must not touch the CSV.

    The CSV is read exactly once, at import. After that, deleting (or moving) it must not break the entry: the rows
    are in the entry's own snapshot, and the bytes they were parsed from are in the clone under ``data/.cas``.
    """
    out = _import_sample(project, sample_csv)
    h = out["hash"]

    # Delete the source CSV; the snapshot and the clone remain.
    sample_csv.unlink()

    cached_result_expr.cache_clear()
    df = cached_result_expr(project, h).execute()
    assert df["id"].tolist() == [3, 1, 2], "the entry must read without the source CSV"
    assert verify_result_faithful(project, h) is True, "the snapshot still matches the digest recorded at import"


def test_a_csv_source_snapshot_lives_in_the_project_compute_cache(project, sample_csv):
    """The snapshot is cache, so it lives under the project's compute_cache (ADR-007 D13, ADR-011 D1).

    The ordered copy it replaces started under TALLYMAN_HOME/csv_ordered, outside every project: never collected,
    not packed, and outside the project root, so a CSV entry's build was not portable. ADR-007 D13 moved it into
    ``compute_cache/ordered_sources``; ADR-011 D1 folded that second store into the entry's own snapshot.
    """
    out = _import_sample(project, sample_csv)

    snapshot = snapshot_path(project, out["hash"])
    assert snapshot.is_file(), f"the snapshot must be under the project's compute cache: {snapshot}"
    assert snapshot.parent.parent == compute_cache_dir(project)
    assert not (compute_cache_dir(project) / "ordered_sources").exists(), "the second source store is retired"
    assert not (tallyman_home() / "csv_ordered").exists(), "csv_ordered under TALLYMAN_HOME is retired"


def test_tallyman_read_csv_forwards_reader_kwargs(project):
    """#10: reader options (separator, skip_rows, ...) are forwarded to polars scan_csv.

    They are named in the import call now and recorded on the entry (ADR-011 D12), so a documented
    ``separator=';'`` must parse the alternate delimiter instead of raising TypeError.
    """
    p = data_dir(project) / "semi.csv"
    p.write_text("id;name\n3;charlie\n1;alice\n2;bob\n")

    out = update_and_depend(p, "semi_src", project=project, schema={"id": "int64", "name": "string"}, separator=";")

    names = [f["name"] for f in out["schema"]["fields"]]
    assert names == ["id", "name", "__row_order"], (
        f"separator=';' not forwarded — columns did not split: {names}"
    )
    assert _snapshot_table(project, out["hash"])["name"].to_pylist() == ["charlie", "alice", "bob"]


# --------------------------------------------------------------------------- #
# The reserved name is now the exact string '__row_order' (ADR-008 D6). A CSV that already
# has a column of that name (a file tallyman exported) has it overwritten, with no
# validation of its values: the import numbers the rows in file order (ADR-008 D2).
# 'original_row_order' is not special any more: it is ordinary data.
# --------------------------------------------------------------------------- #
def test_existing_row_order_column_is_overwritten(project):
    """A CSV that already carries a '__row_order' column ingests with it overwritten by 0..N-1 in file order,
    whatever its values were — no error, no validation, and still exactly one row-order column (last)."""
    p = data_dir(project) / "hasorder.csv"
    # Rows deliberately NOT sorted by name; the incoming __row_order values are not the file sequence.
    p.write_text("__row_order,name\n7,charlie\n3,alice\n5,bob\n")

    out = update_and_depend(p, "hasorder", project=project)

    df = cached_result_expr(project, out["hash"]).execute()
    assert list(df.columns) == ["name", "__row_order"]
    assert df["__row_order"].tolist() == [0, 1, 2]
    assert df["name"].tolist() == ["charlie", "alice", "bob"]


def test_existing_row_order_column_with_explicit_schema_accepted(project):
    """A CSV carrying a '__row_order' column plus an explicit schema for its DATA columns must ingest.
    The reserved column is tallyman's, not the caller's to spec, so the totality check must not demand it be
    named."""
    p = data_dir(project) / "orderschema.csv"
    # __row_order present; rows deliberately NOT sorted by name.
    p.write_text("__row_order,id,name\n7,1,charlie\n3,2,alice\n5,3,bob\n")

    out = update_and_depend(p, "orderschema", project=project, schema={"id": "int64", "name": "string"})

    df = cached_result_expr(project, out["hash"]).execute()
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
def test_original_row_order_is_ordinary_data(project, body, values):
    """'original_row_order' is not reserved any more: ADR-008 D6 reserves only the exact name '__row_order'.

    Before ADR-008 a CSV with such a column raised unless it was the canonical 0..N-1 sequence.
    """
    p = data_dir(project) / "oro.csv"
    p.write_text("original_row_order,name\n" + body)

    out = update_and_depend(p, "oro", project=project)

    df = cached_result_expr(project, out["hash"]).execute()
    assert df["original_row_order"].tolist() == values, "an ordinary column keeps its values"
    assert df["__row_order"].tolist() == list(range(len(values)))


@pytest.mark.parametrize(
    "schema",
    [
        (("__row_order", "int64"), ("&rest", "infer")),  # positional rename target
        (("a", "int64"), ("__row_order", "int64")),  # positional, second column
    ],
)
def test_schema_output_name_row_order_raises(project, schema):
    """A schema that maps a DATA column onto the reserved '__row_order' output name collides with tallyman's
    row index: assigning to the column is not allowed (ADR-008 D6). The import must reject the reserved output
    name with a clear ValueError."""
    p = data_dir(project) / "renameoro.csv"
    p.write_text("a,b\n1,10\n2,20\n")
    with pytest.raises(ValueError, match="__row_order"):
        update_and_depend(p, "renameoro", project=project, schema=schema)


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
def test_suggested_schema_recovery_is_pasteable_with_reserved_column(project):
    """#143 recovery contract, reserved-column path: when an explicit schema fails
    to parse a CSV that carries a '__row_order' column, the
    suggested schema in the error must be paste-ready. The whole-file suggestion
    must NOT emit a cell for the reserved column (which the caller cannot spec) —
    pasting the suggestion back would otherwise over-count the columns.

    The retry also pins that the failed import left nothing behind: the alias is still unclaimed and no entry was
    written, so the second call mints v1 exactly as the first would have.
    """
    import ast

    from tallyman_core.aliases import alias_kind
    from tallyman_xorq.build import list_entries

    p = data_dir(project) / "suggest_oro.csv"
    # __row_order present (as tallyman exports it); the amount column holds a float
    # that an int64 pin cannot parse, so the explicit-mode suggestion path fires.
    p.write_text("id,amount,__row_order\n1,12.5,0\n2,3.0,1\n")
    with pytest.raises(ValueError) as exc:
        update_and_depend(p, "suggest_oro", project=project, schema=(("id", "int64"), ("amount", "int64")))
    msg = str(exc.value)
    assert "Suggested schema" in msg, msg
    assert "__row_order" not in msg, f"suggestion leaked the reserved column: {msg}"

    assert alias_kind(project, "suggest_oro") is None, "a failed import must not claim the alias"
    assert list_entries(project) == [], "a failed import must not leave a half-written entry"

    # The suggestion must paste back and parse.
    suggested = ast.literal_eval(msg.rsplit("schema=", 1)[-1].strip())
    out = update_and_depend(p, "suggest_oro", project=project, schema=suggested)
    assert (out["version"], out["created"]) == (1, True)
    types = {name: str(dtype) for name, dtype in cached_result_expr(project, out["hash"]).schema().items()}
    assert types.get("amount") == "float64", types
    assert "__row_order" in types  # the reserved column is still there, numbered by tallyman


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param((("a", "int64"), ("b", "int64"), ("c", "int64")), id="over-long positional spec"),
        # A guard: this one passes today, because the header listing in a by-name miss already names every column.
        pytest.param({"zzz": "int64", "&rest": "infer"}, id="by-name miss (a guard)"),
    ],
)
def test_schema_error_diagnostic_names_reserved_column(project, schema):
    """A schema error against a CSV that carries '__row_order' must not hide
    that column. The diagnostic must not print the reserved-stripped header: a
    3-column file (a,b,__row_order) reported as 'has 2 [a, b]' is an off-by-one that the user, staring at a
    3-column file, cannot reconcile. The reserved column must appear in the message."""
    p = data_dir(project) / "diag_oro.csv"
    p.write_text("a,b,__row_order\n1,2,0\n3,4,1\n")
    with pytest.raises(ValueError) as exc:
        update_and_depend(p, "diag_oro", project=project, schema=schema)
    assert "__row_order" in str(exc.value), (
        f"diagnostic hides the reserved column: {exc.value}"
    )


def test_positional_schema_rejects_nontrailing_reserved_column(project):
    """Positional cells bind by physical column position. A '__row_order' column that is NOT the last column
    shifts that mapping — excluding it from the middle silently rebinds later cells onto the wrong data
    column (renaming/dropping a column with no error). The import must reject a
    positional schema in this layout rather than silently corrupt the output."""
    p = data_dir(project) / "oro_middle.csv"
    # __row_order sits in the MIDDLE, not trailing.
    p.write_text("sku,__row_order,qty\nA,0,10\nB,1,20\n")
    with pytest.raises(ValueError) as exc:
        update_and_depend(p, "oro_middle", project=project, schema=(("sku", "string"), ("row", "int64")))
    msg = str(exc.value).lower()
    assert "__row_order" in msg and ("position" in msg or "last" in msg or "by-name" in msg), (
        f"expected a positional/last-column guard message; got: {exc.value}"
    )
