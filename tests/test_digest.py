"""ADR-009 D2: ``result_digest`` is a content digest of the snapshot, computed from the file read back.

``snapshot_file_digest(path)`` today is the SHA-256 of the file's bytes, so it moves with the writer's row-group size,
codec, encoding and version, none of which are the result. ADR-009 decision D2 (result_digest is a digest of the
snapshot's content) replaces it with an order-sensitive digest over the Arrow data read back from the file, stored as
``arrow-sha256:<hex>``. These tests pin that definition on parquet files written directly with pyarrow, so no entry
needs to be built.

Every test goes through ``_digest``, which also asserts the ``arrow-sha256:`` prefix. That makes each test a red test
today for the reason the ADR gives (the digest is not a content digest), including the "sees what it must" tests,
which would otherwise pass on a byte hash for the wrong reason.

The per-column digests named by ``tallyman_xorq.digest.column_digests`` (the module is new) are what ADR-009 decision
D6 (create runs the query twice and compares) uses to name the columns whose values differ between two runs.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tallyman_xorq.result_cache import snapshot_file_digest

PREFIX = "arrow-sha256:"
N = 20_000

# The same rows written four ways. None of these settings is the result, so none may move the digest.
FORMATS = {
    "snappy, 2,000-row groups": {"compression": "snappy", "row_group_size": 2_000},
    "zstd, one large group": {
        "compression": "zstd",
        "compression_level": 3,
        "row_group_size": 1_048_576,
        "version": "2.6",
        "data_page_version": "1.0",
    },
    "uncompressed, no dictionary, page v2, 7,500-row groups": {
        "compression": "none",
        "use_dictionary": False,
        "data_page_version": "2.0",
        "row_group_size": 7_500,
    },
    "snappy, 999-row groups": {"compression": "snappy", "row_group_size": 999},
}


def _digest(path: Path) -> str:
    """The file's digest, asserted to be an ``arrow-sha256:<64 hex>`` content digest (ADR-009 D2)."""
    d = snapshot_file_digest(path)
    assert d.startswith(PREFIX), f"expected an {PREFIX}<hex> content digest, got {d!r}"
    hexpart = d[len(PREFIX) :]
    assert len(hexpart) == 64 and all(c in "0123456789abcdef" for c in hexpart), d
    return d


def _column_digests(path: Path) -> dict[str, str]:
    import tallyman_xorq.digest as digest_module

    return digest_module.column_digests(path)


def _write(table: pa.Table, path: Path, **settings) -> Path:
    pq.write_table(table, path, **settings)
    return path


def _table() -> pa.Table:
    """Ints, floats with NaNs and nulls, strings with nulls, booleans and timestamps: the shapes a snapshot holds."""
    rng = np.random.default_rng(11)
    ints = rng.integers(0, 1_000_000, N)
    floats = rng.random(N)
    floats[rng.integers(0, N, 40)] = np.nan
    return pa.table(
        {
            "id": pa.array(np.arange(N)),
            "g": pa.array(ints),
            "f": pa.array(floats, mask=rng.random(N) < 0.02),
            "s": pa.array([f"k{v % 500:04d}" for v in ints], mask=rng.random(N) < 0.02),
            "b": pa.array(ints % 2 == 0),
            "ts": pa.array(ints.astype("datetime64[s]")),
        }
    )


def _replace(table: pa.Table, name: str, values: pa.Array | pa.ChunkedArray) -> pa.Table:
    return table.set_column(table.schema.get_field_index(name), name, values)


def _with_changed_int(table: pa.Table) -> pa.Table:
    g = table["g"].to_numpy().copy()
    g[12_345] += 1
    return _replace(table, "g", pa.array(g))


def _with_null_replaced_by_zero(table: pa.Table) -> pa.Table:
    f = table["f"].combine_chunks()
    first_null = int(np.flatnonzero(f.is_null().to_numpy(zero_copy_only=False))[0])
    patched = pa.concat_arrays([f.slice(0, first_null), pa.array([0.0]), f.slice(first_null + 1)])
    return _replace(table, "f", patched)


def _with_first_two_rows_swapped(table: pa.Table) -> pa.Table:
    return table.take(pa.array(np.r_[1, 0, np.arange(2, N)]))


# ---------------------------------------------------------------------------
# the definition: a content digest, prefixed with its algorithm
# ---------------------------------------------------------------------------


def test_digest_carries_its_algorithm_prefix(tmp_path):
    """ADR-009 D2 (result_digest is a digest of the snapshot's content): stored as ``arrow-sha256:<hex>``.

    The prefix means a future definition can never be compared against this one by accident.
    """
    assert _digest(_write(_table(), tmp_path / "a.parquet")) == _digest(_write(_table(), tmp_path / "b.parquet"))


def test_digest_ignores_row_group_size_codec_and_encoding(tmp_path):
    """ADR-009 D2 (digest ignores batching and format): the same rows written four ways give one digest.

    Today the digest is a hash of the file's bytes, so each of these files gets its own.
    """
    table = _table()
    digests = {
        label: _digest(_write(table, tmp_path / f"{i}.parquet", **kw)) for i, (label, kw) in enumerate(FORMATS.items())
    }
    assert len(set(digests.values())) == 1, digests


def test_digest_treats_string_and_large_string_as_one_type(tmp_path):
    """ADR-009 D2: each column stream is seeded with its logical type; string and large_string are one type."""
    table = _table()
    wide_fields = [pa.field(f.name, pa.large_string() if pa.types.is_string(f.type) else f.type) for f in table.schema]
    wide = table.cast(pa.schema(wide_fields))
    narrow_path = _write(table, tmp_path / "narrow.parquet")
    wide_path = _write(wide, tmp_path / "wide.parquet")
    assert pq.ParquetFile(narrow_path).schema_arrow.field("s").type == pa.string()
    assert pq.ParquetFile(wide_path).schema_arrow.field("s").type == pa.large_string()
    assert _digest(narrow_path) == _digest(wide_path)


def test_digest_ignores_how_nulls_are_encoded_in_the_file(tmp_path):
    """ADR-009 D2: null slots are zeroed or emptied before hashing, because a null slot may hold anything.

    A parquet file cannot carry garbage in a null slot, but a reader may hand back different bytes there depending on
    how the column was encoded (dictionary or plain) and paged (v1 or v2). Those must not reach the digest.
    """
    table = _table()
    plain = _write(table, tmp_path / "plain.parquet", use_dictionary=False, data_page_version="2.0", compression="none")
    dictionary = _write(
        table, tmp_path / "dict.parquet", use_dictionary=True, data_page_version="1.0", compression="zstd"
    )
    assert table["f"].null_count > 0 and table["s"].null_count > 0
    assert _digest(plain) == _digest(dictionary)


def test_digest_of_an_empty_file_is_defined_and_depends_on_the_schema(tmp_path):
    """ADR-009 D2: the streams are combined in schema order together with the row count, so zero rows still digest."""
    empty = _table().slice(0, 0)
    a = _digest(_write(empty, tmp_path / "a.parquet", compression="snappy"))
    b = _digest(_write(empty, tmp_path / "b.parquet", compression="zstd"))
    one_row = _digest(_write(_table().slice(0, 1), tmp_path / "one.parquet"))
    other_schema = _digest(_write(empty.select(["id", "g"]), tmp_path / "other.parquet"))
    assert a == b
    assert a != one_row
    assert a != other_schema


# ---------------------------------------------------------------------------
# what the digest must see
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "change",
    [_with_changed_int, _with_null_replaced_by_zero, _with_first_two_rows_swapped],
    ids=["one value changed", "one null replaced by 0.0", "first two rows swapped"],
)
def test_digest_sees_what_it_must(tmp_path, change):
    """ADR-009 D2 (digest sees what it must): a changed value, a null turned into 0.0 and two swapped rows each move it.

    The digest stays order-sensitive, so the canonical sort is still what makes a snapshot reproducible.
    """
    table = _table()
    reference = _digest(_write(table, tmp_path / "reference.parquet"))
    assert _digest(_write(change(table), tmp_path / "changed.parquet")) != reference


def test_digest_covers_column_names_and_types(tmp_path):
    """ADR-009 D2: each stream is seeded with the column's name and logical type, so neither can change silently."""
    table = _table()
    reference = _digest(_write(table, tmp_path / "reference.parquet"))
    renamed = table.rename_columns(["id", "g2", "f", "s", "b", "ts"])
    narrowed = _replace(table, "g", table["g"].cast(pa.int32()))
    assert _digest(_write(renamed, tmp_path / "renamed.parquet")) != reference
    assert _digest(_write(narrowed, tmp_path / "narrowed.parquet")) != reference


# ---------------------------------------------------------------------------
# nested types: the spike covered fixed-width, boolean, string and binary only (ADR-009 open question 1)
# ---------------------------------------------------------------------------


def _nested_table(changed: str | None = None) -> pa.Table:
    """A list, a struct and a map column, each with nulls; ``changed`` alters one value inside that column."""
    n = 4_000
    lists = [[i, i + 1 + (changed == "l" and i == 100)] if i % 7 else None for i in range(n)]
    structs = [
        {"a": i, "b": f"x{i}" if (changed == "st" and i == 200) else f"s{i}"} if i % 11 else None for i in range(n)
    ]
    maps = [[("k", i), ("z", i * 2 + (changed == "m" and i == 300))] if i % 13 else None for i in range(n)]
    return pa.table(
        {
            "l": pa.array(lists, pa.list_(pa.int64())),
            "st": pa.array(structs, pa.struct([("a", pa.int64()), ("b", pa.string())])),
            "m": pa.array(maps, pa.map_(pa.string(), pa.int64())),
        }
    )


def test_nested_columns_digest_deterministically_whatever_the_format(tmp_path):
    """ADR-009 D2: lists, structs and maps need a recursive definition; theirs must also ignore the file format."""
    table = _nested_table()
    a = _digest(_write(table, tmp_path / "a.parquet", compression="snappy", row_group_size=500))
    b = _digest(_write(table, tmp_path / "b.parquet", compression="zstd", row_group_size=1_048_576))
    again = _digest(_write(_nested_table(), tmp_path / "c.parquet", compression="snappy", row_group_size=500))
    assert a == b == again


@pytest.mark.parametrize("column", ["l", "st", "m"])
def test_nested_digest_sees_a_change_inside_the_nested_value(tmp_path, column):
    """ADR-009 D2: one element changed inside a list, a struct field or a map value moves the digest."""
    reference = _digest(_write(_nested_table(), tmp_path / "reference.parquet"))
    assert _digest(_write(_nested_table(changed=column), tmp_path / "changed.parquet")) != reference


# ---------------------------------------------------------------------------
# per-column digests: how a non-reproducible entry names its offending columns
# ---------------------------------------------------------------------------


def test_column_digests_are_keyed_by_column_in_schema_order(tmp_path):
    """ADR-009 D6 (create runs the query twice and compares) needs a digest per column, keyed by name."""
    table = _table()
    digests = _column_digests(_write(table, tmp_path / "a.parquet"))
    assert list(digests) == table.schema.names
    assert all(isinstance(v, str) and v for v in digests.values())


@pytest.mark.parametrize(
    ("change", "column"),
    [(_with_changed_int, "g"), (_with_null_replaced_by_zero, "f")],
    ids=["an int column", "a float column with a null replaced"],
)
def test_column_digests_name_exactly_the_changed_column(tmp_path, change, column):
    """ADR-009 D6: two files that differ in one column differ, per column, in that column only."""
    table = _table()
    reference = _column_digests(_write(table, tmp_path / "reference.parquet"))
    changed = _column_digests(_write(change(table), tmp_path / "changed.parquet"))
    assert {name for name in reference if reference[name] != changed[name]} == {column}
    assert _digest(_write(table, tmp_path / "r2.parquet")) != _digest(_write(change(table), tmp_path / "c2.parquet"))


def test_column_digests_ignore_the_file_format(tmp_path):
    """ADR-009 D2 and D6: the per-column digests are as format-independent as the whole-file digest."""
    table = _table()
    a = _column_digests(_write(table, tmp_path / "a.parquet", compression="snappy", row_group_size=2_000))
    b = _column_digests(_write(table, tmp_path / "b.parquet", compression="zstd", row_group_size=1_048_576))
    assert a == b


def test_column_digests_of_nested_columns_name_the_changed_column(tmp_path):
    """ADR-009 D6: a change inside a struct column is attributed to that column, not to its neighbours."""
    reference = _column_digests(_write(_nested_table(), tmp_path / "reference.parquet"))
    changed = _column_digests(_write(_nested_table(changed="st"), tmp_path / "changed.parquet"))
    assert {name for name in reference if reference[name] != changed[name]} == {"st"}
