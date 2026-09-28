"""Content digest of a parquet file (ADR-009 D2): what ``result_digest`` is.

A SHA-256 over the file's ordered Arrow data, read back. It does not depend on how the rows were batched or grouped
when the file was written, on the codec, on the writer's version, on whether a text column is ``string``,
``large_string`` or ``string_view``, or on what a null slot happens to hold. It does depend on every value, on which
slots are null, on the order of the rows and on the column names and logical types.

Each column keeps one hash stream per kind of byte: validity (one byte per row), lengths (variable-width types) and
values. One hasher per column would interleave the three chunk by chunk, and the digest would then depend on where
the batch boundaries fell. A nested column keeps the same three streams for itself and one set for each child.

The digest of a column and the digest of the file are both exposed: the file digest is what ``manifest.result_digest``
records, and the per-column digests name the columns that differ between two runs of a recipe that is not
reproducible (ADR-009 D6).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ALGORITHM = "arrow-sha256"
_STREAMS = ("validity", "lengths", "values")
_READ_BATCH_ROWS = 65_536


def logical_type(dtype: pa.DataType) -> str:
    """The type a column is hashed under: physical spellings of one logical type collapse to one name."""
    if pa.types.is_dictionary(dtype):
        return logical_type(dtype.value_type)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
        return "string"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
        return "binary"
    if pa.types.is_map(dtype):
        return f"map<{logical_type(dtype.key_type)},{logical_type(dtype.item_type)}>"
    if _is_list(dtype):
        return f"list<{logical_type(dtype.value_type)}>"
    if pa.types.is_struct(dtype):
        return "struct<" + ",".join(f"{f.name}:{logical_type(f.type)}" for f in dtype) + ">"
    return str(dtype)


def _is_list(dtype: pa.DataType) -> bool:
    return (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
        or pa.types.is_list_view(dtype)
        or pa.types.is_large_list_view(dtype)
    )


class _ColumnHasher:
    """The three streams of one column, plus the hashers of its children when the column is nested."""

    def __init__(self, label: str, dtype: pa.DataType):
        logical = logical_type(dtype)
        self.streams = {s: hashlib.sha256(f"{label}\x00{logical}\x00{s}".encode()) for s in _STREAMS}
        self.children: list[_ColumnHasher] = []
        self.dtype = dtype
        if pa.types.is_dictionary(dtype):
            self.dtype = dtype.value_type
        if pa.types.is_map(self.dtype):
            entries = pa.struct([pa.field("key", self.dtype.key_type), pa.field("value", self.dtype.item_type)])
            self.children = [_ColumnHasher(f"{label}.entries", entries)]
        elif _is_list(self.dtype):
            self.children = [_ColumnHasher(f"{label}.item", self.dtype.value_type)]
        elif pa.types.is_struct(self.dtype):
            self.children = [_ColumnHasher(f"{label}.{f.name}", f.type) for f in self.dtype]

    def update(self, arr: pa.Array | pa.ChunkedArray) -> None:
        if isinstance(arr, pa.ChunkedArray):
            for chunk in arr.chunks:
                self.update(chunk)
            return
        if pa.types.is_dictionary(arr.type):
            arr = arr.dictionary_decode()
        nulls = arr.is_null().to_numpy(zero_copy_only=False)
        self.streams["validity"].update(nulls.astype(np.uint8).tobytes())
        dtype = arr.type
        if pa.types.is_null(dtype):
            return
        if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
            self._update_varlen(arr.cast(pa.large_string()), b"")
        elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
            self._update_varlen(arr.cast(pa.large_binary()), b"")
        elif pa.types.is_boolean(dtype):
            self.streams["values"].update(pc.fill_null(arr, False).to_numpy(zero_copy_only=False).astype(np.uint8))
        elif pa.types.is_map(dtype):
            entries = pa.struct([pa.field("key", dtype.key_type), pa.field("value", dtype.item_type)])
            self._update_list(arr.cast(pa.list_(entries)), nulls)  # a map is a list of (key, value) entries
        elif _is_list(dtype):
            self._update_list(arr, nulls)
        elif pa.types.is_struct(dtype):
            for child, values in zip(self.children, arr.flatten()):
                child.update(values)
        elif _fixed_width(dtype):
            self._update_fixed(arr, nulls, dtype.bit_width // 8)
        else:
            # Anything not covered above (unions, extension types): hash the Python values. Slow, and correct.
            self.streams["values"].update(repr(arr.to_pylist()).encode())

    def _update_varlen(self, arr: pa.Array, empty: bytes) -> None:
        filled = pc.fill_null(arr, empty.decode() if pa.types.is_large_string(arr.type) else empty)
        lengths = pc.binary_length(filled).to_numpy(zero_copy_only=False).astype(np.int64)
        self.streams["lengths"].update(lengths.tobytes())
        if not len(filled):
            return
        offsets = np.frombuffer(filled.buffers()[1], dtype=np.int64)[filled.offset : filled.offset + len(filled) + 1]
        data = filled.buffers()[2]
        if data is not None and int(offsets[-1]) > int(offsets[0]):
            self.streams["values"].update(memoryview(data)[int(offsets[0]) : int(offsets[-1])])

    def _update_fixed(self, arr: pa.Array, nulls: np.ndarray, width: int) -> None:
        if not len(arr):
            return
        raw = np.frombuffer(arr.buffers()[1], dtype=np.uint8)[arr.offset * width : (arr.offset + len(arr)) * width]
        if arr.null_count:
            raw = raw.reshape(len(arr), width).copy()
            raw[nulls] = 0  # a null slot may hold anything; zero it
        self.streams["values"].update(raw.tobytes() if arr.null_count else memoryview(raw))

    def _update_list(self, arr: pa.Array, nulls: np.ndarray) -> None:
        if pa.types.is_fixed_size_list(arr.type):
            lengths = np.full(len(arr), arr.type.list_size, dtype=np.int64)
        else:
            lengths = np.diff(np.asarray(arr.offsets.to_numpy(zero_copy_only=False), dtype=np.int64))
        lengths[nulls] = 0  # a null list may be backed by a non-empty one, and flatten() drops those children
        self.streams["lengths"].update(lengths.tobytes())
        self.children[0].update(arr.flatten())

    def digest(self) -> bytes:
        top = hashlib.sha256()
        for stream in _STREAMS:
            top.update(self.streams[stream].digest())
        for child in self.children:
            top.update(child.digest())
        return top.digest()


def _fixed_width(dtype: pa.DataType) -> bool:
    return isinstance(dtype, pa.FixedSizeBinaryType) or (
        pa.types.is_primitive(dtype) and dtype.bit_width % 8 == 0 and not pa.types.is_boolean(dtype)
    ) or pa.types.is_decimal(dtype) or pa.types.is_interval(dtype)


class _FileHasher:
    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.columns = [_ColumnHasher(f.name, f.type) for f in schema]
        self.rows = 0

    def update(self, batch: pa.RecordBatch | pa.Table) -> None:
        for hasher, column in zip(self.columns, batch.columns):
            hasher.update(column)
        self.rows += batch.num_rows

    def digests(self) -> tuple[str, dict[str, str]]:
        per_column = {f.name: h.digest() for f, h in zip(self.schema, self.columns)}
        top = hashlib.sha256(str(self.rows).encode())
        for digest in per_column.values():
            top.update(digest)
        return f"{ALGORITHM}:{top.hexdigest()}", {name: d.hex() for name, d in per_column.items()}


def digests_of_batches(batches, schema: pa.Schema) -> tuple[str, dict[str, str]]:
    """``(file digest, {column: digest})`` of a record-batch stream, in the order the batches arrive."""
    hasher = _FileHasher(schema)
    for batch in batches:
        hasher.update(batch)
    return hasher.digests()


def file_digests(path: Path) -> tuple[str, dict[str, str]]:
    """``(file digest, {column: digest})`` of a parquet file, read back."""
    pf = pq.ParquetFile(path)
    return digests_of_batches(pf.iter_batches(batch_size=_READ_BATCH_ROWS), pf.schema_arrow)


def content_digest(path: Path) -> str:
    """The digest ``manifest.result_digest`` records: ``arrow-sha256:<hex>``."""
    return file_digests(path)[0]


def column_digests(path: Path) -> dict[str, str]:
    """The digest of each column of a parquet file, by name."""
    return file_digests(path)[1]
