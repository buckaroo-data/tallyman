"""Spike for ADR-010 open question 1: an order-insensitive result digest that is stable across library versions.

Each row gets a 64-bit hash built only from Arrow buffers and numpy uint64 arithmetic (wrapping), with mixing
constants defined here, so no library's hash function is involved and the value cannot change with a version bump.
The file digest is two wrapping sums of the row hashes (the second over a re-mixed copy), plus the row count and the
schema, fed to SHA-256. Summing makes it independent of row order and of batching; a duplicated row counts twice.

Normalisation follows `src/tallyman_xorq/digest.py` (logical types, null slots ignored, string/large_string/view
collapse) and adds two float rules the multiset comparison wants: -0.0 hashes as 0.0 and every NaN as one NaN.

Run: uv run python scripts/spike_multiset_digest.py [rows]   (default 18,000,000)
"""

from __future__ import annotations

import hashlib
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ALGORITHM = "arrow-multiset-v1"
_U = np.uint64
_M1 = _U(0xFF51AFD7ED558CCD)
_M2 = _U(0xC4CEB9FE1A85EC53)
_P = _U(0x9E3779B97F4A7C15)  # odd; folds columns into a row and positions into a list
_NULL = _U(0x6A09E667F3BCC909)
_LANE2 = _U(0xBB67AE8584CAA73B)
_BATCH = 65_536


def _mix(x: np.ndarray) -> np.ndarray:
    """murmur3's 64-bit finaliser, vectorised; wraps mod 2**64."""
    x = x ^ (x >> _U(33))
    x = x * _M1
    x = x ^ (x >> _U(33))
    x = x * _M2
    return x ^ (x >> _U(33))


def _seed(label: str) -> np.uint64:
    return _U(int.from_bytes(hashlib.sha256(label.encode()).digest()[:8], "little"))


def _segment_sums(values: np.ndarray, offsets: np.ndarray) -> np.ndarray:
    """Wrapping sum of values[offsets[i]:offsets[i+1]] for each i; empty segments give 0."""
    cs = np.zeros(len(values) + 1, dtype=_U)
    np.cumsum(values, out=cs[1:])
    return cs[offsets[1:]] - cs[offsets[:-1]]


def _fixed_words(arr: pa.Array, width: int) -> list[np.ndarray]:
    """The value bytes of a fixed-width array as uint64 words, one array per 8-byte word (last one zero-padded)."""
    raw = np.frombuffer(arr.buffers()[1], dtype=np.uint8)[arr.offset * width : (arr.offset + len(arr)) * width]
    raw = raw.reshape(len(arr), width)
    padded = (width + 7) // 8 * 8
    if padded != width:
        raw = np.concatenate([raw, np.zeros((len(arr), padded - width), dtype=np.uint8)], axis=1)
    return list(np.ascontiguousarray(raw).view("<u8").T)


def _logical(dtype: pa.DataType) -> str:
    if pa.types.is_dictionary(dtype):
        return _logical(dtype.value_type)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
        return "string"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
        return "binary"
    if pa.types.is_map(dtype):
        return f"map<{_logical(dtype.key_type)},{_logical(dtype.item_type)}>"
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
        return f"list<{_logical(dtype.value_type)}>"
    if pa.types.is_struct(dtype):
        return "struct<" + ",".join(f"{f.name}:{_logical(f.type)}" for f in dtype) + ">"
    return str(dtype)


def column_hashes(arr: pa.Array, seed: np.uint64) -> np.ndarray:
    """One uint64 per slot of `arr`; a null slot hashes to a constant, whatever it holds."""
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if pa.types.is_dictionary(arr.type):
        arr = arr.dictionary_decode()
    n = len(arr)
    dtype = arr.type
    nulls = arr.is_null().to_numpy(zero_copy_only=False) if arr.null_count else None
    if pa.types.is_null(dtype):
        return np.full(n, _NULL, dtype=_U)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
        h = _varlen(pc.fill_null(arr.cast(pa.large_string()), ""), seed)
    elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
        h = _varlen(pc.fill_null(arr.cast(pa.large_binary()), b""), seed)
    elif pa.types.is_boolean(dtype):
        h = _mix(pc.fill_null(arr, False).to_numpy(zero_copy_only=False).astype(_U) ^ seed)
    elif pa.types.is_floating(dtype):
        vals = pc.fill_null(arr, 0).to_numpy(zero_copy_only=False).astype(np.float64)
        vals = np.where(vals == 0.0, 0.0, vals)  # -0.0 -> 0.0
        vals = np.where(np.isnan(vals), np.nan, vals)  # one NaN
        h = _mix(vals.view(_U) ^ seed)
    elif pa.types.is_map(dtype):
        entries = pa.struct([pa.field("key", dtype.key_type), pa.field("value", dtype.item_type)])
        h = _list(arr.cast(pa.large_list(entries)), seed, nulls)
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
        h = _list(arr, seed, nulls)
    elif pa.types.is_struct(dtype):
        h = np.full(n, seed, dtype=_U)
        for i, (field, child) in enumerate(zip(dtype, arr.flatten())):
            h = _mix(h * _P + column_hashes(child, _seed(f"{field.name}\x00{i}") ^ seed))
    elif (
        isinstance(dtype, pa.FixedSizeBinaryType)
        or pa.types.is_decimal(dtype)
        or (pa.types.is_primitive(dtype) and dtype.bit_width % 8 == 0)
    ):
        width = dtype.byte_width if isinstance(dtype, pa.FixedSizeBinaryType) else dtype.bit_width // 8
        h = np.full(n, seed, dtype=_U)
        for word in _fixed_words(arr, width):
            h = _mix(h * _P + word)
    else:  # unions, extension types: slow and correct
        h = np.array([_seed(repr(v)) for v in arr.to_pylist()], dtype=_U) ^ seed
    if nulls is not None:
        h = np.where(nulls, _NULL ^ seed, h)
    return h


def _varlen(arr: pa.Array, seed: np.uint64) -> np.ndarray:
    offsets = np.frombuffer(arr.buffers()[1], dtype=np.int64)[arr.offset : arr.offset + len(arr) + 1]
    start, end = int(offsets[0]), int(offsets[-1])
    data = np.frombuffer(arr.buffers()[2], dtype=np.uint8)[start:end] if end > start else np.zeros(0, np.uint8)
    rel = offsets - start
    lengths = np.diff(rel).astype(_U)
    # Each byte is hashed with its position in its row, and the row's hash is the wrapping sum of those.
    row_of = np.repeat(np.arange(len(arr)), np.diff(rel))
    pos = np.arange(len(data), dtype=np.int64) - rel[:-1][row_of]
    per_byte = _mix(((pos.astype(_U) << _U(8)) | data.astype(_U)) ^ seed)
    return _mix(_segment_sums(per_byte, rel) ^ _mix(lengths ^ seed))


def _list(arr: pa.Array, seed: np.uint64, nulls) -> np.ndarray:
    if pa.types.is_fixed_size_list(arr.type):
        rel = np.arange(len(arr) + 1, dtype=np.int64) * arr.type.list_size
        values = arr.flatten()
    else:
        offs = np.asarray(arr.offsets.to_numpy(zero_copy_only=False), dtype=np.int64)
        values = arr.values.slice(int(offs[0]), int(offs[-1] - offs[0]))
        rel = offs - offs[0]  # a null list's slots may hold child values; the null constant replaces its hash below
    child = column_hashes(values, seed ^ _seed("item"))
    lengths = np.diff(rel)
    row_of = np.repeat(np.arange(len(arr)), lengths)
    pos = np.arange(len(child), dtype=np.int64) - rel[:-1][row_of]
    per_item = _mix(child * _P + pos.astype(_U))
    return _mix(_segment_sums(per_item, rel) ^ _mix(lengths.astype(_U) ^ seed))


def row_hashes(batch: pa.RecordBatch | pa.Table) -> np.ndarray:
    h = np.full(batch.num_rows, _seed(ALGORITHM), dtype=_U)
    for field, column in zip(batch.schema, batch.columns):
        h = _mix(h * _P + column_hashes(column, _seed(f"{field.name}\x00{_logical(field.type)}")))
    return h


def multiset_digest_of_batches(batches, schema: pa.Schema, drop=("__row_order",)) -> str:
    keep = [f.name for f in schema if f.name not in drop]
    lane1 = lane2 = _U(0)
    rows = 0
    with np.errstate(over="ignore"):
        for batch in batches:
            batch = batch.select(keep)
            h = row_hashes(batch)
            lane1 = lane1 + h.sum(dtype=_U)
            lane2 = lane2 + _mix(h ^ _LANE2).sum(dtype=_U)
            rows += batch.num_rows
    names = ",".join(f"{f.name}:{_logical(f.type)}" for f in schema if f.name in keep)
    top = hashlib.sha256(f"{ALGORITHM}\x00{rows}\x00{names}\x00{int(lane1)}\x00{int(lane2)}".encode())
    return f"{ALGORITHM}:{top.hexdigest()}"


def multiset_digest(path: Path) -> str:
    pf = pq.ParquetFile(path)
    return multiset_digest_of_batches(pf.iter_batches(batch_size=_BATCH), pf.schema_arrow)


# --- checks ---------------------------------------------------------------------------------------------------


def _table(n: int, seed: int = 0) -> pa.Table:
    rng = np.random.default_rng(seed)
    vocab = pa.array([f"w{i:05d}-{'x' * (i % 23)}" for i in range(50_000)])
    mask = rng.random(n) < 0.05
    ints = pa.array(rng.integers(0, 1_000_000, n))
    return pa.table(
        {
            "id": pa.array(np.arange(n)),
            "k": pa.array(rng.integers(0, 200, n).astype(np.int32)),
            "x": pa.array(rng.random(n), mask=mask),
            "s": vocab.take(pa.array(rng.integers(0, len(vocab), n))),
            "d": pa.array(rng.integers(0, 20_000, n).astype(np.int32)).cast(pa.date32()),
            "ts": pa.array(rng.integers(0, 2**40, n)).cast(pa.timestamp("us")),
            "b": pa.array(rng.random(n) < 0.5, mask=rng.random(n) < 0.1),
            "dec": pc.cast(ints, pa.decimal128(24, 2)),
            "tags": pa.ListArray.from_arrays(
                pa.array(np.arange(0, n + 1, dtype=np.int32) * 2), pa.array(rng.integers(0, 50, 2 * n))
            ),
        }
    )


def _digest_table(t: pa.Table) -> str:
    return multiset_digest_of_batches(t.to_batches(max_chunksize=_BATCH), t.schema)


def checks() -> None:
    t = _table(200_000)
    base = _digest_table(t)
    perm = np.random.default_rng(1).permutation(len(t))
    assert _digest_table(t.take(pa.array(perm))) == base, "row order changed the digest"
    assert multiset_digest_of_batches(t.to_batches(max_chunksize=777), t.schema) == base, "batching changed it"
    as_large = t.set_column(3, "s", t["s"].cast(pa.large_string()))
    assert _digest_table(as_large) == base, "string vs large_string changed it"
    stamped = t.append_column("__row_order", pa.array(perm))
    assert _digest_table(stamped) == base, "__row_order changed it"

    one = t.set_column(0, "id", pa.array(t["id"].to_numpy() + (np.arange(len(t)) == 5)))
    assert _digest_table(one) != base, "one changed value went unseen"
    k = t["k"].to_numpy().copy()
    if k[0] != k[1]:
        k[0], k[1] = k[1], k[0]
        assert _digest_table(t.set_column(1, "k", pa.array(k))) != base, "a swap within one column went unseen"
    dup = pa.concat_tables([t, t.slice(0, 1)])
    assert _digest_table(dup) != base, "a duplicated row went unseen"
    z = pa.table({"x": pa.array([0.0, None])})
    assert _digest_table(z) != _digest_table(pa.table({"x": pa.array([0.0, 0.0])})), "null == 0.0"
    assert _digest_table(pa.table({"x": pa.array([-0.0])})) == _digest_table(pa.table({"x": pa.array([0.0])}))
    a = pa.table({"p": ["a", "b"], "q": ["b", "a"]})
    b = pa.table({"p": ["b", "a"], "q": ["b", "a"]})
    assert _digest_table(a) != _digest_table(b), "values moved between rows went unseen"
    ab = pa.table({"s": ["ab", ""]})
    assert _digest_table(ab) != _digest_table(pa.table({"s": ["a", "b"]})), "string boundaries went unseen"
    lst = pa.table({"l": pa.array([[1, 2], [3]])})
    assert _digest_table(lst) != _digest_table(pa.table({"l": pa.array([[1], [2, 3]])})), "list boundaries unseen"
    assert _digest_table(lst) != _digest_table(pa.table({"l": pa.array([[2, 1], [3]])})), "list order unseen"
    st = pa.table({"st": pa.array([{"a": 1, "b": "x"}, None])})
    assert _digest_table(st) == _digest_table(st.take([1, 0])), "struct column changed with order"
    print("checks passed")


def timing(n: int) -> None:
    import polars as pl

    try:  # ADR-009 D2's digest; on #189's branch, not on main
        from tallyman_xorq.digest import content_digest
    except ImportError:
        content_digest = None

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "t.parquet"
        t0 = time.perf_counter()
        pq.write_table(_table(n), path, row_group_size=1_048_576, compression="zstd")
        print(f"rows={n:,} file={path.stat().st_size / 1e6:.0f} MB (written in {time.perf_counter() - t0:.1f}s)")

        t0 = time.perf_counter()
        d1 = multiset_digest(path)
        print(f"multiset digest        {time.perf_counter() - t0:6.1f}s  {d1[:40]}")
        if content_digest is not None:
            t0 = time.perf_counter()
            d2 = content_digest(path)
            print(f"ordered digest (today) {time.perf_counter() - t0:6.1f}s  {d2[:40]}")
        t0 = time.perf_counter()
        pl.read_parquet(path).hash_rows().sum()
        print(f"polars hash_rows (ref) {time.perf_counter() - t0:6.1f}s  (not version-stable)")


if __name__ == "__main__":
    checks()
    timing(int(sys.argv[1]) if len(sys.argv) > 1 else 18_000_000)
