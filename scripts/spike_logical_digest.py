"""ADR-009 evidence: two candidate definitions of ``result_digest`` for a tallyman-written snapshot.

**File bytes** (today's definition). Reproducible only if the writer's input and settings are pinned: the same rows
arriving in differently sized record batches must be regrouped into fixed row groups, and each row group combined
into contiguous arrays before it is written. The parquet footer also embeds the writer's version string, so the
bytes move on every pyarrow upgrade.

**Logical content**. A SHA-256 over the ordered Arrow data, one hasher per column, fed a normalized form that does
not depend on batch boundaries, on the physical string type, or on whatever sits in null slots. It is independent
of codec, row-group size and writer version, and costs a read-back to verify.

The script checks both for the invariances each needs, compares the file shape of a tallyman-side writer with
the shape xorq's ``ParquetStorage`` writes, then times the two digests.

    uv run python scripts/spike_logical_digest.py
"""

from __future__ import annotations

import hashlib
import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import xorq.api as xo

N = 3_000_000
ROW_GROUP = 1_048_576


def _logical_type(dtype: pa.DataType) -> str:
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) or pa.types.is_string_view(dtype):
        return "string"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_binary_view(dtype):
        return "binary"
    return str(dtype)


class LogicalDigest:
    """Order-sensitive digest of a record-batch stream, invariant to chunking and physical encoding."""

    # Each column keeps one hasher per byte stream (validity, lengths, values). A single hasher per column would
    # interleave the streams chunk by chunk, and the digest would then depend on where the batch boundaries fall.
    STREAMS = ("validity", "lengths", "values")

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.hashers = [
            {s: hashlib.sha256(f"{f.name}\x00{_logical_type(f.type)}\x00{s}".encode()) for s in self.STREAMS}
            for f in schema
        ]
        self.rows = 0

    def update(self, batch: pa.RecordBatch | pa.Table) -> None:
        for hashers, column in zip(self.hashers, batch.columns):
            chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
            for arr in chunks:
                self._update_array(hashers, arr)
        self.rows += batch.num_rows

    @staticmethod
    def _update_array(hashers: dict, arr: pa.Array) -> None:
        if pa.types.is_dictionary(arr.type):
            arr = arr.dictionary_decode()
        nulls = arr.is_null().to_numpy(zero_copy_only=False)
        hashers["validity"].update(nulls.astype(np.uint8).tobytes())  # one byte per row, null or not
        dtype = arr.type
        if _logical_type(dtype) in ("string", "binary"):
            target = pa.large_string() if _logical_type(dtype) == "string" else pa.large_binary()
            filled = pc.fill_null(arr.cast(target), "" if target == pa.large_string() else b"")
            hashers["lengths"].update(
                pc.binary_length(filled).to_numpy(zero_copy_only=False).astype(np.int64).tobytes()
            )
            offsets = np.frombuffer(filled.buffers()[1], dtype=np.int64)[
                filled.offset : filled.offset + len(filled) + 1
            ]
            if len(filled):
                hashers["values"].update(memoryview(filled.buffers()[2])[int(offsets[0]) : int(offsets[-1])])
        elif pa.types.is_boolean(dtype):
            hashers["values"].update(pc.fill_null(arr, False).to_numpy(zero_copy_only=False).astype(np.uint8).tobytes())
        elif pa.types.is_primitive(dtype) or pa.types.is_decimal(dtype) or pa.types.is_fixed_size_binary(dtype):
            width = dtype.bit_width // 8
            raw = np.frombuffer(arr.buffers()[1], dtype=np.uint8)[arr.offset * width : (arr.offset + len(arr)) * width]
            if arr.null_count:
                raw = raw.reshape(len(arr), width).copy()
                raw[nulls] = 0  # null slots may hold anything; zero them
            hashers["values"].update(raw.tobytes() if arr.null_count else memoryview(raw))
        else:
            raise NotImplementedError(f"nested/other type not covered by the spike: {dtype}")

    def hexdigest(self) -> str:
        top = hashlib.sha256(str(self.rows).encode())
        for hashers in self.hashers:
            for stream in self.STREAMS:
                top.update(hashers[stream].digest())
        return top.hexdigest()[:16]


def logical_digest(batches, schema: pa.Schema) -> str:
    d = LogicalDigest(schema)
    for batch in batches:
        d.update(batch)
    return d.hexdigest()


def write_pinned(batches, schema: pa.Schema, dest: Path, combine: bool = True, **settings) -> str:
    """Regroup a batch stream into fixed row groups and write it; return the digest of the file's bytes."""
    options = {"compression": "zstd", "compression_level": 3, "version": "2.6", "data_page_version": "1.0"} | settings
    with pq.ParquetWriter(dest, schema, **options) as writer:
        pending, rows = [], 0
        for batch in batches:
            pending.append(batch)
            rows += batch.num_rows
            while rows >= ROW_GROUP:
                table = pa.Table.from_batches(pending)
                head, tail = table.slice(0, ROW_GROUP), table.slice(ROW_GROUP)
                writer.write_table(head.combine_chunks() if combine else head, row_group_size=ROW_GROUP)
                pending, rows = tail.to_batches(), tail.num_rows
        if rows:
            table = pa.Table.from_batches(pending)
            writer.write_table(table.combine_chunks() if combine else table, row_group_size=ROW_GROUP)
    return hashlib.sha256(dest.read_bytes()).hexdigest()[:16]


def make_table() -> pa.Table:
    rng = np.random.default_rng(11)
    floats = rng.random(N)
    floats[rng.integers(0, N, 1000)] = np.nan
    ints = rng.integers(0, 1_000_000, N)
    return pa.table(
        {
            "id": pa.array(np.arange(N)),
            "g": pa.array(ints),
            "f": pa.array(floats, mask=rng.random(N) < 0.01),
            "s": pa.array([f"k{v % 5000:05d}" for v in ints], mask=rng.random(N) < 0.01),
            "b": pa.array(ints % 2 == 0),
            "ts": pa.array(ints.astype("datetime64[s]")),
        }
    )


def main() -> None:
    table = make_table()
    schema = table.schema
    print(f"table: {N:,} rows, {table.nbytes / 1e6:.0f} MB of Arrow data, pyarrow {pa.__version__}\n")
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        print("file bytes, the same rows arriving in different batch sizes:")
        for combine in (True, False):
            digests = {
                size: write_pinned(
                    table.to_batches(max_chunksize=size), schema, tmp / f"c{int(combine)}_{size}.parquet", combine
                )
                for size in (8_192, 100_000, N)
            }
            distinct = sorted(set(digests.values()))
            print(f"  combine_chunks per row group={combine!s:5}: {len(distinct)} distinct digest(s) {distinct}")
        created_by = pq.ParquetFile(tmp / "c1_8192.parquet").metadata.created_by
        print(f"  footer created_by = {created_by!r} (moves with every writer upgrade)\n")

        print("file shape, the same rows:")
        xorq_shape = tmp / "xorq_shape.parquet"
        with pq.ParquetWriter(xorq_shape, schema, compression="snappy") as writer:
            for batch in table.to_batches(max_chunksize=8_192):
                writer.write_batch(batch)  # one row group per batch, as xorq's ParquetStorage writes them
        for label, path in (("zstd, 1,048,576-row groups", tmp / "c1_8192.parquet"), ("xorq's shape", xorq_shape)):
            md = pq.ParquetFile(path).metadata
            size = path.stat().st_size / 1e6
            groups, footer = md.num_row_groups, md.serialized_size / 1e3
            print(f"  {label:27s}: {size:5.1f} MB, {groups:3d} row groups, {footer:6.1f} KB footer")
        print()

        def of_file(path: Path, batch_size: int = 65_536) -> str:
            pf = pq.ParquetFile(path)
            return logical_digest(pf.iter_batches(batch_size=batch_size), pf.schema_arrow)

        print("logical content:")
        by_batch = {size: logical_digest(table.to_batches(max_chunksize=size), schema) for size in (8_192, 100_000, N)}
        distinct = len(set(by_batch.values()))
        print(f"  in-memory stream, batch sizes 8,192 / 100,000 / one table: {distinct} distinct digest(s)")

        zstd_path, snappy_path = tmp / "c1_8192.parquet", tmp / "snappy_small_groups.parquet"
        pq.write_table(table, snappy_path, compression="snappy", row_group_size=8_192)
        reference = of_file(zstd_path)
        stored = pq.ParquetFile(zstd_path).schema_arrow
        coerced = [f"{a.name}: {a.type} -> {b.type}" for a, b in zip(schema, stored) if a.type != b.type]
        print(f"  stream handed to the writer vs the file read back: same={by_batch[8_192] == reference}")
        print(f"    (the writer coerced {coerced})")
        print(f"  same file, read in 1,000-row batches: same={of_file(zstd_path, 1_000) == reference}")
        print(f"  same rows written snappy with 8,192-row groups: same={of_file(snappy_path) == reference}")
        engine = xo.connect().read_parquet(str(zstd_path)).order_by("id").to_pyarrow_batches()
        print(f"  same file through DataFusion (read, order_by id): same={logical_digest(engine, stored) == reference}")

        def rewritten(changed: pa.Table) -> str:
            path = tmp / "changed.parquet"
            pq.write_table(changed, path, compression="zstd", row_group_size=ROW_GROUP)
            return of_file(path)

        flipped = table.set_column(1, "g", pc.add(table["g"], pa.array(np.eye(1, N, 12345, dtype=np.int64)[0])))
        print(f"  one value changed: differs={rewritten(flipped) != reference}")
        f = table["f"].combine_chunks()
        first_null = int(np.flatnonzero(f.is_null().to_numpy(zero_copy_only=False))[0])
        as_zero = pa.concat_arrays([f.slice(0, first_null), pa.array([0.0]), f.slice(first_null + 1)])
        print(f"  one null replaced by 0.0: differs={rewritten(table.set_column(2, 'f', as_zero)) != reference}")
        swapped = table.take(pa.array(np.r_[1, 0, np.arange(2, N)]))
        print(f"  first two rows swapped: differs={rewritten(swapped) != reference}\n")

        print("cost:")
        t0 = time.perf_counter()
        logical_digest(table.to_batches(max_chunksize=8_192), schema)
        secs = time.perf_counter() - t0
        print(f"  logical digest of an in-memory stream: {secs:.2f} s ({table.nbytes / 1e6 / secs:.0f} MB/s)")
        t0 = time.perf_counter()
        of_file(zstd_path)
        secs = time.perf_counter() - t0
        print(f"  logical digest of the file (read back + hash; the build and the verify path): {secs:.2f} s")
        t0 = time.perf_counter()
        hashlib.sha256(zstd_path.read_bytes()).hexdigest()
        secs = time.perf_counter() - t0
        print(f"  file-bytes digest to verify ({zstd_path.stat().st_size / 1e6:.0f} MB file): {secs:.2f} s")


if __name__ == "__main__":
    main()
