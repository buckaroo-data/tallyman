"""A parquet fixture above DataFusion's scan-split threshold, shared by the cache-redesign tests.

DataFusion splits a file scan into parallel byte ranges when the file is larger than
``datafusion.optimizer.repartition_file_min_size`` (10,485,760 bytes in xorq-datafusion 0.2.7), and only then does an
unsorted ``LIMIT/OFFSET`` return rows in an unstable order (ADR-008) or a float aggregate merge partial sums in an
unstable order (ADR-009). A fixture below that size proves nothing about either, so the writer asserts its size.
"""

from __future__ import annotations

from pathlib import Path

SPLIT_THRESHOLD_BYTES = 10_485_760


def write_big_parquet(path: Path, n_rows: int = 1_500_000, seed: int = 0, row_group_size: int = 100_000) -> Path:
    """Write ``n_rows`` rows: ``id`` (the file position), ``g`` (200 values, so a sort on it has ties), ``v`` (float).

    ``v`` is random and therefore incompressible, which is what makes the file larger than the split threshold.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    rng = np.random.default_rng(seed)
    table = pa.table(
        {
            "id": np.arange(n_rows, dtype=np.int64),
            "g": rng.integers(0, 200, size=n_rows, dtype=np.int64),
            "v": rng.normal(scale=1e6, size=n_rows),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, row_group_size=row_group_size)
    size = path.stat().st_size
    assert size > SPLIT_THRESHOLD_BYTES, f"fixture is {size} bytes, below the {SPLIT_THRESHOLD_BYTES}-byte threshold"
    return path
