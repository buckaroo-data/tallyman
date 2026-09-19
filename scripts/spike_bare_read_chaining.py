"""ADR-007 evidence: chaining through a bare read of the parent's snapshot, with no xorq cache node anywhere.

Raw xorq plus tallyman's ``classify_build``; no tallyman build machinery. A worthy parent (an aggregate, canonically
sorted) is built with no cache node and materialized by a plain parquet write to ``<snapshots>/<parent_hash>.parquet``.
A child reads that path with ``deferred_read_parquet`` and adds a filter and a computed column.

Questions, in the order printed:

1. What does the child's build hash depend on: the snapshot's path string, or its bytes/mtime?
2. Can a child be built while the parent's snapshot is absent?
3. Does ``load_expr`` of the child's build need the snapshot? What does executing without it raise?
4. After the parent is re-materialized from its own build, does the child execute to the right answer?
5. Is the child classified cheap? (Under today's inlined chaining the same child is worthy.)
6. Does the child's ``expr.yaml`` carry the literal path, so the portable-path placeholder applies to it?
7. Did anything get written under xorq's global cache directory?

    uv run python scripts/spike_bare_read_chaining.py
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_bare_read_"))
os.environ["XORQ_CACHE_DIR"] = str(HOME / "_global_xorq")  # must be set before xorq is imported

import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import xorq.api as xo  # noqa: E402
from xorq.ibis_yaml.compiler import build_expr, load_expr  # noqa: E402

from tallyman_xorq.result_cache import classify_build  # noqa: E402

N = 400_000
BUILDS = HOME / "builds"
SNAPSHOTS = HOME / "compute_cache" / "result_cache"


def materialize(expr, dest: Path) -> str:
    tmp = dest.with_name(f".{dest.name}.{os.getpid()}.tmp")
    pq.write_table(expr.to_pyarrow(), tmp, compression="zstd")
    os.replace(tmp, dest)
    return hashlib.sha256(dest.read_bytes()).hexdigest()[:16]


def child_of(snapshot: Path, schema=None):
    parent = xo.deferred_read_parquet(str(snapshot), schema=schema)
    return parent.filter(parent.n > 10).mutate(r=parent.s / parent.n)


def attempt(fn) -> str:
    try:
        return str(fn())
    except Exception as exc:  # noqa: BLE001 - the spike reports whatever is raised
        return f"raises {type(exc).__name__}: {str(exc)[:90]}"


def main() -> None:
    SNAPSHOTS.mkdir(parents=True)
    source = HOME / "cas" / "deadbeef.parquet"
    source.parent.mkdir()
    rng = np.random.default_rng(7)
    pq.write_table(
        pa.table({"id": np.arange(N), "g": rng.integers(0, 20_000, N), "v": rng.integers(0, 1000, N)}), source
    )

    t = xo.deferred_read_parquet(str(source))
    parent = t.group_by("g").agg(n=t.count(), s=t.v.sum()).order_by(["g", "n", "s"])
    parent_build = Path(build_expr(parent, builds_dir=BUILDS))
    snapshot = SNAPSHOTS / f"{parent_build.name}.parquet"
    first_digest = materialize(load_expr(parent_build), snapshot)
    print(f"parent {parent_build.name}: {classify_build(parent_build)}, snapshot digest {first_digest}\n")

    same_path = Path(build_expr(child_of(snapshot), builds_dir=BUILDS)).name
    original = snapshot.read_bytes()
    pq.write_table(pa.table({"g": [1, 2], "n": [99, 98], "s": [5, 6]}), snapshot)  # other rows, other size and mtime
    other_bytes = Path(build_expr(child_of(snapshot), builds_dir=BUILDS)).name
    elsewhere = SNAPSHOTS / "0123456789ab.parquet"
    elsewhere.write_bytes(original)
    other_path = Path(build_expr(child_of(elsewhere), builds_dir=BUILDS)).name
    print(f"1. child hash: {same_path}")
    print(f"   same path with other bytes: {other_bytes}; same bytes at another path: {other_path}")

    snapshot.unlink()
    print(f"2. compose with snapshot absent, no schema: {attempt(lambda: child_of(snapshot).schema().names)}")
    with_schema = attempt(lambda: build_expr(child_of(snapshot, parent.schema()), builds_dir=BUILDS))
    print(f"   build with snapshot absent, schema given: {with_schema}")

    child_build = BUILDS / same_path
    print(f"3. load_expr(child) with snapshot absent: {attempt(lambda: type(load_expr(child_build)).__name__)}")
    print(f"   execute with snapshot absent: {attempt(lambda: load_expr(child_build).count().execute())}")

    again = materialize(load_expr(parent_build), snapshot)
    rows = int(load_expr(child_build).count().execute())
    want = int(parent.filter(parent.n > 10).count().execute())
    print(f"4. parent re-materialized from its build: digest unchanged={again == first_digest}")
    print(f"   child rows {rows}, expected {want}")

    inlined = Path(build_expr(parent.filter(parent.n > 10).mutate(r=parent.s / parent.n), builds_dir=BUILDS))
    print(f"5. classify_build(bare-read child) = {classify_build(child_build)}")
    print(f"   classify_build(same child, parent graph inlined) = {classify_build(inlined)}")

    yaml_text = (child_build / "expr.yaml").read_text()
    inlined_size = len((inlined / "expr.yaml").read_text())
    print(f"6. literal snapshot path in child expr.yaml: {str(snapshot) in yaml_text}")
    print(f"   child expr.yaml is {len(yaml_text):,} bytes; with the parent graph inlined it is {inlined_size:,}")

    leaked = sorted(str(f) for f in (HOME / "_global_xorq").rglob("*.parquet"))
    print(f"7. parquet files under XORQ_CACHE_DIR: {leaked or 'none'}")


if __name__ == "__main__":
    main()
