"""#171 regression: a parquet-sourced bake's bytes are stable across heals.

History: as a *probe*, this test demonstrated the drift — on pre-fix main,
DataFusion's parallel scan/aggregation reordered the bake on every heal above
``repartition_file_min_size`` (1 MiB; files below it are never byte-range
split, which is why small fixtures looked deterministic — see
``plans/datafusion-scan-order-findings.md``): 3 heals produced 3 distinct
digests, none matching the build. That evidence flipped ADR D5
(``plans/adr-read-path-loads-builds.md``) to the canonical sort-by-all-columns
bake, which this test now pins: every heal must reproduce the build's exact
bytes, ``verify_result_faithful`` must agree, and the UNFAITHFUL advisory must
stay silent.
"""

from __future__ import annotations

import logging

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir
from tallyman_core.manifest import read_manifest
from tallyman_core.paths import entry_dir
from tallyman_mcp.server import catalog_create
from tallyman_xorq.result_cache import (
    baked_snapshot_path,
    cached_result_expr,
    snapshot_file_digest,
    verify_result_faithful,
)

_HEALS = 3


def _big_agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders_big.parquet", project={project!r})
expr = t.group_by("order_id").aggregate(total=t.price.sum(), n=t.count())
"""


def test_parquet_bake_digest_stable_across_heals(project, monkeypatch, caplog):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    src = write_shoe_orders(data_dir(project) / "orders_big.parquet", n_rows=250_000, seed=7)
    # Below repartition_file_min_size the file is never split and the test
    # proves nothing — fail loudly rather than pass vacuously.
    assert src.stat().st_size > 1_048_576, "fixture too small to provoke a partitioned scan"

    h = catalog_create("big_by_order", _big_agg_code(project))["hash"]
    recorded = read_manifest(entry_dir(project, h)).result_digest
    assert recorded is not None

    for _ in range(_HEALS):
        snap = baked_snapshot_path(project, h)
        if snap is not None and snap.exists():
            snap.unlink()
        cached_result_expr.cache_clear()
        with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
            cached_result_expr(project, h)  # heal fires
        healed = baked_snapshot_path(project, h)
        assert healed is not None and healed.exists()
        # The canonical sort (ADR D5, amended) makes the bake a deterministic
        # total order: every heal reproduces the build's exact bytes.
        assert snapshot_file_digest(healed) == recorded
        assert verify_result_faithful(project, h) is True
    # Scoped to tallyman.perf: xorq's startup logging captures the repo's
    # uncommitted diff into an INFO record, which can itself contain the string.
    assert not any("UNFAITHFUL" in r.getMessage() for r in caplog.records if r.name == "tallyman.perf")
