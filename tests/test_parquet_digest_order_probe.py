"""#171 probe: can DataFusion's parallel scan/aggregation reorder a
parquet-sourced bake, and does the advisory fire correctly when it does?

Deliberately NOT a red test. Whether the reorder fires at this scale is the
empirical question the probe exists to answer — the source is written above
``repartition_file_min_size`` (1 MiB; files below it are never byte-range
split, which is why small fixtures look deterministic — see
``plans/datafusion-scan-order-findings.md``) and the aggregate has one group
per ``order_id`` so the hash aggregation has room to vary. The assertions pin
the MECHANISM, not the outcome: ``verify_result_faithful`` must agree with
the byte comparison, and any mismatch must surface the advisory warning
rather than pass silently (ADR D5, ``plans/adr-read-path-loads-builds.md``).
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


def test_parquet_bake_digest_stability_probe(project, monkeypatch, caplog):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    src = write_shoe_orders(data_dir(project) / "orders_big.parquet", n_rows=250_000, seed=7)
    # Below repartition_file_min_size the file is never split and the probe
    # proves nothing — fail loudly rather than pass vacuously.
    assert src.stat().st_size > 1_048_576, "fixture too small to provoke a partitioned scan"

    h = catalog_create("big_by_order", _big_agg_code(project))["hash"]
    recorded = read_manifest(entry_dir(project, h)).result_digest
    assert recorded is not None

    digests: set[str] = set()
    for _ in range(_HEALS):
        snap = baked_snapshot_path(project, h)
        if snap is not None and snap.exists():
            snap.unlink()
        cached_result_expr.cache_clear()
        with caplog.at_level(logging.WARNING, logger="tallyman.perf"):
            cached_result_expr(project, h)  # heal fires
        healed = baked_snapshot_path(project, h)
        assert healed is not None and healed.exists()
        d = snapshot_file_digest(healed)
        digests.add(d)
        # Mechanism consistency: the audit must agree with the bytes.
        assert verify_result_faithful(project, h) is (d == recorded)
        if d != recorded:
            # Drift occurred: it must be surfaced, not silent.
            assert any("UNFAITHFUL" in r.getMessage() for r in caplog.records)

    # Document the empirical outcome in the test output either way.
    print(
        f"digest-order probe: {len(digests)} distinct digest(s) over {_HEALS} heals; "
        f"build digest reproduced: {recorded in digests}"
    )
