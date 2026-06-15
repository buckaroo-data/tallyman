from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_core import data_dir, entry_dir
from tallyman_xorq.build import BuildError, build_and_persist
from tallyman_xorq.result_cache import cache_worthy

# Rewrite-then-build (#73): non-parquet source reads are cached at the read,
# parquet/delta reads are exempt, and in-memory reads are rejected.


@pytest.fixture
def sales_csv(project: str) -> Path:
    p = data_dir(project) / "sales.csv"
    p.write_text("region,price\nNE,10\nMW,20\nNE,30\nS,40\n")
    return p


def _csv_projection(project: str) -> str:
    return f"""
import xorq.api as xo
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import project_path
p = project_path("sales.csv", project={project!r})
t = xo.deferred_read_csv(p, schema=ibis.schema({{"region": "string", "price": "int64"}}))
expr = t.select("region", "price")
"""


def _csv_aggregate(project: str) -> str:
    return f"""
import xorq.api as xo
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import project_path
p = project_path("sales.csv", project={project!r})
t = xo.deferred_read_csv(p, schema=ibis.schema({{"region": "string", "price": "int64"}}))
expr = t.group_by("region").aggregate(total=t.price.sum())
"""


def _parquet_projection(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _build_yaml(project: str, content_hash: str) -> str:
    return (entry_dir(project, content_hash) / "xorq_build" / "expr.yaml").read_text()


def test_csv_read_gets_source_cache_but_stays_cheap(project, sales_csv, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _csv_projection(project))
    # The non-parquet read is wrapped in a cache node...
    assert "op: CachedNode" in _build_yaml(project, res.content_hash)
    # ...but a plain projection is not itself worth a result cache.
    assert cache_worthy(project, res.content_hash) is False
    # No build-time result.parquet, and the row count came through fine.
    assert not (entry_dir(project, res.content_hash) / "result.parquet").exists()
    assert res.row_count == 4


def test_parquet_read_has_no_source_cache(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _parquet_projection(project))
    # A parquet read is exempt — re-reading a columnar source is cheap, so no
    # cache node is injected and nothing is materialised.
    assert "op: CachedNode" not in _build_yaml(project, res.content_hash)
    assert cache_worthy(project, res.content_hash) is False


def test_expensive_csv_bakes_source_and_result_cache(project, sales_csv, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    res = build_and_persist(project, _csv_aggregate(project))
    # Both the source-read cache and the baked result cache are present.
    assert _build_yaml(project, res.content_hash).count("op: CachedNode") == 2
    assert cache_worthy(project, res.content_hash) is True
    assert not (entry_dir(project, res.content_hash) / "result.parquet").exists()


def test_in_memory_read_is_rejected(project, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = """
import xorq.api as xo
import pandas as pd
expr = xo.memtable(pd.DataFrame({"a": [1, 2, 3]}))
"""
    with pytest.raises(BuildError) as exc:
        build_and_persist(project, code)
    assert "in-memory" in str(exc.value)
    assert "deferred_read_csv" in str(exc.value)
