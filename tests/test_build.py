from __future__ import annotations

import json
from pathlib import Path

import pytest

from pydata_core import entry_dir
from pydata_xorq import BuildError, build_and_persist, list_entries


def _agg_code(parquet_path: Path) -> str:
    return f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet_path)!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def test_build_writes_entry(project: str, orders_parquet: Path):
    code = _agg_code(orders_parquet)
    res = build_and_persist(project, code, prompt="region totals")
    assert res.row_count == 4  # one row per region
    assert res.execute_seconds >= 0.0
    target = entry_dir(project, res.content_hash)
    assert target.is_dir()
    for child in ("manifest.json", "result.parquet", "schema.json", "expr.py"):
        assert (target / child).exists(), child
    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["content_hash"] == res.content_hash
    assert manifest["prompt"] == "region totals"
    assert manifest["row_count"] == 4


def test_build_idempotent_same_code(project: str, orders_parquet: Path):
    code = _agg_code(orders_parquet)
    a = build_and_persist(project, code, prompt="first")
    b = build_and_persist(project, code, prompt="second")
    assert a.content_hash == b.content_hash
    assert a.entry_path == b.entry_path
    assert len(list_entries(project)) == 1


def test_build_distinct_code_distinct_hash(project: str, orders_parquet: Path):
    code_a = _agg_code(orders_parquet)
    code_b = f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(orders_parquet)!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(n=filtered.count())
"""
    a = build_and_persist(project, code_a)
    b = build_and_persist(project, code_b)
    assert a.content_hash != b.content_hash
    assert len(list_entries(project)) == 2


def test_build_missing_expr_variable(project: str, orders_parquet: Path):
    code = "import xorq.api as xo\nx = 1\n"
    with pytest.raises(BuildError, match="not found"):
        build_and_persist(project, code)


def test_build_user_code_raises(project: str):
    code = "raise ValueError('boom')"
    with pytest.raises(BuildError, match="boom"):
        build_and_persist(project, code)


def test_list_entries_empty(project: str):
    assert list_entries(project) == []
