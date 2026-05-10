from __future__ import annotations

from pathlib import Path

from pydata_mcp.server import catalog_list, catalog_load_parquet, catalog_run


def _code(parquet: Path) -> str:
    return f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_catalog_run_success(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_run(_code(orders_parquet), prompt="by region")
    assert "error" not in out
    assert "hash" in out
    assert out["row_count"] == 4
    assert "schema" in out
    assert "fields" in out["schema"]


def test_catalog_run_error_returns_dict(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_run("expr = nope", prompt="broken")
    assert "error" in out
    assert "hash" not in out


def test_catalog_list_after_run(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_run(_code(orders_parquet), prompt="x")
    rows = catalog_list()
    assert len(rows) == 1
    assert rows[0]["row_count"] == 4


def test_catalog_load_parquet_success(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_load_parquet("orders.parquet", prompt="raw orders")
    assert "error" not in out
    assert out["row_count"] == 200  # the test fixture has 200 rows
    assert any(f["name"] == "region" for f in out["schema"]["fields"])


def test_catalog_load_parquet_missing(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_load_parquet("nope.parquet")
    assert "error" in out
    assert "nope.parquet" in out["error"]
