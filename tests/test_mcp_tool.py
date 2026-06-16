from __future__ import annotations

from pathlib import Path

from tallyman_mcp.server import catalog_list, catalog_load_parquet, catalog_run


def _code(parquet: Path) -> str:
    return f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_catalog_run_success(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_run(_code(orders_parquet), prompt="by region")
    assert "error" not in out
    assert "hash" in out
    assert out["row_count"] == 4
    assert "schema" in out
    assert "fields" in out["schema"]


def test_catalog_run_error_returns_dict(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_run("expr = nope", prompt="broken")
    assert "error" in out
    assert "hash" not in out


def test_catalog_list_after_run(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_run(_code(orders_parquet), prompt="x")
    rows = catalog_list()["items"]
    assert len(rows) == 1
    assert rows[0]["row_count"] == 4


def test_catalog_load_parquet_success(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_load_parquet("orders.parquet", prompt="raw orders")
    assert "error" not in out
    assert out["row_count"] == 200  # the test fixture has 200 rows
    assert any(f["name"] == "region" for f in out["schema"]["fields"])


def test_catalog_load_parquet_missing(project: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_load_parquet("nope.parquet")
    assert "error" in out
    assert "nope.parquet" in out["error"]


def test_catalog_load_parquet_with_name_creates_alias(project: str, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_load_parquet("orders.parquet", prompt="raw", name="orders")
    assert "error" not in out
    assert out["alias"] == "orders"
    assert out["version"] == 1
    # Notebook auto-appended.
    from tallyman_core import notebook

    cells = notebook.load(project)["cells"]
    assert len(cells) == 1
    assert cells[0]["alias"] == "orders"


def test_catalog_run_surfaces_nondeterminism_lint(project: str, orders_parquet: Path, monkeypatch):
    # An execution-nondeterministic recipe (now()) builds fine but must carry an
    # advisory lint in the tool reply so the model/user sees it (#88).
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
import xorq.api as xo
import xorq.vendor.ibis as ibis
t = xo.deferred_read_parquet({str(orders_parquet)!r})
expr = t.mutate(built_at=ibis.now())
"""
    out = catalog_run(code, prompt="nondeterministic")
    assert "error" not in out
    assert "lint_warnings" in out
    assert any("now()" in w for w in out["lint_warnings"])


def test_catalog_load_parquet_name_collision_rejected(project: str, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_load_parquet("orders.parquet", name="orders")
    out = catalog_load_parquet("orders.parquet", name="orders")
    assert "error" in out
    assert "already exists" in out["error"]


def test_catalog_load_parquet_records_relative_path_only(project: str, orders_parquet, monkeypatch):
    """The synthesized code uses `from_project(rel_path)` without an explicit
    project= arg, so a project rename wouldn't invalidate the build. (T-24.)"""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_load_parquet("orders.parquet")
    from tallyman_core import entry_dir

    code = (entry_dir(project, out["hash"]) / "expr.py").read_text()
    assert "project=" not in code
    assert "from_project('orders.parquet')" in code
