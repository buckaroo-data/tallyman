from __future__ import annotations

from pathlib import Path

from tallyman_mcp.server import catalog_import_source, catalog_list, catalog_run


def _code(src: str) -> str:
    # The orders data entered the catalog as a source alias, and a recipe reads the alias (ADR-011 D2).
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_catalog_run_success(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_run(_code(orders_src), prompt="by region")
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


def test_catalog_list_after_run(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_run(_code(orders_src), prompt="x")
    rows = catalog_list()["items"]
    # Two entries: the imported source version the fixture minted, and the aggregate built over it.
    assert len(rows) == 2
    assert rows[0]["row_count"] == 4  # most recent first
    assert {r["alias"] for r in rows} == {None, orders_src}


def test_catalog_import_source_success(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_import_source(str(orders_parquet), "orders", prompt="raw orders")
    assert "error" not in out
    assert out["row_count"] == 200  # the test fixture has 200 rows
    assert any(f["name"] == "region" for f in out["schema"]["fields"])
    assert out["alias"] == "orders"
    assert out["version"] == 1
    # Notebook auto-appended on the first version.
    from tallyman_core import notebook

    cells = notebook.load(project)["cells"]
    assert len(cells) == 1
    assert cells[0]["alias"] == "orders"


def test_catalog_import_source_missing(project: str, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_import_source(str(tmp_path / "nope.parquet"), "orders")
    assert "error" in out
    assert "nope.parquet" in out["error"]


def test_catalog_run_surfaces_nondeterminism_lint(project: str, orders_src: str, monkeypatch):
    # An execution-nondeterministic recipe (now()) builds fine but must carry an
    # advisory lint in the tool reply so the model/user sees it (#88).
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.mutate(built_at=ibis.now())
"""
    out = catalog_run(code, prompt="nondeterministic")
    assert "error" not in out
    assert "lint_warnings" in out
    assert any("now()" in w for w in out["lint_warnings"])


def test_catalog_import_source_repeated_on_unchanged_bytes_is_a_noop(project: str, orders_parquet, monkeypatch):
    """The old catalog_load_parquet errored on an existing alias; an import of the same bytes is idempotent."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    first = catalog_import_source(str(orders_parquet), "orders")
    again = catalog_import_source(str(orders_parquet), "orders")
    assert "error" not in again
    assert again["hash"] == first["hash"]
    assert again["version"] == 1
    assert again["created"] is False


def test_catalog_import_source_records_no_project_argument(project: str, orders_parquet, monkeypatch):
    """The generated recipe carries no explicit project=, so a project rename does not invalidate the build. (T-24.)"""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    out = catalog_import_source(str(orders_parquet), "orders")
    from tallyman_core import entry_dir

    code = (entry_dir(project, out["hash"]) / "expr.py").read_text()
    assert "project=" not in code
    assert "read_project_file(" in code
