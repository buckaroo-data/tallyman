from __future__ import annotations

import json
from pathlib import Path

import pytest

from tallyman_core import entry_dir
from tallyman_xorq import BuildError, build_and_persist, list_entries


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


def test_build_hints_bare_ibis_import(project: str, orders_parquet: Path):
    # `import ibis` triggers an AttributeError at user-code exec time because
    # the real ibis package doesn't expose all the methods xorq does. The
    # BuildError should include a hint pointing at xorq.vendor.ibis.
    code = f"""
import xorq.api as xo
import ibis
t = xo.deferred_read_parquet({str(orders_parquet)!r})
expr = t.mutate(flag=ibis.case().when(t.price > 50, "hi").else_("lo").end())
"""
    with pytest.raises(BuildError, match="import xorq.vendor.ibis as ibis"):
        build_and_persist(project, code)


def test_build_no_hint_on_unrelated_error(project: str, orders_parquet: Path):
    code = f"""
import xorq.api as xo
import xorq.vendor.ibis as ibis
t = xo.deferred_read_parquet({str(orders_parquet)!r})
expr = t.nonexistent_column.sum()
"""
    with pytest.raises(BuildError) as excinfo:
        build_and_persist(project, code)
    assert "import xorq.vendor.ibis as ibis" not in str(excinfo.value).split("Traceback")[0]


# ---------------------------------------------------------------------------
# Namespace / column-name hints. The session scan showed the model repeatedly
# reaching for `xorq.<fn>` (the api lives on `xorq.api`/`xo`), `ibis.<fn>` for
# things that are column methods or live elsewhere, inventing `tallyman_xorq.io`
# helpers, guessing column names, and reaching for a duckdb backend. The old
# hint only caught bare `import ibis`, the Expr class mismatch, and `xorq._`.
# ---------------------------------------------------------------------------

from tallyman_xorq.build import _ibis_import_hint, lint_catalog_code  # noqa: E402


# ---------------------------------------------------------------------------
# lint_catalog_code — pre-execution static scanner
# ---------------------------------------------------------------------------


def test_lint_rejects_import_ibis():
    errors = lint_catalog_code("import ibis\nexpr = ibis.table('x')")
    assert errors
    assert any("xorq.vendor.ibis" in e for e in errors)


def test_lint_rejects_from_ibis_import():
    errors = lint_catalog_code("from ibis import window\nexpr = None")
    assert errors
    assert any("xorq.vendor.ibis" in e for e in errors)


def test_lint_rejects_from_ibis_submodule():
    errors = lint_catalog_code("from ibis.expr.types import Column\nexpr = None")
    assert errors


def test_lint_rejects_bare_import_xorq():
    errors = lint_catalog_code("import xorq\nexpr = xorq.connect()")
    assert errors
    assert any("xorq.api" in e for e in errors)


def test_lint_rejects_import_xorq_as_xo():
    errors = lint_catalog_code("import xorq as xo\nexpr = xo.connect()")
    assert errors
    assert any("xorq.api" in e for e in errors)


def test_lint_accepts_correct_imports():
    code = """
import xorq.api as xo
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import from_catalog, from_project
t = from_catalog("foo")
expr = t.filter(t.x > 0)
"""
    assert lint_catalog_code(code) == []


def test_lint_accepts_no_imports():
    code = "t = from_catalog('foo')\nexpr = t.filter(t.x > 0)"
    assert lint_catalog_code(code) == []


def test_lint_returns_syntax_error():
    errors = lint_catalog_code("def broken(\nexpr = None")
    assert errors
    assert any("syntax" in e for e in errors)


def test_build_lint_fires_before_exec(project: str):
    # `import ibis` is caught by the linter — no code should execute.
    code = "import ibis\nexpr = ibis.table('x', schema={'a': 'int64'})"
    with pytest.raises(BuildError, match="pre-execution lint"):
        build_and_persist(project, code)


def test_preinjected_names_available(project: str, orders_parquet: Path):
    # ibis, xo, from_project are pre-injected — no import statements needed.
    code = f"""
t = xo.deferred_read_parquet({str(orders_parquet)!r})
expr = t.group_by("region").aggregate(
    total=t.price.sum(),
    label=ibis.literal("ok"),
)
"""
    res = build_and_persist(project, code)
    assert res.row_count == 4


# ---------------------------------------------------------------------------
# Namespace / column-name hints
# ---------------------------------------------------------------------------


def test_hint_bare_xorq_attribute_points_to_api():
    h = _ibis_import_hint("module 'xorq' has no attribute 'memtable'")
    assert "xorq.api" in h


def test_hint_ibis_read_parquet_points_to_loaders():
    h = _ibis_import_hint("module 'ibis' has no attribute 'read_parquet'")
    assert "from_project" in h


def test_hint_ibis_math_func_is_column_method():
    h = _ibis_import_hint("module 'ibis' has no attribute 'sin'")
    assert ".sin()" in h


def test_hint_io_import_typo_lists_real_exports():
    h = _ibis_import_hint("cannot import name 'load_parquet_expr' from 'tallyman_xorq.io'")
    assert "from_project" in h and "from_catalog" in h


def test_hint_missing_table_column():
    h = _ibis_import_hint("'Table' object has no attribute 'trip_duration'")
    assert "not a column" in h.lower()
    assert "trip_duration" in h


def test_hint_duckdb_reach_points_to_datafusion():
    h = _ibis_import_hint("No module named 'duckdb'")
    assert "datafusion" in h.lower()


def test_build_hints_bare_xorq_namespace(project: str, orders_parquet: Path):
    # End-to-end: a bare `import xorq` + `xorq.deferred_read_parquet(...)` must
    # steer the model to `xorq.api` through the real build path, not just the
    # unit-tested helper.
    code = f"""
import xorq
t = xorq.deferred_read_parquet({str(orders_parquet)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    with pytest.raises(BuildError, match="xorq.api"):
        build_and_persist(project, code)
