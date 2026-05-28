from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from pydata_mcp.server import catalog_create, catalog_diff, catalog_revise
from pydata_xorq import (
    code_diff,
    full_diff,
    head_diff,
    key_diff,
    schema_diff,
    stats_diff,
)


def _agg_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _filter_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(total=filtered.price.sum(), n=filtered.count())
"""


# ---------------------------------------------------------------------------
# pure-function diffs (no xorq build needed)
# ---------------------------------------------------------------------------


def test_code_diff_html_has_pygments_highlight():
    out = code_diff("a = 1\nb = 2\n", "a = 1\nb = 3\n")
    assert "highlight" in out  # Pygments wraps in <div class="highlight">
    assert "+b = 3" in out
    assert "-b = 2" in out


def test_code_diff_identical_returns_sentinel():
    out = code_diff("a = 1\n", "a = 1\n")
    assert "identical" in out.lower()


def test_schema_diff_added_removed_changed():
    before = {"fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}], "row_count": 10}
    after = {"fields": [{"name": "x", "type": "string"}, {"name": "z", "type": "int"}], "row_count": 12}
    d = schema_diff(before, after)
    assert d["added"] == ["z"]
    assert d["removed"] == ["y"]
    assert d["changed_type"] == [{"name": "x", "before": "int", "after": "string"}]
    assert d["row_count"] == {"before": 10, "after": 12}


def test_head_diff_renders_html():
    a = pd.DataFrame({"x": [1, 2, 3]})
    b = pd.DataFrame({"x": [1, 2, 4]})
    d = head_diff(a, b, n=2)
    assert "<table" in d["before"]
    assert "<table" in d["after"]
    assert d["a_total"] == 3 and d["b_total"] == 3


def test_stats_diff_basic():
    a = pd.DataFrame({"region": ["NE", "NE", "S"], "v": [1, 2, 3]})
    b = pd.DataFrame({"region": ["NE", "S", "W"], "v": [10, 20, 30]})
    s = stats_diff(a, b)
    by_col = {row["name"]: row for row in s}
    assert by_col["region"]["before"]["distinct"] == 2
    assert by_col["region"]["after"]["distinct"] == 3
    assert by_col["v"]["before"]["numeric"]["sum"] == 6.0
    assert by_col["v"]["after"]["numeric"]["sum"] == 60.0


def test_key_diff_outer_join():
    a = pd.DataFrame({"region": ["NE", "MW", "S"], "n": [10, 20, 30]})
    b = pd.DataFrame({"region": ["NE", "S", "W"], "n": [15, 25, 35]})
    d = key_diff(a, b)
    assert d is not None
    assert d["keys"] == ["region"]
    assert d["matched"] == 2  # NE + S
    assert d["only_before"] == 1  # MW
    assert d["only_after"] == 1  # W


def test_key_diff_returns_none_when_no_keys():
    a = pd.DataFrame({"x": [1, 2, 3]})
    b = pd.DataFrame({"x": [1, 2, 4]})
    assert key_diff(a, b) is None  # no non-numeric shared columns


# ---------------------------------------------------------------------------
# full_diff against real xorq builds
# ---------------------------------------------------------------------------


def test_full_diff_against_two_builds(project: str, orders_parquet: Path):
    from pydata_xorq import build_and_persist

    a = build_and_persist(project, _agg_code(project))
    b = build_and_persist(project, _filter_code(project))
    from pydata_core import entry_dir

    diff = full_diff(entry_dir(project, a.content_hash), entry_dir(project, b.content_hash))
    assert "highlight" in diff["code"]
    assert diff["schema"]["row_count"]["before"] == 4
    assert "stats" in diff
    assert diff["keyed"] is not None  # region is a shared key


# ---------------------------------------------------------------------------
# catalog_diff MCP tool
# ---------------------------------------------------------------------------


def test_catalog_diff_default_compares_n_minus_one_to_n(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_diff("shoe_sales")
    assert "error" not in out
    assert out["before"]["version"] == 1
    assert out["after"]["version"] == 2


def test_catalog_diff_explicit_versions(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    out = catalog_diff("shoe_sales", 1, 2)
    assert "error" not in out


def test_catalog_diff_no_history(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    out = catalog_diff("nonexistent")
    assert "error" in out


def test_catalog_diff_out_of_range(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    out = catalog_diff("shoe_sales", 1, 5)
    assert "error" in out


# ---------------------------------------------------------------------------
# /diff/<alias>/<va>/<vb> route
# ---------------------------------------------------------------------------


def test_diff_route_default(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get("/diff/shoe_sales")
    assert r.status_code == 200
    assert "shoe_sales" in r.text
    assert "stats per column" in r.text
    assert "head — side by side" in r.text
    assert "schema" in r.text


def test_diff_route_explicit(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filter_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get("/diff/shoe_sales/1/2")
    assert r.status_code == 200
    assert "V1" in r.text and "V2" in r.text


def test_diff_route_single_version_400(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get("/diff/shoe_sales")
    assert r.status_code == 400


def test_diff_route_no_alias_404(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    assert c.get("/diff/missing").status_code == 404
