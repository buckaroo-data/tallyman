from __future__ import annotations

from pathlib import Path

import pytest

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


# DataFusion's SanityCheckPlan rejects a window keyed on .contains() (strpos) or .re_search(): the SortExec below
# the window sorts on the same expression, but the check does not see the ordering as satisfied.
_WINDOW_ON_COMPUTED_KEY = {
    "order_by": 'ibis.window(group_by="region", order_by=[t.category.contains("a").cast("int8"), t.order_id])',
    "group_by": 'ibis.window(group_by=t.category.re_search("a"), order_by=t.order_id)',
}


@pytest.mark.parametrize("window", list(_WINDOW_ON_COMPUTED_KEY.values()), ids=list(_WINDOW_ON_COMPUTED_KEY))
def test_a_window_keyed_on_a_computed_expression_fails_with_a_hint_to_mutate_it_first(
    project: str, orders_src: str, monkeypatch, window: str
):
    """The error names a physical plan node, not the recipe line. The hint, in the error text as every build error's
    is (so the companion, recalc and catalog_query show it too), says to mutate the key into a column first."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.mutate(rn=ibis.row_number().over({window}))
"""
    out = catalog_run(code, prompt="window on a computed key")
    assert "SanityCheckPlan" in out["error"]
    assert "Hint:" in out["error"]
    assert "mutate" in out["error"].split("\nTraceback")[0].split("Hint:")[1]
    assert "hint" not in out


def test_an_ordered_aggregate_above_a_plain_window_gets_no_window_hint(project: str, orders_src: str, monkeypatch):
    """The plan check fails on the aggregate's ORDER BY a computed key; the window below it is keyed on a column. The
    printed plan names the window node too, but the window is not what to change."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
t = t.mutate(rn=ibis.row_number().over(ibis.window(group_by="region", order_by=t.order_id)))
expr = t.group_by("region").aggregate(f=ibis._.order_id.collect(order_by=[ibis._.category.contains("a"), ibis._.rn]))
"""
    out = catalog_run(code, prompt="ordered collect above a window")
    assert "SanityCheckPlan" in out["error"]
    assert "window over that column" not in out["error"] + out.get("hint", "")


def test_an_error_with_no_known_fix_and_a_build_that_succeeds_carry_no_hint(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    assert "Hint:" not in catalog_run("expr = nope", prompt="broken")["error"]
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
t = t.mutate(k=t.category.contains("a"))
expr = t.mutate(rn=ibis.row_number().over(ibis.window(group_by="region", order_by=[t.k, t.order_id])))
"""
    out = catalog_run(code, prompt="the key mutated first")
    assert "error" not in out
    assert "hint" not in out


def test_the_window_hint_matches_the_error_as_the_arrow_reader_wraps_it():
    """A transcript saw the same failure raised through the Arrow C stream, prefixed and naming the bounded node."""
    from tallyman_xorq.build import _error_hint

    message = 'Arrow error: C Data interface error: Invalid: SanityCheckPlan\nPlan: ["BoundedWindowAggExec: ...'
    assert "mutate" in _error_hint(message)
    assert _error_hint('SanityCheckPlan\ncaused by\nPlan: ["SortExec: ...') == ""


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


def _docstrings(tree) -> set[int]:
    """The ids of the string constants that are module, class or function docstrings."""
    import ast

    owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    return {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, owners)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }


def _unknown_catalog_names(src: Path, tools: set[str]) -> dict[str, list[str]]:
    """Each ``catalog_*`` name a string under *src* quotes (a message, not a docstring) that is neither in *tools*
    nor a module, function or class under *src*, with where it is quoted. A variable, parameter or attribute of the
    same spelling is not something a message can send an agent to."""
    import ast
    import re

    defined: set[str] = set()
    quoted: dict[str, list[str]] = {}
    for path in sorted(src.rglob("*.py")):
        tree = ast.parse(path.read_text())
        defined.add(path.stem)
        docstrings = _docstrings(tree)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                defined.add(node.name)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
                for name in re.findall(r"\bcatalog_[a-z_]+\b", node.value):
                    quoted.setdefault(name, []).append(f"{path.relative_to(src)}:{node.lineno}")
    return {name: where for name, where in quoted.items() if name not in tools | defined}


def test_every_catalog_name_a_message_in_src_quotes_is_a_tool_or_a_python_name():
    """#234: an import refusal sent agents to ``catalog_reset_to``, which is no tool. A ``catalog_*`` name in a string
    the code builds (a message, not a docstring) is either a registered MCP tool or a name defined in ``src/``, such
    as the ``catalog_state`` module. Docstrings are left out, since they may name functions that were deleted."""
    import asyncio

    from tallyman_mcp.server import mcp

    tools = {tool.name for tool in asyncio.run(mcp.list_tools())}
    src = Path(__file__).resolve().parent.parent / "src"

    assert _unknown_catalog_names(src, tools) == {}


_SAME_SPELLING = {
    "a parameter": "def undo(catalog_reset_to=None):\n    return 'call catalog_reset_to'\n",
    "a local variable": "catalog_reset_to = 1\nMESSAGE = 'call catalog_reset_to'\n",
    "an attribute": "def undo(ops):\n    ops.catalog_reset_to()\n    return 'call catalog_reset_to'\n",
}


@pytest.mark.parametrize("body", list(_SAME_SPELLING.values()), ids=list(_SAME_SPELLING))
def test_the_guard_catches_a_stale_tool_name_spelled_like_a_variable(tmp_path: Path, body: str):
    """#234 review. The guard counted every name, attribute and parameter under ``src/`` as defined, so a message
    naming a tool that does not exist passed whenever some variable was spelled the same way. Only modules,
    functions and classes are names a message can mean."""
    (tmp_path / "ops.py").write_text(body)

    assert list(_unknown_catalog_names(tmp_path, tools=set())) == ["catalog_reset_to"]
