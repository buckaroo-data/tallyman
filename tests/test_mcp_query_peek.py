"""catalog_peek and catalog_query: the MCP tools that hand rows back to the model and write nothing.

Without them a model explores data and checks results in pandas against the raw parquet files, because catalog_run
always persists an entry and no tool returns rows.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from tallyman_core import catalog_state as cs
from tallyman_core import list_errors, load_aliases, notebook, project_dir
from tallyman_mcp import server
from tallyman_xorq import list_entries


def _tool(name: str):
    """The tool, looked up when the test runs, so a missing tool fails its own tests and not the whole module."""
    return getattr(server, name)


def _agg(src: str, by: str = "region") -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r})
expr = t.group_by({by!r}).aggregate(n=t.count())
"""


def _stamped(src: str) -> str:
    # A cheap entry (row-preserving) with a timestamp column and an all-null float column.
    return f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r})
expr = t.mutate(ts=ibis.timestamp("2024-01-02 03:04:05"), maybe=ibis.null().cast("float64"))
"""


def _strict_json(out: dict) -> dict:
    """What an MCP client receives: plain JSON with no NaN."""
    return json.loads(json.dumps(out, allow_nan=False))


# ---------------------------------------------------------------------------
# catalog_peek
# ---------------------------------------------------------------------------


def test_peek_by_alias_returns_json_safe_rows_in_row_order(project: str, orders_src: str):
    made = server.catalog_create("stamped", _stamped(orders_src))
    assert "error" not in made, made

    out = _tool("catalog_peek")("stamped", limit=5)

    assert "error" not in out, out
    assert out["row_count"] == 200
    assert out["truncated"] is True
    assert len(out["rows"]) == 5
    names = [f["name"] for f in out["schema"]["fields"]]
    assert {"order_id", "region", "price", "ts", "maybe", "__row_order"} <= set(names)
    assert [r["__row_order"] for r in out["rows"]] == [0, 1, 2, 3, 4]
    first = out["rows"][0]
    assert first["ts"] == "2024-01-02T03:04:05"
    assert first["maybe"] is None
    assert _strict_json(out)["rows"] == out["rows"]


def test_peek_by_content_hash(project: str, orders_src: str):
    built = server.catalog_run(_agg(orders_src))
    assert "error" not in built, built

    out = _tool("catalog_peek")(built["hash"])

    assert "error" not in out, out
    assert out["row_count"] == 4
    assert out["truncated"] is False
    assert sorted(r["region"] for r in out["rows"]) == sorted({r["region"] for r in out["rows"]})
    assert sum(r["n"] for r in out["rows"]) == 200


def test_peek_by_version_ref_reads_that_version(project: str, orders_src: str):
    assert "error" not in server.catalog_create("agg", _agg(orders_src, "region"))
    assert "error" not in server.catalog_revise("agg", _agg(orders_src, "category"))
    peek = _tool("catalog_peek")

    v1, v2, head = peek("agg-v1"), peek("agg-v2"), peek("agg")

    assert "region" in v1["rows"][0] and "category" not in v1["rows"][0]
    assert "category" in v2["rows"][0] and "region" not in v2["rows"][0]
    assert head["rows"] == v2["rows"]


def test_peek_columns_and_limit(project: str, orders_src: str):
    out = _tool("catalog_peek")(orders_src, limit=3, columns=["region", "price"])

    assert "error" not in out, out
    assert [f["name"] for f in out["schema"]["fields"]] == ["region", "price"]
    assert [set(r) for r in out["rows"]] == [{"region", "price"}] * 3
    assert out["row_count"] == 200
    assert out["truncated"] is True


def test_peek_unknown_column_names_the_real_ones(project: str, orders_src: str):
    out = _tool("catalog_peek")(orders_src, columns=["regoin"])

    assert "regoin" in out["error"]
    assert "region" in out["error"]


def test_peek_unknown_ref_is_an_error(project: str, orders_src: str):
    out = _tool("catalog_peek")("no_such_thing")

    assert "no_such_thing" in out["error"]
    assert "rows" not in out


def test_peek_version_out_of_range_says_how_many_there_are(project: str, orders_src: str):
    out = _tool("catalog_peek")(f"{orders_src}-v7")

    assert "v1..v1" in out["error"]


# ---------------------------------------------------------------------------
# catalog_query
# ---------------------------------------------------------------------------


def test_query_returns_first_rows_and_full_row_count(project: str, orders_src: str, orders_parquet: Path):
    code = f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.filter(t.price > 100).select("order_id", "price", "__row_order")
"""
    expected = int((pd.read_parquet(orders_parquet)["price"] > 100).sum())

    out = _tool("catalog_query")(code, limit=7)

    assert "error" not in out, out
    assert out["row_count"] == expected
    assert len(out["rows"]) == 7
    assert out["truncated"] is True
    assert [f["name"] for f in out["schema"]["fields"]] == ["order_id", "price", "__row_order"]
    assert all(r["price"] > 100 for r in out["rows"])
    positions = [r["__row_order"] for r in out["rows"]]
    assert positions == sorted(positions)  # a cheap result comes back in __row_order order


def test_query_limit_is_capped(project: str):
    from tallyman_cli.fixtures import write_shoe_orders
    from tallyman_core import data_dir
    from tallyman_xorq.source_import import update_and_depend

    update_and_depend(write_shoe_orders(data_dir(project) / "big.parquet", n_rows=1500, seed=1), "big", project=project)
    code = """
from tallyman_xorq.io import tracked_expr_from_alias
expr = tracked_expr_from_alias("big")
"""
    out = _tool("catalog_query")(code, limit=5000)

    assert "error" not in out, out
    assert out["row_count"] == 1500
    assert len(out["rows"]) == 1000
    assert out["truncated"] is True


def test_query_values_are_json_safe(project: str, orders_src: str):
    code = f"""
import decimal
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.limit(2).select(
    ts=ibis.timestamp("2024-01-02 03:04:05"),
    d=ibis.date("2024-01-02"),
    null_f=ibis.null().cast("float64"),
    nan_f=ibis.literal(float("nan"), type="float64"),
    dec=ibis.literal(decimal.Decimal("1.50"), type="decimal(10, 2)"),
    arr=ibis.array([1, 2]),
    st=ibis.struct({{"a": 1, "b": "x"}}),
)
"""
    out = _tool("catalog_query")(code)

    assert "error" not in out, out
    row = _strict_json(out)["rows"][0]
    assert row["ts"] == "2024-01-02T03:04:05"
    assert row["d"] == "2024-01-02"
    assert row["null_f"] is None
    assert row["nan_f"] is None
    assert row["dec"] == 1.5
    assert row["arr"] == [1, 2]
    assert row["st"] == {"a": 1, "b": "x"}


def _tree(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _manifest_parents(project: str) -> dict[str, object]:
    from tallyman_core import entry_dir

    return {
        e["content_hash"]: json.loads((entry_dir(project, e["content_hash"]) / "manifest.json").read_text()).get(
            "parents"
        )
        for e in list_entries(project)
    }


def test_query_and_peek_persist_nothing(project: str, orders_src: str, monkeypatch, tmp_path: Path):
    # A named entry first: the catalog has a revision history and a notebook cell to compare against.
    assert "error" not in server.catalog_create("agg", _agg(orders_src))
    scratch = tmp_path / "scratch_tmp"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    notified: list[tuple] = []
    monkeypatch.setattr(server, "_notify", lambda *a, **k: notified.append((a, k)))
    _tool("catalog_peek")("agg")  # warm the parent's read before taking the snapshot

    before = {
        "tree": _tree(project_dir(project)),
        "entries": [e["content_hash"] for e in list_entries(project)],
        "aliases": load_aliases(project),
        "cells": notebook.load(project)["cells"],
        "errors": list_errors(project),
        "revisions": cs.list_revisions(project),
        "parents": _manifest_parents(project),
    }
    code = """
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("agg")
expr = t.filter(t.n > 0)
"""
    from tallyman_xorq import parent_capture as pc

    outer = pc.begin_collect()  # a query inside someone else's build must not add an edge to it
    try:
        out = _tool("catalog_query")(code)
        peeked = _tool("catalog_peek")("agg")
    finally:
        leaked = pc.end_collect(outer)

    assert "error" not in out, out
    assert "error" not in peeked, peeked
    assert out["row_count"] == 4
    after = {
        "tree": _tree(project_dir(project)),
        "entries": [e["content_hash"] for e in list_entries(project)],
        "aliases": load_aliases(project),
        "cells": notebook.load(project)["cells"],
        "errors": list_errors(project),
        "revisions": cs.list_revisions(project),
        "parents": _manifest_parents(project),
    }
    assert after == before
    assert leaked == []
    assert notified == []
    assert list(scratch.iterdir()) == []  # the query's script is not left in the temp dir


def test_query_error_matches_catalog_run_and_is_not_recorded(project: str, orders_src: str, monkeypatch):
    code = f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.filter(t.nope > 0)
"""
    notified: list[tuple] = []
    monkeypatch.setattr(server, "_notify", lambda *a, **k: notified.append((a, k)))
    entries = list_entries(project)

    out = _tool("catalog_query")(code)

    assert set(out) == {"error", "project"}
    assert "nope" in out["error"]
    assert list_errors(project) == []
    assert notified == []
    assert list_entries(project) == entries

    built = server.catalog_run(code)
    assert out["error"].split("\nTraceback")[0] == built["error"].split("\nTraceback")[0]


@pytest.mark.parametrize(
    ("code", "says"),
    [
        ("x = 1\n", "variable 'expr' not found"),
        ("expr = nope\n", "nope"),
    ],
    ids=["no-expr", "name-error"],
)
def test_query_errors_follow_catalog_run_rules(project: str, code: str, says: str):
    out = _tool("catalog_query")(code)

    assert says in out["error"]
    assert list_errors(project) == []


def _without_row_order(rows: list[dict]) -> list[dict]:
    return [{k: v for k, v in r.items() if k != "__row_order"} for r in rows]


def test_query_rows_are_the_first_rows_of_the_entry_catalog_run_builds(project: str, orders_src: str):
    """A hash aggregate's output order depends on how DataFusion partitions it, so its first rows differ call to call.
    The build sorts a worthy entry canonically; query does too, so its rows are the ones the built entry serves."""
    code = _agg(orders_src, by="order_id")

    runs = [_tool("catalog_query")(code, limit=10) for _ in range(5)]
    built = server.catalog_run(code)
    peeked = _tool("catalog_peek")(built["hash"], limit=10)

    assert "error" not in peeked, peeked
    for out in runs:
        assert "error" not in out, out
        assert _without_row_order(out["rows"]) == _without_row_order(peeked["rows"])


def test_query_compiles_a_recipe_as_catalog_run_imports_it(project: str, orders_src: str):
    """query.py's ``from __future__ import annotations`` must not reach the recipe: its annotations stay objects."""
    code = f"""
def keep(x: int) -> int:
    return x


assert keep.__annotations__ == {{"x": int, "return": int}}, keep.__annotations__
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.filter(t.price > 0)
"""
    out = _tool("catalog_query")(code, limit=1)

    assert "error" not in out, out
    assert "error" not in server.catalog_run(code)


@pytest.mark.parametrize(
    "body",
    [
        'import xorq.api as xo\nexpr = xo.memtable({{"i": [1, 2]}})\n',
        'from tallyman_xorq.io import tracked_expr_from_alias\nt = tracked_expr_from_alias("{src}")\n'
        'expr = t.select("order_id", "price")\n',
        'from tallyman_xorq.io import tracked_expr_from_alias\nt = tracked_expr_from_alias("{src}")\n'
        "expr = t.mutate(__row_order=t.order_id)\n",
    ],
    ids=["in-memory-table", "cheap-select-drops-row-order", "assigns-row-order"],
)
def test_query_refuses_what_catalog_run_refuses_with_its_message(project: str, orders_src: str, body: str):
    code = body.format(src=orders_src)

    out = _tool("catalog_query")(code)
    built = server.catalog_run(code)

    assert "error" in built, built
    assert "error" in out, out
    assert out["error"].split("\nTraceback")[0] == built["error"].split("\nTraceback")[0]


def test_query_refuses_a_raw_file_read_as_catalog_run_does(project: str, orders_parquet: Path):
    code = f"import xorq.api as xo\nexpr = xo.deferred_read_parquet({str(orders_parquet)!r})\n"

    out = _tool("catalog_query")(code)

    assert "catalog_import_source" in out["error"]
    assert list_errors(project) == []


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------


def test_both_tools_are_registered_and_take_no_checkpoint():
    import asyncio

    tools = {tool.name for tool in asyncio.run(server.mcp.list_tools())}

    assert {"catalog_peek", "catalog_query"} <= tools
    assert {"catalog_peek", "catalog_query"} <= server._NO_CHECKPOINT
    assert {"catalog_peek", "catalog_query"} <= server._CHECKPOINTED_TOOLS
