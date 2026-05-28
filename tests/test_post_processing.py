"""Tests for the project-authored post-processing surface.

Mirror of ``test_summary_stats.py`` for the parallel channel that backs
buckaroo's "post processing" dropdown via
``xorq_loading.load_project_post_processing_klasses``. Two layers:

- ``pydata_core.post_processing``: write/list/remove + the dry-run
  validator that gates ``write_post_processing``.
- MCP tools: the three ``catalog_*_post_processing`` wrappers around
  the core module, exposing the same surface to an agent.

We don't reassert the ``/load_expr`` ``project_root`` plumbing here —
it's covered by ``test_summary_stats.py`` and the same field carries
both channels.
"""
from __future__ import annotations

import pytest

from pydata_core import (
    PostProcessingSourceError,
    list_post_processings,
    remove_post_processing,
    write_post_processing,
)
from pydata_core.post_processing import (
    post_processing_dir,
    validate_post_processing_source,
)

LOW_VOLUME = (
    "def process(expr):\n"
    "    return expr.filter(expr.a < 3)\n"
)
NOOP = "def process(expr):\n    return expr\n"


# ---------------------------------------------------------------------------
# pydata_core.post_processing — write/list/remove + dry-run
# ---------------------------------------------------------------------------

def test_validate_accepts_a_simple_filter():
    validate_post_processing_source("low_volume", LOW_VOLUME)


def test_validate_accepts_a_noop():
    validate_post_processing_source("noop", NOOP)


def test_validate_accepts_pandas_return():
    src = (
        "def process(expr):\n"
        "    return expr.to_pandas()\n"
    )
    validate_post_processing_source("to_pandas", src)


def test_validate_rejects_bad_name():
    with pytest.raises(PostProcessingSourceError, match="not a valid python identifier"):
        validate_post_processing_source("123bad", NOOP)


def test_validate_rejects_no_process():
    with pytest.raises(PostProcessingSourceError, match="must define a callable named 'process'"):
        validate_post_processing_source("no_process", "x = 1\n")


def test_validate_rejects_wrong_arity():
    with pytest.raises(PostProcessingSourceError, match="exactly one parameter"):
        validate_post_processing_source("two_args", "def process(a, b): return a\n")


def test_validate_rejects_non_expr_return():
    with pytest.raises(PostProcessingSourceError, match="must return an ibis expression or pandas DataFrame"):
        validate_post_processing_source("returns_int", "def process(expr): return 42\n")


def test_validate_rejects_import_attempt():
    with pytest.raises(PostProcessingSourceError, match="source raised at exec time"):
        validate_post_processing_source(
            "evil",
            "import os\ndef process(expr): return expr\n",
        )


def test_write_then_list_then_remove_round_trip(project: str):
    written = write_post_processing(project, "low_volume", LOW_VOLUME)
    assert written == post_processing_dir(project) / "low_volume.py"
    assert written.read_text().endswith("\n")  # trailing newline normalised

    listed = list_post_processings(project)
    assert [p["name"] for p in listed] == ["low_volume"]
    assert listed[0]["disabled"] is False

    moved = remove_post_processing(project, "low_volume")
    assert moved is not None
    assert moved.parent.name == "_disabled"
    assert not (post_processing_dir(project) / "low_volume.py").exists()

    after_remove = list_post_processings(project)
    assert [(p["name"], p["disabled"]) for p in after_remove] == [("low_volume", True)]


def test_remove_unknown_returns_none(project: str):
    assert remove_post_processing(project, "never_existed") is None


def test_write_overwrites_existing(project: str):
    write_post_processing(project, "noop", NOOP)
    new_source = "def process(expr):\n    return expr.limit(1)\n"
    write_post_processing(project, "noop", new_source)
    assert (post_processing_dir(project) / "noop.py").read_text() == new_source


def test_listing_with_no_directory_is_empty(project: str):
    assert list_post_processings(project) == []


# ---------------------------------------------------------------------------
# MCP tools — thin wrappers, smoke-tested via direct imports
# ---------------------------------------------------------------------------

def test_mcp_add_remove_list_smoke(project: str):
    from pydata_mcp.server import (
        catalog_add_post_processing,
        catalog_list_post_processings,
        catalog_remove_post_processing,
    )

    add = catalog_add_post_processing("low_volume", LOW_VOLUME)
    assert "error" not in add
    assert add["name"] == "low_volume"

    listed = catalog_list_post_processings()["items"]
    assert any(p["name"] == "low_volume" and not p["disabled"] for p in listed)

    removed = catalog_remove_post_processing("low_volume")
    assert "error" not in removed
    assert removed["name"] == "low_volume"

    listed_after = catalog_list_post_processings()["items"]
    assert any(p["name"] == "low_volume" and p["disabled"] for p in listed_after)


def test_mcp_add_rejects_bad_source(project: str):
    from pydata_mcp.server import catalog_add_post_processing

    resp = catalog_add_post_processing(
        "evil", "import os\ndef process(expr): return expr\n"
    )
    assert "error" in resp
    assert "source raised at exec time" in resp["error"]


def test_mcp_remove_nonexistent_returns_error(project: str):
    from pydata_mcp.server import catalog_remove_post_processing

    resp = catalog_remove_post_processing("never_existed")
    assert resp["error"] == "no post-processing named 'never_existed'"


# ---------------------------------------------------------------------------
# catalog_run_post_processing — test against a real catalog entry
# ---------------------------------------------------------------------------

def _agg_code(project: str) -> str:
    return (
        "from pydata_xorq.io import from_project\n"
        f"t = from_project('orders.parquet', project={project!r})\n"
        "expr = t.group_by('region').aggregate(n=t.count())\n"
    )


def test_run_post_processing_by_alias(project: str, orders_parquet, monkeypatch):
    """run_post_processing resolves an alias and returns filtered rows."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create, catalog_run_post_processing

    catalog_create("agg", _agg_code(project))
    resp = catalog_run_post_processing(
        code="def process(expr):\n    return expr\n",
        entry="agg",
    )
    assert "error" not in resp
    assert resp["row_count"] > 0
    assert "n" in resp["columns"]
    assert isinstance(resp["preview"], list)


def test_run_post_processing_by_hash(project: str, orders_parquet, monkeypatch):
    """run_post_processing accepts a raw content hash."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create, catalog_run_post_processing

    out = catalog_create("agg2", _agg_code(project))
    resp = catalog_run_post_processing(
        code="def process(expr):\n    return expr\n",
        entry=out["hash"],
    )
    assert "error" not in resp
    assert resp["entry"] == out["hash"]


def test_run_post_processing_filter_reduces_rows(project: str, orders_parquet, monkeypatch):
    """A filter in process() produces fewer rows than the original."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create, catalog_run_post_processing

    catalog_create("agg3", _agg_code(project))
    all_rows = catalog_run_post_processing("def process(expr):\n    return expr\n", "agg3")
    filtered = catalog_run_post_processing(
        "def process(expr):\n    return expr.filter(expr.n > 9999)\n", "agg3"
    )
    assert "error" not in all_rows
    assert "error" not in filtered
    assert filtered["row_count"] < all_rows["row_count"]


def test_run_post_processing_bad_code_returns_error(project: str, orders_parquet, monkeypatch):
    """Syntax errors in the process function surface as error dict."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create, catalog_run_post_processing

    catalog_create("agg4", _agg_code(project))
    resp = catalog_run_post_processing("def process(expr):\n    return 42\n", "agg4")
    assert "error" in resp


def test_run_post_processing_unknown_entry_returns_error(project: str, monkeypatch):
    """Unknown alias or hash returns error dict, does not raise."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_run_post_processing

    resp = catalog_run_post_processing("def process(expr):\n    return expr\n", "no_such_entry")
    assert "error" in resp
