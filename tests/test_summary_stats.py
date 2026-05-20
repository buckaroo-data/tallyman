"""Tests for the project-authored summary stats surface.

Three layers:

- ``pydata_core.summary_stats``: write/list/remove + the dry-run
  validator that gates ``write_stat``.
- ``BuckarooManager.ensure_session``: must include ``project_root`` in
  the ``/load_expr`` POST body so buckaroo's project-stats scanner
  picks them up (paired with buckaroo PR #784).
- MCP tools: the three ``catalog_*_summary_stat`` wrappers around the
  core module, exposing the same surface to an agent.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pydata_core import StatSourceError, list_stats, remove_stat, write_stat
from pydata_core.paths import project_dir
from pydata_core.summary_stats import stats_dir, validate_stat_source

PERCENT_AT_SIGN = (
    "def compute(col):\n"
    "    return col.cast('string').contains('@').sum() / col.count()\n"
)
N_ROWS = "def compute(col):\n    return col.count()\n"


# ---------------------------------------------------------------------------
# pydata_core.summary_stats — write/list/remove + dry-run
# ---------------------------------------------------------------------------

def test_validate_accepts_a_simple_ibis_expression():
    validate_stat_source("n_rows", N_ROWS)


def test_validate_rejects_bad_name():
    with pytest.raises(StatSourceError, match="not a valid python identifier"):
        validate_stat_source("123bad", N_ROWS)


def test_validate_rejects_no_compute():
    with pytest.raises(StatSourceError, match="must define a callable named 'compute'"):
        validate_stat_source("no_compute", "x = 1\n")


def test_validate_rejects_wrong_arity():
    with pytest.raises(StatSourceError, match="exactly one parameter"):
        validate_stat_source("two_args", "def compute(a, b): return a + b\n")


def test_validate_rejects_non_ibis_return():
    with pytest.raises(StatSourceError, match="must return an ibis expression"):
        validate_stat_source("returns_int", "def compute(col): return 42\n")


def test_validate_rejects_import_attempt():
    with pytest.raises(StatSourceError, match="source raised at exec time"):
        validate_stat_source(
            "evil",
            "import os\ndef compute(col): return col.count()\n",
        )


def test_write_then_list_then_remove_round_trip(project: str):
    written = write_stat(project, "percent_at", PERCENT_AT_SIGN)
    assert written == stats_dir(project) / "percent_at.py"
    assert written.read_text().endswith("\n")  # trailing newline normalised

    listed = list_stats(project)
    assert [s["name"] for s in listed] == ["percent_at"]
    assert listed[0]["disabled"] is False

    moved = remove_stat(project, "percent_at")
    assert moved is not None
    assert moved.parent.name == "_disabled"
    assert not (stats_dir(project) / "percent_at.py").exists()

    after_remove = list_stats(project)
    assert [(s["name"], s["disabled"]) for s in after_remove] == [("percent_at", True)]


def test_remove_unknown_stat_returns_none(project: str):
    assert remove_stat(project, "never_existed") is None


def test_write_overwrites_existing(project: str):
    write_stat(project, "n_rows", N_ROWS)
    new_source = "def compute(col):\n    return col.count() + 1\n"
    write_stat(project, "n_rows", new_source)
    assert (stats_dir(project) / "n_rows.py").read_text() == new_source


def test_write_does_not_create_disabled_dir_on_active_listing(project: str):
    """list_stats returns [] when there's no stats/ dir, not raise."""
    assert list_stats(project) == []


# ---------------------------------------------------------------------------
# BuckarooManager.ensure_session — project_root in the /load_expr POST
# ---------------------------------------------------------------------------

def test_ensure_session_includes_project_root_in_load_expr_body(
    project: str, orders_parquet: Path, monkeypatch
):
    """The /load_expr POST must carry the project's absolute path so
    buckaroo's load_project_stat_klasses (PR #784) can scan its stats/
    directory."""
    from pydata_companion.buckaroo_lifecycle import BuckarooManager
    from pydata_xorq import build_and_persist

    code = f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    res = build_and_persist(project, code)

    mgr = BuckarooManager(project)
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()

    captured: dict[str, Any] = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"session": "abc"}

    def fake_post(url, json=None, timeout=None):
        captured["json"] = json
        return FakeResponse()

    monkeypatch.setattr(mgr._client, "post", fake_post)
    mgr.ensure_session(res.content_hash)

    posted_project_root = captured["json"].get("project_root")
    assert posted_project_root is not None
    assert Path(posted_project_root) == project_dir(project)


# ---------------------------------------------------------------------------
# MCP tools — thin wrappers, smoke-tested via direct imports
# ---------------------------------------------------------------------------

def test_mcp_add_remove_list_smoke(project: str):
    """Drive the three tools directly (no MCP transport) — they should
    just forward to pydata_core.summary_stats and the companion-notify
    layer (which is best-effort and silent here)."""
    from pydata_mcp.server import (
        catalog_add_summary_stat,
        catalog_list_summary_stats,
        catalog_remove_summary_stat,
    )

    add = catalog_add_summary_stat("percent_at", PERCENT_AT_SIGN)
    assert "error" not in add
    assert add["name"] == "percent_at"

    listed = catalog_list_summary_stats()
    assert any(s["name"] == "percent_at" and not s["disabled"] for s in listed)

    removed = catalog_remove_summary_stat("percent_at")
    assert "error" not in removed
    assert removed["name"] == "percent_at"

    listed_after = catalog_list_summary_stats()
    assert any(s["name"] == "percent_at" and s["disabled"] for s in listed_after)


def test_mcp_add_rejects_bad_source(project: str):
    """A bad source returns ``{"error": <msg>}`` rather than raising —
    matches the convention every catalog_* tool follows."""
    from pydata_mcp.server import catalog_add_summary_stat

    resp = catalog_add_summary_stat("evil", "import os\ndef compute(c): return c.count()\n")
    assert "error" in resp
    assert "source raised at exec time" in resp["error"]


def test_mcp_remove_nonexistent_returns_error(project: str):
    from pydata_mcp.server import catalog_remove_summary_stat

    resp = catalog_remove_summary_stat("never_existed")
    assert resp == {"error": "no stat named 'never_existed'"}
