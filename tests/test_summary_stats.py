"""Tests for the project-authored summary stats surface.

Three layers:

- ``tallyman_core.summary_stats``: write/list/remove + the dry-run
  validator that gates ``write_stat``.
- ``BuckarooManager.ensure_session``: the ``project_root`` in the
  ``/load_expr`` POST body must be where buckaroo's loaders find the
  stats, post-processing functions and display klasses tallyman wrote.
- MCP tools: the three ``catalog_*_summary_stat`` wrappers around the
  core module, exposing the same surface to an agent.
"""

from __future__ import annotations

from typing import Any

import pytest

from tallyman_core import StatSourceError, list_stats, remove_stat, write_stat
from tallyman_core.summary_stats import stats_dir, validate_stat_source

PERCENT_AT_SIGN = "def compute(col):\n    return col.cast('string').contains('@').sum() / col.count()\n"
N_ROWS = "def compute(col):\n    return col.count()\n"


# ---------------------------------------------------------------------------
# tallyman_core.summary_stats — write/list/remove + dry-run
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


MAIN_STYLING = """
class ProjectMain(DefaultMainStyling):
    df_display_name = "main"
"""


def test_ensure_session_project_root_finds_the_project_klasses(project: str, orders_src: str, monkeypatch):
    """Buckaroo loads a session's project stats, post-processing functions and
    display klasses from ``stats/``, ``post_processing/`` and ``display/`` under
    the ``project_root`` in the /load_expr POST. Everything tallyman's writers
    put on disk must be found by buckaroo's own loaders given that root (#170)."""
    from buckaroo.server import xorq_loading

    from tallyman_companion.buckaroo_lifecycle import BuckarooManager
    from tallyman_core import write_display_klass, write_post_processing
    from tallyman_xorq import build_and_persist

    write_stat(project, "n_present", N_ROWS)
    write_post_processing(project, "noop", "def process(expr):\n    return expr\n")
    write_display_klass(project, "project_main", MAIN_STYLING)

    code = f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r}, project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    res = build_and_persist(project, code)

    mgr = BuckarooManager()
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
    mgr.ensure_session(res.content_hash, project)

    root = captured["json"].get("project_root")
    assert root is not None
    assert [f.__name__ for f in xorq_loading.load_project_stat_klasses(root)] == ["n_present"]
    assert [k.post_processing_method for k in xorq_loading.load_project_post_processing_klasses(root)] == ["noop"]
    assert [k.__name__ for k in xorq_loading.load_project_display_klasses(root)] == ["ProjectMain"]


# ---------------------------------------------------------------------------
# MCP tools — thin wrappers, smoke-tested via direct imports
# ---------------------------------------------------------------------------


def test_mcp_add_remove_list_smoke(project: str):
    """Drive the three tools directly (no MCP transport) — they should
    just forward to tallyman_core.summary_stats and the companion-notify
    layer (which is best-effort and silent here)."""
    from tallyman_mcp.server import (
        catalog_add_summary_stat,
        catalog_list_summary_stats,
        catalog_remove_summary_stat,
    )

    add = catalog_add_summary_stat("percent_at", PERCENT_AT_SIGN)
    assert "error" not in add
    assert add["name"] == "percent_at"

    listed = catalog_list_summary_stats()["items"]
    assert any(s["name"] == "percent_at" and not s["disabled"] for s in listed)

    removed = catalog_remove_summary_stat("percent_at")
    assert "error" not in removed
    assert removed["name"] == "percent_at"

    listed_after = catalog_list_summary_stats()["items"]
    assert any(s["name"] == "percent_at" and s["disabled"] for s in listed_after)


def test_mcp_add_rejects_bad_source(project: str):
    """A bad source returns ``{"error": <msg>}`` rather than raising —
    matches the convention every catalog_* tool follows."""
    from tallyman_mcp.server import catalog_add_summary_stat

    resp = catalog_add_summary_stat("evil", "import os\ndef compute(c): return c.count()\n")
    assert "error" in resp
    assert "source raised at exec time" in resp["error"]


def test_mcp_remove_nonexistent_returns_error(project: str):
    from tallyman_mcp.server import catalog_remove_summary_stat

    resp = catalog_remove_summary_stat("never_existed")
    assert resp["error"] == "no stat named 'never_existed'"
