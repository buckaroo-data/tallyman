"""Errors recorded outside a tool call reach the model on its next tool response.

``errors.jsonl`` is written by more than the MCP tools: the companion's heal hook records ``unfaithful_heal``, the
browser records chart render failures. No tool returns those, so a model driving tallyman over MCP never saw them.
Every tool response now carries ``new_errors``: the records this MCP process has not reported yet, each once, from this
process's start on, per project. ``record_error`` called directly here stands in for another process.
"""

from __future__ import annotations

import builtins
import io
import json
from pathlib import Path

import httpx
import pytest

from tallyman_core import ensure_project, record_error, set_active_project
from tallyman_core.aliases import get_alias
from tallyman_core.paths import entry_dir, errors_path


def _ids(out: dict) -> list[str]:
    assert "new_errors" in out, out
    return [e["id"] for e in out["new_errors"]]


def _mock_transport(handler):
    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    return fake_client


def test_an_error_recorded_elsewhere_is_reported_once_on_the_next_response(project: str):
    from tallyman_mcp import server as srv

    first = srv.project_list()
    assert "new_errors" not in first  # nothing recorded: no field

    rec = record_error(project, code="unfaithful_heal", message="self-heal produced different rows", hash="abc123")
    out = srv.project_list()
    assert _ids(out) == [rec["id"]]
    item = out["new_errors"][0]
    assert item["code"] == "unfaithful_heal"
    assert item["hash"] == "abc123"
    assert item["message"] == "self-heal produced different rows"
    assert item["created_at"] == rec["created_at"]

    again = srv.project_list()
    assert "new_errors" not in again  # each record is reported once


def test_errors_from_before_this_process_started_are_not_reported(project: str):
    """A record written before the MCP process started is old history: not reported. One written after the process
    started but before this session's first call (the companion healing while the model was idle) is."""
    from tallyman_mcp import server as srv

    old = {"id": "oldoldoldold", "created_at": "2000-01-01T00:00:00+00:00", "code": "unfaithful_heal", "message": "m"}
    path = errors_path(project)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(old) + "\n")
    before_first_call = record_error(project, code="unfaithful_heal", message="healed while idle")

    out = srv.project_list()
    assert _ids(out) == [before_first_call["id"]]


def test_item_is_compact_and_names_the_alias_version(project: str, orders_src: str):
    from tallyman_mcp import server as srv

    srv.project_list()
    h = get_alias(project, orders_src)
    rec = record_error(
        project,
        code="unfaithful_heal",
        message="x" * 2000,
        hash=h,
        tool="api_code",
        traceback="Traceback (most recent call last):\n  boom",
    )
    (item,) = srv.project_list()["new_errors"]
    assert item["id"] == rec["id"]
    assert item["alias"] == orders_src
    assert item["version"] == 1
    assert item["tool"] == "api_code"
    assert len(item["message"]) <= 301 and item["message"].startswith("xxx")
    assert "traceback" not in item and "prompt" not in item


def test_a_build_records_full_code_which_is_not_echoed(project: str):
    """``code`` on a build failure is the recipe source; only a short kind such as ``unfaithful_heal`` is echoed."""
    from tallyman_mcp import server as srv

    srv.project_list()
    rec = record_error(project, code="import os\nexpr = nope\n", message="NameError: nope", tool="api_code")
    (item,) = srv.project_list()["new_errors"]
    assert item["id"] == rec["id"]
    assert "code" not in item


def test_a_tools_own_build_error_is_not_repeated_in_new_errors(project: str):
    from tallyman_mcp import server as srv

    srv.project_list()
    elsewhere = record_error(project, code="unfaithful_heal", message="healed")
    out = srv.catalog_run("expr = nope", prompt="broken")
    assert out["error_id"]
    assert _ids(out) == [elsewhere["id"]]  # the build's own record is in `error`, not repeated


def test_a_cascade_failure_in_the_recalc_report_is_not_repeated(project: str, orders_src: str, monkeypatch):
    """catalog_revise's auto-recalc records the follower's failure (tool=catalog_revise) and reports it in `recalc`."""
    from tallyman_mcp import server as srv

    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    parent = f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({orders_src!r})
expr = t.select("region", "price", "__row_order")
"""
    child = """
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("a")
expr = t.mutate(doubled=t.price * 2)
"""
    srv.catalog_create("a", parent)
    b1 = srv.catalog_create("b", child)["hash"]
    (entry_dir(project, b1) / "expr.py").write_text("broken (((\n")
    elsewhere = record_error(project, code="unfaithful_heal", message="healed")

    out = srv.catalog_revise("a", parent.replace('"__row_order")', '"__row_order").mutate(extra=1)'))
    assert out["recalc"]["status"] == "failed"
    assert _ids(out) == [elsewhere["id"]]


def test_a_heal_recorded_in_process_during_a_call_is_reported(project: str, monkeypatch):
    """The MCP process heals too, when a tool reads an entry. That record is not in the tool's reply, so it is new."""
    from tallyman_mcp import server as srv

    srv.project_list()
    healed = {}

    def list_entries_that_heal(p):
        healed.update(record_error(p, code="unfaithful_heal", message="healed during the read", hash="h1"))
        return []

    monkeypatch.setattr(srv, "list_entries", list_entries_that_heal)
    out = srv.catalog_list()
    assert _ids(out) == [healed["id"]]


def test_the_list_is_capped_with_an_omitted_count(project: str):
    from tallyman_mcp import server as srv

    srv.project_list()
    recs = [record_error(project, code="unfaithful_heal", message=f"m{i}") for i in range(13)]
    out = srv.project_list()
    assert _ids(out) == [r["id"] for r in reversed(recs)][:10]  # the 10 newest, newest first
    assert out["new_errors_omitted"] == 3
    assert "new_errors" not in srv.project_list()


def test_a_torn_line_does_not_break_the_tool_and_a_half_written_line_waits(project: str):
    from tallyman_mcp import server as srv

    srv.project_list()
    a = record_error(project, code="unfaithful_heal", message="a")
    with errors_path(project).open("a") as fh:
        fh.write('{"id": "torn", "code": \n')
        fh.write("[1, 2]\n")
    b = record_error(project, code="unfaithful_heal", message="b")
    out = srv.project_list()
    assert _ids(out) == [b["id"], a["id"]]

    # An append in progress: the line has no newline yet. It is not read until it is complete.
    line = json.dumps({"id": "halfwritten1", "created_at": b["created_at"], "code": "unfaithful_heal", "message": "c"})
    with errors_path(project).open("a") as fh:
        fh.write(line[:20])
    assert "new_errors" not in srv.project_list()
    with errors_path(project).open("a") as fh:
        fh.write(line[20:] + "\n")
    assert _ids(srv.project_list()) == ["halfwritten1"]


def test_an_unchanged_log_is_not_reread(project: str, monkeypatch):
    from tallyman_mcp import server as srv

    srv.project_list()
    rec = record_error(project, code="unfaithful_heal", message="m")
    assert _ids(srv.project_list()) == [rec["id"]]

    opened = []
    real_open = builtins.open

    def spy_open(file, *args, **kwargs):
        opened.append(str(file))
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", spy_open)
    monkeypatch.setattr(io, "open", spy_open)
    out = srv.project_list()
    assert "new_errors" not in out
    assert str(errors_path(project)) not in opened


def test_each_project_has_its_own_cursor_across_project_switch(isolated_home: Path, running_server, monkeypatch):
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    srv.project_list()

    in_alpha = record_error("alpha", code="unfaithful_heal", message="alpha heal")
    in_beta = record_error("beta", code="unfaithful_heal", message="beta heal")
    assert _ids(srv.project_list()) == [in_alpha["id"]]  # beta's record is not alpha's news

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        return httpx.Response(200, json={"previous": srv._mcp_active_project, "active": body["name"]})

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    switched = srv.project_switch("beta")
    assert switched["project"] == "beta"
    assert _ids(switched) == [in_beta["id"]]

    back = srv.project_switch("alpha")
    assert "new_errors" not in back  # alpha's record was already reported
    later = record_error("alpha", code="unfaithful_heal", message="alpha again")
    assert _ids(srv.project_list()) == [later["id"]]


@pytest.fixture(autouse=True)
def _reset_mcp_server_state():
    """Each test starts with clean module-level MCP server state."""
    from tallyman_mcp import server as srv

    srv._last_project = None
    srv._mcp_active_project = None
    yield
    srv._last_project = None
    srv._mcp_active_project = None
