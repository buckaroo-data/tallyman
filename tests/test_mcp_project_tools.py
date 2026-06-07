"""Tests for the MCP project-lifecycle tools and per-response project tag.

Covers T-45:

- ``project_list`` reads the file directly (works without companion).
- ``project_switch`` / ``project_new`` POST to the companion; surface
  errors when it's unreachable.
- Every ``@mcp.tool()`` response carries a ``project`` field.
- A switch detected between two tool calls emits a ``warning``.

The companion endpoints themselves are exercised by
``test_project_switcher.py``; here we mock ``httpx`` to keep the MCP
unit tests fast and self-contained.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from tallyman_core import ensure_project, set_active_project


def _mock_transport(handler):
    """Wrap ``httpx`` so the module-level client used by tallyman_mcp.server
    routes its requests through ``handler`` instead of the network."""
    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    return fake_client


# ---------------------------------------------------------------------------
# project_list — read-only, no companion needed
# ---------------------------------------------------------------------------


def test_project_list_returns_active_and_available(isolated_home: Path):
    from tallyman_mcp.server import project_list

    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    out = project_list()
    assert out["active"] == "alpha"
    assert out["available"] == ["alpha", "beta"]


def test_project_list_active_none_when_no_active(isolated_home: Path):
    from tallyman_mcp.server import project_list

    ensure_project("alpha")
    # No set_active_project — file absent.
    out = project_list()
    assert out["active"] is None
    assert out["available"] == ["alpha"]


# ---------------------------------------------------------------------------
# project_switch — POSTs to companion
# ---------------------------------------------------------------------------


def test_project_switch_happy_path(isolated_home: Path, monkeypatch):
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/projects/switch"
        assert request.method == "POST"
        body = request.read().decode()
        assert '"name": "beta"' in body or '"name":"beta"' in body
        return httpx.Response(200, json={"previous": "alpha", "active": "beta"})

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    out = srv.project_switch("beta")
    assert out["previous"] == "alpha"
    assert out["active"] == "beta"
    assert "error" not in out


def test_project_switch_returns_error_when_unreachable(isolated_home: Path, monkeypatch):
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    set_active_project("alpha")

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("companion down")

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    out = srv.project_switch("beta")
    assert "error" in out
    assert "active" not in out
    # The configured companion URL should appear so the LLM can diagnose.
    assert srv.COMPANION_URL in out["error"]


def test_project_switch_returns_error_on_http_error(isolated_home: Path, monkeypatch):
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    set_active_project("alpha")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "no such project"})

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    out = srv.project_switch("ghost")
    assert "error" in out


# ---------------------------------------------------------------------------
# project_new — POSTs to companion
# ---------------------------------------------------------------------------


def test_project_new_happy_path_default_no_fixture(isolated_home: Path, monkeypatch):
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    set_active_project("alpha")

    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/projects/new"
        captured["body"] = request.read().decode()
        return httpx.Response(200, json={"name": "fresh", "active": "fresh"})

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    out = srv.project_new("fresh")
    assert out["name"] == "fresh"
    assert out["active"] == "fresh"
    # Default with_fixture=False makes it across the wire.
    assert '"with_fixture": false' in captured["body"] or '"with_fixture":false' in captured["body"]


def test_project_new_with_fixture_true(isolated_home: Path, monkeypatch):
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    set_active_project("alpha")

    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.read().decode()
        return httpx.Response(200, json={"name": "demo", "active": "demo"})

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    out = srv.project_new("demo", with_fixture=True)
    assert out["name"] == "demo"
    assert '"with_fixture": true' in captured["body"] or '"with_fixture":true' in captured["body"]


def test_project_new_returns_error_on_409(isolated_home: Path, monkeypatch):
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    set_active_project("alpha")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "project 'alpha' already exists"})

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    out = srv.project_new("alpha")
    assert "error" in out
    assert "alpha" in out["error"] or "409" in out["error"]


# ---------------------------------------------------------------------------
# Per-response project tag
# ---------------------------------------------------------------------------


def test_existing_tool_response_includes_project(project: str, orders_parquet: Path):
    from tallyman_mcp.server import catalog_load_parquet

    out = catalog_load_parquet("orders.parquet", prompt="raw")
    assert out.get("project") == project


def test_list_returning_tool_wrapped_with_project_and_items(project: str, orders_parquet: Path):
    from tallyman_mcp.server import catalog_list, catalog_load_parquet

    catalog_load_parquet("orders.parquet", prompt="raw")
    out = catalog_list()
    # catalog_list previously returned a bare list; the decorator now wraps it.
    assert isinstance(out, dict)
    assert out["project"] == project
    assert isinstance(out["items"], list)
    assert len(out["items"]) >= 1


def test_catalog_list_includes_compact_columns(project: str, orders_parquet: Path):
    # The model should be able to look up an entry's columns before writing an
    # expression against it, without a build. catalog_list carries a compact
    # "name:type, name:type" summary the LLM can scan at a glance.
    from tallyman_mcp.server import catalog_list, catalog_load_parquet

    catalog_load_parquet("orders.parquet", prompt="raw")
    entry = catalog_list()["items"][0]
    cols = entry["columns"]
    assert isinstance(cols, str)
    assert "region" in cols  # a real column name
    assert ":" in cols  # name:type form
    assert ", " in cols  # multiple columns, comma-separated


def test_project_list_includes_project_field(isolated_home: Path):
    from tallyman_mcp.server import project_list

    ensure_project("alpha")
    set_active_project("alpha")
    out = project_list()
    assert out["project"] == "alpha"


# ---------------------------------------------------------------------------
# Switch warning
# ---------------------------------------------------------------------------


def test_external_disk_change_does_not_override_in_process_project(
    isolated_home: Path,
):
    """Once the MCP server seeds its in-process project on the first tool call,
    external writes to the active-project file have no effect — the session is
    sticky to whatever project was active on the first call (or was set via
    project_switch)."""
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    first = srv.project_list()
    assert first["project"] == "alpha"
    assert "warning" not in first  # first call: no warning, just seed

    # External disk change — should NOT affect the in-process session.
    set_active_project("beta")
    second = srv.project_list()
    assert second["project"] == "alpha"  # sticky to first-call value
    assert second["active"] == "alpha"  # active field must match, not read disk
    assert "warning" not in second


def test_switch_warning_emitted_when_project_switch_called(isolated_home: Path, monkeypatch):
    """project_switch() updates _mcp_active_project so subsequent calls see
    the new project; the switch response itself carries the change warning."""
    from tallyman_mcp import server as srv

    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")

    # Seed in-process project via a regular tool call.
    srv.project_list()
    assert srv._mcp_active_project == "alpha"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"previous": "alpha", "active": "beta"})

    monkeypatch.setattr(srv.httpx, "Client", _mock_transport(handler))
    switched = srv.project_switch("beta")
    # The switch response warns that the project changed.
    assert switched["project"] == "beta"
    assert "warning" in switched
    assert "alpha" in switched["warning"] and "beta" in switched["warning"]

    # Subsequent calls use the new project without further warnings.
    next_call = srv.project_list()
    assert next_call["project"] == "beta"
    assert "warning" not in next_call


def test_no_warning_when_project_unchanged(isolated_home: Path):
    from tallyman_mcp import server as srv

    srv._last_project = None
    ensure_project("alpha")
    set_active_project("alpha")
    srv.project_list()
    out = srv.project_list()
    assert "warning" not in out


@pytest.fixture(autouse=True)
def _reset_mcp_server_state():
    """Each test starts with clean module-level MCP server state."""
    from tallyman_mcp import server as srv

    srv._last_project = None
    srv._mcp_active_project = None
    yield
    srv._last_project = None
    srv._mcp_active_project = None
