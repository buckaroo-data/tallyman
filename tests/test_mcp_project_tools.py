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

from pydata_core import ensure_project, set_active_project


def _mock_transport(handler):
    """Wrap ``httpx`` so the module-level client used by pydata_mcp.server
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
    from pydata_mcp.server import project_list

    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    out = project_list()
    assert out["active"] == "alpha"
    assert out["available"] == ["alpha", "beta"]


def test_project_list_active_none_when_no_active(isolated_home: Path):
    from pydata_mcp.server import project_list

    ensure_project("alpha")
    # No set_active_project — file absent.
    out = project_list()
    assert out["active"] is None
    assert out["available"] == ["alpha"]


# ---------------------------------------------------------------------------
# project_switch — POSTs to companion
# ---------------------------------------------------------------------------


def test_project_switch_happy_path(isolated_home: Path, monkeypatch):
    from pydata_mcp import server as srv

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
    from pydata_mcp import server as srv

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
    from pydata_mcp import server as srv

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
    from pydata_mcp import server as srv

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
    from pydata_mcp import server as srv

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
    from pydata_mcp import server as srv

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
    from pydata_mcp.server import catalog_load_parquet

    out = catalog_load_parquet("orders.parquet", prompt="raw")
    assert out.get("project") == project


def test_list_returning_tool_wrapped_with_project_and_items(
    project: str, orders_parquet: Path
):
    from pydata_mcp.server import catalog_list, catalog_load_parquet

    catalog_load_parquet("orders.parquet", prompt="raw")
    out = catalog_list()
    # catalog_list previously returned a bare list; the decorator now wraps it.
    assert isinstance(out, dict)
    assert out["project"] == project
    assert isinstance(out["items"], list)
    assert len(out["items"]) >= 1


def test_project_list_includes_project_field(isolated_home: Path):
    from pydata_mcp.server import project_list

    ensure_project("alpha")
    set_active_project("alpha")
    out = project_list()
    assert out["project"] == "alpha"


# ---------------------------------------------------------------------------
# Switch warning
# ---------------------------------------------------------------------------


def test_switch_warning_emitted_when_project_changes_between_calls(
    isolated_home: Path,
):
    """Two tool calls with the active-project file changing between them:
    the second response carries the documented warning string."""
    from pydata_mcp import server as srv

    # Reset module state so test order doesn't matter.
    srv._last_project = None

    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    first = srv.project_list()
    assert first["project"] == "alpha"
    assert "warning" not in first  # first call: no warning, just seed

    set_active_project("beta")
    second = srv.project_list()
    assert second["project"] == "beta"
    assert "warning" in second
    assert second["warning"] == (
        "active project changed from 'alpha' to 'beta' since the last call; "
        "proceeding with 'beta'"
    )


def test_no_warning_when_project_unchanged(isolated_home: Path):
    from pydata_mcp import server as srv

    srv._last_project = None
    ensure_project("alpha")
    set_active_project("alpha")
    srv.project_list()
    out = srv.project_list()
    assert "warning" not in out


@pytest.fixture(autouse=True)
def _reset_last_project():
    """Each test starts with a clean module-level last-project state."""
    from pydata_mcp import server as srv

    srv._last_project = None
    yield
    srv._last_project = None
