"""Real wire-protocol test: spawn `pydata mcp` and drive it over stdio.

This is the test that proves a fresh Claude Code session pointed at our
`.mcp.json` will round-trip tool calls. It spawns the CLI as a subprocess
and uses fastmcp's Client + StdioTransport to speak the actual MCP
protocol. All in-process tests cover semantics; this test covers wire.

Slow (~3-5s per test because of process startup); kept in a single
module so the suite cost is bounded.
"""
from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest
from fastmcp import Client
from fastmcp.client.transports import StdioTransport

REPO_ROOT = Path(__file__).resolve().parent.parent


def _has_uv() -> bool:
    return subprocess.run(["which", "uv"], capture_output=True).returncode == 0


needs_uv = pytest.mark.skipif(not _has_uv(), reason="uv not on PATH")


def _client(project_name: str, pydata_home: Path) -> Client:
    env = {
        **os.environ,
        "PYDATA_PROJECT": project_name,
        "PYDATA_HOME": str(pydata_home),
    }
    transport = StdioTransport(
        command="uv",
        args=["run", "pydata", "mcp"],
        env=env,
        cwd=str(REPO_ROOT),
    )
    return Client(transport)


@needs_uv
def test_stdio_lists_tools(isolated_home: Path, project: str):
    async def go():
        async with _client(project, isolated_home) as client:
            tools = await client.list_tools()
            return {t.name for t in tools}

    names = asyncio.run(go())
    # All tools from server.py should be present.
    for required in {
        "catalog_run",
        "catalog_load_parquet",
        "catalog_create",
        "catalog_revise",
        "catalog_alias",
        "catalog_rename",
        "catalog_unalias",
        "catalog_list",
        "catalog_diff",
        "notebook_reorder",
        "notebook_remove",
        "notebook_edit_markdown",
    }:
        assert required in names, f"missing tool over stdio: {required}"


@needs_uv
def test_stdio_round_trips_catalog_load_parquet(
    isolated_home: Path, project: str, orders_parquet: Path
):
    async def go():
        async with _client(project, isolated_home) as client:
            result = await client.call_tool(
                "catalog_load_parquet",
                {"rel_path": "orders.parquet", "prompt": "raw"},
            )
            return result.data

    out = asyncio.run(go())
    assert "hash" in out
    assert out["row_count"] == 200  # the conftest fixture size


@needs_uv
def test_stdio_create_revise_diff_round_trip(
    isolated_home: Path, project: str, orders_parquet: Path
):
    async def go():
        async with _client(project, isolated_home) as client:
            code1 = f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
            code2 = f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(n=filtered.count())
"""
            r1 = await client.call_tool(
                "catalog_create", {"name": "shoe_sales", "code": code1, "prompt": "v1"}
            )
            r2 = await client.call_tool(
                "catalog_revise", {"name": "shoe_sales", "code": code2, "prompt": "v2"}
            )
            r3 = await client.call_tool("catalog_diff", {"name": "shoe_sales"})
            return r1.data, r2.data, r3.data

    v1, v2, diff = asyncio.run(go())
    assert v1["version"] == 1
    assert v2["version"] == 2
    assert diff["before"]["version"] == 1
    assert diff["after"]["version"] == 2
    assert diff["keyed_summary"] is not None
