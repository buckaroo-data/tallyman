from __future__ import annotations

import os

import click
import uvicorn

from pydata_core import data_dir, ensure_project, project_dir, resolve_project
from pydata_cli.fixtures import write_shoe_orders


@click.group()
def cli() -> None:
    """pydata — deconstructed-notebook CLI."""


@cli.command("init")
@click.argument("name")
@click.option("--with-fixture/--no-fixture", default=True, help="Generate a synthetic shoe-orders parquet under data/.")
def init_project(name: str, with_fixture: bool) -> None:
    """Initialize a new project under ~/.pydata-app/projects/<name>/."""
    p = ensure_project(name)
    click.echo(f"created {p}")
    if with_fixture:
        out = write_shoe_orders(data_dir(name) / "orders.parquet")
        click.echo(f"wrote fixture {out} ({out.stat().st_size} bytes)")


@cli.command("run")
@click.option("--project", default=None, help="Project name (defaults to PYDATA_PROJECT env or 'spike').")
@click.option("--port", default=7860, type=int)
@click.option("--host", default="127.0.0.1")
def run_companion(project: str | None, port: int, host: str) -> None:
    """Start the companion FastAPI app."""
    project_name = resolve_project(project)
    if not project_dir(project_name).exists():
        raise click.ClickException(f"project '{project_name}' not found. Run `pydata init {project_name}` first.")
    os.environ.setdefault("PYDATA_PROJECT", project_name)
    click.echo(f"pydata run · project={project_name} · http://{host}:{port}")
    # Lazy import so `pydata mcp` doesn't load FastAPI just to spawn an MCP.
    from pydata_companion import create_app

    app = create_app(project_name)
    uvicorn.run(app, host=host, port=port, log_level="info")


@cli.command("mcp")
@click.option("--project", default=None, help="Project name (defaults to PYDATA_PROJECT env or 'spike').")
def run_mcp(project: str | None) -> None:
    """Start the MCP server (this is what Claude Code spawns)."""
    if project:
        os.environ["PYDATA_PROJECT"] = project
    from pydata_mcp import main as mcp_main

    mcp_main()


if __name__ == "__main__":
    cli()
