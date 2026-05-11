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
@click.option("--buckaroo/--no-buckaroo", default=True, help="Manage a Buckaroo subprocess on :8700 for in-table recon.")
@click.option("--buckaroo-port", default=8700, type=int)
def run_companion(
    project: str | None, port: int, host: str, buckaroo: bool, buckaroo_port: int
) -> None:
    """Start the companion FastAPI app."""
    project_name = resolve_project(project)
    if not project_dir(project_name).exists():
        raise click.ClickException(f"project '{project_name}' not found. Run `pydata init {project_name}` first.")
    os.environ.setdefault("PYDATA_PROJECT", project_name)
    click.echo(f"pydata run · project={project_name} · http://{host}:{port}")

    from pydata_companion import create_app
    from pydata_companion.buckaroo_lifecycle import BuckarooManager, BuckarooUnavailable

    bk: BuckarooManager | None = None
    if buckaroo:
        bk = BuckarooManager(project_name, port=buckaroo_port)
        try:
            bk.start()
            click.echo(f"  buckaroo · {bk.base_url}")
        except BuckarooUnavailable as exc:
            click.echo(f"  buckaroo failed to start: {exc} (continuing without it)")
            bk = None

    app = create_app(project_name, buckaroo=bk)
    try:
        uvicorn.run(app, host=host, port=port, log_level="info")
    finally:
        if bk is not None:
            bk.stop()


@cli.command("mcp")
@click.option("--project", default=None, help="Project name (defaults to PYDATA_PROJECT env or 'spike').")
def run_mcp(project: str | None) -> None:
    """Start the MCP server (this is what Claude Code spawns)."""
    if project:
        os.environ["PYDATA_PROJECT"] = project
    from pydata_mcp import main as mcp_main

    mcp_main()


@cli.command("pack")
@click.argument("project_name", required=False)
@click.option("--project", default=None, help="Project name (defaults to PYDATA_PROJECT env or the positional arg).")
@click.option("-o", "--output", default=None, help="Output .tgz path. Defaults to ./<project>-<date>.tgz.")
@click.option("--exclude-cache/--include-cache", default=True, help="Skip xorq cache deps (smaller bundle, paths still portable).")
def pack_project(project_name: str | None, project: str | None, output: str | None, exclude_cache: bool) -> None:
    """Tar a project directory into a portable .tgz artifact.

    The output bundle can be untarred anywhere and served read-only via
    `pydata serve <extracted_dir>`. Portability is provided by the catalog
    entries' `${PYDATA_PROJECT_ROOT}` placeholder rewriting (see plan.md).
    """
    import datetime
    import tarfile
    from pathlib import Path

    name = project or project_name or resolve_project()
    src = project_dir(name)
    if not src.exists():
        raise click.ClickException(f"project {name!r} not found at {src}")

    if output is None:
        stamp = datetime.date.today().isoformat()
        output = f"./{name}-{stamp}.tgz"
    out_path = Path(output).resolve()

    excluded = {"__pycache__", ".DS_Store", "buckaroo_sessions.json"}

    def _filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
        # Exclude in-process state that doesn't survive a hand-off.
        bare = Path(tarinfo.name).name
        if bare in excluded:
            return None
        if exclude_cache and "/cache/" in tarinfo.name:
            return None
        return tarinfo

    click.echo(f"packing {src} → {out_path}")
    with tarfile.open(out_path, "w:gz") as tar:
        tar.add(src, arcname=name, filter=_filter)

    size = out_path.stat().st_size
    click.echo(f"wrote {out_path} ({size/1024:.1f} KB)")
    click.echo(f"recipient: tar xzf {out_path.name} && pydata serve ./{name}")


@cli.command("serve")
@click.argument("project_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--port", default=7860, type=int)
@click.option("--host", default="127.0.0.1")
def serve_project(project_dir: str, port: int, host: str) -> None:
    """Serve a project directory READ-ONLY (no MCP, no edit affordances).

    This is the "hand off the artifact" mode. The recipient runs this against
    a project directory you tarballed up. They see exactly what you saw — same
    catalog, same Buckaroo (when present), same forensic history. No tools
    that mutate state are exposed.
    """
    from pathlib import Path

    abs_path = Path(project_dir).resolve()
    project_name = abs_path.name
    os.environ["PYDATA_PROJECT"] = project_name
    os.environ["PYDATA_PROJECT_PATH"] = str(abs_path)
    click.echo(f"pydata serve · project={project_name} · path={abs_path} · http://{host}:{port}")
    click.echo("  read-only — mutation routes return 403.")

    from pydata_companion import create_app

    app = create_app(project_name, read_only=True)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    cli()
