from __future__ import annotations

import os
from pathlib import Path

import click
import uvicorn

from pydata_cli.fixtures import write_shoe_orders
from pydata_core import data_dir, ensure_project, project_dir, resolve_project


@click.group()
def cli() -> None:
    """pydata — deconstructed-notebook CLI."""


@cli.command("init")
@click.argument("name")
@click.option(
    "--with-fixture/--no-fixture",
    default=True,
    help="Generate a synthetic shoe-orders parquet under data/.",
)
@click.option(
    "--force",
    is_flag=True,
    help=(
        "Re-run init even if the project already exists. Overwrites the fixture if "
        "--with-fixture; never touches catalog entries or aliases."
    ),
)
def init_project(name: str, with_fixture: bool, force: bool) -> None:
    """Initialize a new project under ~/.pydata-app/projects/<name>/."""
    from pydata_core import project_dir

    already_exists = project_dir(name).exists()
    if already_exists and not force:
        raise click.ClickException(
            f"project {name!r} already exists at {project_dir(name)}. "
            "Use --force to re-init (preserves catalog; overwrites the fixture)."
        )
    p = ensure_project(name)
    click.echo(f"{'re-initialised' if already_exists else 'created'} {p}")
    if with_fixture:
        out = write_shoe_orders(data_dir(name) / "orders.parquet")
        click.echo(f"wrote fixture {out} ({out.stat().st_size} bytes)")
    from pydata_core.catalog_state import genesis

    if genesis(name) is not None:
        click.echo("recorded step-000 genesis baseline")


@cli.command("reset-to")
@click.argument("ref")
@click.option("--project", "project_opt", default=None, help="Project name override.")
def reset_to_step(ref: str, project_opt: str | None) -> None:
    """Restore the project to a step number or label (e.g. 3 or baseline)."""
    from pydata_core.catalog_state import current_step, reset_to

    name = resolve_project(project_opt)
    if name is None:
        raise click.ClickException("no active project — pass --project")
    target: int | str = int(ref) if ref.isdigit() else ref
    try:
        reset_to(name, target)
    except RuntimeError as exc:
        raise click.ClickException(str(exc))
    step = current_step(name)
    where = f"step-{step:03d}" if step is not None else str(target)
    click.echo(f"reset {name} to {where}")
    _notify_companion_reset(name)


def _notify_companion_reset(project: str) -> None:
    """Best-effort: a running companion reloads buckaroo sessions + browsers.

    Names the project explicitly: ``--project`` may target a project that is
    not the companion's active one, and the active-project fallback would
    reload the wrong sessions.
    """
    import httpx

    url = os.environ.get("PYDATA_COMPANION_URL", "http://127.0.0.1:7860")
    try:
        httpx.post(f"{url}/internal/notify", json={"kind": "project_reset", "project": project}, timeout=2.0)
    except Exception:
        pass  # companion may not be running; the reset itself already landed


@cli.group("revisions", invoke_without_command=True)
@click.option("--project", "project_opt", default=None, help="Project name override.")
@click.pass_context
def revisions_group(ctx: click.Context, project_opt: str | None) -> None:
    """List the revision timeline; `label` names a step for reset-to."""
    if ctx.invoked_subcommand is not None:
        return
    from pydata_core.catalog_state import list_revisions

    name = resolve_project(project_opt)
    if name is None:
        raise click.ClickException("no active project — pass --project")
    revs = list_revisions(name)
    if not revs:
        click.echo("no revisions yet")
        return
    for r in revs:
        marker = "*" if r["current"] else " "
        labels = f"  ({', '.join(r['labels'])})" if r["labels"] else ""
        click.echo(f"{marker} step-{r['step']:03d}  {r['commit']}  {r['op']}{labels}")


@revisions_group.command("label")
@click.argument("step", type=int)
@click.argument("name")
@click.option("--project", "project_opt", default=None, help="Project name override.")
def revisions_label(step: int, name: str, project_opt: str | None) -> None:
    """Name a step (e.g. baseline) so the operator can reset-to it."""
    from pydata_core.catalog_state import label_step

    proj = resolve_project(project_opt)
    if proj is None:
        raise click.ClickException("no active project — pass --project")
    try:
        label_step(proj, step, name)
    except RuntimeError as exc:
        raise click.ClickException(str(exc))
    click.echo(f"labelled step-{step:03d} as {name!r}")


@cli.command("run")
@click.option(
    "--project",
    default=None,
    help="Project name (defaults to PYDATA_PROJECT env or 'spike').",
)
@click.option("--port", default=7860, type=int)
@click.option("--host", default="127.0.0.1")
@click.option(
    "--buckaroo/--no-buckaroo",
    default=True,
    help="Manage a Buckaroo subprocess on :8700 for in-table recon.",
)
@click.option("--buckaroo-port", default=8700, type=int)
def run_companion(project: str | None, port: int, host: str, buckaroo: bool, buckaroo_port: int) -> None:
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
        buckaroo_log = project_dir(project_name) / "buckaroo.log"
        bk = BuckarooManager(port=buckaroo_port, log_file=buckaroo_log)
        try:
            bk.start()
            # T-35: include PID + bound port so `ps`/`lsof` disambiguation is
            # trivial when stale buckaroos linger from earlier debugging.
            click.echo(f"  buckaroo · {bk.base_url} pid={bk.proc.pid if bk.proc else '?'} (log: {buckaroo_log})")
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
@click.option(
    "--project",
    default=None,
    help="Project name (defaults to PYDATA_PROJECT env or 'spike').",
)
def run_mcp(project: str | None) -> None:
    """Start the MCP server (this is what Claude Code spawns)."""
    if project:
        os.environ["PYDATA_PROJECT"] = project
    from pydata_mcp import main as mcp_main

    mcp_main()


@cli.command("replay")
@click.argument("storyboard", type=click.Path(exists=True, dir_okay=False))
@click.option("--project", default=None, help="Project name override.")
@click.option(
    "--delay",
    default=0.0,
    type=float,
    help="Seconds to sleep between steps (for stage pacing).",
)
@click.option("--stop-on-error/--continue-on-error", default=True)
def replay_storyboard(storyboard: str, project: str | None, delay: float, stop_on_error: bool) -> None:
    """Replay a storyboard JSON file by calling MCP tools in order.

    The storyboard is a JSON object of shape:

        {
          "project": "spike",
          "steps": [
            {"tool": "catalog_load_parquet", "args": {...}, "narration": "..."},
            ...
          ]
        }

    Each step calls the named tool from pydata_mcp.server with `args` as
    kwargs. Use this for stage rehearsal, fallback recordings, or as a
    deterministic regression test of the full storyboard.
    """
    import importlib
    import json
    import time

    sb = json.loads(Path(storyboard).read_text())
    sb_project = project or sb.get("project")
    if sb_project:
        os.environ["PYDATA_PROJECT"] = sb_project

    mcp_module = importlib.import_module("pydata_mcp.server")
    steps = sb.get("steps", [])
    if not steps:
        click.echo("storyboard has no steps; nothing to do.")
        return

    click.echo(f"replaying {len(steps)} step{'' if len(steps) == 1 else 's'} (project={sb_project!r})")
    failures = 0
    for i, step in enumerate(steps, start=1):
        if step.get("skip"):
            click.echo(f"[{i}/{len(steps)}] (skipped: {step.get('tool')})")
            continue
        tool_name = step.get("tool")
        args = step.get("args", {})
        narration = step.get("narration", "")
        tool_fn = getattr(mcp_module, tool_name, None)
        if not callable(tool_fn):
            click.echo(f"[{i}/{len(steps)}] {tool_name}: NO SUCH TOOL")
            failures += 1
            if stop_on_error:
                raise click.ClickException(f"unknown tool {tool_name!r}")
            continue
        click.echo(f"[{i}/{len(steps)}] {tool_name}({', '.join(f'{k}=…' for k in args)})")
        if narration:
            click.echo(f"    {narration}")
        result = tool_fn(**args)
        if isinstance(result, dict) and "error" in result:
            click.echo(f"    ERROR: {result['error'][:120]}")
            failures += 1
            if stop_on_error:
                raise click.ClickException("step failed (use --continue-on-error to push through)")
        else:
            summary = _summarise(result)
            click.echo(f"    → {summary}")
        if delay > 0 and i < len(steps):
            time.sleep(delay)

    if failures:
        click.echo(f"replay finished with {failures} failure{'' if failures == 1 else 's'}")
    else:
        click.echo("replay complete; no failures.")


def _summarise(result):
    if isinstance(result, dict):
        bits = []
        for key in ("alias", "version", "hash", "row_count", "execute_seconds"):
            if key in result:
                bits.append(f"{key}={result[key]}")
        if bits:
            return " ".join(bits)
    if isinstance(result, list):
        return f"{len(result)} items"
    return str(result)[:120]


@cli.command("pack")
@click.argument("project_name", required=False)
@click.option(
    "--project",
    default=None,
    help="Project name (defaults to PYDATA_PROJECT env or the positional arg).",
)
@click.option(
    "-o",
    "--output",
    default=None,
    help="Output .tgz path. Defaults to ./<project>-<date>.tgz.",
)
@click.option(
    "--exclude-cache/--include-cache",
    default=True,
    help="Skip xorq cache deps (smaller bundle, paths still portable).",
)
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
    click.echo(f"wrote {out_path} ({size / 1024:.1f} KB)")
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
