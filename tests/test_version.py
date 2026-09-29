from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_core.version import git_revision, version_info
from tallyman_xorq import list_entries

# tallyman has no releases yet, so the version is the git revision. Each
# long-lived component stamps the source it runs from so drift is visible
# (the stale-bundle class of bug behind #132): the companion reports its
# revision at GET /api/version and on an X-Tallyman-Revision response header.


def test_git_revision_is_nonempty_token():
    rev = git_revision()
    assert rev and " " not in rev  # a sha (optionally -dirty), or "unknown"


def test_version_info_shape():
    info = version_info()
    assert set(info) >= {"revision", "dirty"}
    assert isinstance(info["dirty"], bool)
    assert info["revision"] == git_revision()


def test_api_version_reports_companion_revision(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(create_app(project))
    r = c.get("/api/version")
    assert r.status_code == 200
    body = r.json()
    assert body["component"] == "companion"
    assert body["revision"] == git_revision()
    assert "dirty" in body


def test_revision_header_rides_every_response(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(create_app(project))
    # The stamp is on all responses, not just /api/version — so any request the
    # SPA makes carries the backend's revision for the drift check.
    r = c.get("/api/projects")
    assert r.headers.get("x-tallyman-revision") == git_revision()


# ---------------------------------------------------------------------------
# every process on a data dir runs the same source
# ---------------------------------------------------------------------------
# The MCP server (one per Claude Code session), the companion (`tallyman run`, one per data dir) and the CLI are
# started independently, so one of them can be running code from before a pull, a checkout or an edit while the others
# run the new code, all against the same catalogs. The companion writes its revision into the owner record of the data
# dir it claims (server_lock), and a client of that data dir refuses to work against a companion on another revision.


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def test_uncommitted_edits_change_the_revision(tmp_path):
    """``git describe --dirty`` names every uncommitted state the same (``<sha>-dirty``), so an MCP server started
    before an edit and a companion started after it would agree while running different code. Two different sets of
    edits on one commit have to be two revisions."""
    from tallyman_core.version import checkout_revision

    repo = tmp_path / "checkout"
    repo.mkdir()
    _git("init", "-q", cwd=repo)
    _git("config", "user.email", "t@example.com", cwd=repo)
    _git("config", "user.name", "tester", cwd=repo)
    (repo / "mod.py").write_text("x = 1\n")
    _git("add", "mod.py", cwd=repo)
    _git("commit", "-q", "-m", "one", cwd=repo)
    sha = _git("rev-parse", "--short=7", "HEAD", cwd=repo)

    assert checkout_revision(repo) == sha

    (repo / "mod.py").write_text("x = 2\n")
    first_edit = checkout_revision(repo)
    (repo / "mod.py").write_text("x = 3\n")
    second_edit = checkout_revision(repo)

    assert first_edit.startswith(f"{sha}-dirty.")
    assert second_edit.startswith(f"{sha}-dirty.")
    assert first_edit != second_edit
    assert checkout_revision(repo) == second_edit  # the same edits are the same revision

    (repo / "mod.py").write_text("x = 1\n")
    assert checkout_revision(repo) == sha


def test_the_server_record_names_the_revision_it_runs(running_server):
    from tallyman_core.server_lock import read_owner

    assert running_server["revision"] == git_revision()
    assert read_owner()["revision"] == git_revision()


def _set_companion_revision(home: Path, revision: str | None) -> None:
    """Rewrite the owner record of the server holding *home* as a companion on *revision* would have written it
    (``None``: a companion from before records carried one). The claim itself is untouched: ``flock`` locks the open
    file, not its contents."""
    lock = home / "server.lock"
    record = json.loads(lock.read_text())
    if revision is None:
        record.pop("revision", None)
    else:
        record["revision"] = revision
    lock.write_text(json.dumps(record))


def test_mcp_tools_refuse_to_run_against_a_companion_on_another_revision(
    project, orders_src, isolated_home, running_server
):
    from fastmcp.exceptions import ToolError

    import tallyman_mcp.server as srv

    entries_before = list_entries(project)
    _set_companion_revision(isolated_home, "0000000")

    with pytest.raises(ToolError) as refused:
        srv.catalog_list()
    message = str(refused.value)
    assert "0000000" in message and git_revision() in message  # both sides named
    assert "/mcp" in message and "restart-tallyman" in message  # and how to restart each

    # A refused build builds nothing.
    recipe = (
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        f"t = tracked_expr_from_alias({orders_src!r})\n"
        'expr = t.group_by("region").aggregate(n=t.count())\n'
    )
    with pytest.raises(ToolError):
        srv.catalog_run(recipe, prompt="must not build")
    assert list_entries(project) == entries_before


def test_mcp_tools_refuse_a_companion_that_records_no_revision(project, isolated_home, running_server):
    """A companion started from source older than this check writes no revision. It is not known to match."""
    from fastmcp.exceptions import ToolError

    import tallyman_mcp.server as srv

    _set_companion_revision(isolated_home, None)

    with pytest.raises(ToolError, match="no revision"):
        srv.catalog_list()


def test_companion_refuses_a_notify_from_another_revision(project, isolated_home):
    """The check the other way round: an MCP server or CLI older than this check has no check of its own, and its
    notify is the only thing the companion ever sees from it."""
    c = TestClient(create_app(project))

    other = c.post("/internal/notify", json={"kind": "new_entry", "revision": "0000000"})
    assert other.status_code == 409, other.text
    assert "0000000" in other.json()["detail"] and git_revision() in other.json()["detail"]

    none = c.post("/internal/notify", json={"kind": "new_entry"})
    assert none.status_code == 409, none.text

    same = c.post("/internal/notify", json={"kind": "new_entry", "revision": git_revision()})
    assert same.status_code == 200, same.text
