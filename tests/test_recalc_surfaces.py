"""Stage 3 surfaces for the reactive consumer (#89): MCP tools + companion routes.

The logic lives in ``tallyman_xorq.recalc`` / ``.staleness``; these pin the thin
surfaces — that the scan is read-only, that ``catalog_recalc`` previews by
default, and that a committed recalc lands as exactly ONE revision (recalc
self-checkpoints and both surfaces are exempt from their generic per-op
checkpoint, so neither double-commits nor misses).
"""

from __future__ import annotations

import subprocess

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir, paths
from tallyman_core.aliases import get_alias


def _base_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _base_code_v2(project: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select("region", "price").mutate(extra=1)
"""


def _child_code(parent: str) -> str:
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _steps(project: str) -> set[str]:
    cd = paths.catalog_dir(project)
    out = subprocess.run(["git", "-C", str(cd), "tag", "-l", "step-*"], capture_output=True, text=True).stdout
    return set(out.split())


def _edit_source(project: str) -> None:
    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=250, seed=7)


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


def test_mcp_scan_staleness_lists_drifted_entries(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    from tallyman_mcp.server import catalog_create, catalog_scan_staleness

    a = catalog_create("a", _base_code(project))["hash"]
    b = catalog_create("b", _child_code("a"))["hash"]
    _edit_source(project)

    out = catalog_scan_staleness()
    # a is directly stale on the source axis; b is a cheap child that inlines a's
    # recipe, so a's source leaks into b's manifest.sources — b is directly stale
    # too (see test_recalc on the cheap-chain leak).
    assert a in out["stale"] and b in out["stale"]
    assert out["entries"][a]["stale"] is True
    assert out["entries"][a]["reasons"][0]["axis"] == "source"


def test_mcp_recalc_dry_run_previews_and_checkpoints_nothing(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    from tallyman_mcp.server import catalog_create, catalog_recalc

    a = catalog_create("a", _base_code(project))["hash"]
    catalog_create("b", _child_code("a"))
    _edit_source(project)

    base = _steps(project)
    out = catalog_recalc(dry_run=True)  # roots default to the stale set
    assert out["dry_run"] is True
    assert out["status"] == "ok"
    assert out["cone"][0] == a  # parent first
    assert out["remap"] == {}
    assert _steps(project) == base  # read-only: no revision
    assert get_alias(project, "a") == a  # nothing rebuilt


def test_mcp_recalc_commit_is_exactly_one_revision(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    from tallyman_mcp.server import catalog_create, catalog_recalc

    a = catalog_create("a", _base_code(project))["hash"]
    catalog_create("b", _child_code("a"))
    _edit_source(project)

    base = _steps(project)
    out = catalog_recalc(dry_run=False)
    assert out["status"] == "ok"
    assert len(out["remap"]) == 2  # a and b advanced
    assert get_alias(project, "a") != a
    assert len(_steps(project)) == len(base) + 1  # one revision for the whole walk


def test_mcp_recalc_no_stale_reports_nothing(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    from tallyman_mcp.server import catalog_create, catalog_recalc

    catalog_create("a", _base_code(project))
    base = _steps(project)
    out = catalog_recalc(dry_run=False)
    assert out["status"] == "ok"
    assert out["roots"] == []
    assert _steps(project) == base


def test_mcp_recalc_notify_forwards_remap_and_step(project, orders_parquet, monkeypatch):
    # A committed recalc must hand the companion BOTH the remap and the checkpoint
    # `step`, so the republished SSE event carries the normalized {kind, remap,
    # step} shape the in-process /api/recalc route emits (not just remap).
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    import tallyman_mcp.server as server
    from tallyman_mcp.server import catalog_create, catalog_recalc

    captured: dict = {}
    monkeypatch.setattr(server, "_notify", lambda kind, **kw: captured.update(kind=kind, **kw))

    catalog_create("a", _base_code(project))
    catalog_create("b", _child_code("a"))
    _edit_source(project)

    out = catalog_recalc(dry_run=False)
    assert out["status"] == "ok"
    assert captured["kind"] == "recalc"
    assert "step" in captured["extra"]  # finding 2: step is forwarded, not dropped
    assert captured["extra"]["step"] == out["checkpoint_step"]
    assert captured["extra"]["remap"] == out["remap"]


# ---------------------------------------------------------------------------
# companion routes
# ---------------------------------------------------------------------------


def test_companion_staleness_endpoint(fresh_companion_app, project, orders_parquet, monkeypatch):
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a = catalog_create("a", _base_code(project))["hash"]
    _edit_source(project)

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/staleness")
    assert r.status_code == 200, r.text
    body = r.json()
    assert a in body["stale"]
    assert body["entries"][a]["stale"] is True


def test_companion_recalc_commit_is_one_revision_and_repoints(
    fresh_companion_app, project, orders_parquet, monkeypatch
):
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a = catalog_create("a", _base_code(project))["hash"]
    catalog_create("b", _child_code("a"))
    _edit_source(project)

    c = TestClient(fresh_companion_app)
    base = _steps(project)

    # Preview first: no revision, nothing rebuilt.
    dry = c.post(f"/{project}/api/recalc", json={"dry_run": True})
    assert dry.status_code == 200, dry.text
    assert dry.json()["remap"] == {}
    assert _steps(project) == base

    # Commit: exactly one revision (middleware-exempt + recalc self-checkpoints).
    r = c.post(f"/{project}/api/recalc", json={"dry_run": False})
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "ok"
    assert len(r.json()["remap"]) == 2
    assert get_alias(project, "a") != a
    assert len(_steps(project)) == len(base) + 1


# ---------------------------------------------------------------------------
# Stage B — companion in-browser revise (PUT /api/code) cascades, parity with MCP
# ---------------------------------------------------------------------------


def test_companion_revise_cascades_and_is_one_revision(fresh_companion_app, project, orders_parquet, monkeypatch):
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    a = catalog_create("a", _base_code(project))["hash"]
    b = catalog_create("b", _child_code("a"))["hash"]

    c = TestClient(fresh_companion_app)
    base = _steps(project)

    r = c.put(f"/{project}/api/code/a", json={"code": _base_code_v2(project)})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["hash"] != a
    # The in-browser revise cascaded to the follower, in one revision.
    assert "recalc" in body
    assert get_alias(project, "b") != b
    assert body["recalc"]["remap"] == {b: get_alias(project, "b")}
    assert len(_steps(project)) == len(base) + 1


def test_companion_revise_publishes_recalc_event(project, orders_parquet, monkeypatch):
    from fastapi.testclient import TestClient

    import tallyman_companion.app as appmod
    from tallyman_companion import create_app
    from tallyman_mcp.server import catalog_create

    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    catalog_create("a", _base_code(project))
    b = catalog_create("b", _child_code("a"))["hash"]

    captured: list[tuple] = []
    monkeypatch.setattr(
        appmod,
        "_recalc_sse_event",
        lambda remap, step: captured.append((remap, step)) or {"kind": "recalc", "remap": remap, "step": step},
        raising=False,
    )

    c = TestClient(create_app(project))
    r = c.put(f"/{project}/api/code/a", json={"code": _base_code_v2(project)})
    assert r.status_code == 200, r.text
    # The companion publishes the canonical recalc SSE event with the cascade's remap.
    assert captured, "no recalc event published"
    remap, _step = captured[0]
    assert remap == {b: get_alias(project, "b")}
