"""Stage 3 surfaces for the reactive consumer (#89): MCP tools + companion routes.

The logic lives in ``tallyman_xorq.recalc`` / ``.staleness``; these pin the thin
surfaces — that the scan is read-only, that ``catalog_recalc`` previews by
default, and that a committed recalc lands as exactly ONE revision (recalc
self-checkpoints and both surfaces are exempt from their generic per-op
checkpoint, so neither double-commits nor misses).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import paths
from tallyman_core.aliases import get_alias
from tallyman_xorq.source_import import update_and_depend


def _base_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.select("region", "price", "__row_order")
"""


def _base_code_v2(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.select("region", "price", "__row_order").mutate(extra=1)
"""


def _child_code(parent: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _steps(project: str) -> set[str]:
    cd = paths.catalog_dir(project)
    out = subprocess.run(["git", "-C", str(cd), "tag", "-l", "step-*"], capture_output=True, text=True).stdout
    return set(out.split())


def _advance_source(project: str, path: Path) -> None:
    """Re-import the orders file with different rows: the ``orders_src`` alias advances (ADR-011 D3).

    Editing the file alone is invisible to the catalog — a build reads an alias, never a path.
    """
    write_shoe_orders(path, n_rows=250, seed=7)
    update_and_depend(path, "orders_src", project=project)


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


def test_mcp_scan_staleness_lists_drifted_entries(project, orders_parquet, orders_src):
    from tallyman_mcp.server import catalog_create, catalog_scan_staleness

    a = catalog_create("a", _base_code(project))["hash"]
    b = catalog_create("b", _child_code("a"))["hash"]
    _advance_source(project, orders_parquet)

    out = catalog_scan_staleness()
    # a reads orders_src, which the re-import advanced: a is directly stale. b reads
    # the alias "a", which has not moved, so b is only carried — transitively stale.
    assert a in out["stale"]
    assert b not in out["stale"] and b in out["transitively_stale"]
    assert out["entries"][a]["stale"] is True
    assert out["entries"][a]["reasons"][0]["axis"] == "alias"
    assert out["entries"][a]["reasons"][0]["ref"] == "orders_src"


def test_mcp_scan_staleness_classifies_orphans_when_auto_recalc_on(project, orders_parquet, orders_src, monkeypatch):
    # D5 scan-on-load backstop: bypasses (and re-imports) leave entries stale
    # without a triggering revise, so no auto-recalc ever classifies them. The
    # scan-on-load surface runs the same orphan classification so a project load
    # surfaces them even when no revise has happened since.
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)  # default ON
    from tallyman_mcp.server import catalog_create, catalog_scan_staleness

    a = catalog_create("a", _base_code(project))["hash"]
    catalog_create("b", _child_code("a"))
    _advance_source(project, orders_parquet)  # a directly stale; no revise fired

    out = catalog_scan_staleness()
    assert a in out["stale"]
    orphans = {o["hash"]: o for o in out["orphan_stale"]}
    assert a in orphans
    # No recorded recalc error explains it → UNEXPLAINED, the loud case.
    assert "UNEXPLAINED" in orphans[a]["explanation"]


def test_mcp_scan_staleness_no_orphans_when_auto_recalc_off(project, orders_parquet, orders_src):
    # Manual mode (flag off) owns its own staleness: the explicit scan→recalc
    # workflow expects directly-stale followers as an intermediate state, so the
    # scan surface must NOT flag them as orphan/UNEXPLAINED ("file a bug").
    from tallyman_core.config import set_auto_recalc
    from tallyman_mcp.server import catalog_create, catalog_scan_staleness

    a = catalog_create("a", _base_code(project))["hash"]
    _advance_source(project, orders_parquet)
    set_auto_recalc(project, False)

    out = catalog_scan_staleness()
    assert a in out["stale"]  # still reports the stale set
    assert out["orphan_stale"] == []  # but no orphan classification in manual mode


def test_mcp_recalc_dry_run_previews_and_checkpoints_nothing(project, orders_parquet, orders_src):
    from tallyman_mcp.server import catalog_create, catalog_recalc

    a = catalog_create("a", _base_code(project))["hash"]
    catalog_create("b", _child_code("a"))
    _advance_source(project, orders_parquet)

    base = _steps(project)
    out = catalog_recalc(dry_run=True)  # roots default to the stale set
    assert out["dry_run"] is True
    assert out["status"] == "ok"
    assert out["cone"][0] == a  # parent first
    assert out["remap"] == {}
    assert _steps(project) == base  # read-only: no revision
    assert get_alias(project, "a") == a  # nothing rebuilt


def test_mcp_recalc_commit_is_exactly_one_revision(project, orders_parquet, orders_src):
    from tallyman_mcp.server import catalog_create, catalog_recalc

    a = catalog_create("a", _base_code(project))["hash"]
    catalog_create("b", _child_code("a"))
    _advance_source(project, orders_parquet)

    base = _steps(project)
    out = catalog_recalc(dry_run=False)
    assert out["status"] == "ok"
    assert len(out["remap"]) == 2  # a and b advanced
    assert get_alias(project, "a") != a
    assert len(_steps(project)) == len(base) + 1  # one revision for the whole walk


def test_mcp_recalc_no_stale_reports_nothing(project, orders_src):
    from tallyman_mcp.server import catalog_create, catalog_recalc

    catalog_create("a", _base_code(project))
    base = _steps(project)
    out = catalog_recalc(dry_run=False)
    assert out["status"] == "ok"
    assert out["roots"] == []
    assert _steps(project) == base


def test_mcp_recalc_notify_forwards_remap_and_step(project, orders_parquet, orders_src, monkeypatch):
    # A committed recalc must hand the companion BOTH the remap and the checkpoint
    # `step`, so the republished SSE event carries the normalized {kind, remap,
    # step} shape the in-process /api/recalc route emits (not just remap).
    import tallyman_mcp.server as server
    from tallyman_mcp.server import catalog_create, catalog_recalc

    captured: dict = {}
    monkeypatch.setattr(server, "_notify", lambda kind, **kw: captured.update(kind=kind, **kw))

    catalog_create("a", _base_code(project))
    catalog_create("b", _child_code("a"))
    _advance_source(project, orders_parquet)

    out = catalog_recalc(dry_run=False)
    assert out["status"] == "ok"
    assert captured["kind"] == "recalc"
    # Flat kwargs, NOT nested under `extra` — `_notify(kind, **extra)` would wrap a
    # literal `extra={...}` as extra.extra, and the companion handler reads
    # payload.extra.remap, so the nested form silently dropped remap on the wire.
    assert captured["remap"] == out["remap"]
    assert captured["step"] == out["checkpoint_step"]


def test_recalc_notify_wire_delivers_remap_to_the_real_handler(project, orders_parquet, orders_src, monkeypatch):
    # End-to-end across the seam the mocked tests miss: the exact JSON
    # tallyman_mcp._notify puts on the wire, fed to the REAL /internal/notify
    # handler, must deliver remap + step to the republished SSE event. Previously
    # the producer half (mocked _notify) and consumer half (hand-built payload)
    # were tested against different shapes, hiding a double-wrap that dropped remap.
    from fastapi.testclient import TestClient

    import tallyman_companion.app as appmod
    import tallyman_mcp.server as server
    from tallyman_companion import create_app
    from tallyman_mcp.server import catalog_create, catalog_recalc


    sent: dict = {}

    class _CaptureClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def post(self, _url, json=None):
            sent.clear()
            sent.update(json or {})

    monkeypatch.setattr(server.httpx, "Client", _CaptureClient)

    catalog_create("a", _base_code(project))
    catalog_create("b", _child_code("a"))
    _advance_source(project, orders_parquet)
    out = catalog_recalc(dry_run=False)  # the real call site fires _notify("recalc", ...)
    assert out["remap"] and sent.get("kind") == "recalc"

    captured: list = []
    monkeypatch.setattr(
        appmod,
        "_recalc_sse_event",
        lambda remap, step: captured.append((remap, step)) or {"kind": "recalc", "remap": remap, "step": step},
        raising=False,
    )
    c = TestClient(create_app(project))
    r = c.post("/internal/notify", json=sent)  # feed the EXACT wire payload to the real handler
    assert r.status_code == 200, r.text
    assert captured, "handler dropped the recalc payload"
    remap, step = captured[0]
    assert remap == out["remap"]  # remap survived the wire (not double-wrapped away)
    assert step == out["checkpoint_step"]


# ---------------------------------------------------------------------------
# companion routes
# ---------------------------------------------------------------------------


def test_companion_staleness_endpoint(fresh_companion_app, project, orders_parquet, orders_src):
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create

    a = catalog_create("a", _base_code(project))["hash"]
    _advance_source(project, orders_parquet)

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/staleness")
    assert r.status_code == 200, r.text
    body = r.json()
    assert a in body["stale"]
    assert body["entries"][a]["stale"] is True


def test_companion_staleness_classifies_orphans(
    fresh_companion_app, project, orders_parquet, orders_src, monkeypatch
):
    # Parity with the MCP scan-on-load surface: /api/staleness returns the same
    # classified orphan_stale so the SPA can surface unexplained staleness on load.
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create

    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)  # default ON
    a = catalog_create("a", _base_code(project))["hash"]
    _advance_source(project, orders_parquet)

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/staleness")
    assert r.status_code == 200, r.text
    body = r.json()
    assert a in body["stale"]
    orphans = {o["hash"]: o for o in body["orphan_stale"]}
    assert a in orphans
    assert "UNEXPLAINED" in orphans[a]["explanation"]


def test_companion_recalc_commit_is_one_revision_and_repoints(
    fresh_companion_app, project, orders_parquet, orders_src
):
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create

    a = catalog_create("a", _base_code(project))["hash"]
    catalog_create("b", _child_code("a"))
    _advance_source(project, orders_parquet)

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


def test_companion_revise_cascades_and_is_one_revision(fresh_companion_app, project, orders_src, monkeypatch):
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create

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


def test_companion_revise_publishes_recalc_event(project, orders_src, monkeypatch):
    from fastapi.testclient import TestClient

    import tallyman_companion.app as appmod
    from tallyman_companion import create_app
    from tallyman_mcp.server import catalog_create

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
    remap, step = captured[0]
    assert remap == {b: get_alias(project, "b")}
    # checkpoint_step is None by design on the companion auto path: the middleware
    # commits the revision AFTER the handler returns, and (unlike the MCP decorator)
    # the response is already serialized, so there's nothing to backfill into.
    assert step is None


def test_companion_promote_diff_is_one_revision(fresh_companion_app, project, orders_src, monkeypatch):
    # Finding #1: the companion POST /api/promote_diff route self-checkpoints
    # (app.py) AND was not in _checkpoint_exempt, so the dispatch middleware
    # checkpointed it a second time — a double-commit (trailing empty revision),
    # the exact bug already fixed on the MCP side. The promote must land in exactly
    # ONE revision, mirroring catalog_promote_diff.
    from fastapi.testclient import TestClient

    from tallyman_mcp.server import catalog_create, catalog_revise

    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    catalog_create("ss", _base_code(project))  # v1
    catalog_revise("ss", _base_code_v2(project))  # v2 → a diff exists between v1 and v2

    c = TestClient(fresh_companion_app)
    base = _steps(project)
    r = c.post(f"/{project}/api/promote_diff/ss/1/2")  # target alias is auto-derived
    assert r.status_code == 200, r.text
    assert len(_steps(project)) == len(base) + 1
