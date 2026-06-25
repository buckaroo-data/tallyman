from __future__ import annotations

import httpx
from fastapi.testclient import TestClient

from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_mcp.server import catalog_create
from tallyman_xorq.build import list_entries

# #133: the catalog detail page loads the Buckaroo grid lazily via
# GET /api/session/{hash}, which returns a typed status so the SPA can show a
# spinner, a precise error, and a retry instead of a silent "not available"
# fallback. load_session classifies the failure; ensure_session stays a thin
# wrapper (session_id | None) for the paginated-read / diff callers. Rendering
# is the SPA's job; these pin the JSON/contract.


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


class _AliveProc:
    def poll(self):
        return None  # looks alive → BuckarooManager.is_running is True


class _RaisingClient:
    def __init__(self, exc):
        self._exc = exc

    def post(self, *a, **k):
        raise self._exc


def test_session_endpoint_unavailable_without_buckaroo(fresh_companion_app, project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    c = TestClient(fresh_companion_app)  # built with no Buckaroo subprocess
    r = c.get(f"/{project}/api/session/{h}")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "unavailable"
    assert body["ws_url"] is None
    assert "Buckaroo" in body["detail"]


def test_entry_detail_does_not_carry_a_session(fresh_companion_app, project, orders_parquet, monkeypatch):
    # The detail request stays metadata-only — the grid loads lazily — so it
    # never blocks on the snapshot bake + /load_expr POST (the #133 hang).
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]
    c = TestClient(fresh_companion_app)
    body = c.get(f"/{project}/api/entry/{h}").json()
    assert body["buckaroo_session"] is None


def test_ensure_session_wraps_load_session(monkeypatch):
    bk = BuckarooManager()
    monkeypatch.setattr(bk, "load_session", lambda *a, **k: {"status": "ok", "session_id": "sess-1", "detail": ""})
    assert bk.ensure_session("abc", "proj") == "sess-1"
    monkeypatch.setattr(bk, "load_session", lambda *a, **k: {"status": "error", "session_id": None, "detail": "x"})
    assert bk.ensure_session("abc", "proj") is None


def test_load_session_no_build_for_unknown_entry(project, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    bk = BuckarooManager()
    bk.proc = _AliveProc()
    bk.bound_port = 8799
    monkeypatch.setattr(bk, "_maybe_restart", lambda: None)
    res = bk.load_session("deadbeef", project)  # no entry dir → no xorq build
    assert res["status"] == "no_build"
    assert res["session_id"] is None


def test_load_session_classifies_timeout_then_error(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project))
    h = list_entries(project)[0]["content_hash"]

    bk = BuckarooManager()
    bk.proc = _AliveProc()
    bk.bound_port = 8799
    monkeypatch.setattr(bk, "_maybe_restart", lambda: None)

    bk._client = _RaisingClient(httpx.TimeoutException("timed out"))
    r = bk.load_session(h, project)
    assert r["status"] == "timeout" and r["session_id"] is None and r["detail"]

    # A timed-out load caches nothing, so the next call re-POSTs and classifies
    # the non-timeout error distinctly.
    bk._client = _RaisingClient(httpx.HTTPError("boom"))
    r = bk.load_session(h, project)
    assert r["status"] == "error" and r["session_id"] is None
