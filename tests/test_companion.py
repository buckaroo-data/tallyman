from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tallyman_core import entry_dir
from tallyman_xorq import build_and_persist


def _build_one(project: str, parquet: Path) -> str:
    code = f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    return build_and_persist(project, code, prompt="by region").content_hash


def test_root_redirects(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.get("/", follow_redirects=False)
    assert r.status_code in (302, 307)
    assert r.headers["location"].endswith(f"/{project}/catalog")


def test_catalog_empty(fresh_companion_app, project: str):
    # UI routes now serve the React SPA; data checks go through the API.
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    assert r.json()["entries"] == []


def test_unknown_project_404s(fresh_companion_app, project: str):
    """Project validation happens at the API layer; UI routes serve the SPA."""
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/entries").status_code == 200  # valid project
    assert c.get("/nope/api/entries").status_code == 404  # unknown project
    assert c.get("/Bad-NAME/api/entries").status_code == 404  # invalid name


def test_api_entries_empty(fresh_companion_app, project: str):
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    assert r.json() == {"project": project, "entries": []}


def test_catalog_renders_after_build(fresh_companion_app, project: str, orders_parquet: Path):
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    entries = r.json()["entries"]
    hashes = [e["content_hash"] for e in entries]
    assert h in hashes
    prompts = [e["prompt"] for e in entries]
    assert "by region" in prompts


def test_entry_detail_renders_table(fresh_companion_app, project: str, orders_parquet: Path):
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{h}")
    assert r.status_code == 200
    body = r.json()
    assert body["content_hash"] == h
    col_names = [f["name"] for f in body["schema"]["fields"]]
    assert "region" in col_names

    # Data endpoint still serves rows.
    rd = c.get(f"/{project}/api/data/{h}")
    assert rd.status_code == 200
    assert rd.json()["total"] > 0


def test_api_data_cheap_entry_serves_page_without_materialising(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    """#90: a paginated read comes off cached_result_expr, not a full result.parquet.

    A cheap entry (parquet read + projection) materialises nothing at build (#74).
    Serving one page must not reverse that by dumping the entry's whole result to
    disk — the old path called ensure_result, which wrote result.parquet to serve
    an O(limit) request.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""
    h = build_and_persist(project, code, prompt="cols").content_hash
    rp = entry_dir(project, h) / "result.parquet"
    assert not rp.exists()  # #74: cheap entry bakes no per-entry result

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{h}?offset=0&limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 200  # from the manifest's row_count, not a parquet
    assert len(body["data"]) == 10
    assert set(body["data"][0]) == {"region", "price"}

    assert not rp.exists()  # #90: serving a page wrote no per-entry result.parquet


def test_api_data_expensive_entry_paginates_without_per_entry_parquet(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    """#90: pages off an expensive entry read the baked snapshot, not a duplicate.

    An expensive entry (Sort) bakes its result into the result_cache snapshot but
    writes no per-entry result.parquet. Paginating must read that snapshot through
    the expression, not materialise a second on-disk copy under the entry.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.order_by("order_id").select("order_id", "region", "price")
"""
    h = build_and_persist(project, code, prompt="ordered").content_hash
    rp = entry_dir(project, h) / "result.parquet"
    assert not rp.exists()  # #73/#74: baked in the snapshot, not under the entry

    c = TestClient(fresh_companion_app)
    page1 = c.get(f"/{project}/api/data/{h}?offset=0&limit=50").json()
    page2 = c.get(f"/{project}/api/data/{h}?offset=50&limit=50").json()
    assert page1["total"] == page2["total"] == 200
    ids1 = [row["order_id"] for row in page1["data"]]
    ids2 = [row["order_id"] for row in page2["data"]]
    assert ids1 == list(range(1, 51))  # ordered, contiguous, deep-offset correct
    assert ids2 == list(range(51, 101))

    assert not rp.exists()  # #90: no duplicate per-entry parquet written on read


def test_api_data_cheap_entry_paginates_consistently_across_pages(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    """#90: a cheap (unsorted) entry tiles consistently across pages.

    Each page of a cheap entry is an independent re-execution of the recipe — no
    baked snapshot is read (only Aggregate/Join/Sort entries bake one). So two
    pages tile the result with no overlap and no gap only if the source scan
    order is stable across executions. The old path sliced a single on-disk
    result.parquet, stable by construction; serving off the expression relies on
    the read being stable. Assert two pages equal the matching slices of a single
    full read to lock that contract in.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("order_id", "region", "price")
"""
    h = build_and_persist(project, code, prompt="cheap").content_hash
    assert not (entry_dir(project, h) / "result.parquet").exists()  # cheap: no snapshot

    c = TestClient(fresh_companion_app)
    full = [row["order_id"] for row in c.get(f"/{project}/api/data/{h}?offset=0&limit=200").json()["data"]]
    page1 = [row["order_id"] for row in c.get(f"/{project}/api/data/{h}?offset=0&limit=50").json()["data"]]
    page2 = [row["order_id"] for row in c.get(f"/{project}/api/data/{h}?offset=50&limit=50").json()["data"]]

    assert len(full) == 200
    # Separate re-executions agree on row order: each page is the matching slice
    # of one full read, so the pages tile it with no overlap and no gap.
    assert page1 == full[:50]
    assert page2 == full[50:100]


def test_api_data_missing_manifest_serves_page_without_500(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    """#90: a missing manifest must not 500 the row read.

    The build dir is written before manifest.json (build.py creates the dir, then
    writes the manifest after executing), so a half-built or pruned entry can pass
    the build-dir check yet have no manifest. cached_result_expr needs none — only
    ``total`` reads it — so the read must be guarded: serve the page with a
    best-effort total rather than raising FileNotFoundError on the manifest read.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""
    h = build_and_persist(project, code, prompt="cols").content_hash
    (entry_dir(project, h) / "manifest.json").unlink()  # half-built / pruned entry

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/data/{h}?offset=0&limit=10")
    assert r.status_code == 200  # served off the expression, not the manifest
    body = r.json()
    assert len(body["data"]) == 10
    assert set(body["data"][0]) == {"region", "price"}
    assert body["total"] == 0  # no manifest → best-effort total, not a crash


def test_entry_detail_sidebar_lists_all_entries_with_current_highlighted(
    fresh_companion_app, project: str, orders_parquet: Path, monkeypatch
):
    """Entries API returns named/forensic/scratch entries with correct flags."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_mcp.server import catalog_create, catalog_run

    catalog_create(
        "shoe_sales",
        f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
""",
    )
    scratch = catalog_run(
        f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.order_by("region")
""",
        prompt="exploratory",
    )

    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entries")
    assert r.status_code == 200
    entries = r.json()["entries"]
    named = [e for e in entries if e["alias"] and e["is_current"]]
    scratches = [e for e in entries if not e["alias"]]
    assert any(e["alias"] == "shoe_sales" for e in named)
    assert any(e["content_hash"] == scratch["hash"] for e in scratches)


def test_entry_detail_shows_build_artifacts(fresh_companion_app, project: str, orders_parquet: Path):
    """Build artifacts (expr.yaml etc) appear in the entry JSON."""
    h = _build_one(project, orders_parquet)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{h}")
    assert r.status_code == 200
    body = r.json()
    artifact_names = [a["name"] for a in body["build_artifacts"]]
    assert any("yaml" in n for n in artifact_names)
    # Portable build references ${TALLYMAN_PROJECT_ROOT}, not absolute paths.
    artifact_texts = " ".join(a["text"] for a in body["build_artifacts"])
    assert "${TALLYMAN_PROJECT_ROOT}" in artifact_texts


def test_entry_detail_missing_returns_404(fresh_companion_app, project: str):
    """Unknown hashes return 404 from the API (React app handles the UI gracefully)."""
    c = TestClient(fresh_companion_app)
    assert c.get(f"/{project}/api/entry/deadbeef").status_code == 404


def test_read_endpoints_reject_malformed_content_hash(fresh_companion_app, project: str):
    """Non-hex content_hash params 400, not a misleading 404 (or 500).

    A content hash names an entry dir, so it's lowercase hex — the same guard the
    cache-delete route already applies. The read endpoints let junk fall through
    to a "not found" 404, and a "." / ".." segment could even 500 by resolving
    onto a real dir with no manifest. ``deadbeef`` (valid hex but absent) still
    404s, and aliases still resolve on the routes that accept them (test_aliases
    covers that) — only the charset is rejected here.
    """
    c = TestClient(fresh_companion_app)
    for path in [
        f"/{project}/api/data/NOPE",
        f"/{project}/api/entry/Deadbeef",  # uppercase: not lowercase hex
    ]:
        assert c.get(path).status_code == 400, path


def test_internal_notify_returns_subscriber_count(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.post("/internal/notify", json={"kind": "new_entry", "hash": "abc"})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["subscribers"] == 0  # no SSE clients connected in this test
