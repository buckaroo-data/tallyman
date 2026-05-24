from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pydata_core import notebook
from pydata_core.notebook import CellNotFound
from pydata_mcp.server import (
    catalog_alias,
    catalog_create,
    catalog_rename,
    catalog_revise,
    catalog_run,
    catalog_unalias,
    notebook_edit_markdown,
    notebook_remove,
    notebook_reorder,
)


def _agg_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def _filter_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(n=filtered.count())
"""


# ---------------------------------------------------------------------------
# pydata_core.notebook storage
# ---------------------------------------------------------------------------

def test_load_empty(project: str):
    assert notebook.load(project) == {"cells": []}


def test_append_creates_cell(project: str):
    cell = notebook.append(project, "shoe_sales", markdown="initial note")
    assert cell["alias"] == "shoe_sales"
    assert cell["markdown"] == "initial note"
    assert len(cell["cell_id"]) == 12


def test_append_idempotent_on_alias(project: str):
    a = notebook.append(project, "x", markdown="first")
    b = notebook.append(project, "x", markdown="ignored second")
    assert a["cell_id"] == b["cell_id"]
    assert b["markdown"] == "first"  # markdown not overwritten


def test_reorder(project: str):
    a = notebook.append(project, "a")
    notebook.append(project, "b")
    notebook.append(project, "c")
    notebook.reorder(project, a["cell_id"], 2)
    assert [x["alias"] for x in notebook.load(project)["cells"]] == ["b", "c", "a"]


def test_reorder_clamps(project: str):
    a = notebook.append(project, "a")
    notebook.append(project, "b")
    notebook.reorder(project, a["cell_id"], 99)  # beyond end
    assert [x["alias"] for x in notebook.load(project)["cells"]] == ["b", "a"]


def test_reorder_missing(project: str):
    with pytest.raises(CellNotFound):
        notebook.reorder(project, "missing", 0)


def test_remove(project: str):
    cell = notebook.append(project, "a")
    notebook.append(project, "b")
    notebook.remove(project, cell["cell_id"])
    assert [c["alias"] for c in notebook.load(project)["cells"]] == ["b"]


def test_edit_markdown(project: str):
    cell = notebook.append(project, "x", markdown="hello")
    notebook.edit_markdown(project, cell["cell_id"], "goodbye")
    assert notebook.find_cell_by_alias(project, "x")["markdown"] == "goodbye"


def test_rename_alias_in_cells(project: str):
    notebook.append(project, "old")
    notebook.append(project, "other")
    n = notebook.rename_alias_in_cells(project, "old", "new")
    assert n == 1
    assert notebook.find_cell_by_alias(project, "new") is not None
    assert notebook.find_cell_by_alias(project, "old") is None


def test_remove_alias_from_cells(project: str):
    notebook.append(project, "drop_me")
    notebook.append(project, "keep_me")
    n = notebook.remove_alias_from_cells(project, "drop_me")
    assert n == 1
    aliases = [c["alias"] for c in notebook.load(project)["cells"]]
    assert "drop_me" not in aliases
    assert "keep_me" in aliases


# ---------------------------------------------------------------------------
# MCP tools: catalog_create/revise/alias/rename interact with notebook
# ---------------------------------------------------------------------------

def test_catalog_create_appends_cell(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project), prompt="seed text")
    cells = notebook.load(project)["cells"]
    assert len(cells) == 1
    assert cells[0]["alias"] == "shoe_sales"
    assert cells[0]["markdown"] == "seed text"


def test_catalog_revise_does_not_duplicate_cell(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project), prompt="seed")
    catalog_revise("shoe_sales", _filter_code(project), prompt="revised")
    cells = notebook.load(project)["cells"]
    assert len(cells) == 1
    # Markdown is NOT auto-overwritten on revise (plan Open Q1 lean).
    assert cells[0]["markdown"] == "seed"


def test_catalog_alias_appends_cell(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    scratch = catalog_run(_agg_code(project), prompt="exploratory")
    catalog_alias(scratch["hash"], "shoe_sales")
    cells = notebook.load(project)["cells"]
    assert len(cells) == 1
    assert cells[0]["alias"] == "shoe_sales"


def test_catalog_rename_renames_cell_in_place(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("old", _agg_code(project))
    catalog_rename("old", "new")
    cells = notebook.load(project)["cells"]
    assert len(cells) == 1
    assert cells[0]["alias"] == "new"


def test_catalog_unalias_drops_cell(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("dropme", _agg_code(project))
    out = catalog_unalias("dropme")
    assert out["notebook_cells_removed"] == 1
    assert notebook.load(project) == {"cells": []}


def test_notebook_reorder_tool(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("a", _agg_code(project))
    catalog_create("b", _filter_code(project))
    cells = notebook.load(project)["cells"]
    notebook_reorder(cells[0]["cell_id"], 1)
    after = [c["alias"] for c in notebook.load(project)["cells"]]
    assert after == ["b", "a"]


def test_notebook_remove_tool(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("a", _agg_code(project))
    cells = notebook.load(project)["cells"]
    notebook_remove(cells[0]["cell_id"])
    assert notebook.load(project) == {"cells": []}


def test_notebook_edit_markdown_tool(project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("a", _agg_code(project), prompt="original")
    cell = notebook.load(project)["cells"][0]
    notebook_edit_markdown(cell["cell_id"], "agent-written narrative")
    assert notebook.find_cell_by_alias(project, "a")["markdown"] == "agent-written narrative"


def test_notebook_tool_errors_on_missing(project: str, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    for fn in (notebook_remove, notebook_edit_markdown):
        if fn is notebook_edit_markdown:
            out = fn("missing", "x")
        else:
            out = fn("missing")
        assert "error" in out


# ---------------------------------------------------------------------------
# Companion routes
# ---------------------------------------------------------------------------

def test_notebook_route_empty(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.get("/notebook")
    assert r.status_code == 200
    assert "no named entries yet" in r.text


def test_notebook_route_renders_cells(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("shoe_sales", _agg_code(project), prompt="region totals")
    c = TestClient(fresh_companion_app)
    r = c.get("/notebook")
    assert r.status_code == 200
    assert "shoe_sales" in r.text
    assert "region totals" in r.text
    assert "data-table" in r.text  # the parquet preview is in the page


def test_api_notebook_reorder(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("a", _agg_code(project))
    catalog_create("b", _filter_code(project))
    cells = notebook.load(project)["cells"]
    c = TestClient(fresh_companion_app)
    r = c.patch(
        "/api/notebook",
        json={"action": "reorder", "cell_id": cells[0]["cell_id"], "new_index": 1},
    )
    assert r.status_code == 200
    assert [x["alias"] for x in notebook.load(project)["cells"]] == ["b", "a"]


def test_api_notebook_remove(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("a", _agg_code(project))
    cells = notebook.load(project)["cells"]
    c = TestClient(fresh_companion_app)
    r = c.patch(
        "/api/notebook",
        json={"action": "remove", "cell_id": cells[0]["cell_id"]},
    )
    assert r.status_code == 200
    assert notebook.load(project) == {"cells": []}


def test_api_markdown_put(fresh_companion_app, project: str, orders_parquet: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    catalog_create("a", _agg_code(project), prompt="seed")
    cell = notebook.load(project)["cells"][0]
    c = TestClient(fresh_companion_app)
    r = c.put(f"/api/markdown/{cell['cell_id']}", json={"markdown": "edited"})
    assert r.status_code == 200
    assert notebook.find_cell_by_alias(project, "a")["markdown"] == "edited"


def test_serve_mode_rejects_notebook_mutations(project: str):
    from pydata_companion import create_app

    app = create_app(project, read_only=True)
    c = TestClient(app)
    assert c.patch("/api/notebook", json={"action": "remove", "cell_id": "x"}).status_code == 403
    assert c.put("/api/markdown/x", json={"markdown": "y"}).status_code == 403


def test_api_notebook_unknown_action(fresh_companion_app):
    c = TestClient(fresh_companion_app)
    r = c.patch("/api/notebook", json={"action": "purge", "cell_id": "x"})
    assert r.status_code == 400


def test_notebook_renders_markdown_as_html(
    fresh_companion_app, project: str, orders_parquet, monkeypatch
):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create as _cc
    from pydata_mcp.server import notebook_edit_markdown as _em
    _cc("shoe_sales", _agg_code(project))
    cell = notebook.load(project)["cells"][0]
    _em(cell["cell_id"], "## Title\n\nSome **bold** text.\n\n- one\n- two\n")
    c = TestClient(fresh_companion_app)
    r = c.get("/notebook")
    assert r.status_code == 200
    # Rendered html markers, NOT the raw markdown characters.
    assert "<h2>Title</h2>" in r.text
    assert "<strong>bold</strong>" in r.text
    assert "<li>one</li>" in r.text
    # And the raw value is preserved on the data-attr for the edit toggle.
    assert "data-raw=\"## Title" in r.text


def test_notebook_empty_markdown_shows_placeholder_in_edit_mode(
    fresh_companion_app, project: str, orders_parquet, monkeypatch
):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create as _cc
    _cc("shoe_sales", _agg_code(project), prompt="")  # empty markdown
    c = TestClient(fresh_companion_app)
    r = c.get("/notebook")
    assert "(no markdown — click edit to add a note)" in r.text


def test_put_markdown_returns_rendered_html(
    fresh_companion_app, project: str, orders_parquet, monkeypatch
):
    """T-10: in-place updates need the server to send back rendered HTML
    so the client can swap it in without a reload."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create as _cc
    _cc("shoe_sales", _agg_code(project), prompt="seed")
    cell = notebook.load(project)["cells"][0]
    c = TestClient(fresh_companion_app)
    r = c.put(f"/api/markdown/{cell['cell_id']}", json={"markdown": "## Hello\n\nworld"})
    assert r.status_code == 200
    body = r.json()
    assert "<h2>Hello</h2>" in body["html"]
    assert body["markdown"] == "## Hello\n\nworld"


def test_notebook_page_includes_sortable_js(
    fresh_companion_app, project: str, orders_parquet, monkeypatch
):
    """T-09: drag-reorder requires SortableJS available."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create as _cc
    _cc("shoe_sales", _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get("/notebook")
    assert "Sortable.min.js" in r.text
    assert 'class="nb-drag"' in r.text


def test_serve_mode_omits_sortable_js(project: str, orders_parquet, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_companion import create_app
    from pydata_mcp.server import catalog_create as _cc
    _cc("shoe_sales", _agg_code(project))
    app = create_app(project, read_only=True)
    c = TestClient(app)
    r = c.get("/notebook")
    assert "Sortable.min.js" not in r.text
    assert 'class="nb-drag"' not in r.text


def test_notebook_uses_buckaroo_embed_when_session_available(
    project: str, orders_parquet, monkeypatch
):
    """Notebook cells render the Buckaroo React embed (BuckarooServerView)
    when the manager is plumbed in. Falls back to pandas.to_html otherwise."""
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_companion import create_app
    from pydata_mcp.server import catalog_create as _cc

    _cc("shoe_sales", _agg_code(project))

    class _StubBuckaroo:
        is_running = True
        bound_port = 8700

        @property
        def base_url(self):
            return "http://127.0.0.1:8700"

        @property
        def ws_base_url(self):
            return "ws://127.0.0.1:8700"

        def ensure_session(self, content_hash):
            return f"sess-{content_hash[:6]}"

    app = create_app(project, buckaroo=_StubBuckaroo())  # type: ignore[arg-type]
    c = TestClient(app)
    r = c.get("/notebook")
    assert 'class="buckaroo-embed nb-buckaroo"' in r.text
    assert 'data-ws-url="ws://127.0.0.1:8700/ws/sess-' in r.text
    # The HTML preview fallback should NOT render when an embed is present.
    assert '<div class="nb-preview">' not in r.text


def test_notebook_falls_back_to_html_when_no_buckaroo(
    fresh_companion_app, project: str, orders_parquet, monkeypatch
):
    monkeypatch.setenv("PYDATA_PROJECT", project)
    from pydata_mcp.server import catalog_create as _cc
    _cc("shoe_sales", _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get("/notebook")
    # No Buckaroo manager → HTML preview, no embed.
    assert 'class="buckaroo-embed nb-buckaroo"' not in r.text
    assert '<div class="nb-preview">' in r.text
