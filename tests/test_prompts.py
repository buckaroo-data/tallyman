from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_xorq import build_and_persist, read_prompts


def _code(src: str) -> str:
    # The orders data entered the catalog as a source alias, and a recipe reads the alias (ADR-011 D2).
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_first_build_records_one_prompt(project: str, orders_src: str):
    res = build_and_persist(project, _code(orders_src), prompt="first")
    history = read_prompts(project, res.content_hash)
    assert len(history) == 1
    assert history[0]["prompt"] == "first"


def test_re_run_appends_prompt(project: str, orders_src: str):
    a = build_and_persist(project, _code(orders_src), prompt="first")
    build_and_persist(project, _code(orders_src), prompt="second")
    history = read_prompts(project, a.content_hash)
    assert [p["prompt"] for p in history] == ["first", "second"]


def test_manifest_prompt_remains_first(project: str, orders_src: str):
    import json

    a = build_and_persist(project, _code(orders_src), prompt="first")
    build_and_persist(project, _code(orders_src), prompt="second")
    manifest = json.loads((a.entry_path / "manifest.json").read_text())
    assert manifest["prompt"] == "first"


def test_no_prompt_does_not_write_history(project: str, orders_src: str):
    res = build_and_persist(project, _code(orders_src), prompt=None)
    assert read_prompts(project, res.content_hash) == []


def test_entry_detail_renders_prompt_history(fresh_companion_app, project: str, orders_src: str):
    a = build_and_persist(project, _code(orders_src), prompt="alpha attempt")
    build_and_persist(project, _code(orders_src), prompt="beta attempt")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{a.content_hash}")
    assert r.status_code == 200
    body = r.json()
    prompts = [p["prompt"] for p in body["prompt_history"]]
    assert "alpha attempt" in prompts
    assert "beta attempt" in prompts
    assert len(body["prompt_history"]) == 2
