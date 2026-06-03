from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from pydata_xorq import build_and_persist, read_prompts


def _code(parquet: Path) -> str:
    return f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_first_build_records_one_prompt(project: str, orders_parquet: Path):
    res = build_and_persist(project, _code(orders_parquet), prompt="first")
    history = read_prompts(res.entry_path)
    assert len(history) == 1
    assert history[0]["prompt"] == "first"


def test_re_run_appends_prompt(project: str, orders_parquet: Path):
    a = build_and_persist(project, _code(orders_parquet), prompt="first")
    build_and_persist(project, _code(orders_parquet), prompt="second")
    history = read_prompts(a.entry_path)
    assert [p["prompt"] for p in history] == ["first", "second"]


def test_manifest_prompt_remains_first(project: str, orders_parquet: Path):
    import json

    a = build_and_persist(project, _code(orders_parquet), prompt="first")
    build_and_persist(project, _code(orders_parquet), prompt="second")
    manifest = json.loads((a.entry_path / "manifest.json").read_text())
    assert manifest["prompt"] == "first"


def test_no_prompt_does_not_write_history(project: str, orders_parquet: Path):
    res = build_and_persist(project, _code(orders_parquet), prompt=None)
    assert read_prompts(res.entry_path) == []


def test_entry_detail_renders_prompt_history(fresh_companion_app, project: str, orders_parquet: Path):
    a = build_and_persist(project, _code(orders_parquet), prompt="alpha attempt")
    build_and_persist(project, _code(orders_parquet), prompt="beta attempt")
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{a.content_hash}")
    assert r.status_code == 200
    body = r.json()
    prompts = [p["prompt"] for p in body["prompt_history"]]
    assert "alpha attempt" in prompts
    assert "beta attempt" in prompts
    assert len(body["prompt_history"]) == 2
