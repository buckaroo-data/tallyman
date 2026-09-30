from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from tallyman_mcp import transcript
from tallyman_xorq import read_prompts

SESSION = "31ea0328-5705-4735-9aad-ce17003d9afe"

QB_EPA = (
    "Make a result called qb_epa: one row per QB per regular season (attempts + carries >= 200), with passing\n"
    "EPA, rushing EPA, and the team, year signed, and APY from the contract that's currently active for that\n"
    "player. Every QB should end up with exactly one contract row, even players with more than one contract on\n"
    "file. APY is already expressed in millions of dollars, so no further scaling is needed."
)


def _typed(text: str) -> dict:
    return {"type": "user", "origin": {"kind": "human"}, "promptSource": "typed", "message": {"content": text}}


def _call(tool_id: str, tool: str) -> dict:
    block = {"type": "tool_use", "id": tool_id, "name": f"mcp__tallyman__{tool}", "input": {}}
    return {"type": "assistant", "message": {"content": [block]}}


def _result(tool_id: str, reply: dict) -> dict:
    block = {"type": "tool_result", "tool_use_id": tool_id, "content": json.dumps(reply)}
    return {"type": "user", "message": {"content": [block]}}


def _session(tmp_path: Path, monkeypatch, *records: dict, tail: str = "") -> Path:
    """Write a Claude Code transcript for SESSION and point this process at it the way Claude Code does."""
    config = tmp_path / "claude"
    path = config / "projects" / "-Users-someone-nfl-demo" / f"{SESSION}.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in records) + tail)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config))
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", SESSION)
    return path


def test_the_message_is_kept_word_for_word(tmp_path, monkeypatch):
    _session(tmp_path, monkeypatch, _typed(QB_EPA))
    assert transcript.user_words() == QB_EPA


def test_a_paste_is_unwrapped(tmp_path, monkeypatch):
    pasted = 'the UI shows\n\n<pasted_content id="ffb5">\n  Load nfl_contracts.parquet\n</pasted_content id="ffb5">'
    _session(tmp_path, monkeypatch, _typed(pasted))
    assert transcript.user_words() == "the UI shows\n\n  Load nfl_contracts.parquet"


def test_only_what_the_user_typed(tmp_path, monkeypatch):
    _session(
        tmp_path,
        monkeypatch,
        {"type": "user", "isMeta": True, "message": {"content": [{"type": "text", "text": "Base directory for"}]}},
        {"type": "user", "message": {"content": "<local-command-stdout>ok</local-command-stdout>"}},
        _typed(QB_EPA),
        _call("t1", "catalog_query"),
        _result("t1", {"rows": []}),
        {
            "type": "user",
            "origin": {"kind": "task-notification"},
            "promptSource": "system",
            "message": {"content": "<task-notification>done</task-notification>"},
        },
    )
    assert transcript.user_words() == QB_EPA


def test_the_words_start_after_the_last_turn_that_changed_the_catalog(tmp_path, monkeypatch):
    _session(
        tmp_path,
        monkeypatch,
        _typed(QB_EPA),
        _call("t1", "catalog_create"),
        _result("t1", {"hash": "e93efae41293", "alias": "qb_epa", "version": 1}),
        _typed("year_signed and season should render as plain integers"),
        _call("t2", "catalog_add_display_klass"),
        _result("t2", {"name": "plain_int"}),
        _typed("reorder qb_epa so the player's name is the first column"),
    )
    assert transcript.user_words() == "reorder qb_epa so the player's name is the first column"


def test_a_turn_that_only_looked_at_the_data_is_part_of_the_request(tmp_path, monkeypatch):
    _session(
        tmp_path,
        monkeypatch,
        _typed(QB_EPA),
        _call("t1", "catalog_query"),
        _result("t1", {"rows": []}),
        _typed("use the contract flagged is_active"),
    )
    assert transcript.user_words() == f"{QB_EPA}\n\nuse the contract flagged is_active"


def test_a_failed_change_does_not_end_the_request(tmp_path, monkeypatch):
    _session(
        tmp_path,
        monkeypatch,
        _typed(QB_EPA),
        _call("t1", "catalog_create"),
        _result("t1", {"error": "build_expr failed: no column apy_millions"}),
        _typed("the column is apy"),
    )
    assert transcript.user_words() == f"{QB_EPA}\n\nthe column is apy"


def test_every_entry_made_in_one_turn_gets_its_message(tmp_path, monkeypatch):
    # The second catalog_create of "make a and b": the first one already ran in this same turn.
    _session(
        tmp_path,
        monkeypatch,
        _typed("make a and b"),
        _call("t1", "catalog_create"),
        _result("t1", {"hash": "aaaa", "alias": "a", "version": 1}),
    )
    assert transcript.user_words() == "make a and b"


def test_a_line_still_being_written_is_skipped(tmp_path, monkeypatch):
    _session(tmp_path, monkeypatch, _typed(QB_EPA), tail='{"type": "assistant", "mess')
    assert transcript.user_words() == QB_EPA


def test_outside_claude_code_there_are_no_words(tmp_path, monkeypatch):
    _session(tmp_path, monkeypatch, _typed(QB_EPA))
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID")
    assert transcript.user_words() is None
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "00000000-0000-0000-0000-000000000000")
    assert transcript.user_words() is None


def _code(src: str, where: str = "") -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r}){where}
expr = t.group_by("region").aggregate(n=t.count())
"""


def test_catalog_create_and_revise_record_the_users_words(
    fresh_companion_app, project: str, orders_src: str, tmp_path, monkeypatch
):
    from tallyman_mcp.server import catalog_create, catalog_revise

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    asked = "count orders by region.\nEvery region should appear once."
    session = _session(tmp_path, monkeypatch, _typed(asked))
    created = catalog_create("by_region", _code(orders_src), prompt="orders per region")
    assert "error" not in created

    manifest = json.loads((Path(created["entry_path"]) / "manifest.json").read_text())
    assert manifest["prompt"] == "orders per region"
    assert manifest["user_prompt"] == asked
    assert read_prompts(project, created["hash"])[-1] == {
        "prompt": "orders per region",
        "user_prompt": asked,
        "at": read_prompts(project, created["hash"])[-1]["at"],
    }
    body = TestClient(fresh_companion_app).get(f"/{project}/api/entry/{created['hash']}").json()
    assert body["manifest"]["user_prompt"] == asked

    # The next turn: the create above ran in the turn before, so only the new message goes on the revision.
    with session.open("a") as fh:
        for record in (_call("t1", "catalog_create"), _result("t1", created), _typed("only orders over $100")):
            fh.write(json.dumps(record) + "\n")
    revised = catalog_revise("by_region", _code(orders_src, "\nt = t.filter(t.price > 100)"), prompt="over $100 only")
    assert "error" not in revised
    manifest = json.loads((Path(revised["entry_path"]) / "manifest.json").read_text())
    assert manifest["user_prompt"] == "only orders over $100"
