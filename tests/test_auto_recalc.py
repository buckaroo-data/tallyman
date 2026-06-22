"""Auto-recalc on revise (#89 follow-up): revising an alias optionally recomputes
its stale dependents in the *same* atomic checkpoint.

The recalc engine is already tested in ``test_recalc.py``; this pins the *trigger*
folded into ``catalog_revise``: that the cascade fires from the revise (no separate
``catalog_recalc``), lands as exactly ONE git revision (so ``reset_to`` the
pre-revise step undoes the revise *and* every dependent it recomputed), respects
the per-project ``auto_recalc`` switch (env → config.json → default ON), stops and
reports on a downstream failure, and accounts for every stale entry it did *not*
recompute (orphan-stale logging + the durable-error tie-back of #6).
"""

from __future__ import annotations

import logging

from tallyman_core.config import auto_recalc_enabled, set_auto_recalc

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir
from tallyman_core.aliases import get_alias
from tallyman_core.catalog_state import current_step, reset_to
from tallyman_core.errors import error_for_hash
from tallyman_core.paths import entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.staleness import scan

# ---------------------------------------------------------------------------
# recipe helpers (mirror test_recalc.py)
# ---------------------------------------------------------------------------


def _src_code(project: str, src: str = "orders.parquet") -> str:  # a source projection
    return f"""
from tallyman_xorq.io import from_project
t = from_project({src!r}, project={project!r})
expr = t.select("region", "price")
"""


def _src_code_v2(project: str, src: str = "orders.parquet") -> str:  # different graph, same source
    return f"""
from tallyman_xorq.io import from_project
t = from_project({src!r}, project={project!r})
expr = t.select("region", "price").mutate(extra=1)
"""


def _child_code(parent: str) -> str:  # from_catalog child (alias name → follow=True)
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _hash(result: dict) -> str:
    assert "hash" in result, result
    return result["hash"]


def _second_source(project: str, name: str = "widgets.parquet", *, seed: int = 1, n: int = 120):
    return write_shoe_orders(data_dir(project) / name, n_rows=n, seed=seed)


def _clean_env(monkeypatch) -> None:
    """Default-ON path: no env override, content-addressed sources."""
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")


# ---------------------------------------------------------------------------
# 1. cascade-on-revise (default ON): revise recomputes the follower
# ---------------------------------------------------------------------------


def test_revise_cascades_to_follower_without_explicit_recalc(project, orders_parquet, monkeypatch):
    _clean_env(monkeypatch)
    a1 = _hash(catalog_create("a", _src_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))

    out = catalog_revise("a", _src_code_v2(project))  # NO catalog_recalc call
    a2 = out["hash"]

    assert a2 != a1 and get_alias(project, "a") == a2
    b2 = get_alias(project, "b")
    assert b2 != b1  # the follower was recomputed and re-pointed by the revise itself
    assert "recalc" in out
    assert out["recalc"]["status"] == "ok"
    assert out["recalc"]["remap"] == {b1: b2}
    # b2 is bound to the advanced a, and a fresh scan is clean.
    parents = next(e for e in list_entries(project) if e["content_hash"] == b2)["parents"]
    assert parents[0]["hash"] == a2 and parents[0]["follow"] is True
    after = scan(project)
    assert after[a2].stale is False and after[b2].stale is False


# ---------------------------------------------------------------------------
# 2. atomic: revise + cascade is exactly ONE undoable revision
# ---------------------------------------------------------------------------


def test_revise_and_cascade_is_one_undoable_revision(project, orders_parquet, monkeypatch):
    _clean_env(monkeypatch)
    a1 = _hash(catalog_create("a", _src_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))
    step_before = current_step(project)

    out = catalog_revise("a", _src_code_v2(project))
    assert out["hash"] != a1 and get_alias(project, "b") != b1  # both moved
    # exactly one checkpoint for the head advance + the whole cascade
    assert current_step(project) == step_before + 1

    # reset_to the pre-revise step rolls back the revise AND the dependent together.
    reset_to(project, step_before)
    assert get_alias(project, "a") == a1
    assert get_alias(project, "b") == b1


# ---------------------------------------------------------------------------
# 3. opt-out: flag off → revise advances the alias, leaves the follower stale
# ---------------------------------------------------------------------------


def test_flag_off_leaves_follower_stale(project, orders_parquet, monkeypatch):
    _clean_env(monkeypatch)
    set_auto_recalc(project, False)
    assert auto_recalc_enabled(project) is False
    a1 = _hash(catalog_create("a", _src_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))

    out = catalog_revise("a", _src_code_v2(project))
    assert out["hash"] != a1 and get_alias(project, "a") == out["hash"]
    assert get_alias(project, "b") == b1  # follower NOT recomputed (today's behavior)
    assert scan(project)[b1].stale is True
    assert "recalc" not in out  # no auto sub-report when off


# ---------------------------------------------------------------------------
# 4. no followers: one checkpoint, empty recalc report, identical to today
# ---------------------------------------------------------------------------


def test_revise_with_no_followers_is_one_checkpoint_empty_report(project, orders_parquet, monkeypatch):
    _clean_env(monkeypatch)
    a1 = _hash(catalog_create("a", _src_code(project)))  # nothing follows a
    step_before = current_step(project)

    out = catalog_revise("a", _src_code_v2(project))
    assert out["hash"] != a1
    assert current_step(project) == step_before + 1
    assert out["recalc"]["status"] == "ok"
    assert out["recalc"]["remap"] == {}
    assert out["recalc"]["cone"] == []


# ---------------------------------------------------------------------------
# 5. stop-and-report: a broken downstream recipe → revise lands, cascade fails
# ---------------------------------------------------------------------------


def test_broken_downstream_stops_and_reports_revise_still_lands(project, orders_parquet, monkeypatch):
    _clean_env(monkeypatch)
    a1 = _hash(catalog_create("a", _src_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))
    # Corrupt b's recipe so its cascade replay raises.
    (entry_dir(project, b1) / "expr.py").write_text("this is not valid python (((\n")
    step_before = current_step(project)

    out = catalog_revise("a", _src_code_v2(project))
    assert out["hash"] != a1 and get_alias(project, "a") == out["hash"]  # revise lands
    assert out["recalc"]["status"] == "failed"
    assert current_step(project) == step_before + 1  # still one checkpoint
    assert get_alias(project, "b") == b1  # broken follower's alias untouched
    by_hash = {e["content_hash"]: e for e in out["recalc"]["entries"]}
    assert by_hash[b1]["action"] == "failed" and by_hash[b1]["error"]


# ---------------------------------------------------------------------------
# 6. return shape: the recalc sub-report is present and well-formed
# ---------------------------------------------------------------------------


def test_recalc_sub_report_shape(project, orders_parquet, monkeypatch):
    _clean_env(monkeypatch)
    _hash(catalog_create("a", _src_code(project)))
    _hash(catalog_create("b", _child_code("a")))

    out = catalog_revise("a", _src_code_v2(project))
    r = out["recalc"]
    assert {"cone", "entries", "remap", "status", "orphan_stale"} <= set(r)
    assert isinstance(r["entries"], list) and isinstance(r["cone"], list)
    assert r["status"] == "ok"


# ---------------------------------------------------------------------------
# 7. orphan-stale: an unrelated pre-existing stale entry is left alone, logged,
#    and returned classified UNEXPLAINED (no matching error record)
# ---------------------------------------------------------------------------


def test_orphan_stale_is_left_untouched_logged_and_classified(project, orders_parquet, monkeypatch, caplog):
    _clean_env(monkeypatch)
    _second_source(project, "widgets.parquet", seed=1)
    a1 = _hash(catalog_create("a", _src_code(project)))  # over orders.parquet
    b1 = _hash(catalog_create("b", _child_code("a")))
    c1 = _hash(catalog_create("c", _src_code(project, "widgets.parquet")))

    # Make c directly source-stale for a reason unrelated to the revise of a.
    _second_source(project, "widgets.parquet", seed=7, n=250)
    assert scan(project)[c1].stale is True  # c is directly source-stale

    with caplog.at_level(logging.WARNING):
        out = catalog_revise("a", _src_code_v2(project))

    assert out["hash"] != a1
    assert get_alias(project, "b") != b1  # the follower recomputed
    assert get_alias(project, "c") == c1  # the orphan left untouched
    assert c1 not in out["recalc"]["remap"]
    orphans = {o["hash"]: o for o in out["recalc"]["orphan_stale"]}
    assert c1 in orphans
    assert "UNEXPLAINED" in orphans[c1]["explanation"]
    # logged at WARNING with the hash
    assert c1 in caplog.text and "UNEXPLAINED" in caplog.text


# ---------------------------------------------------------------------------
# 8. failure persisted + tie-back: a cascade failure writes an errors.jsonl
#    record carrying the failed hash; a later auto-recalc that still sees that
#    entry stale classifies it "explained by error <id>". The record survives reset.
# ---------------------------------------------------------------------------


def test_cascade_failure_persists_and_ties_back(project, orders_parquet, monkeypatch):
    _clean_env(monkeypatch)
    _second_source(project, "widgets.parquet", seed=1)
    _hash(catalog_create("z", _src_code(project, "widgets.parquet")))  # unrelated alias
    _hash(catalog_create("a", _src_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))
    (entry_dir(project, b1) / "expr.py").write_text("broken (((\n")
    step_before = current_step(project)

    # First revise of a: cascade fails on b, recording a durable error for b1.
    out1 = catalog_revise("a", _src_code_v2(project))
    assert out1["recalc"]["status"] == "failed"
    err = error_for_hash(project, b1)
    assert err is not None and err["hash"] == b1

    # A later auto-recalc driven by an UNRELATED alias still sees b1 stale; b1 is
    # not a follower of z, so it lands in orphan_stale, classified explained-by-error.
    out2 = catalog_revise("z", _src_code_v2(project, "widgets.parquet"))
    orphans = {o["hash"]: o for o in out2["recalc"]["orphan_stale"]}
    assert b1 in orphans
    assert err["id"] in orphans[b1]["explanation"]
    assert "explained by error" in orphans[b1]["explanation"]

    # The error record lives outside the catalog repo, so reset_to keeps it.
    reset_to(project, step_before)
    assert error_for_hash(project, b1) is not None


# ---------------------------------------------------------------------------
# 9. config + override: env beats config.json beats the built-in default (ON)
# ---------------------------------------------------------------------------


def test_default_on_when_neither_env_nor_config(project, monkeypatch):
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    assert auto_recalc_enabled(project) is True  # built-in default


def test_config_file_can_disable(project, monkeypatch):
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    set_auto_recalc(project, False)
    assert auto_recalc_enabled(project) is False


def test_env_overrides_config_file(project, monkeypatch):
    set_auto_recalc(project, False)  # config says off
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "1")  # env wins
    assert auto_recalc_enabled(project) is True
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    assert auto_recalc_enabled(project) is False
