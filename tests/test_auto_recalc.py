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

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir
from tallyman_core.aliases import get_alias
from tallyman_core.catalog_state import current_step, reset_to
from tallyman_core.config import auto_recalc_enabled, set_auto_recalc
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
from tallyman_xorq.io import read_project_file
t = read_project_file({src!r}, project={project!r})
expr = t.select("region", "price")
"""


def _src_code_v2(project: str, src: str = "orders.parquet") -> str:  # different graph, same source
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file({src!r}, project={project!r})
expr = t.select("region", "price").mutate(extra=1)
"""


def _child_code(parent: str) -> str:  # cheap tracked_expr_from_alias child (alias name → follow=True)
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _self_chain_code(alias: str) -> str:  # documented revise-in-place pattern: chain off own alias
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({alias!r})
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


def test_auto_recalc_report_carries_real_checkpoint_step(project, orders_parquet, monkeypatch):
    # The auto-path sub-report's `checkpoint_step` must be the step the cascade
    # actually committed in. The walk takes no checkpoint of its own (the per-op
    # dispatch decorator owns it, after the handler returns), so the report is
    # built with checkpoint_step=None; the decorator backfills the real step it
    # gets from checkpoint_catalog before returning the response.
    _clean_env(monkeypatch)
    _hash(catalog_create("a", _src_code(project)))
    _hash(catalog_create("b", _child_code("a")))

    out = catalog_revise("a", _src_code_v2(project))
    assert out["recalc"]["remap"]  # something cascaded
    # The head advance + cascade landed as one revision; that step is reported.
    assert out["recalc"]["checkpoint_step"] is not None
    assert out["recalc"]["checkpoint_step"] == current_step(project)


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


def test_config_json_is_tracked_and_reset_restores_it(project, orders_parquet, monkeypatch):
    # config.json must be on the catalog's tracked-surface allowlist: it is swept
    # into checkpoints and reset_to's assert_catalog_consistent would otherwise
    # reject it as an unlisted tracked path. Pin both that reset doesn't raise and
    # that the setting versions with the catalog.
    from tallyman_core.config import read_config

    _clean_env(monkeypatch)
    set_auto_recalc(project, False)
    _hash(catalog_create("a", _src_code(project)))  # this checkpoint commits config.json (auto_recalc=false)
    step = current_step(project)

    set_auto_recalc(project, True)
    _hash(catalog_create("b", _src_code_v2(project)))  # later checkpoint commits auto_recalc=true
    assert read_config(project)["auto_recalc"] is True

    reset_to(project, step)  # must not raise; reverts the tracked config.json
    assert read_config(project)["auto_recalc"] is False


def test_read_config_tolerates_unreadable_file(project, monkeypatch):
    # The docstring promises a corrupt config never wedges a tool call. A non-UTF8
    # config.json must resolve to defaults (auto_recalc ON), not raise.
    from tallyman_core.config import _config_file, read_config

    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)
    p = _config_file(project)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"\xff\xfe not valid utf-8 \x00\x80")

    assert read_config(project) == {}
    assert auto_recalc_enabled(project) is True


# ---------------------------------------------------------------------------
# tie-back robustness: self-referential revise + skipped-but-directly-stale
# ---------------------------------------------------------------------------


def test_self_referential_revise_is_not_unexplained_orphan(project, orders_parquet, monkeypatch):
    # A self-referential head (a recipe that reads tracked_expr_from_alias of its OWN
    # alias) is permanently alias-stale by design (#74/#85: the self-pin can never
    # refresh). catalog_revise now rejects creating one (#135), but historical heads
    # and indirect cycles still exist, so a later unrelated revise must classify such
    # a head as "self-referential", NOT UNEXPLAINED ("file a bug") — otherwise the
    # UNEXPLAINED signal becomes untrustworthy.
    _clean_env(monkeypatch)
    _second_source(project, "widgets.parquet", seed=1)
    _hash(catalog_create("tpd", _src_code(project)))  # has a price column
    _hash(catalog_create("z", _src_code(project, "widgets.parquet")))  # unrelated alias

    # catalog_revise rejects a self-referential recipe (#135), so build the self-ref
    # head directly to exercise the staleness classification for an existing one.
    from tallyman_core.aliases import set_alias
    from tallyman_xorq import build_and_persist

    res = build_and_persist(project=project, code=_self_chain_code("tpd"))
    set_alias(project, "tpd", res.content_hash, expect_exists=True)
    tpd_head = get_alias(project, "tpd")

    # A later, unrelated revise must classify the self-ref head as explained
    # ("self-referential"), not UNEXPLAINED — keeping the UNEXPLAINED signal trustworthy.
    out = catalog_revise("z", _src_code_v2(project, "widgets.parquet"))
    orphans = {o["hash"]: o for o in out["recalc"]["orphan_stale"]}
    assert tpd_head in orphans  # it IS directly (self-)stale, so it is accounted for
    assert "UNEXPLAINED" not in orphans[tpd_head]["explanation"]
    assert "self-referential" in orphans[tpd_head]["explanation"].lower()


def test_skipped_but_directly_stale_entry_ties_back_to_the_failure(project, orders_parquet, monkeypatch):
    # When a cascade halts, an entry it SKIPS that is itself directly stale must
    # still be accounted for: recorded against the durable error store so a later
    # unrelated revise classifies it "explained", not a false UNEXPLAINED.
    #
    # Two siblings b and c both follow a, so revising a makes BOTH directly
    # alias-stale (both become recalc roots). Break both recipes: whichever the
    # walk reaches first fails and halts, leaving the other SKIPPED while still
    # directly stale — exactly the case the skipped-record covers, order-agnostic.
    _clean_env(monkeypatch)
    _second_source(project, "widgets.parquet", seed=1)
    _hash(catalog_create("z", _src_code(project, "widgets.parquet")))  # unrelated alias
    _hash(catalog_create("a", _src_code(project)))
    # Distinct recipes so b and c are different entries (same recipe → same hash →
    # one entry with two aliases), both following a.
    b1 = _hash(
        catalog_create(
            "b",
            "from tallyman_xorq.io import tracked_expr_from_alias\n"
            "t = tracked_expr_from_alias('a')\nexpr = t.mutate(d2=t.price * 2)\n",
        )
    )
    c1 = _hash(
        catalog_create(
            "c",
            "from tallyman_xorq.io import tracked_expr_from_alias\n"
            "t = tracked_expr_from_alias('a')\nexpr = t.mutate(d3=t.price * 3)\n",
        )
    )
    for h in (b1, c1):
        (entry_dir(project, h) / "expr.py").write_text("broken (((\n")

    out1 = catalog_revise("a", _src_code_v2(project))  # roots [b, c]: one fails, the other is skipped
    assert out1["recalc"]["status"] == "failed"
    by = {e["content_hash"]: e["action"] for e in out1["recalc"]["entries"]}
    assert sorted([by[b1], by[c1]]) == ["failed", "skipped"]
    skipped = b1 if by[b1] == "skipped" else c1
    # the skipped-but-directly-stale entry is recorded, so error_for_hash resolves it
    assert error_for_hash(project, skipped) is not None

    out2 = catalog_revise("z", _src_code_v2(project, "widgets.parquet"))  # unrelated revise
    orphans = {o["hash"]: o for o in out2["recalc"]["orphan_stale"]}
    assert skipped in orphans
    assert "UNEXPLAINED" not in orphans[skipped]["explanation"]
    assert "explained by error" in orphans[skipped]["explanation"]


# ---------------------------------------------------------------------------
# review finding #2: a cascade failure on the revise auto-path attributes the
# error to the triggering tool, not the hard-coded "catalog_recalc"
# ---------------------------------------------------------------------------


def test_cascade_failure_records_triggering_tool(project, orders_parquet, monkeypatch):
    # The persisted recalc failure must name the tool that actually ran. On the
    # auto-recalc path that is catalog_revise; "catalog_recalc" (which never ran
    # here) would misattribute the failure in the error banner / forensics.
    _clean_env(monkeypatch)
    a1 = _hash(catalog_create("a", _src_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))
    (entry_dir(project, b1) / "expr.py").write_text("broken (((\n")

    out = catalog_revise("a", _src_code_v2(project))
    assert out["recalc"]["status"] == "failed"
    err = error_for_hash(project, b1)
    assert err is not None and err["hash"] == b1
    assert err["tool"] == "catalog_revise"
    assert a1 != out["hash"]


# ---------------------------------------------------------------------------
# review finding #3: auto_recalc scans staleness once, not twice (it reused the
# verdicts it already computed instead of letting the cone walk re-scan)
# ---------------------------------------------------------------------------


def test_auto_recalc_scans_staleness_once(project, orders_parquet, monkeypatch):
    # auto_recalc computed verdicts via scan(), then the cone walk scanned again —
    # two full staleness scans (each re-digests every source) per revise. The walk
    # must reuse the verdicts auto_recalc already holds.
    import tallyman_xorq.recalc as recalc_mod

    _clean_env(monkeypatch)
    _hash(catalog_create("a", _src_code(project)))
    _hash(catalog_create("b", _child_code("a")))

    calls = {"n": 0}
    real_scan = recalc_mod.scan

    def counting_scan(p):
        calls["n"] += 1
        return real_scan(p)

    monkeypatch.setattr(recalc_mod, "scan", counting_scan)
    catalog_revise("a", _src_code_v2(project))
    assert calls["n"] == 1
