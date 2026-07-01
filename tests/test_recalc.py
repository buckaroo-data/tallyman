"""Stage 2 of the reactive consumer: recompute stale entries + dependents (#89).

These build real catalog entries (via the MCP create/revise tools, which set the
alias and checkpoint) and drive ``tallyman_xorq.recalc.recalc`` over them. The
walk's correctness rests on three properties the tests pin: a followed parent is
re-resolved to the *advanced* alias head on replay, an unchanged/pinned entry
replays to the same hash (a no-op), and a build failure stops the walk with the
prefix left committed.

Note on staleness axes: a *cheap* child inlines its parent's recipe, so the
parent's source is collected into the child's own ``manifest.sources`` (the
documented cheap-chain leak) — a source edit makes such a child *directly* stale,
not merely transitively. A pure transitive/cascade case therefore needs the
followed-alias axis (advance the alias with ``catalog_revise``), which is what
``test_alias_advance_cascades_to_the_follower`` exercises.
"""

from __future__ import annotations

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir
from tallyman_core.aliases import get_alias
from tallyman_core.catalog_state import current_step, reset_to
from tallyman_core.manifest import Manifest, ParentRef, write_manifest
from tallyman_core.paths import entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.recalc import recalc
from tallyman_xorq.staleness import scan


def _base_code(project: str) -> str:  # root over orders.parquet
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _base_code_v2(project: str) -> str:  # a different graph → a new content hash, same source
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price").mutate(extra=1)
"""


def _child_code(alias: str) -> str:  # tracked child off an alias
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({alias!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _pinned_child_code(content_hash: str) -> str:  # pinned child off a specific hash
    return f"""
from tallyman_xorq.io import pinned_expr_from_alias
t = pinned_expr_from_alias({content_hash!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _hash(result: dict) -> str:
    assert "hash" in result, result
    return result["hash"]


def _edit_source(project: str) -> None:
    """Rewrite orders.parquet in place with different content (new digest)."""
    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=250, seed=7)


# ---------------------------------------------------------------------------
# dry-run preview
# ---------------------------------------------------------------------------


def test_dry_run_previews_the_ordered_cone_without_building(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a = _hash(catalog_create("a", _base_code(project)))
    b = _hash(catalog_create("b", _child_code("a")))
    _edit_source(project)

    before = {e["content_hash"] for e in list_entries(project)}
    report = recalc(project, [a], dry_run=True)

    assert report.dry_run is True
    assert report.status == "ok"
    assert report.cone == [a, b]  # topological: parent before child
    assert report.remap == {}
    # No build happened: the entry set and the alias heads are untouched.
    assert {e["content_hash"] for e in list_entries(project)} == before
    assert get_alias(project, "a") == a
    assert get_alias(project, "b") == b
    # Every previewed action is a dry-run verdict, and the directly-stale root
    # previews as a rebuild.
    actions = {e.content_hash: e.action for e in report.entries}
    assert set(actions.values()) <= {"rebuild", "cascade", "unchanged"}
    assert actions[a] == "rebuild"


# ---------------------------------------------------------------------------
# real run: source-drifted root rebuilds and the cone is recomputed + re-pointed
# ---------------------------------------------------------------------------


def test_recalc_rebuilds_drifted_root_and_repoints_the_cone(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a1 = _hash(catalog_create("a", _base_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))

    _edit_source(project)
    assert scan(project)[a1].stale is True  # source axis

    report = recalc(project, [a1], dry_run=False)

    assert report.status == "ok"
    a2, b2 = get_alias(project, "a"), get_alias(project, "b")
    assert a2 != a1 and b2 != b1  # both advanced
    assert report.remap == {a1: a2, b1: b2}
    assert {e.content_hash: e.action for e in report.entries} == {a1: "rebuilt", b1: "rebuilt"}
    assert report.checkpoint_step is not None  # one checkpoint for the walk

    # The catalog is fresh again, and b2's recorded parent resolves to the new a.
    after = scan(project)
    assert after[a2].stale is False and after[b2].stale is False
    parents = next(e for e in list_entries(project) if e["content_hash"] == b2)["parents"]
    assert parents[0]["hash"] == a2 and parents[0]["follow"] is True


def test_alias_advance_cascades_to_the_follower(project, orders_parquet, monkeypatch):
    # The pure followed-alias axis: advancing a's alias (not its source) makes its
    # follower b *directly* stale on the alias axis; recalc re-resolves b to the
    # advanced head. b's source is unchanged, so this isolates the alias cascade.
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    # Opt out of auto-recalc-on-revise so the revise advances a WITHOUT recomputing
    # b: otherwise the cascade re-points "b" to a fresh head and b1 becomes a
    # superseded husk (never actionably stale under #154), not the *live* directly-
    # stale follower this test means to observe and then recalc explicitly.
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    a1 = _hash(catalog_create("a", _base_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))

    a2 = _hash(catalog_revise("a", _base_code_v2(project)))
    assert a2 != a1 and get_alias(project, "a") == a2
    v = scan(project)[b1]
    assert v.stale is True and [r.axis for r in v.reasons] == ["alias"]

    report = recalc(project, [b1], dry_run=False)

    assert report.status == "ok"
    b2 = get_alias(project, "b")
    assert b2 != b1
    assert report.remap == {b1: b2}
    parents = next(e for e in list_entries(project) if e["content_hash"] == b2)["parents"]
    assert parents[0]["hash"] == a2  # now bound to the advanced a


def test_recalc_is_one_undoable_transaction(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a1 = _hash(catalog_create("a", _base_code(project)))
    _hash(catalog_create("b", _child_code("a")))
    step_before = current_step(project)

    _edit_source(project)
    report = recalc(project, [a1], dry_run=False)
    assert report.checkpoint_step == current_step(project)
    assert report.checkpoint_step > step_before

    # reset_to the pre-recalc step puts the heads back where they were.
    reset_to(project, step_before)
    assert get_alias(project, "a") == a1


# ---------------------------------------------------------------------------
# pinned child stays put; nothing-stale is a no-op
# ---------------------------------------------------------------------------


def test_hash_pinned_child_is_left_on_its_pin(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a1 = _hash(catalog_create("a", _base_code(project)))
    # A literal-hash argument records follow=False — the child asked for THIS
    # revision, so a recalc that advances the alias must not advance the child.
    b1 = _hash(catalog_create("b", _pinned_child_code(a1)))

    a2 = _hash(catalog_revise("a", _base_code_v2(project)))
    assert a2 != a1
    report = recalc(project, [a1], dry_run=False)

    assert get_alias(project, "a") == a2  # the alias advanced (by the revise)
    assert get_alias(project, "b") == b1  # the pin held
    assert b1 not in report.remap
    assert next(e for e in report.entries if e.content_hash == b1).action == "noop"


def test_recalc_with_nothing_stale_is_a_noop(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a1 = _hash(catalog_create("a", _base_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))
    step_before = current_step(project)
    before = {e["content_hash"] for e in list_entries(project)}

    report = recalc(project, [a1], dry_run=False)

    assert report.status == "ok"
    assert report.remap == {}
    assert report.checkpoint_step is None  # nothing changed → no checkpoint
    assert {e.action for e in report.entries} == {"noop"}
    assert {e["content_hash"] for e in list_entries(project)} == before
    assert current_step(project) == step_before
    assert get_alias(project, "a") == a1 and get_alias(project, "b") == b1


# ---------------------------------------------------------------------------
# cascade failure: stop-and-report, prefix committed
# ---------------------------------------------------------------------------


def test_build_failure_stops_the_walk_and_keeps_the_prefix(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    a1 = _hash(catalog_create("a", _base_code(project)))
    b1 = _hash(catalog_create("b", _child_code("a")))

    # Corrupt b's recipe so its replay raises — the realistic failure is a child
    # whose code no longer plans against an advanced parent; an unparseable recipe
    # is the deterministic stand-in.
    (entry_dir(project, b1) / "expr.py").write_text("this is not valid python (((\n")

    _edit_source(project)
    report = recalc(project, [a1], dry_run=False)

    assert report.status == "failed"
    a2 = get_alias(project, "a")
    assert a2 != a1  # the prefix (a) rebuilt and committed
    assert report.remap == {a1: a2}
    assert report.checkpoint_step is not None  # prefix was checkpointed
    by_hash = {e.content_hash: e for e in report.entries}
    assert by_hash[a1].action == "rebuilt"
    assert by_hash[b1].action == "failed" and by_hash[b1].error
    assert get_alias(project, "b") == b1  # b's alias untouched


# ---------------------------------------------------------------------------
# cycle: report, don't hang (resolves before any build)
# ---------------------------------------------------------------------------


def test_recalc_reports_a_cycle_instead_of_hanging(project):
    # Hand-written manifests with cyclic parent edges; recalc surfaces the cycle
    # while building the cone, before any recipe runs.
    for h, parent in (("x", "y"), ("y", "x")):
        d = entry_dir(project, h)
        d.mkdir(parents=True, exist_ok=True)
        write_manifest(
            d, Manifest(content_hash=h, project=project, parents=[ParentRef(hash=parent, ref=parent, follow=True)])
        )

    report = recalc(project, ["x"], dry_run=True)
    assert report.status == "cycle"
    assert report.error
    assert report.entries == []


def test_recalc_skips_a_root_dir_without_a_manifest(project):
    # A directory that exists but holds no manifest.json (a crashed mid-build
    # leftover) is not a real entry — scan() / list_entries() never see it. The
    # root guard must match that set, else the cone carries a hash that isn't in
    # `verdicts` and the walk KeyErrors. It must instead drop it and report clean.
    ghost = "cafebabe" * 4  # a hash-shaped name with a dir but no manifest
    entry_dir(project, ghost).mkdir(parents=True, exist_ok=True)
    assert not (entry_dir(project, ghost) / "manifest.json").exists()

    for dry in (True, False):
        report = recalc(project, [ghost], dry_run=dry)
        assert report.status == "ok"
        assert report.cone == []
        assert report.entries == []
        assert report.remap == {}
