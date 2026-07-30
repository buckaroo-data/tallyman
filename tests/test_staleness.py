from __future__ import annotations

import os

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir
from tallyman_core.aliases import get_alias
from tallyman_mcp.server import catalog_create, catalog_revise, catalog_unalias
from tallyman_xorq.dependents import sources_of
from tallyman_xorq.staleness import entry_staleness, scan


def _base_code(project: str) -> str:  # read_project_file root over orders.parquet
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price")
"""


def _base_code_v2(project: str) -> str:  # a different graph → a new content hash
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


def _pinned_child_code(content_hash: str) -> str:  # pinned (no lineage) child off a specific hash
    return f"""
from tallyman_xorq.io import pinned_expr_from_alias
t = pinned_expr_from_alias({content_hash!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _alias_pinned_child_code(alias: str) -> str:  # pinned by NAME — still follow=False
    return f"""
from tallyman_xorq.io import pinned_expr_from_alias
t = pinned_expr_from_alias({alias!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _mixed_code(parent: str, src: str, project: str) -> str:  # follows an alias AND reads a raw file
    return f"""
from tallyman_xorq.io import read_project_file, tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
u = read_project_file({src!r}, project={project!r})
expr = t.union(u.select("region", "price"))
"""


def _agg_child_code(parent: str) -> str:  # expensive (Aggregate) child → bakes a snapshot
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.group_by("region").aggregate(total=t.price.sum())
"""


def _const_child_code(parent: str) -> str:  # cheap child, schema-independent
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.mutate(flag=1)
"""


def _hash(result: dict) -> str:
    assert "hash" in result, result
    return result["hash"]


def test_followed_alias_advance_marks_child_stale(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_v1 = _hash(catalog_create("base", _base_code(project)))
    child_hash = _hash(catalog_create("child", _child_code("base")))

    # The followed edge still matches the alias head — not stale yet.
    assert entry_staleness(project, child_hash).stale is False

    _hash(catalog_revise("base", _base_code_v2(project)))
    base_v2 = get_alias(project, "base")
    assert base_v2 != base_v1

    v = entry_staleness(project, child_hash)
    assert v.stale is True
    alias_reasons = [r for r in v.reasons if r.axis == "alias"]
    assert len(alias_reasons) == 1
    assert (alias_reasons[0].ref, alias_reasons[0].was, alias_reasons[0].now) == (
        "base",
        base_v1,
        base_v2,
    )


def test_hash_pinned_parent_is_not_stale_when_alias_advances(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_v1 = _hash(catalog_create("base", _base_code(project)))
    # A literal-hash argument is recorded follow=False (a pin), so the alias
    # advancing must not make this child stale.
    child_hash = _hash(catalog_create("child", _pinned_child_code(base_v1)))

    _hash(catalog_revise("base", _base_code_v2(project)))
    assert get_alias(project, "base") != base_v1

    v = entry_staleness(project, child_hash)
    assert v.stale is False
    assert [r for r in v.reasons if r.axis == "alias"] == []


def test_source_edit_marks_entry_stale_under_cas(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_hash = _hash(catalog_create("base", _base_code(project)))
    assert entry_staleness(project, base_hash).stale is False

    # Rewrite the source in place with different data (new bytes, new mtime).
    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=1)

    v = entry_staleness(project, base_hash)
    assert v.stale is True
    source_reasons = [r for r in v.reasons if r.axis == "source"]
    assert len(source_reasons) == 1
    assert source_reasons[0].ref == "orders.parquet"


def test_off_mode_reports_source_axis_unknown_not_fresh(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "off")
    base_hash = _hash(catalog_create("base", _base_code(project)))

    v = entry_staleness(project, base_hash)
    assert v.stale is False
    assert "source" in v.unknown_axes

    # off mode records no source digests, so it cannot see drift — unknown, not stale.
    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=1)
    v2 = entry_staleness(project, base_hash)
    assert v2.stale is False
    assert "source" in v2.unknown_axes


def test_scan_distinguishes_direct_from_transitive_staleness(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    # Opt out of auto-recalc-on-revise: this test needs the revise to advance "a"
    # WITHOUT cascading, so the scan can observe b directly-stale and c only
    # transitively-stale. With the cascade on (the default), the revise would
    # recompute b and c, leaving no transitive-only node to classify.
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    a_v1 = _hash(catalog_create("a", _base_code(project)))
    # b is an expensive (Aggregate) intermediate: reconstructing it reads its
    # baked snapshot rather than re-running its recipe, so its tracked_expr_from_alias("a")
    # edge does not leak into c. (A cheap intermediate re-runs inline, so c would
    # follow "a" directly and there would be no transitive-only node.)
    b = _hash(catalog_create("b", _agg_child_code("a")))
    c = _hash(catalog_create("c", _const_child_code("b")))

    # Advance alias "a". b follows "a" so it is DIRECTLY stale; c follows "b"
    # (unchanged), so it is only TRANSITIVELY stale via its ancestor b.
    _hash(catalog_revise("a", _base_code_v2(project)))
    assert get_alias(project, "a") != a_v1

    verdicts = scan(project)
    assert verdicts[b].stale is True
    assert verdicts[b].transitively_stale is False
    assert verdicts[c].stale is False
    assert verdicts[c].transitively_stale is True


def test_alias_pinned_parent_is_not_stale_when_alias_advances(project, orders_parquet, monkeypatch):
    """docs/reactive-recalc.md: ``pinned_expr_from_alias`` accepts an alias OR a
    hash, and either form records follow=False — the deliberate opt-out. The
    existing pin test uses a literal hash; this pins the by-NAME form: the alias
    advancing must not make the child stale on the alias axis."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    base_v1 = _hash(catalog_create("base", _base_code(project)))
    child_hash = _hash(catalog_create("child", _alias_pinned_child_code("base")))

    _hash(catalog_revise("base", _base_code_v2(project)))
    assert get_alias(project, "base") != base_v1

    v = entry_staleness(project, child_hash)
    assert v.stale is False
    assert [r for r in v.reasons if r.axis == "alias"] == []


def test_deleted_parent_alias_is_unknown_axis_not_stale(project, orders_parquet, monkeypatch):
    """docs/reactive-recalc.md: an axis that can't be evaluated — here, the
    followed alias was deleted — lands in ``unknown_axes`` and never forces
    ``stale: true``."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    catalog_create("base", _base_code(project))
    child_hash = _hash(catalog_create("child", _child_code("base")))

    out = catalog_unalias("base")
    assert "error" not in out, out

    v = entry_staleness(project, child_hash)
    assert v.stale is False
    # A specific gone input is reported axis-qualified ("alias:<ref>"); the bare
    # axis form is reserved for a wholly-unevaluable axis (off mode).
    assert "alias:base" in v.unknown_axes


def test_deleted_source_file_is_unknown_axis_not_stale(project, orders_parquet, monkeypatch):
    """docs/reactive-recalc.md: a removed source file is the other can't-evaluate
    case — unknown, not stale (``now`` can't be computed for a file that is gone)."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_hash = _hash(catalog_create("base", _base_code(project)))

    (data_dir(project) / "orders.parquet").unlink()

    v = entry_staleness(project, base_hash)
    assert v.stale is False
    assert "source:orders.parquet" in v.unknown_axes


def test_recreated_alias_at_new_head_marks_child_stale(project, orders_parquet, monkeypatch):
    """Alias identity is the NAME: unalias a followed parent, then re-create the
    same name at a different entry. The child's recorded edge (``was`` = the old
    head) no longer matches what the name resolves to now — directly stale on
    the alias axis, with was/now naming both hashes."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    base_v1 = _hash(catalog_create("base", _base_code(project)))
    child_hash = _hash(catalog_create("child", _child_code("base")))

    catalog_unalias("base")
    base_v2 = _hash(catalog_create("base", _base_code_v2(project)))
    assert base_v2 != base_v1

    v = entry_staleness(project, child_hash)
    assert v.stale is True
    alias_reasons = [r for r in v.reasons if r.axis == "alias"]
    assert len(alias_reasons) == 1
    assert (alias_reasons[0].ref, alias_reasons[0].was, alias_reasons[0].now) == (
        "base",
        base_v1,
        base_v2,
    )


def test_both_axes_can_be_stale_at_once(project, orders_parquet, monkeypatch):
    """The two axes are independent: an entry that follows an alias AND reads a
    raw file goes stale on both when both inputs move, with one reason per axis."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    write_shoe_orders(data_dir(project) / "widgets.parquet", n_rows=120, seed=1)
    catalog_create("base", _base_code(project))
    mixed = _hash(catalog_create("mixed", _mixed_code("base", "widgets.parquet", project)))

    _hash(catalog_revise("base", _base_code_v2(project)))  # alias axis
    write_shoe_orders(data_dir(project) / "widgets.parquet", n_rows=120, seed=2)  # source axis

    v = entry_staleness(project, mixed)
    assert v.stale is True
    assert {r.axis for r in v.reasons} == {"alias", "source"}
    assert {r.ref for r in v.reasons if r.axis == "alias"} == {"base"}
    assert "widgets.parquet" in {r.ref for r in v.reasons if r.axis == "source"}


def test_cheap_parent_sources_inline_into_child_and_both_go_directly_stale(
    project, orders_parquet, monkeypatch
):
    """docs/reactive-recalc.md (cheap vs expensive parents): a cheap parent's
    recipe is inlined into the child, so the parent's raw reads land in the
    CHILD's own ``manifest.sources`` — a source edit makes the child *directly*
    stale on the source axis, not merely transitively. The doc also names the
    checkable invariant: a child's sources contain the union of its transitive
    cheap parents' sources."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_hash = _hash(catalog_create("base", _base_code(project)))
    child_hash = _hash(catalog_create("child", _child_code("base")))

    child_sources = sources_of(project, child_hash) or {}
    assert "orders.parquet" in child_sources
    assert (sources_of(project, base_hash) or {}).items() <= child_sources.items()

    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=1)

    verdicts = scan(project)
    assert verdicts[base_hash].stale is True
    assert verdicts[child_hash].stale is True  # DIRECTLY stale, not just carried
    assert {r.axis for r in verdicts[child_hash].reasons} == {"source"}


def test_expensive_parent_shields_child_from_source_axis(project, orders_parquet, monkeypatch):
    """The flip side: a child reading an EXPENSIVE parent reads its baked
    snapshot, so the parent's raw sources never enter the child's source set.
    A source edit leaves the child clean (only transitively stale)."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    base_hash = _hash(catalog_create("base", _base_code(project)))
    agg_hash = _hash(catalog_create("agg", _agg_child_code("base")))
    leaf_hash = _hash(catalog_create("leaf", _const_child_code("agg")))

    assert "orders.parquet" not in (sources_of(project, leaf_hash) or {})

    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=1)

    verdicts = scan(project)
    assert verdicts[base_hash].stale is True  # reads the file directly
    assert verdicts[agg_hash].stale is True  # cheap base inlines into agg
    assert verdicts[leaf_hash].stale is False  # shielded by agg's baked snapshot
    assert verdicts[leaf_hash].transitively_stale is True


def test_inplace_edit_preserving_mtime_and_size_is_detected(project, orders_parquet, monkeypatch):
    """docs/reactive-recalc.md: "The check forces a faithful re-read so an
    in-place content swap can't be masked by a cached digest." The source-digest
    memo is stat-keyed, so the masking edit is one that preserves mtime and byte
    length — exactly what this simulates."""
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    monkeypatch.delenv("TALLYMAN_SOURCE_REHASH", raising=False)  # the doc claim, not the escape hatch
    base_hash = _hash(catalog_create("base", _base_code(project)))

    src = data_dir(project) / "orders.parquet"
    st = src.stat()
    raw = bytearray(src.read_bytes())
    raw[len(raw) // 2] ^= 0xFF  # same length, different bytes
    src.write_bytes(bytes(raw))
    os.utime(src, ns=(st.st_atime_ns, st.st_mtime_ns))  # mask the edit from stat()
    assert src.stat().st_mtime_ns == st.st_mtime_ns and src.stat().st_size == st.st_size

    v = entry_staleness(project, base_hash)
    assert v.stale is True
    assert [r.axis for r in v.reasons] == ["source"]
