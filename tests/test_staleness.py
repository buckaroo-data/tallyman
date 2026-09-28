from __future__ import annotations

from tallyman_core.aliases import get_alias
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.staleness import entry_staleness, scan


def _base_code(project: str) -> str:  # root over the imported orders source
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.select("region", "price", "__row_order")
"""


def _base_code_v2(project: str) -> str:  # a different graph → a new content hash
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.select("region", "price", "__row_order").mutate(extra=1)
"""


def _child_code(alias: str) -> str:  # tracked child off an alias
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({alias!r})
expr = t.mutate(doubled=t.price * 2)
"""


def _pinned_child_code(ref: str) -> str:  # pinned (no lineage) child off one version of an alias
    return f"""
from tallyman_xorq.io import pinned_expr_from_alias
t = pinned_expr_from_alias({ref!r})
expr = t.mutate(doubled=t.price * 2)
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


def test_followed_alias_advance_marks_child_stale(project, orders_src, monkeypatch):
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


def test_hash_pinned_parent_is_not_stale_when_alias_advances(project, orders_src, monkeypatch):
    base_v1 = _hash(catalog_create("base", _base_code(project)))
    # A version reference is recorded follow=False (a pin), so the alias
    # advancing must not make this child stale.
    child_hash = _hash(catalog_create("child", _pinned_child_code("base-v1")))

    _hash(catalog_revise("base", _base_code_v2(project)))
    assert get_alias(project, "base") != base_v1

    v = entry_staleness(project, child_hash)
    assert v.stale is False
    assert [r for r in v.reasons if r.axis == "alias"] == []


def test_scan_distinguishes_direct_from_transitive_staleness(project, orders_src, monkeypatch):
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


# ---------------------------------------------------------------------------
# ADR-011 D6 — staleness has one axis. An entry is stale when a followed alias
# has moved, and that is the only reason: the source axis, and the digests it
# took of files outside the arena, are gone.
# ---------------------------------------------------------------------------


def _import(project: str, path, alias: str) -> dict:
    from tallyman_xorq.source_import import update_and_depend

    return update_and_depend(path, alias, project=project)


def test_a_scan_has_one_axis_and_digests_no_file(project, tmp_path, monkeypatch):
    """Three imported sources and two children: the scan reads aliases and manifests, and nothing else.

    The digest counter is its own positive control — each import takes two digests, one of the file as
    given and one of the clone as written (D9) — so a counter that stays at zero across the scan is a
    measurement that the scan took none, not an assertion that the code looks like it wouldn't.
    ``manifest.sources`` and the ``unknown`` source axis go with it: a verdict never carries a source
    reason, and never reports an axis it could not evaluate.
    """
    import pandas as pd

    from tallyman_xorq import source_identity as si
    from tallyman_xorq import staleness

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    outside = tmp_path / "outside"
    outside.mkdir(parents=True, exist_ok=True)

    digested: list[str] = []
    real = si._digest_file

    def counting_digest(path):
        digested.append(str(path))
        return real(path)

    monkeypatch.setattr(si, "_digest_file", counting_digest)

    for alias in ("orders", "orders_b", "orders_c"):
        frame = pd.DataFrame({"region": ["n", "s", "e"], "price": [1.0, 2.0, 3.0], "who": [alias] * 3})
        frame.to_parquet(outside / f"{alias}.parquet")
        _import(project, outside / f"{alias}.parquet", alias)
    catalog_create("totals", _agg_child_code("orders"))
    catalog_create("flagged", _const_child_code("orders_b"))
    assert len(digested) == 6, f"three imports, each digesting the file and then its clone; got {digested}"

    digested.clear()
    verdicts = scan(project)

    assert digested == [], f"a staleness scan must digest nothing; it digested {digested}"
    assert not hasattr(si, "digest_for"), "the stat-memoized digest went with source_digests.json (D6)"
    assert not hasattr(staleness, "_force_source_rehash"), "nothing forces a rehash; nothing hashes"
    for content_hash, verdict in verdicts.items():
        assert verdict.unknown_axes == [], (content_hash, verdict.unknown_axes)
        assert [r.axis for r in verdict.reasons if r.axis != "alias"] == [], (content_hash, verdict.reasons)
