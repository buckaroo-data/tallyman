"""ADR-007: tallyman owns result materialization (no xorq cache nodes in builds).

Covers D1 (builds carry no cache nodes), D2 (a snapshot's location is a function of the content hash), D3 (chaining
through a worthy parent is a bare read of its snapshot), D4 (one writer, used by the build and by every heal), D5
(``ensure_materialized`` makes files exist before anything runs), D7 (the cold state is an empty ``compute_cache``),
D8 (a sentinel keeps xorq's cache directory empty) and D13 (a file is cache only if ``ensure_materialized`` can
re-create it). ``plans/ADR-007-tallyman-owned-materialization.md`` has the decisions; the shared API is fixed by the
lead's contract, so the names imported lazily below do not exist yet and these tests are red until they do.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_companion.diff import build_diff_expr
from tallyman_core import data_dir, entry_dir
from tallyman_core.manifest import read_manifest
from tallyman_core.paths import compute_cache_dir, tallyman_home
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq import result_cache
from tallyman_xorq.build import BuildError, build_and_persist
from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr, snapshot_file_digest

# --------------------------------------------------------------------------- #
# recipes
# --------------------------------------------------------------------------- #


def _agg_code(project: str) -> str:  # an Aggregate: worthy, so it has a snapshot
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _boots_agg_code(project: str) -> str:  # a second version of the same aggregate, over fewer rows
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
b = t.filter(t.category == "boots")
expr = b.group_by("region").aggregate(total=b.price.sum(), n=b.count())
"""


def _cheap_child_code(parent: str) -> str:  # a filter and a computed column over a worthy parent's result
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.filter(t.n > 0).mutate(share=t.total / t.n)
"""


def _worthy_grandchild_code(parent: str) -> str:  # an Aggregate over the cheap child
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.aggregate(total_share=t.share.sum(), rows=t.count())
"""


def _root_code(project: str) -> str:  # a bare read of a source: a cheap root entry
    return f"""
from tallyman_xorq.io import read_project_file
expr = read_project_file("orders.parquet", project={project!r})
"""


def _root_child_code(alias: str) -> str:  # a cheap child over a cheap root, so it inlines the root's graph
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({alias!r})
expr = t.filter(t.qty > 1)
"""


def _hash(res: dict) -> str:
    assert "error" not in res, res
    return res["hash"]


def _digest_of(project: str, content_hash: str) -> str | None:
    return read_manifest(entry_dir(project, content_hash)).result_digest


def _build_yaml(project: str, content_hash: str) -> str:
    return (entry_dir(project, content_hash) / "xorq_build" / "expr.yaml").read_text()


@pytest.fixture(autouse=True)
def _no_cascade(monkeypatch):
    """These tests revise aliases to get a second version; keep the cascade out of them."""
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")


# --------------------------------------------------------------------------- #
# D1: builds carry no cache nodes
# --------------------------------------------------------------------------- #


def test_a_recipe_that_calls_cache_is_a_build_error(project, orders_parquet):
    """ADR-007 D1 (builds carry no cache nodes): a ``CachedNode`` in a recipe would write under ~/.cache/xorq."""
    code = f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select("region", "price").cache()
"""
    with pytest.raises(BuildError, match="cache"):
        build_and_persist(project, code)


def test_a_build_carries_no_cache_node(project, orders_parquet, monkeypatch):
    """ADR-007 D1: neither a worthy entry nor its chained child has a ``CachedNode`` in its frozen build."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    parent = _hash(catalog_create("agg", _agg_code(project)))
    child = _hash(catalog_create("child", _cheap_child_code("agg")))
    for h in (parent, child):
        assert "CachedNode" not in _build_yaml(project, h)


def test_the_xorq_cache_machinery_is_gone():
    """ADR-007 Consequences (retired): the code whose only job was to aim xorq's cache is deleted."""
    import tallyman_xorq.portable as portable
    import tallyman_xorq.source_cache as source_cache
    from tallyman_core.manifest import Manifest

    assert not hasattr(portable, "rewrite_cache_dirs")
    for name in ("_cached_node_path", "_assert_recorded_snapshot_key", "entry_graph_expr", "classify_build"):
        assert not hasattr(result_cache, name), name
    assert not hasattr(source_cache, "_is_worthy_expr")
    assert "snapshot_key" not in Manifest.model_fields


# --------------------------------------------------------------------------- #
# D2: a snapshot's location is a function of the content hash
# --------------------------------------------------------------------------- #


def test_the_snapshot_path_is_derived_from_the_content_hash(project, orders_parquet, monkeypatch):
    """ADR-007 D2 (snapshot location): ``compute_cache/result_cache/<content_hash>.parquet``, written at create."""
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _hash(catalog_create("agg", _agg_code(project)))
    expected = compute_cache_dir(project) / "result_cache" / f"{h}.parquet"
    assert snapshot_path(project, h) == expected
    assert expected.is_file()
    assert baked_snapshot_path(project, h) == expected


def test_the_manifest_records_no_snapshot_key(project, orders_parquet, monkeypatch):
    """ADR-007 D2: with the path computed from the hash there is no second derivation for a tripwire to compare."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _hash(catalog_create("agg", _agg_code(project)))
    assert "snapshot_key" not in json.loads((entry_dir(project, h) / "manifest.json").read_text())


def test_a_worthy_read_is_one_memoised_bare_read_of_the_snapshot(project, orders_parquet, monkeypatch):
    """ADR-007 D2: ``cached_result_expr`` is one bare read of the snapshot, memoised, and loads no build."""
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _hash(catalog_create("agg", _agg_code(project)))
    cached_result_expr.cache_clear()

    def _no_build_load(*a, **k):
        raise AssertionError("a worthy entry whose snapshot exists must be served without loading its build")

    monkeypatch.setattr(result_cache, "load_entry_expr", _no_build_load)
    first = cached_result_expr(project, h)
    assert type(first.op()).__name__ == "Read"
    assert str(snapshot_path(project, h)) in str(dict(first.op().read_kwargs).get("hash_path"))
    assert cached_result_expr(project, h) is first  # one read has one table name


# --------------------------------------------------------------------------- #
# D3: chaining through a worthy parent is a bare read of its snapshot
# --------------------------------------------------------------------------- #


def test_a_child_of_a_worthy_parent_reads_the_parents_snapshot_and_is_cheap(project, orders_parquet, monkeypatch):
    """ADR-007 D3 (chaining is a bare read): a filter over an aggregate's snapshot is cheap, not a second copy."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    parent = _hash(catalog_create("agg", _agg_code(project)))
    child = _hash(catalog_create("child", _cheap_child_code("agg")))
    yaml_text = _build_yaml(project, child)
    assert f"result_cache/{parent}.parquet" in yaml_text
    assert "op: Aggregate" not in yaml_text
    manifest = read_manifest(entry_dir(project, child))
    assert manifest.cache_worthy is False
    assert baked_snapshot_path(project, child) is None


def test_a_child_hash_follows_the_parents_snapshot_path_not_its_bytes(project, orders_parquet, monkeypatch):
    """ADR-007 D3: content identity goes in the path. Other bytes at the same path keep the child's hash; a new
    parent (a new path) changes it."""
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    parent = _hash(catalog_create("agg", _agg_code(project)))
    child = _hash(catalog_create("child", _cheap_child_code("agg")))

    snap = snapshot_path(project, parent)
    table = pq.read_table(snap)
    tampered = table.set_column(table.schema.get_field_index("total"), "total", pa.array([1.0] * table.num_rows))
    pq.write_table(tampered, snap)
    cached_result_expr.cache_clear()
    assert build_and_persist(project, _cheap_child_code("agg")).content_hash == child

    catalog_revise("agg", _boots_agg_code(project))  # a new parent entry, so a new snapshot path
    assert build_and_persist(project, _cheap_child_code("agg")).content_hash != child


# --------------------------------------------------------------------------- #
# D4: one writer
# --------------------------------------------------------------------------- #


def test_a_create_replaces_a_snapshot_that_is_already_on_disk(project, orders_parquet):
    """ADR-007 D4 (one writer) and D14: a create never looks for the file, it runs the query and replaces it."""
    from tallyman_xorq.materialize import snapshot_path

    code = _agg_code(project)
    first = build_and_persist(project, code)
    snap = snapshot_path(project, first.content_hash)
    good = snapshot_file_digest(snap)
    shutil.rmtree(entry_dir(project, first.content_hash))  # the entry goes (a reset), its file stays
    snap.write_bytes(b"this is not parquet")

    again = build_and_persist(project, code)
    assert again.content_hash == first.content_hash
    assert snapshot_file_digest(snap) == good


def test_materialize_leaves_one_file_and_no_temp_files(project, orders_parquet):
    """ADR-007 D4: a unique temp name in the destination directory, then ``os.replace``."""
    from tallyman_xorq.materialize import materialize, snapshot_path

    h = build_and_persist(project, _agg_code(project)).content_hash
    result = materialize(project, h)
    snap = snapshot_path(project, h)
    assert result.path == snap
    assert result.digest == _digest_of(project, h)
    assert sorted(p.name for p in snap.parent.iterdir()) == [snap.name]


def test_a_failed_materialization_keeps_the_previous_file(project, orders_parquet, monkeypatch):
    """ADR-007 D4: the destination only ever changes by an atomic replace of a complete file."""
    from tallyman_xorq.materialize import materialize, snapshot_path

    h = build_and_persist(project, _agg_code(project)).content_hash
    snap = snapshot_path(project, h)
    before = snap.read_bytes()

    def _boom(self, *args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pq.ParquetWriter, "write_table", _boom)
    with pytest.raises(OSError, match="disk full"):
        materialize(project, h)
    assert snap.read_bytes() == before
    assert sorted(p.name for p in snap.parent.iterdir()) == [snap.name]


# --------------------------------------------------------------------------- #
# D5: ensure_materialized
# --------------------------------------------------------------------------- #


def test_ensure_materialized_rewrites_a_missing_snapshot_and_verifies_it(project, orders_parquet):
    """ADR-007 D5 (``ensure_materialized``): a deleted snapshot is re-created and checked against the manifest."""
    from tallyman_xorq.materialize import ensure_materialized, snapshot_path

    h = build_and_persist(project, _agg_code(project)).content_hash
    snap = snapshot_path(project, h)
    snap.unlink()
    ensure_materialized(project, h)
    assert snap.is_file()
    recorded = _digest_of(project, h)
    assert recorded and recorded.startswith("arrow-sha256:")
    assert snapshot_file_digest(snap) == recorded


def test_files_exist_before_anything_runs(project, orders_parquet, monkeypatch):
    """ADR-007 D5: with an ancestor's snapshot deleted, opening a descendant rewrites the ancestor first, so no plan
    is ever executed over a file that is missing. This is #76's reproduction."""
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    parent = _hash(catalog_create("agg", _agg_code(project)))
    child = _hash(catalog_create("child", _cheap_child_code("agg")))
    snapshot_path(project, parent).unlink()
    cached_result_expr.cache_clear()

    expr = cached_result_expr(project, child)  # composing, not executing
    assert snapshot_path(project, parent).is_file()
    assert snapshot_file_digest(snapshot_path(project, parent)) == _digest_of(project, parent)
    assert len(expr.execute()) > 0


def test_building_a_child_rewrites_a_deleted_parent_snapshot_first(project, orders_parquet, monkeypatch):
    """ADR-007 D3 and D5: chaining at mint time makes the parent's file exist, since ``build_expr`` of a child fails
    while the parent's snapshot is absent."""
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    parent = _hash(catalog_create("agg", _agg_code(project)))
    snapshot_path(project, parent).unlink()
    cached_result_expr.cache_clear()

    child = build_and_persist(project, _cheap_child_code("agg")).content_hash
    assert snapshot_path(project, parent).is_file()
    assert f"result_cache/{parent}.parquet" in _build_yaml(project, child)


def test_composing_a_diff_rewrites_both_deleted_snapshots_first(project, orders_parquet, monkeypatch):
    """ADR-007 D5 and D10 (diffs: what this set still does): both sides are read through ``cached_result_expr``, so
    both files exist before the join is composed, and the diff carries no row-order column from either side."""
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    a = _hash(catalog_create("agg", _agg_code(project)))
    b = _hash(catalog_revise("agg", _boots_agg_code(project)))
    for h in (a, b):
        snapshot_path(project, h).unlink()
    cached_result_expr.cache_clear()

    diff = build_diff_expr(a, b, keys=["region"])
    for h in (a, b):
        assert snapshot_path(project, h).is_file()
        assert snapshot_file_digest(snapshot_path(project, h)) == _digest_of(project, h)
    # The inputs carry the column, so the absence below is a decision and not an accident.
    assert cached_result_expr(project, a).columns[-1] == "__row_order"
    assert cached_result_expr(project, b).columns[-1] == "__row_order"
    assert not [c for c in diff.columns if c.startswith("__row_order")], list(diff.columns)
    assert len(diff.execute()) > 0


# --------------------------------------------------------------------------- #
# D7: the cold state is an empty compute_cache
# --------------------------------------------------------------------------- #


def test_an_empty_compute_cache_reproduces_every_snapshot_an_entry_needs(project, orders_parquet, monkeypatch):
    """ADR-007 D7 (the cold seam): with ``compute_cache/`` removed the canonical read reproduces every snapshot the
    entry needs, each with its recorded digest, ordered copies of sources included."""
    from tallyman_xorq.materialize import snapshot_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    parent = _hash(catalog_create("agg", _agg_code(project)))
    child = _hash(catalog_create("child", _cheap_child_code("agg")))
    grandchild = _hash(catalog_create("grand", _worthy_grandchild_code("child")))
    assert child != parent
    recorded = {h: _digest_of(project, h) for h in (parent, grandchild)}
    assert all(d and d.startswith("arrow-sha256:") for d in recorded.values())

    shutil.rmtree(compute_cache_dir(project))
    cached_result_expr.cache_clear()
    assert len(cached_result_expr(project, grandchild).execute()) == 1

    for h, digest in recorded.items():
        assert snapshot_file_digest(snapshot_path(project, h)) == digest, h


# --------------------------------------------------------------------------- #
# D8: a sentinel keeps xorq's cache directory empty
# --------------------------------------------------------------------------- #


class _AliveProc:
    def poll(self):
        return None


def _fake_buckaroo():
    """A ``BuckarooManager`` whose subprocess is 'alive' and whose HTTP client accepts every ``/load_expr``."""
    from tallyman_companion.buckaroo_lifecycle import BuckarooManager

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        return httpx.Response(200, json={"session": body.get("session") or "s"})

    bk = BuckarooManager()
    bk.proc = _AliveProc()
    bk.bound_port = 8799
    bk._maybe_restart = lambda: None
    bk._client = httpx.Client(transport=httpx.MockTransport(handler))
    return bk


def _xorq_cache_files() -> set[str]:
    # tests/conftest.py points the whole session at one XORQ_CACHE_DIR (xorq freezes it at first import), so the
    # test compares the directory before and after instead of asserting it is empty.
    root = Path(os.environ["XORQ_CACHE_DIR"])
    return {str(p) for p in root.rglob("*") if p.is_file()}


def test_xorq_cache_directory_stays_untouched(project, orders_parquet, monkeypatch):
    """ADR-007 D8 (the sentinel): a build, a chained child build, a view, a delete and a reopen write nothing under
    xorq's cache directory. Fails on main: a child build re-executes its parent into ``~/.cache/xorq``."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    before = _xorq_cache_files()

    parent = _hash(catalog_create("agg", _agg_code(project)))
    child = _hash(catalog_create("child", _cheap_child_code("agg")))
    assert len(cached_result_expr(project, child).execute()) > 0
    _fake_buckaroo().load_session(parent, project)  # the view of a worthy entry
    baked_snapshot_path(project, parent).unlink()  # the user deletes the file...
    cached_result_expr.cache_clear()
    assert len(cached_result_expr(project, parent).execute()) > 0  # ...and reopens the entry
    _fake_buckaroo().load_session(child, project)

    assert _xorq_cache_files() == before


# --------------------------------------------------------------------------- #
# D13: a file is cache only if ensure_materialized can re-create it
# --------------------------------------------------------------------------- #


def _ordered_copies(project: str) -> list[Path]:
    return sorted((compute_cache_dir(project) / "ordered_sources").glob("*.parquet"))


def _clones(project: str) -> list[Path]:
    cas = data_dir(project) / ".cas"
    return sorted(cas.iterdir()) if cas.is_dir() else []


def _rows(project: str, content_hash: str) -> list[dict]:
    df = cached_result_expr(project, content_hash).execute().sort_values("__row_order")
    return json.loads(df.to_json(orient="records"))


def test_the_ordered_copy_lives_under_the_compute_cache_and_is_recorded(project, orders_parquet, monkeypatch):
    """ADR-007 D13 (ordered copies are cache): they live under ``compute_cache/``, ``csv_ordered/`` is retired, and
    the manifest records what ``ensure_materialized`` needs to make one again."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _hash(catalog_create("orders", _root_code(project)))
    copies = _ordered_copies(project)
    assert len(copies) == 1
    assert not (tallyman_home() / "csv_ordered").exists()
    record = read_manifest(entry_dir(project, h)).ordered_copies[copies[0].stem]
    assert record["source"] == "orders.parquet"
    assert record["reader"]["kind"] == "parquet"
    assert record["digest"] == read_manifest(entry_dir(project, h)).sources["orders.parquet"]
    assert record["content_digest"] == snapshot_file_digest(copies[0])


def test_a_deleted_ordered_copy_is_made_again_from_the_clone_and_checked(project, orders_parquet, monkeypatch):
    """ADR-007 D13 and D5: opening a root entry re-creates its ordered copy from the clone."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _hash(catalog_create("orders", _root_code(project)))
    [copy] = _ordered_copies(project)
    digest = snapshot_file_digest(copy)
    rows = _rows(project, h)
    copy.unlink()
    cached_result_expr.cache_clear()

    assert _rows(project, h) == rows
    assert snapshot_file_digest(copy) == digest


def test_a_deleted_clone_is_made_again_from_the_unchanged_live_source(project, orders_parquet, monkeypatch):
    """ADR-007 D13: a clone can be copied again from the live source while the live bytes still hash to its name."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _hash(catalog_create("orders", _root_code(project)))
    [copy] = _ordered_copies(project)
    [clone] = _clones(project)
    rows = _rows(project, h)
    copy.unlink()
    clone.unlink()
    cached_result_expr.cache_clear()

    assert _rows(project, h) == rows
    assert clone.is_file() and copy.is_file()


def test_a_child_that_inlines_a_cheap_parent_can_make_the_ordered_copy_again(project, orders_parquet, monkeypatch):
    """ADR-007 D5 and D13: a child's build reads the parent's ordered copy directly, so the child's manifest carries
    the record that lets it be made again."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _hash(catalog_create("orders", _root_code(project)))
    child = _hash(catalog_create("multi", _root_child_code("orders")))
    [copy] = _ordered_copies(project)
    [clone] = _clones(project)
    rows = _rows(project, child)
    copy.unlink()
    clone.unlink()
    cached_result_expr.cache_clear()

    assert _rows(project, child) == rows
    assert copy.is_file()


def test_when_nothing_can_make_a_file_again_the_error_names_the_source(project, orders_parquet, monkeypatch):
    """ADR-007 D13: the clone is gone and the live source has changed, so the built rows are unrecoverable and the
    error names the source file."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    h = _hash(catalog_create("orders", _root_code(project)))
    [copy] = _ordered_copies(project)
    [clone] = _clones(project)
    copy.unlink()
    clone.unlink()
    write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=37, seed=99)  # the live source changed
    cached_result_expr.cache_clear()

    with pytest.raises(Exception, match=r"orders\.parquet"):
        cached_result_expr(project, h)


def test_a_csv_ordered_copy_is_made_again_with_the_recorded_reader_options(project, monkeypatch):
    """ADR-007 D13: the manifest records each source's reader options, so a CSV copy is re-created with the schema and
    the ``scan_csv`` options it was first read with."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    csv = data_dir(project) / "sample.csv"
    csv.write_text("id;name;value\n3;charlie;30\n1;alice;10\n2;bob;20\n")
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tallyman_read_csv
schema = ibis.schema({{"id": "int64", "name": "string", "value": "int64"}})
expr = tallyman_read_csv({str(csv)!r}, schema=schema, separator=";")
"""
    h = build_and_persist(project, code).content_hash
    [copy] = _ordered_copies(project)
    record = read_manifest(entry_dir(project, h)).ordered_copies[copy.stem]
    assert record["reader"]["kind"] == "csv"
    assert record["reader"]["scan_kwargs"] == {"separator": ";"}
    rows = _rows(project, h)
    assert [r["id"] for r in rows] == [3, 1, 2]  # file order, not sorted

    copy.unlink()
    [clone] = _clones(project)
    clone.unlink()
    cached_result_expr.cache_clear()
    assert _rows(project, h) == rows
    assert snapshot_file_digest(copy) == record["content_digest"]
