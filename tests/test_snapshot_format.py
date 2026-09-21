"""ADR-009: the snapshot's format, single-partition materialization, and the create-twice reproducibility check.

Decisions covered, all from ``plans/ADR-009-digest-stability.md``:

- D1 (materialization runs single-partition): float aggregates heal to the digest they were built with, and the plan
  runs on a connection with ``target_partitions = 1`` that is not the process default.
- D3 (the snapshot's format): one row group is 1,048,576 rows, zstd, format 2.6, statistics and a page index, and
  ``__row_order`` is the last column. The ordered copy of a source has 122,880-row groups. ``schema.json`` is read from
  the file that was written.
- D4 (a mismatch record names its likely cause): the manifest records engine versions and the snapshot format
  version, and an unfaithful heal says "the engine changed" when it did.
- D6 (create runs the query twice and compares): a recipe that differs run to run is recorded as not reproducible and
  its file is pinned; a heal runs the query once.

The digest definition itself (D2) is in ``tests/test_digest.py``.

Names that do not exist yet (``tallyman_xorq.materialize`` and the manifest and result fields the contract adds) are
imported inside the tests, so each test fails on its own when its name is missing. The tests that need a source above
DataFusion's 10,485,760-byte scan-split threshold share one 26 MB file (``big_source``) and, for the layout
assertions, one built entry (``big_built``).
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from xorq.config import default_backend

from tallyman_core import data_dir, ensure_project
from tallyman_core.errors import list_errors
from tallyman_core.manifest import read_manifest
from tallyman_core.paths import compute_cache_dir, entry_dir, entry_schema_path
from tallyman_xorq.build import build_and_persist
from tallyman_xorq.result_cache import (
    UNFAITHFUL_HEAL_HOOKS,
    baked_snapshot_path,
    cached_result_expr,
    snapshot_file_digest,
    verify_result_faithful,
)
from tests.big_parquet import write_big_parquet

PREFIX = "arrow-sha256:"
SNAPSHOT_ROW_GROUP = 1_048_576  # ADR-009 D3
ORDERED_COPY_ROW_GROUP = 122_880  # ADR-009 D3, io._CSV_PARQUET_WRITE
BIG_ROWS = 1_500_000  # tests/big_parquet.py default


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _materialize_module():
    import tallyman_xorq.materialize as materialize_module

    return materialize_module


def _show(backend, key: str) -> str:
    return str(backend.raw_sql(f"SHOW {key}").to_pandas().iloc[0, 1])


def _unfaithful(project: str) -> list[dict]:
    return [e for e in list_errors(project, limit=1_000_000) if e.get("code") == "unfaithful_heal"]


def _edit_manifest(project: str, content_hash: str, edit) -> None:
    path = entry_dir(project, content_hash) / "manifest.json"
    doc = json.loads(path.read_text())
    edit(doc)
    path.write_text(json.dumps(doc, indent=2))


def _evict(project: str, content_hash: str) -> Path:
    """Delete the entry's snapshot (what the Cache page's delete does) and drop the in-process memo."""
    snap = baked_snapshot_path(project, content_hash)
    assert snap is not None and snap.exists()
    snap.unlink()
    cached_result_expr.cache_clear()
    return snap


def _install(project: str, big_source: Path, name: str = "big.parquet") -> None:
    shutil.copy(big_source, data_dir(project) / name)


# ---------------------------------------------------------------------------
# recipes
# ---------------------------------------------------------------------------


def _agg_code(project: str) -> str:  # Aggregate: worthy, deterministic
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _order_by_code(project: str) -> str:  # a Sort over the 1.5M-row file: worthy, and more than one row group
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("big.parquet", project={project!r})
expr = t.order_by("id")
"""


def _float_agg_code(project: str, *, grouped: bool) -> str:
    aggregate = "t.group_by('g').aggregate" if grouped else "t.aggregate"
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("big.parquet", project={project!r})
expr = {aggregate}(s=t.v.sum(), m=t.v.mean())
"""


def _noisy_udf_code(project: str) -> str:
    """A scalar UDF that returns a different value on every call: the entry is worthy and not reproducible."""
    return f"""
from tallyman_xorq.io import read_project_file
from xorq.expr.udf import make_pandas_udf
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.vendor.ibis import schema as ibis_schema

t = read_project_file("orders.parquet", project={project!r})


def noisy(df):
    import numpy as np

    return df["price"] * 0.0 + np.random.random(len(df))


_udf = make_pandas_udf(noisy, ibis_schema({{"price": dt.float64}}), dt.float64, name="noisy")
expr = t.mutate(noise=_udf.on_expr(t))
"""


def _counting_udf_code(project: str, counter: Path) -> str:
    """A deterministic scalar UDF that appends one byte to ``counter`` per call, so a test can count executions.

    xorq pickles a UDF into the build, so a module-level counter would be a copy; a file is visible from outside.
    """
    return f"""
from tallyman_xorq.io import read_project_file
from xorq.expr.udf import make_pandas_udf
import xorq.vendor.ibis.expr.datatypes as dt
from xorq.vendor.ibis import schema as ibis_schema

COUNTER = {str(counter)!r}
t = read_project_file("orders.parquet", project={project!r})


def bump(df):
    with open(COUNTER, "ab") as fh:
        fh.write(b"x")
    return df["qty"] + 1


_udf = make_pandas_udf(bump, ibis_schema({{"qty": dt.int64}}), dt.int64, name="bump")
expr = t.mutate(qty_plus=_udf.on_expr(t))
"""


def _timestamp_code(project: str) -> str:
    """An expression whose type is ``timestamp[s]``, which parquet stores as ``timestamp[ms]``."""
    return f"""
from tallyman_xorq.io import read_project_file
import xorq.vendor.ibis.expr.datatypes as dt

t = read_project_file("events.parquet", project={project!r})
expr = t.mutate(ts0=t.ts.cast(dt.Timestamp(scale=0))).order_by("id")
"""


# ---------------------------------------------------------------------------
# fixtures: one big source for the module, one built entry for the layout assertions
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def big_source(tmp_path_factory) -> Path:
    return write_big_parquet(tmp_path_factory.mktemp("big_source") / "big.parquet")


@dataclass(frozen=True)
class _Built:
    home: Path
    project: str
    content_hash: str


@pytest.fixture(scope="module")
def _big_built(tmp_path_factory, big_source):
    """Build ``order_by("id")`` over the 1.5M-row source once, in a home of its own, for every layout test."""
    home = tmp_path_factory.mktemp("format_home")
    patch = pytest.MonkeyPatch()
    patch.setenv("TALLYMAN_HOME", str(home))
    try:
        ensure_project("fmt")
        _install("fmt", big_source)
        result = build_and_persist("fmt", _order_by_code("fmt"))
        yield _Built(home, "fmt", result.content_hash)
    finally:
        patch.undo()


@pytest.fixture
def big_built(_big_built, monkeypatch) -> _Built:
    monkeypatch.setenv("TALLYMAN_HOME", str(_big_built.home))
    return _big_built


def _snapshot(built: _Built) -> Path:
    snap = baked_snapshot_path(built.project, built.content_hash)
    assert snap is not None and snap.exists()
    return snap


def _row_group_sizes(path: Path) -> list[int]:
    md = pq.ParquetFile(path).metadata
    return [md.row_group(i).num_rows for i in range(md.num_row_groups)]


# ---------------------------------------------------------------------------
# D1: materialization is single-partition, so a float aggregate heals to the digest it was built with
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("grouped", [False, True], ids=["ungrouped SUM and AVG", "group_by g SUM and AVG"])
def test_float_aggregate_heals_to_the_digest_it_was_built_with(project, big_source, grouped):
    """ADR-009 D1 (materialization runs single-partition): three heals, three matching digests, no false alarm.

    DataFusion sums each partition separately and merges the partial sums in arrival order, and float addition is not
    associative, so on the default 14-partition connection every heal of this entry produced different low bits: a
    different file, an ``unfaithful_heal`` record and a stat-cache wipe each time. Both shapes are checked because the
    ungrouped accumulator and the grouped one merge differently.
    """
    _install(project, big_source)
    result = build_and_persist(project, _float_agg_code(project, grouped=grouped))
    h = result.content_hash
    recorded = read_manifest(entry_dir(project, h)).result_digest
    assert recorded, "a worthy entry records the digest of its snapshot"

    for attempt in range(3):
        snap = _evict(project, h)
        cached_result_expr(project, h)  # the heal
        assert snap.exists(), f"heal {attempt} did not write the snapshot back"
        assert snapshot_file_digest(snap) == recorded, f"heal {attempt} produced a different digest than the build"
    assert _unfaithful(project) == []
    assert recorded.startswith(PREFIX)  # ADR-009 D2: the recorded value is a content digest


# ---------------------------------------------------------------------------
# D1: the plan runs on a single-partition connection that is not the process default
# ---------------------------------------------------------------------------


def test_single_partition_backend_is_one_partition_with_a_pinned_batch_size():
    """ADR-009 D1: ``SET datafusion.execution.target_partitions = 1`` and an explicit ``batch_size``.

    ``batch_size`` decides the batch boundaries an ungrouped float total sees (#187), so it is part of the
    reproducibility contract along with the row-group size.
    """
    materialize_module = _materialize_module()
    assert materialize_module.SNAPSHOT_ROW_GROUP_ROWS == SNAPSHOT_ROW_GROUP
    backend = materialize_module.single_partition_backend()
    assert _show(backend, "datafusion.execution.target_partitions") == "1"
    assert _show(backend, "datafusion.execution.batch_size") == str(materialize_module.SNAPSHOT_BATCH_SIZE)
    assert materialize_module.single_partition_backend() is not backend, "each call is a fresh connection"
    if (os.cpu_count() or 1) > 1:
        assert _show(default_backend(), "datafusion.execution.target_partitions") != "1"


def test_materialize_runs_the_plan_on_the_single_partition_backend(project, orders_parquet, monkeypatch):
    """ADR-009 D1: the loaded build is rebound onto the single-partition connection, not left on its own backends.

    ``load_expr`` mints its own backend objects, so a loaded build ignores a single-partition connection it was never
    bound to. The spy records every connection ``materialize`` asks for. xorq registers a read's table on the backend
    it executes on, so a connection with tables afterwards is one the plan ran on. The process default backend must
    be left as it was.
    """
    materialize_module = _materialize_module()
    h = build_and_persist(project, _agg_code(project)).content_hash
    before = _show(default_backend(), "datafusion.execution.target_partitions")

    seen = []
    real = materialize_module.single_partition_backend

    def spy():
        backend = real()
        seen.append(backend)
        return backend

    monkeypatch.setattr(materialize_module, "single_partition_backend", spy)
    materialize_module.materialize(project, h)

    assert seen, "materialize never asked for a single-partition backend"
    assert all(_show(b, "datafusion.execution.target_partitions") == "1" for b in seen)
    assert any(b.list_tables() for b in seen), "the plan did not run on the single-partition backend"
    assert _show(default_backend(), "datafusion.execution.target_partitions") == before


# ---------------------------------------------------------------------------
# D3: the snapshot's format
# ---------------------------------------------------------------------------


def test_snapshot_row_groups_are_1048576_rows_with_a_short_last_group(big_built):
    """ADR-009 D3 (the snapshot's format): rows are regrouped into 1,048,576-row groups, not one group per batch.

    xorq's writer makes one Snappy row group per DataFusion batch (8,192 rows): 184 groups for this file, and a 3.68 GB
    snapshot in the corpus has 9,525 groups and a 44.9 MB footer that every page request opens.
    """
    sizes = _row_group_sizes(_snapshot(big_built))
    assert sizes[:-1] == [SNAPSHOT_ROW_GROUP] * (len(sizes) - 1), (len(sizes), sizes[:3])
    assert 0 < sizes[-1] <= SNAPSHOT_ROW_GROUP
    assert sum(sizes) == BIG_ROWS
    assert len(sizes) == 2


def test_snapshot_is_zstd_format_2_6_with_statistics_and_a_page_index(big_built):
    """ADR-009 D3 and ADR-008 D5: zstd level 3, parquet format 2.6, statistics on, and a parquet page index.

    The page index is what takes a range request on ``__row_order`` from about 90 ms to about 20 ms.
    """
    md = pq.ParquetFile(_snapshot(big_built)).metadata
    assert md.format_version == "2.6"
    for rg in range(md.num_row_groups):
        for col in range(md.num_columns):
            chunk = md.row_group(rg).column(col)
            where = f"row group {rg}, column {chunk.path_in_schema}"
            assert chunk.compression == "ZSTD", where
            assert chunk.statistics is not None and chunk.statistics.has_min_max, where
            assert chunk.has_offset_index and chunk.has_column_index, where


def test_snapshot_footer_is_small(big_built):
    """ADR-009 D3: 2 row groups and 4 columns is a footer of about a kilobyte, not 60 KB."""
    assert pq.ParquetFile(_snapshot(big_built)).metadata.serialized_size < 8_192


def test_snapshot_ends_in_row_order_numbered_from_zero_in_file_order(big_built):
    """ADR-009 D3 and ADR-008 D2: the writer's last column is ``__row_order``, int64, ``0..N-1`` in the file's order."""
    table = pq.read_table(_snapshot(big_built))
    assert table.schema.names == ["id", "g", "v", "__row_order"]
    assert table.schema.field("__row_order").type == pa.int64()
    n = table.num_rows
    assert n == BIG_ROWS
    assert (table["__row_order"].to_numpy() == np.arange(n)).all()
    # the recipe is order_by("id") over ids that are the source's positions, so file order and id order coincide
    assert (table["id"].to_numpy() == np.arange(n)).all()


def test_ordered_copy_of_a_source_has_row_groups_of_122880_rows(big_built):
    """ADR-009 D3 (the format version covers the ordered copies): polars writes them, in pinned 122,880-row groups.

    An ungrouped float total over a source reads that layout (#187), so it is as pinned as the snapshot's.
    """
    copies = sorted((compute_cache_dir(big_built.project) / "ordered_sources").glob("*.parquet"))
    assert copies, "reading a source should have written an ordered copy under compute_cache/ordered_sources/"
    for copy in copies:
        sizes = _row_group_sizes(copy)
        assert sizes[:-1] == [ORDERED_COPY_ROW_GROUP] * (len(sizes) - 1), (copy.name, len(sizes), sizes[:3])
        assert 0 < sizes[-1] <= ORDERED_COPY_ROW_GROUP
        assert pq.read_schema(copy).names[-1] == "__row_order"


def test_manifest_records_the_snapshot_format_version(project, orders_parquet):
    """ADR-009 D3: row-group size and ``batch_size`` are contract; the manifest records a version for both."""
    materialize_module = _materialize_module()
    result = build_and_persist(project, _agg_code(project))
    manifest = read_manifest(entry_dir(project, result.content_hash))
    assert manifest.snapshot_format == materialize_module.SNAPSHOT_FORMAT_VERSION


def test_materialize_returns_the_content_digest_of_the_file_it_wrote(project, orders_parquet):
    """ADR-009 D2 and ADR-007 D4 (one writer): the digest is computed by one function, from the file read back."""
    materialize_module = _materialize_module()
    h = build_and_persist(project, _agg_code(project)).content_hash
    manifest = read_manifest(entry_dir(project, h))

    out = materialize_module.materialize(project, h)

    assert out.path == baked_snapshot_path(project, h) == materialize_module.snapshot_path(project, h)
    assert out.digest == snapshot_file_digest(out.path)
    assert out.digest == manifest.result_digest
    assert out.row_count == manifest.row_count
    assert out.schema.names[-1] == "__row_order"


# ---------------------------------------------------------------------------
# D3: the recorded schema is the written file's, and every schema ends in __row_order
# ---------------------------------------------------------------------------


def test_recorded_schema_is_read_from_the_written_file(project):
    """ADR-009 D3: parquet changes some types on the way in, and the file is what every consumer reads.

    The expression's type is ``timestamp[s]``; parquet has no seconds unit, so the file holds ``timestamp[ms]``.
    """
    pq.write_table(
        pa.table(
            {
                "id": np.arange(100),
                "ts": pa.array(np.arange(100).astype("datetime64[s]")),
                "v": np.random.default_rng(1).random(100),
            }
        ),
        data_dir(project) / "events.parquet",
    )
    h = build_and_persist(project, _timestamp_code(project)).content_hash

    recorded = {f["name"]: f["type"] for f in json.loads(entry_schema_path(project, h).read_text())["fields"]}
    snap = baked_snapshot_path(project, h)
    assert snap is not None
    on_disk = {f.name: str(f.type) for f in pq.read_schema(snap)}

    assert on_disk["ts0"] == "timestamp[ms]"
    assert recorded == on_disk


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("expr = t.mutate(total=t.price * t.qty)", ["order_id", "region", "category", "price", "qty", "total"]),
        ("expr = t.group_by('region').aggregate(n=t.count())", ["region", "n"]),
        ("expr = t.order_by('price')", ["order_id", "region", "category", "price", "qty"]),
        ("expr = t.mutate(rn=ibis.row_number())", ["order_id", "region", "category", "price", "qty", "rn"]),
    ],
    ids=["cheap computed column", "aggregate", "sort", "window function"],
)
def test_every_recorded_schema_ends_in_row_order(project, orders_parquet, body, expected):
    """ADR-009 D3 and ADR-008 D2: cheap entries carry ``__row_order`` last, worthy ones get it from the writer.

    A worthy entry that keeps its parent's rows inherits ``__row_order`` mid-table and the writer replaces it with a
    fresh last column; a cheap entry's computed column must not push it into the middle (ADR-008 D3).
    """
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
{body}
"""
    h = build_and_persist(project, code).content_hash
    names = [f["name"] for f in json.loads(entry_schema_path(project, h).read_text())["fields"]]
    assert names == [*expected, "__row_order"]


# ---------------------------------------------------------------------------
# D4: engine versions, and an unfaithful heal that says so
# ---------------------------------------------------------------------------


def test_manifest_records_the_engine_versions(project, orders_parquet):
    """ADR-009 D4 (a mismatch record names its likely cause): xorq, xorq-datafusion and pyarrow at build."""
    result = build_and_persist(project, _agg_code(project))
    recorded = read_manifest(entry_dir(project, result.content_hash)).engine_versions
    assert recorded is not None
    for key, distribution in (("xorq", "xorq"), ("xorq_datafusion", "xorq-datafusion"), ("pyarrow", "pyarrow")):
        assert recorded[key] == version(distribution), key


@pytest.mark.parametrize("engine_changed", [True, False], ids=["engine version changed", "engine versions match"])
def test_an_unfaithful_heal_names_an_engine_change_instead_of_blaming_the_recipe(
    project, orders_parquet, engine_changed
):
    """ADR-009 D4: with an upgrade in play the record says so, and the loud response of ADR-006 D7 still happens.

    The recorded digest is replaced by one no heal can match, so the heal is unfaithful by construction. When the
    manifest's recorded xorq version is not the installed one, the message names the engine, not the recipe's
    "execution" or "structural" nondeterminism, which today it blames.
    """
    h = build_and_persist(project, _agg_code(project)).content_hash
    recorded_versions = dict(read_manifest(entry_dir(project, h)).engine_versions)

    def tamper(doc: dict) -> None:
        doc["result_digest"] = PREFIX + "0" * 64
        if engine_changed:
            doc["engine_versions"] = {**recorded_versions, "xorq": "0.0.1"}

    _edit_manifest(project, h, tamper)
    fired = []

    def hook(hook_project: str, hook_hash: str) -> None:
        fired.append((hook_project, hook_hash))

    UNFAITHFUL_HEAL_HOOKS.append(hook)
    try:
        _evict(project, h)
        cached_result_expr(project, h)
    finally:
        UNFAITHFUL_HEAL_HOOKS.remove(hook)

    records = _unfaithful(project)
    assert len(records) == 1 and records[0]["hash"] == h, records
    assert fired == [(project, h)]
    message = records[0]["message"]
    if engine_changed:
        assert "xorq" in message.lower(), message
        assert "0.0.1" in message or version("xorq") in message, message
        assert "execution (#83)" not in message and "structural (#88)" not in message, message


# ---------------------------------------------------------------------------
# D6: create runs the query twice and compares
# ---------------------------------------------------------------------------


def test_create_records_a_non_reproducible_recipe_and_names_the_column(project, orders_parquet):
    """ADR-009 D6 (create runs the query twice and compares): the build succeeds, the entry says it is not reproducible.

    A recipe that calls ``sample()`` is legitimate, so the build still succeeds. The columns whose per-column digests
    differ between the two runs are named, and the file is kept.
    """
    result = build_and_persist(project, _noisy_udf_code(project))
    assert result.reproducible is False
    assert result.nonreproducible_columns == ["noise"]

    manifest = read_manifest(entry_dir(project, result.content_hash))
    assert manifest.reproducible is False
    assert manifest.nonreproducible_columns == ["noise"]
    assert manifest.result_digest and manifest.result_digest.startswith(PREFIX)
    snap = baked_snapshot_path(project, result.content_hash)
    assert snap is not None and snap.exists()


def test_create_records_a_deterministic_recipe_as_reproducible(project, orders_parquet):
    """ADR-009 D6: a recipe that runs the same both times is recorded as reproducible, with no offending columns."""
    result = build_and_persist(project, _agg_code(project))
    assert result.reproducible is True
    assert result.nonreproducible_columns == []
    manifest = read_manifest(entry_dir(project, result.content_hash))
    assert manifest.reproducible is True
    assert not manifest.nonreproducible_columns


def test_a_cheap_entry_is_not_checked_for_reproducibility(project, orders_parquet):
    """ADR-009 D6: a cheap entry is not run twice here; it records no digest either (ADR-006 D9, no cheap digests)."""
    code = f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.mutate(total=t.price * t.qty)
"""
    result = build_and_persist(project, code)
    assert result.reproducible is None
    manifest = read_manifest(entry_dir(project, result.content_hash))
    assert manifest.reproducible is None
    assert manifest.result_digest is None


def test_create_runs_the_query_twice_and_a_heal_runs_it_once(project, orders_parquet, tmp_path):
    """ADR-009 D6: only a create runs the query twice, since a create has nothing recorded to compare against.

    A heal is compared against the recorded digest, so once is enough. The UDF appends a byte to a file on every call.
    """
    counter = tmp_path / "calls.bin"
    counter.write_bytes(b"")
    h = build_and_persist(project, _counting_udf_code(project, counter)).content_hash
    at_create = len(counter.read_bytes())

    counter.write_bytes(b"")
    snap = _evict(project, h)
    cached_result_expr(project, h)
    at_heal = len(counter.read_bytes())

    assert snap.exists()
    assert at_heal > 0
    assert at_create == 2 * at_heal, f"create called the UDF {at_create} times, a heal {at_heal}"


def test_materialize_runs_once_and_runs_twice_only_when_asked_to_check(project, orders_parquet, tmp_path):
    """ADR-009 D6 and ADR-007 D4: the checking run is an option of the one writer, not a second code path."""
    materialize_module = _materialize_module()
    counter = tmp_path / "calls.bin"
    counter.write_bytes(b"")
    h = build_and_persist(project, _counting_udf_code(project, counter)).content_hash

    counter.write_bytes(b"")
    materialize_module.materialize(project, h)
    once = len(counter.read_bytes())
    assert once > 0

    counter.write_bytes(b"")
    checked = materialize_module.materialize(project, h, check_reproducible=True)
    assert len(counter.read_bytes()) == 2 * once
    assert checked.reproducible is True
    assert checked.differing_columns == []


def test_materialize_names_the_columns_that_differ_between_the_two_runs(project, orders_parquet):
    """ADR-009 D6: ``differing_columns`` is what the build result reports to the author."""
    materialize_module = _materialize_module()
    h = build_and_persist(project, _noisy_udf_code(project)).content_hash
    checked = materialize_module.materialize(project, h, check_reproducible=True)
    assert checked.reproducible is False
    assert checked.differing_columns == ["noise"]


# ---------------------------------------------------------------------------
# verify: the digest a heal is checked against is the content digest
# ---------------------------------------------------------------------------


def test_verify_result_faithful_follows_the_content_not_the_bytes(project, orders_parquet):
    """ADR-009 D2 in the verify path: the same rows in another format are faithful, a changed value is not.

    ``verify_result_faithful`` runs on every heal and in the corpus sweep. Under a byte hash, rewriting the snapshot
    with a different codec and row-group size reads as drift; under the content digest it does not.
    """
    h = build_and_persist(project, _agg_code(project)).content_hash
    manifest = read_manifest(entry_dir(project, h))
    assert manifest.result_digest.startswith(PREFIX)
    assert verify_result_faithful(project, h) is True

    snap = baked_snapshot_path(project, h)
    assert snap is not None
    assert snapshot_file_digest(snap) == manifest.result_digest
    table = pq.read_table(snap)

    pq.write_table(table, snap, compression="snappy", row_group_size=2)
    assert verify_result_faithful(project, h) is True

    changed = table.set_column(table.schema.get_field_index("n"), "n", pc.add(table["n"], 1))
    pq.write_table(changed, snap)
    assert verify_result_faithful(project, h) is False


def test_verify_result_faithful_is_false_after_the_recorded_digest_is_tampered(project, orders_parquet):
    """ADR-009 D2: the recorded value is an ``arrow-sha256:`` digest, and a wrong one fails verification."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    assert read_manifest(entry_dir(project, h)).result_digest.startswith(PREFIX)
    assert verify_result_faithful(project, h) is True

    _edit_manifest(project, h, lambda doc: doc.update(result_digest=PREFIX + "0" * 64))
    assert verify_result_faithful(project, h) is False


# ---------------------------------------------------------------------------
# D6: a pinned file survives an explicit delete
# ---------------------------------------------------------------------------


def test_a_non_reproducible_entrys_snapshot_survives_an_explicit_delete(fresh_companion_app, project, orders_parquet):
    """ADR-009 D6: the Cache page's delete skips a file that cannot be recreated, and says why.

    A snapshot whose recipe is not reproducible would come back as different rows, and everything built on the
    original would then disagree with it. The listing flags the row as pinned.
    """
    h = build_and_persist(project, _noisy_udf_code(project)).content_hash
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    client = TestClient(fresh_companion_app)

    response = client.delete(f"/{project}/api/result_cache/{h}")

    assert response.status_code == 409, response.text
    assert "reproducible" in response.json()["detail"].lower()
    assert snap.exists()
    rows = {row["hash"]: row for row in client.get(f"/{project}/api/result_cache").json()["entries"]}
    assert rows[h]["pinned"] is True and rows[h]["pinned_reason"]


def test_an_entry_whose_heal_was_unfaithful_is_pinned_against_an_explicit_delete(
    fresh_companion_app, project, orders_parquet
):
    """ADR-009 D6 and ADR-006 D12 (unfaithful entries are pinned and badged): the same pin, reached after the fact."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    _edit_manifest(project, h, lambda doc: doc.update(result_digest=PREFIX + "0" * 64))
    _evict(project, h)
    cached_result_expr(project, h)  # an unfaithful heal: the durable record is the pin
    assert len(_unfaithful(project)) == 1
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()

    response = TestClient(fresh_companion_app).delete(f"/{project}/api/result_cache/{h}")

    assert response.status_code == 409, response.text
    assert snap.exists()
