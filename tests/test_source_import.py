"""Importing a file is how data enters the catalog (ADR-011).

A raw input is an alias whose versions are ordinary entries. ``update_and_depend`` copies the bytes into the arena
(the ``.cas`` clone store), writes ONE parquet snapshot carrying ``__row_order`` named by the source entry's content
hash, and points a source alias at that entry. A recipe then reads ``tracked_expr_from_alias("orders")`` like any
other parent, and the file the bytes came from is provenance: never read again.

These pin stage 1 of the ADR — the import path and the refusals (D1, D2, D3, D5, D9, D10, D12). The staleness source
axis (D6) and the source-identity modes (D8) are stage 2.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tallyman_core import data_dir, entry_dir, get_alias, history_for, read_manifest
from tallyman_xorq.materialize import snapshot_path

ROW_ORDER = "__row_order"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _outside(tmp_path: Path) -> Path:
    """A directory outside the project: an import takes any path (data/ is not special)."""
    d = tmp_path / "outside"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_parquet(path: Path, n_rows: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        {"region": [f"r{i % 3}" for i in range(n_rows)], "price": [float(i) for i in range(n_rows)]}
    )
    frame.to_parquet(path)
    return path


def _write_csv(path: Path, rows: list[tuple[str, int]], sep: str = ",") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = sep.join(("region", "n")) + "\n" + "\n".join(sep.join((r, str(n))) for r, n in rows) + "\n"
    path.write_text(body)
    return path


def _snapshot_frame(project: str, content_hash: str):
    import polars as pl

    return pl.read_parquet(str(snapshot_path(project, content_hash)))


def _child_code(alias: str, project: str) -> str:
    return (
        "from tallyman_xorq.io import tracked_expr_from_alias\n"
        f"t = tracked_expr_from_alias({alias!r}, project={project!r})\n"
        "expr = t.group_by('region').aggregate(total=t.price.sum())\n"
    )


# ---------------------------------------------------------------------------
# D3 — the full case table of update_and_depend
# ---------------------------------------------------------------------------


def test_absent_alias_mints_v1(project: str, tmp_path: Path, monkeypatch):
    """Row 1: the alias does not exist, so the import mints v1 and points the alias at it."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    out = source_import.update_and_depend(str(src), "orders")

    assert out["alias"] == "orders"
    assert out["version"] == 1
    assert out["created"] is True
    assert get_alias(project, "orders") == out["hash"]
    assert history_for(project, "orders") == [out["hash"]]
    assert entry_dir(project, out["hash"]).is_dir()


def test_same_bytes_is_a_noop(project: str, tmp_path: Path, monkeypatch):
    """Row 3: no pin and the bytes equal the head, so the head does not move and no entry is minted."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    first = source_import.update_and_depend(str(src), "orders")

    again = source_import.update_and_depend(str(src), "orders")

    assert again["hash"] == first["hash"]
    assert again["version"] == 1
    assert again["created"] is False
    assert history_for(project, "orders") == [first["hash"]]


def test_changed_bytes_mint_the_next_version(project: str, tmp_path: Path, monkeypatch):
    """Row 2: no pin and the bytes differ from the head, so the next version is minted and the head advances."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    v1 = source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 20)

    v2 = source_import.update_and_depend(str(src), "orders")

    assert v2["version"] == 2
    assert v2["created"] is True
    assert v2["hash"] != v1["hash"]
    assert history_for(project, "orders") == [v1["hash"], v2["hash"]]
    assert get_alias(project, "orders") == v2["hash"]


def test_pinned_version_with_matching_digest_is_a_noop(project: str, tmp_path: Path, monkeypatch):
    """Row 4: the pin names a version that exists and the bytes are that version's, so nothing happens."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    v1 = source_import.update_and_depend(str(src), "orders")

    again = source_import.update_and_depend(str(src), "orders", pinned_version=1)

    assert again["hash"] == v1["hash"]
    assert again["version"] == 1
    assert again["created"] is False


def test_pinned_version_with_a_different_digest_errors(project: str, tmp_path: Path, monkeypatch):
    """Row 5: the pin claims a version the file's bytes are not."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 20)

    with pytest.raises(source_import.SourceImportError) as exc:
        source_import.update_and_depend(str(src), "orders", pinned_version=1)

    assert "orders-v1" in str(exc.value)


def test_pinned_next_version_mints_it(project: str, tmp_path: Path, monkeypatch):
    """Row 6: the pin names head+1 and the bytes are new, so that version is minted."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 20)

    v2 = source_import.update_and_depend(str(src), "orders", pinned_version=2)

    assert v2["version"] == 2
    assert v2["created"] is True
    assert get_alias(project, "orders") == v2["hash"]


def test_pinned_version_beyond_next_errors(project: str, tmp_path: Path, monkeypatch):
    """Row 7: version numbers are consecutive, so a pin past head+1 is refused rather than leaving a gap."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 20)

    with pytest.raises(source_import.SourceImportError) as exc:
        source_import.update_and_depend(str(src), "orders", pinned_version=5)

    assert "skip" in str(exc.value).lower()


def test_pinned_older_version_returns_it_without_moving_the_head(project: str, tmp_path: Path, monkeypatch):
    """Row 8: the caller explicitly claims an older version and its bytes match, so that version is returned."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    v1 = source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 20)
    v2 = source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 10)  # back to v1's bytes

    out = source_import.update_and_depend(str(src), "orders", pinned_version=1)

    assert out["hash"] == v1["hash"]
    assert out["version"] == 1
    assert out["created"] is False
    assert get_alias(project, "orders") == v2["hash"], "an older pin must not move the head"


def test_bytes_matching_an_older_version_error_names_reset(project: str, tmp_path: Path, monkeypatch):
    """Row 9 / D11: history is append-only and monotonic, so restoring v1's bytes cannot mint a v3."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 20)
    source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 10)  # back to v1's bytes

    with pytest.raises(source_import.SourceImportError) as exc:
        source_import.update_and_depend(str(src), "orders")

    message = str(exc.value)
    assert "orders-v1" in message
    assert "reset" in message.lower()


def test_absent_alias_with_a_skipped_pin_errors(project: str, tmp_path: Path, monkeypatch):
    """A brand-new source alias starts at v1; a pin of v2 would leave version 1 undefined forever."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    with pytest.raises(source_import.SourceImportError) as exc:
        source_import.update_and_depend(str(src), "orders", pinned_version=2)

    assert "skip" in str(exc.value).lower()
    assert get_alias(project, "orders") is None


def test_an_import_names_its_own_project(project: str, tmp_path: Path, monkeypatch):
    """``project=`` is an override, so an import must not depend on which project happens to be active.

    The generated recipe's ``read_project_file`` is legal because a contextvar says which source entry
    is being minted, and the read has to resolve to the same project that contextvar names. Resolving
    the ambient active project instead makes the importer's own recipe hit the D2 refusal written for
    authored recipes, and the import fails with an error about a file tallyman does not own.
    """
    from tallyman_core import ensure_project, set_active_project
    from tallyman_xorq import source_import

    other = "other_project"
    ensure_project(other)
    set_active_project(project)  # the active project is NOT the one being imported into
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 6)

    out = source_import.update_and_depend(str(src), "orders", project=other)

    assert out["version"] == 1
    assert get_alias(other, "orders") == out["hash"]
    assert snapshot_path(other, out["hash"]).is_file()
    assert not snapshot_path(project, out["hash"]).exists()


def test_import_refuses_a_directory(project: str, tmp_path: Path, monkeypatch):
    """Multi-file datasets are open question 2 of the ADR; until one is designed a directory is refused clearly."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    d = _outside(tmp_path) / "parts"
    d.mkdir(parents=True, exist_ok=True)
    _write_parquet(d / "part-0000.parquet", 5)

    with pytest.raises(source_import.SourceImportError) as exc:
        source_import.update_and_depend(str(d), "orders")

    assert "director" in str(exc.value).lower()


def test_import_refuses_a_missing_path(project: str, tmp_path: Path, monkeypatch):
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    missing = _outside(tmp_path) / "nope.parquet"

    with pytest.raises(source_import.SourceImportError) as exc:
        source_import.update_and_depend(str(missing), "orders")

    assert "nope.parquet" in str(exc.value)


# ---------------------------------------------------------------------------
# D1 — a source entry is worthy and its snapshot IS the ordered copy
# ---------------------------------------------------------------------------


def test_the_snapshot_is_the_ordered_copy(project: str, tmp_path: Path, monkeypatch):
    """One parquet file, in ``result_cache/`` under the entry's content hash, carrying ``__row_order`` 0..N-1.

    ``compute_cache/ordered_sources/`` and the copy key go away for an import: the entry hash names the file.
    """
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 7)

    out = source_import.update_and_depend(str(src), "orders")

    snap = snapshot_path(project, out["hash"])
    assert snap.is_file()
    frame = _snapshot_frame(project, out["hash"])
    assert frame.columns[-1] == ROW_ORDER
    assert frame[ROW_ORDER].to_list() == list(range(7))

    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.materialize import RESULT_CACHE_DIRNAME

    subdirs = [d.name for d in compute_cache_dir(project).iterdir()]
    assert subdirs == [RESULT_CACHE_DIRNAME], f"an import writes no separate ordered copy: {subdirs}"
    assert read_manifest(entry_dir(project, out["hash"])).cache_worthy is True


def test_two_aliases_over_identical_bytes_share_one_entry(project: str, tmp_path: Path, monkeypatch):
    """The entry hash is a function of the bytes and the reader options, so one file serves both aliases."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    a = source_import.update_and_depend(str(src), "orders")
    b = source_import.update_and_depend(str(src), "orders_again")

    assert a["hash"] == b["hash"]
    assert get_alias(project, "orders") == get_alias(project, "orders_again")
    snaps = sorted(p.name for p in snapshot_path(project, a["hash"]).parent.glob("*.parquet"))
    assert snaps == [f"{a['hash']}.parquet"]


def test_a_source_version_keeps_its_raw_bytes_in_the_clone_store(project: str, tmp_path: Path, monkeypatch):
    """Open question 1, answered for stage 1: the imported bytes stay in ``.cas`` beside the snapshot.

    ADR-005's CSV suggestion-and-retry contract needs to re-read the file as imported, and the outside path may be
    gone by then.
    """
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    out = source_import.update_and_depend(str(src), "orders")

    clone = data_dir(project) / ".cas" / f"{out['digest']}.parquet"
    assert clone.is_file()
    assert clone.read_bytes() == src.read_bytes()


def test_the_provenance_path_is_recorded_and_never_read_again(project: str, tmp_path: Path, monkeypatch):
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    out = source_import.update_and_depend(str(src), "orders")

    provenance = read_manifest(entry_dir(project, out["hash"])).provenance
    assert provenance is not None
    assert provenance.path == str(src)
    assert provenance.alias == "orders"
    assert provenance.version == 1
    assert provenance.digest == out["digest"]


def test_editing_the_outside_file_after_import_changes_nothing(project: str, tmp_path: Path, monkeypatch):
    """The build reads the arena, so the file the bytes came from can change under it with no effect."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq import build_and_persist

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    source_import.update_and_depend(str(src), "orders")
    code = _child_code("orders", project)
    before = build_and_persist(project, code)

    _write_parquet(src, 999)
    after = build_and_persist(project, code)

    assert after.content_hash == before.content_hash
    assert after.row_count == before.row_count


def test_deleting_the_outside_file_after_import_changes_nothing(project: str, tmp_path: Path, monkeypatch):
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.result_cache import cached_result_expr

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    src.unlink()

    rows = cached_result_expr(project, out["hash"]).to_pyarrow().num_rows
    assert rows == 10


def test_a_child_of_a_source_alias_goes_stale_and_recalc_clears_it(project: str, tmp_path: Path, monkeypatch):
    """An import is a catalog operation (D7): it advances an alias, so followers go stale like any other revise."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    from tallyman_mcp.server import catalog_create
    from tallyman_xorq.recalc import recalc
    from tallyman_xorq.staleness import scan

    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    source_import.update_and_depend(str(src), "orders")
    child = catalog_create("totals", _child_code("orders", project))
    assert "error" not in child, child

    _write_parquet(src, 20)
    source_import.update_and_depend(str(src), "orders")

    verdicts = scan(project)
    child_hash = get_alias(project, "totals")
    assert verdicts[child_hash].stale is True
    assert [r.axis for r in verdicts[child_hash].reasons] == ["alias"]

    recalc(project, [child_hash])
    assert scan(project)[get_alias(project, "totals")].stale is False


def test_a_source_entry_reports_no_source_axis(project: str, tmp_path: Path, monkeypatch):
    """A source entry has no recorded source file, so the source axis is not applicable rather than unknown."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.staleness import entry_staleness

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")

    verdict = entry_staleness(project, out["hash"])
    assert verdict.stale is False
    assert verdict.unknown_axes == []


# ---------------------------------------------------------------------------
# D1 — pyarrow writes the snapshot, in the pinned layout
# ---------------------------------------------------------------------------
#
# ``compute_cache/result_cache/`` holds one shape of parquet file. polars cannot write it — it has no option for the
# parquet format version and none for the page index (pola-rs/polars#12752, open against 1.44.2) — so polars stays
# where it is genuinely needed, parsing a CSV in file order under the schema DSL of ADR-005, and its Arrow data goes
# to the writer every computed snapshot already uses.


def _layout(project: str, content_hash: str) -> dict:
    md = pq.ParquetFile(snapshot_path(project, content_hash)).metadata
    return {
        "format_version": md.format_version,
        "created_by": md.created_by,
        "row_group_rows": [md.row_group(i).num_rows for i in range(md.num_row_groups)],
        "page_index": all(
            md.row_group(i).column(c).has_offset_index and md.row_group(i).column(c).has_column_index
            for i in range(md.num_row_groups)
            for c in range(md.row_group(i).num_columns)
        ),
    }


def test_a_source_snapshot_is_written_by_pyarrow_in_the_pinned_layout(project: str, tmp_path: Path, monkeypatch):
    """The parquet format version, the page index and the row-group size of every other file in ``result_cache/``."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.ordered_copy import ORDERED_COPY_ROW_GROUP_ROWS

    rows = ORDERED_COPY_ROW_GROUP_ROWS + 5_000
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", rows)

    out = source_import.update_and_depend(str(src), "orders")

    layout = _layout(project, out["hash"])
    assert layout["format_version"] == "2.6", layout
    assert layout["created_by"].startswith("parquet-cpp-arrow"), layout
    assert layout["page_index"] is True, layout
    assert layout["row_group_rows"] == [ORDERED_COPY_ROW_GROUP_ROWS, 5_000], layout


def test_a_csv_source_snapshot_is_written_by_pyarrow_too(project: str, tmp_path: Path, monkeypatch):
    """polars parses the CSV; the bytes on disk are written by the same writer as a parquet source's."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [("east", 1), ("west", 2), ("east", 3)])

    out = source_import.update_and_depend(str(src), "orders")

    layout = _layout(project, out["hash"])
    assert layout["format_version"] == "2.6", layout
    assert layout["created_by"].startswith("parquet-cpp-arrow"), layout
    assert layout["page_index"] is True, layout
    frame = _snapshot_frame(project, out["hash"])
    assert frame["region"].to_list() == ["east", "west", "east"]
    assert frame[ROW_ORDER].to_list() == [0, 1, 2]


def test_a_source_snapshot_has_the_layout_of_a_computed_one(project: str, tmp_path: Path, monkeypatch):
    """Two files in one directory, written by one writer: the format version and the page index agree."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    from tallyman_mcp.server import catalog_create

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 20)
    out = source_import.update_and_depend(str(src), "orders")
    assert "error" not in catalog_create("totals", _child_code("orders", project))

    source = _layout(project, out["hash"])
    computed = _layout(project, get_alias(project, "totals"))
    assert source["format_version"] == computed["format_version"]
    assert source["created_by"] == computed["created_by"]
    assert source["page_index"] == computed["page_index"] is True


def test_importing_a_parquet_needs_no_polars(project: str, tmp_path: Path, monkeypatch):
    """A parquet source has its rows and its types in the file, so pyarrow reads it and nothing else is involved."""
    import polars as pl

    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    def boom(*a, **k):
        raise AssertionError("a parquet import read the file with polars")

    monkeypatch.setattr(pl, "scan_parquet", boom)
    monkeypatch.setattr(pl, "read_parquet", boom)

    out = source_import.update_and_depend(str(src), "orders")

    # Read back with pyarrow: polars is still patched out, and the point of the test is that it is not needed.
    written = pq.read_table(snapshot_path(project, out["hash"]))
    assert written[ROW_ORDER].to_pylist() == list(range(10))


def test_a_parquet_source_keeps_the_types_the_file_has(project: str, tmp_path: Path, monkeypatch):
    """polars rewrote a date as a timestamp, a time32 as a time64 and a map as a list of structs (#197)."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "typed.parquet"
    table = pa.table(
        {
            "d": pa.array([19_723, 19_724], type=pa.date32()),
            "tm": pa.array([1_000, 2_000], type=pa.time32("ms")),
            "m": pa.array([[("a", 1)], [("b", 2)]], type=pa.map_(pa.string(), pa.int64())),
            "x": pa.array([1.5, 2.5], type=pa.float64()),
        }
    )
    pq.write_table(table, src)

    out = source_import.update_and_depend(str(src), "typed")

    written = pq.read_schema(snapshot_path(project, out["hash"]))
    assert [(f.name, str(f.type)) for f in written] == [
        *[(f.name, str(f.type)) for f in pq.read_schema(src)],
        (ROW_ORDER, "int64"),
    ]


def test_a_csv_import_streams_into_the_pyarrow_writer(project: str, tmp_path: Path, monkeypatch):
    """A big source is the point of the row-group size, so the CSV goes batch by batch into pyarrow's writer.

    polars never collects the frame — patching ``collect`` out proves the rows arrive through ``collect_batches``,
    and the layout of the file proves they arrive at the writer this branch introduces rather than at polars' sink.
    """
    import polars as pl

    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [(f"r{i % 3}", i) for i in range(500)])

    def boom(*a, **k):
        raise AssertionError("the CSV import collected the whole frame into memory")

    monkeypatch.setattr(pl.LazyFrame, "collect", boom)

    out = source_import.update_and_depend(str(src), "orders")

    # Read back with pyarrow: polars' own reader collects, and the test has just patched that out.
    written = pq.read_table(snapshot_path(project, out["hash"]))
    assert written[ROW_ORDER].to_pylist() == list(range(500))
    assert _layout(project, out["hash"])["created_by"].startswith("parquet-cpp-arrow")


# ---------------------------------------------------------------------------
# D1 / ADR-007 D13 — the snapshot is CACHE, re-created from the clone
# ---------------------------------------------------------------------------
#
# The clone under ``data/.cas`` holds the bytes as imported and the entry records the reader options, so
# ``ensure_materialized`` CAN make the snapshot again — which is ADR-007 D13's whole test for whether a file is
# cache. Only when the clone is gone too is the version unrecoverable.


def _clone_of(project: str, out: dict) -> Path:
    return data_dir(project) / ".cas" / f"{out['digest']}{Path(out['path']).suffix}"


def test_ensure_materialized_recreates_a_source_snapshot_from_the_clone(project: str, tmp_path: Path, monkeypatch):
    """A deleted source snapshot comes back from the clone, with the rows it had."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.errors import list_errors
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    before = snapshot_path(project, out["hash"]).read_bytes()
    src.unlink()  # the outside file is gone: the clone is what re-creates the snapshot
    snapshot_path(project, out["hash"]).unlink()

    ensure_materialized(project, out["hash"])

    assert snapshot_path(project, out["hash"]).is_file()
    assert snapshot_path(project, out["hash"]).read_bytes() == before
    frame = _snapshot_frame(project, out["hash"])
    assert frame[ROW_ORDER].to_list() == list(range(10))
    assert [r for r in list_errors(project, limit=1000) if r.get("hash") == out["hash"]] == []


def test_a_csv_source_snapshot_is_recreated_under_the_reader_options_it_was_imported_with(
    project: str, tmp_path: Path, monkeypatch
):
    """The re-creation has only the manifest's record of the reader, which went through JSON (#198)."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_csv(_outside(tmp_path) / "orders.csv", [("east", 1), ("west", 2)], sep=";")
    schema = (("region", "string"), ("n", "float64"))
    out = source_import.update_and_depend(str(src), "orders", separator=";", schema=schema)
    before = snapshot_path(project, out["hash"]).read_bytes()
    src.unlink()
    snapshot_path(project, out["hash"]).unlink()

    ensure_materialized(project, out["hash"])

    assert snapshot_path(project, out["hash"]).read_bytes() == before
    frame = _snapshot_frame(project, out["hash"])
    assert frame.columns == ["region", "n", ROW_ORDER]
    assert frame["n"].to_list() == [1.0, 2.0]


def test_a_recreated_source_snapshot_is_verified_against_its_recorded_digest(
    project: str, tmp_path: Path, monkeypatch
):
    """Verified exactly as any other re-created snapshot is: a digest that is not the recorded one is recorded."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.errors import list_errors
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    # The clone is overwritten with other rows under the same digest name, so the re-creation cannot be faithful.
    other = _write_parquet(_outside(tmp_path) / "other.parquet", 4)
    _clone_of(project, out).write_bytes(other.read_bytes())
    snapshot_path(project, out["hash"]).unlink()

    ensure_materialized(project, out["hash"])

    codes = [r.get("code") for r in list_errors(project, limit=1000) if r.get("hash") == out["hash"]]
    assert "unfaithful_heal" in codes, codes


def test_a_source_snapshot_whose_clone_is_gone_names_the_re_import(project: str, tmp_path: Path, monkeypatch):
    """Only when the bytes are gone from the arena as well is a source version unrecoverable, and it says so."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    snapshot_path(project, out["hash"]).unlink()
    _clone_of(project, out).unlink()

    with pytest.raises(BuildError) as exc:
        ensure_materialized(project, out["hash"])

    message = str(exc.value)
    assert "orders-v1" in message
    assert "catalog_import_source" in message
    # It has to say WHICH file is missing: the snapshot alone is a cache miss, the clone as well is the loss.
    assert ".cas" in message, message
    assert out["digest"] in message, message
    assert not snapshot_path(project, out["hash"]).exists()


def test_a_child_reads_after_its_sources_snapshot_is_deleted(project: str, tmp_path: Path, monkeypatch):
    """The payoff: deleting ``result_cache/`` no longer leaves a source entry unreadable with the data right there."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "0")
    from tallyman_mcp.server import catalog_create
    from tallyman_xorq.result_cache import cached_result_expr

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 12)
    out = source_import.update_and_depend(str(src), "orders")
    child = catalog_create("totals", _child_code("orders", project))
    assert "error" not in child, child
    child_hash = get_alias(project, "totals")
    expected = cached_result_expr(project, child_hash).execute()

    for snap in snapshot_path(project, out["hash"]).parent.glob("*.parquet"):
        snap.unlink()
    cached_result_expr.cache_clear()

    assert len(cached_result_expr(project, child_hash).execute()) == len(expected)
    assert snapshot_path(project, out["hash"]).is_file()


def test_the_cache_page_offers_to_delete_a_source_snapshot(project: str, tmp_path: Path, monkeypatch):
    """A source snapshot is an ordinary deletable snapshot while its clone is on disk to make it again from."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.materialize import pinned_reason

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")

    assert pinned_reason(project, out["hash"]) is None


def test_a_source_snapshot_whose_clone_is_gone_is_pinned(project: str, tmp_path: Path, monkeypatch):
    """Without the clone the snapshot is the last copy of those rows, so the delete leaves it alone and says why."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.materialize import pinned_reason

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    _clone_of(project, out).unlink()

    reason = pinned_reason(project, out["hash"])
    assert reason is not None
    assert "orders-v1" in reason
    assert ".cas" in reason, reason  # the pin is the missing clone, not the fact that this is a source


def test_a_reset_keeps_the_clone_of_an_imported_source_alive(project: str, tmp_path: Path, monkeypatch):
    """``gc_cas`` walks the retention closure; an imported version's clone is in it via the entry's provenance."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.catalog_state import _live_source_digests, capture_tallyman_state

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    capture_tallyman_state(project)  # what a checkpoint does; the retention closure reads its pointer list

    assert out["digest"] in (_live_source_digests(project) or set())


# ---------------------------------------------------------------------------
# D12 — reader options are fixed at import
# ---------------------------------------------------------------------------


def test_csv_reader_options_are_recorded_on_the_source_entry(project: str, tmp_path: Path, monkeypatch):
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [("north", 1), ("south", 2)], sep=";")

    out = source_import.update_and_depend(str(src), "orders", separator=";")

    frame = _snapshot_frame(project, out["hash"])
    assert frame.columns == ["region", "n", ROW_ORDER]
    assert frame["region"].to_list() == ["north", "south"]
    reader = read_manifest(entry_dir(project, out["hash"])).provenance.reader
    assert reader["kind"] == "csv"
    assert reader["scan_kwargs"]["separator"] == ";"


def test_the_same_csv_under_two_readers_is_two_entries(project: str, tmp_path: Path, monkeypatch):
    """Two recipes cannot read one file two ways: import it twice under two aliases, and the hashes differ."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [("north", 1), ("south", 2)])

    plain = source_import.update_and_depend(str(src), "orders")
    typed = source_import.update_and_depend(str(src), "orders_typed", schema=(("region", "string"), ("n", "float64")))

    assert plain["hash"] != typed["hash"]
    assert str(_snapshot_frame(project, typed["hash"])["n"].dtype) == "Float64"


def test_reader_options_are_not_re_derived_at_build_time(project: str, tmp_path: Path, monkeypatch):
    """A parquet source records the reader it was imported with; a build reads the snapshot and derives nothing."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    out = source_import.update_and_depend(str(src), "orders")

    assert read_manifest(entry_dir(project, out["hash"])).provenance.reader == {"kind": "parquet"}


# ---------------------------------------------------------------------------
# D9 — ingest verifies what it wrote
# ---------------------------------------------------------------------------


def test_ensure_cas_path_rejects_a_clone_that_does_not_match_its_name(project: str, tmp_path: Path):
    """A file edited mid-copy would otherwise produce a clone whose name lies about its content."""
    from tallyman_xorq import source_identity as si

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    with pytest.raises(si.CloneDigestMismatch):
        si.ensure_cas_path(project, src, "0" * 32)


def test_recon_cas_path_raises_instead_of_serving_drifted_live_bytes(project: str, tmp_path: Path):
    """A version tallyman promised and then lost is a failure, not a downgrade to whatever is on disk now."""
    from tallyman_xorq import source_identity as si

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    digest = si._digest_file(src)
    _write_parquet(src, 20)  # the live bytes drift and no clone was ever written

    with pytest.raises(si.LostSourceVersion):
        si.recon_cas_path(project, src, digest)


# ---------------------------------------------------------------------------
# D10 — the MCP surface
# ---------------------------------------------------------------------------


def test_catalog_load_parquet_is_gone(project: str):
    """D10: the old tool's semantics change too much to keep the name."""
    import tallyman_mcp.server as server

    assert not hasattr(server, "catalog_load_parquet")


def test_catalog_import_source_mints_and_records_the_event(project: str, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.events import read_events
    from tallyman_mcp.server import catalog_import_source

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    out = catalog_import_source(str(src), "orders")

    assert "error" not in out, out
    assert out["alias"] == "orders"
    assert out["version"] == 1
    assert out["row_count"] == 10
    assert any(f["name"] == "region" for f in out["schema"]["fields"])
    kinds = [e.get("kind") for e in read_events(project)]
    assert "alias_set" in kinds


def test_catalog_import_source_takes_a_checkpoint(project: str, tmp_path: Path, monkeypatch):
    """An import is a catalog operation (D7), so it lands as one revision like a revise."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.catalog_state import list_revisions
    from tallyman_mcp.server import catalog_import_source

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    before = len(list_revisions(project))

    catalog_import_source(str(src), "orders")

    assert len(list_revisions(project)) == before + 1


def test_catalog_import_source_triggers_auto_recalc(project: str, tmp_path: Path, monkeypatch):
    """A source advancing is indistinguishable downstream from an alias being revised."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_AUTO_RECALC", "1")
    from tallyman_mcp.server import catalog_create, catalog_import_source

    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    catalog_import_source(str(src), "orders")
    assert "error" not in catalog_create("totals", _child_code("orders", project))
    first = get_alias(project, "totals")

    _write_parquet(src, 20)
    out = catalog_import_source(str(src), "orders")

    assert out["recalc"]["remap"], out
    assert get_alias(project, "totals") != first


def test_catalog_import_source_reports_an_error_as_a_dict(project: str, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_mcp.server import catalog_import_source

    out = catalog_import_source(str(_outside(tmp_path) / "nope.parquet"), "orders")

    assert "error" in out
    assert "nope.parquet" in out["error"]


def test_catalog_revise_refuses_a_source_alias(project: str, tmp_path: Path, monkeypatch):
    """There is no recipe to revise: a source alias advances only by an import."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_mcp.server import catalog_import_source, catalog_revise

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    catalog_import_source(str(src), "orders")

    out = catalog_revise("orders", _child_code("orders", project))

    assert "error" in out
    assert "catalog_import_source" in out["error"]


def test_promote_diff_refuses_a_source_alias_as_its_target(project: str, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_mcp.server import catalog_create, catalog_import_source, catalog_promote_diff, catalog_revise

    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    catalog_import_source(str(src), "orders")
    catalog_create("totals", _child_code("orders", project))
    catalog_revise("totals", _child_code("orders", project) + "expr = expr.mutate(x=1)\n")

    out = catalog_promote_diff("totals", alias="orders")

    assert "error" in out
    assert "source alias" in out["error"]


def test_catalog_alias_refuses_a_source_entry(project: str, tmp_path: Path, monkeypatch):
    """A source version cannot take a catalog name: the alias's kind must match its entry's.

    catalog_alias checked only the kind of the *name*, so ``catalog_alias(<source hash>, "x")`` made a catalog alias
    whose head was an imported file, and ``catalog_revise("x", ...)`` was then allowed on it. The refusal names the
    source alias-version that holds the entry and steers to a catalog entry that reads it, which follows the source
    when it is imported again.
    """
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.aliases import alias_kind
    from tallyman_mcp.server import catalog_alias, catalog_create, catalog_import_source

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    imported = catalog_import_source(str(src), "orders")

    out = catalog_alias(imported["hash"], "x")

    assert "error" in out, out
    assert "orders-v1" in out["error"]
    assert "catalog_create" in out["error"]
    assert get_alias(project, "x") is None
    assert alias_kind(project, "x") is None

    steer = "from tallyman_xorq.io import tracked_expr_from_alias\nexpr = tracked_expr_from_alias('orders')"
    followed = catalog_create("x", steer)

    assert "error" not in followed, followed
    assert followed["hash"] != imported["hash"]
    assert alias_kind(project, "x") == "catalog"


# ---------------------------------------------------------------------------
# D6 — the ordered-copy store is subsumed into the source entry's snapshot
# ---------------------------------------------------------------------------


def test_the_ordered_copy_store_is_gone(project: str, tmp_path: Path, monkeypatch):
    """There is one shape of file under ``compute_cache/``: a snapshot named by an entry hash.

    ADR-008 D2 gave every source an ordered copy keyed by ``md5(digest ǀ reader signature)``. With the
    source itself an entry, the entry hash names the file and the reader options live on the entry that
    used them (D12), so the second store and the key that addressed it have nothing left to do.
    """
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq import ordered_copy as oc
    from tallyman_xorq import source_import
    from tallyman_xorq.materialize import RESULT_CACHE_DIRNAME

    for name in (
        "copy_key",
        "ensure_ordered_copy",
        "existing_ordered_copy",
        "recreate_ordered_copy",
        "ORDERED_COPIES_DIRNAME",
        "ordered_copies_dir",
        "is_ordered_copy_path",
        "note_ordered_copy",
        "begin_collect",
        "end_collect",
        "SourceUnavailable",
    ):
        assert not hasattr(oc, name), f"ordered_copy.{name} is deleted by ADR-011 D6"

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 12)
    out = source_import.update_and_depend(str(src), "orders")
    from tallyman_xorq.build import build_and_persist

    build_and_persist(project, _child_code("orders", project))

    assert snapshot_path(project, out["hash"]).is_file()
    assert [d.name for d in sorted(compute_cache_dir(project).iterdir())] == [RESULT_CACHE_DIRNAME]
