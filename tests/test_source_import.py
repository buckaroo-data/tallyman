"""Importing a file is how data enters the catalog (ADR-011).

A raw input is an alias whose versions are ordinary entries. ``update_and_depend`` copies the bytes into the arena
(the ``.cas`` clone store), writes ONE parquet snapshot carrying ``__row_order`` named by the source entry's content
hash, and points a source alias at that entry. A recipe then reads ``tracked_expr_from_alias("orders")`` like any
other parent, and the file the bytes came from is provenance: never read again.

These pin stage 1 of the ADR — the import path and the refusals (D1, D2, D3, D5, D9, D10, D12). The staleness source
axis (D6) and the source-identity modes (D8) are stage 2.
"""

from __future__ import annotations

import errno
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


def test_the_append_only_refusal_does_not_advise_a_second_alias(project: str, tmp_path: Path, monkeypatch):
    """D11's refusal offers ways back that work: a reset, or reading the old version pinned.

    It used to offer "import these bytes under a different alias", which is refused too, because bytes an alias
    already holds cannot be imported under another (ADR-011 D1).
    """
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
    assert "different alias" not in message, message
    assert "pinned_expr_from_alias('orders-v1')" in message, message
    with pytest.raises(source_import.SourceImportError, match="orders-v1"):
        source_import.update_and_depend(str(src), "orders_again")


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


def test_identical_bytes_under_a_second_alias_is_an_error(project: str, tmp_path: Path, monkeypatch):
    """One set of bytes is one source version under one alias; importing it again under another name is refused.

    The likeliest way to get here is not knowing the bytes are already in the project, so the error names the alias
    and version that hold them and the way to give them a second name. The first alias's entry is untouched: a second
    import used to rewrite its manifest and recipe, and a failure part-way deleted the entry outright.
    """
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    a = source_import.update_and_depend(str(src), "orders")
    entry = entry_dir(project, a["hash"])
    manifest_before = (entry / "manifest.json").read_bytes()
    recipe_before = (entry / "expr.py").read_bytes()
    copy = _write_parquet(_outside(tmp_path) / "copy_of_orders.parquet", 10)
    assert copy.read_bytes() == src.read_bytes()

    with pytest.raises(source_import.SourceImportError) as info:
        source_import.update_and_depend(str(copy), "orders_again")

    message = str(info.value)
    assert "orders-v1" in message
    assert "tracked_expr_from_alias('orders')" in message
    assert "catalog_create" in message
    assert get_alias(project, "orders_again") is None
    assert (entry / "manifest.json").read_bytes() == manifest_before
    assert (entry / "expr.py").read_bytes() == recipe_before


def test_bytes_of_an_older_version_of_another_alias_are_refused(project: str, tmp_path: Path, monkeypatch):
    """The bytes need not be another alias's head: any version of any other alias holds them."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.parquet"
    _write_parquet(src, 10)
    first_bytes = src.read_bytes()
    source_import.update_and_depend(str(src), "orders")
    _write_parquet(src, 20)
    source_import.update_and_depend(str(src), "orders")
    old = _outside(tmp_path) / "old_orders.parquet"
    old.write_bytes(first_bytes)

    with pytest.raises(source_import.SourceImportError, match="orders-v1"):
        source_import.update_and_depend(str(old), "archive")

    assert get_alias(project, "archive") is None


def test_a_repair_import_writes_the_snapshot_and_nothing_else(project: str, tmp_path: Path, monkeypatch):
    """Re-importing a version whose snapshot is gone restores the snapshot and leaves the entry alone.

    The entry's recipe, build and manifest are the record of the import, and a repair is not an import: it goes
    through the heal ``ensure_materialized`` uses. So nothing is rebuilt (a failing ``build_expr`` does not matter),
    and repairing from another path does not move the recorded provenance path or its time.
    """
    import xorq.ibis_yaml.compiler as compiler

    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    a = source_import.update_and_depend(str(src), "orders")
    entry = entry_dir(project, a["hash"])
    record = {name: (entry / name).read_bytes() for name in ("manifest.json", "expr.py")}
    snapshot = snapshot_path(project, a["hash"])
    before = snapshot.read_bytes()
    snapshot.unlink()
    moved = _outside(tmp_path) / "moved" / "orders.parquet"
    moved.parent.mkdir()
    moved.write_bytes(src.read_bytes())

    def rebuild(*args, **kwargs):
        raise RuntimeError("a repair rebuilt the entry")

    monkeypatch.setattr(compiler, "build_expr", rebuild)
    out = source_import.update_and_depend(str(moved), "orders")

    assert out["created"] is False
    assert snapshot.read_bytes() == before
    assert {name: (entry / name).read_bytes() for name in record} == record


def test_a_repair_import_is_verified_like_any_heal(project: str, tmp_path: Path, monkeypatch):
    """A repair that writes other rows under the recorded hash is an unfaithful heal, and is recorded as one.

    The recorded digest and row count stay the build-time ones. A repair used to take whatever it wrote as the new
    truth, so a child built on the old rows read the new ones with nothing to say they had changed.
    """
    from tallyman_core.errors import list_errors
    from tallyman_xorq import source_import
    from tallyman_xorq.digest import content_digest

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    a = source_import.update_and_depend(str(src), "orders")
    recorded = read_manifest(entry_dir(project, a["hash"]))
    snapshot_path(project, a["hash"]).unlink()
    write = source_import._write_snapshot

    def one_row_short(clone, reader, dest):  # a reader that now parses the same bytes into other rows
        write(clone, reader, dest)
        table = pq.read_table(dest)
        pq.write_table(table.slice(0, table.num_rows - 1), dest)
        return content_digest(dest)

    monkeypatch.setattr(source_import, "_write_snapshot", one_row_short)
    source_import.update_and_depend(str(src), "orders")

    after = read_manifest(entry_dir(project, a["hash"]))
    assert (after.result_digest, after.row_count) == (recorded.result_digest, recorded.row_count)
    codes = [r.get("code") for r in list_errors(project, limit=1000) if r.get("hash") == a["hash"]]
    assert "unfaithful_heal" in codes, codes


def test_an_entry_whose_manifest_is_gone_is_written_again_by_a_re_import(project: str, tmp_path: Path, monkeypatch):
    """An entry directory with no readable manifest, which a crash part-way through an import leaves, is not an entry.

    A re-import of its bytes writes it again rather than failing to read it.
    """
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    a = source_import.update_and_depend(str(src), "orders")
    (entry_dir(project, a["hash"]) / "manifest.json").unlink()

    out = source_import.update_and_depend(str(src), "orders")

    assert (out["hash"], out["created"]) == (a["hash"], False)
    assert read_manifest(entry_dir(project, a["hash"])).provenance.alias == "orders"


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


# ``manifest.provenance.alias`` and ``.version`` are the name the import was made under, recorded once. A rename
# carries an alias's history and kind to the new name (``aliases.rename_alias``) and an unalias drops it, so a
# message that names a source version, or tells the user which import repairs it, has to ask the alias store who
# holds the entry now.


def _advised_import(message: str) -> tuple[list, dict]:
    """The arguments of the ``catalog_import_source(...)`` call that ends *message*, parsed as Python literals."""
    import ast

    call = ast.parse(message[message.index("catalog_import_source(") :].rstrip("."), mode="eval").body
    assert isinstance(call, ast.Call), message
    return [ast.literal_eval(a) for a in call.args], {k.arg: ast.literal_eval(k.value) for k in call.keywords}


def test_a_pinned_source_snapshot_is_named_by_the_alias_it_was_renamed_to(project: str, tmp_path: Path, monkeypatch):
    """The pin names the version as the catalog knows it now; the name it was imported under is history."""
    from tallyman_core.aliases import rename_alias
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.materialize import pinned_reason

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "a_src")
    rename_alias(project, "a_src", "renamed_src")
    _clone_of(project, out).unlink()

    reason = pinned_reason(project, out["hash"])
    assert reason is not None
    assert "renamed_src-v1" in reason, reason
    assert "source version a_src-v1" not in reason, reason


def test_the_re_import_advice_names_the_alias_a_source_was_renamed_to(project: str, tmp_path: Path, monkeypatch):
    """The heal error names the version by its alias now, and so does the import it advises."""
    from tallyman_core.aliases import rename_alias
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "a_src")
    rename_alias(project, "a_src", "renamed_src")
    snapshot_path(project, out["hash"]).unlink()
    _clone_of(project, out).unlink()

    with pytest.raises(BuildError) as exc:
        ensure_materialized(project, out["hash"])

    message = str(exc.value)
    assert "renamed_src-v1" in message, message
    assert f"catalog_import_source({str(src)!r}, 'renamed_src', pinned_version=1)" in message, message
    assert "'a_src'" not in message, message


def test_following_the_re_import_advice_after_a_rename_repairs_the_version(project: str, tmp_path: Path, monkeypatch):
    """The advice is a call the user runs as written, so it has to repair the renamed alias's version.

    Advice that named the import-time alias minted a second source alias, ``a_src``, over the same entry, and the
    renamed one's snapshot came back only by accident.
    """
    from tallyman_core.aliases import load_kinds, rename_alias
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "a_src")
    before = snapshot_path(project, out["hash"]).read_bytes()
    rename_alias(project, "a_src", "renamed_src")
    snapshot_path(project, out["hash"]).unlink()
    _clone_of(project, out).unlink()
    with pytest.raises(BuildError) as exc:
        ensure_materialized(project, out["hash"])
    args, kwargs = _advised_import(str(exc.value))

    repaired = source_import.update_and_depend(*args, **kwargs)

    assert load_kinds(project) == {"renamed_src": "source"}
    assert (repaired["alias"], repaired["version"], repaired["created"]) == ("renamed_src", 1, False)
    assert repaired["hash"] == out["hash"]
    assert history_for(project, "renamed_src") == [out["hash"]]
    assert snapshot_path(project, out["hash"]).read_bytes() == before


def test_catalog_rename_then_the_advised_catalog_import_source_repairs_the_version(
    project: str, tmp_path: Path, monkeypatch
):
    """The same, through the tools a user has: the advice is an MCP call, run against the MCP tool."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.aliases import load_kinds
    from tallyman_mcp.server import catalog_import_source, catalog_rename
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = catalog_import_source(str(src), "a_src")
    assert "error" not in catalog_rename("a_src", "renamed_src")
    snapshot_path(project, out["hash"]).unlink()
    _clone_of(project, out).unlink()
    with pytest.raises(BuildError) as exc:
        ensure_materialized(project, out["hash"])
    args, kwargs = _advised_import(str(exc.value))

    repaired = catalog_import_source(*args, **kwargs)

    assert "error" not in repaired, repaired
    assert load_kinds(project) == {"renamed_src": "source"}
    assert snapshot_path(project, out["hash"]).is_file()


def test_a_source_version_no_alias_holds_is_not_advised_back_under_its_old_name(
    project: str, tmp_path: Path, monkeypatch
):
    """After an unalias no alias holds the version, and the messages say so instead of naming the dead alias.

    Re-importing under the old name would not restore anything that exists: it would mint a new alias of that name.
    """
    from tallyman_core.aliases import remove_alias
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import ensure_materialized, pinned_reason

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "a_src")
    remove_alias(project, "a_src")
    _clone_of(project, out).unlink()

    reason = pinned_reason(project, out["hash"])
    assert reason is not None
    assert "no source alias" in reason, reason
    assert "imported as a_src-v1" in reason, reason

    snapshot_path(project, out["hash"]).unlink()
    with pytest.raises(BuildError) as exc:
        ensure_materialized(project, out["hash"])

    message = str(exc.value)
    assert "no source alias" in message, message
    assert "'a_src'" not in message, message
    assert "pinned_version" not in message, message


def test_following_the_re_import_advice_for_a_csv_repairs_the_version(project: str, tmp_path: Path, monkeypatch):
    """A CSV's entry hash covers its reader options (D12), so the advised import has to carry them.

    Without them the same file hashes to another entry, and the advised call is refused as "not orders-v1".
    """
    from tallyman_mcp.server import catalog_import_source
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import ensure_materialized

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [("north", 1), ("south", 2)], sep=";")
    out = catalog_import_source(
        str(src), "orders", schema={"region": "string", "n": "int64"}, reader_options={"separator": ";"}
    )
    assert "error" not in out, out
    snapshot_path(project, out["hash"]).unlink()
    _clone_of(project, out).unlink()
    with pytest.raises(BuildError) as exc:
        ensure_materialized(project, out["hash"])
    args, kwargs = _advised_import(str(exc.value))

    repaired = catalog_import_source(*args, **kwargs)

    assert "error" not in repaired, repaired
    assert (repaired["hash"], repaired["created"]) == (out["hash"], False)
    assert snapshot_path(project, out["hash"]).is_file()


def test_a_pinned_import_read_another_way_says_the_reader_options_differ(project: str, tmp_path: Path, monkeypatch):
    """The same bytes under other reader options are another entry, and the refusal has to say so.

    "This file is not orders-v1" is false about the file; what differs is how it is read.
    """
    from tallyman_mcp.server import catalog_import_source

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [("north", 1), ("south", 2)], sep=";")
    assert "error" not in catalog_import_source(str(src), "orders", reader_options={"separator": ";"})

    out = catalog_import_source(str(src), "orders", pinned_version=1)

    assert "reader options" in out.get("error", ""), out


def test_a_source_recipe_names_its_alias_as_the_one_it_was_imported_under(project: str, tmp_path: Path, monkeypatch):
    """The generated recipe is written once, at import, so the Code tab must not present that name as the entry's.

    After a rename the entry is ``renamed_src-v1``; a header reading ``# a_src-v1: a source version`` says otherwise.
    """
    from tallyman_core.aliases import rename_alias
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "a_src")
    rename_alias(project, "a_src", "renamed_src")

    header = (entry_dir(project, out["hash"]) / "expr.py").read_text().splitlines()[0]
    assert "imported as a_src-v1" in header, header
    assert not header.startswith("# a_src-v1:"), header


def test_a_reset_keeps_the_clone_of_an_imported_source_alive(project: str, tmp_path: Path, monkeypatch):
    """``gc_cas`` walks the retention closure; an imported version's clone is in it via the entry's provenance."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.catalog_state import _live_source_digests, capture_tallyman_state

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    capture_tallyman_state(project)  # what a checkpoint does; the retention closure reads its pointer list

    assert out["digest"] in (_live_source_digests(project) or set())


def test_a_failed_re_import_keeps_the_snapshot_a_reset_left_on_disk(project: str, tmp_path: Path, monkeypatch):
    """#193 for a source entry. A reset back past an import parks the entry and leaves its snapshot (ADR-007 D14), and
    importing the file again mints over that file. ``_mint`` keeps a snapshot already at the path, since the entry hash
    fixes its rows (D12), so a mint that fails, here at its manifest write, leaves the file as it was. A build stages
    its snapshot for this; a mint does not need to."""
    import tallyman_core
    from tallyman_core import catalog_state as cs
    from tallyman_xorq import source_import
    from tallyman_xorq.result_cache import snapshot_file_digest

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    cs.ensure_catalog_repo(project)
    empty = cs.checkpoint_catalog(project, "empty")
    out = source_import.update_and_depend(str(src), "orders")
    assert empty is not None and cs.checkpoint_catalog(project, "imported") is not None
    snap = snapshot_path(project, out["hash"])
    digest = snapshot_file_digest(snap)
    cs.reset_to(project, empty)
    assert not entry_dir(project, out["hash"]).exists()

    def _disk_full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(tallyman_core, "write_manifest", _disk_full)
    with pytest.raises(Exception, match="No space left on device"):
        source_import.update_and_depend(str(src), "orders")
    assert not entry_dir(project, out["hash"]).exists()
    assert snapshot_file_digest(snap) == digest


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


def test_catalog_import_source_reports_a_duplicate_import_as_an_error(project: str, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_core.catalog_state import list_revisions
    from tallyman_mcp.server import catalog_import_source

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    catalog_import_source(str(src), "orders")
    before = len(list_revisions(project))

    out = catalog_import_source(str(src), "orders_again")

    assert "orders-v1" in out.get("error", ""), out
    assert get_alias(project, "orders_again") is None
    assert len(list_revisions(project)) == before


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


# ---------------------------------------------------------------------------
# a second name for a source is an expression over it, not a second import
# ---------------------------------------------------------------------------


def test_a_second_name_for_a_source_is_a_catalog_entry_over_it(project: str, tmp_path: Path, monkeypatch):
    """What the duplicate-import error tells the user to do instead, and why it needs nothing to force the hash apart.

    A source entry's hash is an md5 of its bytes and reader options; a recipe's hash is xorq's hash of the expression.
    An expression that only reads the source is therefore a different entry already, with a followed parent edge, so a
    re-import of the source advances it like any other follower.
    """
    from tallyman_mcp.server import catalog_create
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    source = source_import.update_and_depend(str(src), "orders")

    out = catalog_create(
        "orders_eu", "from tallyman_xorq.io import tracked_expr_from_alias\nexpr = tracked_expr_from_alias('orders')\n"
    )

    assert "error" not in out, out
    assert out["hash"] != source["hash"]
    parents = read_manifest(entry_dir(project, out["hash"])).parents
    assert [(p.ref, p.hash, p.follow) for p in parents] == [("orders", source["hash"], True)]


# ---------------------------------------------------------------------------
# two projects importing one file share nothing
# ---------------------------------------------------------------------------


@pytest.fixture
def two_projects(isolated_home: Path) -> tuple[str, str]:
    from tallyman_core import ensure_project, set_active_project

    for name in ("alpha", "beta"):
        ensure_project(name)
    set_active_project("alpha")
    return "alpha", "beta"


def test_two_projects_importing_one_file_each_get_their_own_copy(two_projects, tmp_path: Path):
    """Same bytes, same entry hash, but each project holds its own entry, snapshot and clone."""
    from tallyman_xorq import source_import
    from tallyman_xorq.source_import import source_clone_path

    alpha, beta = two_projects
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 12)

    a = source_import.update_and_depend(str(src), "orders", project=alpha)
    b = source_import.update_and_depend(str(src), "orders", project=beta)

    assert a["hash"] == b["hash"]
    assert a["created"] and b["created"], "the second project's import is not a no-op on the first's entry"
    for proj in (alpha, beta):
        manifest = read_manifest(entry_dir(proj, a["hash"]))
        assert manifest.project == proj
        assert snapshot_path(proj, a["hash"]).is_file()
        assert source_clone_path(proj, manifest.provenance).is_file()
        assert get_alias(proj, "orders") == a["hash"]
    assert snapshot_path(alpha, a["hash"]) != snapshot_path(beta, b["hash"])


def test_the_duplicate_import_check_is_per_project(two_projects, tmp_path: Path):
    """One file under different alias names in two projects is two unrelated imports, not a duplicate."""
    from tallyman_xorq import source_import

    alpha, beta = two_projects
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 12)

    source_import.update_and_depend(str(src), "orders", project=alpha)
    out = source_import.update_and_depend(str(src), "sales", project=beta)

    assert out["created"] is True
    assert history_for(beta, "sales") == [out["hash"]]
    assert history_for(beta, "orders") == []


def test_deleting_and_healing_one_projects_snapshot_leaves_the_others_alone(two_projects, tmp_path: Path):
    from tallyman_xorq import source_import
    from tallyman_xorq.build import build_and_persist
    from tallyman_xorq.digest import content_digest
    from tallyman_xorq.materialize import ensure_materialized
    from tallyman_xorq.result_cache import cached_result_expr

    alpha, beta = two_projects
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 12)
    h = source_import.update_and_depend(str(src), "orders", project=alpha)["hash"]
    source_import.update_and_depend(str(src), "orders", project=beta)
    child_a = build_and_persist(alpha, _child_code("orders", alpha)).content_hash
    child_b = build_and_persist(beta, _child_code("orders", beta)).content_hash
    beta_snapshot = snapshot_path(beta, h)
    beta_digest, beta_mtime = content_digest(beta_snapshot), beta_snapshot.stat().st_mtime_ns

    snapshot_path(alpha, h).unlink()
    ensure_materialized(alpha, h)

    assert snapshot_path(alpha, h).is_file()
    assert (content_digest(beta_snapshot), beta_snapshot.stat().st_mtime_ns) == (beta_digest, beta_mtime)
    assert len(cached_result_expr(alpha, child_a).execute()) == 3
    assert len(cached_result_expr(beta, child_b).execute()) == 3


def test_retiring_one_projects_clones_leaves_the_others(two_projects, tmp_path: Path):
    """``.cas`` lives under each project's ``data/``; a sweep in one project never reaches another's."""
    from tallyman_xorq import source_identity as si
    from tallyman_xorq import source_import
    from tallyman_xorq.source_import import source_clone_path

    alpha, beta = two_projects
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 12)
    h = source_import.update_and_depend(str(src), "orders", project=alpha)["hash"]
    source_import.update_and_depend(str(src), "orders", project=beta)
    beta_clone = source_clone_path(beta, read_manifest(entry_dir(beta, h)).provenance)

    assert si.gc_cas(alpha, set(), bullpen=tmp_path / "bullpen") == 1

    assert beta_clone.is_file()


def test_a_re_import_in_one_project_does_not_stale_the_other(two_projects, tmp_path: Path):
    from tallyman_core.aliases import set_alias
    from tallyman_xorq import source_import
    from tallyman_xorq.build import build_and_persist
    from tallyman_xorq.staleness import scan

    alpha, beta = two_projects
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 12)
    source_import.update_and_depend(str(src), "orders", project=alpha)
    source_import.update_and_depend(str(src), "orders", project=beta)
    child_a = build_and_persist(alpha, _child_code("orders", alpha)).content_hash
    child_b = build_and_persist(beta, _child_code("orders", beta)).content_hash
    for proj, child in ((alpha, child_a), (beta, child_b)):
        set_alias(proj, "totals", child)  # the scan judges live alias heads (#154)

    _write_parquet(src, 20)
    source_import.update_and_depend(str(src), "orders", project=beta)

    assert scan(alpha)[child_a].stale is False
    assert scan(beta)[child_b].stale is True


# ---------------------------------------------------------------------------
# import hardening: what a failed import says, records and leaves behind (#224, #225, #227, #234, #239)
# ---------------------------------------------------------------------------


def _arena(project: str) -> dict[str, list[str]]:
    """The files in ``result_cache/`` and ``data/.cas/``, the two places an import writes before its entry."""
    from tallyman_xorq.materialize import snapshots_dir

    def names(d: Path) -> list[str]:
        return sorted(p.name for p in d.iterdir()) if d.is_dir() else []

    return {"result_cache": names(snapshots_dir(project)), "cas": names(data_dir(project) / ".cas")}


def test_a_parquet_column_with_no_ibis_type_is_refused_before_anything_is_written(
    project: str, tmp_path: Path, monkeypatch
):
    """#224. A fixed_size_binary column, and a UUID column, which is stored as fixed_size_binary(16), have no ibis
    type. The generated recipe's read of the snapshot failed on them with a KeyError that named no column, after the
    clone and the snapshot were written. The import checks the types the read will see before it writes anything,
    and names every such column, a nested one by its path, with the cast that makes the file importable."""
    import uuid

    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "ids.parquet"
    table = pa.table(
        {
            "id": pa.array([b"a" * 16, b"b" * 16], type=pa.binary(16)),
            "meta": pa.array([{"key": b"k1k1"}, {"key": b"k2k2"}], type=pa.struct([("key", pa.binary(4))])),
            "guid": pa.array([uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes], type=pa.uuid()),
            "n": pa.array([1, 2]),
        }
    )
    pq.write_table(table, src)

    with pytest.raises(source_import.SourceImportError) as exc:
        source_import.update_and_depend(str(src), "ids")

    message = str(exc.value)
    for column in ("'id'", "'meta.key'", "'guid'"):
        assert column in message, message
    assert "'n'" not in message, message
    assert "binary" in message and "string" in message, message  # the casts: to binary, and a UUID to string
    assert str(src) in message, message
    assert "read_project_file" not in message and "Traceback" not in message, message
    assert _arena(project) == {"result_cache": [], "cas": []}
    assert get_alias(project, "ids") is None


def test_an_import_that_fails_in_its_generated_recipe_leaves_the_arena_as_it_was(
    project: str, tmp_path: Path, monkeypatch
):
    """#225. The clone and the snapshot are written before the generated recipe runs, and a failure there removed
    the entry directory and nothing else. Nothing lists the clone, and a retry reused the orphan snapshot."""
    from tallyman_xorq import build, source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    before = _arena(project)

    def fails(code):
        raise build.BuildError("executing user code raised: KeyError")

    monkeypatch.setattr(build, "_import_script", fails)
    with pytest.raises(build.BuildError):
        source_import.update_and_depend(str(src), "orders")

    assert _arena(project) == before
    assert get_alias(project, "orders") is None


def test_a_csv_that_fails_to_parse_leaves_no_clone(project: str, tmp_path: Path, monkeypatch):
    """#225. The snapshot of a CSV that does not parse goes to a temporary name that is removed, but the clone was
    written first, and every CSV import that failed left one, one per distinct set of bytes tried."""
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [("east", 1), ("west", 2)])

    with pytest.raises(ValueError):
        source_import.update_and_depend(str(src), "orders", schema={"region": "int64", "n": "int64"})

    assert _arena(project) == {"result_cache": [], "cas": []}


@pytest.fixture
def notified(monkeypatch) -> list:
    """What the MCP server announces to the companion, captured here instead of posted to it."""
    import tallyman_mcp.server as server

    sent: list = []
    monkeypatch.setattr(server, "_notify", lambda kind, content_hash=None, **extra: sent.append((kind, extra)))
    return sent


def _call_import(args: dict):
    """``catalog_import_source`` called through fastmcp's client, the way an agent calls it."""
    import asyncio

    from fastmcp import Client

    from tallyman_mcp.server import mcp

    async def go():
        async with Client(mcp) as client:
            return await client.call_tool("catalog_import_source", args, raise_on_error=False)

    return asyncio.run(go())


def _assert_recorded(project: str, reply, notified: list) -> dict:
    """The reply is the tool's error dict, and the failure is in errors.jsonl, the activity log and a notification."""
    from tallyman_core.errors import list_errors
    from tallyman_core.events import read_events

    assert reply.is_error is False, reply
    out = reply.data
    assert {"error", "error_id"} <= set(out), out
    records = list_errors(project, limit=1000)
    assert [(r["id"], r["tool"]) for r in records] == [(out["error_id"], "catalog_import_source")], records
    events = [e for e in read_events(project) if e.get("kind") == "build_error"]
    assert [e.get("error_id") for e in events] == [out["error_id"]], events
    assert ("build_failed", {"error_id": out["error_id"], "tool": "catalog_import_source"}) in notified, notified
    return out


_UNREADABLE_CSVS = {
    "a value that does not fit a pinned type": (b"a,b\n1,2\nx,3\n", {"schema": {"a": "int64", "b": "int64"}}),
    "a ragged row": (b"a,b\n1,2\n3,4,5\n", {"schema": {"a": "int64", "b": "int64"}}),
    "invalid utf-8": (b"a,b\n1,\xff\xfe\n", {"schema": {"a": "int64", "b": "string"}}),
    "a dtype that does not parse": (b"a,b\n1,2\n", {"schema": {"a": "i64", "b": "int64"}}),
    "a two-byte separator": (b"a,b\n1,2\n", {"reader_options": {"separator": "ab"}}),
    "an unknown reader option": (b"a,b\n1,2\n", {"reader_options": {"frobnicate": 1}}),
    "an empty file": (b"", {}),
}


@pytest.mark.parametrize("body, options", list(_UNREADABLE_CSVS.values()), ids=list(_UNREADABLE_CSVS))
def test_a_csv_the_reader_refuses_is_a_recorded_import_error(
    project: str, tmp_path: Path, monkeypatch, notified, body: bytes, options: dict
):
    """#227. polars' errors are ValueError, TypeError, parsy's ParseError and NoDataError, and the tool caught only
    SourceImportError, BuildError and OSError, so they reached the agent as a raw tool error with no error_id and no
    record. The message named the clone under data/.cas and ``tallyman_read_csv``, which a recipe may not call."""
    import hashlib

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.csv"
    src.write_bytes(body)

    out = _assert_recorded(project, _call_import({"outside_path": str(src), "alias": "orders", **options}), notified)

    message = out["error"]
    assert str(src) in message, message
    assert "catalog_import_source" in message, message
    assert "tallyman_read_csv" not in message, message
    assert hashlib.md5(body).hexdigest() not in message and ".cas" not in message, message


def test_a_csv_parse_failure_gives_the_retry_in_the_tools_argument_shape(
    project: str, tmp_path: Path, monkeypatch, notified
):
    """#227. The message keeps the column polars reports, drops polars' advice (``infer_schema_length`` and
    ``schema_overrides`` are refused by the import), and ends with the retry as a ``catalog_import_source`` call that
    works when run as written: the suggested schema as a list of lists, which the tool takes, not a tuple of tuples."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "orders.csv"
    src.write_text("a,b\n1,2\nx,3\n")

    reply = _call_import({"outside_path": str(src), "alias": "orders", "schema": {"a": "int64", "b": "int64"}})

    message = _assert_recorded(project, reply, notified)["error"]
    assert "column 'a'" in message, message
    for refused in ("infer_schema_length", "schema_overrides"):
        assert refused not in message, message
    args, kwargs = _advised_import(message)
    assert args == [str(src), "orders"], message
    assert kwargs == {"schema": [["a", "string"], ["b", "int64"]]}, message

    retried = _call_import({"outside_path": args[0], "alias": args[1], **kwargs}).data

    assert "error" not in retried, retried
    assert (retried["version"], retried["created"]) == (1, True)


def test_a_clone_that_fails_its_digest_check_is_a_recorded_import_error(
    project: str, tmp_path: Path, monkeypatch, notified
):
    """#227, from its comment. ``CloneDigestMismatch`` (ADR-011 D9, ingest verifies what it wrote) subclasses
    ValueError, so it escaped the tool's handler as well."""
    from tallyman_xorq import source_identity as si

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)

    def changed_while_copying(source: Path, dest: Path) -> None:
        dest.write_bytes(source.read_bytes() + b"appended while the copy ran")

    monkeypatch.setattr(si, "_clone", changed_while_copying)

    out = _assert_recorded(project, _call_import({"outside_path": str(src), "alias": "orders"}), notified)

    assert str(src) in out["error"], out
    assert f"catalog_import_source({str(src)!r}, 'orders')" in out["error"], out
    assert _arena(project) == {"result_cache": [], "cas": []}


def test_the_append_only_refusal_names_reset_commands_that_exist(project: str, tmp_path: Path, monkeypatch):
    """#234. The refusal sent the user to ``tallyman reset`` and ``catalog_reset_to``, and neither exists: the CLI
    has ``tallyman revisions`` and ``tallyman reset-to <step>``, and the MCP server has no reset tool."""
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
    assert "tallyman revisions" in message, message
    assert "tallyman reset-to <step>" in message, message
    assert "shell" in message, message
    assert "catalog_reset_to" not in message, message
    assert "tallyman reset " not in message, message


@pytest.mark.parametrize("suffix", [".parquet", ".pq"], ids=["the same suffix", "another suffix"])
def test_a_re_import_restores_a_lost_clone_while_the_snapshot_exists(
    project: str, tmp_path: Path, monkeypatch, suffix: str
):
    """#239. Without its clone a source snapshot is the last copy of its rows, so it is pinned, and re-importing the
    bytes is the documented repair. The existing-entry branch restored the clone only when the snapshot was gone
    too, so the version stayed pinned. The clone restored is the one the entry names, whatever the suffix of the
    file the bytes come from this time."""
    from tallyman_xorq import source_import
    from tallyman_xorq.materialize import pinned_reason
    from tallyman_xorq.source_import import source_clone_path

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    clone = source_clone_path(project, read_manifest(entry_dir(project, out["hash"])).provenance)
    clone.unlink()
    assert pinned_reason(project, out["hash"]) is not None
    again = _outside(tmp_path) / f"orders_again{suffix}"
    again.write_bytes(src.read_bytes())

    repaired = source_import.update_and_depend(str(again), "orders", pinned_version=1)

    assert (repaired["hash"], repaired["version"], repaired["created"]) == (out["hash"], 1, False)
    assert clone.read_bytes() == src.read_bytes()
    assert pinned_reason(project, out["hash"]) is None
    assert _arena(project)["cas"] == [clone.name]


def test_the_type_check_passes_every_type_the_read_takes(project: str, tmp_path: Path, monkeypatch):
    """#224's check refuses only what the read refuses. These types all imported before it, including three
    extension types whose pyarrow schema ``PyArrowType.to_ibis`` has no entry for: DataFusion, which the read asks for
    the schema, gives a top-level extension column its storage type. A check over ``pq.read_schema`` would refuse
    them."""
    import datetime

    import numpy as np

    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _outside(tmp_path) / "typed.parquet"
    table = pa.table(
        {
            "binary": pa.array([b"ab"], type=pa.binary()),
            "large_binary": pa.array([b"ab"], type=pa.large_binary()),
            "dictionary": pa.array(["a"]).dictionary_encode(),
            "fixed_size_list": pa.array([[1, 2]], type=pa.list_(pa.int64(), 2)),
            "duration": pa.array([1], type=pa.duration("s")),
            "float16": pa.array([np.float16(1.5)], type=pa.float16()),
            "null": pa.array([None], type=pa.null()),
            "uint64": pa.array([2**63], type=pa.uint64()),
            "time64": pa.array([1], type=pa.time64("ns")),
            "tz": pa.array([datetime.datetime(2026, 1, 1)], type=pa.timestamp("us", tz="America/New_York")),
            "json": pa.ExtensionArray.from_storage(pa.json_(), pa.array(['{"a": 1}'])),
            "bool8": pa.ExtensionArray.from_storage(pa.bool8(), pa.array([1], type=pa.int8())),
            "tensor": pa.FixedShapeTensorArray.from_numpy_ndarray(np.zeros((1, 2, 2))),
        }
    )
    pq.write_table(table, src)

    out = source_import.update_and_depend(str(src), "typed")

    assert out["created"] is True
    assert [f["name"] for f in out["schema"]["fields"]] == [*table.column_names, ROW_ORDER]


def test_a_failed_import_keeps_a_clone_another_entry_uses(project: str, tmp_path: Path, monkeypatch):
    """#225's cleanup removes what the failed import wrote and nothing else. A CSV read two ways is two entries over
    one clone (ADR-011 D12), so a second reading that fails must leave the first entry's clone, and its pin, alone."""
    from tallyman_xorq import source_import
    from tallyman_xorq.materialize import pinned_reason

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    src = _write_csv(_outside(tmp_path) / "orders.csv", [("east", 1), ("west", 2)])
    first = source_import.update_and_depend(str(src), "orders")
    before = _arena(project)

    with pytest.raises(source_import.SourceImportError):
        source_import.update_and_depend(str(src), "orders_typed", schema={"region": "int64", "n": "int64"})

    assert _arena(project) == before
    assert _clone_of(project, first).read_bytes() == src.read_bytes()
    assert pinned_reason(project, first["hash"]) is None
