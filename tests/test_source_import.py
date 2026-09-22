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

    from tallyman_xorq.ordered_copy import ordered_copies_dir

    assert not ordered_copies_dir(project).exists(), "an import writes no separate ordered copy"
    manifest = read_manifest(entry_dir(project, out["hash"]))
    assert manifest.ordered_copies is None
    assert manifest.cache_worthy is True


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
# D1 / ADR-007 D13 — the snapshot is DATA, not cache
# ---------------------------------------------------------------------------


def test_ensure_materialized_does_not_rebuild_a_source_snapshot(project: str, tmp_path: Path, monkeypatch):
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.materialize import ensure_materialized

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")
    snapshot_path(project, out["hash"]).unlink()

    with pytest.raises(BuildError) as exc:
        ensure_materialized(project, out["hash"])

    assert "orders" in str(exc.value)
    assert not snapshot_path(project, out["hash"]).exists(), "a source snapshot is data; nothing re-creates it"


def test_the_cache_page_does_not_offer_to_delete_a_source_snapshot(project: str, tmp_path: Path, monkeypatch):
    from tallyman_xorq import source_import

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.materialize import pinned_reason

    src = _write_parquet(_outside(tmp_path) / "orders.parquet", 10)
    out = source_import.update_and_depend(str(src), "orders")

    reason = pinned_reason(project, out["hash"])
    assert reason is not None
    assert "source" in reason.lower()


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
