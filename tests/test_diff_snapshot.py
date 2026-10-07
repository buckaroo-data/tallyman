"""The live diff writes its join to one parquet file and hands Buckaroo a build that reads that file.

Buckaroo runs its summary stats, pages and sorts over whatever it is handed. Handed the outer join, it ran the join for
every one of them. Handed a file, its whole plan is one bare read, so the join runs once, in tallyman, when the pair is
first opened (``plans/diff-performance-proposals.md``, section 4a).
"""

from __future__ import annotations

import threading
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from tallyman_mcp.server import catalog_create, catalog_revise


def _agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _filtered_agg_code(project: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(total=filtered.price.sum(), n=filtered.count())
"""


def _rows_code(project: str, min_qty: int) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.filter(t.qty > {min_qty})
"""


def _worthy_pair(project: str) -> None:
    """``shoe_sales`` V1 and V2 are aggregates, so both sides have snapshots. The join key is ``region``."""
    catalog_create("shoe_sales", _agg_code(project))
    catalog_revise("shoe_sales", _filtered_agg_code(project))


def _cheap_pair(project: str) -> None:
    """``rows`` V1 and V2 are filters over the source, so neither side has a snapshot. The join key is ``order_id``."""
    catalog_create("rows", _rows_code(project, 0))
    catalog_revise("rows", _rows_code(project, 1))


class _Resp:
    status_code = 200


def _buckaroo(posted: list[dict]):
    """A Buckaroo manager that is running and answers every ``/load_expr`` with 200, recording the bodies."""
    from tallyman_companion.buckaroo_lifecycle import BuckarooManager

    class _Client:
        def post(self, url, json=None, timeout=None):
            posted.append(json)
            return _Resp()

    mgr = BuckarooManager()
    mgr.bound_port = 65000
    mgr.proc = type("FakeProc", (), {"poll": staticmethod(lambda: None)})()
    mgr._client = _Client()
    return mgr


def _open_diff(project: str, alias: str, posted: list[dict] | None = None, *, buckaroo: bool = True) -> dict:
    """GET the diff route for V1 to V2 of *alias*. A fake Buckaroo is up by default, and its posts go to *posted*."""
    from tallyman_companion import create_app

    mgr = _buckaroo(posted if posted is not None else []) if buckaroo else None
    r = TestClient(create_app(project, buckaroo=mgr)).get(f"/{project}/api/diff_data/{alias}/1/2")
    assert r.status_code == 200, r.text
    return r.json()


def _diff_cache(project: str) -> Path:
    from tallyman_core.paths import compute_cache_dir

    return compute_cache_dir(project) / "diff_cache"


def _diff_files(project: str) -> list[Path]:
    return sorted(_diff_cache(project).glob("*.parquet"))


def _ops(build_dir: str) -> list[str]:
    """Every ``op`` named in a build's ``expr.yaml``."""
    found: list[str] = []

    def walk(node):
        if isinstance(node, dict):
            if isinstance(node.get("op"), str):
                found.append(node["op"])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(yaml.safe_load((Path(build_dir) / "expr.yaml").read_text()))
    return found


@pytest.fixture
def writer_calls(monkeypatch) -> list[Path]:
    """Where each diff file was written to, one entry per run of the snapshot writer, while the test runs."""
    import tallyman_xorq.materialize as m

    real = m._stream_to_parquet
    calls: list[Path] = []

    def spy(expr, dest):
        if Path(dest).parent.name == "diff_cache":
            calls.append(Path(dest))
        return real(expr, dest)

    monkeypatch.setattr(m, "_stream_to_parquet", spy)
    return calls


@pytest.mark.parametrize("pair", [_worthy_pair, _cheap_pair], ids=["worthy-sides", "cheap-sides"])
def test_buckaroo_is_handed_one_bare_read_of_the_diff_file(project: str, orders_src: str, monkeypatch, pair):
    """The build Buckaroo loads has no join, aggregate or case expression in it: one read, of the diff file."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    pair(project)
    alias = "shoe_sales" if pair is _worthy_pair else "rows"
    posted: list[dict] = []

    body = _open_diff(project, alias, posted)

    assert body["compare_session"], "the compare view must have loaded"
    assert len(posted) == 1
    ops = _ops(posted[0]["build_dir"])
    assert ops.count("Read") == 1
    assert set(ops) <= {"Read", "DataType"}, f"the build is more than one read: {sorted(set(ops))}"
    files = _diff_files(project)
    assert len(files) == 1
    assert str(files[0]) in (Path(posted[0]["build_dir"]) / "expr.yaml").read_text()


def test_diff_file_holds_the_rows_of_the_compare_join(project: str, orders_src: str, monkeypatch):
    """The file has the compare view's columns, every joined row, and ``membership`` for each."""
    from tallyman_companion.diff import build_compare_expr
    from tallyman_core.aliases import history_for
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)
    _open_diff(project, "shoe_sales")

    a_hash, b_hash = history_for(project, "shoe_sales")
    a_expr, b_expr = cached_result_expr(project, a_hash), cached_result_expr(project, b_hash)
    expected, _ = build_compare_expr(a_expr, b_expr, ["region"])
    want = expected.execute().sort_values("region").reset_index(drop=True)
    (path,) = _diff_files(project)
    got = pq.read_table(path).to_pandas()
    assert list(got.columns[:-1]) == list(want.columns)
    assert got.columns[-1] == "__row_order"
    got = got.drop(columns="__row_order").sort_values("region").reset_index(drop=True)
    pd.testing.assert_frame_equal(got, want, check_dtype=False)
    assert set(got["membership"]) <= {1, 2, 3}


def test_keyed_counts_come_from_the_file_and_full_diff_does_not_run(project: str, orders_src: str, monkeypatch):
    """With the view loaded the page needs three counts and the two file diffs. The stats table, the head and the
    50-row preview besides would be 8 queries nobody sees."""
    import tallyman_companion.app as app_mod
    from tallyman_core.aliases import history_for
    from tallyman_xorq.result_cache import cached_result_expr

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)

    def _no_full_diff(*args, **kwargs):
        raise AssertionError("full_diff must not run when Buckaroo has the compare view")

    monkeypatch.setattr(app_mod, "full_diff", _no_full_diff)
    body = _open_diff(project, "shoe_sales")

    a_hash, b_hash = history_for(project, "shoe_sales")
    a = cached_result_expr(project, a_hash).execute()
    b = cached_result_expr(project, b_hash).execute()
    merged = a[["region"]].merge(b[["region"]], on="region", how="outer", indicator=True)
    diff = body["diff"]
    assert diff["keyed"]["keys"] == ["region"]
    assert diff["keyed"]["matched"] == int((merged["_merge"] == "both").sum())
    assert diff["keyed"]["only_before"] == int((merged["_merge"] == "left_only").sum())
    assert diff["keyed"]["only_after"] == int((merged["_merge"] == "right_only").sum())
    assert diff["schema"]["row_count"] == {"before": len(a), "after": len(b)}
    assert "highlight" in diff["code"]
    # The shape the page's TypeScript expects stays whole, so a page that falls back to the static tables never throws.
    assert diff["stats"] == []
    assert set(diff["head"]) == {"n", "a_total", "b_total", "before", "after"}


def test_second_open_reuses_the_file_and_the_build(project: str, orders_src: str, monkeypatch, writer_calls):
    """One run of the join per pair, whatever opens it, and one build directory, since Buckaroo's stat cache is keyed by
    the build's path."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)
    writer_calls.clear()
    first_posts: list[dict] = []
    second_posts: list[dict] = []

    first = _open_diff(project, "shoe_sales", first_posts)
    (path,) = _diff_files(project)
    stamp = (path.stat().st_ino, path.stat().st_mtime_ns)
    second = _open_diff(project, "shoe_sales", second_posts)  # a new manager, so Buckaroo is asked again

    assert len(writer_calls) == 1
    assert (path.stat().st_ino, path.stat().st_mtime_ns) == stamp
    assert first_posts[0]["build_dir"] == second_posts[0]["build_dir"]
    assert first["diff"]["keyed"] == second["diff"]["keyed"]
    assert first["compare_session"] == second["compare_session"]


def test_concurrent_first_opens_write_the_file_once(project: str, orders_src: str, monkeypatch):
    """Two tabs opening the same new pair run the join once. The second waits for the first's file."""
    import time

    import tallyman_xorq.materialize as m

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)
    real = m._stream_to_parquet
    writes: list[Path] = []

    def slow(expr, dest):
        if Path(dest).parent.name == "diff_cache":
            writes.append(Path(dest))
            time.sleep(0.5)  # long enough that every thread below has reached the writer's door
        return real(expr, dest)

    monkeypatch.setattr(m, "_stream_to_parquet", slow)
    errors: list[BaseException] = []

    def open_it():
        try:
            _open_diff(project, "shoe_sales")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=open_it) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    assert len(writes) == 1
    assert len(_diff_files(project)) == 1


def test_failed_write_leaves_no_file_and_the_static_diff_is_served(project: str, orders_src: str, monkeypatch):
    """A writer that dies half way leaves nothing that looks like a diff file, the page gets its static tables, and the
    next open makes the file."""
    import tallyman_xorq.materialize as m

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)
    real = m._stream_to_parquet

    def dies(expr, dest):
        if Path(dest).parent.name == "diff_cache":
            Path(dest).write_bytes(b"PAR1 half a file")
            raise OSError("disk full")
        return real(expr, dest)

    monkeypatch.setattr(m, "_stream_to_parquet", dies)
    posted: list[dict] = []
    body = _open_diff(project, "shoe_sales", posted)

    assert body["compare_session"] is None
    assert posted == []
    assert body["diff"]["stats"] and body["diff"]["keyed"]["table_html"]
    leftovers = list(_diff_cache(project).glob("*")) if _diff_cache(project).exists() else []
    assert leftovers == []

    monkeypatch.setattr(m, "_stream_to_parquet", real)
    body = _open_diff(project, "shoe_sales", posted)
    assert body["compare_session"]
    assert len(_diff_files(project)) == 1


def _set_heal_digest(project: str, content_hash: str, digest: str) -> None:
    from tallyman_core import entry_dir, read_manifest, write_manifest

    d = entry_dir(project, content_hash)
    write_manifest(d, read_manifest(d).model_copy(update={"unfaithful_heal_digest": digest}))


def test_a_heal_of_either_side_makes_a_new_file_and_a_new_session(project: str, orders_src: str, monkeypatch):
    """An unfaithful heal rewrites a snapshot under the same content hash. The diff of the new rows is another file, and
    another Buckaroo session, so neither the file nor the session's cells are the old ones."""
    from tallyman_core.aliases import history_for

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)
    posted: list[dict] = []
    first = _open_diff(project, "shoe_sales", posted)
    _, b_hash = history_for(project, "shoe_sales")

    _set_heal_digest(project, b_hash, "arrow-sha256:healed")
    second = _open_diff(project, "shoe_sales", posted)

    assert len(_diff_files(project)) == 2
    assert posted[0]["build_dir"] != posted[1]["build_dir"]
    assert first["compare_session"] != second["compare_session"]
    assert posted[0]["session"] == first["compare_session"]
    assert posted[1]["session"] == second["compare_session"]


def test_a_heal_of_a_parent_of_a_cheap_side_makes_a_new_file(project: str, orders_src: str, monkeypatch):
    """A cheap entry reads its parent's snapshot, so its rows change when the parent's file does."""
    from tallyman_core import entry_dir, read_manifest
    from tallyman_core.aliases import history_for

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _cheap_pair(project)
    _open_diff(project, "rows")
    assert len(_diff_files(project)) == 1
    _, b_hash = history_for(project, "rows")
    (parent,) = read_manifest(entry_dir(project, b_hash)).parents

    _set_heal_digest(project, parent.hash, "arrow-sha256:healed-parent")
    _open_diff(project, "rows")

    assert len(_diff_files(project)) == 2


def test_the_diff_grid_hides_row_order_and_keeps_its_other_overrides(project: str, orders_src: str, monkeypatch):
    """The file ends in ``__row_order`` as every file tallyman writes does. The compare grid never showed one."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)
    posted: list[dict] = []
    _open_diff(project, "shoe_sales", posted)

    body = posted[0]
    overrides = body["column_config_overrides"]
    assert overrides["__row_order"] == {"merge_rule": "hidden"}
    assert overrides["membership"] == {"merge_rule": "hidden"}
    assert overrides["region"]["color_map_config"]["val_column"] == "membership"
    assert body["session"] == _open_diff(project, "shoe_sales")["compare_session"]
    assert body["extra_grid_config"] == {"searchDebounceMs": 3000}
    assert body["project_root"].endswith("diff_extras")
    assert Path(body["cache_storage_path"]).parent.name == "diff_stat_cache"


def test_without_buckaroo_the_static_diff_is_whole_and_no_file_is_written(project: str, orders_src: str, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    _worthy_pair(project)

    body = _open_diff(project, "shoe_sales", buckaroo=False)

    assert body["compare_session"] is None
    assert body["diff"]["stats"] and body["diff"]["head"]["before"]
    assert body["diff"]["keyed"]["table_html"]
    assert not _diff_cache(project).exists()


def test_a_pair_with_no_key_stays_static_with_buckaroo_up(project: str, orders_src: str, monkeypatch):
    """No key, no join: nothing to write, nothing to post."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    dup = f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
u = t.union(t, distinct=False)
expr = u
"""
    catalog_create("dups", dup)
    catalog_revise("dups", dup.replace("expr = u", "expr = u.filter(u.qty > 1)"))
    posted: list[dict] = []

    body = _open_diff(project, "dups", posted)

    assert body["compare_session"] is None
    assert posted == []
    assert body["diff"]["keyed"] is None
    assert not _diff_cache(project).exists()
