from __future__ import annotations

import json
from pathlib import Path

import pytest

from tallyman_core import entry_dir
from tallyman_xorq import BuildError, build_and_persist, list_entries


def _agg_code(parquet_path: Path) -> str:
    return f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet_path)!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _cheap_code(parquet_path: Path) -> str:  # parquet read + projection → cheap, bakes nothing
    return f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(parquet_path)!r})
expr = t.select("region", "price")
"""


def test_build_writes_entry(project: str, orders_parquet: Path):
    code = _agg_code(orders_parquet)
    res = build_and_persist(project, code, prompt="region totals")
    assert res.row_count == 4  # one row per region
    assert res.execute_seconds >= 0.0
    target = entry_dir(project, res.content_hash)
    assert target.is_dir()
    # #73: no build-time result.parquet — an expensive entry's rows live in its
    # baked cache, a cheap entry's in nothing. The durable per-entry artifacts
    # are the recipe, manifest, schema, and source.
    for child in ("manifest.json", "schema.json", "expr.py", "xorq_build"):
        assert (target / child).exists(), child
    assert not (target / "result.parquet").exists()
    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["content_hash"] == res.content_hash
    assert manifest["prompt"] == "region totals"
    assert manifest["row_count"] == 4


def test_migrate_drop_result_parquet_sweeps_legacy(project: str):
    # The upgrade migration deletes any per-entry on-demand result.parquet and the
    # dead #71 viewer-read build dirs, keeps the durable recipe, writes a marker,
    # and is idempotent (marker-gated).
    from tallyman_core.paths import catalog_dir
    from tallyman_xorq.build import _MIGRATION_MARKER, migrate_drop_result_parquet

    e1 = entry_dir(project, "aaaa")
    e1.mkdir(parents=True)
    (e1 / "result.parquet").write_bytes(b"old")
    (e1 / "xorq_build").mkdir()  # durable recipe must survive
    e2 = entry_dir(project, "bbbb")
    (e2 / ".xorq_result_build").mkdir(parents=True)
    (e2 / ".xorq_result_build_expanded").mkdir()

    assert migrate_drop_result_parquet(project) == 3
    assert not (e1 / "result.parquet").exists()
    assert (e1 / "xorq_build").is_dir()  # recipe kept
    assert not (e2 / ".xorq_result_build").exists()
    assert not (e2 / ".xorq_result_build_expanded").exists()
    assert (catalog_dir(project) / _MIGRATION_MARKER).exists()
    # A second run is a no-op (marker present).
    assert migrate_drop_result_parquet(project) == 0


def test_build_records_admission_instrumentation(project: str, orders_parquet: Path, caplog):
    # #87: the build records the structural cache_worthy verdict alongside the
    # measured value-per-byte inputs (compile_seconds, execute_seconds, snapshot
    # bytes), so the structural-vs-measured cache decision (#30) is decidable
    # from data rather than first principles. An expensive (Aggregate) entry is
    # structurally worthy and bakes a snapshot, and the decision is logged on the
    # tallyman.perf namespace with both verdicts side by side.
    import logging

    with caplog.at_level(logging.INFO, logger="tallyman.perf"):
        res = build_and_persist(project, _agg_code(orders_parquet))

    m = json.loads((entry_dir(project, res.content_hash) / "manifest.json").read_text())
    assert m["compile_seconds"] is not None and m["compile_seconds"] >= 0.0
    assert m["execute_seconds"] >= 0.0
    assert m["cache_worthy"] is True
    assert "Aggregate" in m["cache_worthy_why"]
    assert m["cache_bytes"] is not None and m["cache_bytes"] > 0
    # Mirrored on the BuildResult for in-process callers.
    assert res.cache_worthy is True
    assert res.compile_seconds is not None and res.compile_seconds >= 0.0
    assert res.cache_bytes is not None and res.cache_bytes > 0
    # The admission decision is logged once, on the perf namespace.
    perf = [r for r in caplog.records if r.name == "tallyman.perf"]
    assert any("admission" in r.getMessage() and res.content_hash in r.getMessage() for r in perf)


def test_build_cheap_entry_records_no_snapshot_bytes(project: str, orders_parquet: Path):
    # A cheap (projection) entry is structurally not worthy and bakes no
    # snapshot, so cache_bytes is absent — it materialises nothing to size. The
    # compile/structural verdict is still recorded for the shadow comparison.
    res = build_and_persist(project, _cheap_code(orders_parquet))
    m = json.loads((entry_dir(project, res.content_hash) / "manifest.json").read_text())
    assert m["cache_worthy"] is False
    assert "cheap" in m["cache_worthy_why"]
    assert m["cache_bytes"] is None
    assert m["compile_seconds"] is not None
    assert res.cache_worthy is False
    assert res.cache_bytes is None


def test_build_idempotent_same_code(project: str, orders_parquet: Path):
    code = _agg_code(orders_parquet)
    a = build_and_persist(project, code, prompt="first")
    b = build_and_persist(project, code, prompt="second")
    assert a.content_hash == b.content_hash
    assert a.entry_path == b.entry_path
    assert len(list_entries(project)) == 1


def test_build_distinct_code_distinct_hash(project: str, orders_parquet: Path):
    code_a = _agg_code(orders_parquet)
    code_b = f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(orders_parquet)!r})
filtered = t.filter(t.category == "boots")
expr = filtered.group_by("region").aggregate(n=filtered.count())
"""
    a = build_and_persist(project, code_a)
    b = build_and_persist(project, code_b)
    assert a.content_hash != b.content_hash
    assert len(list_entries(project)) == 2


def test_build_missing_expr_variable(project: str, orders_parquet: Path):
    code = "import xorq.api as xo\nx = 1\n"
    with pytest.raises(BuildError, match="not found"):
        build_and_persist(project, code)


def test_build_user_code_raises(project: str):
    code = "raise ValueError('boom')"
    with pytest.raises(BuildError, match="boom"):
        build_and_persist(project, code)


def test_list_entries_empty(project: str):
    assert list_entries(project) == []


def test_build_hints_bare_ibis_import(project: str, orders_parquet: Path):
    # `import ibis` triggers an AttributeError at user-code exec time because
    # the real ibis package doesn't expose all the methods xorq does. The
    # BuildError should include a hint pointing at xorq.vendor.ibis.
    code = f"""
import xorq.api as xo
import ibis
t = xo.deferred_read_parquet({str(orders_parquet)!r})
expr = t.mutate(flag=ibis.case().when(t.price > 50, "hi").else_("lo").end())
"""
    with pytest.raises(BuildError, match="import xorq.vendor.ibis as ibis"):
        build_and_persist(project, code)


def test_build_no_hint_on_unrelated_error(project: str, orders_parquet: Path):
    code = f"""
import xorq.api as xo
import xorq.vendor.ibis as ibis
t = xo.deferred_read_parquet({str(orders_parquet)!r})
expr = t.nonexistent_column.sum()
"""
    with pytest.raises(BuildError) as excinfo:
        build_and_persist(project, code)
    assert "import xorq.vendor.ibis as ibis" not in str(excinfo.value).split("Traceback")[0]


# ---------------------------------------------------------------------------
# Namespace / column-name hints. The session scan showed the model repeatedly
# reaching for `xorq.<fn>` (the api lives on `xorq.api`/`xo`), `ibis.<fn>` for
# things that are column methods or live elsewhere, inventing `tallyman_xorq.io`
# helpers, guessing column names, and reaching for a duckdb backend. The old
# hint only caught bare `import ibis`, the Expr class mismatch, and `xorq._`.
# ---------------------------------------------------------------------------

from tallyman_xorq.build import _ibis_import_hint  # noqa: E402


def test_hint_bare_xorq_attribute_points_to_api():
    h = _ibis_import_hint("module 'xorq' has no attribute 'memtable'")
    assert "xorq.api" in h


def test_hint_ibis_read_parquet_points_to_loaders():
    h = _ibis_import_hint("module 'ibis' has no attribute 'read_parquet'")
    assert "from_project" in h


def test_hint_ibis_math_func_is_column_method():
    h = _ibis_import_hint("module 'ibis' has no attribute 'sin'")
    assert ".sin()" in h


def test_hint_io_import_typo_lists_real_exports():
    h = _ibis_import_hint("cannot import name 'load_parquet_expr' from 'tallyman_xorq.io'")
    assert "from_project" in h and "from_catalog" in h


def test_hint_missing_table_column():
    h = _ibis_import_hint("'Table' object has no attribute 'trip_duration'")
    assert "not a column" in h.lower()
    assert "trip_duration" in h


def test_hint_duckdb_reach_points_to_datafusion():
    h = _ibis_import_hint("No module named 'duckdb'")
    assert "datafusion" in h.lower()


def test_build_hints_bare_xorq_namespace(project: str, orders_parquet: Path):
    # End-to-end: a bare `import xorq` + `xorq.deferred_read_parquet(...)` must
    # steer the model to `xorq.api` through the real build path, not just the
    # unit-tested helper.
    code = f"""
import xorq
t = xorq.deferred_read_parquet({str(orders_parquet)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    with pytest.raises(BuildError, match="xorq.api"):
        build_and_persist(project, code)


# ---------------------------------------------------------------------------
# Nondeterminism lint (#88). Execution-nondeterministic ops (now / random /
# uuid / sample) make an entry's result vary run-to-run under one structural
# content_hash; #74's recompute-on-cold-read then serves different bytes than
# were built, and the hash never notices. build_and_persist surfaces an
# advisory lint, not a hard stop.
# ---------------------------------------------------------------------------


def _nondeterministic_code(parquet_path: Path) -> str:
    return f"""
import xorq.api as xo
import xorq.vendor.ibis as ibis
t = xo.deferred_read_parquet({str(parquet_path)!r})
expr = t.mutate(built_at=ibis.now())
"""


def test_build_lints_nondeterministic_now(project: str, orders_parquet: Path):
    res = build_and_persist(project, _nondeterministic_code(orders_parquet))
    joined = " ".join(res.lint_warnings)
    assert res.lint_warnings, "expected a nondeterminism lint warning"
    assert "now()" in joined
    assert "#88" in joined


def test_build_no_lint_for_deterministic_recipe(project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(orders_parquet))
    assert res.lint_warnings == []


def test_nondeterminism_warnings_lists_friendly_ops():
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.build import _nondeterminism_warnings

    t = ibis.table({"x": "int64"}, name="t")
    expr = t.mutate(r=ibis.random(), n=ibis.now())
    joined = " ".join(_nondeterminism_warnings(expr))
    assert "random()" in joined
    assert "now()" in joined


def test_nondeterminism_warnings_empty_for_deterministic():
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.build import _nondeterminism_warnings

    t = ibis.table({"x": "int64"}, name="t")
    expr = t.mutate(y=t.x + 1)
    assert _nondeterminism_warnings(expr) == []


def test_nondeterminism_warnings_skips_seeded_sample():
    # A seeded sample is reproducible run-to-run, so it must not be flagged;
    # the lint's "seed the sample" advice would otherwise contradict itself (#88).
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.build import _nondeterminism_warnings

    t = ibis.table({"x": "int64"}, name="t")
    assert _nondeterminism_warnings(t.sample(0.5)) != []
    assert _nondeterminism_warnings(t.sample(0.5, seed=42)) == []
