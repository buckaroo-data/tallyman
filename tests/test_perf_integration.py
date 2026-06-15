"""Tier-B perf integration harness over the real parking-ticket corpus.

Issue #59 (Stage 2 of the catalog-perf work) asks for a two-tier measurement of
what a user actually waits for when opening a large catalog entry:

* **Tier A — Playwright (ground truth).** Drive a real browser against the
  catalog + Buckaroo server and measure to data-grid-visible. Heavy and flaky to
  run routinely. *Deferred to a follow-up* (the `pw-tests/server-buckaroo-*`
  pattern in buckaroo-js-core is the template).
* **Tier B — python-only proxy (this module).** Call the same backend the
  Buckaroo ``/load_expr`` (``mode=buckaroo``) handler dispatches to —
  ``xorq_loading.load_expr_build_dir`` + the ``XorqServerDataflow`` summary-stat
  pipeline — with no server and no browser. Fast and deterministic; this is what
  runs in the suite.

What this PR measures, against the **existing on-disk corpus** (reuse, don't
rebuild), each across a cold/warm cache axis:

1. **Execution time** — ``load_entry`` then ``to_parquet`` per entry.
2. **Page load (Tier B proxy)** — the dominant user-visible cost behind #34's
   10s ``/load_expr`` timeout: load the xorq expr + compute the summary stats,
   then serve the first row page (the ``infinite_request`` 0-300 window).
3. **Diff stat path** — #45's 27s-on-first-open: ``diff_keys`` → the
   ``build_compare_expr`` outer-join → the same stat pipeline, over the
   consecutive revision pairs in the corpus's alias history (``catalog.yaml``'s
   ``alias_history`` key, or a legacy ``alias_history.json``).

Cold/warm semantics. Every measurement runs against an **overlay home**: the
real entries, source ``data``, and catalog bookkeeping are symlinked read-only,
but ``compute_cache`` / ``result_cache`` / the Buckaroo stat cache are fresh,
isolated temp dirs. So "cold" is a genuine empty-cache first hit and "warm"
reuses the on-disk caches that survive a process/server restart — *without ever
wiping or mutating the user's real project caches*. (Warm also benefits from
this process's in-memory memoization, which mirrors the long-lived companion
server; a fork-per-measurement cold-process refinement and precise per-entry
peak RSS are what ``scripts/perf_report.py`` already does and are left there.)

Counts over wall-clock. The issue's premise is that wall-clock is dictated by
*how many* expressions are built / loaded / executed, not by the cache-key
strategy — so a regression must be attributable to a changed count. This harness
records the loads/executes/stat-runs it issues alongside each wall number; the
richer in-app build/load/execute event stream is #60's instrumentation-
consolidation work, which this harness will read once it lands.

Deferred to follow-ups: Tier A Playwright; the full-vs-truncated corpus variants
(needs the Stage 1 ``scripts/rebuild_parking_catalog.py`` rebuild on
``perf/parking-catalog-rebuild`` to land and a ``head(1M)`` raw-source truncate);
the Tier-A/Tier-B calibration ratio; promoting #45's diff time to a regression
gate once baselined.

Local-only, marker-gated (``perf``), and skipped when the corpus is absent, so
it never runs on CI. Run it explicitly:

    uv run pytest -m perf tests/test_perf_integration.py -s

Override the corpus with ``TALLYMAN_PERF_PROJECT`` / ``TALLYMAN_PERF_ENTRIES``
(comma-separated hashes or aliases) and write the markdown report somewhere
durable with ``TALLYMAN_PERF_OUT``. Point ``TALLYMAN_PERF_PROJECT`` at the
truncated corpus (``parking_ticket_analysis_1m``, built by the rebuild script
with ``--rows 1000000``) for the fast same-DAG variant.
"""

from __future__ import annotations

import importlib.util
import json
import os
import time
from pathlib import Path

import psutil
import pytest

from tallyman_core.paths import (
    ENTRY_ARTIFACT_NAMES,
    ENTRY_BUILD_DIRNAME,
    ENTRY_CACHE_NAMES,
    ENTRY_RESULT_FILENAME,
)

pytestmark = pytest.mark.perf

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROJECT = "parking_ticket_analysis"
# The per-entry artifacts symlinked read-only into an overlay entry: the single
# source of truth is paths.ENTRY_ARTIFACT_NAMES, not a tuple maintained here.
# Everything an entry accrues that *isn't* an artifact — the per-entry caches
# (paths.ENTRY_CACHE_NAMES: .xorq_build_expanded, .buckaroo_stat_cache) plus the
# pk cache — is deliberately omitted, so it's written fresh into the writable
# overlay dir and the first hit is honestly cold.
_ENTRY_LINK_NAMES = ENTRY_ARTIFACT_NAMES
assert set(_ENTRY_LINK_NAMES).isdisjoint(ENTRY_CACHE_NAMES)  # no artifact is also a cache
# Catalog bookkeeping the read paths need; the caches are deliberately omitted so
# the overlay starts cold.
_CATALOG_LINK_NAMES = ("aliases.json", "alias_history.json", "catalog.yaml", "aliases", "chart_specs", "metadata")


# ---------------------------------------------------------------------------
# reuse the report-only suite's measurement helpers
# ---------------------------------------------------------------------------


def _load_perf_report():
    """Import ``scripts/perf_report.py`` as a module (it's a script, not a package).

    Reuses ``RssSampler`` / ``MB`` / ``_dir_kb_and_files`` rather than copying
    them, so the two perf surfaces can't drift.
    """
    path = REPO_ROOT / "scripts" / "perf_report.py"
    spec = importlib.util.spec_from_file_location("perf_report", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


pr = _load_perf_report()
MB = pr.MB


# ---------------------------------------------------------------------------
# corpus discovery + safe overlay
# ---------------------------------------------------------------------------


def _real_projects_root() -> Path:
    """The real on-disk projects root (TALLYMAN_HOME before we override it)."""
    home = Path(os.environ.get("TALLYMAN_PERF_HOME", Path.home() / ".tallyman-notebooks"))
    return home / "projects"


def _entries_with_build(project_dir: Path) -> list[str]:
    base = project_dir / "artifacts" / "catalog" / "entries"
    if not base.is_dir():
        return []
    return sorted(
        child.name
        for child in base.iterdir()
        if (child / ENTRY_BUILD_DIRNAME).is_dir() and (child / ENTRY_RESULT_FILENAME).exists()
    )


def _resolve_corpus_or_skip() -> tuple[str, Path, list[str]]:
    """Locate the corpus or skip — this keeps the harness inert on CI.

    Returns ``(project, real_project_dir, hashes)`` where *hashes* honours a
    ``TALLYMAN_PERF_ENTRIES`` subset (hashes or aliases) when set.
    """
    project = os.environ.get("TALLYMAN_PERF_PROJECT", DEFAULT_PROJECT)
    real_dir = _real_projects_root() / project
    if not real_dir.is_dir():
        pytest.skip(f"perf corpus absent: {real_dir} (build it with scripts/rebuild_parking_catalog.py)")
    hashes = _entries_with_build(real_dir)
    if not hashes:
        pytest.skip(f"perf corpus {project} has no built entries")

    subset = os.environ.get("TALLYMAN_PERF_ENTRIES")
    if subset:
        aliases = _load_aliases(real_dir)
        wanted = []
        for token in (t.strip() for t in subset.split(",") if t.strip()):
            resolved = aliases.get(token, token)
            if resolved in hashes:
                wanted.append(resolved)
            else:
                pytest.skip(f"requested entry {token!r} ({resolved}) has no build in {project}")
        hashes = wanted
    return project, real_dir, hashes


def _load_aliases(real_dir: Path) -> dict[str, str]:
    p = real_dir / "artifacts" / "catalog" / "aliases.json"
    return json.loads(p.read_text()) if p.exists() else {}


def _load_alias_history(real_dir: Path) -> dict[str, list[str]]:
    """Alias revision chains, from whichever store the corpus uses.

    Pre-#57 catalogs kept these in ``alias_history.json``; a corpus rebuilt
    through the fixed machinery (``scripts/rebuild_parking_catalog.py``) carries
    them as the ``alias_history`` key in ``catalog.yaml`` (#57 moved alias
    bookkeeping off separate files). Read the legacy file if present, else fall
    back to the catalog.yaml key.
    """
    catalog = real_dir / "artifacts" / "catalog"
    legacy = catalog / "alias_history.json"
    if legacy.exists():
        return json.loads(legacy.read_text())
    catalog_yaml = catalog / "catalog.yaml"
    if catalog_yaml.exists():
        import yaml  # noqa: PLC0415

        data = yaml.safe_load(catalog_yaml.read_text()) or {}
        return data.get("alias_history", {}) or {}
    return {}


def _diff_pairs(real_dir: Path, built: set[str]) -> list[tuple[str, str, str]]:
    """Consecutive (alias, prev_hash, cur_hash) revisions with both sides built.

    This is the revision-pair definition #45 cares about: re-opening an entry's
    diff against its immediate predecessor.
    """
    pairs: list[tuple[str, str, str]] = []
    for alias, history in _load_alias_history(real_dir).items():
        for prev, cur in zip(history, history[1:]):
            if prev in built and cur in built:
                pairs.append((alias, prev, cur))
    return pairs


def _link(src: Path, dst: Path) -> None:
    if src.exists() or src.is_symlink():
        dst.symlink_to(src)


def _build_overlay(overlay_home: Path, project: str, real_dir: Path) -> Path:
    """Construct a write-isolated overlay of *project* under *overlay_home*.

    Big, immutable artifacts (``xorq_build``, ``result.parquet``, the source
    ``data``, catalog bookkeeping, project-authored stats) are symlinked so no
    bytes are copied; the per-project caches are left empty so the first hit is
    honestly cold. Anything an entry writes lands in the writable overlay dir,
    so the user's real catalog is never mutated. Returns *overlay_home*.

    *Every* built entry is mirrored, not just the measured subset — entries
    chain (``from_catalog(parent_hash)``), so a measured entry's build can read
    a parent entry's ``result.parquet``; omitting parents breaks the path
    resolution at execute time.
    """
    proj = overlay_home / "projects" / project
    cat = proj / "artifacts" / "catalog"
    (cat / "entries").mkdir(parents=True)

    _link(real_dir / "data", proj / "data")
    for name in ("stats", "post_processing", "display"):
        _link(real_dir / "artifacts" / name, proj / "artifacts" / name)
    for name in _CATALOG_LINK_NAMES:
        _link(real_dir / "artifacts" / "catalog" / name, cat / name)

    real_entries = real_dir / "artifacts" / "catalog" / "entries"
    for real_entry in real_entries.iterdir():
        if not (real_entry / ENTRY_BUILD_DIRNAME).is_dir():
            continue
        overlay_entry = cat / "entries" / real_entry.name
        overlay_entry.mkdir()
        for name in _ENTRY_LINK_NAMES:
            _link(real_entry / name, overlay_entry / name)
    return overlay_home


# ---------------------------------------------------------------------------
# measurement primitives (Tier B — in-process, no server, no browser)
# ---------------------------------------------------------------------------


def _import_buckaroo_loading():
    from buckaroo.server import xorq_loading  # noqa: PLC0415

    return xorq_loading


def _stats_dataflow(xorq_loading, expr, cache_dir: Path):
    """Run the summary-stat pipeline exactly as ``LoadExprHandler.post`` does.

    The expensive work happens inside ``XorqServerDataflow.__init__`` (it calls
    ``populate_df_meta`` → the ``XorqDfStatsV2`` pipeline); ``get_xorq_metadata``
    just reads the schema + row count off the result.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    df = xorq_loading.XorqServerDataflow(expr, skip_main_serial=True, cache_storage_path=str(cache_dir))
    meta = xorq_loading.get_xorq_metadata(df, "perf")
    return df, meta


def measure_execute(project: str, content_hash: str, scratch: Path) -> dict:
    """``load_entry`` → ``to_parquet``, cold then warm (compute cache)."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    from tallyman_xorq.build import load_entry  # noqa: PLC0415

    out = scratch / f"exec_{content_hash}.parquet"

    sampler = pr.RssSampler(psutil.Process(), interval=0.05)
    t0 = time.monotonic()
    load_entry(project, content_hash).to_parquet(str(out))
    cold_s = round(time.monotonic() - t0, 2)
    rows = pq.ParquetFile(str(out)).metadata.num_rows

    t0 = time.monotonic()
    load_entry(project, content_hash).to_parquet(str(out))
    warm_s = round(time.monotonic() - t0, 2)
    sampler.stop()

    return {
        "entry": content_hash,
        "rows": rows,
        "cold_s": cold_s,
        "warm_s": warm_s,
        "peak_mb": round(sampler.peak() / MB),
        # Each timing issues exactly one load + one execute; the headline number
        # is attributable to that fixed count (#59's count-over-wall premise).
        "loads": 1,
        "executes": 1,
    }


def measure_pageload(project: str, content_hash: str, scratch: Path) -> dict:
    """Tier-B proxy: load the build, run the stat pipeline (cold/warm), 1st page."""
    from tallyman_core.paths import (  # noqa: PLC0415
        entry_build_dir,
        entry_expanded_build_dir,
        entry_stat_cache_dir,
        project_dir,
    )
    from tallyman_xorq.portable import ensure_expanded_build  # noqa: PLC0415

    xorq_loading = _import_buckaroo_loading()
    build_dir = entry_build_dir(project, content_hash)
    expanded = ensure_expanded_build(
        build_dir, project_dir(project), entry_expanded_build_dir(project, content_hash)
    )
    stat_cache = entry_stat_cache_dir(project, content_hash)

    sampler = pr.RssSampler(psutil.Process(), interval=0.05)
    t0 = time.monotonic()
    expr = xorq_loading.load_expr_build_dir(str(expanded))
    load_s = round(time.monotonic() - t0, 2)

    t0 = time.monotonic()
    _df, meta = _stats_dataflow(xorq_loading, expr, stat_cache)
    cold_stats_s = round(time.monotonic() - t0, 2)
    cache_kb, cache_files = pr._dir_kb_and_files(stat_cache)

    # Warm: a fresh dataflow re-reading the now-populated on-disk stat cache —
    # the path a server restart takes. warm ≈ cold means the cache is unused
    # (the live empty-cache failure behind #34).
    t0 = time.monotonic()
    df2, _ = _stats_dataflow(xorq_loading, xorq_loading.load_expr_build_dir(str(expanded)), stat_cache)
    warm_stats_s = round(time.monotonic() - t0, 2)

    # First row page — the infinite_request 0-300 window the grid asks for.
    t0 = time.monotonic()
    _msg, parquet_bytes = xorq_loading.handle_infinite_request_xorq(
        df2, {"start": 0, "end": 300, "sort": None, "sort_direction": None}
    )
    first_page_s = round(time.monotonic() - t0, 2)
    sampler.stop()

    return {
        "entry": content_hash,
        "rows": meta["rows"],
        "cols": len(meta["columns"]),
        "load_s": load_s,
        "cold_stats_s": cold_stats_s,
        "warm_stats_s": warm_stats_s,
        "first_page_s": first_page_s,
        "first_page_kb": len(parquet_bytes) // 1024,
        "stat_cache_kb": cache_kb,
        "stat_cache_files": cache_files,
        "peak_mb": round(sampler.peak() / MB),
        # One build load + (cold) one stat run; warm adds one load + one stat run
        # that should hit the on-disk cache rather than recompute.
        "loads": 2,
        "stat_runs": 2,
    }


def measure_diff(project: str, alias: str, a_hash: str, b_hash: str, scratch: Path) -> dict:
    """#45's diff stat path: key resolution → compare expr → stat pipeline."""
    from tallyman_companion.diff import build_compare_expr  # noqa: PLC0415
    from tallyman_core.paths import diff_stat_cache_dir  # noqa: PLC0415
    from tallyman_xorq.primary_key import diff_keys  # noqa: PLC0415
    from tallyman_xorq.result_cache import cached_result_expr  # noqa: PLC0415

    xorq_loading = _import_buckaroo_loading()

    sampler = pr.RssSampler(psutil.Process(), interval=0.05)
    t0 = time.monotonic()
    keys = diff_keys(project, a_hash, b_hash)
    keys_s = round(time.monotonic() - t0, 2)

    a_expr = cached_result_expr(project, a_hash)
    b_expr = cached_result_expr(project, b_hash)
    cmp_expr, _overrides = build_compare_expr(a_expr, b_expr, keys)

    cache_dir = diff_stat_cache_dir(project, a_hash, b_hash)
    t0 = time.monotonic()
    _df, meta = _stats_dataflow(xorq_loading, cmp_expr, cache_dir)
    cold_s = round(time.monotonic() - t0, 2)
    cache_kb, cache_files = pr._dir_kb_and_files(cache_dir)

    t0 = time.monotonic()
    _stats_dataflow(xorq_loading, cmp_expr, cache_dir)
    warm_s = round(time.monotonic() - t0, 2)
    sampler.stop()

    return {
        "pair": f"{alias} {a_hash[:8]}→{b_hash[:8]}",
        "keys": ",".join(keys) or "(none)",
        "rows": meta["rows"],
        "cols": len(meta["columns"]),
        "keys_s": keys_s,
        "cold_stats_s": cold_s,
        "warm_stats_s": warm_s,
        "stat_cache_kb": cache_kb,
        "stat_cache_files": cache_files,
        "peak_mb": round(sampler.peak() / MB),
        "loads": 2,
        "stat_runs": 2,
    }


# ---------------------------------------------------------------------------
# report rendering
# ---------------------------------------------------------------------------


def _render_report(project: str, hashes: list[str], execs, loads, diffs, aliases: dict[str, str]) -> str:
    by_hash = {h: a for a, h in aliases.items()}

    def label(h: str) -> str:
        return f"{by_hash.get(h, '(scratch)')} `{h[:8]}`"

    L = ["# tallyman Tier-B perf integration report", "", f"project `{project}` · {len(hashes)} entries", ""]

    L += [
        "## Execution (`load_entry` → `to_parquet`)",
        "",
        "| entry | rows | cold s | warm s | peak MB |",
        "|---|---|---|---|---|",
    ]
    for r in execs:
        if "error" in r:
            L.append(f"| {label(r['entry'])} | ERROR: {r['error']} ||||")
        else:
            L.append(f"| {label(r['entry'])} | {r['rows']:,} | {r['cold_s']} | {r['warm_s']} | {r['peak_mb']} |")

    L += [
        "",
        "## Page load — Tier B proxy (load build + summary stats + 1st page)",
        "",
        "| entry | rows | cols | load s | cold stats s | warm stats s | 1st page s | stat cache |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in loads:
        if "error" in r:
            L.append(f"| {label(r['entry'])} | ERROR: {r['error']} |||||||")
        else:
            cache = f"{r['stat_cache_kb']}KB ({r['stat_cache_files']})"
            L.append(
                f"| {label(r['entry'])} | {r['rows']:,} | {r['cols']} | {r['load_s']} | {r['cold_stats_s']} "
                f"| {r['warm_stats_s']} | {r['first_page_s']} | {cache} |"
            )

    L += [
        "",
        "## Diff stat path (revision pairs, #45)",
        "",
        "| pair | keys | rows | cols | keys s | cold stats s | warm stats s | peak MB |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in diffs:
        if "error" in r:
            L.append(f"| {r['pair']} | ERROR: {r['error']} |||||||")
        else:
            L.append(
                f"| {r['pair']} | {r['keys']} | {r['rows']:,} | {r['cols']} | {r['keys_s']} "
                f"| {r['cold_stats_s']} | {r['warm_stats_s']} | {r['peak_mb']} |"
            )
    L.append("")
    return "\n".join(L)


def _emit_report(text: str) -> None:
    print("\n" + text)
    out = os.environ.get("TALLYMAN_PERF_OUT")
    if out:
        Path(out).write_text(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as fh:
            fh.write(text + "\n")


# ---------------------------------------------------------------------------
# the test — report-only: it asserts the harness ran, not a threshold
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def corpus(tmp_path_factory, monkeypatch_module):
    project, real_dir, hashes = _resolve_corpus_or_skip()
    overlay_home = _build_overlay(tmp_path_factory.mktemp("perf-home"), project, real_dir)
    monkeypatch_module.setenv("TALLYMAN_HOME", str(overlay_home))
    monkeypatch_module.setenv("TALLYMAN_PROJECT", project)
    return project, real_dir, hashes


@pytest.fixture(scope="module")
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch  # noqa: PLC0415

    mp = MonkeyPatch()
    yield mp
    mp.undo()


def test_perf_integration_report(corpus, tmp_path_factory):
    project, real_dir, hashes = corpus
    scratch = tmp_path_factory.mktemp("perf-scratch")
    aliases = _load_aliases(real_dir)

    execs, loads = [], []
    for h in hashes:
        # pr._safe turns an exception into {"error": ...}; tag it with the
        # identifier the report renders by so a failed row still has a label.
        r_exec = pr._safe(measure_execute, project, h, scratch)
        r_exec.setdefault("entry", h)
        execs.append(r_exec)
        r_load = pr._safe(measure_pageload, project, h, scratch)
        r_load.setdefault("entry", h)
        loads.append(r_load)

    diffs = []
    for alias, a, b in _diff_pairs(real_dir, set(hashes)):
        r_diff = pr._safe(measure_diff, project, alias, a, b, scratch)
        r_diff.setdefault("pair", f"{alias} {a[:8]}→{b[:8]}")
        diffs.append(r_diff)

    _emit_report(_render_report(project, hashes, execs, loads, diffs, aliases))

    # Report-only: assert every measurement produced a row and at least one
    # succeeded end to end. No wall-clock threshold yet (#45's diff time is the
    # obvious first gate once baselined).
    assert len(execs) == len(hashes) and len(loads) == len(hashes)
    assert any("error" not in r for r in execs), "no execute measurement succeeded"
    assert any("error" not in r for r in loads), "no page-load measurement succeeded"
