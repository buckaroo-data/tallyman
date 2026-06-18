"""Tier-B perf measurement of ``from_catalog`` chain-depth build/reconstruct cost (#82).

Issue #82 (Stage 2 of the catalog-perf-reactive work, and the ADR's "Deep-chain
build cost" open question) asks for a *number* on how much #74's composition path
costs as a function of ``from_catalog`` chain depth.

Pre-#74, ``from_catalog`` read the parent's materialised ``result.parquet`` as a
bare ``deferred_read_parquet`` — every entry truncated the graph at a parquet
boundary, so build/reconstruct cost was depth-independent by construction. #74
(and #104, which made ``cached_result_expr`` the *sole* read path) dropped that:

* **Expensive parent** (``cache_worthy`` — Aggregate / Join / Sort / window / UDF):
  ``cached_result_expr`` returns a bare ``deferred_read_parquet`` of the baked
  ``ParquetSnapshotCache`` snapshot. Still truncates — for the *returned*
  expression.
* **Cheap parent** (projection / rename / row-wise scalar math): re-imports the
  parent's ``expr.py`` recipe and composes its full expression into the child.
  No truncation. The recipe re-import runs on every cold read (since #73).

So the per-entry parquet boundary that used to hide deep-graph build cost is gone
for cheap chains. This module measures what that costs, against the three
hypotheses #82 names:

* **H1** — all-cheap reconstruct/build cost grows *linearly* with depth
  (acceptable) vs *super-linearly* (the per-entry truncation was load-bearing and
  cheap chains need their own boundary).
* **H2** — an expensive entry truncates the chain: a child reading it pays a bare
  snapshot read, independent of the depth *below* it.
* **H3** — the per-cold-read recipe-reconstruction cost (new since #73) is small
  relative to execute, or it is a material cold-read regression.

What "build/tokenize ... no ``.execute()``" means here. ``cached_result_expr``
*returns the reconstructed expression without executing it* — for a cheap chain
that is the full graph-construction cost (recipe re-import + compose) the issue
asks for. We time that, then separately time ``to_sql`` (graph → plan compile)
and ``.execute()`` (the H3 denominator). Cold = ``cache_clear()`` + an evicted
compute cache (recipe re-imported); warm = the ``_resolve_result_plan`` LRU is hot
(``result_cache.py``). The reconstruct *count* — the explanatory variable per
#59's "counts over wall-clock" convention — is read straight off the
``tallyman.perf`` cold-read log lines (#87/#60 instrumentation): one per entry
reconstructed in the chain.

Why a *synthetic* chain, not the parking corpus. The parking corpus is deep in
*revisions*, not in ``from_catalog`` chaining (its deepest chain is ~4), so it
can't sweep depth. Per #82 we build the chain explicitly via
``catalog_create`` + ``from_catalog`` at parameterised depth, in an isolated temp
``TALLYMAN_HOME``. The reconstruct/build cost (H1) is row-count-independent
(it's graph construction), so a modest seed keeps the build fast; ``TALLYMAN_PERF_CHAIN_ROWS``
raises it for a realistic execute denominator (H3).

Local-only, marker-gated (``perf``) so it never runs on CI. Run it explicitly:

    uv run pytest -m perf tests/test_perf_chain_depth.py -s

Tunables (env): ``TALLYMAN_PERF_CHAIN_DEPTHS`` (default ``1,2,4,8,16``),
``TALLYMAN_PERF_CHAIN_ROWS`` (default ``20000``), ``TALLYMAN_PERF_OUT`` (write the
markdown report to a durable path).
"""

from __future__ import annotations

import importlib.util
import logging
import os
import shutil
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

pytestmark = pytest.mark.perf

REPO_ROOT = Path(__file__).resolve().parents[1]

PROJECT = "chain_perf"
DEPTHS = sorted({int(x) for x in os.environ.get("TALLYMAN_PERF_CHAIN_DEPTHS", "1,2,4,8,16").split(",") if x.strip()})
MAX_DEPTH = max(DEPTHS)
ROWS = int(os.environ.get("TALLYMAN_PERF_CHAIN_ROWS", "20000"))


# ---------------------------------------------------------------------------
# reuse the report-only suite's measurement helpers (same pattern as
# test_perf_integration.py — load scripts/perf_report.py as a module so the
# perf surfaces share RssSampler / MB / _safe rather than copying them).
# ---------------------------------------------------------------------------


def _load_perf_report():
    path = REPO_ROOT / "scripts" / "perf_report.py"
    spec = importlib.util.spec_from_file_location("perf_report", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


pr = _load_perf_report()


# ---------------------------------------------------------------------------
# reconstruct-count capture (#87/#60 tallyman.perf cold-read instrumentation)
# ---------------------------------------------------------------------------


@contextmanager
def _count_cold_reads():
    """Count ``cached_result_expr`` cold-read events emitted on ``tallyman.perf``.

    Each cold reconstruct of an entry logs one ``... cold read ...`` line
    (``result_cache.py``). During a single cold read of a chain leaf, every entry
    reconstructed in the chain emits one — so the count is the number of links the
    read actually walked, i.e. #59's explanatory variable for the wall time.

    Yields a dict whose ``count`` key is filled in once the block exits.
    """
    logger = logging.getLogger("tallyman.perf")
    msgs: list[str] = []

    class _Collect(logging.Handler):
        def emit(self, record):
            msgs.append(record.getMessage())

    handler = _Collect()
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    box = {"count": 0, "heals": 0}
    try:
        yield box
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
        box["count"] = sum(1 for m in msgs if "cold read" in m)
        box["heals"] = sum(1 for m in msgs if "self-heal" in m)


# ---------------------------------------------------------------------------
# synthetic-chain recipes — cheap composes the full graph, expensive truncates
# ---------------------------------------------------------------------------


def _root_code() -> str:  # cheap: parquet read + projection (no expensive op)
    return f"""
from tallyman_xorq.io import from_project
t = from_project("seed.parquet", project={PROJECT!r})
expr = t.select("region", "price", "qty")
"""


def _cheap_child_code(parent: str, i: int) -> str:  # cheap: row-wise mutate off a from_catalog parent
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.mutate(d{i}=t.price + {i})
"""


def _expensive_code(parent: str) -> str:  # Aggregate → cache_worthy → bakes a snapshot (the truncating boundary)
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _cheap_over_expensive_code(parent: str) -> str:  # cheap leaf above the expensive boundary
    return f"""
from tallyman_xorq.io import from_catalog
t = from_catalog({parent!r})
expr = t.mutate(total2=t.total * 2)
"""


def _create(name: str, code: str) -> str:
    """``catalog_create`` the entry, asserting success, and return its content hash."""
    from tallyman_mcp.server import catalog_create

    res = catalog_create(name, code)
    assert "error" not in res, f"build of {name!r} failed: {res}"
    return res["hash"]


def _build_corpus() -> dict:
    """Build both chain shapes in one project; return the hashes to measure.

    Shape A — an all-cheap chain ``a0 → a1 → … → aMAX`` (each ``ak`` is
    ``from_catalog(a{k-1}).mutate(...)``). ``shape_a[k]`` is the entry at depth k.

    Shape B — for each measured depth k, an expensive ``Aggregate`` ``eb_k`` over
    shape A's ``ak`` (so the cheap chain ``a0..ak`` sits *below* the boundary),
    then a cheap leaf ``lb_k = from_catalog(eb_k).mutate(...)`` *above* it.
    ``shape_b[k]`` is ``lb_k``. The H2 truncation test is whether ``shape_b[k]``'s
    *execute* stays flat as the cheap chain below the boundary deepens (k low→high) —
    it reads only the baked aggregate snapshot — while ``shape_a[k]`` (no boundary)
    grows with depth; shape A at matching k is the reference.
    """
    seed_dir = Path(os.environ["TALLYMAN_HOME"]) / "projects" / PROJECT / "data"
    seed_dir.mkdir(parents=True, exist_ok=True)
    from tallyman_cli.fixtures import write_shoe_orders

    write_shoe_orders(seed_dir / "seed.parquet", n_rows=ROWS, seed=0)

    shape_a: dict[int, str] = {0: _create("a0", _root_code())}
    for i in range(1, MAX_DEPTH + 1):
        shape_a[i] = _create(f"a{i}", _cheap_child_code(f"a{i - 1}", i))

    shape_b: dict[int, str] = {}
    for k in DEPTHS:
        _create(f"eb_{k}", _expensive_code(f"a{k}"))  # expensive boundary over the depth-k cheap chain
        shape_b[k] = _create(f"lb_{k}", _cheap_over_expensive_code(f"eb_{k}"))
    return {"shape_a": shape_a, "shape_b": shape_b}


# ---------------------------------------------------------------------------
# measurement (Tier B — in-process, no server, no browser)
# ---------------------------------------------------------------------------


def _evict(project: str) -> None:
    """Force a genuinely cold read: drop the reconstruct LRU and the baked snapshots."""
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.result_cache import cached_result_expr

    cached_result_expr.cache_clear()
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)


def measure_chain(project: str, depth: int, content_hash: str, shape: str) -> dict:
    """Cold/warm reconstruct + compile + execute of one chain leaf, no threshold.

    * ``cold_s`` / ``cold_reads`` — reconstruct the leaf on an evicted LRU + cold
      compute cache, *without executing*; the count is how many entries the read
      walked (#59 explanatory variable). For a shape-B leaf the expensive parent's
      snapshot is evicted too, so the cold read also self-heals it once
      (``heals``); that heal does execute the aggregate — an honest cold-read cost
      of chaining off an expensive parent.
    * ``compile_s`` — ``to_sql`` of the reconstructed expression (graph → plan),
      the pure graph-construction the issue calls "build/tokenize".
    * ``warm_ms`` — re-reading with the LRU hot (memoised plan); should collapse to
      the snapshot existence check.
    * ``execute_s`` — materialise the result (H3 denominator).
    """
    import psutil

    from tallyman_xorq.result_cache import cached_result_expr

    try:
        from xorq.expr.api import to_sql
    except ImportError:
        to_sql = None

    sampler = pr.RssSampler(psutil.Process(), interval=0.05)

    _evict(project)
    with _count_cold_reads() as box:
        t0 = time.monotonic()
        cold_expr = cached_result_expr(project, content_hash)
        cold_s = round(time.monotonic() - t0, 3)
    cold_reads, heals = box["count"], box["heals"]
    top_op = type(cold_expr.op()).__name__

    compile_s = None
    if to_sql is not None:
        c = pr._safe(lambda: round(_timed(to_sql, cold_expr), 3))
        compile_s = c if not isinstance(c, dict) else None

    # Warm: LRU hot, snapshot now present — the memoised re-read path.
    t0 = time.monotonic()
    cached_result_expr(project, content_hash)
    warm_ms = round((time.monotonic() - t0) * 1000, 1)

    t0 = time.monotonic()
    n = len(cached_result_expr(project, content_hash).execute())
    execute_s = round(time.monotonic() - t0, 3)
    sampler.stop()

    return {
        "shape": shape,
        "depth": depth,
        "entry": content_hash,
        "rows": n,
        "top_op": top_op,
        "cold_reads": cold_reads,
        "heals": heals,
        "cold_s": cold_s,
        "compile_s": compile_s,
        "warm_ms": warm_ms,
        "execute_s": execute_s,
        "peak_mb": round(sampler.peak() / pr.MB),
    }


def _timed(fn, *args):
    t0 = time.monotonic()
    fn(*args)
    return time.monotonic() - t0


# ---------------------------------------------------------------------------
# hypotheses — computed from the collected numbers (report-only, not asserted)
# ---------------------------------------------------------------------------


def _fmt(v) -> str:
    return "—" if v is None else (f"{v:.3f}" if isinstance(v, float) else str(v))


def _hypotheses(rows_a: list[dict], rows_b: list[dict]) -> list[str]:
    a = {r["depth"]: r for r in rows_a if "error" not in r}
    b = {r["depth"]: r for r in rows_b if "error" not in r}
    depths = sorted(a)
    L = ["## Hypotheses (computed from the table above)", ""]
    if len(depths) < 2:
        return L + ["_need ≥2 depths to evaluate growth — widen TALLYMAN_PERF_CHAIN_DEPTHS._", ""]

    lo, hi = depths[0], depths[-1]

    # H1 — all-cheap reconstruct cost vs depth. The count is the cleanest signal:
    # a linear chain walks ~depth+1 entries, so cold_reads ≈ depth + c is linear.
    reads_lo, reads_hi = a[lo]["cold_reads"], a[hi]["cold_reads"]
    per_link = (reads_hi - reads_lo) / (hi - lo) if hi != lo else float("nan")
    s_lo, s_hi = a[lo]["cold_s"], a[hi]["cold_s"]
    # Ratio of entries the chain walks (depth k walks ~k+1), so it's well-defined
    # even when 0 ∈ DEPTHS — a literal hi/lo would be NaN/inf and flip H1 to a
    # bogus "super-linear" verdict.
    depth_ratio = (hi + 1) / (lo + 1)
    time_ratio = (s_hi / s_lo) if s_lo else float("inf")
    h1_linear = abs(per_link - 1.0) < 0.25 and time_ratio <= depth_ratio * 1.6
    L += [
        f"**H1 — all-cheap build cost vs depth.** cold_reads grows {reads_lo}→{reads_hi} over "
        f"depth {lo}→{hi} (~{per_link:.2f} reconstruct/link); cold reconstruct wall {s_lo:.3f}s→{s_hi:.3f}s "
        f"({time_ratio:.1f}× for {depth_ratio:.1f}× depth). "
        + (
            "Roughly **linear** — #74's truncate-only-at-expensive bet holds for cheap chains."
            if h1_linear
            else "**Super-linear** — the per-entry parquet boundary was load-bearing; cheap chains likely "
            "need their own depth boundary (promote a cheap entry to a snapshot past a depth threshold)."
        ),
        "",
    ]

    # H2 — does an expensive boundary truncate the cost above it? The discriminating
    # signal is EXECUTE, not warm_ms: a warm re-read is an O(1) hot-LRU lookup that is
    # ~0 for both shapes regardless of depth (it proves the memo works, not truncation).
    # A truncating boundary means shape B's leaf executes only the small baked snapshot,
    # so its execute is flat as the cheap chain *below* the boundary deepens — while
    # shape A's recompute, with no boundary, grows with depth. cold_reads is the
    # counter-signal: if it still grows, the cold snapshot-key derivation re-walks the
    # sub-DAG and does NOT truncate.
    kk = sorted(set(b) & set(a))
    if kk:
        klo, khi = kk[0], kk[-1]
        ex_b_lo, ex_b_hi = b[klo]["execute_s"], b[khi]["execute_s"]
        ex_a_lo, ex_a_hi = a[klo]["execute_s"], a[khi]["execute_s"]
        reads_b = {k: b[k]["cold_reads"] for k in kk}
        # B's execute stays flat across depth-below; A's grows. The 0.01s floor keeps
        # sub-ms jitter from reading as growth at tiny seeds.
        b_flat = ex_b_hi <= max(2.0 * ex_b_lo, ex_b_lo + 0.01)
        a_grows = ex_a_hi > ex_a_lo
        cold_grows = (reads_b[khi] - reads_b[klo]) > 2
        L += [
            f"**H2 — expensive boundary truncation.** A cheap leaf above an `Aggregate` boundary, with a "
            f"depth-k cheap chain *below* it (shape A at depth k is the no-boundary reference). "
            f"Execute stays {ex_b_lo:.3f}s→{ex_b_hi:.3f}s across depth-below {klo}→{khi} while shape A grows "
            f"{ex_a_lo:.3f}s→{ex_a_hi:.3f}s: "
            + (
                "the executed/returned expression **truncates** at the boundary as #74 claims — the leaf reads "
                "only its parent's baked snapshot, independent of the depth below it."
                if (b_flat and a_grows)
                else "execute does **not** show the expected flat-B / growing-A split — investigate."
            ),
            f"  But cold reconstruct count below the boundary {reads_b} "
            + (
                "still grows with depth-below: locating the parent snapshot's structural key re-imports the "
                "expensive recipe and re-walks its full sub-DAG, so truncation does **not** extend to the cold "
                "key derivation (the evicted parent snapshot also self-heals once — the `heals` column)."
                if cold_grows
                else "is flat — cold key derivation truncates too."
            ),
            "",
        ]

    # H3 — cold reconstruct vs execute. On a small seed execute is cheap, so the
    # ratio is an upper bound on how much reconstruct dominates; raise
    # TALLYMAN_PERF_CHAIN_ROWS for a realistic execute denominator.
    ratios = {d: (a[d]["cold_s"] / a[d]["execute_s"]) if a[d]["execute_s"] else float("inf") for d in depths}
    worst = max(ratios.values())
    L += [
        f"**H3 — reconstruct vs execute** (seed {ROWS:,} rows). cold reconstruct / execute by depth: "
        + ", ".join(f"d{d}={ratios[d]:.1f}×" for d in depths)
        + ". "
        + (
            "Reconstruct **dominates** cold reads on cheap chains at this row count — the recipe re-import is "
            "the cold-read cost, not execution. Re-run with a larger TALLYMAN_PERF_CHAIN_ROWS to place it "
            "against a realistic execute."
            if worst >= 1.0
            else "Reconstruct stays small relative to execute."
        ),
        "",
    ]
    return L


# ---------------------------------------------------------------------------
# report rendering
# ---------------------------------------------------------------------------


def _render(rows_a: list[dict], rows_b: list[dict]) -> str:
    L = [
        "# tallyman from_catalog chain-depth perf report (#82)",
        "",
        f"project `{PROJECT}` · seed {ROWS:,} rows · depths {DEPTHS}",
        "",
        "Cold = evicted reconstruct LRU + cold compute cache (recipe re-imported); "
        "warm = memoised plan (an O(1) hot-LRU lookup — flat for both shapes, a memo sanity check, not a "
        "truncation signal). `cold_reads` = entries the cold read walked (#59 explanatory variable). "
        "`cold_s` is reconstruct *without* execute for Shape A; for a Shape B leaf it additionally includes "
        "the one-time self-heal execute of the evicted expensive-parent snapshot (the `heals` column), so "
        "B's `cold_s` is not directly comparable to A's.",
        "",
    ]

    def table(title: str, rows: list[dict]) -> list[str]:
        out = [
            f"## {title}",
            "",
            "| depth | top op | cold_reads | heals | cold s | compile s | warm ms | execute s | rows | peak MB |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for r in rows:
            if "error" in r:
                out.append(f"| {r.get('depth', '?')} | ERROR: {r['error']} ||||||||")
                continue
            out.append(
                f"| {r['depth']} | {r['top_op']} | {r['cold_reads']} | {r['heals']} | {r['cold_s']} "
                f"| {_fmt(r['compile_s'])} | {r['warm_ms']} | {r['execute_s']} | {r['rows']:,} | {r['peak_mb']} |"
            )
        out.append("")
        return out

    L += table("Shape A — all-cheap chain (no truncation)", rows_a)
    L += table("Shape B — cheap leaf above an expensive `Aggregate` boundary (depth = chain below boundary)", rows_b)
    L += _hypotheses(rows_a, rows_b)
    return "\n".join(L)


def _emit(text: str) -> None:
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


@pytest.fixture
def chain_home(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_HOME", str(tmp_path))
    monkeypatch.setenv("TALLYMAN_PROJECT", PROJECT)
    from tallyman_core import ensure_project, set_active_project

    ensure_project(PROJECT)
    set_active_project(PROJECT)
    return tmp_path


def test_chain_depth_report(chain_home):
    corpus = _build_corpus()

    rows_a = [
        pr._safe(measure_chain, PROJECT, d, corpus["shape_a"][d], "A") | {"depth": d, "shape": "A"} for d in DEPTHS
    ]
    rows_b = [
        pr._safe(measure_chain, PROJECT, d, corpus["shape_b"][d], "B") | {"depth": d, "shape": "B"} for d in DEPTHS
    ]

    _emit(_render(rows_a, rows_b))

    # Report-only: every depth produced a row and at least one cold reconstruct
    # succeeded end to end. No wall-clock threshold.
    assert len(rows_a) == len(DEPTHS) and len(rows_b) == len(DEPTHS)
    assert any("error" not in r for r in rows_a), "no shape-A measurement succeeded"
    assert any("error" not in r for r in rows_b), "no shape-B measurement succeeded"
