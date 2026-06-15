"""Tier-A perf calibration: Playwright ground truth + Tier-B alignment (#64).

Tier B (``tests/test_perf_integration.py``) measures the *backend* cost of
opening a catalog entry — ``load_expr`` + the summary-stat pipeline + the first
row page — in-process, with no server and no browser. It is fast and
deterministic, so it is what the suite runs. But a python-only proxy cannot see
what the user also waits for: the browser render, the WS round-trip, first
paint. Without a browser run on the same entry there is no number for that gap,
so the Tier-B page-load figures are uncalibrated.

This module is the ground-truth tier #59 deferred (and #62 explicitly left for a
follow-up). For each measured entry it:

1. **Backend (server-side).** Spins up the *real* Buckaroo subprocess the
   companion spawns (``BuckarooManager`` → ``python -m buckaroo.server``) and
   calls ``ensure_session`` — the exact ``/load_expr`` POST the companion makes
   on first entry-detail hit. Timing this is the ground-truth backend cost
   (xorq load + stat pipeline), the same work Tier B's proxy stands in for.
2. **Render (browser-side).** Drives Playwright to Buckaroo's standalone
   ``/s/<session>`` page and measures to **data-grid-visible** —
   ``.df-viewer .ag-cell`` first-visible — the same "loaded" definition Tier B's
   page-load proxy uses. This is the WS connect + first-page fetch + ag-grid
   render that the proxy cannot see. (Follows Buckaroo's server-mode examples:
   ``pw-tests/server-buckaroo-summary.spec.ts``, ``playwright.config.server.ts``.)
3. **Calibration.** ``total = backend + render``; the headline number is
   ``backend / total``. For the big parking datasets the backend dominates, so
   the proxy should stay faithful (ratio → 1) — but the ratio is written into the
   report so the proxy can't silently drift from reality. As an independent
   cross-check the same entry is run through Tier B's in-process proxy on a
   *separate* cold overlay; ``backend`` (Tier A) and ``load + cold stats``
   (Tier B) measure the same load_expr+stats work and should agree.

Note on ``/load_expr`` vs ``/load``. #64 phrases the drive as ``/load`` with
``mode: 'buckaroo'`` (the buckaroo-js-core example template, which loads a CSV
*path*). The companion never takes that path — a catalog entry is a xorq build,
not a materialised file, and the companion serves it via ``/load_expr`` against
``xorq_build/`` (``buckaroo_lifecycle.ensure_session``). ``/load_expr`` is also
the only path that exercises the load+stat work Tier B proxies, so it is the one
that calibrates Tier B. We therefore drive ``/load_expr`` (via ``ensure_session``,
zero payload duplication) and render the resulting session at ``/s/<session>``.

Cold/warm. Tier A runs on its own fresh **overlay home** (real entries + source
``data`` + catalog bookkeeping symlinked read-only; caches fresh, isolated temp
dirs — never the user's real caches), exactly like Tier B. "Cold" is a genuine
empty-cache first ``/load_expr``; "warm" re-POSTs a fresh session against the
now-populated on-disk stat cache (the path a Buckaroo restart takes). Tier B's
cross-check runs on a *second* fresh overlay so both tiers see a true cold cache
for the same entry rather than one warming the other.

Heavy and flakier than Tier B by nature — it spawns a server and a browser — so
it is the occasional calibration run, not the routine suite. Separate marker
(``perf_tier_a``), local-only, skips when the corpus, Buckaroo, or a browser is
absent. Run it explicitly:

    uv run pytest -m perf_tier_a tests/test_perf_tier_a.py -s

Same overrides as Tier B: ``TALLYMAN_PERF_PROJECT`` / ``TALLYMAN_PERF_ENTRIES``
(comma-separated hashes or aliases) select the corpus and entries;
``TALLYMAN_PERF_OUT`` writes the markdown report somewhere durable. With no
``TALLYMAN_PERF_ENTRIES`` the harness measures only the single largest entry (by
``result.parquet`` size) — calibration is per-entry and the big dataset is the
one where the proxy's faithfulness matters; set ``TALLYMAN_PERF_ENTRIES`` to
widen the run.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

# Reuse Tier B's corpus discovery + safe-overlay machinery rather than copying
# it, so the two perf surfaces share one definition of "the corpus" and one
# write-isolation guarantee.
from tests.test_perf_integration import (
    MB,
    _build_overlay,
    _load_aliases,
    _resolve_corpus_or_skip,
    measure_pageload,
    pr,
)

pytestmark = pytest.mark.perf_tier_a

# The "loaded" selector — identical to Tier B's page-load definition and to the
# buckaroo-js-core server-mode specs' ``waitForDataGrid``.
DATA_GRID_SELECTOR = ".df-viewer .ag-cell"
# Generous because a cold parking-dataset render follows a cold stat compute on
# the same subprocess; the buckaroo specs use 15s for trivial CSVs.
GRID_TIMEOUT_MS = 60_000
# Ground-truth backend wait. ``ensure_session`` caps its ``/load_expr`` POST at
# 10s (#34) and returns None past that, which is exactly the failure that
# motivated this measurement — a cold parking-dataset stat compute blows past
# 10s. To measure the real backend cost we POST the same payload with a long,
# overridable timeout rather than reusing the capped path.
LOAD_EXPR_TIMEOUT_S = float(os.environ.get("TALLYMAN_PERF_LOAD_TIMEOUT", "180"))


# ---------------------------------------------------------------------------
# skip guards (keep the tier inert without a corpus, Buckaroo, or a browser)
# ---------------------------------------------------------------------------


def _require_browser_or_skip():
    """Import Playwright and resolve a Chromium, else skip.

    Both the package (a dev dependency) and the browser binary (installed out
    of band via ``playwright install chromium``) must be present; either being
    absent is a clean skip, not a failure — this never runs on CI.
    """
    try:
        from playwright.sync_api import sync_playwright  # noqa: PLC0415
    except ImportError:
        pytest.skip("playwright not installed (dev dependency)")
    p = sync_playwright().start()
    try:
        exe = Path(p.chromium.executable_path)
    except Exception as exc:  # pragma: no cover - environment-dependent
        p.stop()
        pytest.skip(f"playwright chromium unavailable ({exc}); run `playwright install chromium`")
    if not exe.exists():  # pragma: no cover - environment-dependent
        p.stop()
        pytest.skip("playwright chromium binary missing; run `playwright install chromium`")
    return p  # caller owns stop()


def _largest_entry(real_dir: Path, hashes: list[str]) -> str:
    """The built entry with the biggest ``result.parquet`` (symlink target size).

    The default single-entry calibration target: the big dataset is where the
    backend dominates and the proxy's faithfulness actually matters.
    """
    entries = real_dir / "artifacts" / "catalog" / "entries"

    def size(h: str) -> int:
        p = entries / h / "result.parquet"
        try:
            return p.stat().st_size  # follows the symlink to the real bytes
        except OSError:
            return 0

    return max(hashes, key=size)


# ---------------------------------------------------------------------------
# measurement primitives
# ---------------------------------------------------------------------------


def _time_grid_render(browser, base_url: str, session_id: str) -> dict:
    """Navigate to ``/s/<session>`` and time to data-grid-visible.

    Returns the wall time to ``.df-viewer .ag-cell`` first-visible plus the
    browser-only timings (first-contentful-paint, DOMContentLoaded) the proxy
    is blind to — these document *what* the render second is spent on.
    """
    page = browser.new_page()
    try:
        t0 = time.monotonic()
        page.goto(f"{base_url}/s/{session_id}", wait_until="domcontentloaded")
        page.locator(DATA_GRID_SELECTOR).first.wait_for(state="visible", timeout=GRID_TIMEOUT_MS)
        grid_s = round(time.monotonic() - t0, 2)

        # navigationStart-relative marks, in ms — the slice of the render the
        # Tier-B proxy structurally cannot observe.
        fcp_ms = page.evaluate(
            "() => { const e = performance.getEntriesByName('first-contentful-paint')[0];"
            " return e ? Math.round(e.startTime) : null; }"
        )
        dcl_ms = page.evaluate(
            "() => { const n = performance.getEntriesByType('navigation')[0];"
            " return n ? Math.round(n.domContentLoadedEventEnd) : null; }"
        )
        cells = page.locator(DATA_GRID_SELECTOR).count()
        return {"grid_s": grid_s, "fcp_ms": fcp_ms, "dcl_ms": dcl_ms, "cells": cells}
    finally:
        page.close()


def _load_expr(mgr, project: str, content_hash: str) -> str:
    """POST ``/load_expr`` with the companion's exact payload, long timeout.

    Mirrors ``buckaroo_lifecycle.ensure_session``'s payload (expanded build dir,
    ``project_root`` = artifacts dir, on-disk ``cache_storage_path``) so the
    backend work is identical to production — but issues the POST directly with
    ``LOAD_EXPR_TIMEOUT_S`` instead of ensure_session's 10s cap, which a cold
    parking-dataset stat compute exceeds (#34). Keep the payload in sync with
    ensure_session. Buckaroo mints a fresh session id per POST, so calling this
    twice gives two live sessions over the same on-disk cache (cold then warm).
    """
    import httpx  # noqa: PLC0415

    from tallyman_core.paths import artifacts_dir, entry_dir, project_dir  # noqa: PLC0415
    from tallyman_xorq.portable import ensure_expanded_build  # noqa: PLC0415

    build_dir = entry_dir(project, content_hash) / "xorq_build"
    expanded = ensure_expanded_build(
        build_dir, project_dir(project), entry_dir(project, content_hash) / ".xorq_build_expanded"
    )
    stat_cache = entry_dir(project, content_hash) / ".buckaroo_stat_cache"
    stat_cache.mkdir(parents=True, exist_ok=True)
    resp = httpx.post(
        f"{mgr.base_url}/load_expr",
        json={
            "build_dir": str(expanded),
            "no_browser": True,
            "project_root": str(artifacts_dir(project)),
            "cache_storage_path": str(stat_cache),
        },
        timeout=LOAD_EXPR_TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()["session"]


def measure_tier_a(mgr, browser, project: str, content_hash: str) -> dict:
    """Backend (``/load_expr``) + render (Playwright) for one entry.

    Cold then warm: the first ``/load_expr`` hits an empty overlay cache; the
    warm pass re-POSTs a fresh session against the now-populated on-disk stat
    cache (the path a Buckaroo restart takes), so a warm ≈ cold backend means
    the cache is unused. Each session is rendered in its own page.
    """
    sampler = pr.RssSampler(__import__("psutil").Process(), interval=0.05)

    # Cold: genuine empty-cache first /load_expr.
    t0 = time.monotonic()
    session_cold = _load_expr(mgr, project, content_hash)
    backend_cold_s = round(time.monotonic() - t0, 2)
    render_cold = _time_grid_render(browser, mgr.base_url, session_cold)

    # Warm: re-POST against the now-populated on-disk stat cache.
    t0 = time.monotonic()
    session_warm = _load_expr(mgr, project, content_hash)
    backend_warm_s = round(time.monotonic() - t0, 2)
    render_warm = _time_grid_render(browser, mgr.base_url, session_warm)
    sampler.stop()

    total_cold_s = round(backend_cold_s + render_cold["grid_s"], 2)
    return {
        "entry": content_hash,
        "backend_cold_s": backend_cold_s,
        "render_cold_s": render_cold["grid_s"],
        "total_cold_s": total_cold_s,
        # The deliverable: fraction of the cold user-wait the proxy can see.
        "backend_ratio": round(backend_cold_s / total_cold_s, 3) if total_cold_s else None,
        "backend_warm_s": backend_warm_s,
        "render_warm_s": render_warm["grid_s"],
        "fcp_ms": render_cold["fcp_ms"],
        "dcl_ms": render_cold["dcl_ms"],
        "cells": render_cold["cells"],
        "peak_mb": round(sampler.peak() / MB),
    }


# ---------------------------------------------------------------------------
# report rendering
# ---------------------------------------------------------------------------


def _render_report(project: str, rows: list[dict], aliases: dict[str, str]) -> str:
    by_hash = {h: a for a, h in aliases.items()}

    def label(h: str) -> str:
        return f"{by_hash.get(h, '(scratch)')} `{h[:8]}`"

    L = [
        "# tallyman Tier-A perf calibration report",
        "",
        f"project `{project}` · {len(rows)} entr{'y' if len(rows) == 1 else 'ies'}",
        "",
        "Ground truth: real Buckaroo subprocess + Chromium, to "
        "`.df-viewer .ag-cell` first-visible. `backend` is the `/load_expr` "
        "(xorq load + stat pipeline) cost; `render` is the WS + first-page + "
        "ag-grid paint the Tier-B proxy cannot see. `ratio = backend / total` "
        "is the fraction of the cold user-wait the proxy observes.",
        "",
        "## Calibration (Tier A ground truth vs Tier B proxy)",
        "",
        "| entry | backend cold s | render cold s | total cold s | ratio | "
        "Tier B backend cold s | backend warm s | render warm s | peak MB |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        a = r["tier_a"]
        if "error" in a:
            L.append(f"| {label(r['entry'])} | ERROR: {a['error']} ||||||||")
            continue
        b = r.get("tier_b", {})
        tier_b_backend = "—" if "error" in b else round(b.get("load_s", 0) + b.get("cold_stats_s", 0), 2)
        ratio = "—" if a["backend_ratio"] is None else f"{a['backend_ratio']:.0%}"
        L.append(
            f"| {label(r['entry'])} | {a['backend_cold_s']} | {a['render_cold_s']} "
            f"| {a['total_cold_s']} | {ratio} | {tier_b_backend} "
            f"| {a['backend_warm_s']} | {a['render_warm_s']} | {a['peak_mb']} |"
        )

    L += [
        "",
        "## What the proxy cannot see (browser-only render breakdown)",
        "",
        "| entry | first-contentful-paint ms | DOMContentLoaded ms | data cells |",
        "|---|---|---|---|",
    ]
    for r in rows:
        a = r["tier_a"]
        if "error" in a:
            continue
        L.append(f"| {label(r['entry'])} | {a['fcp_ms']} | {a['dcl_ms']} | {a['cells']} |")
    L.append("")
    return "\n".join(L)


def _emit_report(text: str) -> None:
    print("\n" + text)
    out = os.environ.get("TALLYMAN_PERF_OUT")
    if out:
        Path(out).write_text(text)


# ---------------------------------------------------------------------------
# the test — report-only: it asserts the calibration ran end to end
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch  # noqa: PLC0415

    mp = MonkeyPatch()
    yield mp
    mp.undo()


def test_perf_tier_a_calibration(tmp_path_factory, monkeypatch_module):
    from tallyman_companion.buckaroo_lifecycle import BuckarooManager  # noqa: PLC0415

    project, real_dir, hashes = _resolve_corpus_or_skip()
    if not os.environ.get("TALLYMAN_PERF_ENTRIES"):
        # Default to one entry: the calibration is per-entry and the largest
        # dataset is where the backend dominates. Name the rest so the cap is
        # never silent.
        chosen = _largest_entry(real_dir, hashes)
        dropped = [h for h in hashes if h != chosen]
        if dropped:
            print(
                f"\nTier A: measuring largest entry {chosen[:8]} only; "
                f"set TALLYMAN_PERF_ENTRIES to include {len(dropped)} others "
                f"({', '.join(h[:8] for h in dropped)})"
            )
        hashes = [chosen]
    aliases = _load_aliases(real_dir)

    playwright = _require_browser_or_skip()

    # Two cold overlays: Tier A drives the server against overlay_a; Tier B's
    # in-process proxy cross-check runs on overlay_b so neither warms the
    # other's cache for the same entry.
    overlay_a = _build_overlay(tmp_path_factory.mktemp("tier-a-home"), project, real_dir)
    overlay_b = _build_overlay(tmp_path_factory.mktemp("tier-b-home"), project, real_dir)

    browser = playwright.chromium.launch(headless=True)
    rows: list[dict] = []
    mgr = BuckarooManager(port=0, startup_timeout=20.0)
    try:
        monkeypatch_module.setenv("TALLYMAN_HOME", str(overlay_a))
        monkeypatch_module.setenv("TALLYMAN_PROJECT", project)
        mgr.start()
        assert mgr.is_running, "buckaroo subprocess failed to start"
        for h in hashes:
            r_a = pr._safe(measure_tier_a, mgr, browser, project, h)
            r_a.setdefault("entry", h)
            rows.append({"entry": h, "tier_a": r_a})
    finally:
        mgr.stop()
        browser.close()
        playwright.stop()

    # Tier B proxy cross-check on the second cold overlay (no server/browser).
    monkeypatch_module.setenv("TALLYMAN_HOME", str(overlay_b))
    scratch = tmp_path_factory.mktemp("tier-a-scratch")
    for row in rows:
        row["tier_b"] = pr._safe(measure_pageload, project, row["entry"], scratch)

    _emit_report(_render_report(project, rows, aliases))

    # Report-only: assert the calibration produced a row per entry and that at
    # least one entry rendered to data-grid-visible end to end. No wall-clock
    # threshold — this tier exists to keep the Tier-B proxy honest, not to gate.
    assert len(rows) == len(hashes)
    assert any("error" not in r["tier_a"] for r in rows), "no Tier-A measurement reached data-grid-visible"
    for r in rows:
        if "error" not in r["tier_a"]:
            assert r["tier_a"]["cells"] >= 1, f"grid visible but no data cells for {r['entry']}"
            assert r["tier_a"]["backend_ratio"] is not None
