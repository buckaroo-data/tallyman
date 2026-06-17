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
import re
import time
from pathlib import Path

import pytest

# Reuse Tier B's corpus discovery + safe-overlay machinery rather than copying
# it, so the two perf surfaces share one definition of "the corpus" and one
# write-isolation guarantee.
from tests.test_perf_integration import (
    MB,
    _build_overlay,
    _entry_rows,
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
    """The built entry with the most rows (from manifest.json, #73-safe).

    The default single-entry calibration target: the big dataset is where the
    backend dominates and the proxy's faithfulness actually matters. Ranks by
    manifest ``row_count`` rather than ``result.parquet`` bytes, which a #73
    build never writes.
    """
    return max(hashes, key=lambda h: _entry_rows(real_dir, h))


# ---------------------------------------------------------------------------
# buckaroo #926 server spans (BUCKAROO_PERF) — parsed from the server log
# ---------------------------------------------------------------------------

# With BUCKAROO_PERF=1 (set in the subprocess env before it spawns), buckaroo's
# /load_expr handler emits one `perf span=firstpull.<phase> secs=<n> session=<id>`
# line per phase to its server log. The spans carry session= so they correlate
# across the cold and warm POSTs (and any concurrent loads). We read the slice of
# the log appended during the run and pick the cold session's backend phases —
# turning the single `backend cold s` number into a server-side breakdown.
_FIRSTPULL_RE = re.compile(
    r"perf span=(?P<label>firstpull\.\S+) secs=(?P<secs>[\d.]+).*?session=(?P<session>\S+)"
)


def _server_log_path() -> Path:
    """Where buckaroo's server writes spans (`buckaroo/server/__main__.py`)."""
    return Path.home() / ".buckaroo" / "logs" / "server.log"


def _read_backend_spans(text: str, session_id: str) -> dict[str, float]:
    """`/load_expr` firstpull spans for *session_id* from a server-log slice.

    Excludes `firstpull.ws_first_payload` — that span fires during the browser
    render (the WS first send), which Tier A already times separately as
    `render`, not part of the `/load_expr` backend wait.
    """
    spans: dict[str, float] = {}
    for line in text.splitlines():
        m = _FIRSTPULL_RE.search(line)
        if not m or m["session"] != session_id or m["label"] == "firstpull.ws_first_payload":
            continue
        spans[m["label"]] = spans.get(m["label"], 0.0) + float(m["secs"])
    return spans


def _read_first_backend_spans(text: str) -> dict[str, float]:
    """Backend spans for the *first* session id seen in *text* — the cold POST.

    When ``ensure_session`` returns None (its 10s cap tripped) we never learn the
    session id, but the server runs the ``/load_expr`` to completion and logs the
    firstpull spans under one id. With only the cold load issued so far, the
    first session in the slice is that capped cold POST, so its
    ``firstpull.load_expr`` is the true (uncapped) backend cost.
    """
    for line in text.splitlines():
        m = _FIRSTPULL_RE.search(line)
        if m and m["label"] != "firstpull.ws_first_payload":
            return _read_backend_spans(text, m["session"])
    return {}


def _evict_session(mgr, content_hash: str) -> None:
    """Drop the in-memory cached session so the next ensure_session re-POSTs.

    ``ensure_session`` caches the session id for the Buckaroo lifetime, so a
    second call returns it instantly (the same-lifetime re-view). To measure the
    server-restart warm path — a fresh ``/load_expr`` against the now-populated
    on-disk stat cache — evict first, mirroring what a restart does to the
    in-memory map (``start()`` resets ``_sessions``).
    """
    with mgr._session_lock:
        mgr._sessions.pop(content_hash, None)


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

    The **cap-fallback** path: ``measure_tier_a`` drives the production
    ``ensure_session`` (10s cap) for the headline numbers; this is only used when
    that cap trips (``ensure_session`` returned None) to obtain a renderable —
    now-warm — session so the grid still paints and the calibration completes.
    Mirrors ``buckaroo_lifecycle.ensure_session``'s payload (expanded build dir,
    ``project_root`` = artifacts dir, on-disk ``cache_storage_path``) but issues
    the POST directly with ``LOAD_EXPR_TIMEOUT_S`` instead of the 10s cap. Keep
    the payload in sync with ensure_session.
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
    """Production ``ensure_session`` backend + render (Playwright) for one entry.

    Drives the **exact** companion path a first entry-detail hit takes —
    ``BuckarooManager.ensure_session`` → ``/load_expr`` *with the production 10s
    cap* (#34) — not a hand-rolled long-timeout POST. The headline is whether a
    cold open returns a usable session within that cap: a None is the live
    failure that drops the user to the pandas preview. The server runs the load
    to completion regardless, so the true (uncapped) backend cost is recovered
    from the buckaroo#926 ``firstpull.load_expr`` span even when the cap trips.

    Cold then two warms. Cold is a genuine empty-overlay-cache ``ensure_session``.
    ``warm_cached`` re-calls without eviction (the same-lifetime re-view — an
    in-memory map hit, ~0ms, the path a second detail view takes). ``warm_reload``
    evicts then re-calls: a fresh ``/load_expr`` against the now-populated on-disk
    stat cache, the path a Buckaroo restart takes — so warm_reload ≈ cold means
    the on-disk cache is unused.

    Resource sampling targets the **Buckaroo subprocess tree** (``mgr.proc.pid``
    and its children), not this driver process — the stat compute runs server-
    side, so a driver-process RSS reads a few hundred MB while Buckaroo holds
    the GBs (the #34 pile-up hit 16.9GB / ~355% CPU). The sampler also records
    system memory + swap so a run on a pressured machine is self-documenting.
    """
    sampler = pr.ProcTreeSampler(mgr.proc.pid, interval=0.1)

    # Read the server log only from where it stands now, so we parse this run's
    # spans (BUCKAROO_PERF=1) rather than any prior content.
    log_path = _server_log_path()
    log_offset = log_path.stat().st_size if log_path.exists() else 0

    # Cold: production ensure_session against the empty overlay cache (10s cap).
    t0 = time.monotonic()
    session_cold = mgr.ensure_session(content_hash, project)
    backend_cold_s = round(time.monotonic() - t0, 2)
    cold_capped = session_cold is None
    if cold_capped:
        # The cap tripped: the server still finished, so a plain uncapped POST
        # mints a renderable (now-warm) session to confirm the grid paints. Its
        # wall time is NOT the cold cost — that comes from the load_expr span.
        session_cold = _load_expr(mgr, project, content_hash)
    render_cold = _time_grid_render(browser, mgr.base_url, session_cold)

    # Warm A — same-lifetime re-view: ensure_session returns the cached id.
    t0 = time.monotonic()
    mgr.ensure_session(content_hash, project)
    warm_cached_s = round(time.monotonic() - t0, 3)

    # Warm B — server-restart path: evict, then re-POST against the on-disk cache.
    _evict_session(mgr, content_hash)
    t0 = time.monotonic()
    session_warm = mgr.ensure_session(content_hash, project)
    backend_warm_s = round(time.monotonic() - t0, 2)
    if session_warm is None:  # warm capped too (unexpected) — fall back to render-only
        session_warm = _load_expr(mgr, project, content_hash)
    render_warm = _time_grid_render(browser, mgr.base_url, session_warm)
    sampler.stop()

    # buckaroo#926 server-side breakdown for the cold /load_expr. When the cap
    # tripped we don't have the cold session id, so take the first session's
    # spans (the capped cold POST, logged on server-side completion).
    appended = ""
    if log_path.exists():
        with log_path.open() as f:
            f.seek(log_offset)
            appended = f.read()
    backend_spans = _read_first_backend_spans(appended) if cold_capped else _read_backend_spans(appended, session_cold)
    true_backend_cold = backend_spans.get("firstpull.load_expr")

    total_cold_s = round(backend_cold_s + render_cold["grid_s"], 2)
    return {
        "entry": content_hash,
        # Production wall: ensure_session's capped /load_expr (≈10s if it tripped).
        "backend_cold_s": backend_cold_s,
        # Whether production would have served a live session or fallen back.
        "cold_capped": cold_capped,
        # Uncapped server-side truth from the #926 span (present even when capped).
        "true_backend_cold_s": round(true_backend_cold, 2) if true_backend_cold is not None else None,
        "render_cold_s": render_cold["grid_s"],
        "total_cold_s": total_cold_s,
        # The deliverable: fraction of the cold user-wait the proxy can see.
        "backend_ratio": round(backend_cold_s / total_cold_s, 3) if total_cold_s else None,
        # Same-lifetime re-view (in-memory session cache hit).
        "warm_cached_s": warm_cached_s,
        # Server-restart re-load against the on-disk stat cache.
        "backend_warm_s": backend_warm_s,
        "render_warm_s": render_warm["grid_s"],
        "fcp_ms": render_cold["fcp_ms"],
        "dcl_ms": render_cold["dcl_ms"],
        "cells": render_cold["cells"],
        # buckaroo#926 server-side /load_expr phase breakdown for the cold session.
        "backend_spans": backend_spans,
        # Buckaroo subprocess tree, not the driver (see docstring).
        "buckaroo_peak_mb": round(sampler.peak_rss() / MB),
        "buckaroo_peak_cpu": round(sampler.peak_cpu()),
        "sys_mem_pct": round(sampler.peak_sys_mem_pct()),
        "sys_swap_mb": sampler.peak_swap_mb(),
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
        "Ground truth: real Buckaroo subprocess (driven through the companion's "
        "production `ensure_session` → `/load_expr`, **10s cap**) + Chromium, to "
        "`.df-viewer .ag-cell` first-visible. `backend cold` is the capped "
        "`ensure_session` wall — `cold ok` says whether it returned a live "
        "session (✓) or the cap tripped (CAPPED → production falls back to the "
        "pandas preview, #34); `true backend` is the uncapped server-side cost "
        "from the #926 span. `render` is the WS + first-page + ag-grid paint the "
        "Tier-B proxy cannot see. `warm cached` is a same-lifetime re-view "
        "(in-memory session hit); `backend warm` is a server-restart re-load "
        "against the on-disk stat cache.",
        "",
        "## Calibration (Tier A ground truth vs Tier B proxy)",
        "",
        "| entry | cold ok | backend cold s | true backend s | render cold s | total cold s | ratio | "
        "Tier B backend cold s | warm cached s | backend warm s | render warm s |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        a = r["tier_a"]
        if "error" in a:
            L.append(f"| {label(r['entry'])} | ERROR: {a['error']} ||||||||||")
            continue
        b = r.get("tier_b", {})
        tier_b_backend = "—" if "error" in b else round(b.get("load_s", 0) + b.get("cold_stats_s", 0), 2)
        ratio = "—" if a["backend_ratio"] is None else f"{a['backend_ratio']:.0%}"
        cold_ok = "CAPPED" if a.get("cold_capped") else "✓"
        true_backend = "—" if a.get("true_backend_cold_s") is None else a["true_backend_cold_s"]
        L.append(
            f"| {label(r['entry'])} | {cold_ok} | {a['backend_cold_s']} | {true_backend} "
            f"| {a['render_cold_s']} | {a['total_cold_s']} | {ratio} | {tier_b_backend} "
            f"| {a['warm_cached_s']} | {a['backend_warm_s']} | {a['render_warm_s']} |"
        )

    L += [
        "",
        "## Server resource usage (Buckaroo subprocess tree)",
        "",
        "`/load_expr` runs in the Buckaroo subprocess, so these are sampled from "
        "`mgr.proc.pid` + its children — not this test driver, which barely "
        "moves. `peak CPU` can exceed 100% on multi-core stat compute. "
        "`sys mem` / `sys swap` are the worst the machine showed during the run.",
        "",
        "| entry | Buckaroo peak MB | Buckaroo peak CPU % | sys mem % | sys swap MB |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        a = r["tier_a"]
        if "error" in a:
            continue
        L.append(
            f"| {label(r['entry'])} | {a['buckaroo_peak_mb']} | {a['buckaroo_peak_cpu']} "
            f"| {a['sys_mem_pct']} | {a['sys_swap_mb']} |"
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

    L += [
        "",
        "## Backend breakdown — `/load_expr` server spans (BUCKAROO_PERF=1, buckaroo#926)",
        "",
        "Server-side spans for the **cold** `/load_expr`, parsed from the buckaroo "
        "server log by `session=`. `firstpull.load_expr` is the outer total the "
        "`backend cold s` column measures; the rest break it down. "
        "`firstpull.ws_first_payload` is excluded (it fires during the render).",
        "",
        "| entry | span | secs |",
        "|---|---|---|",
    ]
    any_spans = False
    for r in rows:
        a = r["tier_a"]
        if "error" in a:
            continue
        spans = a.get("backend_spans") or {}
        # load_expr (the total) first, then the rest by descending secs.
        ordered = sorted(spans.items(), key=lambda kv: (kv[0] != "firstpull.load_expr", -kv[1]))
        for span_label, secs in ordered:
            any_spans = True
            L.append(f"| {label(r['entry'])} | {span_label} | {secs:.4f} |")
    if not any_spans:
        L.append("| (no spans captured — BUCKAROO_PERF off, or buckaroo < 0.15) | | |")

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
        # #926: the subprocess inherits this env (BuckarooManager.start passes no
        # env=), so its /load_expr handler emits firstpull spans to the server log.
        monkeypatch_module.setenv("BUCKAROO_PERF", "1")
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
