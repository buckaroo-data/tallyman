"""Measure the Tier-B page-load warm path and first-page render, optionally under cProfile.

The Tier-B harness (``tests/test_perf_integration.py``) reports a ``warm stats s``
and a ``1st page s`` per entry. When one of those numbers looks wrong, guessing
where the time goes is a copout — this script runs *exactly* those two calls for
each entry and (with ``--profile``) attributes the wall time to a call stack.

Profiling is opt-in. By default the two calls are plain-timed, since cProfile
instrumentation inflates wall time; pass ``--profile`` for the per-stage
attribution (``count`` / ``stats`` / ``window``) and ``.prof`` dumps.

It reuses the harness's own overlay + loading helpers (imported from the test
module) so it can't drift from what the report measures. It builds one
write-isolated overlay — the user's real caches are never touched — and for each
entry warms the on-disk stat cache with one un-profiled cold run, then measures:

* **warm stats** — a fresh ``XorqServerDataflow`` re-reading the now-populated
  on-disk stat cache. This is the ``warm stats s`` column: the path a server
  restart takes (cold in-memory, warm on disk).
* **first page** — ``handle_infinite_request_xorq`` for the 0-300 window. This
  is *not* catalog-server bookkeeping: Tier B has no server. It is real
  in-process datafusion query execution against the expression —
  ``expr.count().execute()`` (a full ``COUNT(*)``) plus a ``LIMIT 300`` slice
  materialised to parquet bytes (``window_to_parquet``).

Both calls execute the **bare build recipe** loaded by ``/load_expr`` — not the
cached-result expression — so for a cache-worthy entry (Aggregate/Join) the
count and the window each re-run the whole DAG instead of reading the
materialised ``result.parquet``. The per-stage attribution below makes that
visible: ``count s`` + ``window s`` are the recompute cost, ``stats s`` is the
only thing the (stat) cache actually covers.

Usage::

    # every built entry in the corpus — plain wall timing (default)
    uv run python scripts/profile_pageload.py --all [--project ...]
    # one entry under cProfile, with a full top-N pstats dump
    uv run python scripts/profile_pageload.py <hash-or-alias> --profile --top 40 --out DIR

``--out DIR`` writes the raw ``.prof`` dumps (loadable in ``snakeviz`` /
``pstats``) and a markdown report; without it the report prints to stdout.
"""

from __future__ import annotations

import argparse
import cProfile
import importlib.util
import json
import os
import pstats
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_test_module():
    """Import ``tests/test_perf_integration.py`` for its overlay + helpers.

    The module is marker-gated (``pytestmark = pytest.mark.perf``) but importing
    it runs no tests; we only borrow ``_build_overlay`` and the loading glue so
    the profile path stays byte-for-byte the report's path.
    """
    path = REPO_ROOT / "tests" / "test_perf_integration.py"
    spec = importlib.util.spec_from_file_location("perf_integration", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def _built_entries(real_dir: Path) -> list[str]:
    entries = real_dir / "artifacts" / "catalog" / "entries"
    return sorted(c.name for c in entries.iterdir() if (c / "xorq_build").is_dir() and (c / "result.parquet").exists())


def _resolve_entry(real_dir: Path, token: str) -> str:
    """Resolve an alias or hash prefix to a full content hash with a build."""
    built = _built_entries(real_dir)
    aliases_path = real_dir / "artifacts" / "catalog" / "aliases.json"
    aliases = json.loads(aliases_path.read_text()) if aliases_path.exists() else {}

    if token in aliases and aliases[token] in built:
        return aliases[token]
    if token in built:
        return token
    matches = [h for h in built if h.startswith(token)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        sys.exit(f"ambiguous entry {token!r}: matches {', '.join(m[:12] for m in matches)}")
    sys.exit(f"no built entry resolves {token!r} in {real_dir.name} ({len(built)} built entries)")


def _profile(fn) -> tuple[cProfile.Profile, float, object]:
    """Run ``fn`` once under cProfile; return (profiler, wall_s, result)."""
    prof = cProfile.Profile()
    t0 = time.monotonic()
    prof.enable()
    result = fn()
    prof.disable()
    return prof, time.monotonic() - t0, result


def _time(fn) -> tuple[None, float, object]:
    """Run ``fn`` once with plain wall timing — no cProfile overhead.

    Same return shape as ``_profile`` (profiler slot is ``None``) so the caller
    is mode-agnostic. cProfile instrumentation inflates wall time, so the
    default (un-profiled) run reports walls that line up with the harness; pass
    ``--profile`` only when you want the per-stage attribution.
    """
    t0 = time.monotonic()
    result = fn()
    return None, time.monotonic() - t0, result


def _cumtime(prof: cProfile.Profile, func_substr: str, file_substr: str = "") -> float:
    """Largest cumulative time among functions matching name (+ optional file).

    pstats keys are ``(file, line, funcname)`` → ``(cc, nc, tt, ct, callers)``.
    Matching by name and taking the max ``ct`` picks the outermost occurrence,
    which is the attribution we want (e.g. the top-level ``to_pyarrow``).
    Returns 0.0 when nothing matches (a stage that didn't run / wasn't hit).
    """
    stats = pstats.Stats(prof)
    best = 0.0
    for (fname, _line, func), (_cc, _nc, _tt, ct, _callers) in stats.stats.items():  # type: ignore[attr-defined]
        if func_substr == func and (not file_substr or file_substr in fname):
            best = max(best, ct)
    return best


def _dump(prof: cProfile.Profile, label: str, wall: float, top: int) -> str:
    buf = __import__("io").StringIO()
    stats = pstats.Stats(prof, stream=buf)
    stats.sort_stats("cumulative")
    buf.write(f"### {label} — wall {wall:.3f}s (top {top} by cumulative)\n\n```\n")
    stats.print_stats(top)
    buf.write("```\n")
    return buf.getvalue()


def _profile_entry(
    tm, xorq_loading, project: str, content_hash: str, profile: bool, out: Path | None, top: int
) -> dict:
    """Warm the entry's stat cache, then measure warm-stats and first-page.

    ``profile`` toggles cProfile: off (default) just times the two calls — the
    accurate, low-overhead path; on attributes each wall to its call stack
    (``count`` / ``stats`` / ``window``) and dumps ``.prof`` files.
    """
    from tallyman_core.paths import (  # noqa: PLC0415
        entry_build_dir,
        entry_expanded_build_dir,
        entry_stat_cache_dir,
        project_dir,
    )
    from tallyman_xorq.portable import ensure_expanded_build  # noqa: PLC0415

    build_dir = entry_build_dir(project, content_hash)
    expanded = ensure_expanded_build(build_dir, project_dir(project), entry_expanded_build_dir(project, content_hash))
    stat_cache = entry_stat_cache_dir(project, content_hash)

    # Cold run (always un-profiled): populate the on-disk stat cache so the
    # measured warm run is genuinely warm — the setup measure_pageload's warm leg uses.
    tm._stats_dataflow(xorq_loading, xorq_loading.load_expr_build_dir(str(expanded)), stat_cache)

    run = _profile if profile else _time
    warm_prof, warm_wall, (df2, meta) = run(
        lambda: tm._stats_dataflow(xorq_loading, xorq_loading.load_expr_build_dir(str(expanded)), stat_cache)
    )
    page_prof, page_wall, _ = run(
        lambda: xorq_loading.handle_infinite_request_xorq(
            df2, {"start": 0, "end": 300, "sort": None, "sort_direction": None}
        )
    )

    row = {
        "entry": content_hash,
        "rows": meta["rows"],
        "cols": len(meta["columns"]),
        "warm_wall": warm_wall,
        "page_wall": page_wall,
        # Per-stage attribution only exists when cProfile ran; None renders "—".
        # warm = load recipe + COUNT(*) (populate_df_meta) + stat pipeline;
        # first page = COUNT(*) (cached from warm, ~0) + LIMIT 300 to parquet.
        "warm_load": _cumtime(warm_prof, "load_expr_build_dir") if profile else None,
        "warm_count": _cumtime(warm_prof, "_expr_count") if profile else None,
        "warm_stats": _cumtime(warm_prof, "_get_summary_sd") if profile else None,
        "page_count": _cumtime(page_prof, "_expr_count") if profile else None,
        "page_window": _cumtime(page_prof, "to_pyarrow", "expr/api.py") if profile else None,
    }
    if profile and out:
        out.mkdir(parents=True, exist_ok=True)
        warm_prof.dump_stats(str(out / f"warm_{content_hash[:12]}.prof"))
        page_prof.dump_stats(str(out / f"page_{content_hash[:12]}.prof"))
        if top:
            (out / f"pstats_{content_hash[:12]}.md").write_text(
                _dump(warm_prof, f"warm stats — {content_hash[:12]}", warm_wall, top)
                + "\n"
                + _dump(page_prof, f"first page — {content_hash[:12]}", page_wall, top)
            )
    return row


def _classify(build_dir: Path) -> str:
    from tallyman_xorq.result_cache import classify_build  # noqa: PLC0415

    return classify_build(build_dir)["why"]


def _render(project: str, rows: list[dict], real_dir: Path, profiled: bool) -> str:
    aliases = real_dir / "artifacts" / "catalog" / "aliases.json"
    by_hash = {h: a for a, h in (json.loads(aliases.read_text()) if aliases.exists() else {}).items()}

    def cell(v) -> str:
        return f"{v:.2f}" if isinstance(v, (int, float)) else "—"

    mode = (
        "warm stats + first page, each under cProfile, attributed by cumulative time."
        if profiled
        else "warm stats + first page, plain wall timing (run with `--profile` for the per-stage attribution)."
    )
    L = [
        f"# Page-load profile — `{project}` (Tier-B full corpus)",
        "",
        f"{len(rows)} entries · {mode}",
        "",
        "`count` is `expr.count().execute()`; `stats` is the XorqDfStatsV2 pipeline (the only stage the "
        "stat cache covers); `window` is the `LIMIT 300` slice to parquet. For an aggregate/join recipe "
        "`count`+`window` recompute the whole DAG — the materialised `result.parquet` is never read by "
        "the viewer path.",
        "",
        "| entry | rows | class | warm s | ↳load | ↳count | ↳stats | page s | ↳count | ↳window |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        h = r["entry"]
        bd = real_dir / "artifacts" / "catalog" / "entries" / h / "xorq_build"
        cls = _classify(bd).replace("cheap (parquet read + projection/scalar only)", "cheap")
        label = f"{by_hash.get(h, '(scratch)')} `{h[:8]}`"
        L.append(
            f"| {label} | {r['rows']:,} | {cls} | {cell(r['warm_wall'])} | {cell(r['warm_load'])} | "
            f"{cell(r['warm_count'])} | {cell(r['warm_stats'])} | {cell(r['page_wall'])} | "
            f"{cell(r['page_count'])} | {cell(r['page_window'])} |"
        )
    L.append("")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("entry", nargs="?", help="entry alias or content-hash prefix (omit with --all)")
    ap.add_argument("--all", action="store_true", help="measure every built entry in the corpus")
    ap.add_argument(
        "--profile",
        action="store_true",
        help="run under cProfile for per-stage attribution + .prof dumps (off by default — "
        "plain timing avoids cProfile's wall inflation)",
    )
    ap.add_argument("--project", default="parking_ticket_analysis")
    ap.add_argument("--top", type=int, default=0, help="with --profile: also dump top-N pstats per entry (needs --out)")
    ap.add_argument("--out", type=Path, default=None, help="dir for .prof dumps + markdown")
    args = ap.parse_args()
    if not args.all and not args.entry:
        ap.error("give an entry or --all")
    if args.top and not args.profile:
        ap.error("--top needs --profile")

    tm = _load_test_module()
    real_dir = tm._real_projects_root() / args.project
    if not real_dir.is_dir():
        sys.exit(f"corpus absent: {real_dir}")

    hashes = _built_entries(real_dir) if args.all else [_resolve_entry(real_dir, args.entry)]

    overlay_root = Path(tempfile.mkdtemp(prefix="profile-pageload-"))
    tm._build_overlay(overlay_root, args.project, real_dir)
    os.environ["TALLYMAN_HOME"] = str(overlay_root)
    os.environ["TALLYMAN_PROJECT"] = args.project

    xorq_loading = tm._import_buckaroo_loading()

    verb = "profiling" if args.profile else "measuring"
    rows = []
    for i, h in enumerate(hashes, 1):
        print(f"[{i}/{len(hashes)}] {verb} {h[:12]} ...", file=sys.stderr)
        try:
            rows.append(_profile_entry(tm, xorq_loading, args.project, h, args.profile, args.out, args.top))
        except Exception as e:  # one bad entry shouldn't sink the batch
            print(f"  ERROR {h[:12]}: {e}", file=sys.stderr)
            rows.append(
                {
                    "entry": h,
                    "rows": 0,
                    "cols": 0,
                    "warm_wall": 0,
                    "page_wall": 0,
                    "warm_load": None,
                    "warm_count": None,
                    "warm_stats": None,
                    "page_count": None,
                    "page_window": None,
                }
            )

    rows.sort(key=lambda r: -r["rows"])
    report = _render(args.project, rows, real_dir, args.profile)
    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "profile_corpus.md").write_text(report)
        print(f"\nwrote {args.out / 'profile_corpus.md'} + .prof dumps", file=sys.stderr)
    print("\n" + report)


if __name__ == "__main__":
    main()
