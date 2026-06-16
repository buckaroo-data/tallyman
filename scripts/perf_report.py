"""tallyman perf regression report — one scenario per known pathology.

Usage:
    uv run python scripts/perf_report.py [--workdir DIR] [--scale X] \
        [--out perf_report.md] [--json perf_report.json] [--force]

This is a REPORT, not a test suite. It always exits 0 when the harness
completes — known problems are expected to show up in the numbers until they
are fixed, so regressions are read off the report (CI job summary + artifact),
not off a pass/fail bit. Per-scenario failures are recorded as ERROR rows.
Target total wall time is under 4 minutes on a GitHub ubuntu-latest runner;
the report flags itself when it goes over.

All inputs are synthetic, seeded (np.random.default_rng(0)), generated under
--workdir (default <tmpdir>/tallyman-perf) and reused when present; --force
wipes the workdir first. Shapes are scaled-down mirrors of the
parking_ticket_analysis catalog whose 2026-06-10 instrumented replay surfaced
the regressions this suite pins (tallyman#34, tallyman#35 context,
buckaroo#910, buckaroo#911).

Scenarios — one failure mode each:

  build-<entry>        xorq build_expr -> execute -> persist pipeline, fresh
                       process per entry. Good: wall time dominated by the
                       execute; flat overhead across releases.
  xorq-union-stream    scan/union execution streams. Good: peak RSS well below
                       input size. Bad: peak RSS ~ input size (lost
                       out-of-core).
  xorq-agg-highcard    hash-aggregation (count/min/max) state is bounded by
                       group cardinality. Good: peak RSS ~ cardinality, not
                       input rows (~1GB at 4M rows / 1.5M groups).
  xorq-agg-nunique     grouped exact COUNT(DISTINCT): ~37KB transient PER
                       GROUP in datafusion (measured 2026-06-10: 100k rows /
                       37k groups -> 3.7GB peak; 400k / 150k -> 13.9GB; the
                       min/max equivalent peaks at ~260MB). Deliberately
                       pinned at a tiny 100k-row shape so the suite survives;
                       do NOT scale this one up.
  bk-highcard-cold     buckaroo /load_expr stat pipeline on a high-cardinality
                       string column — exact nunique + top-k categorical
                       histogram are ~the entire memory cost (buckaroo#911).
                       Compare against bk-lowcard-cold for the premium.
  bk-highcard-warm     same entry, fresh server, warm stat-snapshot cache.
                       Good: no spike, load well under cold. Bad: cache files
                       absent or unused (the live-system empty-cache failure).
  bk-highcard-nocache  same entry, no cache_storage_path. cold-with-cache
                       minus this isolates snapshot WRITE overhead, which
                       nearly doubled cold loads at 26M rows (buckaroo#910).
  bk-lowcard-cold      control: identical shape, low-cardinality strings.
  bk-agg-firstpage     aggregate-backed entry: stat pipeline over an
                       ~all-unique column + the first row page, which re-runs
                       the whole aggregation just to serve rows 0-300.
  companion-session    the real companion path (BuckarooManager.ensure_session,
                       cold cache): wall time vs the hard /load_expr timeout
                       that silently drops sessions when exceeded
                       (tallyman#34). Good: session created with headroom.

Derived signals at the bottom tie these together: warm speedup, snapshot-write
overhead, high-card stat premium, cache-actually-written, timeout headroom.

Not covered yet (follow-ups): diff-compare /load_expr (result_cache path,
PR #27), customizations-endpoint request cost (#28), checkpoint capture
cost vs cache size (#22).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

MB = 2**20
SEED = 0
PROJECT = "perf"
DEFAULT_WORKDIR = Path(tempfile.gettempdir()) / "tallyman-perf"
BUDGET_SECONDS = 240.0
IDLE_SETTLE_SECONDS = 4.0

# Row counts at --scale 1.0. Calibrated so the high-card stat spike is an
# unambiguous multiple of the low-card control while staying safe on a 7GB
# CI runner (the full-size pathology was 26M rows / 5.3M distinct -> 2.3GB).
EVENTS_ROWS = 4_000_000
PLATE_POOL = 1_500_000
LOWCARD_POOL = 1_000
WIDE_ROWS = 3_000_000
WIDE_COLS = 8
SMALL_LIMIT = 50_000
# The grouped-nunique memory bomb is pinned small ON PURPOSE: ~37KB transient
# per group means 150k groups already cost ~14GB (see module docstring).
AGG_NUNIQUE_ROWS = 100_000
AGG_NUNIQUE_POOL = 37_000

log = logging.getLogger("perf_report")


# ---------------------------------------------------------------------------
# measurement helpers
# ---------------------------------------------------------------------------


def _ru_maxrss_bytes() -> int:
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kilobytes.
    return ru if sys.platform == "darwin" else ru * 1024


class RssSampler:
    """Sample a process's RSS on a background thread; keeps (t_rel, rss) pairs."""

    def __init__(self, psutil_proc, interval: float = 0.05):
        self._proc = psutil_proc
        self._interval = interval
        self._stop = threading.Event()
        self._t0 = time.monotonic()
        self.samples: list[tuple[float, int]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        import psutil

        while not self._stop.is_set():
            try:
                self.samples.append((time.monotonic() - self._t0, self._proc.memory_info().rss))
            except psutil.Error:
                return
            self._stop.wait(self._interval)

    def now(self) -> float:
        return time.monotonic() - self._t0

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)

    def peak(self, until: float | None = None) -> int:
        vals = [r for t, r in self.samples if until is None or t <= until]
        return max(vals, default=0)


class ProcTreeSampler:
    """Sample RSS + CPU% of a process *and all its descendants* on a bg thread.

    ``RssSampler`` watches a single process — right when the work runs
    in-process (Tier B), wrong when it runs in a spawned server (Tier A): the
    Buckaroo subprocess and any workers it forks are where the GBs live, and a
    driver-process sampler reports a few hundred MB while the machine swaps.
    This samples the whole tree rooted at ``root_pid``, so server child
    processes count toward the peak, and records system memory + swap each tick
    so a run on a memory-pressured machine is self-documenting.

    ``peak_rss`` is the max over ticks of the summed RSS of every live process
    in the tree; ``peak_cpu`` the max summed ``cpu_percent`` (can exceed 100 on
    multi-core compute — the #34 pile-up hit ~355%). System fields are the
    worst (lowest free / highest swap) seen across the run.
    """

    def __init__(self, root_pid: int, interval: float = 0.1):
        import psutil

        self._psutil = psutil
        self._root_pid = root_pid
        self._interval = interval
        self._stop = threading.Event()
        self._t0 = time.monotonic()
        self.rss_samples: list[int] = []
        self.cpu_samples: list[float] = []
        self.sys_mem_pct: list[float] = []
        self.sys_swap_mb: list[int] = []
        # pid -> psutil.Process, kept across ticks: cpu_percent() diffs against
        # the previous call *on the same instance*, so a fresh Process every
        # tick would read 0.0 forever. Cache and reuse the instances.
        self._procs: dict[int, object] = {}
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _refresh_tree(self) -> list:
        """Current tree procs, reusing cached instances so cpu_percent has a
        reference point. New children are primed (first cpu_percent → 0.0) and
        dead pids dropped."""
        psutil = self._psutil
        try:
            root = self._procs.get(self._root_pid) or psutil.Process(self._root_pid)
        except psutil.Error:
            return []
        self._procs[self._root_pid] = root
        alive = {self._root_pid}
        try:
            for child in root.children(recursive=True):
                alive.add(child.pid)
                if child.pid not in self._procs:
                    self._procs[child.pid] = child
                    try:
                        child.cpu_percent()  # prime the new child
                    except psutil.Error:
                        pass
        except psutil.Error:
            pass
        for pid in [p for p in self._procs if p not in alive]:
            del self._procs[pid]
        return [self._procs[pid] for pid in alive if pid in self._procs]

    def _run(self) -> None:
        psutil = self._psutil
        # Prime cpu_percent: the first call per process returns 0.0 (it measures
        # since the previous call), so seed every proc before the first reading.
        for p in self._refresh_tree():
            try:
                p.cpu_percent()
            except psutil.Error:
                pass
        while not self._stop.is_set():
            rss = 0
            cpu = 0.0
            for p in self._refresh_tree():
                try:
                    rss += p.memory_info().rss
                    cpu += p.cpu_percent()  # 0.0 the first time a new child appears
                except psutil.Error:
                    continue
            self.rss_samples.append(rss)
            self.cpu_samples.append(cpu)
            vm = psutil.virtual_memory()
            sw = psutil.swap_memory()
            self.sys_mem_pct.append(vm.percent)
            self.sys_swap_mb.append(round(sw.used / MB))
            self._stop.wait(self._interval)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)

    def peak_rss(self) -> int:
        return max(self.rss_samples, default=0)

    def peak_cpu(self) -> float:
        return max(self.cpu_samples, default=0.0)

    def peak_sys_mem_pct(self) -> float:
        return max(self.sys_mem_pct, default=0.0)

    def peak_swap_mb(self) -> int:
        return max(self.sys_swap_mb, default=0)


def _dir_kb_and_files(path: Path) -> tuple[int, int]:
    total = 0
    files = 0
    if path.is_dir():
        for p in path.rglob("*"):
            if p.is_file():
                total += p.stat().st_size
                files += 1
    return total // 1024, files


# ---------------------------------------------------------------------------
# synthetic data (seeded, idempotent)
# ---------------------------------------------------------------------------


def _plate_pool(n: int, rng) -> "object":
    import numpy as np

    return np.char.add("PLT", np.char.zfill(rng.permutation(n).astype("U9"), 9))


def gen_events(path: Path, rows: int, pool_size: int, rng) -> None:
    """Parking-slice shape: high/low-card strings + timestamp + binary flag."""
    import numpy as np
    import polars as pl

    pool = _plate_pool(pool_size, rng)
    states = np.array([f"S{i:02d}" for i in range(50)])
    df = pl.DataFrame(
        {
            "plate": pool[rng.integers(0, pool_size, rows)],
            "state": states[rng.integers(0, 50, rows)],
            "violation": np.array(["SPEED", "REDLIGHT"])[rng.integers(0, 2, rows)],
            "issue_ts": (
                np.datetime64("2025-01-01") + rng.integers(0, 365 * 24 * 3600, rows).astype("timedelta64[s]")
            ).astype("datetime64[us]"),
        }
    )
    df.write_parquet(path)


def gen_wide(path: Path, rows: int, rng) -> None:
    """Numeric-heavy scan source for the union-streaming scenario."""
    import polars as pl

    cols = {f"f_{i}": rng.normal(size=rows) for i in range(WIDE_COLS)}
    cols["id"] = rng.integers(0, 1_000_000, rows)
    pl.DataFrame(cols).write_parquet(path)


def generate_datasets(data_dir: Path, scale: float) -> list[dict]:
    import numpy as np

    rows_events = max(int(EVENTS_ROWS * scale), 10_000)
    pool_high = max(int(PLATE_POOL * scale), 5_000)
    rows_wide = max(int(WIDE_ROWS * scale), 10_000)
    rows_nunique = max(int(AGG_NUNIQUE_ROWS * min(scale, 1.0)), 10_000)
    pool_nunique = max(int(AGG_NUNIQUE_POOL * min(scale, 1.0)), 3_000)

    specs = [
        (
            "events.parquet",
            rows_events,
            lambda p, r: gen_events(p, rows_events, pool_high, np.random.default_rng(SEED)),
        ),
        (
            "events_lowcard.parquet",
            rows_events,
            lambda p, r: gen_events(p, rows_events, LOWCARD_POOL, np.random.default_rng(SEED)),
        ),
        ("wide_a.parquet", rows_wide, lambda p, r: gen_wide(p, rows_wide, np.random.default_rng(SEED))),
        ("wide_b.parquet", rows_wide, lambda p, r: gen_wide(p, rows_wide, np.random.default_rng(SEED + 1))),
        (
            "agg_small.parquet",
            rows_nunique,
            lambda p, r: gen_events(p, rows_nunique, pool_nunique, np.random.default_rng(SEED + 2)),
        ),
    ]
    out = []
    for name, rows, fn in specs:
        path = data_dir / name
        if path.exists():
            out.append({"dataset": name, "rows": rows, "mb": path.stat().st_size // MB, "gen_s": None})
            continue
        t0 = time.monotonic()
        fn(path, rows)
        out.append(
            {"dataset": name, "rows": rows, "mb": path.stat().st_size // MB, "gen_s": round(time.monotonic() - t0, 2)}
        )
        log.info("generated %s (%d rows, %dMB)", name, rows, path.stat().st_size // MB)
    return out


# ---------------------------------------------------------------------------
# catalog entries
# ---------------------------------------------------------------------------

# Built in dependency order; agg_by_plate references events_scan's hash.
ENTRY_ORDER = ["events_scan", "lowcard_scan", "union_wide", "small_limit", "agg_by_plate", "agg_nunique"]


def entry_code(key: str, hashes: dict[str, str], scale: float) -> str:
    limit = max(int(SMALL_LIMIT * scale), 1_000)
    codes = {
        "events_scan": "from tallyman_xorq.io import from_project\nexpr = from_project('events.parquet')\n",
        "lowcard_scan": "from tallyman_xorq.io import from_project\nexpr = from_project('events_lowcard.parquet')\n",
        "union_wide": (
            "from tallyman_xorq.io import from_project\n"
            "a = from_project('wide_a.parquet')\n"
            "b = from_project('wide_b.parquet')\n"
            "expr = a.union(b)\n"
        ),
        "small_limit": (
            f"from tallyman_xorq.io import from_project\nexpr = from_project('events.parquet').limit({limit})\n"
        ),
        # Grouped approximate distinct count. The exact `nunique()` here is the
        # datafusion memory bomb (#46: ~37KB transient/group); `approx_nunique`
        # is the bounded-state alternative an entry should use. Kept on its own
        # small dataset regardless; keep it OFF the 4M-row events table.
        "agg_nunique": (
            "from tallyman_xorq.io import from_project\n"
            "t = from_project('agg_small.parquet')\n"
            "expr = t.group_by('plate').aggregate(\n"
            "    n_tickets=t.count(),\n"
            "    n_days=t.issue_ts.approx_nunique(),\n"
            "    n_states=t.state.approx_nunique(),\n"
            ")\n"
        ),
    }
    if key == "agg_by_plate":
        events_hash = hashes["events_scan"]
        return (
            "from tallyman_xorq.io import from_catalog\n"
            f"t = from_catalog('{events_hash}')\n"
            "expr = t.group_by('plate').aggregate(\n"
            "    n_tickets=t.count(),\n"
            "    first_seen=t.issue_ts.min(),\n"
            "    last_seen=t.issue_ts.max(),\n"
            ")\n"
        )
    return codes[key]


# ---------------------------------------------------------------------------
# child modes (fresh process per measurement, JSON on the last stdout line)
# ---------------------------------------------------------------------------


def child_build(key: str, workdir: Path, scale: float) -> None:
    import psutil

    sampler = RssSampler(psutil.Process(), interval=0.025)
    hashes = _load_hashes(workdir)
    code = entry_code(key, hashes, scale)

    from tallyman_xorq.build import build_and_persist

    t0 = time.monotonic()
    res = build_and_persist(PROJECT, code)
    wall = time.monotonic() - t0
    sampler.stop()
    result_path = res.entry_path / "result.parquet"
    print(
        json.dumps(
            {
                "key": key,
                "hash": res.content_hash,
                "rows": res.row_count,
                "wall_s": round(wall, 2),
                "execute_s": res.execute_seconds,
                "peak_mb": round(max(sampler.peak(), _ru_maxrss_bytes()) / MB),
                # #73: a build no longer writes result.parquet (cheap entries
                # materialise nothing; expensive ones bake into the compute
                # cache). None when absent — the comparable signal is wall/exec.
                "result_mb": round(result_path.stat().st_size / MB, 1) if result_path.exists() else None,
            }
        )
    )


def child_exec(content_hash: str, out_path: str) -> None:
    import psutil

    sampler = RssSampler(psutil.Process(), interval=0.025)

    from tallyman_xorq.build import load_entry

    t0 = time.monotonic()
    expr = load_entry(PROJECT, content_hash)
    load_s = time.monotonic() - t0
    t0 = time.monotonic()
    expr.to_parquet(out_path)
    exec_s = time.monotonic() - t0
    sampler.stop()

    import pyarrow.parquet as pq

    print(
        json.dumps(
            {
                "hash": content_hash,
                "rows": pq.ParquetFile(out_path).metadata.num_rows,
                "load_s": round(load_s, 2),
                "exec_s": round(exec_s, 2),
                "peak_mb": round(max(sampler.peak(), _ru_maxrss_bytes()) / MB),
                "out_mb": round(os.path.getsize(out_path) / MB, 1),
            }
        )
    )


def run_child(mode: str, argv: list[str], log_path: Path, workdir: Path, scale: float) -> dict:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--workdir",
        str(workdir),
        "--scale",
        str(scale),
        "--child",
        mode,
        *argv,
    ]
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    with open(log_path, "w") as err:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=err, text=True, env=env)
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    if proc.returncode != 0 or not lines:
        tail = Path(log_path).read_text()[-2000:]
        raise RuntimeError(f"child {mode} {argv} failed (rc={proc.returncode}):\n{tail}")
    return json.loads(lines[-1])


# ---------------------------------------------------------------------------
# buckaroo server scenarios
# ---------------------------------------------------------------------------


class BuckarooServer:
    """Spawn a fresh `python -m buckaroo.server --port 0` and wait for readiness."""

    def __init__(self, log_path: Path, startup_timeout: float = 30.0):
        import httpx
        import psutil

        self._log_fp = open(log_path, "w")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "buckaroo.server", "--port", "0", "--no-browser"],
            stdout=subprocess.PIPE,
            stderr=self._log_fp,
            text=True,
            bufsize=1,
        )
        port = None
        deadline = time.monotonic() + startup_timeout
        assert self.proc.stdout is not None
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"buckaroo exited during startup (rc={self.proc.returncode})")
                continue
            self._log_fp.write(line)
            if line.startswith("BUCKAROO_PORT="):
                port = int(line.strip().split("=", 1)[1])
                break
        if port is None:
            self.stop()
            raise RuntimeError("buckaroo did not print a port handshake")
        self.url = f"http://127.0.0.1:{port}"
        self.ws_url = f"ws://127.0.0.1:{port}"
        threading.Thread(target=self._drain, daemon=True).start()
        with httpx.Client() as client:
            while time.monotonic() < deadline:
                try:
                    if client.get(f"{self.url}/health", timeout=1.0).status_code == 200:
                        break
                except httpx.HTTPError:
                    time.sleep(0.1)
            else:
                self.stop()
                raise RuntimeError("buckaroo did not respond to /health")
        self.ps = psutil.Process(self.proc.pid)

    def _drain(self) -> None:
        try:
            for line in self.proc.stdout:  # type: ignore[union-attr]
                self._log_fp.write(line)
        except Exception:
            pass

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self._log_fp.close()


def run_bk_scenario(
    name: str,
    build_dir: Path,
    cache_dir: Path | None,
    logs_dir: Path,
    wipe_cache: bool,
) -> dict:
    """One fresh-server /load_expr + first-page measurement."""
    import httpx
    from websockets.sync.client import connect as ws_connect

    if cache_dir is not None:
        if wipe_cache and cache_dir.exists():
            shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)

    srv = BuckarooServer(logs_dir / f"{name}.log")
    out: dict = {"scenario": name}
    try:
        sampler = RssSampler(srv.ps)
        out["baseline_mb"] = round(srv.ps.memory_info().rss / MB)

        payload: dict = {"build_dir": str(build_dir), "no_browser": True}
        if cache_dir is not None:
            payload["cache_storage_path"] = str(cache_dir)
        t0 = time.monotonic()
        resp = httpx.post(f"{srv.url}/load_expr", json=payload, timeout=300)
        out["load_s"] = round(time.monotonic() - t0, 2)
        load_done = sampler.now()
        resp.raise_for_status()
        body = resp.json()
        session = body["session"]
        out["rows"] = body.get("rows")
        out["after_load_mb"] = round(srv.ps.memory_info().rss / MB)

        with ws_connect(f"{srv.ws_url}/ws/{session}", max_size=200 * MB) as ws:
            t0 = time.monotonic()
            initial = ws.recv(timeout=120)
            out["init_state_s"] = round(time.monotonic() - t0, 2)
            out["init_state_kb"] = len(initial) // 1024
            req = {
                "type": "infinite_request",
                "payload_args": {"start": 0, "end": 300, "sort": None, "sort_direction": None},
            }
            t0 = time.monotonic()
            ws.send(json.dumps(req))
            for _ in range(2):
                ws.recv(timeout=120)
            out["first_page_s"] = round(time.monotonic() - t0, 2)

        time.sleep(IDLE_SETTLE_SECONDS)
        out["idle_mb"] = round(srv.ps.memory_info().rss / MB)
        sampler.stop()
        out["peak_mb"] = round(sampler.peak() / MB)
        out["peak_during_load_mb"] = round(sampler.peak(until=load_done) / MB)
    finally:
        srv.stop()

    if cache_dir is not None:
        kb, files = _dir_kb_and_files(cache_dir)
        out["cache_kb"] = kb
        out["cache_files"] = files
    return out


def run_companion_scenario(events_hash: str, entry_path: Path, logs_dir: Path) -> dict:
    """The real ensure_session path, cold stat cache, vs its hard timeout."""
    import psutil

    from tallyman_companion.buckaroo_lifecycle import BuckarooManager

    # ensure_session writes stat snapshots into the entry's own cache dir;
    # wipe it (ours by construction — built into this suite's workdir) so the
    # scenario is cold every run.
    stat_cache = entry_path / ".buckaroo_stat_cache"
    if stat_cache.exists():
        shutil.rmtree(stat_cache)

    timeout_s = float(os.environ.get("TALLYMAN_LOAD_EXPR_TIMEOUT", "10.0"))
    out: dict = {"scenario": "companion-session", "timeout_s": timeout_s}
    mgr = BuckarooManager(port=0, log_file=logs_dir / "companion_bk.log")
    try:
        t0 = time.monotonic()
        mgr.start()
        out["start_s"] = round(time.monotonic() - t0, 2)
        sampler = RssSampler(psutil.Process(mgr.proc.pid))
        t0 = time.monotonic()
        session = mgr.ensure_session(events_hash, PROJECT)
        wall = time.monotonic() - t0
        sampler.stop()
        out["ensure_session_s"] = round(wall, 2)
        out["session_created"] = session is not None
        out["headroom_s"] = round(timeout_s - wall, 2)
        out["bk_peak_mb"] = round(sampler.peak() / MB)
    finally:
        mgr.stop()
    kb, files = _dir_kb_and_files(stat_cache)
    out["cache_kb"] = kb
    out["cache_files"] = files
    return out


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------


def _hashes_path(workdir: Path) -> Path:
    return workdir / "entries.json"


def _load_hashes(workdir: Path) -> dict[str, str]:
    p = _hashes_path(workdir)
    return json.loads(p.read_text()) if p.exists() else {}


def _entry_exists(workdir: Path, content_hash: str) -> bool:
    entry = workdir / "home" / "projects" / PROJECT / "artifacts" / "catalog" / "entries" / content_hash
    return (entry / "manifest.json").exists()


def _safe(fn, *args, **kwargs):
    """Run one scenario; turn an exception into an ERROR record, keep going."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — report-only harness
        log.warning("scenario failed: %s", exc)
        return {"error": f"{type(exc).__name__}: {exc}"}


def _versions() -> dict:
    from importlib.metadata import version

    try:
        commit = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parents[1]), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
        ).stdout.strip()
    except OSError:
        commit = "unknown"
    import psutil

    return {
        "tallyman_commit": commit,
        "buckaroo": version("buckaroo"),
        "xorq": version("xorq"),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "mem_gb": round(psutil.virtual_memory().total / 2**30, 1),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR)
    ap.add_argument("--scale", type=float, default=1.0, help="row-count multiplier (use <1 for a quick smoke run)")
    ap.add_argument("--out", type=Path, default=None, help="write the markdown report here (also printed to stdout)")
    ap.add_argument("--json", type=Path, default=None, help="write raw results JSON here")
    ap.add_argument("--force", action="store_true", help="wipe the workdir before running")
    ap.add_argument("--child", choices=["build", "exec"], help=argparse.SUPPRESS)
    ap.add_argument("child_args", nargs="*", help=argparse.SUPPRESS)
    args = ap.parse_args()

    workdir: Path = args.workdir.resolve()
    os.environ["TALLYMAN_HOME"] = str(workdir / "home")
    os.environ["TALLYMAN_PROJECT"] = PROJECT

    if args.child == "build":
        child_build(args.child_args[0], workdir, args.scale)
        return 0
    if args.child == "exec":
        child_exec(args.child_args[0], args.child_args[1])
        return 0

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s", stream=sys.stderr)

    if args.force and workdir.exists():
        # Only ever wipe our own namespaced directory.
        if not workdir.name.startswith("tallyman-perf"):
            ap.error(f"--force refuses to delete {workdir}: basename must start with 'tallyman-perf'")
        shutil.rmtree(workdir)

    logs_dir = workdir / "logs"
    scratch = workdir / "scratch"
    for d in (logs_dir, scratch):
        d.mkdir(parents=True, exist_ok=True)

    from tallyman_core.paths import data_dir, ensure_project, entry_dir, project_dir

    ensure_project(PROJECT)

    t_total = time.monotonic()
    results: dict = {"versions": _versions(), "scale": args.scale}

    # -- datasets ------------------------------------------------------------
    log.info("phase: datasets")
    results["datasets"] = generate_datasets(data_dir(PROJECT), args.scale)

    # -- entry builds (fresh process each) -----------------------------------
    log.info("phase: entry builds")
    hashes = _load_hashes(workdir)
    builds = []
    for key in ENTRY_ORDER:
        cached_hash = hashes.get(key)
        if cached_hash and _entry_exists(workdir, cached_hash):
            builds.append({"key": key, "hash": cached_hash, "cached": True})
            continue
        res = _safe(run_child, "build", [key], logs_dir / f"build_{key}.stderr.log", workdir, args.scale)
        if "hash" in res:
            hashes[key] = res["hash"]
            _hashes_path(workdir).write_text(json.dumps(hashes, indent=2))
        builds.append(res)
        log.info("built %s: %s", key, res)
    results["builds"] = builds

    # -- xorq execution (fresh process each) ----------------------------------
    log.info("phase: xorq execution")
    execs = []
    for key, label in (
        ("union_wide", "xorq-union-stream"),
        ("agg_by_plate", "xorq-agg-highcard"),
        ("agg_nunique", "xorq-agg-nunique"),
    ):
        if key not in hashes:
            execs.append({"scenario": label, "error": f"no build for {key}"})
            continue
        out_path = scratch / f"exec_{key}.parquet"
        res = _safe(
            run_child, "exec", [hashes[key], str(out_path)], logs_dir / f"exec_{key}.stderr.log", workdir, args.scale
        )
        res["scenario"] = label
        execs.append(res)
        log.info("exec %s: %s", label, res)
    results["execs"] = execs

    # -- buckaroo /load_expr scenarios ----------------------------------------
    log.info("phase: buckaroo /load_expr")
    from tallyman_xorq.portable import ensure_expanded_build

    expanded: dict[str, Path] = {}
    for key in ("events_scan", "lowcard_scan", "agg_by_plate"):
        if key in hashes:
            entry = entry_dir(PROJECT, hashes[key])
            expanded[key] = ensure_expanded_build(
                entry / "xorq_build", project_dir(PROJECT), entry / ".xorq_build_expanded"
            )

    bk_specs = [
        # (name, entry key, cache dir name or None, wipe_cache)
        ("bk-highcard-cold", "events_scan", "cache_highcard", True),
        ("bk-highcard-warm", "events_scan", "cache_highcard", False),
        ("bk-highcard-nocache", "events_scan", None, False),
        ("bk-lowcard-cold", "lowcard_scan", "cache_lowcard", True),
        ("bk-agg-firstpage", "agg_by_plate", "cache_agg", True),
    ]
    bks = []
    for name, key, cache_name, wipe in bk_specs:
        if key not in expanded:
            bks.append({"scenario": name, "error": f"no build for {key}"})
            continue
        cache = scratch / cache_name if cache_name else None
        res = _safe(run_bk_scenario, name, expanded[key], cache, logs_dir, wipe)
        res["scenario"] = name
        bks.append(res)
        log.info("bk %s: %s", name, res)
    results["buckaroo"] = bks

    # -- companion ensure_session ----------------------------------------------
    log.info("phase: companion ensure_session")
    if "events_scan" in hashes:
        results["companion"] = _safe(
            run_companion_scenario, hashes["events_scan"], entry_dir(PROJECT, hashes["events_scan"]), logs_dir
        )
    else:
        results["companion"] = {"error": "no events_scan build"}
    log.info("companion: %s", results["companion"])

    results["total_s"] = round(time.monotonic() - t_total, 1)

    report = render_report(results)
    print(report)
    if args.out:
        args.out.write_text(report)
    if args.json:
        args.json.write_text(json.dumps(results, indent=2))
    return 0


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------


def _bk_row(r: dict) -> str:
    if "error" in r:
        return f"| {r['scenario']} | ERROR: {r['error']} |||||||"
    cache = f"{r['cache_kb']}KB ({r['cache_files']})" if "cache_kb" in r else "—"
    return (
        f"| {r['scenario']} | {r['load_s']} | {r['peak_during_load_mb']} | {r['after_load_mb']} "
        f"| {r['idle_mb']} | {r['init_state_kb']} | {r['first_page_s']} | {cache} |"
    )


def _derived(results: dict) -> list[str]:
    bk = {r.get("scenario"): r for r in results.get("buckaroo", [])}
    ex = {r.get("scenario"): r for r in results.get("execs", []) if "error" not in r}
    lines = []

    def ok(*names: str) -> bool:
        return all(n in bk and "error" not in bk[n] for n in names)

    if "xorq-union-stream" in ex:
        u = ex["xorq-union-stream"]
        input_mb = sum(d["mb"] for d in results.get("datasets", []) if d["dataset"].startswith("wide_"))
        ratio = u["peak_mb"] / input_mb if input_mb else float("inf")
        lines.append(
            f"- **union scan/write working set**: peak {u['peak_mb']}MB over {input_mb}MB of input parquet "
            f"({ratio:.1f}x; ~260MB of that is process baseline). Track drift — losing the streaming exec "
            f"would push this toward input+output resident at once."
        )
    if "xorq-agg-nunique" in ex and "xorq-agg-highcard" in ex:
        lines.append(
            f"- **grouped exact nunique premium**: {ex['xorq-agg-nunique']['peak_mb']}MB peak on a "
            f"~37k-group/100k-row input vs {ex['xorq-agg-highcard']['peak_mb']}MB for count/min/max over "
            f"1.5M groups/4M rows — per-group COUNT(DISTINCT) state is the memory bomb, not group count."
        )

    if ok("bk-highcard-cold", "bk-highcard-warm"):
        cold, warm = bk["bk-highcard-cold"], bk["bk-highcard-warm"]
        speedup = cold["load_s"] / warm["load_s"] if warm["load_s"] else float("inf")
        lines.append(
            f"- **warm stat cache**: cold {cold['load_s']}s / warm {warm['load_s']}s "
            f"({speedup:.1f}x); peak {cold['peak_during_load_mb']}MB -> {warm['peak_during_load_mb']}MB. "
            f"Bad: warm ~= cold means snapshots absent/unused (the live empty-cache failure, tallyman#34 fallout)."
        )
    if ok("bk-highcard-cold", "bk-highcard-nocache"):
        delta = bk["bk-highcard-cold"]["load_s"] - bk["bk-highcard-nocache"]["load_s"]
        lines.append(
            f"- **snapshot write overhead** (buckaroo#910): cold-with-cache minus no-cache = {delta:+.2f}s "
            f"({bk['bk-highcard-cold']['load_s']}s vs {bk['bk-highcard-nocache']['load_s']}s)."
        )
    if ok("bk-highcard-cold", "bk-lowcard-cold"):
        hi, lo = bk["bk-highcard-cold"], bk["bk-lowcard-cold"]
        lines.append(
            f"- **high-card stat premium** (buckaroo#911: exact nunique + top-k histogram): "
            f"peak {hi['peak_during_load_mb']}MB vs {lo['peak_during_load_mb']}MB control "
            f"({hi['peak_during_load_mb'] - lo['peak_during_load_mb']:+d}MB), "
            f"load {hi['load_s']}s vs {lo['load_s']}s."
        )
    if ok("bk-highcard-cold"):
        c = bk["bk-highcard-cold"]
        wrote = c.get("cache_files", 0) > 0
        lines.append(
            f"- **stat snapshots written on cold load**: {'yes' if wrote else 'NO — regression'} "
            f"({c.get('cache_kb', 0)}KB in {c.get('cache_files', 0)} files)."
        )
    comp = results.get("companion", {})
    if comp and "error" not in comp:
        verdict = "session created" if comp["session_created"] else "TIMED OUT -> silent pandas fallback (tallyman#34)"
        lines.append(
            f"- **companion ensure_session**: {comp['ensure_session_s']}s vs {comp['timeout_s']}s timeout "
            f"(headroom {comp['headroom_s']}s) — {verdict}."
        )
    return lines


def render_report(results: dict) -> str:
    v = results["versions"]
    L: list[str] = []
    L.append("# tallyman perf regression report")
    L.append("")
    L.append(
        f"tallyman `{v['tallyman_commit']}` · buckaroo {v['buckaroo']} · xorq {v['xorq']} · "
        f"py {v['python']} · {v['platform']} · {v['cpus']} cpus · {v['mem_gb']}GB · scale {results['scale']}"
    )
    L.append("")

    L.append("## Datasets (seeded synthetic; cached across runs)")
    L.append("")
    L.append("| dataset | rows | size MB | gen s |")
    L.append("|---|---|---|---|")
    for d in results["datasets"]:
        L.append(
            f"| {d['dataset']} | {d['rows']:,} | {d['mb']} | {d['gen_s'] if d['gen_s'] is not None else 'cached'} |"
        )
    L.append("")

    L.append("## Entry builds (build_expr -> execute -> persist, fresh process each)")
    L.append("")
    L.append("| entry | rows | wall s | execute s | peak RSS MB | result MB |")
    L.append("|---|---|---|---|---|---|")
    for b in results["builds"]:
        if b.get("cached"):
            L.append(f"| {b['key']} | cached ({b['hash'][:12]}) |||||")
        elif "error" in b:
            L.append(f"| {b.get('key', '?')} | ERROR: {b['error']} |||||")
        else:
            L.append(
                f"| {b['key']} | {b['rows']:,} | {b['wall_s']} | {b['execute_s']} | {b['peak_mb']} | {b['result_mb']} |"
            )
    L.append("")

    L.append("## xorq execution (fresh process; load_entry -> to_parquet)")
    L.append("")
    L.append(
        "Good: union peak RSS tracks a bounded working set, not input+output; "
        "count/min/max agg peak tracks group cardinality; the pinned nunique row is the known memory bomb."
    )
    L.append("")
    L.append("| scenario | load s | exec s | peak RSS MB | rows out | output MB |")
    L.append("|---|---|---|---|---|---|")
    for e in results["execs"]:
        if "error" in e:
            L.append(f"| {e['scenario']} | ERROR: {e['error']} |||||")
        else:
            L.append(
                f"| {e['scenario']} | {e['load_s']} | {e['exec_s']} | {e['peak_mb']} | {e['rows']:,} | {e['out_mb']} |"
            )
    L.append("")

    L.append("## buckaroo /load_expr (fresh server per scenario)")
    L.append("")
    L.append("| scenario | load s | peak@load MB | after MB | idle MB | init KB | 1st page s | stat cache |")
    L.append("|---|---|---|---|---|---|---|---|")
    for r in results["buckaroo"]:
        L.append(_bk_row(r))
    L.append("")

    comp = results.get("companion", {})
    L.append("## companion ensure_session (cold cache, real BuckarooManager path)")
    L.append("")
    if "error" in comp:
        L.append(f"ERROR: {comp['error']}")
    else:
        L.append(
            f"buckaroo start {comp['start_s']}s · ensure_session {comp['ensure_session_s']}s · "
            f"session created: {comp['session_created']} · timeout {comp['timeout_s']}s "
            f"(headroom {comp['headroom_s']}s) · bk peak {comp['bk_peak_mb']}MB · "
            f"stat cache {comp['cache_kb']}KB ({comp['cache_files']} files)"
        )
    L.append("")

    L.append("## Derived signals")
    L.append("")
    L.extend(_derived(results))
    L.append("")

    over = results["total_s"] > BUDGET_SECONDS
    L.append(f"Total wall: **{results['total_s']}s** (budget {BUDGET_SECONDS:.0f}s{' — OVER BUDGET' if over else ''})")
    L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    sys.exit(main())
