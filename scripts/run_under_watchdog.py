"""Run a command under an RSS watchdog: SIGKILL the whole process tree the
instant its combined resident memory crosses a cap.

macOS ``ulimit -v`` caps address space, not RSS, so it does not stop a process
that balloons resident memory — the box just swaps to death and locks. This
wrapper polls the process tree's RSS with psutil and kills it ourselves before
that happens. It is the guard used to run the memory-heavy perf surfaces safely:

    uv run python scripts/run_under_watchdog.py 20 \\
        uv run pytest -m perf tests/test_perf_integration.py -s
    uv run python scripts/run_under_watchdog.py 16 \\
        uv run python scripts/rebuild_parking_catalog.py --rows 1000000

Child stdout/stderr are inherited (streamed straight through). A heartbeat line
goes to stderr every ~15s so a long quiet stretch still shows live RSS. Exits
137 if the cap was breached (the tree was killed), else the child's exit code.

This is a backstop for *unknown* spikes (e.g. a full-source ``pq.read_table`` of
a 100M-row parquet). The known reconstruction cycle has its own in-process
backstop — ``result_cache._MAX_RECON_DEPTH`` — which turns a runaway
``from_catalog`` cycle into a fast ``BuildError`` instead of an OOM.
"""

from __future__ import annotations

import subprocess
import sys
import time

import psutil


def tree_rss(proc: psutil.Process) -> int:
    """Combined RSS of *proc* and every descendant, in bytes."""
    total = 0
    try:
        procs = [proc, *proc.children(recursive=True)]
    except psutil.Error:
        return 0
    for p in procs:
        try:
            total += p.memory_info().rss
        except psutil.Error:
            pass
    return total


def main() -> None:
    if len(sys.argv) < 3:
        sys.exit("usage: run_under_watchdog.py <cap_gb> <cmd> [args...]")
    cap_gb = float(sys.argv[1])
    cmd = sys.argv[2:]
    cap = cap_gb * 1024**3

    t0 = time.monotonic()
    proc = subprocess.Popen(cmd)
    mon = psutil.Process(proc.pid)
    peak = 0
    last_log = 0.0
    killed = False

    while proc.poll() is None:
        rss = tree_rss(mon)
        peak = max(peak, rss)
        now = time.monotonic()
        if now - last_log > 15:
            sys.stderr.write(f"[watchdog t={now - t0:.0f}s] tree RSS={rss / 1024**3:.2f}GB peak={peak / 1024**3:.2f}GB\n")
            sys.stderr.flush()
            last_log = now
        if rss > cap:
            killed = True
            sys.stderr.write(f"\n!! WATCHDOG KILL: tree RSS {rss / 1024**3:.1f}GB > cap {cap_gb}GB at t={now - t0:.0f}s\n")
            sys.stderr.flush()
            for p in [mon, *mon.children(recursive=True)]:
                try:
                    p.kill()
                except psutil.Error:
                    pass
            break
        time.sleep(0.25)

    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
    status = "KILLED" if killed else f"exit {proc.returncode}"
    sys.stderr.write(f"\n=== watchdog: {status} peak={peak / 1024**3:.2f}GB elapsed={time.monotonic() - t0:.0f}s cap={cap_gb}GB ===\n")
    sys.stderr.flush()
    sys.exit(137 if killed else (proc.returncode or 0))


if __name__ == "__main__":
    main()
