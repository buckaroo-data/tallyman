"""``tallyman_core.spawn.run``: the state a child started with ``os.posix_spawn`` begins in, and who reaps it.

``os.posix_spawn`` hands the child whatever the parent has unless told otherwise. ``subprocess`` resets the signals
CPython ignores and kills its child when waiting is interrupted; ``run`` has to do both itself. It also gives the child
no stdin: in the MCP server, the parent's stdin is the JSON-RPC pipe from Claude Code.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys

import pytest

from tallyman_core import spawn


@pytest.mark.parametrize("sig", [signal.SIGPIPE, signal.SIGXFSZ], ids=lambda s: s.name)
def test_run_starts_the_child_with_default_signal_dispositions(sig):
    """CPython ignores SIGPIPE and SIGXFSZ; the child gets them back at their defaults, as ``subprocess`` gives them.

    Under an inherited SIG_IGN the producer in a pipeline (``yes | head`` in a git hook) gets EPIPE instead of dying.
    A shell that signals itself dies only when the disposition is the default; an ignored one cannot be reset.
    """
    rc, _, _ = spawn.run(["/bin/sh", "-c", f"ulimit -c 0; kill -{sig.name.removeprefix('SIG')} $$; exit 0"])
    assert rc == -sig


def test_run_gives_the_child_no_stdin():
    """The child reads /dev/null, never the parent's stdin, so a hook that reads stdin cannot eat an MCP request."""
    script = "from tallyman_core import spawn\nprint(repr(spawn.run(['cat'])[1]))\n"
    proc = subprocess.run(
        [sys.executable, "-c", script], input="the next MCP request\n", capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    assert proc.stdout.strip() == "''", f"the child read the parent's stdin: {proc.stdout.strip()}"


def test_run_kills_and_reaps_the_child_when_waiting_is_interrupted(monkeypatch):
    """Ctrl-C while a child runs leaves no child behind: git would keep ``index.lock``, cp would keep writing."""
    pids: list[int] = []
    real = os.posix_spawn

    def recording(*args, **kwargs):
        pids.append(real(*args, **kwargs))
        return pids[-1]

    def interrupted(pid, timeout):
        raise KeyboardInterrupt

    monkeypatch.setattr(os, "posix_spawn", recording)
    monkeypatch.setattr(spawn, "_wait", interrupted)
    try:
        with pytest.raises(KeyboardInterrupt):
            spawn.run(["sleep", "30"])
        with pytest.raises(ChildProcessError):  # killed and reaped: it is no longer a child of this process
            os.waitpid(pids[0], os.WNOHANG)
    finally:
        with contextlib.suppress(ChildProcessError):
            if os.waitpid(pids[0], os.WNOHANG)[0] == 0:
                os.kill(pids[0], signal.SIGKILL)
                os.waitpid(pids[0], 0)
