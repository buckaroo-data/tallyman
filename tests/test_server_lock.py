"""One tallyman server per data directory (#183).

A tallyman server (``tallyman run``) holds state in its own process that no other process sees: the SSE subscribers,
the Buckaroo subprocess and its sessions, the loaded-build caches. Two servers on one data directory (``TALLYMAN_HOME``)
each serve a view the other's writes never reach, so the second one has to be refused. A server on another data
directory must still start, because that is how a second tallyman runs for testing and dev.

The claim on a data directory is held by a real process here, never by a thread: an exclusive ``flock`` belongs to an
open file description, and what matters is what another process sees, including after that process is SIGKILLed. Every
test runs on a tmp ``TALLYMAN_HOME`` (``isolated_home``) and binds only ephemeral ports of its own.
"""

from __future__ import annotations

import os
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

# A process that claims the data dir named by its TALLYMAN_HOME, says so on stdout and holds the claim until its stdin
# closes or it is killed. With "spawn" it first starts a child that outlives it, the way a Buckaroo subprocess could.
_HOLDER = """
import subprocess, sys
from tallyman_core.server_lock import claim_data_dir

claim_data_dir(port=int(sys.argv[1]), bind_host=sys.argv[2])
child = None
if sys.argv[3] == "spawn":
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], close_fds=False)
print("CLAIMED", child.pid if child else "", flush=True)
sys.stdin.read()
"""


class _Holder:
    def __init__(self, proc: subprocess.Popen, child_pid: int | None):
        self.proc = proc
        self.pid = proc.pid
        self.child_pid = child_pid

    def sigkill(self) -> None:
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)


@pytest.fixture
def hold_data_dir():
    """Start a process that claims a data dir, and kill it (and any child it spawned) at teardown."""
    holders: list[_Holder] = []

    def _hold(home: Path, *, port: int, bind_host: str = "127.0.0.1", spawn_child: bool = False) -> _Holder:
        env = {**os.environ, "TALLYMAN_HOME": str(home)}
        env.pop("TALLYMAN_COMPANION_URL", None)
        proc = subprocess.Popen(
            [sys.executable, "-c", _HOLDER, str(port), bind_host, "spawn" if spawn_child else "-"],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        ready, _, _ = select.select([proc.stdout], [], [], 60)
        line = proc.stdout.readline() if ready else ""
        if not line.startswith("CLAIMED"):
            proc.kill()
            _, err = proc.communicate(timeout=10)
            last = err.strip().splitlines()[-1] if err.strip() else "no output"
            pytest.fail(f"the holder process could not claim {home}: {last}")
        fields = line.split()
        holder = _Holder(proc, int(fields[1]) if len(fields) > 1 else None)
        holders.append(holder)
        return holder

    yield _hold

    for holder in holders:
        if holder.proc.poll() is None:
            holder.proc.kill()
        holder.proc.wait(timeout=10)
        for stream in (holder.proc.stdin, holder.proc.stdout, holder.proc.stderr):
            if stream is not None:
                stream.close()
        if holder.child_pid is not None:
            try:
                os.kill(holder.child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.fixture
def no_companion_url_env(monkeypatch):
    monkeypatch.delenv("TALLYMAN_COMPANION_URL", raising=False)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


# ---------------------------------------------------------------------------
# the claim
# ---------------------------------------------------------------------------


def test_a_second_claim_on_a_held_data_dir_is_refused_and_names_the_holder(isolated_home, hold_data_dir):
    """A data dir another process has claimed is refused, and the error names that process: its pid and the port it
    serves on, the data dir, and how to run a second tallyman instead."""
    from tallyman_core.server_lock import DataDirInUse, claim_data_dir

    holder = hold_data_dir(isolated_home, port=17861)

    with pytest.raises(DataDirInUse) as refused:
        claim_data_dir(port=17862, bind_host="127.0.0.1")

    message = str(refused.value)
    assert f"pid {holder.pid}" in message
    assert "17861" in message
    assert str(isolated_home.resolve()) in message
    assert "TALLYMAN_HOME" in message and "--port" in message


def test_a_claim_refused_over_an_unreadable_owner_record_still_says_why(isolated_home, hold_data_dir):
    """The owner record can be read half-written (a claim truncates it and then writes it). The refusal is still clear
    about what holds the data dir, without a pid it does not have."""
    from tallyman_core.server_lock import DataDirInUse, claim_data_dir

    holder = hold_data_dir(isolated_home, port=17863)
    (isolated_home / "server.lock").write_text('{"pid": ')  # flock is advisory: the holder keeps its lock

    with pytest.raises(DataDirInUse) as refused:
        claim_data_dir(port=17864, bind_host="127.0.0.1")

    message = str(refused.value)
    assert f"pid {holder.pid}" not in message
    assert str(isolated_home.resolve()) in message
    assert "in use" in message
    assert "TALLYMAN_HOME" in message and "--port" in message


def test_claims_on_two_different_data_dirs_both_hold(tmp_path, monkeypatch, hold_data_dir):
    """A second tallyman on another data dir is the supported way to run two (for testing and dev)."""
    from tallyman_core.server_lock import claim_data_dir, read_owner, release_data_dir

    home_a, home_b = tmp_path / "a", tmp_path / "b"
    holder = hold_data_dir(home_a, port=17865)
    monkeypatch.setenv("TALLYMAN_HOME", str(home_b))

    record = claim_data_dir(port=17866, bind_host="127.0.0.1")
    try:
        assert record["pid"] == os.getpid()
        assert read_owner(home_a)["pid"] == holder.pid
        assert read_owner(home_b)["pid"] == os.getpid()
        assert read_owner(home_b)["port"] == 17866
    finally:
        release_data_dir(home_b)


def test_the_claim_of_a_sigkilled_server_is_gone_and_its_record_is_not_believed(isolated_home, hold_data_dir):
    """The kernel drops a flock when its process dies, SIGKILL included, so a server that was killed leaves nothing to
    clean up. Its owner record is still in the file, and read_owner must not report it as the owner."""
    from tallyman_core.server_lock import claim_data_dir, read_owner, release_data_dir

    holder = hold_data_dir(isolated_home, port=17867)
    assert read_owner()["pid"] == holder.pid

    holder.sigkill()
    assert str(holder.pid) in (isolated_home / "server.lock").read_text()  # the stale record is still on disk
    assert read_owner() is None

    record = claim_data_dir(port=17868, bind_host="127.0.0.1")
    try:
        assert record["pid"] == os.getpid()
    finally:
        release_data_dir()
    assert read_owner() is None


def test_the_claim_descriptor_is_not_inherited(isolated_home, hold_data_dir):
    """A child of the server (Buckaroo) must not inherit the claim, or a child that outlives a killed server would
    keep the data dir claimed. The holder spawns its child with close_fds=False, so only non-inheritance protects it."""
    from tallyman_core.server_lock import claim_data_dir, claim_fd, release_data_dir

    holder = hold_data_dir(isolated_home, port=17869, spawn_child=True)
    holder.sigkill()
    assert _alive(holder.child_pid)

    claim_data_dir(port=17870, bind_host="127.0.0.1")
    try:
        fd = claim_fd()
        assert fd is not None
        assert os.get_inheritable(fd) is False
    finally:
        release_data_dir()


# ---------------------------------------------------------------------------
# tallyman run
# ---------------------------------------------------------------------------


class _Calls:
    """Stands in for BuckarooManager and uvicorn.run, and records whether either was touched."""

    def __init__(self):
        self.buckaroo: list[str] = []
        self.uvicorn: list[tuple] = []

    def buckaroo_manager(self):
        calls = self

        class FakeBuckaroo:
            base_url = "http://127.0.0.1:1"
            proc = None

            def __init__(self, **kwargs):
                calls.buckaroo.append("init")

            def start(self):
                calls.buckaroo.append("start")

            def stop(self):
                calls.buckaroo.append("stop")

        return FakeBuckaroo


@pytest.fixture
def run_calls(monkeypatch) -> _Calls:
    import uvicorn

    import tallyman_companion
    import tallyman_companion.buckaroo_lifecycle as lifecycle

    calls = _Calls()
    monkeypatch.setattr(lifecycle, "BuckarooManager", calls.buckaroo_manager())
    monkeypatch.setattr(uvicorn, "run", lambda *a, **k: calls.uvicorn.append((a, k)))
    monkeypatch.setattr(tallyman_companion, "create_app", lambda *a, **k: object())
    return calls


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_run_refuses_a_data_dir_another_server_holds(project, isolated_home, hold_data_dir, run_calls):
    """`tallyman run` on a data dir another server holds exits non-zero with the refusal, before it starts Buckaroo
    or uvicorn, so nothing is left running."""
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    holder = hold_data_dir(isolated_home, port=17871)

    result = CliRunner().invoke(cli, ["run", "--project", project, "--port", str(_free_port())])

    assert result.exit_code != 0, result.output
    assert f"pid {holder.pid}" in result.output
    assert "17871" in result.output
    assert "TALLYMAN_HOME" in result.output and "--port" in result.output
    assert run_calls.buckaroo == []
    assert run_calls.uvicorn == []


def test_run_refuses_a_port_already_in_use_before_starting_anything(project, isolated_home, run_calls):
    """A second tallyman on its own data dir that forgot --port fails with a clear error, before Buckaroo starts, and
    gives the data dir back."""
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    with socket.socket() as busy:
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        port = busy.getsockname()[1]

        result = CliRunner().invoke(cli, ["run", "--project", project, "--host", "127.0.0.1", "--port", str(port)])

    assert result.exit_code != 0, result.output
    assert f"port {port}" in result.output
    assert "in use" in result.output
    assert "--port" in result.output
    assert run_calls.buckaroo == []
    assert run_calls.uvicorn == []

    from tallyman_core.server_lock import read_owner

    assert read_owner() is None  # the refused run gave the data dir back


# ---------------------------------------------------------------------------
# clients find the companion of their own data dir
# ---------------------------------------------------------------------------


def test_companion_url_is_the_port_the_owner_of_this_data_dir_serves_on(
    tmp_path, monkeypatch, hold_data_dir, no_companion_url_env
):
    """With two tallymans running, a client of data dir B must reach B's companion, not the one on 7860."""
    from tallyman_core.server_lock import companion_url

    home_a, home_b, home_c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    hold_data_dir(home_a, port=17872, bind_host="0.0.0.0")
    hold_data_dir(home_b, port=17873, bind_host="127.0.0.1")

    monkeypatch.setenv("TALLYMAN_HOME", str(home_a))
    assert companion_url() == "http://127.0.0.1:17872"  # a wildcard bind is reached on the loopback
    monkeypatch.setenv("TALLYMAN_HOME", str(home_b))
    assert companion_url() == "http://127.0.0.1:17873"
    monkeypatch.setenv("TALLYMAN_HOME", str(home_c))
    assert companion_url() == "http://127.0.0.1:7860"  # nothing serves c: the default

    monkeypatch.setenv("TALLYMAN_HOME", str(home_b))
    monkeypatch.setenv("TALLYMAN_COMPANION_URL", "http://127.0.0.1:19999")
    assert companion_url() == "http://127.0.0.1:19999"  # an explicit URL wins


def test_mcp_notifies_and_links_the_companion_of_its_own_data_dir(
    project, isolated_home, monkeypatch, hold_data_dir, no_companion_url_env
):
    """The MCP server read TALLYMAN_COMPANION_URL once at import, defaulting to 7860. It has to resolve the companion
    per call, and name its data dir in the notify so a companion of another data dir can refuse it."""
    import tallyman_mcp.server as srv

    hold_data_dir(isolated_home, port=17874)
    posted: list[tuple[str, dict]] = []

    class _Capture:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def post(self, url, json=None):
            posted.append((url, json))

    monkeypatch.setattr(srv.httpx, "Client", _Capture)

    srv._notify("new_entry", content_hash="abc")

    assert posted == [
        (
            "http://127.0.0.1:17874/internal/notify",
            {"kind": "new_entry", "hash": "abc", "extra": None, "home": str(isolated_home.resolve())},
        )
    ]
    assert srv._entry_url(project, "abc") == f"http://127.0.0.1:17874/{project}/catalog/abc"


def test_cli_reset_notifies_the_companion_of_its_own_data_dir(
    isolated_home, monkeypatch, hold_data_dir, no_companion_url_env
):
    import httpx
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    hold_data_dir(isolated_home, port=17875)
    sent: dict = {}
    monkeypatch.setattr(httpx, "post", lambda url, json=None, timeout=None: sent.update(url=url, json=json))

    runner = CliRunner()
    assert runner.invoke(cli, ["init", "beta", "--no-fixture"]).exit_code == 0
    result = runner.invoke(cli, ["reset-to", "0", "--project", "beta"])

    assert result.exit_code == 0, result.output
    assert sent["url"] == "http://127.0.0.1:17875/internal/notify"
    assert sent["json"]["home"] == str(isolated_home.resolve())


# ---------------------------------------------------------------------------
# the companion refuses a notify from another data dir
# ---------------------------------------------------------------------------


def test_notify_from_another_data_dir_is_refused(project, isolated_home, tmp_path):
    from fastapi.testclient import TestClient

    from tallyman_companion import create_app

    other = tmp_path / "another-data-dir"
    c = TestClient(create_app(project))

    refused = c.post("/internal/notify", json={"kind": "new_entry", "hash": "abc", "home": str(other)})
    assert refused.status_code == 409, refused.text
    detail = refused.json()["detail"]
    assert str(other.resolve()) in detail
    assert str(isolated_home.resolve()) in detail

    assert c.post("/internal/notify", json={"kind": "new_entry", "home": str(isolated_home)}).status_code == 200
    assert c.post("/internal/notify", json={"kind": "new_entry"}).status_code == 200  # no home: still accepted


def _next_event(lines) -> str:
    """The name of the next SSE event on the stream that is not a keep-alive ping."""
    for line in lines:
        if line.startswith("event:"):
            name = line.split(":", 1)[1].strip()
            if name != "ping":
                return name
    raise AssertionError("the SSE stream ended")


def test_a_refused_notify_publishes_no_sse_event(project, isolated_home, tmp_path):
    """Observed on a real SSE stream: a notify from another data dir must not reach the browsers of this one. Events
    queue in order, so if the refused notify had been published it would arrive before the accepted one."""
    import httpx
    import uvicorn

    from tallyman_companion import create_app

    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    base = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(create_app(project), log_level="warning"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while not server.started:
            assert time.monotonic() < deadline, "the companion did not start"
            time.sleep(0.02)

        with httpx.Client(timeout=30.0) as client, client.stream("GET", f"{base}/{project}/api/sse") as stream:
            lines = stream.iter_lines()
            assert _next_event(lines) == "hello"  # subscribed before anything is posted
            refused = client.post(
                f"{base}/internal/notify", json={"kind": "from_another_data_dir", "home": str(tmp_path / "other")}
            )
            accepted = client.post(
                f"{base}/internal/notify", json={"kind": "from_this_data_dir", "home": str(isolated_home)}
            )
            first = _next_event(lines)

        assert (refused.status_code, accepted.status_code, first) == (409, 200, "from_this_data_dir")
    finally:
        server.should_exit = True
        thread.join(timeout=15)
        sock.close()
