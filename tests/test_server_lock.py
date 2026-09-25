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
from urllib.parse import urlsplit

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


@pytest.fixture(autouse=True)
def _reset_mcp_server_state():
    """Each test starts and ends with clean module-level MCP server state: a project tool that succeeds (against a
    stubbed companion) makes its project the MCP's sticky in-process one, which every later tool call would use."""
    from tallyman_mcp import server as srv

    srv._last_project = None
    srv._mcp_active_project = None
    yield
    srv._last_project = None
    srv._mcp_active_project = None


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
    from tallyman_core.server_lock import claim_data_dir, release_data_dir

    holder = hold_data_dir(isolated_home, port=17869, spawn_child=True)
    holder.sigkill()
    assert _alive(holder.child_pid)

    claim_data_dir(port=17870, bind_host="127.0.0.1")  # the child is alive and does not hold the claim
    release_data_dir()


# ---------------------------------------------------------------------------
# tallyman run
# ---------------------------------------------------------------------------


class _Calls:
    """Stands in for BuckarooManager and uvicorn.run, and records whether either was touched, and how Buckaroo was
    configured."""

    def __init__(self):
        self.buckaroo: list[str] = []
        self.buckaroo_kwargs: list[dict] = []
        self.uvicorn: list[tuple] = []

    def buckaroo_manager(self):
        calls = self

        class FakeBuckaroo:
            base_url = "http://127.0.0.1:1"
            proc = None

            def __init__(self, **kwargs):
                calls.buckaroo.append("init")
                calls.buckaroo_kwargs.append(kwargs)

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

    class FakeServer:
        """uvicorn.Server, which serves a socket tallyman bound itself."""

        def __init__(self, config):
            self.config = config

        def run(self, sockets=None):
            calls.uvicorn.append(((self.config.app,), {"sockets": sockets}))

    calls = _Calls()
    monkeypatch.setattr(lifecycle, "BuckarooManager", calls.buckaroo_manager())
    monkeypatch.setattr(uvicorn, "run", lambda *a, **k: calls.uvicorn.append((a, k)))
    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    monkeypatch.setattr(tallyman_companion, "create_app", lambda *a, **k: object())
    return calls


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _has_ipv6_loopback() -> bool:
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.bind(("::1", 0))
    except OSError:
        return False
    return True


_needs_ipv6_loopback = pytest.mark.skipif(not _has_ipv6_loopback(), reason="this machine has no IPv6 loopback")


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


def test_run_with_no_active_project_says_to_pass_one(isolated_home, monkeypatch, run_calls):
    """A fresh data dir has no active project: `tallyman init` makes a project without making it active. That is where
    a second tallyman starts, and `tallyman run` there has to say to pass --project and name the projects it could
    pass, not fail with a TypeError."""
    from click.testing import CliRunner

    from tallyman_cli.main import cli
    from tallyman_core import ensure_project
    from tallyman_core.paths import resolve_project
    from tallyman_core.server_lock import read_owner

    monkeypatch.delenv("TALLYMAN_PROJECT", raising=False)
    ensure_project("demo")
    assert resolve_project() is None

    result = CliRunner().invoke(cli, ["run", "--port", str(_free_port())])

    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.exit_code != 0
    assert "no active project" in result.output
    assert "--project" in result.output and "demo" in result.output
    assert run_calls.buckaroo == []
    assert run_calls.uvicorn == []
    assert read_owner() is None  # the refused run gave the data dir back


@_needs_ipv6_loopback
def test_run_on_the_ipv6_wildcard_gives_buckaroo_a_companion_url_it_can_reach(
    project, isolated_home, monkeypatch, run_calls
):
    """With --host ::, Buckaroo posts its telemetry to the companion on 127.0.0.1, which a server on :: serves too:
    the wildcard is no address to connect to, and `http://:::<port>` is no URL."""
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    monkeypatch.setenv("TALLYMAN_PROJECT", project)  # `run` sets it in os.environ; this puts it back at teardown
    port = _free_port()

    result = CliRunner().invoke(cli, ["run", "--project", project, "--host", "::", "--port", str(port)])

    assert result.exit_code == 0, result.output
    assert [kw["companion_base_url"] for kw in run_calls.buckaroo_kwargs] == [f"http://127.0.0.1:{port}"]


@_needs_ipv6_loopback
def test_run_on_the_ipv6_wildcard_serves_ipv4_too(project, isolated_home):
    """A server on --host :: serves IPv6 and IPv4 both, and every local URL tallyman gives for it is 127.0.0.1: local
    work defaults to IPv4. uvicorn alone binds :: through asyncio's create_server, which sets IPV6_V6ONLY, and then
    127.0.0.1 is refused."""
    from tallyman_core.server_lock import companion_url

    env = {**os.environ, "TALLYMAN_HOME": str(isolated_home)}
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "tallyman_cli.main", "run", "--project", project, "--host", "::", "--port", str(port)]
        + ["--no-buckaroo"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60
        while True:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
                if probe.connect_ex(("::1", port)) == 0:
                    break
            assert proc.poll() is None, proc.stdout.read()
            assert time.monotonic() < deadline, "tallyman run did not start listening on ::1"
            time.sleep(0.1)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ipv4:
            assert ipv4.connect_ex(("127.0.0.1", port)) == 0, "a server on :: refuses 127.0.0.1"
        assert companion_url() == f"http://127.0.0.1:{port}"

        proc.send_signal(signal.SIGTERM)
        output, _ = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate(timeout=10)

    assert f"http://127.0.0.1:{port}" in output, output  # the URL `run` prints to open


# ---------------------------------------------------------------------------
# clients find the companion of their own data dir
# ---------------------------------------------------------------------------


def test_companion_url_is_the_port_the_owner_of_this_data_dir_serves_on(tmp_path, monkeypatch, hold_data_dir):
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
    assert companion_url() is None  # nothing serves c, so there is no companion: 7860 would be some other data dir's

    # The owner record is the only source: a URL in the environment can outlive the server it named, and then points
    # this data dir's clients at another data dir's companion.
    monkeypatch.setenv("TALLYMAN_COMPANION_URL", "http://127.0.0.1:19999")
    monkeypatch.setenv("TALLYMAN_HOME", str(home_b))
    assert companion_url() == "http://127.0.0.1:17873"
    monkeypatch.setenv("TALLYMAN_HOME", str(home_c))
    assert companion_url() is None


@_needs_ipv6_loopback
def test_companion_url_reaches_a_server_on_the_ipv6_wildcard(isolated_home, hold_data_dir):
    """uvicorn binds through asyncio's create_server, which sets IPV6_V6ONLY on an IPv6 socket, so a server on `::`
    listens on ::1 and not on 127.0.0.1. Its clients have to be sent to ::1."""
    from tallyman_core.server_lock import companion_url

    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        server.bind(("::", 0))
        server.listen()
        port = server.getsockname()[1]
        hold_data_dir(isolated_home, port=port, bind_host="::")

        url = urlsplit(companion_url())
        assert url.port == port
        with socket.create_connection((url.hostname, url.port), timeout=5):
            pass


def test_mcp_notifies_and_links_the_companion_of_its_own_data_dir(project, isolated_home, monkeypatch, hold_data_dir):
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


def test_cli_reset_notifies_the_companion_of_its_own_data_dir(isolated_home, monkeypatch, hold_data_dir):
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


# ---------------------------------------------------------------------------
# no server on this data dir: no companion to reach
# ---------------------------------------------------------------------------


class _CapturePosts:
    """Stands in for httpx.Client in tallyman_mcp.server and records every POST."""

    def __init__(self, posted: list):
        self.posted = posted

    def __call__(self, *a, **k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def post(self, url, json=None):
        import httpx

        self.posted.append((url, json))
        return httpx.Response(200, json={"previous": None, "active": (json or {}).get("name")})


def test_mcp_with_no_server_on_its_data_dir_posts_nothing_and_links_nothing(project, isolated_home, monkeypatch):
    """With no server holding this data dir, whatever listens on 7860 (or on a URL left in the environment) serves some
    other data dir. The MCP must not send it notifies or project changes, nor hand out links into it."""
    import tallyman_mcp.server as srv

    monkeypatch.setenv("TALLYMAN_COMPANION_URL", "http://127.0.0.1:19999")  # stale: names no server of this data dir
    posted: list = []
    monkeypatch.setattr(srv.httpx, "Client", _CapturePosts(posted))

    srv._notify("new_entry", content_hash="abc")
    assert srv._entry_url(project, "abc") is None
    out = srv.project_switch("beta")

    assert posted == []
    assert "active" not in out
    assert "no tallyman server" in out["error"] and str(isolated_home.resolve()) in out["error"]
    assert srv._mcp_active_project != "beta"  # the MCP did not switch on its own either


def test_cli_reset_with_no_server_on_its_data_dir_posts_nothing(isolated_home, monkeypatch):
    import httpx
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    monkeypatch.setenv("TALLYMAN_COMPANION_URL", "http://127.0.0.1:19999")
    sent: list = []
    monkeypatch.setattr(httpx, "post", lambda url, json=None, timeout=None: sent.append(url))

    runner = CliRunner()
    assert runner.invoke(cli, ["init", "beta", "--no-fixture"]).exit_code == 0
    result = runner.invoke(cli, ["reset-to", "0", "--project", "beta"])

    assert result.exit_code == 0, result.output
    assert sent == []


# ---------------------------------------------------------------------------
# project changes from another data dir are refused
# ---------------------------------------------------------------------------


def test_mcp_project_changes_name_their_data_dir(project, isolated_home, monkeypatch, hold_data_dir):
    import tallyman_mcp.server as srv

    hold_data_dir(isolated_home, port=17876)
    posted: list = []
    monkeypatch.setattr(srv.httpx, "Client", _CapturePosts(posted))

    srv.project_switch("beta")
    srv.project_new("gamma")

    assert [url for url, _ in posted] == [
        "http://127.0.0.1:17876/api/projects/switch",
        "http://127.0.0.1:17876/api/projects/new",
    ]
    assert all(body["home"] == str(isolated_home.resolve()) for _, body in posted)


def test_project_switch_and_new_from_another_data_dir_are_refused(project, isolated_home, tmp_path):
    """A client of another data dir that reached this companion must not create projects here or switch this data
    dir's active project (which reloads every browser tab of this data dir)."""
    from fastapi.testclient import TestClient

    from tallyman_companion import create_app
    from tallyman_core import ensure_project
    from tallyman_core.paths import projects_root

    ensure_project("beta")
    other = str(tmp_path / "another-data-dir")
    c = TestClient(create_app(project))

    switched = c.post("/api/projects/switch", json={"name": "beta", "home": other})
    created = c.post("/api/projects/new", json={"name": "gamma", "home": other})

    assert (switched.status_code, created.status_code) == (409, 409), (switched.text, created.text)
    assert c.get("/api/projects").json()["active"] == project
    assert not (projects_root() / "gamma").exists()

    assert c.post("/api/projects/switch", json={"name": "beta", "home": str(isolated_home)}).status_code == 200


def test_notify_from_this_data_dir_spelled_in_another_case_is_accepted(project, isolated_home):
    """On a case-insensitive filesystem (macOS APFS by default) one directory has many spellings, and the claim treats
    them as one data dir, so the home check must too. Path.resolve() keeps the spelling it was given."""
    from fastapi.testclient import TestClient

    from tallyman_companion import create_app

    respelled = isolated_home.parent / isolated_home.name.swapcase()
    if not respelled.exists() or not os.path.samefile(respelled, isolated_home):
        pytest.skip("case-sensitive filesystem: another spelling is another directory")
    c = TestClient(create_app(project))

    accepted = c.post("/internal/notify", json={"kind": "new_entry", "home": str(respelled)})
    assert accepted.status_code == 200, accepted.text
    switched = c.post("/api/projects/switch", json={"name": project, "home": str(respelled)})
    assert switched.status_code == 200, switched.text


# ---------------------------------------------------------------------------
# the port check sees a listener on an overlapping address
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("listen_on", "probe"), [("0.0.0.0", "127.0.0.1"), ("127.0.0.1", "0.0.0.0")])
def test_port_in_use_sees_a_listener_on_an_overlapping_address(listen_on, probe):
    """A listener on the wildcard address takes the port on the loopback too, and the other way round. On macOS a bind
    with SO_REUSEADDR succeeds beside it anyway, so a bind alone reports the port free."""
    from tallyman_companion.buckaroo_lifecycle import port_in_use

    with socket.socket() as busy:
        busy.bind((listen_on, 0))
        busy.listen()
        port = busy.getsockname()[1]
        assert port_in_use(probe, port) is True
    assert port_in_use(probe, port) is False  # nothing listens once it is closed


def test_run_refuses_a_port_a_wildcard_listener_holds(project, isolated_home, run_calls):
    """A second tallyman that forgot --port, next to one serving on 0.0.0.0:<port>, would bind the loopback beside it
    and take over the first one's clients (they reach a wildcard bind on the loopback)."""
    from click.testing import CliRunner

    from tallyman_cli.main import cli

    with socket.socket() as busy:
        busy.bind(("0.0.0.0", 0))
        busy.listen()
        port = busy.getsockname()[1]

        result = CliRunner().invoke(cli, ["run", "--project", project, "--host", "127.0.0.1", "--port", str(port)])

    assert result.exit_code != 0, result.output
    assert f"port {port}" in result.output and "in use" in result.output
    assert run_calls.uvicorn == []


# ---------------------------------------------------------------------------
# SIGTERM shuts tallyman run down through its cleanup
# ---------------------------------------------------------------------------


def test_sigterm_runs_the_cleanup_of_tallyman_run(project, isolated_home):
    """uvicorn stops on SIGTERM and then re-raises it with the default handler, which kills the process before the
    `finally` of `tallyman run` releases the claim and stops Buckaroo. The restart script stops the server this way."""
    env = {**os.environ, "TALLYMAN_HOME": str(isolated_home)}
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "tallyman_cli.main", "run", "--project", project, "--port", str(port), "--no-buckaroo"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60
        while True:
            with socket.socket() as probe:
                if probe.connect_ex(("127.0.0.1", port)) == 0:
                    break
            assert proc.poll() is None, proc.stdout.read()
            assert time.monotonic() < deadline, "tallyman run did not start listening"
            time.sleep(0.1)

        proc.send_signal(signal.SIGTERM)
        output, _ = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate(timeout=10)

    assert "tallyman run · stopped" in output, output
