"""Tallyman starts every child process without ``fork`` (#305).

On macOS a ``fork_exec`` child (any ``subprocess`` call CPython does not route to ``posix_spawn``) dies with SIGSEGV
before ``exec`` once the parent first imported pyproj on a thread that has since exited. ``import pyproj`` opens PROJ's
``proj.db`` and registers a ``pthread_atfork`` child handler that closes it; Apple's libsqlite3 logs through
``os_log`` on that close, and ``os_log`` faults in the child. xorq's first pandas ``execute`` imports geopandas, which
imports pyproj, on whatever thread runs it: a companion route on an AnyIO worker, a pool thread in a test. From then
on every fork of that process crashes. ``os.posix_spawn`` runs no atfork handlers and never does.

Linux CI cannot reproduce the crash, so the static rule, the spawn spies and the conftest and CLI checks are what CI
enforces. The crash tests reproduce it in a child interpreter and skip off macOS. Each child imports pyproj on a thread
before anything else, which made 8 of 8 fresh processes fork-unsafe; with tallyman's modules imported first it was 5
of 8.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_xorq import source_identity as si

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src"

needs_macos = pytest.mark.skipif(sys.platform != "darwin", reason="the PROJ atfork crash is macOS-only")


# ---------------------------------------------------------------------------
# the static rule — what Linux CI enforces
# ---------------------------------------------------------------------------

SPAWN = SRC / "tallyman_core" / "spawn.py"  # the one module that starts children; the run-time tests below check it

_SUBPROCESS_SPAWNS = {"run", "Popen", "call", "check_call", "check_output", "getoutput", "getstatusoutput"}
_OS_SPAWNS = {"fork", "forkpty", "system", "popen", "posix_spawn", "posix_spawnp"}  # and every os.spawn*
_SPAWNING_MODULES = ("multiprocessing", "pty", "concurrent.futures.process")
# Functions and methods that start a child whatever they are reached through: asyncio's subprocesses (a
# ``subprocess.Popen`` underneath) and process pools.
_SPAWNING_ATTRS = {
    "create_subprocess_exec",
    "create_subprocess_shell",
    "subprocess_exec",
    "subprocess_shell",
    "ProcessPoolExecutor",
}


def _is_spawn(module: str, name: str) -> bool:
    if module == "os":
        return name in _OS_SPAWNS or name.startswith("spawn")
    return module == "subprocess" and name in _SUBPROCESS_SPAWNS


def _spawning_module(module: str) -> bool:
    return any(module == m or module.startswith(f"{m}.") for m in _SPAWNING_MODULES)


def _child_starts(tree: ast.AST) -> list[tuple[int, str]]:
    """Where *tree* starts a child process, or imports something that does: ``(line, what)``.

    No argument makes a ``subprocess`` call safe here. Which path CPython takes depends on the executable, every
    std stream and half a dozen keywords, most of them values the rule cannot see; ``tallyman_core.spawn`` is where
    that is decided, once, and checked at run time.
    """
    aliases = {"os": {"os"}, "subprocess": {"subprocess"}}  # the names each module is bound to
    sites = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name in aliases:
                    aliases[a.name].add(a.asname or a.name)
                elif _spawning_module(a.name):
                    sites.append((node.lineno, f"import {a.name}"))
        elif isinstance(node, ast.ImportFrom) and node.module:
            spawns = sorted(a.name for a in node.names if _is_spawn(node.module, a.name) or a.name in _SPAWNING_ATTRS)
            if _spawning_module(node.module) or spawns:
                sites.append((node.lineno, f"from {node.module} import {', '.join(spawns) or '...'}"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in _SPAWNING_ATTRS:
            sites.append((node.lineno, f".{node.attr}"))
        elif isinstance(node, ast.Call) and isinstance(f := node.func, ast.Attribute) and isinstance(f.value, ast.Name):
            if any(f.value.id in bound and _is_spawn(module, f.attr) for module, bound in aliases.items()):
                sites.append((node.lineno, f"{f.value.id}.{f.attr}()"))
    return sites


def test_src_starts_no_child_outside_spawn():
    """No src/ module but ``tallyman_core/spawn.py`` starts a child process.

    ``spawn.run`` and ``spawn.start`` start children with ``posix_spawn``, and set up what ``posix_spawn`` leaves to
    the caller; ``test_clone_spawns_cp_without_fork`` and ``test_buckaroo_start_spawns_without_fork`` check them at run
    time with ``fork_exec`` made to fail.
    """
    offenders = []
    for p in sorted(SRC.rglob("*.py")):
        if p != SPAWN:
            offenders += [f"{p.relative_to(SRC)}:{n} {what}" for n, what in _child_starts(ast.parse(p.read_text()))]
    assert not offenders, "children started outside tallyman_core.spawn:\n" + "\n".join(offenders)


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("import asyncio\nasyncio.create_subprocess_exec('git', 'status')", id="asyncio-exec"),
        pytest.param("async def f(loop, proto):\n    await loop.subprocess_exec(proto, 'git')", id="loop-exec"),
        pytest.param("import multiprocessing\nmultiprocessing.get_context('spawn').Process().start()", id="mp-spawn"),
        pytest.param("from concurrent.futures import ProcessPoolExecutor\nProcessPoolExecutor()", id="pool-import"),
        pytest.param("import concurrent.futures\nconcurrent.futures.ProcessPoolExecutor()", id="pool-attr"),
        pytest.param("import pty\npty.spawn(['sh'])", id="pty"),
        pytest.param("import os as _os\n_os.system('true')", id="os-alias"),
        pytest.param("from os import system\nsystem('true')", id="os-from-import"),
        pytest.param("import os\nos.posix_spawn('/bin/true', ['true'], os.environ)", id="posix-spawn-elsewhere"),
        pytest.param(
            "import subprocess, sys\nsubprocess.run([sys.executable], close_fds=False, umask=0o22)", id="umask"
        ),
        pytest.param(
            "import subprocess, sys\nsubprocess.run([sys.executable], close_fds=False, stderr=subprocess.STDOUT)",
            id="stderr-to-stdout",
        ),
        pytest.param("import subprocess\ncmd = ['git']\nsubprocess.run(cmd, close_fds=False)", id="argv-variable"),
        pytest.param("import subprocess\nsubprocess.run(['/x'], close_fds=False, executable='git')", id="executable"),
    ],
)
def test_the_static_rule_flags(source: str):
    """Each of these starts a child outside ``tallyman_core.spawn``, and all but one fork (CPython 3.13).

    multiprocessing's ``spawn`` start method and ``ProcessPoolExecutor`` still reach ``_posixsubprocess.fork_exec``;
    asyncio's subprocesses are ``subprocess.Popen``. ``umask``, a child std stream on fd 0-2, and a bare program name
    in ``executable`` or in an argv the rule cannot see each send ``subprocess`` down ``fork_exec``.
    """
    assert _child_starts(ast.parse(source)), f"not flagged:\n{source}"


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("from tallyman_core import spawn\nspawn.run(['git', 'status'])", id="spawn-run"),
        pytest.param("import subprocess\nproc: subprocess.Popen | None = None\nsubprocess.DEVNULL", id="names"),
        pytest.param("import os\nos.execv('/bin/true', ['true'])", id="exec"),
        pytest.param("from concurrent.futures import ThreadPoolExecutor\nThreadPoolExecutor()", id="thread-pool"),
        pytest.param("import asyncio\nasyncio.subprocess.PIPE", id="asyncio-constant"),
    ],
)
def test_the_static_rule_passes(source: str):
    """Naming ``subprocess``'s types and constants, exec, and thread pools start no child."""
    assert not _child_starts(ast.parse(source))


# ---------------------------------------------------------------------------
# tallyman_core.spawn, at run time
# ---------------------------------------------------------------------------


@pytest.fixture
def spawned(spawn_spy, monkeypatch) -> list[list[str]]:
    """``spawn_spy``, with a child started through fork failing the test."""

    def forked(args, *rest):
        raise AssertionError(f"started {args!r} through fork_exec, not posix_spawn")

    monkeypatch.setattr(subprocess, "_fork_exec", forked)
    return spawn_spy


@pytest.mark.skipif(sys.platform not in ("darwin", "linux"), reason="_clone shells out to cp only on macOS and Linux")
def test_clone_spawns_cp_without_fork(tmp_path: Path, spawned):
    src = tmp_path / "src.bin"
    src.write_bytes(os.urandom(64 * 1024))
    dst = tmp_path / "dst.bin"

    si._clone(src, dst)

    assert dst.read_bytes() == src.read_bytes()
    assert [Path(argv[0]).name for argv in spawned] == ["cp"], "the clone fell back to a plain copy"


def test_buckaroo_start_spawns_without_fork(spawned, monkeypatch):
    """``start`` (and ``_maybe_restart``, which calls it) launches Buckaroo through ``posix_spawn``.

    The spawn is stopped at ``os.posix_spawn``, so no Buckaroo runs.
    """

    class Spawned(Exception):
        pass

    def stop_at_spawn(path, argv, env, **kwargs):
        spawned.append([str(a) for a in argv])
        raise Spawned

    monkeypatch.setattr(os, "posix_spawn", stop_at_spawn)
    with pytest.raises(Spawned):
        BuckarooManager(port=0).start()
    argv = spawned[0]
    assert argv[argv.index("-m") + 1] == "buckaroo.server"


# ---------------------------------------------------------------------------
# the crash itself, in a child interpreter (macOS)
# ---------------------------------------------------------------------------

_POISON = """\
import json, os, subprocess, sys, threading

os.chdir(sys.argv[1])
# What xorq's first pandas execute does on a pool thread: import pyproj (which opens proj.db) on a thread that exits.
_t = threading.Thread(target=lambda: __import__("pyproj"))
_t.start()
_t.join()
"""

_CANARY = """
canary = subprocess.run(["true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode  # fork_exec
"""


def _run_child(script: str, cwd: Path, *, timeout: float) -> dict:
    # No cwd= (it forces fork_exec); the script changes directory itself. Its temp files go under *cwd* too.
    proc = subprocess.run(
        [sys.executable, "-c", script, str(cwd)],
        capture_output=True,
        text=True,
        timeout=timeout,
        close_fds=False,
        env={**os.environ, "TMPDIR": str(cwd)},
    )
    assert proc.returncode == 0, f"child exited {proc.returncode}:\n{proc.stderr[-4000:]}"
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _in_a_fork_unsafe_child(body: str, cwd: Path, *, timeout: float = 120.0) -> dict:
    """Run *body* in a fresh interpreter whose forks crash, and return the JSON object it prints last.

    The canary, a bare ``subprocess.run(["true"])``, is what shows the process is fork-unsafe; without that the test
    would prove nothing. It returned -11 in 8 of 8 processes set up this way; the test skips rather than pass on a
    process that came out fork-safe.
    """
    out = _run_child(_POISON + _CANARY + textwrap.dedent(body), cwd, timeout=timeout)
    if out["canary"] >= 0:
        pytest.skip(f"a fork from this child did not crash (canary rc={out['canary']}); nothing to test")
    return out


@needs_macos
def test_clone_survives_a_fork_unsafe_process(tmp_path: Path):
    """In a process whose forks crash, ``_clone`` still clones with ``cp`` and does not fall back to a full copy."""
    out = _in_a_fork_unsafe_child(
        """
        import os, shutil
        from pathlib import Path
        from tallyman_xorq import source_identity as si

        fallbacks = 0
        real_copy2 = shutil.copy2

        def counting_copy2(*a, **k):
            global fallbacks
            fallbacks += 1
            return real_copy2(*a, **k)

        si.shutil.copy2 = counting_copy2
        src = Path("src.bin")
        src.write_bytes(os.urandom(4096))
        for i in range(3):
            si._clone(src, Path(f"clone{i}.bin"))
        print(json.dumps({"canary": canary, "fallbacks": fallbacks}))
        """,
        tmp_path,
    )
    assert out["fallbacks"] == 0, f"{out['fallbacks']} of 3 clones fell back to shutil.copy2 (cp died in the fork)"


@needs_macos
@pytest.mark.integration
def test_buckaroo_starts_in_a_fork_unsafe_process(tmp_path: Path):
    """In a process whose forks crash, ``BuckarooManager.start`` still starts Buckaroo (a restart after a crash)."""
    out = _in_a_fork_unsafe_child(
        """
        from tallyman_companion.buckaroo_lifecycle import BuckarooManager, BuckarooUnavailable

        mgr = BuckarooManager(port=0, startup_timeout=60.0)
        try:
            mgr.start()
            error = None
        except BuckarooUnavailable as e:
            error = str(e)
        finally:
            mgr.stop()
        print(json.dumps({"canary": canary, "error": error}))
        """,
        tmp_path,
    )
    assert out["error"] is None, out["error"]


def test_conftest_keeps_the_test_process_fork_safe(tmp_path: Path):
    """The suite's own forks (git in fixtures, GitPython's ``git version`` at import) survive a pool-thread execute.

    ``tests/conftest.py`` imports pyproj on the main thread, so a later import on a worker thread is a no-op and the
    process never becomes fork-unsafe. Without it, ``test_execution_lock.py`` then ``test_git_state_guard.py`` errored
    in 5 of 8 runs: the first pyproj import ran on an AnyIO worker under the companion's TestClient. Checked in a
    child, so a failure here cannot leave this process fork-unsafe.
    """
    script = f"import sys\nsys.path.insert(0, {str(REPO)!r})\nimport tests.conftest  # noqa: F401\n"
    script += 'pinned = "pyproj" in sys.modules\n' + _POISON + _CANARY
    script += 'print(json.dumps({"pinned": pinned, "canary": canary}))\n'
    out = _run_child(script, tmp_path, timeout=120.0)
    assert out["pinned"], "tests/conftest.py does not import pyproj on the main thread"
    assert out["canary"] == 0, f"a fork after a pool-thread pyproj import returned {out['canary']}"


def test_conftest_removes_its_xorq_cache_dir(tmp_path: Path):
    """The temporary XORQ_CACHE_DIR conftest makes is gone once the process exits; test runs do not pile them up."""
    script = f"import sys\nsys.path.insert(0, {str(REPO)!r})\nimport tests.conftest  # noqa: F401\nprint('{{}}')\n"
    _run_child(script, tmp_path, timeout=120.0)
    assert not list(tmp_path.glob("tallyman_xorq_cache_*")), "conftest left its xorq cache dir behind"


def test_the_cli_keeps_its_process_fork_safe(tmp_path: Path):
    """Every tallyman command (``tallyman run``, ``tallyman mcp``) imports pyproj on the main thread before its work.

    The companion's first pandas execute on an AnyIO worker would otherwise make the process fork-unsafe, and from
    then on any child tallyman does not start itself, a library's ``subprocess`` call or a multiprocessing worker,
    dies with SIGSEGV. ``tallyman_core.spawn`` keeps tallyman's own children safe either way.
    """
    script = (
        "import sys\n"
        "from click.testing import CliRunner\n"
        "from tallyman_cli.main import cli\n"
        "CliRunner().invoke(cli, ['mcp', '--help'])  # the group runs first, on the main thread\n"
        'pinned = "pyproj" in sys.modules\n'
    )
    script += _POISON + _CANARY + 'print(json.dumps({"pinned": pinned, "canary": canary}))\n'
    out = _run_child(script, tmp_path, timeout=120.0)
    assert out["pinned"], "the tallyman CLI does not import pyproj on the main thread"
    assert out["canary"] == 0, f"a fork after a pool-thread pyproj import returned {out['canary']}"
