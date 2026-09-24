"""One tallyman server per data directory (#183).

A tallyman server (``tallyman run``) holds state in its own process that no other process sees: the SSE subscribers,
the Buckaroo subprocess and its sessions, the loaded-build caches. Two servers on one data directory (``TALLYMAN_HOME``)
would each serve a view that the other's writes never reach, so the second is refused. A server on another data
directory is fine, and that is how a second tallyman runs for testing and dev.

The claim is an exclusive ``flock`` on ``<data dir>/server.lock``, taken on a descriptor the server keeps open while it
serves. The kernel drops the lock when the process exits, however it exits (SIGKILL included), so there is no stale
lock to clean up. ``flock`` rather than ``fcntl.lockf``: a POSIX record lock is dropped when the process closes *any*
descriptor of the file, which ``read_owner`` does in the server's own process. The file also holds an owner record
(JSON: pid, host, port, bind host, start time, data dir, argv). It names the holder in a refusal, and it tells a
client of this data dir which port its companion serves on (``companion_url``). The record is believed only while the
lock is held; a dead server's record stays in the file and ``read_owner`` returns None for it.

Not claimed here: ``tallyman mcp`` (each Claude Code session spawns one, and their writes are serialized by
``catalog_state.project_lock``) and ``tallyman serve`` (read-only). ``flock`` holds between processes on one machine;
on a network filesystem it may not exclude a server on another host.
"""

from __future__ import annotations

import datetime
import fcntl
import json
import os
import socket
import sys
import time
from pathlib import Path

from tallyman_core.paths import tallyman_home

LOCK_FILENAME = "server.lock"
DEFAULT_COMPANION_URL = "http://127.0.0.1:7860"
_WILDCARD_HOSTS = frozenset({"", "0.0.0.0", "::", "[::]"})

# How long a claim keeps retrying a lock it finds taken before it refuses. ``read_owner`` probes the lock by holding a
# shared lock for the length of one syscall, and a claim that lands in that moment would otherwise be refused by a
# server that does not exist. A real server holds the lock for as long as it runs, so it is still refused.
_CLAIM_RETRY_SECONDS = 0.25

# The data dirs this process has claimed: resolved data dir -> the open descriptor that holds its lock.
_claims: dict[Path, int] = {}


class DataDirInUse(RuntimeError):
    """Another tallyman server holds this data dir. ``owner`` is its record, or None when it could not be read."""

    def __init__(self, data_dir: Path, owner: dict | None):
        self.data_dir = data_dir
        self.owner = owner
        super().__init__(_refusal(data_dir, owner))


def resolved_home(home: Path | str | None = None) -> Path:
    """The data dir as an absolute path with symlinks resolved, so two spellings of one directory compare equal."""
    return Path(home if home is not None else tallyman_home()).expanduser().resolve()


def lock_path(home: Path | str | None = None) -> Path:
    return resolved_home(home) / LOCK_FILENAME


def claim_data_dir(*, port: int, bind_host: str, home: Path | str | None = None) -> dict:
    """Claim the data dir for a server on *bind_host*:*port*, and return the owner record written for it.

    Raises ``DataDirInUse`` when another process (or another claim in this one) holds it. The claim lasts until
    ``release_data_dir`` or the end of the process.
    """
    data_dir = resolved_home(home)
    data_dir.mkdir(parents=True, exist_ok=True)
    # os.open makes a non-inheritable descriptor (PEP 446), so a child such as the Buckaroo subprocess never holds it:
    # a child that outlived a killed server would otherwise keep the data dir claimed. No O_TRUNC: the file is only
    # rewritten once the lock is ours, since until then it names the current holder.
    fd = os.open(data_dir / LOCK_FILENAME, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        _lock_exclusive(fd)
    except BlockingIOError:
        owner = _parse(_read_all(fd))
        os.close(fd)
        raise DataDirInUse(data_dir, owner) from None
    except BaseException:
        os.close(fd)
        raise
    record = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "port": port,
        "bind_host": bind_host,
        "started_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "argv": sys.argv,
    }
    os.ftruncate(fd, 0)
    os.pwrite(fd, json.dumps(record).encode(), 0)
    _claims[data_dir] = fd
    return record


def release_data_dir(home: Path | str | None = None) -> None:
    """Give up this process's claim on the data dir. A no-op when it holds none. The record stays in the file, stale."""
    fd = _claims.pop(resolved_home(home), None)
    if fd is None:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def claim_fd(home: Path | str | None = None) -> int | None:
    """The descriptor holding this process's claim on the data dir, or None when it holds none."""
    return _claims.get(resolved_home(home))


def read_owner(home: Path | str | None = None) -> dict | None:
    """The owner record of the server holding the data dir, or None when no server holds it.

    The lock decides, not the file: a shared lock that can be taken means nobody holds the exclusive one, so whatever
    record is in the file was left by a server that is gone. A held lock whose record cannot be parsed (a claim is
    between truncating the file and writing it) gives ``{}``.
    """
    try:
        fd = os.open(lock_path(home), os.O_RDONLY)
    except FileNotFoundError:
        return None
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            record = _parse(_read_all(fd))
            return record if record is not None else {}
        fcntl.flock(fd, fcntl.LOCK_UN)
        return None
    finally:
        os.close(fd)


def companion_url(home: Path | str | None = None) -> str:
    """Where a client of this data dir reaches its companion, resolved at call time.

    ``TALLYMAN_COMPANION_URL`` when set; else the address the server holding this data dir serves on (the loopback
    when it is bound to a wildcard address); else ``http://127.0.0.1:7860``, the default port of ``tallyman run``.
    """
    explicit = os.environ.get("TALLYMAN_COMPANION_URL")
    if explicit:
        return explicit.rstrip("/")
    try:
        owner = read_owner(home)
    except OSError:
        owner = None
    port = (owner or {}).get("port")
    if isinstance(port, int):
        host = str(owner.get("bind_host") or "")
        if host in _WILDCARD_HOSTS:
            host = "127.0.0.1"
        elif ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{port}"
    return DEFAULT_COMPANION_URL


def _lock_exclusive(fd: int) -> None:
    deadline = time.monotonic() + _CLAIM_RETRY_SECONDS
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)


def _read_all(fd: int) -> bytes:
    return os.pread(fd, max(os.fstat(fd).st_size, 1), 0)


def _parse(raw: bytes) -> dict | None:
    try:
        record = json.loads(raw)
    except ValueError:
        return None
    return record if isinstance(record, dict) else None


def _refusal(data_dir: Path, owner: dict | None) -> str:
    if owner and owner.get("pid"):
        where = ""
        if owner.get("hostname") and owner["hostname"] != socket.gethostname():
            where = f" on host {owner['hostname']}"
        holder = (
            f"another tallyman server (pid {owner['pid']}{where}, port {owner.get('port', '?')}, "
            f"started {owner.get('started_at', '?')})"
        )
    else:
        holder = (
            f"another tallyman server (its owner record in {data_dir / LOCK_FILENAME} could not be read, "
            "which happens while a server is starting)"
        )
    return (
        f"data dir {data_dir} is in use by {holder}. One tallyman server runs per data dir. To run this one, stop "
        "that one, or give this one its own data dir and port: TALLYMAN_HOME=<another dir> tallyman run --port <port>"
    )
