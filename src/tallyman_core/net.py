"""How a process on this machine reaches a host and port: the URL host for a server's bind address, and whether a
listener already holds a port. Used by ``tallyman run`` before it starts anything, and by Buckaroo's port fallback, so
it imports nothing but the standard library."""

from __future__ import annotations

import errno
import socket


def client_host(bind_host: str) -> str:
    """The host, as written in a URL, at which a process on this machine reaches a server bound to *bind_host*.

    A wildcard is no address to connect to, so it becomes 127.0.0.1, since local work defaults to IPv4. That holds for
    ``::`` too, because ``tallyman run`` binds ``::`` dual-stack (IPv4 as well). An IPv6 address is bracketed.
    """
    host = bind_host.strip("[]")
    if host in ("", "0.0.0.0", "::"):
        return "127.0.0.1"
    return f"[{host}]" if ":" in host else host


def port_in_use(host: str, port: int) -> bool:
    """Whether a listener already holds *port* on *host*, or on an address that overlaps it.

    Two probes. A bind with SO_REUSEADDR, the way the servers bind (uvicorn through asyncio's create_server, Tornado
    for Buckaroo): without it, a port a stopped server left in TIME_WAIT (its closed browser and WS connections) is
    reported busy for ~60s after a restart, though the server itself could bind it. And a connect, because on macOS
    that bind succeeds beside a live listener on an overlapping address: 127.0.0.1 next to 0.0.0.0, or the reverse.
    A wildcard *host* is probed on the loopback, so a listener only on another interface's address is not seen. ``::``
    is probed on both loopbacks, since ``tallyman run`` serves it dual-stack and its clients reach it on 127.0.0.1.
    A bind error other than EADDRINUSE (a *host* that is not an address of this machine) is left for the server to
    report.
    """
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    bare = host.strip("[]")
    with socket.socket(family, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((bare, port))
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                return True
    targets = {"": ["127.0.0.1"], "0.0.0.0": ["127.0.0.1"], "::": ["::1", "127.0.0.1"]}.get(bare, [bare])
    return any(_accepts(target, port) for target in targets)


def _accepts(address: str, port: int) -> bool:
    with socket.socket(socket.AF_INET6 if ":" in address else socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        return s.connect_ex((address, port)) == 0
