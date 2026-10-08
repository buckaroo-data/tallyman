"""Check that the Buckaroo pieces tallyman runs are the versions it pins.

Buckaroo is two packages that share a wire protocol and must move together: the
Python grid server (``buckaroo`` on PyPI, pinned in ``pyproject.toml``) and the
React renderer (``buckaroo-js-core`` on npm, pinned in ``packages/app/package.json``).
Two failure modes each look like a working app:

- A renderer older than the server does not advertise ``?caps=stats_update``, so
  the server computes every summary stat before it sends the first row. Nothing
  errors; the grid is only slow to appear.
- ``node_modules`` can lag the lockfile, and ``dist`` can lag ``node_modules``
  (``pnpm install`` after the last ``pnpm build``). What the browser runs is the
  version baked into ``dist``, so that is the one that matters.

``tests/test_buckaroo_version_lockstep.py`` checks the pins and the installed
packages, but only when pytest runs. This module makes the same comparison at
the places a person looks: the companion's log at startup, ``GET /api/version``
(the SPA shows a banner from it), and the vite build.

``dist/build-info.json`` is written by ``vite.config.ts`` and records the
renderer version the bundle was built with.
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

# buckaroo_versions.py lives at <repo>/src/tallyman_core/buckaroo_versions.py.
_REPO_ROOT = Path(__file__).resolve().parents[2]

BUILD_INFO_NAME = "build-info.json"


def _app_dir(repo: Path) -> Path:
    return repo / "packages" / "app"


def server_pin(repo: Path = _REPO_ROOT) -> str | None:
    """The exact ``buckaroo==X`` pin in ``pyproject.toml``, or None."""
    try:
        deps = tomllib.loads((repo / "pyproject.toml").read_text())["project"]["dependencies"]
    except (OSError, KeyError, tomllib.TOMLDecodeError):
        return None
    for dep in deps:
        m = re.fullmatch(r"buckaroo(?:\[[^\]]*\])?==([0-9][^\s;]*)", dep.strip())
        if m:
            return m.group(1)
    return None


def js_core_pin(repo: Path = _REPO_ROOT) -> str | None:
    """The ``buckaroo-js-core`` version in ``packages/app/package.json``, or None."""
    try:
        pkg = json.loads((_app_dir(repo) / "package.json").read_text())
        return pkg["dependencies"]["buckaroo-js-core"].lstrip("^~=")
    except (OSError, KeyError, json.JSONDecodeError):
        return None


def installed_js_core(repo: Path = _REPO_ROOT) -> str | None:
    """The ``buckaroo-js-core`` version in ``node_modules``, or None when absent."""
    path = _app_dir(repo) / "node_modules" / "buckaroo-js-core" / "package.json"
    try:
        return json.loads(path.read_text())["version"]
    except (OSError, KeyError, json.JSONDecodeError):
        return None


def bundled_js_core(repo: Path = _REPO_ROOT) -> str | None:
    """The ``buckaroo-js-core`` version baked into ``dist``, or None when
    ``dist`` has no ``build-info.json``."""
    path = _app_dir(repo) / "dist" / BUILD_INFO_NAME
    try:
        return json.loads(path.read_text())["buckaroo_js_core"]
    except (OSError, KeyError, json.JSONDecodeError):
        return None


def check_versions(server_running: str | None, repo: Path = _REPO_ROOT) -> dict:
    """Compare every Buckaroo version in play against its pin.

    ``server_running`` is the version the live Buckaroo subprocess reports on
    ``/health``, or None when none is running. Returns ``{"pins", "seen",
    "problems"}``; ``problems`` is a list of sentences, each naming the
    mismatch and the command that fixes it, and is empty when everything
    agrees. Never raises: a missing file is reported, not thrown.
    """
    server, js = server_pin(repo), js_core_pin(repo)
    installed, bundled = installed_js_core(repo), bundled_js_core(repo)
    dist_built = (_app_dir(repo) / "dist" / "index.html").exists()
    problems: list[str] = []

    if server and js and server != js:
        problems.append(
            f"buckaroo is pinned {server} in pyproject.toml but buckaroo-js-core is pinned {js} in "
            "packages/app/package.json; they share a wire protocol, so bump both together."
        )
    if server and server_running and server_running != server:
        problems.append(
            f"the running Buckaroo server is {server_running}, but pyproject.toml pins {server}. "
            "Run `uv sync` and restart."
        )
    if js and installed and installed != js:
        problems.append(
            f"node_modules has buckaroo-js-core {installed}, but package.json pins {js}. "
            "Run `pnpm -C packages/app install`, then `pnpm -C packages/app build`."
        )
    if js and bundled and bundled != js:
        problems.append(
            f"the served dist bundles buckaroo-js-core {bundled}, but package.json pins {js}. "
            "Run `pnpm -C packages/app build`. A renderer older than the server makes the grid wait "
            "for every summary stat before it shows any rows."
        )
    elif dist_built and bundled is None:
        problems.append(
            f"the served dist has no {BUILD_INFO_NAME}, so the renderer version it bundles is unknown. "
            "Run `pnpm -C packages/app build`."
        )

    return {
        "pins": {"buckaroo": server, "buckaroo_js_core": js},
        "seen": {
            "buckaroo_server": server_running,
            "js_core_installed": installed,
            "js_core_bundled": bundled,
        },
        "problems": problems,
    }
