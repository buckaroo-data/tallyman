"""Guard: the Buckaroo Python server and the buckaroo-js-core renderer must
stay on the same version.

``buckaroo`` (PyPI — the Tornado grid server, pinned in ``pyproject.toml``) and
``buckaroo-js-core`` (npm — the React grid renderer, pinned in
``packages/app/package.json``) share a WebSocket wire protocol and are published
in lockstep from the same repo. When they drift, the grid renders **empty**:
rows ship over the socket but the renderer can't decode them. Buckaroo 0.15.2's
``#938`` changed the row-cell parquet transport *without* bumping
``protocol_version``, so the server's own protocol field is not a reliable
signal — version equality is.

This bit us once already: bumping the server pin 0.15.1 -> 0.15.3 left the
frontend pin at 0.14.8 (and a *stale* 0.12.0 actually installed), and every grid
silently went blank. These checks fail loudly the instant the two drift — naming
the mismatch, in milliseconds, no browser needed — across all four drift modes:
the two source pins, the installed server, and the installed renderer.
"""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
_PYPROJECT = _REPO / "pyproject.toml"
_PACKAGE_JSON = _REPO / "packages" / "app" / "package.json"
_JS_INSTALLED = _REPO / "packages" / "app" / "node_modules" / "buckaroo-js-core" / "package.json"


def _buckaroo_pin() -> str:
    """The exact-pinned ``buckaroo==X`` version from pyproject.toml."""
    deps = tomllib.loads(_PYPROJECT.read_text())["project"]["dependencies"]
    for d in deps:
        m = re.fullmatch(r"buckaroo(?:\[[^\]]*\])?==([0-9][^\s;]*)", d.strip())
        if m:
            return m.group(1)
    raise AssertionError("no exact-pinned `buckaroo==X` in pyproject.toml dependencies")


def _js_core_pin() -> str:
    """The pinned ``buckaroo-js-core`` version from packages/app/package.json."""
    pkg = json.loads(_PACKAGE_JSON.read_text())
    return pkg["dependencies"]["buckaroo-js-core"].lstrip("^~=")


def test_buckaroo_python_and_js_pins_match():
    """The two source pins must be the same version.

    This is the check that catches an author bumping one side without the other
    — exactly the 0.15.1 -> 0.15.3 server bump that left the renderer pin behind.
    """
    py, js = _buckaroo_pin(), _js_core_pin()
    assert py == js, (
        f"buckaroo (python server) is pinned {py!r} in pyproject.toml but "
        f"buckaroo-js-core (renderer) is pinned {js!r} in packages/app/package.json. "
        "They share a wire protocol and must move together — bump both to the same version."
    )


def test_installed_buckaroo_matches_pin():
    """The installed server matches its pin (catches a stale Python env)."""
    import buckaroo

    pin = _buckaroo_pin()
    assert buckaroo.__version__ == pin, (
        f"installed buckaroo {buckaroo.__version__!r} != pyproject pin {pin!r} — run `uv sync`"
    )


def test_installed_js_core_matches_pin():
    """The installed renderer matches its pin.

    This is the exact drift that produced the blank grids: pin said 0.14.8 but
    0.12.0 was on disk because ``pnpm install`` was never re-run. Skipped on a
    Python-only checkout with no ``node_modules``; CI installs it before the suite.
    """
    if not _JS_INSTALLED.exists():
        pytest.skip("packages/app/node_modules absent — run `pnpm -C packages/app install`")
    installed = json.loads(_JS_INSTALLED.read_text())["version"]
    pin = _js_core_pin()
    assert installed == pin, (
        f"installed buckaroo-js-core {installed!r} != package.json pin {pin!r} — "
        "run `pnpm -C packages/app install`"
    )
