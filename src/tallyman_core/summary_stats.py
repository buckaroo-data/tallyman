"""Project-authored summary stats.

Each stat is one ``<project_root>/stats/<name>.py`` file defining a
``compute(col)`` function that returns an ibis expression. The buckaroo
server (PR buckaroo-data/buckaroo#784) scans this directory at
``/load_expr`` time and exec's each file into a generated ``@stat()``
wrapper alongside the built-in ``XORQ_STATS_V2`` entries.

This module owns the authoring surface — write/remove/list — and the
validation pass. We dry-run the source against a 1-row ibis table on
``write_stat`` so syntax / type / shape errors fail at the MCP-tool call
rather than later in the buckaroo subprocess where the only signal is a
log line nobody reads.

Soft-delete by default: ``remove_stat`` moves ``stats/<name>.py`` to
``stats/_disabled/<name>.py`` (the ``_`` prefix on the subdirectory keeps
buckaroo from scanning it). Hard-delete is a manual ``rm`` under the
project directory — matches the catalog-is-curated principle: deletions
are deliberate.
"""

from __future__ import annotations

import builtins
import inspect
import re
from pathlib import Path

from tallyman_core.fsutil import atomic_write_text
from tallyman_core.paths import stats_dir as _stats_dir

# Mirrors the allowlist in ``buckaroo/server/xorq_loading.py``. Keeping
# them in sync by hand for now — these are the names the function body
# can resolve through builtin lookup. Notable absences: ``__import__``,
# ``open``, ``exec``, ``eval``.
_SAFE_BUILTIN_NAMES = (
    "True",
    "False",
    "None",
    "abs",
    "min",
    "max",
    "round",
    "sum",
    "len",
    "int",
    "float",
    "str",
    "bool",
    "list",
    "tuple",
    "dict",
    "set",
    "range",
    "enumerate",
    "zip",
    "map",
    "filter",
    "isinstance",
    "issubclass",
    "type",
)
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class StatSourceError(ValueError):
    """Raised by ``validate_stat_source`` when the source is bad enough to
    reject at the MCP-tool level. The error message becomes the tool's
    response — write it for an LLM reader."""


def stats_dir(project: str) -> Path:
    return _stats_dir(project)


def disabled_dir(project: str) -> Path:
    # Underscore prefix so buckaroo's scanner skips it.
    return stats_dir(project) / "_disabled"


def list_stats(project: str) -> list[dict]:
    """Return ``[{name, path, source, disabled}]`` for every stat file."""
    out: list[dict] = []
    active = stats_dir(project)
    if active.is_dir():
        for p in sorted(active.glob("*.py")):
            if p.name.startswith("_"):
                continue
            out.append(
                {
                    "name": p.stem,
                    "path": str(p),
                    "source": p.read_text(),
                    "disabled": False,
                }
            )
    parked = disabled_dir(project)
    if parked.is_dir():
        for p in sorted(parked.glob("*.py")):
            out.append(
                {
                    "name": p.stem,
                    "path": str(p),
                    "source": p.read_text(),
                    "disabled": True,
                }
            )
    return out


def _restricted_globals() -> dict:
    globs: dict = {
        "__builtins__": {n: getattr(builtins, n) for n in _SAFE_BUILTIN_NAMES},
    }
    try:
        from xorq.vendor import ibis as _ibis  # noqa: PLC0415

        globs["ibis"] = _ibis
    except ImportError:
        pass
    try:
        import xorq.api as _xo  # noqa: PLC0415

        globs["xorq"] = _xo
    except ImportError:
        pass
    return globs


def validate_stat_source(name: str, source: str) -> None:
    """Parse, exec in restricted globals, and dry-run against a 1-row ibis
    table. Raises ``StatSourceError`` on any failure — the message is
    safe to surface back to the agent verbatim."""
    if not _NAME_RE.match(name):
        raise StatSourceError(f"stat name {name!r} is not a valid python identifier")

    ns: dict = {}
    try:
        exec(compile(source, f"<stat: {name}>", "exec"), _restricted_globals(), ns)
    except SyntaxError as e:
        raise StatSourceError(f"syntax error: {e}") from None
    except Exception as e:
        raise StatSourceError(f"source raised at exec time: {e!r}") from None

    compute = ns.get("compute")
    if not callable(compute):
        raise StatSourceError("source must define a callable named 'compute'")

    sig = inspect.signature(compute)
    params = list(sig.parameters.values())
    if len(params) != 1:
        raise StatSourceError(f"compute() must take exactly one parameter, got {len(params)}")

    try:
        from xorq.expr.api import memtable  # noqa: PLC0415
    except ImportError as e:
        raise StatSourceError(f"xorq is not installed: {e}") from None

    table = memtable({"a": [1, 2, 3]}, name="_stat_dry_run")
    col = table["a"]
    try:
        result = compute(col)
    except Exception as e:
        raise StatSourceError(f"compute() raised when called with a column: {e!r}") from None

    # Loose return-type check — any ibis Value has ``.execute()``. Strict
    # isinstance against ``ibis.expr.types.Value`` is brittle across xorq
    # versions, this is the contract every existing XORQ_STATS_V2 entry
    # honours anyway.
    if not hasattr(result, "execute"):
        raise StatSourceError(f"compute() must return an ibis expression (got {type(result).__name__})")


def write_stat(project: str, name: str, source: str) -> Path:
    """Validate then persist. Returns the path of the written file.

    Overwrites an existing same-named stat — that's the intended path for
    "edit a stat" since we don't ship a separate update tool.
    """
    validate_stat_source(name, source)
    d = stats_dir(project)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name}.py"
    return atomic_write_text(path, source if source.endswith("\n") else source + "\n")


def remove_stat(project: str, name: str) -> Path | None:
    """Soft-delete by moving ``stats/<name>.py`` to
    ``stats/_disabled/<name>.py``. Returns the new path, or None if the
    stat didn't exist."""
    src = stats_dir(project) / f"{name}.py"
    if not src.exists():
        return None
    dd = disabled_dir(project)
    dd.mkdir(parents=True, exist_ok=True)
    dst = dd / f"{name}.py"
    if dst.exists():
        dst.unlink()  # idempotent re-disable
    src.rename(dst)
    return dst
