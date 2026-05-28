"""Project-authored post-processing functions.

Each function is one ``<project_root>/post_processing/<name>.py`` file
defining a ``process(expr)`` function that returns either a new ibis
expression or a pandas DataFrame. The buckaroo server
(``xorq_loading.load_project_post_processing_klasses``) scans this
directory at ``/load_expr`` time and wraps each entry into a
``ColAnalysis`` subclass with ``post_processing_method = <stem>``; the
filename stem becomes the option in the embed's "post processing"
dropdown, and selecting it re-renders the table through ``process``.

This module owns the authoring surface — write/remove/list — and the
validation pass, mirroring ``summary_stats.py``. We dry-run the source
against a small ibis memtable on ``write_post_processing`` so syntax /
type / arity errors fail at the MCP-tool call rather than later in the
buckaroo subprocess where the only signal is a log line nobody reads.

Soft-delete by default: ``remove_post_processing`` moves
``post_processing/<name>.py`` to ``post_processing/_disabled/<name>.py``
(the ``_`` prefix on the subdirectory keeps buckaroo from scanning it).
"""
from __future__ import annotations

import builtins
import inspect
import re
from pathlib import Path

from pydata_core.paths import project_dir

# Mirrors the allowlist in ``buckaroo/server/xorq_loading.py``. Keeping
# them in sync by hand for now — these are the names the function body
# can resolve through builtin lookup.
_SAFE_BUILTIN_NAMES = (
    "True", "False", "None",
    "abs", "min", "max", "round", "sum", "len",
    "int", "float", "str", "bool", "list", "tuple", "dict", "set",
    "range", "enumerate", "zip", "map", "filter",
    "isinstance", "issubclass", "type",
)
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostProcessingSourceError(ValueError):
    """Raised by ``validate_post_processing_source`` when the source is bad
    enough to reject at the MCP-tool level. The error message becomes the
    tool's response — write it for an LLM reader."""


def post_processing_dir(project: str) -> Path:
    return project_dir(project) / "post_processing"


def disabled_dir(project: str) -> Path:
    # Underscore prefix so buckaroo's scanner skips it.
    return post_processing_dir(project) / "_disabled"


def list_post_processings(project: str) -> list[dict]:
    """Return ``[{name, path, source, disabled}]`` for every post-processing file."""
    out: list[dict] = []
    active = post_processing_dir(project)
    if active.is_dir():
        for p in sorted(active.glob("*.py")):
            if p.name.startswith("_"):
                continue
            out.append({
                "name": p.stem,
                "path": str(p),
                "source": p.read_text(),
                "disabled": False,
            })
    parked = disabled_dir(project)
    if parked.is_dir():
        for p in sorted(parked.glob("*.py")):
            out.append({
                "name": p.stem,
                "path": str(p),
                "source": p.read_text(),
                "disabled": True,
            })
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


def validate_post_processing_source(name: str, source: str) -> None:
    """Parse, exec in restricted globals, and dry-run against a tiny ibis
    table. Raises ``PostProcessingSourceError`` on any failure — the
    message is safe to surface back to the agent verbatim."""
    if not _NAME_RE.match(name):
        raise PostProcessingSourceError(
            f"post-processing name {name!r} is not a valid python identifier")

    ns: dict = {}
    try:
        exec(compile(source, f"<post_processing: {name}>", "exec"),
             _restricted_globals(), ns)
    except SyntaxError as e:
        raise PostProcessingSourceError(f"syntax error: {e}") from None
    except Exception as e:
        raise PostProcessingSourceError(
            f"source raised at exec time: {e!r}") from None

    process = ns.get("process")
    if not callable(process):
        raise PostProcessingSourceError(
            "source must define a callable named 'process'")

    sig = inspect.signature(process)
    params = list(sig.parameters.values())
    if len(params) != 1:
        raise PostProcessingSourceError(
            f"process() must take exactly one parameter, got {len(params)}")

    try:
        import xorq.api as xo  # noqa: PLC0415
    except ImportError as e:
        raise PostProcessingSourceError(
            f"xorq is not installed: {e}") from None

    # A multi-row, multi-column memtable — post-processing functions
    # typically reference columns by name and may filter rows, so a
    # 1-row single-column dry-run (like stats use) wouldn't exercise
    # the common shape. Three rows × two cols is small enough that any
    # ``process`` that runs at all will return quickly.
    table = xo.memtable(
        {"a": [1, 2, 3], "b": ["x", "y", "z"]},
        name="_post_processing_dry_run",
    )
    try:
        result = process(table)
    except Exception as e:
        raise PostProcessingSourceError(
            f"process() raised when called with an expression: {e!r}") from None

    # Accept either an ibis expression (has ``.execute``) or a pandas
    # DataFrame. Buckaroo's ``XorqDataflow.add_processing`` accepts both
    # shapes; we match its contract rather than picking one.
    if hasattr(result, "execute"):
        return
    try:
        import pandas as pd  # noqa: PLC0415
        if isinstance(result, pd.DataFrame):
            return
    except ImportError:
        pass
    raise PostProcessingSourceError(
        f"process() must return an ibis expression or pandas DataFrame "
        f"(got {type(result).__name__})")


def write_post_processing(project: str, name: str, source: str) -> Path:
    """Validate then persist. Returns the path of the written file.

    Overwrites an existing same-named file — that's the intended path
    for "edit a post-processing function" since we don't ship a separate
    update tool.
    """
    validate_post_processing_source(name, source)
    d = post_processing_dir(project)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name}.py"
    path.write_text(source if source.endswith("\n") else source + "\n")
    return path


class PostProcessingRunError(ValueError):
    """Raised by ``run_post_processing`` when the function fails against real data."""


def run_post_processing(project: str, entry_name_or_hash: str, source: str) -> dict:
    """Execute a ``process(expr)`` function against a catalog entry's result.

    Resolves *entry_name_or_hash* as an alias first, then as a raw content
    hash. Loads ``result.parquet`` for that entry, execs *source* in a
    restricted namespace, calls ``process(table)``, executes the result, and
    returns a preview dict.

    Returns::

        {
            "entry": "<hash>",
            "row_count": <int>,
            "columns": ["col1", ...],
            "preview": [{...}, ...],   # first 20 rows
        }

    Raises ``PostProcessingRunError`` on any failure — caller surfaces this
    as the tool's ``{"error": ...}`` response.
    """
    from pydata_core.aliases import get_alias  # avoid circular at module level
    from pydata_core.paths import entry_dir

    # Resolve alias → hash, or treat as bare hash.
    content_hash = get_alias(project, entry_name_or_hash) or entry_name_or_hash
    parquet_path = entry_dir(project, content_hash) / "result.parquet"
    if not parquet_path.exists():
        raise PostProcessingRunError(
            f"no result.parquet found for entry {entry_name_or_hash!r} "
            f"(resolved hash: {content_hash})"
        )

    try:
        import pyarrow.parquet as pq  # noqa: PLC0415
        import xorq.api as xo  # noqa: PLC0415
    except ImportError as exc:
        raise PostProcessingRunError(f"missing dependency: {exc}") from None

    table = pq.read_table(parquet_path)
    ibis_table = xo.memtable(table.to_pandas(), name="_post_processing_run")

    # Exec the source and extract process().
    ns: dict = {}
    try:
        exec(compile(source, "<post_processing_run>", "exec"), _restricted_globals(), ns)
    except SyntaxError as exc:
        raise PostProcessingRunError(f"syntax error: {exc}") from None
    except Exception as exc:
        raise PostProcessingRunError(f"source raised at exec time: {exc!r}") from None

    process = ns.get("process")
    if not callable(process):
        raise PostProcessingRunError("source must define a callable named 'process'")

    try:
        result = process(ibis_table)
    except Exception as exc:
        raise PostProcessingRunError(f"process() raised: {exc!r}") from None

    # Execute to get actual rows.
    try:
        import pandas as pd  # noqa: PLC0415
        if hasattr(result, "execute"):
            df = result.execute()
        elif isinstance(result, pd.DataFrame):
            df = result
        else:
            raise PostProcessingRunError(
                f"process() must return an ibis expression or pandas DataFrame "
                f"(got {type(result).__name__})"
            )
    except PostProcessingRunError:
        raise
    except Exception as exc:
        raise PostProcessingRunError(f"execute() raised: {exc!r}") from None

    preview = df.head(20).to_dict(orient="records")
    return {
        "entry": content_hash,
        "row_count": len(df),
        "columns": list(df.columns),
        "preview": preview,
    }


def remove_post_processing(project: str, name: str) -> Path | None:
    """Soft-delete by moving ``post_processing/<name>.py`` to
    ``post_processing/_disabled/<name>.py``. Returns the new path, or
    None if the file didn't exist."""
    src = post_processing_dir(project) / f"{name}.py"
    if not src.exists():
        return None
    dd = disabled_dir(project)
    dd.mkdir(parents=True, exist_ok=True)
    dst = dd / f"{name}.py"
    if dst.exists():
        dst.unlink()  # idempotent re-disable
    src.rename(dst)
    return dst
