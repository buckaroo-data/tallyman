"""Rows for a reader, with nothing written: the MCP ``catalog_peek`` and ``catalog_query`` tools.

``peek`` reads an existing entry through ``cached_result_expr``, the read every consumer uses. ``query`` runs a recipe
the way a build imports one, with the same fatal checks, the same rewrite (``rewrite_for_build``: a worthy result is
sorted canonically, a cheap one is ordered by ``__row_order``) and the same error text, then executes it and stops: no
entry directory, manifest, parent edge, alias, notebook cell or error record. Both hand back JSON-safe rows.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import linecache
import math
import re
import sys
import traceback
import types
import uuid
from decimal import Decimal

from tallyman_core.aliases import VERSION_REF_RE, get_alias, history_for, resolve_version_ref
from tallyman_core.execution import execution_lock
from tallyman_core.paths import entry_dir

MAX_ROWS = 1000
ROW_ORDER = "__row_order"
_HASH_RE = re.compile(r"[0-9a-f]+")


def json_safe(value):
    """*value* as plain JSON: dates and times as ISO strings, NaN as None, Decimal as float, bytes as text."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return None
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Decimal):
        return json_safe(float(value))
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return f"<{len(raw)} bytes>"
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return str(value)


def _rows(table) -> list[dict]:
    return [{k: json_safe(v) for k, v in row.items()} for row in table.to_pylist()]


def _schema(arrow_schema) -> dict:
    return {"fields": [{"name": f.name, "type": str(f.type)} for f in arrow_schema]}


def _cap(limit: int) -> int:
    return max(0, min(int(limit), MAX_ROWS))


def resolve_ref(project: str, ref: str) -> str:
    """The content hash *ref* names: a content hash, an alias (its head), or ``"<alias>-v<N>"``. LookupError if none."""
    if _HASH_RE.fullmatch(ref) and entry_dir(project, ref).is_dir():
        return ref
    content_hash = get_alias(project, ref) or resolve_version_ref(project, ref)
    if content_hash is None:
        m = VERSION_REF_RE.match(ref)
        hist = history_for(project, m["name"]) if m else []
        if hist:
            raise LookupError(f"{ref!r}: alias {m['name']!r} has {len(hist)} version(s) (v1..v{len(hist)})")
        raise LookupError(
            f"no catalog entry {ref!r} in project {project!r}: pass an alias, '<alias>-v<N>' or a content hash, "
            "as catalog_list shows them"
        )
    if not entry_dir(project, content_hash).is_dir():
        raise LookupError(f"{ref!r} names entry {content_hash}, which is not in project {project!r}")
    return content_hash


def peek(project: str, ref: str, limit: int = 20, columns: list[str] | None = None) -> dict:
    """The first *limit* rows of an existing entry, in ``__row_order`` order, and its full row count."""
    from tallyman_xorq.result_cache import cached_result_expr, entry_manifest

    content_hash = resolve_ref(project, ref)
    expr = cached_result_expr(project, content_hash)  # may heal a missing snapshot, so before the execution lock
    available = list(expr.columns)
    if columns:
        missing = [c for c in columns if c not in available]
        if missing:
            raise ValueError(f"no column(s) {missing} in {ref!r}; its columns are {available}")
    if ROW_ORDER in available:
        expr = expr.order_by(ROW_ORDER)
    if columns:
        expr = expr.select(*columns)
    row_count = entry_manifest(project, content_hash).row_count
    with execution_lock():
        table = expr.limit(_cap(limit)).to_pyarrow()
        if row_count is None:
            row_count = int(expr.count().execute())
    rows = _rows(table)
    return {
        "hash": content_hash,
        "schema": _schema(table.schema),
        "row_count": row_count,
        "rows": rows,
        "truncated": row_count > len(rows),
    }


@contextlib.contextmanager
def _imported(code: str):
    """Run *code* in a fresh module, as a build imports a recipe (``build._import_script``), with the same error text.

    The code never touches disk: it runs from memory, with its source registered in ``linecache`` so tracebacks and
    ``inspect.getsource`` still show its lines. The module and the ``linecache`` entry are removed on exit.
    """
    from tallyman_xorq.build import BuildError, _error_hint

    name = f"tallyman_query_{uuid.uuid4().hex}"
    filename = f"<{name}>"
    module = types.ModuleType(name)
    module.__file__ = filename
    linecache.cache[filename] = (len(code), None, code.splitlines(keepends=True), filename)
    sys.modules[name] = module
    try:
        try:
            # dont_inherit: this module's ``from __future__ import annotations`` is not the recipe's.
            exec(compile(code, filename, "exec", dont_inherit=True), module.__dict__)
        except Exception as exc:
            hint = _error_hint(str(exc), code)
            raise BuildError(f"executing user code raised: {exc}{hint}\n{traceback.format_exc()}") from exc
        yield module
    finally:
        sys.modules.pop(name, None)
        linecache.cache.pop(filename, None)


def query(project: str, code: str, limit: int = 50) -> dict:
    """Run *code* (a recipe binding ``expr``) and return its first *limit* rows and full row count. Writes nothing.

    Raises ``BuildError`` with the text a build gives for the same mistake.
    """
    from tallyman_xorq import parent_capture as pc
    from tallyman_xorq.build import BuildError, _checked_rewrite, _execution_error, _recipe_expr

    # A private collector: the reads the recipe notes as parents are dropped, and none reach an enclosing build's.
    token = pc.begin_collect()
    try:
        with _imported(code) as module:
            expr = _recipe_expr(module)
            if hasattr(expr, "as_table"):
                expr = expr.as_table()  # a column or scalar comes back as a one-column table
            if not hasattr(expr, "to_pyarrow_batches"):
                raise BuildError(f"expr must be a xorq/ibis expression, not {type(expr).__name__}")
            expr, verdict = _checked_rewrite(expr, project)
            if not verdict.worthy:
                expr = expr.order_by(ROW_ORDER)  # a worthy one is sorted canonically; a cheap one keeps __row_order
            cap = _cap(limit)
            kept: list = []
            row_count = 0
            try:
                with execution_lock():
                    reader = expr.to_pyarrow_batches()
                    arrow_schema = reader.schema
                    for batch in reader:
                        if row_count < cap:
                            kept.append(batch.slice(0, cap - row_count))
                        row_count += batch.num_rows
            except Exception as exc:
                raise _execution_error("query execution", exc, code) from exc
    finally:
        pc.end_collect(token)

    import pyarrow as pa  # noqa: PLC0415

    rows = _rows(pa.Table.from_batches(kept, schema=arrow_schema))
    return {"schema": _schema(arrow_schema), "row_count": row_count, "rows": rows, "truncated": row_count > len(rows)}
