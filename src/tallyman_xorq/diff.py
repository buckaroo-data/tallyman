"""Catalog-level diff: code, schema, and full entry comparison.

The general DataFrame comparison utilities (stats_diff*, head_diff*,
key_diff* for pandas/polars/xorq backends) live in buckaroo.compare and
are re-exported here for backward compatibility.
"""

from __future__ import annotations

import difflib
import json
from pathlib import Path

from buckaroo.compare import (
    head_diff,
    head_diff_polars,
    head_diff_xorq,
    key_diff,
    key_diff_polars,
    key_diff_xorq,
    stats_diff,
    stats_diff_polars,
    stats_diff_xorq,
)

from tallyman_core.paths import ENTRY_SCHEMA_FILENAME

__all__ = [
    "code_diff",
    "full_diff",
    "head_diff",
    "head_diff_polars",
    "head_diff_xorq",
    "key_diff",
    "key_diff_polars",
    "key_diff_xorq",
    "schema_diff",
    "stats_diff",
    "stats_diff_polars",
    "stats_diff_xorq",
]


def code_diff(a: str, b: str, *, a_label: str = "before", b_label: str = "after") -> str:
    """Return a Pygments-rendered unified diff as HTML."""
    from pygments import highlight
    from pygments.formatters import HtmlFormatter
    from pygments.lexers import DiffLexer

    diff_text = "\n".join(
        difflib.unified_diff(
            a.splitlines(),
            b.splitlines(),
            fromfile=a_label,
            tofile=b_label,
            lineterm="",
            n=3,
        )
    )
    if not diff_text.strip():
        return '<pre class="code">(identical)</pre>'

    formatter = HtmlFormatter(noclasses=True, style="monokai", cssstyles="padding: 12px; border-radius: 4px;")
    return highlight(diff_text, DiffLexer(), formatter)


def schema_diff(a_schema: dict, b_schema: dict) -> dict:
    """Compare two schema-doc shapes (as produced by tallyman_xorq.build)."""
    a_fields = {f["name"]: f["type"] for f in a_schema.get("fields", [])}
    b_fields = {f["name"]: f["type"] for f in b_schema.get("fields", [])}
    added = [n for n in b_fields if n not in a_fields]
    removed = [n for n in a_fields if n not in b_fields]
    changed_type = [
        {"name": n, "before": a_fields[n], "after": b_fields[n]}
        for n in a_fields
        if n in b_fields and a_fields[n] != b_fields[n]
    ]
    return {
        "added": added,
        "removed": removed,
        "changed_type": changed_type,
        "row_count": {
            "before": a_schema.get("row_count"),
            "after": b_schema.get("row_count"),
        },
    }


def full_diff(
    a_entry: Path,
    b_entry: Path,
    *,
    a_label: str = "before",
    b_label: str = "after",
    a_expr=None,
    b_expr=None,
    keys: list[str] | None = None,
) -> dict:
    """Produce every diff flavour for two catalog entry directories (xorq-only).

    Pass ``a_expr`` / ``b_expr`` — the two entries' cache-resolving expressions,
    e.g. from ``tallyman_xorq.result_cache.cached_result_expr``.  The diff
    composes ``a_expr`` ⋈ ``b_expr`` and each side resolves its own cache (a
    cheap entry recomputes, an expensive one reads its baked snapshot), so
    nothing depends on a per-entry ``result.parquet`` existing.  Code and schema
    diffs always come from the entry dirs.

    Both expressions are required: with the on-demand ``result.parquet`` layer
    gone there is no file to fall back to, so a missing expr is a programming
    error, surfaced as ``ValueError`` rather than a silent empty diff.  The
    pandas / polars backends (which read a materialised parquet) are dropped —
    every live caller passes ``cached_result_expr`` exprs.
    """
    a_code = (a_entry / "expr.py").read_text() if (a_entry / "expr.py").exists() else ""
    b_code = (b_entry / "expr.py").read_text() if (b_entry / "expr.py").exists() else ""
    a_sj, b_sj = a_entry / ENTRY_SCHEMA_FILENAME, b_entry / ENTRY_SCHEMA_FILENAME
    a_schema = json.loads(a_sj.read_text()) if a_sj.exists() else {}
    b_schema = json.loads(b_sj.read_text()) if b_sj.exists() else {}

    if a_expr is None or b_expr is None:
        raise ValueError(
            "full_diff requires a_expr and b_expr (the entries' cache-resolving "
            "expressions, e.g. from cached_result_expr); the on-demand "
            "result.parquet fallback was removed"
        )

    stats = stats_diff_xorq(a_expr, b_expr)
    head = head_diff_xorq(a_expr, b_expr)
    keyed = key_diff_xorq(a_expr, b_expr, keys=keys)

    return {
        "code": code_diff(a_code, b_code, a_label=a_label, b_label=b_label),
        "schema": schema_diff(a_schema, b_schema),
        "head": head,
        "stats": stats,
        "keyed": keyed,
    }
