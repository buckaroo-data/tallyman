"""Catalog-level diff: code, schema, and full entry comparison.

The general DataFrame comparison utilities (stats_diff*, head_diff*,
key_diff* for pandas/polars/xorq backends) live in buckaroo.compare and
are re-exported here for backward compatibility.
"""

from __future__ import annotations

import difflib
import json
import logging
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
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

from tallyman_core.paths import ENTRY_RESULT_FILENAME, ENTRY_SCHEMA_FILENAME

log = logging.getLogger(__name__)

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


def _ensure_entry_parquet(entry: Path) -> Path:
    """Return *entry*'s ``result.parquet`` path, materialising it on demand (#73).

    Entries no longer write a build-time ``result.parquet``; the parquet-backed
    diff backends still want a file. Derive ``(project, hash)`` from the entry
    dir layout (``<project>/artifacts/catalog/entries/<hash>``) and regenerate
    via ``ensure_result``. Returns the (possibly still-absent) path either way —
    callers guard on ``.exists()``.
    """
    pq_path = entry / ENTRY_RESULT_FILENAME
    if not pq_path.exists() and (entry / "xorq_build").is_dir():
        from tallyman_xorq.result_cache import ensure_result

        try:
            ensure_result(entry.parents[3].name, entry.name)
        except Exception:
            # The diff side that needed this file degrades to empty (callers
            # guard on .exists()); log so a genuine materialisation failure is
            # diagnosable rather than silently masked.
            log.debug("ensure_result failed materialising %s", entry, exc_info=True)
    return pq_path


def full_diff(
    a_entry: Path,
    b_entry: Path,
    *,
    a_label: str = "before",
    b_label: str = "after",
    backend: str = "pandas",
    a_expr=None,
    b_expr=None,
    keys: list[str] | None = None,
) -> dict:
    """Produce every diff flavour for two catalog entry directories.

    backend: "pandas" (default), "polars", or "xorq".

    For the xorq backend, pass ``a_expr`` / ``b_expr`` — the two entries'
    (cache-wrapped) expressions, e.g. from
    ``tallyman_xorq.result_cache.cached_result_expr``.  The diff then composes
    ``expr1`` ⋈ ``expr2`` and each side resolves its own cache, so nothing
    depends on ``<entry>/result.parquet`` existing.  Code and schema diffs
    always come from the entry dirs.
    """
    a_code = (a_entry / "expr.py").read_text() if (a_entry / "expr.py").exists() else ""
    b_code = (b_entry / "expr.py").read_text() if (b_entry / "expr.py").exists() else ""
    a_sj, b_sj = a_entry / ENTRY_SCHEMA_FILENAME, b_entry / ENTRY_SCHEMA_FILENAME
    a_schema = json.loads(a_sj.read_text()) if a_sj.exists() else {}
    b_schema = json.loads(b_sj.read_text()) if b_sj.exists() else {}

    # #73: entries no longer carry a build-time result.parquet. Only the
    # parquet-backed branches below need a file, so materialise lazily via
    # ensure_result — the common xorq-with-expressions diff composes the passed
    # cache-resolving expressions and must not regenerate two parquets it never
    # reads.
    if backend == "xorq":
        # Prefer the passed expressions (each carries its own cache node);
        # fall back to materialising + reading result.parquet only when no expr
        # is supplied.
        if a_expr is not None and b_expr is not None:
            a_src, b_src = a_expr, b_expr
        else:
            a_pq, b_pq = _ensure_entry_parquet(a_entry), _ensure_entry_parquet(b_entry)
            a_src, b_src = (a_pq, b_pq) if a_pq.exists() and b_pq.exists() else (None, None)
        if a_src is not None:
            stats = stats_diff_xorq(a_src, b_src)
            head = head_diff_xorq(a_src, b_src)
            keyed = key_diff_xorq(a_src, b_src, keys=keys)
        else:
            stats, head, keyed = [], {}, None
    elif backend == "polars":
        import polars as pl

        a_pq, b_pq = _ensure_entry_parquet(a_entry), _ensure_entry_parquet(b_entry)
        a_df_pl = pl.scan_parquet(a_pq).collect() if a_pq.exists() else pl.DataFrame()
        b_df_pl = pl.scan_parquet(b_pq).collect() if b_pq.exists() else pl.DataFrame()
        stats = stats_diff_polars(a_df_pl, b_df_pl)
        head = head_diff_polars(a_pq, b_pq) if a_pq.exists() and b_pq.exists() else {}
        keyed = key_diff_polars(a_df_pl, b_df_pl)
    else:
        a_pq, b_pq = _ensure_entry_parquet(a_entry), _ensure_entry_parquet(b_entry)
        a_df = pq.read_table(a_pq).to_pandas() if a_pq.exists() else pd.DataFrame()
        b_df = pq.read_table(b_pq).to_pandas() if b_pq.exists() else pd.DataFrame()
        stats = stats_diff(a_df, b_df)
        head = head_diff(a_df, b_df)
        keyed = key_diff(a_df, b_df)

    return {
        "code": code_diff(a_code, b_code, a_label=a_label, b_label=b_label),
        "schema": schema_diff(a_schema, b_schema),
        "head": head,
        "stats": stats,
        "keyed": keyed,
    }
