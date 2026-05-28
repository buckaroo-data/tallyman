"""Diff between two versions of an aliased catalog entry.

Three kinds of data diff are computed and surfaced together; the demo gets
to pick whichever is most legible for a given situation.

- `head_diff`: literal side-by-side of the first N rows. Cheapest to
  reason about; useless when row order is not meaningful.
- `stats_diff`: column-by-column count / nulls / distinct / numeric mean.
  Schema-shape-independent; surfaces "the distribution shifted" at a
  glance.
- `key_diff`: when both sides share key columns (typically aggregates),
  outer-join on the keys and show per-key value changes. The most
  informative diff for `region → total` style results.

Code and schema diffs are also computed; schema_diff feeds key inference.
"""

from __future__ import annotations

import difflib
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def code_diff(a: str, b: str, *, a_label: str = "before", b_label: str = "after") -> str:
    """Return a Pygments-rendered unified diff as HTML.

    Embeds Pygments' "monokai" style inline so the output matches the
    rest of the dark theme without requiring a global CSS file.
    """
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
    """Compare two schema-doc shapes (as produced by pydata_xorq.build)."""
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


def head_diff(a: pd.DataFrame, b: pd.DataFrame, n: int = 10) -> dict:
    """Side-by-side first-N rows as HTML tables."""
    return {
        "before": a.head(n).to_html(classes="data-table", index=False, border=0),
        "after": b.head(n).to_html(classes="data-table", index=False, border=0),
        "n": n,
        "a_total": len(a),
        "b_total": len(b),
    }


def _numeric_summary(s: pd.Series) -> dict | None:
    if not pd.api.types.is_numeric_dtype(s) or s.empty:
        return None
    return {
        "mean": float(s.mean()),
        "min": float(s.min()),
        "max": float(s.max()),
        "sum": float(s.sum()),
    }


def stats_diff(a: pd.DataFrame, b: pd.DataFrame) -> list[dict]:
    """Per-column count / null% / distinct / numeric summary for shared columns.

    Returns a list keyed by column name (intersection of a.columns and b.columns).
    Columns present in only one side appear with the missing side set to None.
    """
    cols = list(dict.fromkeys(list(a.columns) + list(b.columns)))
    out = []
    for col in cols:
        sa = a[col] if col in a.columns else None
        sb = b[col] if col in b.columns else None
        out.append(
            {
                "name": col,
                "before": _column_summary(sa),
                "after": _column_summary(sb),
            }
        )
    return out


def _column_summary(s: pd.Series | None) -> dict | None:
    if s is None:
        return None
    return {
        "count": int(s.size),
        "nulls": int(s.isnull().sum()),
        "distinct": int(s.nunique(dropna=True)),
        "numeric": _numeric_summary(s),
    }


def _infer_keys(a: pd.DataFrame, b: pd.DataFrame) -> list[str]:
    """Heuristic: shared non-numeric columns whose distinct counts are small
    relative to row count look like keys (region, category, ...).
    """
    shared = [c for c in a.columns if c in b.columns]
    candidates: list[str] = []
    for c in shared:
        sa = a[c]
        if pd.api.types.is_numeric_dtype(sa):
            continue
        if sa.nunique(dropna=False) <= max(20, int(len(sa) * 0.5)):
            candidates.append(c)
    return candidates


def key_diff(a: pd.DataFrame, b: pd.DataFrame) -> dict | None:
    """Outer-join a and b on inferred key columns; show per-key value changes.

    Returns None if no key columns could be inferred or schemas don't permit
    a meaningful join.
    """
    keys = _infer_keys(a, b)
    if not keys:
        return None
    try:
        merged = a.merge(b, on=keys, how="outer", suffixes=("_before", "_after"), indicator=True)
    except Exception:
        return None
    only_left = int((merged["_merge"] == "left_only").sum())
    only_right = int((merged["_merge"] == "right_only").sum())
    both = int((merged["_merge"] == "both").sum())
    merged = merged.drop(columns=["_merge"])
    return {
        "keys": keys,
        "only_before": only_left,
        "only_after": only_right,
        "matched": both,
        "table_html": merged.head(50).to_html(classes="data-table", index=False, border=0),
    }


def full_diff(
    a_entry: Path,
    b_entry: Path,
    *,
    a_label: str = "before",
    b_label: str = "after",
) -> dict:
    """Top-level: produce every flavor of diff for two catalog entry directories."""
    a_code = (a_entry / "expr.py").read_text() if (a_entry / "expr.py").exists() else ""
    b_code = (b_entry / "expr.py").read_text() if (b_entry / "expr.py").exists() else ""
    a_schema = json.loads((a_entry / "schema.json").read_text()) if (a_entry / "schema.json").exists() else {}
    b_schema = json.loads((b_entry / "schema.json").read_text()) if (b_entry / "schema.json").exists() else {}
    a_df = (
        pq.read_table(a_entry / "result.parquet").to_pandas()
        if (a_entry / "result.parquet").exists()
        else pd.DataFrame()
    )
    b_df = (
        pq.read_table(b_entry / "result.parquet").to_pandas()
        if (b_entry / "result.parquet").exists()
        else pd.DataFrame()
    )
    return {
        "code": code_diff(a_code, b_code, a_label=a_label, b_label=b_label),
        "schema": schema_diff(a_schema, b_schema),
        "head": head_diff(a_df, b_df),
        "stats": stats_diff(a_df, b_df),
        "keyed": key_diff(a_df, b_df),
    }
