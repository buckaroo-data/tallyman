"""Chart specs attached to catalog entries.

V0 stores arbitrary Vega-Lite JSON keyed by content hash under
`catalog/chart_specs/<hash>.vl.json`. The companion's entry-detail
page embeds vega-embed to render the chart above the result table.

The spec is stored as-is — no Vega-Lite validation in Python (the
schema is enormous). The browser renders or fails loudly; failed
renders fall through to "no chart" semantics.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydata_core.paths import catalog_dir


class ChartSpecError(ValueError):
    pass


def _chart_dir(project: str) -> Path:
    return catalog_dir(project) / "chart_specs"


def chart_path(project: str, content_hash: str) -> Path:
    return _chart_dir(project) / f"{content_hash}.vl.json"


def set_chart(project: str, content_hash: str, spec) -> Path:
    """Persist a Vega-Lite spec for `content_hash`.

    `spec` may be a dict OR a string containing JSON. Other types are an error.
    """
    if isinstance(spec, str):
        try:
            spec = json.loads(spec)
        except json.JSONDecodeError as exc:
            raise ChartSpecError(f"spec is not valid JSON: {exc}") from exc
    if not isinstance(spec, dict):
        raise ChartSpecError(f"spec must be a dict (got {type(spec).__name__})")

    out = chart_path(project, content_hash)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(spec, indent=2))
    return out


def get_chart(project: str, content_hash: str) -> dict | None:
    p = chart_path(project, content_hash)
    if not p.exists():
        return None
    return json.loads(p.read_text())


def remove_chart(project: str, content_hash: str) -> bool:
    p = chart_path(project, content_hash)
    if not p.exists():
        return False
    p.unlink()
    return True


def list_charts(project: str) -> list[str]:
    base = _chart_dir(project)
    if not base.exists():
        return []
    # `Path.stem` only strips the final extension, so "a.vl.json" → "a.vl".
    # Strip both segments to recover the bare content hash.
    return sorted(p.name[: -len(".vl.json")] for p in base.glob("*.vl.json"))
