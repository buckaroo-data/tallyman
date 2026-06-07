"""Display configurations attached to catalog entries.

Stores column_config_overrides (and optional diff provenance) keyed by
content hash under catalog/display_configs/<hash>.json.  The companion
reads this when building a Buckaroo session for an entry so the coloring
travels with the entry across views and marimo exports.
"""

from __future__ import annotations

import json
from pathlib import Path

from tallyman_core.paths import catalog_dir


def _config_dir(project: str) -> Path:
    return catalog_dir(project) / "display_configs"


def config_path(project: str, content_hash: str) -> Path:
    return _config_dir(project) / f"{content_hash}.json"


def set_display_config(project: str, content_hash: str, config: dict) -> Path:
    """Persist a display configuration for content_hash."""
    out = config_path(project, content_hash)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(config, indent=2))
    return out


def get_display_config(project: str, content_hash: str) -> dict | None:
    p = config_path(project, content_hash)
    if not p.exists():
        return None
    return json.loads(p.read_text())


def remove_display_config(project: str, content_hash: str) -> bool:
    p = config_path(project, content_hash)
    if not p.exists():
        return False
    p.unlink()
    return True


def list_display_configs(project: str) -> list[str]:
    base = _config_dir(project)
    if not base.exists():
        return []
    return sorted(p.name[: -len(".json")] for p in base.glob("*.json"))
