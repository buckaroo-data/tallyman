"""Per-project configuration, tracked in the catalog repo.

A small ``config.json`` lives alongside ``aliases.jsonl`` in ``catalog_dir`` — a
single tracked JSON object of project-level settings. Because it is tracked, it
clones / versions / ``reset_to``\\ s with the catalog exactly like the alias
state, so a setting is part of the project's history, not a machine-local
preference.

Currently it holds one key, ``auto_recalc`` (the auto-recalc-on-revise switch).
Resolution precedence for that switch, most-significant first:

1. ``TALLYMAN_AUTO_RECALC`` env var (a one-shot override, not persisted).
2. the ``auto_recalc`` key in ``config.json``.
3. the built-in default, **ON** — auto-recalc is the intended workflow; the
   explicit scan→recalc path stays available by turning the flag off.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from tallyman_core.fsutil import atomic_write_text
from tallyman_core.paths import catalog_dir, ensure_project

_AUTO_RECALC_ENV = "TALLYMAN_AUTO_RECALC"
_AUTO_RECALC_DEFAULT = True

_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off"})


def _config_file(project: str) -> Path:
    return catalog_dir(project) / "config.json"


def read_config(project: str) -> dict:
    """The project's ``config.json`` as a dict; ``{}`` when absent or unreadable.

    Tolerant by design: a missing, empty, or malformed file resolves to the
    built-in defaults rather than raising — a corrupt config must never wedge a
    tool call.
    """
    p = _config_file(project)
    if not p.exists():
        return {}
    text = p.read_text().strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def write_config(project: str, cfg: dict) -> Path:
    """Persist *cfg* to the tracked ``config.json`` (sorted for a stable diff)."""
    ensure_project(project)
    p = _config_file(project)
    p.parent.mkdir(parents=True, exist_ok=True)
    return atomic_write_text(p, json.dumps(cfg, indent=2, sort_keys=True) + "\n")


def _parse_bool(val: str) -> bool | None:
    """Parse a string flag; ``None`` for an unrecognized token (caller falls through)."""
    v = val.strip().lower()
    if v in _TRUE_TOKENS:
        return True
    if v in _FALSE_TOKENS:
        return False
    return None


def auto_recalc_enabled(project: str) -> bool:
    """Whether revising an alias should cascade-recompute its stale dependents.

    ``TALLYMAN_AUTO_RECALC`` (if set to a recognized token) wins; otherwise the
    ``auto_recalc`` key in ``config.json``; otherwise the built-in default (ON).
    """
    env = os.environ.get(_AUTO_RECALC_ENV)
    if env is not None:
        parsed = _parse_bool(env)
        if parsed is not None:
            return parsed
    value = read_config(project).get("auto_recalc")
    if isinstance(value, bool):
        return value
    return _AUTO_RECALC_DEFAULT


def set_auto_recalc(project: str, enabled: bool) -> None:
    """Persist the ``auto_recalc`` switch into the project's ``config.json``."""
    cfg = read_config(project)
    cfg["auto_recalc"] = bool(enabled)
    write_config(project, cfg)
