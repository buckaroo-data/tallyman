"""Alias bookkeeping for the catalog.

Aliases are mutable handles that name a sequence of content hashes:

    aliases.json        {alias: latest_hash}
    alias_history.json  {alias: [hash_v1, hash_v2, ...]}   # oldest first

Identity is the content hash; the alias is a *name* for a concept that may
evolve. The latest hash is always the current best answer; older hashes
remain in the catalog as forensic artifacts.
"""
from __future__ import annotations

import json
from pathlib import Path

from pydata_core.paths import catalog_dir, ensure_project


class AliasExists(ValueError):
    pass


class AliasNotFound(KeyError):
    pass


def _aliases_path(project: str) -> Path:
    return catalog_dir(project) / "aliases.json"


def _history_path(project: str) -> Path:
    return catalog_dir(project) / "alias_history.json"


def _read_json(p: Path, default):
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return default


def _write_json(p: Path, data):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2, sort_keys=True))


def load_aliases(project: str) -> dict[str, str]:
    return _read_json(_aliases_path(project), {})


def load_history(project: str) -> dict[str, list[str]]:
    return _read_json(_history_path(project), {})


def get_alias(project: str, name: str) -> str | None:
    return load_aliases(project).get(name)


def history_for(project: str, name: str) -> list[str]:
    return load_history(project).get(name, [])


def alias_for_hash(project: str, content_hash: str) -> str | None:
    """If `content_hash` is the latest version of some alias, return the alias name."""
    for name, latest in load_aliases(project).items():
        if latest == content_hash:
            return name
    return None


def version_of_hash(project: str, content_hash: str) -> tuple[str, int] | None:
    """If `content_hash` appears in any alias history, return (alias, 1-based version)."""
    hist = load_history(project)
    for name, hashes in hist.items():
        if content_hash in hashes:
            return name, hashes.index(content_hash) + 1
    return None


def set_alias(project: str, name: str, content_hash: str, *, expect_exists: bool | None = None) -> dict:
    """Point `name` at `content_hash` and append to history.

    expect_exists:
      None — accept either fresh or existing alias.
      True — error if alias does NOT exist (`catalog_revise` semantics).
      False — error if alias DOES exist (`catalog_create`/`catalog_alias` semantics).
    """
    ensure_project(project)
    aliases = load_aliases(project)
    history = load_history(project)
    exists = name in aliases
    if expect_exists is True and not exists:
        raise AliasNotFound(name)
    if expect_exists is False and exists:
        raise AliasExists(name)

    aliases[name] = content_hash
    history.setdefault(name, [])
    # Avoid duplicate consecutive entries.
    if not history[name] or history[name][-1] != content_hash:
        history[name].append(content_hash)

    _write_json(_aliases_path(project), aliases)
    _write_json(_history_path(project), history)
    return {
        "name": name,
        "hash": content_hash,
        "version": len(history[name]),
    }


def rename_alias(project: str, old_name: str, new_name: str) -> dict:
    aliases = load_aliases(project)
    history = load_history(project)
    if old_name not in aliases:
        raise AliasNotFound(old_name)
    if new_name in aliases:
        raise AliasExists(new_name)
    aliases[new_name] = aliases.pop(old_name)
    history[new_name] = history.pop(old_name, [])
    _write_json(_aliases_path(project), aliases)
    _write_json(_history_path(project), history)
    return {"old": old_name, "new": new_name, "hash": aliases[new_name]}


def remove_alias(project: str, name: str) -> None:
    aliases = load_aliases(project)
    history = load_history(project)
    aliases.pop(name, None)
    history.pop(name, None)
    _write_json(_aliases_path(project), aliases)
    _write_json(_history_path(project), history)
