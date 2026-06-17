"""Alias bookkeeping for the catalog.

Aliases are mutable handles that name a sequence of content hashes:

    alias_map:      {alias: latest_hash}
    alias_history:  {alias: [hash_v1, hash_v2, ...]}   # oldest first

Both live as tallyman-owned keys in the catalog's ``catalog.yaml`` (read and
written through ``catalog_state``), not as separate files. catalog.yaml is the
versioned state carrier the catalog git repo tracks, so alias state clones and
versions with the catalog and ``reset_to``'s ``git reset`` rolls it back for
free — a sidecar store would have to be reconciled separately, and committing
one *inside* the repo broke xorq's ``assert_consistency`` (#48). xorq keeps its
own ``aliases`` key + alias symlinks in the same file; these tallyman keys sit
alongside, and xorq round-trips them untouched.

Identity is the content hash; the alias is a *name* for a concept that may
evolve. The latest hash is always the current best answer; older hashes
remain in the catalog as forensic artifacts.
"""

from __future__ import annotations

from tallyman_core.paths import ensure_project


class AliasExists(ValueError):
    pass


class AliasNotFound(KeyError):
    pass


def _read(project: str) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Read the alias map + history out of catalog.yaml.

    Lazy import: ``aliases`` is imported very early in ``tallyman_core``'s
    package init (before ``catalog_state``'s transitive deps), so resolving
    ``catalog_state`` at call time keeps that ordering clean.
    """
    from tallyman_core import catalog_state as cs

    state = cs.read_tallyman_state(project)
    return state["alias_map"], state["alias_history"]


def _write(project: str, aliases: dict[str, str], history: dict[str, list[str]]) -> None:
    """Persist the alias map + history into catalog.yaml (preserving xorq's keys)."""
    from tallyman_core import catalog_state as cs

    cs.write_tallyman_state(project, alias_map=aliases, alias_history=history)


def load_aliases(project: str) -> dict[str, str]:
    return _read(project)[0]


def load_history(project: str) -> dict[str, list[str]]:
    return _read(project)[1]


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


def previous_version(project: str, content_hash: str) -> str | None:
    """The hash one revision earlier in this entry's alias history, or None.

    None when the entry is a lineage root (version 1) or appears in no history.
    """
    info = version_of_hash(project, content_hash)
    if not info:
        return None
    name, version = info
    if version <= 1:
        return None
    hist = history_for(project, name)
    idx = version - 2  # version is 1-based; the parent is the entry before it
    return hist[idx] if 0 <= idx < len(hist) else None


def set_alias(project: str, name: str, content_hash: str, *, expect_exists: bool | None = None) -> dict:
    """Point `name` at `content_hash` and append to history.

    expect_exists:
      None — accept either fresh or existing alias.
      True — error if alias does NOT exist (`catalog_revise` semantics).
      False — error if alias DOES exist (`catalog_create`/`catalog_alias` semantics).
    """
    ensure_project(project)
    aliases, history = _read(project)
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

    _write(project, aliases, history)

    return {
        "name": name,
        "hash": content_hash,
        "version": len(history[name]),
    }


def rename_alias(project: str, old_name: str, new_name: str) -> dict:
    aliases, history = _read(project)
    if old_name not in aliases:
        raise AliasNotFound(old_name)
    if new_name in aliases:
        raise AliasExists(new_name)
    aliases[new_name] = aliases.pop(old_name)
    history[new_name] = history.pop(old_name, [])
    _write(project, aliases, history)
    return {"old": old_name, "new": new_name, "hash": aliases[new_name]}


def remove_alias(project: str, name: str) -> None:
    aliases, history = _read(project)
    aliases.pop(name, None)
    history.pop(name, None)
    _write(project, aliases, history)
