"""Alias bookkeeping for the catalog.

Aliases are mutable handles that name a sequence of content hashes:

    alias_map:      {alias: latest_hash}
    alias_history:  {alias: [hash_v1, hash_v2, ...]}   # oldest first

Both live in a tracked ``aliases.jsonl`` in the catalog repo — one line per
alias, ``{"alias", "latest", "history": [...]}``. The native store tracks the
file directly, so alias state clones and versions with the catalog and
``reset_to``'s ``git reset`` rolls it back with the rest of the tree (no
separate reconcile). Before the native cut this had to be smuggled into
catalog.yaml keys, because committing a separate file inside the (then xorq)
catalog repo broke its ``assert_consistency`` (#48); that constraint is gone.

Identity is the content hash; the alias is a *name* for a concept that may
evolve. The latest hash is always the current best answer; older hashes
remain in the catalog as forensic artifacts.
"""

from __future__ import annotations

import json
import re

from tallyman_core.fsutil import atomic_write_text
from tallyman_core.paths import catalog_dir, ensure_project


class AliasExists(ValueError):
    pass


class AliasNotFound(KeyError):
    pass


# "<alias>-v<N>" — the version-reference syntax pinned_expr_from_alias accepts
# (#166), matching the V1…Vn vocabulary the UI and catalog_diff use. Alias
# names matching it are rejected at creation/rename so a name can never
# collide with version syntax.
VERSION_REF_RE = re.compile(r"^(?P<name>.+)-v(?P<n>[1-9]\d*)$")


def validate_alias_name(name: str) -> None:
    """Raise ValueError if *name* is not a legal alias name.

    Currently one rule: a name matching the version-reference syntax would
    make ``pinned_expr_from_alias("<name>")`` ambiguous forever. The steer for
    the common intent — a parallel take on an existing concept — is the
    ``-o<N>`` (option) convention: distinct aliases, each with its own V1…Vn
    history, no collision with version syntax.
    """
    m = VERSION_REF_RE.match(name)
    if m:
        raise ValueError(
            f"alias {name!r} matches the version-reference syntax '<alias>-v<N>' "
            f"(reserved for pinned_expr_from_alias, #166). If this is a parallel "
            f"take on {m['name']!r}, name it {m['name'] + '-o' + m['n']!r} "
            f"(option {m['n']}) instead."
        )


def resolve_version_ref(project: str, ref: str) -> str | None:
    """Resolve ``"<alias>-v<N>"`` (1-based) against the alias history, or None.

    None when *ref* is not version-shaped or names an unknown alias; an
    out-of-range N also returns None (the caller distinguishes it via
    ``history_for`` for a better error message).
    """
    m = VERSION_REF_RE.match(ref)
    if not m:
        return None
    hist = history_for(project, m["name"])
    n = int(m["n"])
    return hist[n - 1] if 1 <= n <= len(hist) else None


def _aliases_file(project: str):
    return catalog_dir(project) / "aliases.jsonl"


def _read(project: str) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Read the alias map + history out of the tracked ``aliases.jsonl``.

    One line per alias: ``{"alias", "latest", "history": [...]}``. The native
    store tracks this file directly, so ``reset_to``'s ``git reset`` rolls alias
    state back with the rest of the tree (no separate reconcile).
    """
    p = _aliases_file(project)
    alias_map: dict[str, str] = {}
    history: dict[str, list[str]] = {}
    if p.exists():
        for line in p.read_text().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            alias_map[rec["alias"]] = rec["latest"]
            history[rec["alias"]] = rec.get("history", [])
    return alias_map, history


def _write(project: str, aliases: dict[str, str], history: dict[str, list[str]]) -> None:
    """Persist the alias map + history to ``aliases.jsonl`` (sorted for a stable
    diff; an empty map removes the file)."""
    ensure_project(project)
    p = _aliases_file(project)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not aliases:
        p.unlink(missing_ok=True)
        return
    body = "".join(
        json.dumps({"alias": name, "latest": aliases[name], "history": history.get(name, [])}) + "\n"
        for name in sorted(aliases)
    )
    atomic_write_text(p, body)


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


def version_of_hash(project: str, content_hash: str, alias: str | None = None) -> tuple[str, int] | None:
    """If `content_hash` appears in an alias history, return (alias, 1-based version).

    When `alias` is given and that alias's history contains `content_hash`,
    resolution is scoped to that lineage. Otherwise (no hint, or the hint's
    history does not contain the hash) it falls back to a dict-order scan and
    returns the first alias whose history contains the hash. The fallback keeps
    every existing caller unchanged; the hint only matters when a hash is shared
    across more than one alias history (#85).
    """
    hist = load_history(project)
    if alias is not None and content_hash in hist.get(alias, []):
        return alias, hist[alias].index(content_hash) + 1
    for name, hashes in hist.items():
        if content_hash in hashes:
            return name, hashes.index(content_hash) + 1
    return None


def previous_version(project: str, content_hash: str, alias: str | None = None) -> str | None:
    """The hash one revision earlier in this entry's alias history, or None.

    None when the entry is a lineage root (version 1) or appears in no history.
    When `alias` is given, resolution is scoped to that alias's lineage so a hash
    shared across more than one alias steps back through the *requested* lineage
    rather than whichever alias sorts first (#85). The hint disambiguates only
    the step being resolved; a multi-hop walk past a parent that has left the
    hinted alias's history falls back to dict order for that hop.
    """
    info = version_of_hash(project, content_hash, alias=alias)
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
    validate_alias_name(name)
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
    validate_alias_name(new_name)
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
