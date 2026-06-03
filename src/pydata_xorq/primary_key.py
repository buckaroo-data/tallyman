"""Lineage-aware primary-key resolution for catalog entries.

Detecting a primary key means scanning every column's cardinality — too
expensive to repeat on every diff of a multi-million-row entry.  But a PK is a
property of the data established at the source: row-preserving revisions
(projection, reorder, derive, cast, filter) keep it valid as long as the key
columns survive.  Only grain-changing ops (aggregate / join) can invalidate it
— exactly the entries the structural ``cache_worthy`` classifier flags.

So we detect once and propagate:

  * resolve walks back through the alias lineage; a row-preserving (cheap)
    revision inherits its parent's key when those columns still exist — no scan;
  * a grain-changing (expensive) entry, or a lineage root, detects once;
  * every resolved key is cached per-entry in ``primary_key.json`` (entries are
    content-addressed, so the cache never goes stale).

At diff time, :func:`diff_keys` returns the resolved key that exists in *both*
sides' schemas, so the join skips detection entirely.
"""
from __future__ import annotations

import json
from pathlib import Path


def _entry_columns(project: str, content_hash: str) -> list[str]:
    from pydata_core.paths import entry_dir

    sj = entry_dir(project, content_hash) / "schema.json"
    if not sj.exists():
        return []
    fields = json.loads(sj.read_text()).get("fields", [])
    return [f["name"] for f in fields if "name" in f]


def _pk_cache_path(project: str, content_hash: str) -> Path:
    from pydata_core.paths import entry_dir

    return entry_dir(project, content_hash) / "primary_key.json"


def _read_cached(project: str, content_hash: str) -> list[str] | None:
    p = _pk_cache_path(project, content_hash)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())["keys"]
    except (OSError, ValueError, KeyError):
        return None


def _write_cached(project: str, content_hash: str, keys: list[str]) -> None:
    try:
        _pk_cache_path(project, content_hash).write_text(json.dumps({"keys": keys}))
    except OSError:
        pass


def _parent_hash(project: str, content_hash: str) -> str | None:
    """The previous version of the alias this entry is the current version of."""
    from pydata_core.aliases import history_for, version_of_hash

    info = version_of_hash(project, content_hash)
    if not info:
        return None
    alias, version = info
    if version <= 1:
        return None
    hist = history_for(project, alias)
    idx = version - 2  # version is 1-based; parent is the entry before it
    return hist[idx] if 0 <= idx < len(hist) else None


def resolve_primary_key(
    project: str,
    content_hash: str,
    *,
    threshold: float = 0.98,
    max_group: int | None = 10_000,
) -> list[str]:
    """Resolved primary key for an entry (``[]`` if none), cached + inherited.

    Cheap (row-preserving) revisions inherit the parent's key when its columns
    survive; otherwise the key is detected once and cached.  Returns the key
    columns; an empty list means "no usable key" and is cached too.
    """
    cached = _read_cached(project, content_hash)
    if cached is not None:
        return cached

    cols = set(_entry_columns(project, content_hash))

    # Row-preserving revision → inherit the parent's key if it still applies.
    from pydata_xorq.result_cache import cache_worthy

    if not cache_worthy(project, content_hash):
        parent = _parent_hash(project, content_hash)
        if parent is not None:
            parent_pk = resolve_primary_key(
                project, parent, threshold=threshold, max_group=max_group)
            if parent_pk and set(parent_pk) <= cols:
                _write_cached(project, content_hash, parent_pk)
                return parent_pk

    # Root, grain-changing entry, or key column dropped → detect once.
    from buckaroo.compare import _detect_pk_xorq

    from pydata_xorq.result_cache import cached_result_expr

    expr = cached_result_expr(project, content_hash)
    # _rank_pk_xorq re-sorts candidates by distinctness regardless of the order we
    # pass, so reordering doesn't help.  Instead: try _pk-named columns in isolation
    # first (they're likely explicit PKs), then fall back to the full column set.
    pk_cols = [c for c in cols if "_pk" in c]
    keys = (pk_cols and _detect_pk_xorq(
        expr, threshold=threshold, max_group=max_group, columns=pk_cols)) or []
    if not keys:
        keys = _detect_pk_xorq(
            expr, threshold=threshold, max_group=max_group, columns=list(cols)) or []
    _write_cached(project, content_hash, keys)
    return keys


def diff_keys(
    project: str,
    a_hash: str,
    b_hash: str,
    *,
    threshold: float = 0.98,
    max_group: int | None = 10_000,
) -> list[str]:
    """Join key for diffing two entries: a resolved PK present in both schemas.

    Returns ``[]`` when neither side's key applies to both — the caller then
    lets ``key_diff_xorq`` fall back to live detection.
    """
    a_cols = set(_entry_columns(project, a_hash))
    b_cols = set(_entry_columns(project, b_hash))
    for h in (a_hash, b_hash):
        pk = resolve_primary_key(project, h, threshold=threshold, max_group=max_group)
        if pk and set(pk) <= a_cols and set(pk) <= b_cols:
            return pk
    return []
