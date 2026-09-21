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

Detection is cheap to rule out and time-boxed.  Before trying combinations,
one distinct count over every candidate column settles whether *any* key can
exist: a table whose full rows fall under the threshold (duplicated rows,
repeated contracts) has none, and resolves to ``[]`` at once.  Otherwise the
search runs one query per column combination, with the budget checked before
each; when it runs out, :class:`PrimaryKeySearchTimeout` is raised and nothing
is cached.
"""

from __future__ import annotations

import json
import time
from itertools import combinations
from pathlib import Path

from tallyman_xorq.row_order import ROW_ORDER

PK_SEARCH_BUDGET_S = 1.0

# Indirection so tests can drive the budget with a fake clock.
_clock = time.monotonic


class PrimaryKeySearchTimeout(TimeoutError):
    """The primary-key search ran past its time budget."""


def _check_deadline(deadline: float, content_hash: str) -> None:
    if _clock() >= deadline:
        raise PrimaryKeySearchTimeout(f"primary key search for {content_hash[:12]} exceeded {PK_SEARCH_BUDGET_S:g}s")


def _keyable(dtype) -> bool:
    """Scalar columns only: a list / struct / map column is never a join key."""
    return not (dtype.is_array() or dtype.is_struct() or dtype.is_map())


def _column_stats(expr, cols: list[str], *, deadline: float, content_hash: str) -> tuple[int, dict[str, int]]:
    """Row count and per-column distinct counts, in one query."""
    _check_deadline(deadline, content_hash)
    aggs = [expr.count().name("__n__")] + [expr[c].nunique().name(c) for c in cols]
    row = expr.aggregate(aggs).execute().iloc[0]
    return int(row["__n__"]), {c: int(row[c]) for c in cols}


def _any_key_possible(expr, cols: list[str], need: float, *, deadline: float, content_hash: str) -> bool:
    """Whether some subset of ``cols`` could reach ``need`` distinct tuples.

    Dropping columns from a tuple can only merge distinct tuples, never split
    them, so no subset of ``cols`` has more distinct tuples than ``cols`` as a
    whole.  If all of them together fall short, every combination does.
    """
    _check_deadline(deadline, content_hash)
    return int(expr.select(*cols).distinct().count().execute()) >= need


def _detect_pk(
    expr,
    columns: list[str],
    *,
    n: int,
    distinct: dict[str, int],
    threshold: float,
    max_group: int | None,
    deadline: float,
    content_hash: str,
    max_width: int = 4,
) -> list[str] | None:
    """buckaroo's ``_rank_pk_xorq`` search with a deadline checked before each query.

    Single columns (most-unique first), then composites of width 2..``max_width``
    (shortest first); the first whose distinct-tuple fraction reaches
    ``threshold`` and whose largest group is within ``max_group`` wins.
    ``n`` / ``distinct`` come from :func:`_column_stats`.
    """
    from buckaroo.compare import _max_group_xorq

    cols = [c for c in expr.schema() if c in columns]
    if not cols:
        return None
    need = threshold * n

    def _accept(combo: tuple[str, ...], d: int) -> bool:
        if d < need:
            return False
        if max_group is None:
            return True
        _check_deadline(deadline, content_hash)
        return _max_group_xorq(expr, list(combo)) <= max_group

    for c in sorted(cols, key=lambda c: distinct[c], reverse=True):
        if _accept((c,), distinct[c]):
            return [c]

    usable = [c for c in cols if distinct[c] > 1]
    for width in range(2, max_width + 1):
        for combo in combinations(usable, width):
            bound = 1
            for c in combo:
                bound *= distinct[c]
                if bound >= need:
                    break
            if bound < need:
                continue
            _check_deadline(deadline, content_hash)
            d = int(expr.select(*combo).distinct().count().execute())
            if _accept(combo, d):
                return list(combo)
    return None


def _entry_columns(project: str, content_hash: str) -> list[str]:
    from tallyman_core.paths import entry_schema_path

    sj = entry_schema_path(project, content_hash)
    if not sj.exists():
        return []
    fields = json.loads(sj.read_text()).get("fields", [])
    return [f["name"] for f in fields if "name" in f]


def _pk_cache_path(project: str, content_hash: str) -> Path:
    from tallyman_core.paths import entry_dir

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
    from tallyman_core.aliases import previous_version

    # PK inheritance keys purely on content_hash and has no requested alias in
    # scope, so it keeps the dict-order fallback (#85). A hash shared across two
    # histories is harmless here: the inherited key is re-validated against this
    # entry's own columns by the caller. An alias-scoped pass is deferred.
    return previous_version(project, content_hash)


def resolve_primary_key(
    project: str,
    content_hash: str,
    *,
    threshold: float = 0.98,
    max_group: int | None = 10_000,
    deadline: float | None = None,
) -> list[str]:
    """Resolved primary key for an entry (``[]`` if none), cached + inherited.

    Cheap (row-preserving) revisions inherit the parent's key when its columns
    survive; otherwise the key is detected once and cached.  Returns the key
    columns; an empty list means "no usable key" and is cached too.

    Detection must finish by ``deadline`` (a ``_clock()`` reading; default
    ``PK_SEARCH_BUDGET_S`` from now) or :class:`PrimaryKeySearchTimeout` is
    raised.
    """
    if deadline is None:
        deadline = _clock() + PK_SEARCH_BUDGET_S
    cached = _read_cached(project, content_hash)
    if cached is not None:
        return cached

    # __row_order is unique in every table, so it would win the search for any table without a real key, and row
    # positions shift between versions, so a diff keyed on it would be meaningless (ADR-008 D6).
    cols = set(_entry_columns(project, content_hash)) - {ROW_ORDER}

    # Row-preserving revision → inherit the parent's key if it still applies.
    from tallyman_xorq.result_cache import cache_worthy

    if not cache_worthy(project, content_hash):
        parent = _parent_hash(project, content_hash)
        if parent is not None:
            parent_pk = resolve_primary_key(
                project, parent, threshold=threshold, max_group=max_group, deadline=deadline
            )
            if parent_pk and set(parent_pk) <= cols:
                _write_cached(project, content_hash, parent_pk)
                return parent_pk

    # Root, grain-changing entry, or key column dropped → detect once.
    from tallyman_xorq.result_cache import cached_result_expr

    expr = cached_result_expr(project, content_hash)
    schema = expr.schema()
    candidates = [c for c in schema if c in cols and _keyable(schema[c])]
    if not candidates:
        _write_cached(project, content_hash, [])
        return []

    n, distinct = _column_stats(expr, candidates, deadline=deadline, content_hash=content_hash)
    if n == 0 or not _any_key_possible(expr, candidates, threshold * n, deadline=deadline, content_hash=content_hash):
        _write_cached(project, content_hash, [])
        return []

    # _rank_pk_xorq sorts candidates by distinctness, which lets high-cardinality
    # float columns (e.g. avg_duration_seconds) beat meaningful composite string keys.
    # We try candidate pools in semantic priority order so floats are only reached
    # when no better key exists:
    #   1. string columns only            (single string, then string composites)
    #   2. string + integer columns       (cross-type: string+int combos)
    #   3. integer columns only           (prefer names containing "pk" or "id")
    #   4. everything (floats included)   (last resort)
    str_cols = [c for c in candidates if schema[c].is_string()]
    pk_id_int = [c for c in candidates if schema[c].is_integer() and any(w in c.lower() for w in ("pk", "id"))]
    other_int = [c for c in candidates if schema[c].is_integer() and c not in pk_id_int]
    int_cols = pk_id_int + other_int  # pk/id-named ints first within this group

    candidate_groups: list[list[str]] = []
    if str_cols:
        candidate_groups.append(str_cols)
    if str_cols and int_cols:
        candidate_groups.append(str_cols + int_cols)
    if int_cols:
        candidate_groups.append(int_cols)
    candidate_groups.append(candidates)  # floats only reached here

    keys: list[str] = []
    for group in candidate_groups:
        found = (
            _detect_pk(
                expr,
                group,
                n=n,
                distinct=distinct,
                threshold=threshold,
                max_group=max_group,
                deadline=deadline,
                content_hash=content_hash,
            )
            or []
        )
        if found:
            keys = found
            break

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
    skips the keyed diff (``full_diff(keys=[])``).  Both sides share one
    ``PK_SEARCH_BUDGET_S`` budget.
    """
    a_cols = set(_entry_columns(project, a_hash)) - {ROW_ORDER}
    b_cols = set(_entry_columns(project, b_hash)) - {ROW_ORDER}
    deadline = _clock() + PK_SEARCH_BUDGET_S
    for h in (a_hash, b_hash):
        pk = resolve_primary_key(project, h, threshold=threshold, max_group=max_group, deadline=deadline)
        if pk and set(pk) <= a_cols and set(pk) <= b_cols:
            return pk
    return []
