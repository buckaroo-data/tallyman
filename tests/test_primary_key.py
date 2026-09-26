from __future__ import annotations

import itertools

import pytest

import tallyman_xorq.primary_key as pk
from tallyman_core.paths import entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.primary_key import _parent_hash, diff_keys, resolve_primary_key


def _select(project: str, src: str, cols: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r}, project={project!r})
expr = t.select({cols})
"""


def _dup_rows(project: str, src: str) -> str:
    # Every order twice: high-cardinality columns, but no column set is more
    # than 50% distinct, so the search can never stop early.
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r}, project={project!r})
expr = t.union(t, distinct=False)
"""


def _ticking_clock(step: float):
    """A fake monotonic clock that advances ``step`` seconds per read."""
    ticks = itertools.count()
    return lambda: next(ticks) * step


def _current_hash(project: str) -> str:
    # newest entry first
    return list_entries(project)[0]["content_hash"]


def test_resolve_detects_and_caches(project, orders_src, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, orders_src, '"order_id", "region", "price", "__row_order"'))
    h = _current_hash(project)
    assert resolve_primary_key(project, h) == ["order_id"]  # order_id is unique
    assert (entry_dir(project, h) / "primary_key.json").exists()  # cached
    # second call reads the cache (same answer)
    assert resolve_primary_key(project, h) == ["order_id"]


def test_row_preserving_revision_inherits_key(project, orders_src, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, orders_src, '"order_id", "region", "price", "__row_order"'))
    base = _current_hash(project)
    catalog_revise("rides", _select(project, orders_src, '"price", "region", "order_id", "__row_order"'))  # reorder
    child = _current_hash(project)
    assert child != base
    assert _parent_hash(project, child) == base
    # child inherits without its own detection — and the key works for a diff
    assert resolve_primary_key(project, child) == ["order_id"]
    assert (entry_dir(project, child) / "primary_key.json").exists()
    assert diff_keys(project, base, child) == ["order_id"]


def test_dropping_key_column_breaks_inheritance(project, orders_src, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, orders_src, '"order_id", "region", "price", "__row_order"'))
    base = _current_hash(project)
    catalog_revise("rides", _select(project, orders_src, '"region", "price", "__row_order"'))  # drops order_id
    child = _current_hash(project)
    # order_id no longer present → can't be the join key for the pair
    assert diff_keys(project, base, child) != ["order_id"]


def test_search_times_out_without_caching(project, orders_src, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("dups", _dup_rows(project, orders_src))
    h = _current_hash(project)
    # Each query is charged the whole 1s budget, so the search stops before
    # its second query.
    monkeypatch.setattr(pk, "_clock", _ticking_clock(1.0))
    with pytest.raises(TimeoutError, match="primary key search"):
        resolve_primary_key(project, h)
    # A timeout is not an answer: nothing is cached, so a later call retries.
    assert not (entry_dir(project, h) / "primary_key.json").exists()


def test_search_within_budget_still_finds_key(project, orders_src, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, orders_src, '"order_id", "region", "price", "__row_order"'))
    h = _current_hash(project)
    # A unique single column is found in a couple of queries, well inside budget.
    monkeypatch.setattr(pk, "_clock", _ticking_clock(0.3))
    assert resolve_primary_key(project, h) == ["order_id"]


def test_keyless_table_resolves_empty_without_timing_out(project, orders_src, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("dups", _dup_rows(project, orders_src))
    h = _current_hash(project)
    # Same few-query budget as the timeout test: one distinct count over every
    # candidate column shows no subset can reach the threshold, so the search
    # answers "no key" at once instead of trying every combination.
    monkeypatch.setattr(pk, "_clock", _ticking_clock(0.3))
    assert resolve_primary_key(project, h) == []
    # A definite "no key" is cached, unlike a timeout.
    assert (entry_dir(project, h) / "primary_key.json").exists()


def test_waiting_for_the_execution_lock_does_not_spend_the_budget(project, orders_src, monkeypatch):
    """#118: the budget bounds the search's own queries. Another thread holds the execution lock for longer than the
    whole budget; the search waits for it and then finds the key, instead of timing out on time it spent queueing."""
    import threading

    from tallyman_core.execution import execution_lock

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, orders_src, '"order_id", "region", "price", "__row_order"'))
    h = _current_hash(project)

    held, done = threading.Event(), threading.Event()

    def _hold():
        with execution_lock():
            held.set()
            done.wait(pk.PK_SEARCH_BUDGET_S * 1.5)

    holder = threading.Thread(target=_hold)
    holder.start()
    held.wait()
    try:
        assert diff_keys(project, h, h) == ["order_id"]
    finally:
        done.set()
        holder.join()


def test_a_heal_before_the_search_does_not_spend_the_budget(project, orders_src, monkeypatch):
    """#118: the budget starts when the search's queries execute. Reading the entry (cached_result_expr, which may
    heal a snapshot) takes longer than the whole budget here, and the search still finds the key."""
    import tallyman_xorq.result_cache as result_cache

    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, orders_src, '"order_id", "region", "price", "__row_order"'))
    h = _current_hash(project)

    now = [0.0]
    monkeypatch.setattr(pk, "_clock", lambda: now[0])
    real = result_cache.cached_result_expr

    def _slow_heal(*args, **kwargs):
        now[0] += 5 * pk.PK_SEARCH_BUDGET_S
        return real(*args, **kwargs)

    monkeypatch.setattr(result_cache, "cached_result_expr", _slow_heal)
    assert resolve_primary_key(project, h) == ["order_id"]
