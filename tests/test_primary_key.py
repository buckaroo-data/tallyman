from __future__ import annotations

import itertools

import pytest

import tallyman_xorq.primary_key as pk
from tallyman_core.paths import entry_dir
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries
from tallyman_xorq.primary_key import _parent_hash, diff_keys, resolve_primary_key


def _select(project: str, cols: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select({cols})
"""


def _dup_rows(project: str) -> str:
    # Every order twice: high-cardinality columns, but no column set is more
    # than 50% distinct, so the search can never stop early.
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.union(t, distinct=False)
"""


def _ticking_clock(step: float):
    """A fake monotonic clock that advances ``step`` seconds per read."""
    ticks = itertools.count()
    return lambda: next(ticks) * step


def _current_hash(project: str) -> str:
    # newest entry first
    return list_entries(project)[0]["content_hash"]


def test_resolve_detects_and_caches(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, '"order_id", "region", "price"'))
    h = _current_hash(project)
    assert resolve_primary_key(project, h) == ["order_id"]  # order_id is unique
    assert (entry_dir(project, h) / "primary_key.json").exists()  # cached
    # second call reads the cache (same answer)
    assert resolve_primary_key(project, h) == ["order_id"]


def test_row_preserving_revision_inherits_key(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, '"order_id", "region", "price"'))
    base = _current_hash(project)
    catalog_revise("rides", _select(project, '"price", "region", "order_id"'))  # reorder
    child = _current_hash(project)
    assert child != base
    assert _parent_hash(project, child) == base
    # child inherits without its own detection — and the key works for a diff
    assert resolve_primary_key(project, child) == ["order_id"]
    assert (entry_dir(project, child) / "primary_key.json").exists()
    assert diff_keys(project, base, child) == ["order_id"]


def test_dropping_key_column_breaks_inheritance(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, '"order_id", "region", "price"'))
    base = _current_hash(project)
    catalog_revise("rides", _select(project, '"region", "price"'))  # drops order_id
    child = _current_hash(project)
    # order_id no longer present → can't be the join key for the pair
    assert diff_keys(project, base, child) != ["order_id"]


def test_search_times_out_without_caching(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("dups", _dup_rows(project))
    h = _current_hash(project)
    # Each budget check costs 0.3s, so the 1s budget runs out after a few queries
    # — long before an exhaustive search over a key-less table finishes.
    monkeypatch.setattr(pk, "_clock", _ticking_clock(0.3), raising=False)
    with pytest.raises(TimeoutError, match="primary key search"):
        resolve_primary_key(project, h)
    # A timeout is not an answer: nothing is cached, so a later call retries.
    assert not (entry_dir(project, h) / "primary_key.json").exists()
