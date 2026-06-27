"""Rewrite-then-build source caching for catalog entries.

Before a build, tallyman rewrites the submitted expression: every non-parquet
*file* read (``read_csv`` / ``read_json``) gets a ``.cache()`` node injected
immediately downstream, so the CSV→parquet parse is materialised once and
shared — the snapshot strategy keys on the read's path only, so every entry
that reads the same source hits the same cached parquet. ``read_parquet`` /
``read_delta`` are exempt: already columnar, re-reading them costs about the
same as reading a cached copy.

An in-memory read (``read_in_memory`` or an ``ibis.memtable``) is a hard error.
It signals the author dropped to pandas (e.g. ``pd.read_csv`` then
``read_in_memory``) instead of a native deferred reader; the bytes would not
round-trip through the portable build, and what actually wants caching is the
native parse. The error steers the author back to ``deferred_read_csv``.

The author never writes ``.cache()``. ``build_and_persist`` builds the
*rewritten* expression, so the entry's ``content_hash`` and ``xorq_build/``
always carry the cache node while ``expr.py`` keeps the LLM's literal source —
tallyman does not depend on the submitted form (the LLM doesn't reliably track
it). The injected cache's ``base_path`` is supplied at load time by xorq's
``load_expr(cache_dir=...)`` (``compiler.replace_base_path`` rewrites every
``CachedNode`` storage to that dir), which tallyman points at the project's
``compute_cache`` — so the source-read parquet lands under the per-project,
reset-reconciled compute cache, content-addressed and self-healing.
"""

from __future__ import annotations

# File reads cheap enough to re-read rather than cache: columnar formats whose
# scan is already a pushdown read. Everything else (read_csv, read_json) parses
# text and is worth materialising once.
_EXEMPT_READS = frozenset({"read_parquet", "read_delta"})


def _should_cache_read(method_name: str) -> bool:
    """True if a deferred file read should get a source-cache node injected.

    Membership-based, not format-special-cased: every non-exempt reader
    (``read_csv``, ``read_json``) is cached; columnar formats are exempt. Kept a
    named predicate so the read_csv/read_json parity stays unit-testable — this
    xorq has no ``deferred_read_json``, so the read_json path has no end-to-end
    producer to build a fixture from.
    """
    return method_name not in _EXEMPT_READS


_IN_MEMORY_MSG = (
    "expression reads in-memory data (read_in_memory / ibis.memtable). This "
    "usually means the source was loaded into pandas (e.g. pd.read_csv) and "
    "handed to xorq in memory, instead of a native deferred reader. Read the "
    "file with tallyman_read_csv(abs_path, schema=...) for CSVs or "
    "xo.deferred_read_parquet(abs_path) for parquet, so the source round-trips "
    "through the portable build and is cached at the read."
)


class InMemoryReadError(RuntimeError):
    """The expression reads in-memory data instead of a deferred file read."""


def _is_worthy_expr(expr) -> bool:
    """True if the expression does expensive work worth materialising.

    Mirrors ``result_cache.classify_build`` (which decides the same thing from
    the serialized build) on the *live* expression, so the bake decision below
    and the read-time worthiness check stay in lockstep: an Aggregate / Join /
    Sort / window / UDF anywhere in the graph makes the result worth caching.

    UDFs are matched by ancestry, not leaf name. A ``make_pandas_udf`` node is
    classed after the user's function (e.g. ``plusone``), so its live leaf name
    carries no "UDF" — but its MRO holds the base op name (``ScalarUDF`` /
    ``AggUDF`` / …) that the serialized YAML emits and ``classify_build`` greps.
    Testing the leaf name only missed scalar UDFs and broke the lockstep (#81).
    """
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.vendor.ibis.expr.operations.core import Node

    from tallyman_xorq.result_cache import _EXPENSIVE_OPS

    for node in walk_nodes((Node,), expr):
        if type(node).__name__ in _EXPENSIVE_OPS:
            return True
        if any("UDF" in base.__name__ for base in type(node).__mro__):
            return True
    return False


def rewrite_for_build(expr, project: str):
    """Rewrite the submitted expression before build (#73).

    Three rewrites, in order:

    1. **Reject in-memory reads** (always). ``read_in_memory`` / an
       ``ibis.memtable`` signals the author dropped to pandas instead of a
       native deferred reader — raise :class:`InMemoryReadError`.
    2. **Cache each non-parquet source read.** A ``.cache()`` after every
       ``read_csv`` / ``read_json`` materialises the parse once, shared across
       entries (path-only snapshot key). ``read_parquet`` / ``read_delta`` are
       exempt — re-reading a columnar source is already cheap.
    3. **Bake a top-level result cache when the expression is expensive.** The
       cache node becomes part of the durable recipe, so *every* loader — the
       Buckaroo viewer, diffs, ``tracked_expr_from_alias`` chaining — reads the cached
       result instead of re-running the whole DAG (the bug #71 papered over at
       read time). A cheap expression bakes nothing and recomputes ~free.

    All cache nodes resolve their storage at load time to the per-project
    compute cache (xorq's ``load_expr(cache_dir=…)`` rewrites every node's base
    path uniformly), so there is no separate ``result_cache/`` dir — source
    parses and baked results co-locate there, content-addressed and
    reset-reconciled.

    Cache injection is skipped under ``salt`` source-identity mode: its
    path-only snapshot keys would collide across salted entries with identical
    paths but different content. In-memory rejection still applies.
    """
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.caching import ParquetSnapshotCache
    from xorq.common.utils.graph_utils import replace_nodes, walk_nodes
    from xorq.expr.relations import Read

    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq import source_identity as si
    from tallyman_xorq.backend import connect

    reads = walk_nodes(Read, expr)
    if walk_nodes(ops.InMemoryTable, expr) or any(r.method_name == "read_in_memory" for r in reads):
        raise InMemoryReadError(_IN_MEMORY_MSG)

    if si.mode() == "salt":
        return expr

    base = str(compute_cache_dir(project))

    # 2. Source-read caches. One cache (one connection) shared by every read;
    # the snapshot key is path-only so distinct reads get distinct keys. The
    # `wrapped` memo breaks re-descent recursion: replace_nodes re-applies the
    # replacer to a fresh CachedNode's `parent` (graph_utils process_node), the
    # same Read — without the memo it would wrap it again, forever.
    targets = {r for r in reads if _should_cache_read(r.method_name)}
    if targets:
        src_cache = ParquetSnapshotCache.from_kwargs(source=connect(), base_path=base)
        wrapped: dict = {}

        def replacer(node, kwargs):
            if isinstance(node, Read) and node in targets:
                if node in wrapped:
                    return node
                cached = node.to_expr().cache(cache=src_cache).op()
                wrapped[node] = cached
                return cached
            return node.__recreate__(kwargs) if kwargs else node

        expr = replace_nodes(replacer, expr).to_expr()

    # 3. Bake the result cache for an expensive expression. A distinct
    # relative_path keeps baked results inspectable apart from source parses
    # under the same compute-cache root.
    if _is_worthy_expr(expr):
        result_cache = ParquetSnapshotCache.from_kwargs(source=connect(), base_path=base, relative_path="result_cache")
        expr = expr.cache(cache=result_cache)

    return expr
