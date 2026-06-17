"""Catalog-free datafusion connect shim (Cut B).

``xorq.api.connect()`` is the no-arg datafusion ``Backend`` factory tallyman's
source cache needs, but ``import xorq.api`` transitively loads the whole
``xorq.catalog`` package (12 modules). This replicates ``connect`` exactly —
``Backend(); do_connect(None)`` (xorq/api.py) — from the catalog-free backend
module instead.

The naive narrowing ``xorq.expr.api.connect`` is *not* a drop-in: that is ibis's
``connect(resource, **kwargs)`` (a different function requiring a positional
arg), not this no-arg datafusion factory — so a swap to it would resolve the
symbol and then fail at call time or build the wrong backend. The
determinism-equivalence is pinned by
``test_native_catalog_store.py::test_connect_shim_snapshot_is_byte_identical``.
"""

from __future__ import annotations

from xorq.backends.xorq_datafusion import Backend


def connect() -> Backend:
    """A fresh no-arg datafusion backend — equivalent to ``xorq.api.connect()``,
    without importing ``xorq.api`` (and therefore without loading the catalog
    package)."""
    backend = Backend()
    backend.do_connect(None)
    return backend
