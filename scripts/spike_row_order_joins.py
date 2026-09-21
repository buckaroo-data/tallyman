"""ADR-008 evidence (D6): what happens to ``__row_order`` when entries are joined.

Every file tallyman reads carries ``__row_order`` (ADR-008 D2), so both sides of a join have a column of that name and
ibis renames the right side's to ``__row_order_right``. Raw xorq plus tallyman's ``_canonical_sorted``.

Questions, in the order printed:

1. A two-way join: which columns come out?
2. A three-way join written in one recipe: does it build, and where does it fail?
3. A join ENTRY's snapshot keeps ``__row_order_right`` as data. Can that entry be joined to a third entry?
4. The same, when the writer drops ``__row_order_right`` from the snapshot (the rule D6 adopts).
5. What an author has to write for a three-way join in one recipe.
6. After a fan-out join or an outer join, is the left side's ``__row_order`` still unique and non-null?

    uv run python scripts/spike_row_order_joins.py
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_row_order_joins_"))
os.environ["XORQ_CACHE_DIR"] = str(HOME / "_global_xorq")  # must be set before xorq is imported

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import xorq.api as xo  # noqa: E402
from xorq.ibis_yaml.compiler import build_expr  # noqa: E402

from tallyman_xorq.source_cache import _canonical_sorted  # noqa: E402

N = 5


def entry(name: str) -> Path:
    path = HOME / f"{name}.parquet"
    pq.write_table(
        pa.table({"k": list(range(N)), name: [f"{name}{i}" for i in range(N)], "__row_order": list(range(N))}), path
    )
    return path


def attempt(label: str, fn) -> object:
    try:
        out = fn()
    except Exception as exc:  # noqa: BLE001 - the spike reports whatever is raised
        print(f"   {label}: raises {type(exc).__name__}: {str(exc)[:100]}")
        return None
    shown = list(out.columns) if hasattr(out, "columns") else out
    print(f"   {label}: ok -> {shown}")
    return out


def main() -> None:
    a, b, c = (xo.deferred_read_parquet(str(entry(n))) for n in ("a", "b", "c"))

    print("1. two-way join")
    ab = attempt("a.join(b, 'k')", lambda: a.join(b, "k"))

    print("2. three-way join in one recipe")
    abc = a.join(b, "k").join(c, "k")
    attempt("the expression's columns", lambda: abc)
    attempt("build_expr", lambda: Path(build_expr(abc, builds_dir=HOME / "builds")).name)
    attempt("_canonical_sorted (the next build step)", lambda: _canonical_sorted(abc))
    attempt("execute", lambda: len(abc.execute()))

    print("3. a join entry's snapshot, with __row_order_right kept as data, joined to a third entry")
    kept = HOME / "ab_kept.parquet"
    pq.write_table(ab.to_pyarrow(), kept)
    ab_kept = xo.deferred_read_parquet(str(kept))
    attempt("execute", lambda: len(ab_kept.join(c, "k").execute()))

    print("4. the same, when the writer drops __row_order_right")
    dropped = HOME / "ab_dropped.parquet"
    pq.write_table(ab.to_pyarrow().drop_columns(["__row_order_right"]), dropped)
    ab_dropped = xo.deferred_read_parquet(str(dropped))
    attempt("execute", lambda: len(ab_dropped.join(c, "k").execute()))

    print("5. a three-way join in one recipe, with __row_order dropped on the right-hand inputs")
    attempt("execute", lambda: len(a.join(b.drop("__row_order"), "k").join(c.drop("__row_order"), "k").execute()))

    print("6. the left side's __row_order after a fan-out join and after an outer join")
    twice = xo.union(b, b).drop("__row_order")
    fan = a.join(twice, "k").execute()
    print(f"   fan-out join: {len(fan)} rows, {fan['__row_order'].nunique()} distinct values of __row_order")
    outer = a.filter(a.k < 3).outer_join(c.drop("__row_order"), "k").execute()
    print(f"   outer join: {len(outer)} rows, {int(outer['__row_order'].isna().sum())} with a null __row_order")


if __name__ == "__main__":
    main()
