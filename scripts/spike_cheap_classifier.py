"""ADR-008 evidence (D4): what a test for "cheap" has to look at, now that two kinds of entry stay.

A cheap entry has no file of its own and pages by its parent's ``__row_order``, so the test has to guarantee that the
column stays unique through the plan. D4's first wording was an allow-list of RELATION operations. This script runs
three tests over the same recipe shapes:

- today's deny-list (``result_cache.classify_build``, a regex over ``expr.yaml``);
- a relation-only allow-list, which is D4 as first worded, with drop-null and fill-null added to its list;
- the test D4 now specifies: the relation allow-list, exactly one file read, and no value operation that multiplies
  rows, depends on row order, or is not pure, decided by base class on the live expression.

For each shape it also executes the plan and reports whether ``__row_order`` is still unique.

    uv run python scripts/spike_cheap_classifier.py
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_cheap_classifier_"))
os.environ["XORQ_CACHE_DIR"] = str(HOME / "_global_xorq")  # must be set before xorq is imported

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import xorq.api as xo  # noqa: E402
import xorq.vendor.ibis as ibis  # noqa: E402
import xorq.vendor.ibis.expr.operations as ops  # noqa: E402
from xorq.common.utils.graph_utils import walk_nodes  # noqa: E402
from xorq.expr.relations import Read  # noqa: E402
from xorq.ibis_yaml.compiler import build_expr  # noqa: E402
from xorq.vendor.ibis.expr.operations.core import Node  # noqa: E402

from tallyman_xorq.result_cache import classify_build  # noqa: E402

ROW_PRESERVING_RELATIONS = (Read, ops.Filter, ops.Project, ops.DropColumns, ops.DropNull, ops.FillNull)
NEVER_CHEAP_VALUES = (ops.Unnest, ops.WindowFunction, ops.Impure)
NEVER_CHEAP_BY_NAME = {"TimestampNow", "DateNow"}  # these are Constant, not Impure, in xorq's ibis


def relation_only_allow_list(expr) -> bool:
    relations = [n for n in walk_nodes((Node,), expr) if isinstance(n, ops.Relation)]
    return all(isinstance(n, ROW_PRESERVING_RELATIONS) for n in relations)


def is_cheap(expr) -> bool:
    nodes = list(walk_nodes((Node,), expr))
    relations = [n for n in nodes if isinstance(n, ops.Relation)]
    if len({n for n in relations if isinstance(n, Read)}) != 1:
        return False
    if not all(isinstance(n, ROW_PRESERVING_RELATIONS) for n in relations):
        return False
    for n in nodes:
        if isinstance(n, NEVER_CHEAP_VALUES) or type(n).__name__ in NEVER_CHEAP_BY_NAME:
            return False
        if any("UDF" in base.__name__ for base in type(n).__mro__):
            return False
    return True


def main() -> None:
    n = 6
    tags = [["x", "y"], ["z"], [], ["x"], ["y", "z", "w"], ["q"]]
    parent = {"k": list(range(n)), "v": [1.5, 2.5, None, 4.5, 5.5, 6.5], "tags": tags, "__row_order": list(range(n))}
    pq.write_table(pa.table(parent), HOME / "t.parquet")
    pq.write_table(pa.table({"k": [1, 3, 5], "__row_order": [0, 1, 2]}), HOME / "u.parquet")
    t = xo.deferred_read_parquet(str(HOME / "t.parquet"))
    u = xo.deferred_read_parquet(str(HOME / "u.parquet"))

    shapes = {
        "filter + computed column": t.filter(t.k > 0).mutate(w=t.v * 2),
        "rename, cast, drop a column": t.rename(key="k").mutate(v=t.v.cast("float32")).drop("tags"),
        "drop_null, fill_null": t.drop_null(["v"]).fill_null({"v": 0.0}),
        "unnest inside a select": t.select("k", "__row_order", tag=t.tags.unnest()),
        "row_number() in a mutate": t.mutate(rn=ibis.row_number()),
        "share of total in a mutate": t.mutate(share=t.v / t.v.sum()),
        "lag() in a mutate": t.mutate(prev=t.v.lag()),
        "random() in a mutate": t.mutate(r=ibis.random()),
        "filter by membership in a second file": t.filter(t.k.isin(u.k)),
        "filter against a scalar subquery": t.filter(t.v > t.v.mean()),
        "limit": t.limit(3),
        "distinct": t.select("k", "__row_order").distinct(),
    }
    print(f"parent has {n} rows\n")
    print(f"{'recipe shape':40s} {'today':8s} {'relations only':15s} {'D4':7s} rows  __row_order unique")
    names: set[str] = set()
    for label, expr in shapes.items():
        build = Path(build_expr(expr, builds_dir=HOME / "builds"))
        names |= {m for p in build.glob("*.yaml") for m in re.findall(r"op:\s*([A-Za-z_]+)", p.read_text())}
        today = "worthy" if classify_build(build)["worthy"] else "cheap"
        relations = "cheap" if relation_only_allow_list(expr) else "worthy"
        proposed = "cheap" if is_cheap(expr) else "worthy"
        out = expr.execute()
        print(f"{label:40s} {today:8s} {relations:15s} {proposed:7s} {len(out):4d}  {out['__row_order'].is_unique}")

    relation_names = {c.__name__ for c in ops.Relation.__subclasses__()} | {"Read"}
    not_relations = sorted(n for n in names if n not in relation_names and not hasattr(ops, n))
    print(f"\nnames the classify_build regex matches that are not operations at all: {not_relations}")
    print(
        f"value operations it matches alongside the relations: {sorted(n for n in names if n in ('Field', 'Literal'))}"
    )


if __name__ == "__main__":
    main()
