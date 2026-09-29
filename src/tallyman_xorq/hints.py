"""Hints for engine errors whose message does not say how to fix the recipe.

Each entry pairs a regex, searched in a failed build's or query's error message, with a short hint.
``build._ibis_import_hint`` appends the hint of every entry that matches to the error text, so it reaches every place a
build error is shown (a tool reply, the companion, a recalc report, ``catalog_query``). An entry is for an error that
recurs, is distinctive, and has a known fix, where the message names an engine internal rather than the recipe line to
change. A gotcha that only matters when its error shows up belongs here, not in ``catalog_run``'s docstring, which every
call pays for.
"""

from __future__ import annotations

import re

HINTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        # DataFusion's SanityCheckPlan rejects a window keyed on .contains() (strpos) or .re_search(), in order_by or
        # group_by: the sort below the window is on the same expression, but the check does not see it as satisfied.
        # The plan printed is the failing node's whole subtree, so anchor on its head: a window further down is not
        # what failed.
        re.compile(r'SanityCheckPlan.*?Plan: \["(?:Bounded)?WindowAggExec', re.DOTALL),
        "A window keyed on a computed expression (order_by or group_by) can fail DataFusion's plan check.\n"
        "Mutate the key into a column first, then window over that column:\n"
        '  before: w = ibis.window(order_by=t.name.contains("x"))\n'
        '  after:  t = t.mutate(k=t.name.contains("x")); w = ibis.window(order_by=t.k)',
    ),
    (
        re.compile(r"Cannot add <[\w.]*reductions\.CountStar object at 0x[0-9a-f]+> to projection, they belong to"),
        "`t.count()` in an aggregate over a filtered, mutated or grouped `t` counts `t` as it was before those steps,\n"
        "another relation. Refer to the table being aggregated with `ibis._`; count a subset with a conditional sum:\n"
        '  before: t.mutate(k=...).group_by("k").aggregate(n=t.count())\n'
        '  after:  t.mutate(k=...).group_by("k").aggregate(n=ibis._.count(), m=(ibis._.a > 0).sum())',
    ),
    (
        re.compile(r"Cast error: Cannot cast string '[^']*' to value of \w+ type"),
        "A value in the column is not a valid number or date (an empty string, a stray label). `try_cast` raises the\n"
        "same error on this engine. Cast only the rows that match the format and make the rest null:\n"
        '  t.a.re_search(r"^-?[0-9]+$").ifelse(t.a, ibis.null().cast("string")).cast("int64")',
    ),
    (
        re.compile(r"Physical plan does not support logical expression WindowFunction"),
        "A window function is nested inside another window or an aggregate, which the engine cannot plan.\n"
        "Mutate the inner one into a column first, then use that column:\n"
        "  before: t.mutate(s=(t.a - t.a.lag().over(w)).sum().over(w2))\n"
        "  after:  t = t.mutate(g=t.a - t.a.lag().over(w)); expr = t.mutate(s=t.g.sum().over(w2))",
    ),
    (
        re.compile(
            r"No field named \w+\.(\w+)\. (?:Did you mean '\w+\.\w+\.\1'|Valid fields are [^\n]*\"\w+\.\1\")"
            r"|Projections require unique expression names"
        ),
        "After a join, a select that keeps a column and also computes from it leaves two fields the engine names\n"
        "alike, and a later step that names the column fails. Give the kept column a temporary name for that step:\n"
        '  before: j.select(r=j.a / j.b, a=j.a).order_by("a")\n'
        '  after:  j.select(r=j.a / j.b, a_=j.a).order_by("a_").rename(a="a_")',
    ),
    (
        re.compile(r"window\(\) got an unexpected keyword argument 'partition_by'"),
        '`ibis.window` takes `group_by=`, not `partition_by=`: ibis.window(group_by="k", order_by="a")',
    ),
)


def hints_for(message: str) -> list[str]:
    """The hint of every entry whose pattern *message* matches, in table order."""
    return [hint for pattern, hint in HINTS if pattern.search(message)]
