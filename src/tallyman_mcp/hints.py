"""Hints for build errors whose message does not say how to fix the recipe.

Each entry pairs a regex, searched in a failed build's error message, with a short hint that the tool reply carries
as ``hint``. An entry is for an error that is distinctive and has a known fix, where the message names an engine
internal rather than the recipe line to change. A gotcha that only matters when its error shows up belongs here,
not in ``catalog_run``'s docstring, which every call pays for.
"""

from __future__ import annotations

import re

HINTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        # DataFusion's SanityCheckPlan rejects a window keyed on .contains() (strpos) or .re_search(), in order_by or
        # group_by: the sort below the window is on the same expression, but the check does not see it as satisfied.
        re.compile(r"SanityCheckPlan.*WindowAggExec", re.DOTALL),
        "A window keyed on a computed expression (order_by or group_by) can fail DataFusion's plan check.\n"
        "Mutate the key into a column first, then window over that column:\n"
        '  before: w = ibis.window(order_by=t.team.contains("/"))\n'
        '  after:  t = t.mutate(k=t.team.contains("/")); w = ibis.window(order_by=t.k)',
    ),
)


def hint_for(message: str) -> str | None:
    """The hint of the first entry whose pattern *message* matches, or None."""
    for pattern, hint in HINTS:
        if pattern.search(message):
            return hint
    return None
