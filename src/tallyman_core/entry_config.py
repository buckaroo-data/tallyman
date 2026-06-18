"""Per-entry config that travels across alias versions.

Charts (``chart_specs/<hash>.vl.json``) and display configs
(``display_configs/<hash>.json``) are keyed by content hash, so a revise —
which mints a new hash for the new version — would orphan them ("the chart
didn't carry over"). :func:`carry_forward_entry_config` seeds the new version
from its predecessor.

Post-processing and summary stats are deliberately NOT handled here: they are
project-global (keyed by name under ``post_processing/`` and ``stats/``, not by
hash), so they already apply to every version of every entry and need no
carry-forward.
"""

from __future__ import annotations

from tallyman_core.charts import get_chart, set_chart
from tallyman_core.display_configs import get_display_config, set_display_config


def carry_forward_entry_config(project: str, src_hash: str, dst_hash: str) -> list[str]:
    """Copy hash-keyed per-entry config from ``src_hash`` to ``dst_hash``.

    Called when a new version of an existing alias is recorded (a revise), with
    ``src_hash`` the previous latest and ``dst_hash`` the new version. Each
    section is copied only when the new version does not already define its own,
    so an explicit chart/display set on the new hash is never clobbered. No-op
    when there is no predecessor or the content is unchanged (same hash).

    Returns the names of the sections actually carried (for logging / tool
    output); empty when nothing was carried.
    """
    if not src_hash or src_hash == dst_hash:
        return []

    carried: list[str] = []

    if get_chart(project, dst_hash) is None:
        spec = get_chart(project, src_hash)
        if spec is not None:
            set_chart(project, dst_hash, spec)
            carried.append("chart")

    if get_display_config(project, dst_hash) is None:
        config = get_display_config(project, src_hash)
        if config is not None:
            set_display_config(project, dst_hash, config)
            carried.append("display_config")

    return carried
