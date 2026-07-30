from __future__ import annotations

from tallyman_core.charts import get_chart, set_chart
from tallyman_core.display_configs import get_display_config, set_display_config
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries


def _select(project: str, cols: str) -> str:
    return f"""
from tallyman_xorq.io import read_project_file
t = read_project_file("orders.parquet", project={project!r})
expr = t.select({cols})
"""


def _current_hash(project: str) -> str:
    # newest entry first
    return list_entries(project)[0]["content_hash"]


def _kid_code(parent: str) -> str:  # tracked follower — rebuilt by the cascade
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({parent!r})
expr = t.mutate(doubled=t.price * 2)
"""


def test_revise_carries_chart_and_display_config(project, orders_parquet, monkeypatch):
    """A revise must seed the new version's per-entry config (chart + display
    config) from the previous version. Both are keyed by content hash, so the
    new version's fresh hash would otherwise orphan them ("chart didn't carry
    over")."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    catalog_create("rides", _select(project, '"order_id", "region", "price"'))
    base = _current_hash(project)

    chart = {"mark": "bar", "encoding": {"x": {"field": "region"}, "y": {"field": "price"}}}
    display = {"column_config_overrides": {"price": {"color_map_config": "BLUE_TO_RED"}}}
    set_chart(project, base, chart)
    set_display_config(project, base, display)

    catalog_revise("rides", _select(project, '"price", "region", "order_id"'))  # reorder
    child = _current_hash(project)
    assert child != base

    assert get_chart(project, child) == chart, "chart did not carry over to the new version"
    assert get_display_config(project, child) == display, "display config did not carry over"


def test_cascade_carries_chart_to_rebuilt_follower(project, orders_parquet, monkeypatch):
    """Per-hash config is keyed by content hash, and an auto-recalc cascade mints
    NEW hashes for rebuilt followers — exactly the orphaning problem
    ``carry_forward_entry_config`` exists to solve for ``catalog_revise``. A
    follower's chart and display config must survive an upstream revise's
    cascade the same way, or every upstream edit silently strips the charts off
    everything downstream."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    monkeypatch.setenv("TALLYMAN_SOURCE_IDENTITY", "cas")
    monkeypatch.delenv("TALLYMAN_AUTO_RECALC", raising=False)  # default ON
    catalog_create("base", _select(project, '"order_id", "region", "price"'))
    catalog_create("kid", _kid_code("base"))
    kid1 = _current_hash(project)

    chart = {"mark": "bar", "encoding": {"x": {"field": "region"}, "y": {"field": "price"}}}
    display = {"column_config_overrides": {"price": {"color_map_config": "BLUE_TO_RED"}}}
    set_chart(project, kid1, chart)
    set_display_config(project, kid1, display)

    out = catalog_revise("base", _select(project, '"price", "region", "order_id"'))  # reorder
    kid2 = ((out.get("recalc") or {}).get("remap") or {}).get(kid1)
    assert kid2, f"cascade did not rebuild the follower: {out.get('recalc')}"

    assert get_chart(project, kid2) == chart, "chart did not carry over through the cascade"
    assert get_display_config(project, kid2) == display, (
        "display config did not carry over through the cascade"
    )
