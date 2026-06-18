from __future__ import annotations

from tallyman_core.charts import get_chart, set_chart
from tallyman_core.display_configs import get_display_config, set_display_config
from tallyman_mcp.server import catalog_create, catalog_revise
from tallyman_xorq.build import list_entries


def _select(project: str, cols: str) -> str:
    return f"""
from tallyman_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.select({cols})
"""


def _current_hash(project: str) -> str:
    # newest entry first
    return list_entries(project)[0]["content_hash"]


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
