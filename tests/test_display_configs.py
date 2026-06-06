from __future__ import annotations

from fastapi.testclient import TestClient

from pydata_core.display_configs import (
    get_display_config,
    list_display_configs,
    remove_display_config,
    set_display_config,
)
from pydata_xorq import build_and_persist

SAMPLE_OVERRIDES = {"membership": {"merge_rule": "hidden"}, "region": {"color_map_config": {"color_rule": "color_categorical"}}}
SAMPLE_CONFIG = {
    "column_config_overrides": SAMPLE_OVERRIDES,
    "diff_provenance": {
        "source_alias": "sales",
        "va": 1,
        "vb": 2,
        "a_hash": "abc123",
        "b_hash": "def456",
        "keys": ["region"],
    },
}


def _agg_code(project: str) -> str:
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project!r})
expr = t.group_by("region").aggregate(n=t.count())
"""


# ---------------------------------------------------------------------------
# storage layer
# ---------------------------------------------------------------------------


def test_set_get_display_config_round_trip(project: str):
    set_display_config(project, "abc", SAMPLE_CONFIG)
    got = get_display_config(project, "abc")
    assert got["diff_provenance"]["source_alias"] == "sales"
    assert got["column_config_overrides"] == SAMPLE_OVERRIDES


def test_get_display_config_returns_none_for_missing(project: str):
    assert get_display_config(project, "missing") is None


def test_list_display_configs(project: str):
    set_display_config(project, "a", SAMPLE_CONFIG)
    set_display_config(project, "b", SAMPLE_CONFIG)
    assert list_display_configs(project) == ["a", "b"]


def test_remove_display_config(project: str):
    set_display_config(project, "a", SAMPLE_CONFIG)
    assert remove_display_config(project, "a") is True
    assert remove_display_config(project, "a") is False
    assert get_display_config(project, "a") is None


# ---------------------------------------------------------------------------
# catalog_state capture / materialize round-trip
# ---------------------------------------------------------------------------


def test_display_configs_captured_in_catalog_state(project: str):
    from pydata_core.catalog_state import capture_pydata_state, read_pydata_state

    set_display_config(project, "abc123", SAMPLE_CONFIG)
    capture_pydata_state(project)
    state = read_pydata_state(project)
    hashes = [c["content_hash"] for c in state["display_configs"]]
    assert "abc123" in hashes


def test_display_configs_materialize_round_trip(project: str):
    from pydata_core.catalog_state import capture_pydata_state, materialize

    set_display_config(project, "abc123", SAMPLE_CONFIG)
    capture_pydata_state(project)
    remove_display_config(project, "abc123")
    assert get_display_config(project, "abc123") is None
    materialize(project)
    restored = get_display_config(project, "abc123")
    assert restored is not None
    assert restored["diff_provenance"]["source_alias"] == "sales"


def test_display_configs_materialize_removes_unrecorded(project: str):
    from pydata_core.catalog_state import capture_pydata_state, materialize, write_pydata_state

    set_display_config(project, "orphan", SAMPLE_CONFIG)
    write_pydata_state(project, display_configs=[])
    materialize(project)
    assert get_display_config(project, "orphan") is None


# ---------------------------------------------------------------------------
# entry detail API includes display_config
# ---------------------------------------------------------------------------


def test_entry_detail_includes_display_config(fresh_companion_app, project: str, orders_parquet):
    res = build_and_persist(project, _agg_code(project))
    set_display_config(project, res.content_hash, SAMPLE_CONFIG)
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
    body = r.json()
    assert body["display_config"] is not None
    assert body["display_config"]["diff_provenance"]["source_alias"] == "sales"


def test_entry_detail_display_config_null_when_absent(fresh_companion_app, project: str, orders_parquet):
    res = build_and_persist(project, _agg_code(project))
    c = TestClient(fresh_companion_app)
    r = c.get(f"/{project}/api/entry/{res.content_hash}")
    assert r.status_code == 200
    assert r.json()["display_config"] is None
