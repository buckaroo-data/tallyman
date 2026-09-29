"""Tests for the project-authored display klass surface.

``catalog_add_display_klass`` runs the class through buckaroo before the
file lands on disk: buckaroo's loader execs it, buckaroo computes summary
stats for a sample, and every column is styled with the class. A class
that would fail when buckaroo renders a table is rejected with the
exception and the column it failed on, and nothing is written.
"""

from __future__ import annotations

import pytest

from tallyman_core.paths import display_dir

USES_GETATTR = """
class MainUsesGetattr(DefaultMainStyling):
    df_display_name = "main"

    @classmethod
    def style_column(cls, col, column_metadata):
        cc = super().style_column(col, column_metadata)
        if getattr(cc, "displayer_args", None) is None:
            cc["displayer_args"] = {"displayer": "obj"}
        return cc
"""

RAISES_ON_REGION = """
class MainRegionBug(DefaultMainStyling):
    df_display_name = "main"

    @classmethod
    def style_column(cls, col, column_metadata):
        cc = super().style_column(col, column_metadata)
        if column_metadata.get("orig_col_name") == "region":
            cc["width"] = 1 / 0
        return cc
"""

MISSING_STAT = """
class MainMissingStat(DefaultMainStyling):
    df_display_name = "main"

    @classmethod
    def style_column(cls, col, column_metadata):
        cc = super().style_column(col, column_metadata)
        if column_metadata["no_such_stat"] > 0:
            cc["displayer_args"] = {"displayer": "string"}
        return cc
"""


def test_mcp_add_rejects_getattr_and_writes_nothing(project: str):
    """``getattr`` is not in buckaroo's sandbox builtins, so ``style_column``
    raises NameError on every column at render time."""
    from tallyman_mcp.server import catalog_add_display_klass

    resp = catalog_add_display_klass("uses_getattr", USES_GETATTR)
    assert "error" in resp, resp
    assert "NameError" in resp["error"]
    assert "getattr" in resp["error"]
    assert "style_column" in resp["error"]
    assert not (display_dir(project) / "uses_getattr.py").exists()


def test_mcp_add_rejects_klass_that_raises_on_one_entry_column(project: str, orders_src: str):
    """The class fails only on a column the catalog's entries actually have,
    so the check has to style a sample of a real entry, not just synthetic
    columns. The error names the column."""
    from tallyman_mcp.server import catalog_add_display_klass

    resp = catalog_add_display_klass("region_bug", RAISES_ON_REGION)
    assert "error" in resp, resp
    assert "ZeroDivisionError" in resp["error"]
    assert "'region'" in resp["error"]
    assert not (display_dir(project) / "region_bug.py").exists()


def test_mcp_add_rejects_klass_reading_a_missing_stat(project: str):
    from tallyman_mcp.server import catalog_add_display_klass

    resp = catalog_add_display_klass("missing_stat", MISSING_STAT)
    assert "error" in resp, resp
    assert "KeyError" in resp["error"]
    assert "no_such_stat" in resp["error"]
    assert not (display_dir(project) / "missing_stat.py").exists()


# The two examples catalog_add_display_klass documents.
PINNED_ROW = """
class MainWithMostFreq(DefaultMainStyling):
    df_display_name = "main"
    requires_summary = ["histogram", "is_numeric", "dtype", "_type", "most_freq"]
    pinned_rows = [
        {'primary_key_val': 'dtype',      'displayer_args': {'displayer': 'obj'}},
        {'primary_key_val': 'histogram',  'displayer_args': {'displayer': 'histogram'}},
        {'primary_key_val': 'most_freq',  'displayer_args': {'displayer': 'inherit'}},
    ]
"""

PER_COLUMN = """
class Money(DefaultMainStyling):
    df_display_name = "main"

    @classmethod
    def style_column(cls, col, column_metadata):
        cc = super().style_column(col, column_metadata)
        name = column_metadata.get("orig_col_name", col)
        if name == "year":
            cc["displayer_args"] = {"displayer": "string"}
        elif name == "price":
            cc["displayer_args"] = {"displayer": "float", "min_fraction_digits": 1,
                                    "max_fraction_digits": 1, "prefix": "$", "suffix": "M"}
        return cc
"""


def test_mcp_add_accepts_the_documented_examples(project: str, orders_src: str):
    from tallyman_mcp.server import catalog_add_display_klass

    for name, source in (("main_with_most_freq", PINNED_ROW), ("money", PER_COLUMN)):
        resp = catalog_add_display_klass(name, source)
        assert "error" not in resp, resp
        assert (display_dir(project) / f"{name}.py").exists()


def test_validate_without_a_project_styles_the_synthetic_sample():
    """With no project there are no entries to sample; the synthetic columns still catch a sandbox NameError."""
    from tallyman_core.display_klasses import DisplayKlassError, validate_display_klass_source

    validate_display_klass_source("money", PER_COLUMN)
    with pytest.raises(DisplayKlassError, match="getattr"):
        validate_display_klass_source("uses_getattr", USES_GETATTR)


READS_PROJECT_STAT = """
class MainShowsPresent(DefaultMainStyling):
    df_display_name = "main"

    @classmethod
    def style_column(cls, col, column_metadata):
        cc = super().style_column(col, column_metadata)
        if column_metadata["n_present"] > 0:
            cc["displayer_args"] = {"displayer": "string"}
        return cc
"""


def test_mcp_add_accepts_klass_reading_a_project_stat(project: str, orders_src: str):
    """The check computes the project's stats the way buckaroo's session does, so a class may read one from
    column_metadata. The stat is added through the same tool an agent uses, so it lands where buckaroo looks."""
    from tallyman_mcp.server import catalog_add_display_klass, catalog_add_summary_stat

    added = catalog_add_summary_stat("n_present", "def compute(col):\n    return col.count()\n")
    assert "error" not in added, added

    resp = catalog_add_display_klass("shows_present", READS_PROJECT_STAT)
    assert "error" not in resp, resp
    assert (display_dir(project) / "shows_present.py").exists()
