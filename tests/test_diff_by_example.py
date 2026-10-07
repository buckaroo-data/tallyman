"""The diff of two five-row tables, small enough to check by eye.

Nothing here starts Buckaroo or a catalog. Each test builds the compare table the diff grid shows
(``tallyman_companion.diff.build_compare_expr``), runs it, and compares the result with a table written out below.

The two tables, joined on ``a_primary``::

    before                              after
    a_primary  b_int  c_float           a_primary  c_float  d_string
    1          100    1.5               1          1.5      one
    2          200    2.5               2          5.0      two
    3          300    3.5               3          3.0      three
    10         1000   10.5              20         20.5     twenty
    11         1100   11.5              21         21.5     twenty-one

* Keys 1, 2 and 3 are in both. Key 1 is identical, keys 2 and 3 differ in ``c_float`` (2.5 to 5.0, 3.5 to 3.0).
* Keys 10 and 11 are only in ``before``. Keys 20 and 21 are only in ``after``.
* ``b_int`` is only in ``before`` and ``d_string`` is only in ``after``, so ``c_float`` is the one column both sides
  have besides the key, and the only one whose values can be compared.
"""

import pandas as pd
import pytest
import xorq.api as xo

from tallyman_companion.diff import build_compare_expr
from tallyman_xorq.diff import schema_diff

BEFORE = pd.DataFrame(
    {
        "a_primary": [1, 2, 3, 10, 11],
        "b_int": [100, 200, 300, 1000, 1100],
        "c_float": [1.5, 2.5, 3.5, 10.5, 11.5],
    }
)

AFTER = pd.DataFrame(
    {
        "a_primary": [1, 2, 3, 20, 21],
        "c_float": [1.5, 5.0, 3.0, 20.5, 21.5],
        "d_string": ["one", "two", "three", "twenty", "twenty-one"],
    }
)

KEYS = ["a_primary"]

# ``membership`` says which side a row came from.
ONLY_BEFORE, ONLY_AFTER, BOTH = 1, 2, 3

# ``c_float_eq`` is the row's membership, plus 4 if ``c_float`` is equal on both sides. It stays the bare membership
# for a row that is on one side only, because there is nothing to be equal to.
EQUAL, DIFFERENT = 4, 0

NA = float("nan")


def compare_table() -> pd.DataFrame:
    """The compare table of BEFORE and AFTER as a pandas frame, in key order."""
    expr, _overrides = build_compare_expr(xo.memtable(BEFORE), xo.memtable(AFTER), KEYS)
    return expr.execute().sort_values("a_primary").reset_index(drop=True)


def test_the_diff_of_two_small_tables():
    """The whole compare table, row by row. Read the columns left to right:

    * ``a_primary``: the key, taken from whichever side has the row.
    * ``b_int``, ``c_float``: the before values. ``c_float`` is the "before" of the column both sides have.
    * ``c_float_v2``: the after value of ``c_float``.
    * ``c_float_pct_delta``: (after - before) / |before|, empty when there is no before or after.
    * ``c_float_abs_delta``: after - before.
    * ``membership``, ``c_float_eq``: see the constants above.
    """
    # fmt: off
    expected = pd.DataFrame(
        columns=[
            "a_primary", "b_int", "c_float", "c_float_v2",
            "c_float_pct_delta", "c_float_abs_delta", "membership", "c_float_eq",
        ],
        data=[
            # in both, identical
            [1,    100,  1.5,  1.5,  0.0,     0.0,   BOTH,        BOTH + EQUAL],
            # in both, c_float went from 2.5 to 5.0: up 2.5, which is +100%
            [2,    200,  2.5,  5.0,  1.0,     2.5,   BOTH,        BOTH + DIFFERENT],
            # in both, c_float went from 3.5 to 3.0: down 0.5, which is -1/7
            [3,    300,  3.5,  3.0,  -1 / 7,  -0.5,  BOTH,        BOTH + DIFFERENT],
            # only in before: no after value, so no delta
            [10,   1000, 10.5, NA,   NA,      NA,    ONLY_BEFORE, ONLY_BEFORE],
            [11,   1100, 11.5, NA,   NA,      NA,    ONLY_BEFORE, ONLY_BEFORE],
            # only in after: no before value (and no b_int), so no delta
            [20,   NA,   NA,   20.5, NA,      NA,    ONLY_AFTER,  ONLY_AFTER],
            [21,   NA,   NA,   21.5, NA,      NA,    ONLY_AFTER,  ONLY_AFTER],
        ],
    )
    # fmt: on

    # dtypes differ only because a column with a hole in it comes back as float (b_int) and the codes come back as int8
    pd.testing.assert_frame_equal(compare_table(), expected, check_dtype=False)


def test_membership_says_which_side_each_row_came_from():
    table = compare_table()

    keys_by_membership = {m: sorted(rows["a_primary"]) for m, rows in table.groupby("membership")}

    assert keys_by_membership == {
        ONLY_BEFORE: [10, 11],
        ONLY_AFTER: [20, 21],
        BOTH: [1, 2, 3],
    }


def test_of_the_three_rows_in_both_one_matches_and_two_differ():
    table = compare_table()
    in_both = table[table["membership"] == BOTH].set_index("a_primary")

    equal_keys = list(in_both.index[in_both["c_float_eq"] == BOTH + EQUAL])
    different_keys = list(in_both.index[in_both["c_float_eq"] == BOTH + DIFFERENT])

    assert equal_keys == [1]
    assert different_keys == [2, 3]
    assert in_both.loc[2, "c_float_abs_delta"] == 2.5
    assert in_both.loc[2, "c_float_pct_delta"] == pytest.approx(1.0)
    assert in_both.loc[3, "c_float_abs_delta"] == -0.5
    assert in_both.loc[3, "c_float_pct_delta"] == pytest.approx(-1 / 7)


def test_schema_diff_names_the_column_that_went_and_the_column_that_came():
    # The schema documents an entry writes next to its data: column names and types, and a row count.
    before_schema = {
        "fields": [
            {"name": "a_primary", "type": "int64"},
            {"name": "b_int", "type": "int64"},
            {"name": "c_float", "type": "float64"},
        ],
        "row_count": 5,
    }
    after_schema = {
        "fields": [
            {"name": "a_primary", "type": "int64"},
            {"name": "c_float", "type": "float64"},
            {"name": "d_string", "type": "string"},
        ],
        "row_count": 5,
    }

    assert schema_diff(before_schema, after_schema) == {
        "added": ["d_string"],
        "removed": ["b_int"],
        "changed_type": [],
        "row_count": {"before": 5, "after": 5},
    }


@pytest.mark.xfail(
    strict=True,
    reason="#275: build_compare_expr loops over the before side's columns, so a column only after has is dropped",
)
def test_a_column_only_the_after_table_has_appears_in_the_diff():
    # Whatever name the fix gives it, ``d_string`` should be in the compare table, with the three values
    # "one", "two", "three" on the rows in both and "twenty", "twenty-one" on the rows only in after.
    table = compare_table()

    d_string_columns = [c for c in table.columns if c.startswith("d_string")]

    assert d_string_columns, f"no d_string column in {list(table.columns)}"
    shown = table.set_index("a_primary")[d_string_columns[0]]
    assert shown.loc[[1, 2, 3, 20, 21]].tolist() == ["one", "two", "three", "twenty", "twenty-one"]
