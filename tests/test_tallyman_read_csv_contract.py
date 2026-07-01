"""Contract tests for tallyman_read_csv — the intelligent-import redesign.

ADR plans/adr-intelligent-csv-import.md. Covers the #137-review cluster:

- #144 — `time` / `decimal` / non-nullable ibis types in `_polars_overrides`.
- #145 — timestamp tz + sub-microsecond precision preserved, not flattened.
- #141 — schema spec: dict binds by name (mismatch raises a listed error),
  tuple-of-tuples binds by position, `&rest` closes a partial spec, a non-total
  spec raises.
- #143 — no-schema inference escalates past the default window; an explicit
  pinned type that can't parse raises with a paste-ready schema suggestion.

These are RED on the pre-fix tree (seen failing on CI), green after the fixes.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_core import data_dir
from tallyman_mcp.server import catalog_create


# --------------------------------------------------------------------------- #
# #144 / #145 — type coverage, unit tests on the ibis->polars dtype mapping
# --------------------------------------------------------------------------- #
def test_polars_overrides_covers_time_decimal_and_nonnull():
    """#144: time / decimal(p,s) / non-nullable types must map, not raise."""
    import polars as pl
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import _polars_overrides

    schema = ibis.schema(
        {
            "t": "time",
            "d": "decimal(10, 2)",
            "n": ibis.dtype("int64").copy(nullable=False),
        }
    )
    ov = _polars_overrides(schema)
    assert ov["t"] == pl.Time
    assert ov["d"] == pl.Float64  # decimal -> float64 for now; exact decimal deferred to UI (#150)
    assert ov["n"] == pl.Int64


def test_polars_overrides_preserves_timestamp_tz_and_precision():
    """#145: tz and sub-microsecond precision survive instead of flattening to naive us."""
    import polars as pl
    import xorq.vendor.ibis as ibis

    from tallyman_xorq.io import _polars_overrides

    schema = ibis.schema(
        {
            "naive": "timestamp",
            "utc": "timestamp('UTC')",
            "nanos": "timestamp(9)",
            "millis": "timestamp(3)",
        }
    )
    ov = _polars_overrides(schema)
    # Must be concrete Datetime *instances*: the pre-fix code returns the bare
    # `pl.Datetime` class (which compares == to any instance, so equality can't
    # catch the flattening) — assert the unit/tz directly.
    for k in ("naive", "utc", "nanos", "millis"):
        assert isinstance(ov[k], pl.Datetime), f"{k}: {ov[k]!r} is not a Datetime instance"
    assert ov["naive"].time_zone is None and ov["naive"].time_unit == "us"
    assert ov["utc"].time_zone == "UTC"
    assert ov["nanos"].time_unit == "ns"
    assert ov["millis"].time_unit == "ms"


# --------------------------------------------------------------------------- #
# #141 — schema spec contract
# --------------------------------------------------------------------------- #
def _types_of(res: dict) -> dict[str, str]:
    return {f["name"]: f["type"] for f in res["schema"]["fields"]}


def _build(name: str, csv: Path, schema_literal: str) -> dict:
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(csv)!r}, schema={schema_literal})
"""
    return catalog_create(name, code)


def test_schema_as_plain_dict_binds_by_name(project, monkeypatch):
    """A plain dict (not just an ibis schema) binds by header name."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "dict.csv"
    p.write_text("id,name\n1,alice\n2,bob\n")
    res = _build("dict_named", p, '{"id": "int64", "name": "string"}')
    assert "error" not in res, res
    types = _types_of(res)
    assert types["id"] == "int64"
    assert types["name"] == "string"


def test_schema_name_not_in_header_raises_listed_suggestion(project, monkeypatch):
    """#141: a by-name schema whose name is absent from the header raises a
    listed, actionable error steering toward the positional tuple form."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "yf.csv"
    p.write_text("Price,Close\n2020-01-01,10.0\n2020-01-02,11.0\n")
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tallyman_read_csv
schema = ibis.schema({{"Date": "date", "Close": "float64"}})
expr = tallyman_read_csv({str(p)!r}, schema=schema)
"""
    res = catalog_create("yf_named", code)
    assert "error" in res
    err = res["error"]
    assert "Price" in err  # the actual header is listed
    assert "&rest" in err  # steered toward the positional tuple form (tallyman phrasing)


def test_tuple_schema_renames_by_position(project, monkeypatch):
    """#141: tuple-of-tuples binds by position — the yfinance Price->Date rename."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "yf2.csv"
    p.write_text("Price,Close\n2020-01-01,10.0\n2020-01-02,11.0\n")
    res = _build("yf_pos", p, '(("Date", "date"), ("Close", "float64"))')
    assert "error" not in res, res
    types = _types_of(res)
    assert "Date" in types and "Price" not in types  # col 0 renamed positionally
    assert types["Date"].startswith("date")  # date32[day] in the serialized schema


def test_tuple_schema_rest_infers_tail(project, monkeypatch):
    """#141: ('&rest', 'infer') keeps the tail's names and infers their types."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "rest.csv"
    p.write_text("a,b,c\n2020-01-01,5,xy\n2020-01-02,6,zz\n")
    res = _build("rest_tail", p, '(("when", "date"), ("&rest", "infer"))')
    assert "error" not in res, res
    types = _types_of(res)
    assert types["when"].startswith("date")  # col 0 renamed + pinned (date32[day])
    assert types["b"] == "int64"  # tail kept name, inferred int
    assert types["c"] == "string"  # tail kept name, inferred string


def test_dict_schema_rest_infers_others(project, monkeypatch):
    """#141: dict '&rest' pins some columns by name and infers the rest."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "drest.csv"
    p.write_text("id,amount\n01,5\n02,6\n")
    res = _build("drest", p, '{"id": "string", "&rest": "infer"}')
    assert "error" not in res, res
    types = _types_of(res)
    assert types["id"] == "string"  # pinned string keeps leading zeros
    assert types["amount"] == "int64"  # inferred


def test_partial_spec_without_rest_raises(project, monkeypatch):
    """#141: a partial spec with no wildcard is non-total and must raise."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    p = data_dir(project) / "partial.csv"
    p.write_text("a,b,c\n1,2,3\n4,5,6\n")
    res = _build("partial", p, '(("a", "int64"), ("b", "int64"))')
    assert "error" in res
    err = res["error"]
    assert "&rest" in err or "total" in err.lower()


# --------------------------------------------------------------------------- #
# #143 — escalating inference + suggested-schema feedback
# --------------------------------------------------------------------------- #
@pytest.fixture
def late_poison_csv(project: str) -> Path:
    """Column ``v`` is integer for the first 12000 rows then ``N/A`` — past the
    default infer window, so a fixed-window infer aborts at parse time.
    Escalation must widen to whole-file and fall ``v`` back to string."""
    p = data_dir(project) / "poison.csv"
    lines = ["k,v"]
    lines += [f"{i},{i * 2}" for i in range(12000)]
    lines.append("12000,N/A")
    lines += [f"{i},{i * 2}" for i in range(12001, 12010)]
    p.write_text("\n".join(lines) + "\n")
    return p


def test_no_schema_escalates_past_default_window(project, late_poison_csv, monkeypatch):
    """#143: no-schema inference escalates the window and resolves the messy tail."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
from tallyman_xorq.io import tallyman_read_csv
expr = tallyman_read_csv({str(late_poison_csv)!r})
"""
    res = catalog_create("escal", code)
    assert "error" not in res, res
    types = _types_of(res)
    assert types["v"] == "string"  # whole-file infer fell it back to string


def test_explicit_type_failure_suggests_schema(project, late_poison_csv, monkeypatch):
    """#143: a pinned int64 that can't parse raises with a paste-ready suggestion."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    code = f"""
import xorq.vendor.ibis as ibis
from tallyman_xorq.io import tallyman_read_csv
schema = ibis.schema({{"k": "int64", "v": "int64"}})
expr = tallyman_read_csv({str(late_poison_csv)!r}, schema=schema)
"""
    res = catalog_create("explicit_fail", code)
    assert "error" in res
    err = res["error"]
    # tallyman emits a paste-ready suggestion (absent pre-fix; "schema=" alone
    # would false-match the recipe code echoed in the traceback).
    assert "suggested schema" in err.lower()
    assert "'v'" in err or '"v"' in err  # names the failing column
    assert "string" in err  # suggests string for the unparseable column


# --------------------------------------------------------------------------- #
# #148-review — reserved scan kwargs must not collide with the internal ones
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad_kwarg", ["infer_schema_length", "schema_overrides"])
def test_reserved_scan_kwarg_raises_clear_error(project, monkeypatch, bad_kwarg):
    """``infer_schema_length`` and ``schema_overrides`` are managed internally — the
    former by the escalation ladder, the latter by the ``schema=`` parameter — so
    forwarding one as a ``**kwargs`` reader option must raise a clear ValueError, not
    the raw polars ``TypeError: got multiple values for keyword argument`` (or, for
    ``schema_overrides`` with no schema, silently bypass the schema system)."""
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    from tallyman_xorq.io import tallyman_read_csv

    p = data_dir(project) / "kw.csv"
    p.write_text("a,b\n1,x\n2,y\n")
    kwargs = {bad_kwarg: 1000 if bad_kwarg == "infer_schema_length" else {"a": "int64"}}
    with pytest.raises(ValueError, match="managed internally"):
        tallyman_read_csv(str(p), **kwargs)
