"""Run the dastardly CSV corpus through ``tallyman_read_csv``.

The CSV sibling of buckaroo's ``test_widget_weird_types.py``: take each
pathological fixture from :mod:`dastardly_csv_library` and assert how
``tallyman_read_csv`` handles it. Three layers:

- :class:`TestCorpusIsDeterministic` — the corpus-level guard. *Every* case must
  reach a deterministic outcome: a parsed schema, or a ``ValueError`` carrying
  the ``tallyman_read_csv:`` contract prefix. Never an unhandled exception, a
  polars panic, or a silent crash.
- :class:`TestDocumentedBehavior` — characterization / regression guards pinning
  tallyman's *current* behavior on each pathology, with the per-tool contrast in
  each docstring (drawn from ``docs/research/csv-importers/``).
- :class:`TestKnownGaps` — ``xfail`` tests that assert the behavior the ADR /
  edge-cases.md *wants* but tallyman does not yet implement. These are the
  ready-made red tests for the holistic edge-case pass (#142 chief among them).

All citations: ``docs/research/csv-importers/edge-cases.md`` (case numbers) and
``plans/adr-intelligent-csv-import.md`` (decisions D2/D6).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from tallyman_core import data_dir
from tallyman_xorq.io import tallyman_read_csv
from tests import dastardly_csv_library as ddd


@dataclass
class ReadOutcome:
    """The deterministic result of feeding one dastardly CSV to the reader."""

    ok: bool
    schema: dict[str, str] | None  # name -> ibis type str (sans original_row_order)
    nrows: int | None
    error: str | None


def read_dastardly(project: str, builder, *, count: bool = False, **reader_kwargs) -> ReadOutcome:
    """Write ``builder()``'s bytes under the project data dir and read them.

    Returns a :class:`ReadOutcome` for either path — a successful parse (schema,
    optional row count) or a raised ``ValueError`` (its message). Any *other*
    exception type propagates: the corpus contract is parse-or-clean-ValueError,
    so a different exception is a real failure, not an outcome.
    """
    p: Path = data_dir(project) / f"{builder.__name__}.csv"
    p.write_bytes(builder())
    try:
        expr = tallyman_read_csv(str(p), **reader_kwargs)
    except ValueError as exc:
        return ReadOutcome(ok=False, schema=None, nrows=None, error=str(exc))
    schema = {k: str(v) for k, v in expr.schema().items() if k != "original_row_order"}
    nrows = int(expr.count().execute()) if count else None
    return ReadOutcome(ok=True, schema=schema, nrows=nrows, error=None)


# --------------------------------------------------------------------------- #
# Corpus-level guard: every case is handled deterministically.
# --------------------------------------------------------------------------- #
class TestCorpusIsDeterministic:
    @pytest.mark.parametrize("builder", ddd.ALL_CASES, ids=lambda f: f.__name__)
    def test_parse_or_clean_valueerror(self, project, builder):
        """Each pathology yields a schema, or a ValueError with the contract
        prefix — never an unhandled exception or silent crash."""
        out = read_dastardly(project, builder)
        if out.ok:
            assert out.schema, f"{builder.__name__}: parsed but empty schema"
        else:
            assert "tallyman_read_csv" in out.error or "csv" in out.error.lower(), (
                f"{builder.__name__}: error lacks the contract prefix: {out.error!r}"
            )


# --------------------------------------------------------------------------- #
# Characterization: tallyman's current behavior, pathology by pathology.
# --------------------------------------------------------------------------- #
class TestDocumentedBehavior:
    # --- 1. embedded newline in a quoted field -------------------------------
    def test_embedded_newline_is_data_not_a_row(self, project):
        """Case 1: a quoted ``\\n`` stays inside the field — 2 rows, not 3."""
        out = read_dastardly(project, ddd.csv_embedded_newline, count=True)
        assert out.nrows == 2
        assert out.schema == {"note": "string", "n": "int64"}

    def test_embedded_newline_in_header_kept_in_name(self, project):
        """Case 1: a quoted newline in a header cell stays in the column name."""
        out = read_dastardly(project, ddd.csv_embedded_newline_in_header, count=True)
        assert out.nrows == 2
        assert "first\nsecond" in out.schema

    # --- 2. BOM --------------------------------------------------------------
    def test_utf8_bom_stripped_from_header(self, project):
        """Case 2: a UTF-8 BOM is stripped so by-name binding sees ``id``, not ``﻿id``."""
        out = read_dastardly(project, ddd.csv_bom_utf8)
        assert "id" in out.schema and "﻿id" not in out.schema

    def test_utf8_bom_stripped_before_quoted_header(self, project):
        """Case 2: BOM stripped even ahead of a quoted first header cell."""
        out = read_dastardly(project, ddd.csv_bom_on_quoted_header)
        assert "id" in out.schema

    # --- 3. encoding ---------------------------------------------------------
    def test_latin1_under_default_utf8_fails_loud(self, project):
        """Case 3: invalid UTF-8 bytes raise at parse, never mojibake silently."""
        out = read_dastardly(project, ddd.csv_latin1)
        assert not out.ok and "utf-8" in out.error.lower()

    def test_latin1_encoding_kwarg_is_restricted_to_utf8_variants(self, project):
        """Case 3: polars scan only supports utf8/utf8-lossy — ``encoding='latin1'``
        is rejected with a clear message (use utf8-lossy or pre-convert)."""
        out = read_dastardly(project, ddd.csv_latin1, encoding="latin1")
        assert not out.ok and "utf8-lossy" in out.error

    # --- 4. decimal comma ----------------------------------------------------
    def test_decimal_comma_needs_separator_and_flag(self, project):
        """Case 4: with the default ``,`` delimiter, ``1,5`` splits into extra
        fields and raises; ``separator=';'`` + ``decimal_comma=True`` parses it
        as a float."""
        default = read_dastardly(project, ddd.csv_decimal_comma)
        assert not default.ok and "more fields" in default.error
        tamed = read_dastardly(project, ddd.csv_decimal_comma, separator=";", decimal_comma=True)
        assert tamed.ok and tamed.schema["amount"] == "float64"

    # --- 5. thousands separator ----------------------------------------------
    def test_thousands_grouped_numbers_stay_string(self, project):
        """Case 5: polars has no thousands option, so ``"1,234"`` stays string
        (the research-flagged gap — needs a custom pass to become numeric)."""
        out = read_dastardly(project, ddd.csv_thousands_separator)
        assert out.schema == {"qty": "string", "label": "string"}

    # --- 6. mixed type past the window ---------------------------------------
    def test_mixed_type_late_escalates_to_string(self, project):
        """Case 6 / #143: a violator past the 10k window makes no-schema infer
        escalate to whole-file and fall the column back to ``string``."""
        out = read_dastardly(project, ddd.csv_mixed_type_late, count=True)
        assert out.nrows == 12010
        assert out.schema["v"] == "string"

    # --- 7. all-null column --------------------------------------------------
    def test_all_null_column_infers_string(self, project):
        """Case 7: a column with no type evidence resolves to ``string``."""
        out = read_dastardly(project, ddd.csv_all_null_column)
        assert out.schema["b"] == "string"

    # --- 8. leading zeros (deliberate, per ADR D2) ---------------------------
    def test_leading_zeros_become_int_by_design(self, project):
        """Case 8: the parse layer does NO semantic cleaning (ADR D2) — ``01234``
        infers ``int64`` and loses the zero. Keeping it means pinning ``string``
        in the schema; this test pins the deliberate default."""
        out = read_dastardly(project, ddd.csv_leading_zeros)
        assert out.schema["zip"] == "int64"

    def test_leading_zeros_preserved_when_pinned_string(self, project):
        """Case 8: pinning the column to ``string`` keeps the leading zero."""
        out = read_dastardly(project, ddd.csv_leading_zeros, schema={"zip": "string", "&rest": "infer"})
        assert out.ok and out.schema["zip"] == "string"

    # --- 9. scientific notation ----------------------------------------------
    def test_scientific_notation_widens_to_float(self, project):
        """Case 9: an int-looking column holding ``1e5`` infers ``float64``,
        never a silently-truncated int and never a date."""
        out = read_dastardly(project, ddd.csv_scientific_notation)
        assert out.schema["energy"] == "float64"

    # --- 10. duplicate headers -----------------------------------------------
    def test_duplicate_headers_deduped_deterministically(self, project):
        """Case 10: polars dedups the second ``id`` to ``id_duplicated_0`` — a
        stable, documented rename (tallyman binds by name, so this matters)."""
        out = read_dastardly(project, ddd.csv_duplicate_headers)
        assert set(out.schema) == {"id", "id_duplicated_0"}

    # --- 12. headerless all-string -------------------------------------------
    def test_all_string_file_assumes_header(self, project):
        """Case 12: when every column is string the header can't be
        type-distinguished, so row 0 is assumed to be the header (2 data rows)."""
        out = read_dastardly(project, ddd.csv_headerless_all_string, count=True)
        assert out.nrows == 2
        assert set(out.schema) == {"alpha", "beta"}

    # --- 13. trailing delimiter ----------------------------------------------
    def test_trailing_delimiter_makes_ghost_column(self, project):
        """Case 13: a uniform trailing comma yields an extra empty-named column."""
        out = read_dastardly(project, ddd.csv_trailing_delimiter)
        assert "" in out.schema

    # --- 15. ragged long (loud) ----------------------------------------------
    def test_ragged_long_raises(self, project):
        """Case 15: a too-many-fields row raises ``found more fields than
        defined`` (the loud half of the short/long asymmetry)."""
        out = read_dastardly(project, ddd.csv_ragged_long)
        assert not out.ok and "more fields" in out.error

    # --- 17. whitespace flips inferred type ----------------------------------
    def test_leading_whitespace_changes_inferred_type(self, project):
        """Case 17: polars does not strip surrounding whitespace before
        inference, so `` 1`` stays ``string`` while a clean ``1`` is ``int64``."""
        out = read_dastardly(project, ddd.csv_leading_whitespace)
        assert out.schema["clean"] == "int64"
        assert out.schema["spaced"] == "string"

    # --- 18. mixed newlines ---------------------------------------------------
    def test_mixed_newlines_parsed_cleanly(self, project):
        """Case 18: a file mixing ``\\n`` and ``\\r\\n`` parses to clean ints —
        no trailing ``\\r`` riding along on the last field."""
        out = read_dastardly(project, ddd.csv_mixed_newlines, count=True)
        assert out.nrows == 3
        assert out.schema == {"a": "int64", "b": "int64"}

    # --- 20. over-long line ---------------------------------------------------
    def test_over_long_line_parses(self, project):
        """Case 20: polars has no ~2 MB line cap, so a multi-megabyte field
        parses rather than raising ``LINE SIZE OVER MAXIMUM`` (DuckDB's bound).
        Pins the absence of a line-size guard so a future cap is a deliberate
        change, not a surprise."""
        out = read_dastardly(project, ddd.csv_over_long_line)
        assert out.ok and set(out.schema) == {"a", "b"}

    # --- 21. ambiguous / impossible dates ------------------------------------
    def test_ambiguous_date_stays_string(self, project):
        """Case 21: dates are opt-in — ``1/6/2000`` and ``32/32/2019`` stay
        ``string`` rather than being silently swapped or turned into a wrong
        date."""
        out = read_dastardly(project, ddd.csv_ambiguous_date)
        assert out.schema["d"] == "string"

    # --- 23. naive timestamp + tz-aware schema pin (#145) --------------------
    def test_naive_ts_pinned_utc_attaches_not_converts(self, project):
        """Case 23 / #145: polars *attaches* UTC to a naive timestamp string
        when schema_overrides pins Datetime(us, 'UTC') — it reinterprets the
        value as UTC in place (no shift, no error). Pinning ``timestamp('UTC')``
        on a naive-string column is a declaration, not a conversion request."""
        out = read_dastardly(
            project,
            ddd.csv_naive_ts_pinned_utc,
            count=True,
            schema={"ts": "timestamp('UTC')", "&rest": "infer"},
        )
        assert out.ok, f"unexpected error: {out.error}"
        # The ibis type round-trips with the precision polars inferred from the
        # sub-second digits in the data (6 digits → scale 6 = us).
        assert out.schema["ts"] == "timestamp('UTC', 6)", out.schema
        assert out.nrows == 2


# --------------------------------------------------------------------------- #
# Known gaps: the behavior the ADR / edge-cases.md wants but tallyman lacks.
# These xfail today and are the red tests for the holistic edge-case pass.
# --------------------------------------------------------------------------- #
class TestKnownGaps:
    @pytest.mark.xfail(
        strict=True,
        reason="#142: polars scan_csv silently null-fills short rows; tallyman "
        "should fail loud (or reject-log). Fix lands via a pyarrow.csv validation "
        "gate — see edge-cases.md case 14.",
    )
    def test_ragged_short_should_not_silently_nullfill(self, project):
        """Case 14 / #142: ``a,b\\n1,2\\n3\\n4,5`` must NOT silently bake
        ``(3, null)``. Desired: a clean ValueError (or a routed reject row)."""
        out = read_dastardly(project, ddd.csv_ragged_short, count=True)
        assert not out.ok, f"short row silently parsed to {out.nrows} rows: {out.schema}"

    @pytest.mark.xfail(
        strict=False,
        reason="edge-cases.md case 19 (suggestion, not yet decided): a NUL byte "
        "should be a hard structural error; today it is absorbed into a string.",
    )
    def test_nul_byte_should_be_structural_error(self, project):
        """Case 19: a ``\\x00`` mid-field should raise, not silently ride along
        inside a string value."""
        out = read_dastardly(project, ddd.csv_nul_byte)
        assert not out.ok

    @pytest.mark.xfail(
        strict=False,
        reason="ADR D6 says infer mode always resolves (mixed → string). An "
        "int64-overflow value in a pure-integer column is not a type *mix*, so "
        "the merge-to-string fallback never fires and the cast raises instead of "
        "resolving. edge-cases.md case 22.",
    )
    def test_infer_mode_resolves_int_overflow(self, project):
        """Case 22: a no-schema read of a column with a past-int64 value should
        resolve (to string, or Int128 where the build supports it), not raise."""
        out = read_dastardly(project, ddd.csv_integer_overflow)
        assert out.ok
