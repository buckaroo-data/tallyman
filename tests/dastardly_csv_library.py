"""The dastardly CSV dataset.

The weirdest CSVs that cause trouble frequently — the pathology corpus that
tallyman's ``tallyman_read_csv`` must handle deterministically (parse with a
known schema, or raise a clean ``ValueError`` — never silently corrupt, never
crash).

This is the CSV sibling of buckaroo's ``ddd_library.py`` ("the dastardly
dataframe dataset"): one builder per pathology, each docstring naming the
hazard, how the three reference importers (DuckDB / Polars / Pandas) behave, and
the behavior tallyman wants. Cases and citations are drawn from
``docs/research/csv-importers/edge-cases.md`` (the seed corpus) — case numbers
below match that file.

Every builder returns the raw CSV **bytes** (not str), because several
pathologies live below the text layer: BOM bytes, NUL bytes, non-UTF-8
encodings. The accompanying tests in ``test_dastardly_csv.py`` write these
bytes to a project data dir and run them through ``tallyman_read_csv``.

A builder returns only the dastardly *data*; any reader option needed to tame a
case (``separator``, ``encoding``, ``decimal_comma``, a pinned ``schema``) is
supplied by the test, never baked into the fixture — same split buckaroo keeps
between ``ddd_library`` (the frames) and the widget tests (the config).
"""

from __future__ import annotations

from collections.abc import Callable


# --------------------------------------------------------------------------- #
# 1. Embedded newline inside a quoted field
# --------------------------------------------------------------------------- #
def csv_embedded_newline() -> bytes:
    """A real ``\\n`` inside a ``"..."``-quoted field (data cell and header cell).

    Why: the most common "row count is wrong" bug — an in-quote newline must be
    data, not a record terminator. DuckDB/Polars/Pandas all honor it when the
    quote char is set. tallyman (polars) should keep 2 data rows, not 3+.
    """
    return b'note,n\n"line one\nline two",1\n"single",2\n'


def csv_embedded_newline_in_header() -> bytes:
    """A quoted newline inside a *header* cell — the name spans two physical lines."""
    return b'"first\nsecond",val\nx,1\ny,2\n'


# --------------------------------------------------------------------------- #
# 2. Byte-order mark (BOM)
# --------------------------------------------------------------------------- #
def csv_bom_utf8() -> bytes:
    """A UTF-8 BOM (``EF BB BF``) prefixing the first header cell.

    Why: an unstripped BOM corrupts the first column name (``﻿id``),
    silently breaking by-name schema binding (#141). DuckDB/Polars/Pandas all
    strip a valid UTF-8 BOM; tallyman must too, so the header is ``id`` not
    ``﻿id``.
    """
    return b"\xef\xbb\xbfid,name\n1,alice\n2,bob\n"


def csv_bom_on_quoted_header() -> bytes:
    """A UTF-8 BOM in front of a *quoted* first header cell."""
    return b'\xef\xbb\xbf"id",name\n1,alice\n2,bob\n'


# --------------------------------------------------------------------------- #
# 3. Alternate / non-UTF-8 encoding
# --------------------------------------------------------------------------- #
def csv_latin1() -> bytes:
    """A latin-1 (ISO-8859-1) file with ``café`` / ``£`` — invalid as UTF-8.

    Why: encoding is undecidable from content; a wrong guess is mojibake or a
    mid-parse failure. Default strict-UTF-8 should fail loud; reading it needs an
    explicit ``encoding`` knob. The ``\xe9``/``\xa3`` bytes are not valid UTF-8.
    """
    return "name,price\ncafé,£5\nrésumé,£9\n".encode("latin-1")


# --------------------------------------------------------------------------- #
# 4. Decimal comma (European numerics)
# --------------------------------------------------------------------------- #
def csv_decimal_comma() -> bytes:
    """Floats written ``1,5`` with ``;`` as the field separator (European style).

    Why: with ``,`` as the decimal point you must NOT also use it as the
    delimiter. Read with ``separator=';'`` + ``decimal_comma=True``. Polars:
    ``decimal_comma`` switches ``FLOAT_RE``; without it ``1,5`` stays String.
    """
    return b"label;amount\na;1,5\nb;2,75\n"


# --------------------------------------------------------------------------- #
# 5. Thousands separator
# --------------------------------------------------------------------------- #
def csv_thousands_separator() -> bytes:
    """Grouped integers ``1,234`` — the grouping char must be stripped only from
    numeric columns, never from the string column.

    Why: Polars has **no** thousands option (the gap called out in the research),
    so grouped numbers stay String unless pre-cleaned. Pandas guards against
    stripping dots from non-numeric columns.
    """
    return b'qty,label\n"1,234","a,b"\n"5,678","c,d"\n'


# --------------------------------------------------------------------------- #
# 6. Mixed-type column past the inference window
# --------------------------------------------------------------------------- #
def csv_mixed_type_late(n_clean: int = 12_000) -> bytes:
    """Column ``v`` is integer for ``n_clean`` rows, then a string far past the
    default 100-row (and 10k) infer window.

    Why: *the* canonical importer bug — the schema is decided from the window
    and the violator is unseen until parse time. tallyman's escalation ladder
    (ADR D6) must widen to whole-file and fall ``v`` back to ``string`` on a
    no-schema read; an explicit ``int64`` must raise with a suggestion.
    """
    rows = [b"k,v"]
    rows += [f"{i},{i * 2}".encode() for i in range(n_clean)]
    rows.append(b"oops,not_a_number")
    rows += [f"{i},{i * 2}".encode() for i in range(n_clean + 1, n_clean + 10)]
    return b"\n".join(rows) + b"\n"


# --------------------------------------------------------------------------- #
# 7. All-null column
# --------------------------------------------------------------------------- #
def csv_all_null_column() -> bytes:
    """A column whose every value is empty — no evidence for a type.

    Why: whatever the importer guesses may conflict with a later widening.
    Polars resolves an all-null column to ``null``/``String``; with an explicit
    schema it is a non-issue.
    """
    return b"a,b\n1,\n2,\n3,\n"


# --------------------------------------------------------------------------- #
# 8. Leading-zero identifiers (ZIP / FIPS / account numbers)
# --------------------------------------------------------------------------- #
def csv_leading_zeros() -> bytes:
    """``01234`` ZIP-style identifiers that are NOT integers.

    Why: promoting to int destroys the leading zero irreversibly. DuckDB keeps
    leading-zero values as VARCHAR by a deliberate sniffer rule; Polars (and so
    tallyman by default) infers ``Int64`` and loses the zero. Per ADR D2 the
    parse layer does **no** semantic cleaning — keeping zeros means pinning the
    column to ``string`` in the schema.
    """
    return b"zip,city\n01234,boston\n02115,boston\n"


# --------------------------------------------------------------------------- #
# 9. Scientific notation and signed numbers
# --------------------------------------------------------------------------- #
def csv_scientific_notation() -> bytes:
    """An int-looking column that holds one ``1e5`` value.

    Why: scientific notation is float-shaped — an int-typed column that meets
    ``1e5`` fails the cast (the mixed-type trap in disguise). Must surface as a
    float (inference) or a value-error (explicit int schema), never silently
    truncate. Also a scientific number must never be coerced to a date.
    """
    return b"energy\n10\n20\n1e5\n30\n"


# --------------------------------------------------------------------------- #
# 10. Duplicate header names
# --------------------------------------------------------------------------- #
def csv_duplicate_headers() -> bytes:
    """Two columns both named ``id``.

    Why: by-name schema binding (#141) is ambiguous when names collide; a silent
    de-dup can bind the wrong column. Polars dedups to ``id``/``id_duplicated_0``;
    tallyman binds by name so the post-dedup names must be deterministic and
    surfaced.
    """
    return b"id,id\n1,2\n3,4\n"


# --------------------------------------------------------------------------- #
# 11. Empty / blank header cell
# --------------------------------------------------------------------------- #
def csv_empty_header_cell() -> bytes:
    """A header row with an empty middle cell (``a,,c``)."""
    return b"a,,c\n1,2,3\n4,5,6\n"


# --------------------------------------------------------------------------- #
# 12. Headerless file (all-string, header ambiguous)
# --------------------------------------------------------------------------- #
def csv_headerless_all_string() -> bytes:
    """No header; every field is a string, so row 0 can't be type-distinguished
    from a header.

    Why: an importer that assumes a header eats a real data row as names. DuckDB
    documents that an all-VARCHAR file is assumed to *have* a header; Polars
    (tallyman) assumes the same unless told ``has_header=False`` + names.
    """
    return b"alpha,beta\ngamma,delta\nepsilon,zeta\n"


# --------------------------------------------------------------------------- #
# 13. Trailing delimiter (ghost column)
# --------------------------------------------------------------------------- #
def csv_trailing_delimiter() -> bytes:
    """Every line ends with the delimiter (``a,b,``) → a spurious empty final
    column.

    Why: the ghost column shifts width and can be mistaken for an index column.
    Polars emits an extra all-null/empty-named column; the decision (keep vs
    drop) should be deterministic and surfaced.
    """
    return b"a,b,\n1,2,\n3,4,\n"


# --------------------------------------------------------------------------- #
# 14. Ragged SHORT row (too few fields) — tallyman #142
# --------------------------------------------------------------------------- #
def csv_ragged_short() -> bytes:
    """A row with fewer fields than the header: ``a,b\\n1,2\\n3\\n4,5``.

    Why: **this is tallyman #142.** The old datafusion path raised; the polars
    ``scan_csv`` path silently null-fills, baking ``(3, null)`` into the
    canonical snapshot with no signal. Silent-by-direction is the trap — DuckDB
    raises ``MISSING COLUMNS`` by default. Empirically polars cannot be made to
    raise on a short row (pola-rs/polars#10585); the fail-loud detector is a
    ``pyarrow.csv`` validation gate (edge-cases.md, 2026-06-26).
    """
    return b"a,b\n1,2\n3\n4,5\n"


# --------------------------------------------------------------------------- #
# 15. Ragged LONG row (too many fields) — #142 sibling
# --------------------------------------------------------------------------- #
def csv_ragged_long() -> bytes:
    """A row with more fields than the header: ``a,b\\n1,2\\n3,4,5\\n6,7``.

    Why: the asymmetric twin of the short row — most tools are *loud* here and
    *silent* on short rows. Polars raises ``found more fields than defined``
    unless ``truncate_ragged_lines=True``; tallyman surfaces that as a
    ``ValueError``. The asymmetry between short and long is what to eliminate.
    """
    return b"a,b\n1,2\n3,4,5\n6,7\n"


# --------------------------------------------------------------------------- #
# 16. Quoted-empty vs unquoted-empty as null
# --------------------------------------------------------------------------- #
def csv_quoted_vs_bare_empty() -> bytes:
    """``""`` (quoted empty) vs a bare empty field — should one be null and the
    other empty string?

    Why: the null-vs-empty distinction changes downstream joins/aggregations and
    the tools disagree. DuckDB ``allow_quoted_nulls`` / ``force_not_null``;
    Polars ``missing_utf8_is_empty_string``; Pandas treats bare empty as NA.
    """
    return b'a,b\n"",x\n,y\nz,w\n'


# --------------------------------------------------------------------------- #
# 17. Whitespace sensitivity
# --------------------------------------------------------------------------- #
def csv_leading_whitespace() -> bytes:
    """Leading spaces after the delimiter (`` 1, 2``) that can flip inferred type.

    Why: Polars does **not** strip surrounding whitespace before inference, so
    `` 1`` may not match ``INTEGER_RE`` and the column stays String. Pandas has
    opt-in ``skipinitialspace``. The fixture pairs a clean numeric column with a
    space-prefixed one so the type difference is visible.
    """
    return b"clean,spaced\n1, 1\n2, 2\n3, 3\n"


# --------------------------------------------------------------------------- #
# 18. Windows vs Unix newlines (and mixed)
# --------------------------------------------------------------------------- #
def csv_mixed_newlines() -> bytes:
    """One file mixing ``\\n`` and ``\\r\\n`` line terminators.

    Why: a trailing ``\\r`` can ride along on the last field's value; mixed
    newlines can confuse record boundaries. Default behavior should normalize
    CRLF/CR and strip the trailing ``\\r`` so values don't carry it.
    """
    return b"a,b\r\n1,2\n3,4\r\n5,6\n"


# --------------------------------------------------------------------------- #
# 19. NUL bytes and other control bytes mid-field
# --------------------------------------------------------------------------- #
def csv_nul_byte() -> bytes:
    """A ``\\x00`` byte embedded in a data field.

    Why: NUL bytes corrupt C string handling and signal a binary file mis-fed as
    CSV — a classic fuzzer crash. Pandas raises ``"NULL byte detected"``; DuckDB
    has dedicated null-byte tests. edge-cases.md suggests tallyman treat a NUL as
    a hard structural error rather than silently absorbing it into a string.
    """
    return b"a,b\n1,2\n3,4\x005\n"


# --------------------------------------------------------------------------- #
# 20. Huge / wide files and over-long lines
# --------------------------------------------------------------------------- #
def csv_over_long_line(width: int = 2_000_000) -> bytes:
    """A single field roughly ``width`` bytes long.

    Why: an over-long line can blow read buffers. DuckDB bounds it with
    ``max_line_size`` (~2 MB) and raises ``LINE SIZE OVER MAXIMUM``. The fixture
    is a deterministic 'x'*width cell so the boundary behavior is exercised.
    """
    big = b"x" * width
    return b"a,b\n1,2\n" + big + b",3\n"


# --------------------------------------------------------------------------- #
# 21. Datetime format zoo
# --------------------------------------------------------------------------- #
def csv_ambiguous_date() -> bytes:
    """Ambiguous ``1/6/2000`` (Jan-6 vs Jun-1) and an impossible ``32/32/2019``.

    Why: date parsing is the richest source of silent month/day swaps. tallyman
    keeps dates opt-in/explicit (no auto-parse), so without a ``date`` schema
    these stay ``string`` — never silently swapped, never a wrong date.
    """
    return b"d\n1/6/2000\n2/7/2001\n32/32/2019\n"


# --------------------------------------------------------------------------- #
# 22. Integer overflow / large-int fidelity
# --------------------------------------------------------------------------- #
def csv_integer_overflow() -> bytes:
    """A value past int64 range alongside ordinary ints.

    Why: silent wraparound or precision loss on identifiers/large counts. Polars
    falls back i64 → Int128 on overflow; with an explicit narrow type an
    out-of-range value should be a value-error, not a silent wrap.
    """
    return b"big\n1\n2\n99999999999999999999999\n"


# --------------------------------------------------------------------------- #
# 23. Naive timestamp string with a tz-aware schema pin (#145)
# --------------------------------------------------------------------------- #
def csv_naive_ts_pinned_utc() -> bytes:
    """Naive timestamp strings (no tz offset) with a schema that pins ``timestamp('UTC')``.

    Why: the timestamp scale → polars time_unit mapping (#145) resolved the
    rounding direction but left open a factual question: when polars is given a
    naive string and a tz-aware ``Datetime(unit, time_zone='UTC')`` override,
    does it (a) *attach* UTC (reinterpret the value as UTC, no shift), (b)
    *convert* from local time to UTC (shift the clock), or (c) raise?

    Verdict (polars 1.x, verified 2026-06-26): **attach** — ``2024-01-15
    10:30:00`` becomes ``2024-01-15T10:30:00+00:00`` with no shift.  tallyman
    inherits this semantics: pinning ``timestamp('UTC')`` on a naive-string
    column is a declaration that the values *are* UTC, not a conversion request.
    """
    return b"ts,val\n2024-01-15 10:30:00,1\n2024-06-20 08:00:00.123456,2\n"


# --------------------------------------------------------------------------- #
# Registry — every dastardly case, for the parametrized corpus sweep.
# (Builders are also importable individually, buckaroo-ddd style.)
# --------------------------------------------------------------------------- #
ALL_CASES: list[Callable[[], bytes]] = [
    csv_embedded_newline,
    csv_embedded_newline_in_header,
    csv_bom_utf8,
    csv_bom_on_quoted_header,
    csv_latin1,
    csv_decimal_comma,
    csv_thousands_separator,
    csv_mixed_type_late,
    csv_all_null_column,
    csv_leading_zeros,
    csv_scientific_notation,
    csv_duplicate_headers,
    csv_empty_header_cell,
    csv_headerless_all_string,
    csv_trailing_delimiter,
    csv_ragged_short,
    csv_ragged_long,
    csv_quoted_vs_bare_empty,
    csv_leading_whitespace,
    csv_mixed_newlines,
    csv_nul_byte,
    csv_over_long_line,
    csv_ambiguous_date,
    csv_integer_overflow,
    csv_naive_ts_pinned_utc,
]
