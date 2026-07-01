# Funky-CSV edge-case corpus

The canonical pathological-CSV cases drawn from the DuckDB, Polars, and Pandas
test suites (see [`duckdb.md`](./duckdb.md), [`polars.md`](./polars.md),
[`pandas.md`](./pandas.md)). This is the **seed corpus for the fast-follow** that
ports these cases into tallyman's own CSV test suite — one fixture per pathology,
mirroring DuckDB's by-pathology test layout and Polars' chunk-boundary harness.

For each case: what it is, why it matters, how each of the three tools behaves
today, and a **suggested tallyman behavior**. Test-file citations are to each
tool's note. The companion comparison is in [`comparison.md`](./comparison.md).

A recurring theme: the three tools disagree most on **short rows**, **null vs
empty string**, **dialect sniffing**, and **whether bad rows come back as data**.
Those are the cells to copy into tallyman's regression tests verbatim.

---

## 1. Embedded newline inside a quoted field

**What:** a real `\n`/`\r\n` line break inside a `"..."`-quoted field (and the
same in a header cell). A stray unbalanced quote can make a parser swallow a true
line break.

**Why it matters:** the most common "row count is wrong" bug; the parser must
treat in-quote newlines as data, not record terminators.

**Tools:** DuckDB handles natively; the failure mode (stray quote eats a line
break) is `csv_quoted_newline_incorrect.test`. Polars honors it when `quote_char`
is set — `test_skip_new_line_embedded_lines`, `test_csv_quoted_newlines_skip_rows_19535`.
Pandas respects `quotechar`/`doublequote` — `test_tokenize_CR_with_quoting`,
`test_data_after_quote`.

**Suggested tallyman:** honor quoted newlines by default; add a fixture with an
embedded newline in both a data cell and a header cell, and run it through any
chunked reader at a tiny chunk size (Polars' 7-byte harness) so the newline
straddles a buffer boundary.

---

## 2. Byte-order mark (BOM)

**What:** a UTF-8 BOM (`EF BB BF`), or UTF-16/32 BOM, prefixing the first cell —
including on a quoted first header cell.

**Why it matters:** an unstripped BOM corrupts the first column name (`﻿id`),
silently breaking by-name schema binding (#141).

**Tools:** DuckDB strips the UTF-8 BOM automatically. Polars strips a valid BOM
(`test_utf8_bom`) but keeps invalid BOM bytes as data (`test_invalid_utf8_bom`).
Pandas strips utf-8/utf-8-sig (`test_utf8_bom_removal`), preserves the BOM as
content under latin-1 (`test_latin1_bom_preserved`), and handles utf-16 BOM with
skiprows.

**Suggested tallyman:** strip a leading UTF-8 BOM before header parsing always
(so binding-by-name never sees `﻿`); for utf-16/32 require an explicit
`encoding`. Add a fixture with a BOM on a quoted first header cell.

---

## 3. Alternate / non-UTF-8 encodings

**What:** latin-1, windows-1252, Shift-JIS, Big5, utf-16/32, plus invalid bytes
for the declared codec.

**Why it matters:** encoding is undecidable from content alone; a wrong guess
yields mojibake or a mid-parse failure deep in the file.

**Tools:** DuckDB builds in only utf-8 / utf-16 / latin-1; invalid bytes raise
`INVALID ENCODING` (`test_invalid_utf_rejects.test`). Polars defaults to strict
utf8; inference tolerates via `from_utf8_lossy` but execution can raise
`InvalidOperationError: file encoding is not UTF-8`; anything beyond
`utf8`/`utf8-lossy` is decoded in Python in memory first (`scan_csv` only supports
utf8/utf8-lossy). Pandas takes any Python codec with an `encoding_errors` policy
(strict/replace/ignore); bad bytes raise `UnicodeDecodeError`.

**Suggested tallyman:** detect-or-declare encoding up front, never let an
encoding error surface mid-parse. Default strict utf-8; expose an explicit
`encoding` and a lossy fallback that routes replaced bytes into the reject log so
the substitution is auditable.

---

## 4. Decimal comma (European numerics)

**What:** floats written `1,5` or `1.234,56` with `,` as the decimal point.

**Why it matters:** without the flag, `1,5` is split into two fields (comma is the
default delimiter) or read as a string; with it, `,` stops being a separator for
numerics.

**Tools:** DuckDB `decimal_separator=','` (`csv_decimal_separator.test`). Polars
`decimal_comma=True` switches `FLOAT_RE`→`FLOAT_RE_DECIMAL`
(`test_read_csv_float_type_decimal_comma`). Pandas `decimal=','` (must be length-1,
else `Only length-1 decimal markers supported`); `common/test_decimal.py`.

**Suggested tallyman:** explicit `decimal_separator` option (default `.`); when set
to `,`, ensure it does not collide with a `,` field delimiter — require a different
separator and validate, or error clearly.

---

## 5. Thousands separator

**What:** grouped integers like `1,234.56` or `1.234,56` with a grouping char to
strip.

**Why it matters:** `1,234` reads as `1234` only if the grouping char is stripped;
otherwise it splits or stays a string. The grouping char must NOT be stripped from
non-numeric columns.

**Tools:** DuckDB `thousands=','` strips grouping. Pandas `thousands=','` with the
guard `test_no_thousand_convert_for_non_numeric_cols`. Polars has **no thousands
option** — grouped numbers stay String unless pre-cleaned.

**Suggested tallyman:** offer a `thousands` strip option but apply it only to
columns the schema types as numeric, never to string columns (Pandas' guard).
Note Polars' gap: if tallyman's engine is Polars-based, this needs a custom pass.

---

## 6. Mixed-type column past the inference window

**What:** a column that looks `int` for the first N sampled rows, then holds a
float/string value far deeper in the file.

**Why it matters:** *the* canonical importer bug. The schema is decided from the
window and the violating value is never seen until parse time, so the failure
surfaces 30k rows in — or, worse, silently.

**Tools:** DuckDB demotes to the lowest common type within the sample, but a value
*past* `sample_size` (default 20480) aborts the read; fix with `sample_size=-1` or
explicit `types=` (`csv_mixed_casts.test`, `test_cast_error.test_slow`). Polars
(window 100) raises at parse time `could not parse '1e5' as dtype 'i64' at column
'energy' (column number 3)` (`test_error_message`). Pandas infers whole-column by
default (no miss) but under `low_memory=True` infers per buffer and returns
`object` with **mixed Python int+str values, silently, no warning**.

**Suggested tallyman:** never silently mix like `low_memory`. With an explicit
schema (tallyman's contract) the violating value is a value-error → route it to
the reject log with column + value + line. On the inference/suggestion path, use a
bounded multi-offset sample, surface the window size, and emit a "type changed past
the window" diagnostic.

---

## 7. All-null column

**What:** a column whose every value is empty / a null token.

**Why it matters:** there is no evidence for a type; whatever the importer picks is
a guess that may conflict with a later widening.

**Tools:** DuckDB sniffs the lowest-priority candidate (SQLNULL→...), risky to
widen. Polars: empty fields set a null flag but contribute no type candidate, so
an all-null column resolves to null/String (`test_csv_single_categorical_null`).
Pandas: all-NA column types as `float64` or `object`.

**Suggested tallyman:** with an explicit schema this is a non-issue — honor the
declared type and fill nulls. On inference, type an all-null column as nullable
String (the safe universal fallback) and flag it as "no type evidence" in the
suggestion UI so the user picks.

---

## 8. Leading-zero identifiers (ZIP / FIPS / account numbers)

**What:** values like `01234` that are identifiers, not integers.

**Why it matters:** promoting to int silently destroys the leading zero — the
"my ZIP codes became 1234" surprise — and it is irreversible once cached.

**Tools:** DuckDB **keeps** leading-zero values as VARCHAR by deliberate sniffer
rule (`leading_zeros_autodetect.test`). Polars infers `Int64` and **loses** the
zero unless forced to String (`test_csv_int_types`). Pandas strips to int unless
`dtype=str`.

**Suggested tallyman:** treat leading-zero retention as a deliberate, opinionated,
overridable rule (DuckDB's stance), not a bug. On inference, keep a leading-zero
column as String and surface it as a suggestion; with an explicit schema, honor
the declared type. Add a ZIP-code fixture asserting `01234` survives.

---

## 9. Scientific notation and signed numbers

**What:** `1e3`, `1E-9`, `1e27`, `+5`, leading/trailing-dot floats.

**Why it matters:** scientific notation is float-shaped; an int-typed column that
meets `1e5` fails the cast (this is the mixed-type trap in disguise). Scientific
numbers can also be mis-read as dates.

**Tools:** DuckDB `integer_exponent.test`, `plus_autodetect.test`. Polars `FLOAT_RE`
matches scientific (`test_csv_float_parsing` covers signs, `inf`, `NaN`, `1e27`,
`1E65`, `1e-28`). Pandas `test_decimal_and_exponential`; the parse_dates
mis-as-date trap is tested separately.

**Suggested tallyman:** recognize scientific-notation floats as a deliberate rule;
never auto-coerce a scientific number to a date. Fixture: an int-looking column
that holds one `1e5` value → must surface as a float (or a value-error against an
int schema), not silently truncate.

---

## 10. Duplicate header names

**What:** two columns named `id`, or repeated blank names.

**Why it matters:** by-name schema binding (#141) is ambiguous when names collide;
silent de-dup can bind the wrong column.

**Tools:** DuckDB de-duplicates / synthesizes names (`test_csv_duplicate_columns.test`).
Polars dedups to `name_duplicated_0`, `name_duplicated_1` (`schema_inference.rs:74-87`,
`test_duplicated_columns`). Pandas mangles to `a`, `a.1`, `a.2`
(`test_mangle_dupes.py`).

**Suggested tallyman:** dedup with a deterministic, documented scheme (pick one and
test it), but since tallyman binds by name (#141), **warn loudly** when the header
has duplicates and surface the renamed columns so a schema can't bind the wrong
one. Fixture asserting the exact post-dedup names.

---

## 11. Empty / blank header cell

**What:** a header row with an empty cell (`a,,c`) or a header that is just `""`.

**Why it matters:** an empty name can't be bound by name and collides with other
empties (case 10).

**Tools:** DuckDB synthesizes a name. Polars: `b'""'` → empty-named header
(`test_only_empty_quote_string`); `test_only_header_with_newline`. Pandas
synthesizes / mangles via the dup path.

**Suggested tallyman:** synthesize stable positional names (`column_1`…) for empty
header cells, dedup deterministically, and surface them. Same warn-on-rename signal
as the duplicate-header case.

---

## 12. Headerless file

**What:** no header row; the first line is data. Often combined with varying row
widths.

**Why it matters:** an importer that assumes a header consumes a real data row as
column names. When every column is String, the header can't be type-distinguished
at all.

**Tools:** DuckDB auto-assumes a header only when types disambiguate row 0; when
all columns are VARCHAR it **assumes a header exists** (override `header=false` +
`names=[...]`) — called out as a known limitation. Polars `has_header=False`
autogenerates `column_1..N` and **grows the schema to the widest row**
(`extend_header_with_unknown_column`, `test_csv_no_header_ragged_lines_1505`).
Pandas `header=None` + `names`.

**Suggested tallyman:** for headerless input, require explicit `names` (or accept a
positional/list schema — the one place positional binding is right, per #141). Do
not guess header presence for all-String files; ask. Mirror DuckDB's documented
all-VARCHAR ambiguity in a fixture.

---

## 13. Trailing delimiter (ghost column)

**What:** every line ends with the delimiter (`a,b,`), implying a spurious empty
final column.

**Why it matters:** the ghost column shifts width and can be mistaken for an index
column, throwing off by-position binding and field counts.

**Tools:** DuckDB sees an extra (often all-null) column. Polars: trailing sep → an
extra all-null column (`test_trailing_separator_8240`). Pandas: a trailing comma
creates a spurious unnamed column it may use as the index unless `index_col=False`.

**Suggested tallyman:** detect a uniform trailing delimiter and decide
deterministically — either keep the empty column (named `column_N`) or drop it —
and surface the decision. Don't let it silently become an index. Fixture with a
uniform trailing comma.

---

## 14. Ragged SHORT row (too few fields) — tallyman #142

**What:** a row with fewer fields than the header, e.g. `a,b\n1,2\n3\n4,5`.

**Why it matters:** **this is tallyman #142.** The old datafusion path raised; the
Polars `scan_csv` path silently null-fills, baking `(3, null)` into the canonical
snapshot with no signal. Silent-by-direction is the trap.

**Tools:** DuckDB raises `MISSING COLUMNS` by default; `null_padding=true` pads.
Polars **silently null-fills**, no error. Pandas **silently NaN-pads** — a short
row is explicitly *not* a "bad line."

**Suggested tallyman:** **fail loud on any field-count mismatch by default**
(restore the datafusion/DuckDB-strict behavior). Offer an opt-in pad/salvage mode
that routes every padded row into the reject log so the null-fill is never
invisible. Direct regression fixture for #142: `a,b\n1,2\n3\n4,5` must raise (or
log), never silently produce `(3, null)`.

---

## 15. Ragged LONG row (too many fields) — tallyman #142 sibling

**What:** a row with more fields than the header, e.g. an unescaped delimiter
inside an unquoted field.

**Why it matters:** the asymmetric twin of case 14 — most tools are *loud* here and
*silent* on short rows, the direction-dependent default that confuses users.

**Tools:** DuckDB raises `TOO MANY COLUMNS`; `strict_mode=false` drops extras.
Polars raises `ComputeError: found more fields than defined` unless
`truncate_ragged_lines=True` (`test_csv_ragged_lines`, `test_csv_ragged_lines_20062`).
Pandas `on_bad_lines`: error (default) / warn / skip / callable — and the callable
only fires for too-many-fields rows.

**Suggested tallyman:** same single policy as case 14 — fail loud by default in
**both** directions; opt-in truncate routes dropped fields + affected rows into the
reject log. The asymmetry between short and long is exactly what to eliminate.

### Detection mechanism — empirical finding (2026-06-26)

Verified on the installed polars 1.40.1 / pyarrow 21.0.0. **Status: the ragged
*policy* (fail-loud vs salvage) is punted to this holistic edge-case pass; this note
records the implementation facts so the policy can be decided against the real
corpus.** The architecture decision (errors ride the build error-contract; the
reader is engine-opaque behind the agnostic dialect vocabulary) is settled
independently in the CSV-import ADR.

- **polars cannot be made to raise on a SHORT row.** No flag, schema trick, or idiom
  turns a too-few-fields row into an error — `truncate_ragged_lines` governs only
  long rows, there is no `raise_if_ragged`/`strict`, and `missing_utf8_is_empty_string`
  only swaps null for `""`. Confirmed empirically on eager `read_csv`, lazy
  `scan_csv` in-memory, and streaming (all silently null-pad). This is the still-open
  asymmetry [pola-rs/polars#10585](https://github.com/pola-rs/polars/issues/10585).
- **polars raises on LONG rows** identically on all engines:
  `polars.exceptions.ComputeError: found more fields than defined in 'Schema'`.
- **pyarrow is the fail-loud, quote-aware detector.**
  `pyarrow.csv.read_csv(path, parse_options=ParseOptions(newlines_in_values=True))`
  raises `pyarrow.lib.ArrowInvalid: CSV parse error: Expected N columns, got M: <row>`
  on **both** short and long rows, respects quoted embedded commas/newlines, and is
  fast (~4 ms on a 200k-row / 4.1 MB file, *faster* than polars' ~23 ms native read).
  pyarrow is already a tallyman dependency.
- **Rejected alternatives:** an N+1 overflow column catches long rows but is blind to
  short rows (trailing nulls are indistinguishable from legitimate nulls); any
  per-physical-line delimiter count breaks on quoted embedded newlines;
  `pl.read_csv(use_pyarrow=True)` omits `newlines_in_values` and dies on large
  multi-chunk files ("CSV parser got out of sync with the chunker"). Call pyarrow
  directly.
- **Two implementation shapes when the policy lands:** (a) a ~4 ms pyarrow.csv
  *validation gate* in front of PR 137's tested, byte-stable polars
  `scan_csv → with_row_index → sink_parquet` pipeline (keeps the merged snapshot
  machinery untouched; two reads, but the gate is noise at once-per-CSV ingest); or
  (b) pyarrow.csv as the *sole* reader (one pass, Arrow-native) — tempting but
  reopens PR 137's byte-stability guarantees. Both sit behind the opacity boundary;
  the one agnostic dialect vocabulary translates to both pyarrow `ParseOptions` and
  polars kwargs so they cannot disagree on dialect. Lean: (a).

---

## 16. Quoted-empty vs unquoted-empty as null

**What:** `""` (quoted empty) vs a bare empty field — should one be NULL and the
other empty string?

**Why it matters:** the null-vs-empty distinction changes downstream
aggregation/joins; tools disagree on it and on whether quoted null tokens count.

**Tools:** DuckDB: `allow_quoted_nulls` (default true) decides whether `"NA"` is
NULL; `force_not_null` keeps `""` as empty string per column
(`test_allow_quoted_nulls_option.test`). Polars: `missing_utf8_is_empty_string`
toggles missing → `""` vs null; `null_values` (str/list/dict) filtered before
inference (`test_csv_missing_utf8_is_empty_string`, `test_empty_string_missing_round_trip`).
Pandas: empty string is in the 19-token default NA set, so a bare empty field is
NA; quoted NA tokens still count as NA (quotes stripped before matching).

**Suggested tallyman:** make NA semantics first-class and inspectable (Pandas'
editable default-NA set + `keep_default_na`×`na_values` truth table is the model),
and expose a per-column `force_not_null` / empty-vs-null toggle. Fixture covering
`""`, bare empty, and a quoted null token across a column.

---

## 17. Whitespace sensitivity

**What:** leading/trailing spaces around values and right after the delimiter
(` 1, 2`).

**Why it matters:** whitespace can flip an inferred type (Polars does **not** strip
before inference, so ` 1` may not match `INTEGER_RE`) and silently change values.

**Tools:** Polars does **not** strip surrounding whitespace before inference —
`test_csv_field_schema_inference_with_whitespace`,
`test_preserve_whitespace_at_line_start`,
`test_csv_whitespace_separator_at_start_do_not_skip`. Pandas has
`skipinitialspace` (strip after the delimiter), default False. DuckDB parses
strictly; whitespace stays part of the value unless quoted.

**Suggested tallyman:** be explicit — default to NOT stripping (preserve bytes),
with an opt-in `skipinitialspace`-style strip, and surface that surrounding
whitespace can change inferred types. Fixture: ` 1` vs `1` inferring differently.

---

## 18. Windows vs Unix newlines (and mixed)

**What:** `\r\n` vs `\n`, a bare `\r` terminator, or a file that mixes both.

**Why it matters:** a trailing `\r` can ride along on the last field's value;
mixed newlines can confuse record boundaries.

**Tools:** DuckDB sniffs `new_line`; mixed separators recoverable in permissive
mode (`csv_windows_mixed_separators.test`). Polars handles CRLF with default `\n`
(strips trailing `\r`); custom `eol_char`; `test_skip_crlf`, `test_different_eol_char`.
Pandas C engine `lineterminator`; CR-in-field tested (`test_cr_in_field_with_trailing_space`).

**Suggested tallyman:** normalize CRLF/CR to `\n` and strip the trailing `\r` from
the last field by default; expose `eol`/`lineterminator` for the rare custom case.
Fixture mixing `\n` and `\r\n` in one file.

---

## 19. NUL bytes and other control bytes mid-field

**What:** a `\x00` byte (or other control char) embedded in a data field.

**Why it matters:** NUL bytes corrupt C string handling and are a classic
fuzzer-found crash; they also signal a binary file mis-fed as CSV.

**Tools:** DuckDB `csv_null_byte.test`, `null_terminator.test`. Pandas C engine
raises `ParserError` asserted as `"NULL byte detected"` (`test_internal_null_byte`,
`test_null_byte_char`). Polars: covered by the SIMD/field-split path tests.

**Suggested tallyman:** treat a NUL byte as a hard structural error (raise or
reject-log the line) — do not silently include it. Fixture with `\x00` mid-field.

---

## 20. Huge / wide files and over-long lines

**What:** a line exceeding ~2 MB; a file with 10000 columns; records straddling the
internal read buffer.

**Why it matters:** memory and buffer-boundary correctness; an over-long line can
blow buffers, and a field split across a chunk boundary can corrupt.

**Tools:** DuckDB `max_line_size` (default ~2 MB) raises `LINE SIZE OVER MAXIMUM`
(`csv_line_too_long.test`, `maximum_line_size.test_slow`, `test_big_line.test_slow`).
Pandas `test_large_difference_in_columns` (10000 vs 2 cols), `>262KB`
chunk-boundary fields (`test_c_parser_only.py`), `test_parse_trim_buffers`. Polars'
`chunk_override` fixture parametrizes nearly every test over a 7-byte read chunk to
force records across buffer boundaries.

**Suggested tallyman:** bound max line size with a clear `LINE SIZE OVER MAXIMUM`-
style error; if tallyman does any chunked/streaming read, **adopt Polars' tiny-
chunk harness** — run the fixture suite once normally and once with a pathologically
small chunk so straddling records are exercised.

---

## 21. Datetime format zoo

**What:** ISO-8601 with offset, dotted `27.03.2003 14:55:00.000`, slash dates,
ambiguous `1/6/2000` (DMY vs MDY), `2021-1-1`, `32/32/2019`, mixed timezones.

**Why it matters:** date parsing is the richest source of silent misreads
(month/day swap) and the place auto-inference most often guesses wrong.

**Tools:** DuckDB auto-parses with an ISO-first priority list and `day>12`
disambiguation (`test_auto_date.test`); `dateformat`/`timestampformat` to pin.
Polars opt-in `try_parse_dates` (tried last, after numeric); tz-aware → UTC;
`test_datetime_parsing_default_formats`, `test_tz_aware_try_parse_dates`,
`test_fallback_chrono_parser`. Pandas opt-in `parse_dates` + `date_format` +
`dayfirst`; ISO fast-path; unparseable → NaT or left as string; mixed tz → object.

**Suggested tallyman:** keep dates **opt-in and explicit** (Polars/Pandas stance,
not DuckDB's auto-parse) — require a format or an explicit `date`/`timestamp`
schema type; keep ISO-8601 a fast path. tallyman already warns that date-only
columns must use `date` not `timestamp`. Fixture: ambiguous `1/6/2000` must not be
silently swapped; `32/32/2019` must error/reject, not become a wrong date.

---

## 22. Integer overflow / large-int fidelity

**What:** integers exceeding int64 range, or values that overflow a declared narrow
int type (e.g. `400` into Int8).

**Why it matters:** silent wraparound or precision loss on identifiers and large
counts.

**Tools:** DuckDB sniffs `BIGINT` then `DOUBLE`; `auto_type_candidates` can add
narrower types. Polars parses `i64`, falls back to `Int128` when it overflows
(`test_infer_field_schema_i64_overflow`); with `ignore_errors=True` an Int8 column
where `400` overflows yields `null` for that value (`test_ignore_errors_casting_dtypes`
→ `[10, None, None, 90]`). Pandas falls back int64→`uint64` on overflow
(`test_accurate_parsing_of_large_integers`, `test_ea_int_avoid_overflow`).

**Suggested tallyman:** with an explicit schema, an out-of-range value is a
value-error → reject-log it (don't silently null or wrap). On inference, widen to a
type that holds every sampled value (Int128/uint64 fallback) and surface the chosen
width. Fixture: a value past int64 and a value overflowing a declared narrow type.

---

## How to port this corpus

- **One fixture file per case**, named by pathology, mirroring DuckDB's
  by-pathology layout under `test/sql/copy/csv/`.
- **Mirror a `rejects/` subdir** for the quarantine path once tallyman has a reject
  table — the short-row (#142) and long-row, NUL-byte, encoding, and integer-
  overflow cases all produce reject rows.
- **Run every fixture through a tiny-chunk variant** (Polars' 7-byte
  `chunk_override`) if tallyman reads in chunks.
- **Assert exact post-transform names** for the duplicate/empty-header cases, since
  tallyman binds schemas by name (#141) and a wrong dedup binds the wrong column.
- **Seed from Pollock** (DuckDB's robustness benchmark categories: inconsistent
  rows, mixed newlines, multiple headers, unescaped quotes, multibyte delimiters,
  multiple tables) for a real robustness bar.
