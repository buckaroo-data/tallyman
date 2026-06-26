# Pandas `read_csv` — CSV Import Research Note

Reference for tallyman's CSV-importer architecture decision. Grounded against
pandas 3.0.x primary sources: the API reference, the IO user guide, the Cython
tokenizer (`pandas/_libs/parsers.pyx`), and the parser test suite
(`pandas/tests/io/parser/`).

Primary sources:
- API reference: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
- IO user guide: https://pandas.pydata.org/docs/user_guide/io.html#io-read-csv-table
- C tokenizer: https://github.com/pandas-dev/pandas/blob/main/pandas/_libs/parsers.pyx
- Test suite: https://github.com/pandas-dev/pandas/tree/main/pandas/tests/io/parser

A useful secondary deep-dive on inference internals:
https://rushter.com/blog/pandas-data-type-inference/

> Version note: defaults below are pandas 3.0.x. Pandas 3.0 **removed** several
> legacy params still in tutorials: `date_parser`, `infer_datetime_format`,
> `keep_date_col`, `verbose`, `delim_whitespace`, `error_bad_lines`,
> `warn_bad_lines`, `squeeze`, `prefix`, `mangle_dupe_cols`. Don't model these.

---

## Config options

`read_csv` has ~40 keyword parameters. Grouped by category, with verbatim
defaults from the pandas 3.0.x signature
(https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html).

| Option | Default | Category | Purpose |
|---|---|---|---|
| `filepath_or_buffer` | (required) | source | Path, URL, or file-like / buffer to read. |
| `sep` | `','` (sentinel `<no_default>`) | dialect | Field delimiter. Single char for C/pyarrow; **regex / multi-char** forces the python engine. `sep=None` triggers delimiter sniffing (python engine, `csv.Sniffer`). |
| `delimiter` | `None` | dialect | Alias for `sep`. |
| `quotechar` | `'"'` | dialect/quote | Char that wraps quoted fields (delimiters/newlines allowed inside). |
| `quoting` | `0` (`csv.QUOTE_MINIMAL`) | dialect/quote | Quoting mode: `QUOTE_MINIMAL`(0), `QUOTE_ALL`(1), `QUOTE_NONNUMERIC`(2), `QUOTE_NONE`(3). `QUOTE_NONNUMERIC` also changes inference (see below). |
| `doublequote` | `True` | dialect/quote | Treat `""` inside a quoted field as one literal `"`. |
| `escapechar` | `None` | dialect/quote | Char that escapes the next char (e.g. an embedded quote when `quoting=QUOTE_NONE`). |
| `skipinitialspace` | `False` | dialect | Strip whitespace immediately after the delimiter. |
| `lineterminator` | `None` | dialect | Explicit line break char (C engine only, single char). |
| `dialect` | `None` | dialect | A `csv.Dialect` (or name) bundling delimiter/quote/etc.; overrides individual params it sets. |
| `comment` | `None` | dialect | Char marking rest-of-line as a comment (single char). |
| `header` | `'infer'` | header | Row(s) with column labels. `'infer'` = row 0 unless `names` given. `header=None` = no header. List (`[0,1,2]`) = MultiIndex columns. |
| `names` | `<no_default>` | header | Explicit column labels. Combine with `header=0` to override, `header=None` for headerless. Duplicate names are **mangled** (`x`, `x.1`, `x.2`). |
| `index_col` | `None` | columns | Column(s) to use as the row index. `index_col=False` forces no index (handles a trailing-delimiter "ghost" column). |
| `usecols` | `None` | columns | Subset of columns by name, position, or **callable** predicate. Big parse-time savings. |
| `dtype` | `None` | types | Force column dtype(s): a single dtype, a `{col: dtype}` dict, or a `defaultdict`. Bypasses inference for those columns. |
| `engine` | `None` | engine | `'c'` (default, fast), `'python'` (most features), `'pyarrow'` (multithreaded, fewest options). |
| `converters` | `None` | types | `{col: fn}` applied to raw strings. Runs **instead of** dtype inference for that column. Not on pyarrow engine. |
| `true_values` / `false_values` | `None` | types | Extra strings mapped to `True` / `False`. |
| `skipinitialspace` | `False` | dialect | (listed above) |
| `skiprows` | `None` | skip | Int (skip first N), list of line numbers, or **callable** `idx -> bool`. |
| `skipfooter` | `0` | skip | Skip N lines at the end. **python engine only.** |
| `nrows` | `None` | streaming | Read only the first N rows. |
| `skip_blank_lines` | `True` | skip | Drop fully blank lines instead of emitting NaN rows. |
| `na_values` | `None` | nulls | Extra strings to treat as NA. Scalar, list, or per-column dict. |
| `keep_default_na` | `True` | nulls | Whether the built-in NA string set is active (see Inference). |
| `na_filter` | `True` | nulls | Master switch for NA detection. `False` = no NA scanning (perf win on NA-free files). |
| `parse_dates` | `None` | dates | `True` (index), list of cols, or list-of-lists to combine cols into one date. |
| `date_format` | `None` | dates | strptime format string or `{col: fmt}` (pandas ≥2.0). |
| `dayfirst` | `False` | dates | Interpret ambiguous `DD/MM` as day-first. |
| `cache_dates` | `True` | dates | Cache+reuse conversions of repeated date strings. |
| `thousands` | `None` | numbers | Thousands-grouping char (e.g. `','`). |
| `decimal` | `'.'` | numbers | Decimal point char (e.g. `','` for European). Must be length-1. |
| `float_precision` | `None` | numbers | C-engine float converter: `None`/`'legacy'`, `'high'`, `'round_trip'`. |
| `encoding` | `None` (→ utf-8) | encoding | Text codec. BOM-aware for utf-8-sig/utf-16/utf-32. |
| `encoding_errors` | `'strict'` | encoding | Codec error policy: `strict`/`replace`/`ignore`/`backslashreplace`/etc. |
| `compression` | `'infer'` | source | `infer` from extension, or `gzip`/`bz2`/`zip`/`xz`/`zstd`/`tar`/`None`. |
| `iterator` | `False` | streaming | Return a `TextFileReader` supporting `.get_chunk(n)`. |
| `chunksize` | `None` | streaming | Return a `TextFileReader` yielding DataFrames of N rows. |
| `low_memory` | `True` | perf/types | C engine processes the file in internal buffers and infers per-buffer — the **mixed-dtype hazard** (see Inference). |
| `memory_map` | `False` | perf | `mmap` the file to cut I/O syscalls. |
| `on_bad_lines` | `'error'` | errors | `'error'`/`'warn'`/`'skip'`, or a **callable** (python engine) for too-many-fields rows. |
| `dtype_backend` | `<no_default>` (numpy) | types | `'numpy_nullable'` (Int64/Float64/boolean + `pd.NA`) or `'pyarrow'` (arrow-backed). |
| `storage_options` | `None` | source | fsspec options for remote URLs (s3/gcs/http). |

### Engine feature matrix (what NOT every engine supports)

From the IO guide. The C and pyarrow engines reject regex/multi-char `sep` and
`skipfooter`. The pyarrow engine additionally ignores/rejects: `float_precision`,
`chunksize`, `comment`, `nrows`, `thousands`, `memory_map`, `dialect`,
`on_bad_lines` (callable), `quoting`, `lineterminator`, `converters`, `decimal`,
`iterator`, `dayfirst`, `skipinitialspace`, `low_memory`, and dict `na_values`.
The `on_bad_lines` **callable** and `skipfooter` and delimiter **sniffing**
(`sep=None`) are python-engine only.

---

## Inference

### Dialect / delimiter inference

The C and pyarrow engines do **not** sniff — you must pass `sep`. Only the
**python engine** infers a delimiter, via Python's stdlib `csv.Sniffer`, and only
when `sep=None`. Tests: `test_sniff_delimiter`, `test_sniff_delimiter_comment`,
`test_sniff_delimiter_encoding`, and `test_none_delimiter` in
`pandas/tests/io/parser/test_python_parser_only.py`. If the sniffer can't decide
it raises `"Could not determine delimiter"`. Header presence is not "sniffed"
statistically — `header='infer'` just means "row 0 is the header unless `names`
is supplied."

### Type inference — order and mechanics

The tokenizer stores every field as a **string** and makes no assumptions; dtype
is guessed afterward, per column. The C engine's cast order is literally
(`pandas/_libs/parsers.pyx`):

```python
dtype_order = ["int64", "float64", "bool", "object"]
if quoting == QUOTE_NONNUMERIC:
    dtype_order = dtype_order[1:]          # skip int64; start at float64
self.dtype_cast_order = [np.dtype(x) for x in dtype_order]
```

So each column is tried as `int64`, then `float64`, then `bool`, falling back to
`object` (string). `uint64` is handled as a special case: when an int64 attempt
overflows, it retries as `uint64` before giving up. Empty fields and the default
NA tokens become NaN, which forces an int column to `float64` (NaN is a float) —
the classic "my IDs became 1.0" surprise. `inf`/`-inf` (case-insensitive) parse to
floating infinity. With `dtype_backend='numpy_nullable'` an all-int column with
NAs stays `Int64` (nullable) instead of upcasting to float.

### The inference window: WHOLE column, but chunked under `low_memory=True`

There is **no fixed sampling window**. By design pandas infers a dtype from the
*entire* column, not the first N rows. The tradeoff is memory: a true whole-file
inference needs the whole file resident.

To bound memory the C engine ships `low_memory=True` (default), which infers in
**internal buffers** sized by a heuristic in `pandas/_libs/parsers.pyx`:

```python
int64_t DEFAULT_CHUNKSIZE = 256 * 1024
DEFAULT_BUFFER_HEURISTIC = 2 ** 20
...
heuristic = DEFAULT_BUFFER_HEURISTIC // self.table_width   # ~1M / ncols
self.buffer_lines = 1
while self.buffer_lines * 2 < heuristic:
    self.buffer_lines *= 2                                  # largest pow2 < heuristic
```

Each buffer is inferred **independently** and the pieces are concatenated. If one
buffer sees only ints for a column and a later buffer sees a string, the column
comes back as `object` with **mixed Python int + str values** — silently, no
error. The user guide's own example:

```python
col_1 = list(range(500000)) + ["a", "b"] + list(range(500000))
# round-trip through CSV:
pd.read_csv("foo.csv")["col_1"].dtype   # dtype('O')  <-- object, mixed
```

The two fixes the docs recommend: pass an explicit `dtype`, or set
`low_memory=False` (single-pass whole-column inference, more RAM). pyarrow engine
does not have this hazard (whole-column typing). This is the single most
important pandas-CSV gotcha for a downstream importer to be aware of.

`QUOTE_NONNUMERIC` interaction: under that quoting mode pandas assumes every
**unquoted** field is numeric, so int64 is dropped from the cast order and bare
tokens go straight to float/object.

### NA inference

`keep_default_na=True` (default) activates this built-in NA string set (from the
IO guide / `test_na_values.py::test_default_na_values`):

```
'' , '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
'1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'None',
'n/a', 'nan', 'null'
```

(19 tokens; note **empty string** and `'<NA>'` are included.) `keep_default_na`
× `na_values` truth table:

| keep_default_na | na_values | recognized NA set |
|---|---|---|
| `True` | given | defaults ∪ na_values |
| `True` | none | defaults only |
| `False` | given | na_values only |
| `False` | none | nothing is NA |

`na_values` may be a dict for per-column NA tokens. `na_filter=False` disables NA
scanning entirely for speed on files known to be NA-free. Quoted NA tokens are
still recognized as NA (the quotes are stripped before matching).

### Date inference

Dates are **not** inferred automatically — you opt in with `parse_dates`. A
fast-path exists for ISO-8601 (`2000-01-01T00:01:02+00:00`). `date_format` (≥2.0)
pins a strptime format (string or per-column dict); `dayfirst` disambiguates
`DD/MM`; `cache_dates=True` reuses conversions of repeated strings. Unparseable
values become `NaT` (or are left as object with a warning, depending on mix).
Mixed-timezone columns come back as object — the documented pattern is to read
raw then `pd.to_datetime(col, utc=True)` or `format='mixed'`.

### dtype_backend

`'numpy_nullable'` → `Int64`/`Float64`/`boolean`/`string` with `pd.NA`.
`'pyarrow'` → `int64[pyarrow]` etc. backed by Arrow. Default remains classic numpy
(NaN-based, int→float on NA). Tests: `test_dtype_backend`,
`test_dtype_backend_pyarrow`, `test_dtype_backend_string` in
`pandas/tests/io/parser/dtypes/test_dtypes_basic.py`.

---

## Error handling

Pandas's posture is **fail-loud on structure, coerce-quietly on values**.

### Ragged rows (structural) → raise / warn / skip

A row with the wrong field count is a structural error. Behavior is governed by
`on_bad_lines`:

- `'error'` (default): raise `pandas.errors.ParserError`. The C-engine message is
  the well-known:
  ```
  Error tokenizing data. C error: Expected 3 fields in line 4, saw 5
  ```
  (Test `common/test_read_errors.py::test_malformed` asserts
  `"Expected 3 fields in line 4, saw 5"`;
  `test_read_csv_wrong_num_columns` asserts `"Expected 6 fields in line 3, saw 7"`.)
- `'warn'`: emit a `ParserWarning` (`"Skipping line N: ..."` on C/python) and drop
  the row.
- `'skip'`: drop the row silently.
- **callable** (python engine): the function receives the over-long split line and
  returns a corrected list (or `None` to skip). Tests:
  `test_python_parser_only.py::test_on_bad_lines_callable*`. Note: the callable
  only fires for **too-many-fields** rows; **too-few-fields** rows are
  back/forward-filled with NaN, not sent to the callable.

Important asymmetry: a **short** row (too few fields) is *not* a bad line — pandas
pads the missing trailing fields with NaN. Only **extra** fields trip
`on_bad_lines`. A common real cause of "extra fields" is an unescaped delimiter
inside an unquoted field. There is **no reject-table / quarantine / `ignore_errors`
DataFrame** — bad rows are either raised on, warned+dropped, or silently dropped.
You don't get the offending rows back as data.

### Value-level type mismatches → coerce or raise depending on path

- **Inference path (no `dtype`):** a value that doesn't fit the guessed type just
  widens the column (int→float→object). No error; you may silently get `object`.
- **Forced `dtype` path:** an unsafe cast raises. Examples from the suite:
  - int dtype on a column containing NA → `ValueError: Integer column has NA values`
    (`dtypes/test_dtypes_basic.py::test_raise_on_passed_int_dtype_with_nas`).
  - bool dtype with NA → `Bool column has NA values` (`test_na_values.py`).
  - generally unsafe user dtype →
    `"cannot safely convert passed user dtype ..."`
    (`test_c_parser_only.py::test_dtype_and_names_error`).
  - `dtype=datetime64`/`timedelta64` →
    `"the dtype datetime64 is not supported for parsing, pass this column using parse_dates instead"`.
  - both `converters` and `dtype` for one col →
    `"Both a converter and dtype were specified for column a -- only the converter will be used"`.

### Encoding errors

Bad bytes for the chosen codec raise a `UnicodeDecodeError` by default
(`encoding_errors='strict'`), e.g.
`"'utf-8' codec can't decode byte 0xe4 ..."` (tests `test_bad_stream_exception`,
`test_open_file`). Switch `encoding_errors` to `'replace'`/`'ignore'`/
`'backslashreplace'` to survive. A NUL byte in C-engine input raises a
`ParserError` the test asserts as `"NULL byte detected"`
(`common/test_read_errors.py::test_null_byte_char`).

### Empty / degenerate input

- Truly empty input → `pandas.errors.EmptyDataError: No columns to parse from file`
  (`test_raise_on_no_columns`).
- More names than columns →
  `"Too many columns specified: expected 4 and found 3"` /
  `"Number of passed names did not match number of header fields"`
  (`test_catch_too_many_names`).
- Unclosed quote → `"unexpected end of data"` / "EOF inside string"
  (`test_python_parser_only.py::test_read_csv_unclosed_double_quote_in_data_still_errors`).
- `decimal`/`thousands` longer than one char →
  `"Only length-1 decimal markers supported"` (`test_empty_decimal_marker`).
- Bad `skipfooter` → `"skipfooter must be an integer"` /
  `"skipfooter cannot be negative"`.
- Invalid `on_bad_lines` value →
  `"Argument abc is invalid for on_bad_lines"`.
- Unknown `float_precision` → `"Unrecognized float_precision option: junk"`.

Engine note: the pyarrow engine produces *different* wording for the same bad-line
condition, e.g. `"Expected 1 columns, but found 3: 1,2,3"` — so error-string
matching is engine-specific.

---

## Test suite

Lives at `pandas/tests/io/parser/`
(https://github.com/pandas-dev/pandas/tree/main/pandas/tests/io/parser). Organized
by feature, plus engine-specific files and three subpackages. Most feature tests
are **parametrized across engines** via fixtures in `conftest.py` (the `all_parsers`
fixture), so one test name runs on c/python/pyarrow.

Top-level feature files:
- `test_header.py`, `test_index_col.py`, `test_mangle_dupes.py` — header/index/dup-name handling.
- `test_na_values.py` — NA recognition (31 tests; default-NA set, keep_default_na matrix, per-column dicts, bool/NA interplay).
- `test_parse_dates.py` — the date-format zoo (ISO8601+tz, dotted, slash, dayfirst swap warnings, malformed-date caching, multi-col combine).
- `test_quoting.py`, `test_dialect.py`, `test_comment.py`, `test_converters.py`.
- `test_encoding.py` — BOM + codec edge cases (see below).
- `test_skiprows.py`, `test_compression.py`, `test_multi_thread.py`, `test_network.py`.
- `test_upcast.py` — dtype upcasting; `test_textreader.py` — low-level reader.
- `test_unsupported.py` — asserts the right error when an option meets the wrong engine.
- `test_read_fwf.py` — fixed-width sibling of read_csv.

Engine-specific:
- `test_c_parser_only.py` — buffer overflow, CR-in-field, NUL byte, trim-buffer
  stress (`test_parse_trim_buffers`), wide-file variance (`test_large_difference_in_columns`,
  10000 vs 2 cols), `>262KB` chunk-boundary fields, float_precision, mmap.
- `test_python_parser_only.py` — sniffer, skipfooter, multichar/regex sep,
  `on_bad_lines` callable family.

Subpackages:
- `common/` — `test_common_basic.py`, `test_chunksize.py`, `test_iterator.py`,
  `test_ints.py`, `test_float.py`, `test_inf.py`, `test_index.py`, `test_decimal.py`,
  `test_data_list.py`, `test_file_buffer_url.py`, and **`test_read_errors.py`** (the
  canonical home of the bad-line / EmptyDataError / NUL-byte assertions).
- `dtypes/` — `test_dtypes_basic.py` (33 tests: dtype_backend, nullable Int64/Float64,
  per-column dict, defaultdict, large-int accuracy, EA overflow avoidance,
  string-inference), `test_categorical.py`, `test_empty.py`.
- `usecols/` — column-selection tests.

---

## Notable edge cases

The "funky" cases the suite deliberately stresses:

- **Mixed-type column under low_memory** — int...str...int round-trips to `object`
  with mixed Python types, no warning. (IO guide example; the governing variable is
  `buffer_lines = largest pow2 < 2**20//ncols`.)
- **Embedded newlines / CR inside quoted fields** — `test_tokenize_CR_with_quoting`,
  `test_cr_in_field_with_trailing_space`, `test_data_after_quote`. Quoted fields may
  contain the delimiter and line breaks.
- **CR / `\r` line endings and `\x00` NUL bytes** — `test_buffer_overflow`,
  `test_internal_null_byte`, `test_null_byte_char`.
- **BOM** — `test_utf8_bom_removal` (utf-8 / utf-8-sig BOM stripped),
  `test_latin1_bom_preserved` (BOM bytes survive as data under latin-1),
  `test_utf16_bom_skiprows`. BOM on a quoted first header cell is handled.
- **utf-16 / utf-32 / Shift-JIS / CP1255 / CP037** — multibyte codecs incl. a
  multibyte char split across a read boundary (`test_chunk_splits_multibyte_char`,
  GH43540). pyarrow is *skipped* for utf-16/32 (block-boundary corruption).
- **Decimal-comma + thousands-dot (European)** — `decimal=','`, `thousands='.'`;
  `common/test_decimal.py`, `test_1000_sep_with_decimal`,
  `test_1000_sep_decimal_float_precision`. Non-numeric columns must NOT have their
  dots stripped (`test_no_thousand_convert_for_non_numeric_cols`).
- **Scientific notation** — `test_decimal_and_exponential`; also scientific numbers
  mis-read as dates is a tested parse_dates trap.
- **Large-integer fidelity / overflow** — `test_accurate_parsing_of_large_integers`,
  `test_na_values_uint64`, `test_ea_int_avoid_overflow` (int64→uint64 fallback).
- **Leading zeros / ZIP codes** — preserving `"01234"` requires `dtype=str`; inference
  will strip to int `1234`. (Canonical user gotcha; addressed via dtype/converters.)
- **Duplicate headers** — mangled `a`, `a.1`, `a.2` (`test_mangle_dupes.py`,
  `test_dtype_mangle_dup_cols`).
- **Empty / headerless / all-null columns** — `dtypes/test_empty.py`; all-NA column
  types as `float64`/`object`; `header=None` + `names`.
- **Trailing delimiter "ghost" index column** — handled via `index_col=False`.
- **Ragged rows** — `test_parse_ragged_csv` (up to 50 named cols), short rows padded,
  long rows hit `on_bad_lines`.
- **Wide / huge files** — `test_large_difference_in_columns` (10000 vs 2 cols),
  `test_read_nrows_large` (>262KB), `test_grow_boundary_at_cap`,
  `test_parse_trim_buffers`.
- **Quoted nulls / quoted numbers** — quoted NA tokens still count as NA; under
  `QUOTE_NONNUMERIC` unquoted tokens are forced numeric.
- **Datetime format zoo** — ISO8601+offset, dotted `27.03.2003 14:55:00.000`,
  ambiguous `1/6/2000` with dayfirst-swap warning, `"32/32/2019"` left as string,
  mixed timezones → object.

---

## Takeaways for tallyman

1. **Decide whole-column vs windowed inference explicitly, and document it.**
   Pandas's default is *whole-column* inference, but its memory-saving
   `low_memory=True` silently degrades that to per-buffer inference and yields mixed
   `object` columns. That foot-gun (silent, no warning, governed by
   `largest_pow2 < 2**20/ncols`) is the cautionary tale. If tallyman samples a
   window, surface the window size and a "type changed past the window" diagnostic
   rather than silently producing a mixed column.

2. **Tokenize-to-strings first, then cast in a fixed, documented order.** Pandas's
   `int64 → float64 → bool → object` cast order is simple and predictable. Mirror an
   explicit ladder (and treat NA-presence as forcing a nullable/float widening, the
   way pandas forces int→float on NaN, or better, default to nullable types).

3. **Separate structural errors from value errors.** Pandas raises loudly on ragged
   rows (`Expected N fields in line L, saw M`) but coerces values quietly. Short rows
   pad with NaN; only *extra* fields are "bad." Copy this split, but consider going
   further than pandas: offer a real **reject/quarantine table** (pandas has none —
   bad rows are raised-on or silently dropped, never returned as data). An
   `ignore_errors` mode that hands back the rejected rows is a genuine gap to fill.

4. **Make NA semantics a first-class, inspectable knob.** Pandas's 19-token default
   NA set (incl. empty string and `<NA>`) plus the `keep_default_na`×`na_values`
   matrix is good prior art. Reproduce the truth table; let users see and edit the
   active NA set rather than guessing.

5. **Dates and leading-zero strings are opt-in, never inferred.** Pandas does not
   auto-parse dates and will strip ZIP-code leading zeros to ints. Match that
   conservatism: don't guess dates/IDs; require an explicit format or dtype, and keep
   ISO-8601 a fast-path.

6. **Engine capability matrix is a maintenance tax.** Pandas's three engines have
   divergent feature support *and divergent error strings* for the same condition.
   If tallyman has multiple backends, normalize the error vocabulary so callers don't
   string-match per engine.

7. **Encoding/BOM handling must be deliberate.** utf-8-sig BOM stripping, utf-16/32
   detection, codec-error policy (`strict`/`replace`/`ignore`), and multibyte chars
   straddling read boundaries are all real cases pandas tests. Budget for them.

8. **Steal the test corpus.** `pandas/tests/io/parser/` (esp. `common/test_read_errors.py`,
   `dtypes/test_dtypes_basic.py`, `test_na_values.py`, `test_encoding.py`,
   `test_c_parser_only.py`) is a ready-made pathology checklist — ragged rows, NUL
   bytes, BOMs, decimal-comma, mixed types, wide files, large ints. Port the cases,
   not just the idea.
