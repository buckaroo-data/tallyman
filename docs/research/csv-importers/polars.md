# Polars CSV import — deep research note

Reference for tallyman's CSV-importer architecture decision. Grounded in Polars
primary sources: the Python wrapper, the Rust reader, and the unit test suite.
All line citations are against `pola-rs/polars@main` as fetched 2026-06-26.

Primary sources:

- Python wrappers (`read_csv`, `read_csv_batched`, `scan_csv`):
  `py-polars/src/polars/io/csv/functions.py`
- Batched reader: `py-polars/src/polars/io/csv/batched_reader.py`
- Schema-inference default: `py-polars/src/polars/datatypes/constants.py:9`
  (`N_INFER_DEFAULT: Final = 100`)
- Rust schema inference: `crates/polars-io/src/csv/read/schema_inference.rs`
- Rust option defaults: `crates/polars-io/src/csv/read/options.rs`
- Test suite: `py-polars/tests/unit/io/test_csv.py` (3209 lines, ~164 tests),
  `py-polars/tests/unit/io/test_lazy_csv.py`
- Docs (JS-rendered, do not WebFetch cleanly):
  <https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html>,
  <https://docs.pola.rs/api/python/stable/reference/api/polars.scan_csv.html>

A note on architecture: the Python `read_csv` is a thin marshalling layer over a
Rust reader. It does argument validation and some override-remapping gymnastics
(`functions.py:399-485`), then calls `PyDataFrame.read_csv`. The lazy `scan_csv`
calls `PyLazyFrame.new_from_csv`. `read_csv` will internally **dispatch to the
lazy/streaming engine** under several conditions (HuggingFace `hf://` paths,
`POLARS_FORCE_STREAMING=1`, `POLARS_FORCE_ASYNC=1`), which changes some
edge-case behavior — notably list-form `schema_overrides` is rejected on the
lazy path (`functions.py:499-564`, `2146-2155`).

## Config options

Every `read_csv` keyword argument (signature `functions.py:59-97`; docstrings
`functions.py:110-245`). Defaults are the Python-signature defaults. "Rust
default" column notes where the underlying `CsvReadOptions`/`CsvParseOptions`
default (`options.rs`) differs from what Python passes.

| Option | Category | Default | Purpose |
|---|---|---|---|
| `source` | source | — | Path, file-like (`read()`), `bytes`, or `BytesIO`/`StringIO`. `fsspec` used for remote if installed. Compressed files supported only when reading from a path. |
| `glob` | source | `True` | Expand the path via globbing rules (`foods*.csv`). |
| `storage_options` | source/cloud | `None` | Passed to `fsspec.open()` (host, port, creds, etc.). |
| `has_header` | header | `True` | First row is the header. If `False`, columns autogenerate as `column_1`, `column_2`, … (1-based). |
| `new_columns` | header | `None` | Rename columns right after parse. Shorter-than-width list leaves trailing columns at original names. |
| `columns` | column selection | `None` | Select by list of int indices (0-based) or list of names. |
| `separator` | dialect | `","` | Single-byte field separator. Validated 1 byte, non-empty (`_check_arg_is_1byte`). |
| `quote_char` | dialect | `'"'` | Single-byte quote char. `None` turns off all quote handling/escaping. |
| `eol_char` | dialect | `"\n"` | Single-byte end-of-line char. CRLF works with default `\n`; the trailing `\r` is stripped. |
| `comment_prefix` | dialect/skip | `None` | String marking comment lines (skipped). Single-byte → `CommentPrefix::Single`; multi-char → `CommentPrefix::Multi`. Common: `#`, `//`, `##`. |
| `skip_rows` | skip | `0` | Skip N **CSV records** before the header is parsed (respects quoting/escaping/comments). |
| `skip_lines` | skip | `0` | Skip N **raw newline-delimited lines** before the header (does NOT respect CSV escaping). |
| `skip_rows_after_header` | skip | `0` | Skip N rows after the header is parsed. |
| `n_rows` | selection | `None` | Stop after N rows. Under multi-threaded parse, N is an upper bound only (not guaranteed exact). |
| `schema` | types | `None` | Full explicit schema → **no inference at all**. Matched **by position/order**: "the order of the columns in the provided `schema` must match the order of the columns in the CSV." |
| `schema_overrides` | types | `None` | Partial dtype override. `dict[str, dtype]` matched **by NAME**; `list[dtype]` matched **by POSITION** (first N columns). See Inference. |
| `infer_schema` | types/inference | `True` | `False` → no inference; all columns become `pl.String` unless set in `schema`/`schema_overrides`. Internally sets `infer_schema_length = 0` (`functions.py:487-488`). |
| `infer_schema_length` | inference window | `100` (`N_INFER_DEFAULT`) | Max rows scanned for inference. `None` → scan **whole file into memory** (slow). `0` → all `String`. |
| `try_parse_dates` | dates | `False` | Attempt ISO8601-ish date/datetime inference; on failure column stays `String`. |
| `null_values` | nulls | `None` | `str` (global), `list[str]` (any-of, global), or `dict[col, str]` (per-column null token). |
| `missing_utf8_is_empty_string` | nulls | `False` | A missing UTF-8 field becomes `""` instead of `null`. Maps to Rust `missing_is_null` (default `true`). |
| `decimal_comma` | numeric | `False` | Parse floats with `,` as decimal separator (switches `FLOAT_RE` → `FLOAT_RE_DECIMAL`). |
| `encoding` | encoding | `"utf8"` | `{'utf8', 'utf8-lossy', 'windows-1252', 'windows-1252-lossy', …}`. Lossy → invalid bytes become `�`. Anything other than `utf8`/`utf8-lossy` is decoded in Python first, in memory. |
| `ignore_errors` | errors | `False` | Keep reading past lines that error (null-fill the failures). See Error handling. |
| `truncate_ragged_lines` | errors/ragged | `False` | Truncate lines **longer** than the schema. `False` → over-long line raises `found more fields than defined`. |
| `raise_if_empty` | errors | `True` | Empty source raises `NoDataError`; `False` returns an empty (0-col) DataFrame. |
| `n_threads` | performance | `None` | CSV parse threads. Defaults to physical CPU count. |
| `batch_size` | performance | `8192` | Lines buffered per read. (`read_csv_batched` default is `50_000`; Rust `chunk_size` default is `1<<18` = 262144.) |
| `low_memory` | performance | `False` | Reduce memory at the cost of speed. |
| `rechunk` | performance | `False` | Coalesce chunks into one contiguous array after parse. (Rust `CsvReadOptions` default `false`; `BatchedCsvReader` passes `True`.) |
| `use_pyarrow` | engine | `False` | Use pyarrow's native CSV parser when the argument set permits (`functions.py:318-397`). pyarrow ALWAYS parses dates and may infer types differently from Polars. Disabled when `schema_overrides`/`n_rows`/`n_threads`/`null_values`/`low_memory` are set. |
| `row_index_name` | augment | `None` | Prepend a row-index column with this name. |
| `row_index_offset` | augment | `0` | Starting value for the row index (only if name set; non-negative). |
| `sample_size` | — | `1024` | **Deprecated no-op since 1.10.0** (emits `DeprecationWarning` if changed). |

### `scan_csv` (lazy) — extra / differing options

`scan_csv` (`functions.py:1102-1151`) shares most options but adds lazy/cloud
ones and drops eager-only ones (`n_threads`, `batch_size`, `use_pyarrow`,
`sample_size`, `columns`/projection — projection is expressed via `.select()`):

| Option | Default | Notes |
|---|---|---|
| `with_column_names` | `None` | `Callable[[list[str]], list[str]]` applied to headers just-in-time. Mutually exclusive with `new_columns` (`functions.py:1394-1396`). |
| `new_columns` | `None` | Explicit names (e.g. headerless file). A list-form `schema_overrides` is `zip`-ped to these names (`functions.py:1397-1398`). |
| `include_file_paths` | `None` | Add a column with the source file path. |
| `missing_columns` | `None` | `"insert"` (NULL-fill) or `"raise"` when a schema column is absent. **Unstable** (warns). |
| `credential_provider` | `"auto"` | Cloud-credential callback. **Unstable**. |
| `storage_options` | `None` | AWS/GCP/Azure/HF connection keys. |
| `cache` | `None` | **Deprecated 1.39.0** — file cache removed. |
| `retries` | `None` | **Deprecated 1.37.1** — use `storage_options["max_retries"]`. |
| `file_cache_ttl` | `None` | **Deprecated 1.39.0** — file cache removed. |

Key lazy/eager divergences in `scan_csv`:

- **`schema_overrides` must be a dict** on the lazy path. A bare `Sequence`
  raises `TypeError: expected 'schema_overrides' dict, found 'list'`
  (`functions.py:1390-1392`, again in `_scan_csv_impl` at `1522-1524`) — unless
  `new_columns` is also given, in which case the list is zipped to those names.
- `encoding` lazy only supports `{'utf8', 'utf8-lossy'}`.
- `_scan_csv_impl` passes `truncate_ragged_lines: bool = True` as its own
  internal default (`functions.py:1512`), but the public `scan_csv` default is
  `False` and is what actually flows through.
- When `schema` is provided AND `has_header`, `missing_columns` is forced to
  `"insert"` to mimic legacy behavior (`functions.py:1539-1540`).

### `read_csv_batched`

`read_csv_batched` (`functions.py:774-807`) is **deprecated as of 1.37.0** in
favor of `scan_csv().collect_batches()`. Its default `batch_size` is `50_000`
(vs `8192` for `read_csv`). It builds a `BatchedCsvReader`
(`batched_reader.py:16-121`) that is now just a thin wrapper: it calls
`pl.scan_csv(...)` then `.collect_batches(chunk_size=batch_size)` and exposes
`next_batches(n) -> list[DataFrame] | None`. Upon construction it runs
`q.collect_schema()` to trigger the empty-data check when `raise_if_empty`.

## Inference

### Sampling window

- Default inference window is **100 rows** (`N_INFER_DEFAULT = 100`,
  `constants.py:9`), threaded down as `infer_schema_length`.
- `infer_schema_length=None` → scan the entire file into memory (documented as
  slow). `infer_schema_length=0` (or `infer_schema=False`) → every column is
  `pl.String`. `infer_schema=False` literally sets `infer_schema_length = 0`
  (`functions.py:487-488`, `1417-1418`).
- The Rust `CsvReadOptions` default is `Some(100)` (`options.rs`). The eager
  reader builds the inference input by reading lines up to `infer_schema_length`
  and handing a pre-sliced `&[Buffer<u8>]` of content lines to
  `infer_file_schema_impl` (`schema_inference.rs:17-47`); the per-row work is
  `infer_types_from_line`.

### Per-field type guessing (`infer_field_schema`, `schema_inference.rs:261-335`)

Each non-null field is classified, in this exact order:

1. **Quoted** (`bytes[0] == '"' && bytes[-1] == '"'`): if `try_parse_dates`,
   try a date pattern on the inner text, else `String`. (Comment at
   `schema_inference.rs:262-263`: quotes aren't escaped at inference time, so
   quoted fields default to `String`.)
2. `BOOLEAN_RE` → `Boolean`.
3. `FLOAT_RE` (or `FLOAT_RE_DECIMAL` when `decimal_comma`) → `Float64`.
4. `INTEGER_RE` → parse as `i64`; fits → `Int64`, else `Int128` (i128 feature).
5. If `try_parse_dates`: `date_infer::infer_pattern_single` →
   `Date` / `Datetime(µs[, UTC])` / `Time`, else `String`.
6. Otherwise → `String`.

Floats are tried **before** ints, so `1.0` is `Float64`. Dates are tried
**last**, after numeric, and only when `try_parse_dates=True` — otherwise
date-looking columns stay `String`.

### Column-level type merge (`finish_infer_field_schema`, `schema_inference.rs:230-258`)

Per column, every inferred row-type is collected into a `HashSet`, then reduced:

- 1 candidate → that type.
- `{Int64, Float64}` → **`Float64`** (int widens to float).
- `{Int64, Int128}` → `Int128`.
- `{Int128, Float64}` → `Float64`.
- **anything else** (e.g. `{Boolean, Int64}`, `{Int64, String}`) → **`String`**.

`String` is the universal fallback for incompatible mixes. Empty fields set a
null flag but contribute **no** type candidate. Values matching `null_values`
are filtered out **before** `infer_field_schema` is called
(`schema_inference.rs:136-186`), so null tokens don't pollute inference.

### `schema` vs `schema_overrides` vs `dtypes` — and the NAME-vs-POSITION rule (tallyman #141)

This is the crux for tallyman #141. There are two distinct override channels at
the Rust level — `schema_overwrite` (by name) and `dtype_overwrite` (by
position) — and the Python layer routes your argument into one of them by its
Python type:

- **`schema`** (full schema): no inference; matched **by position/order**. The
  docstring is explicit: column order in `schema` must match the CSV
  (`functions.py:146-150`).
- **`schema_overrides` as `dict[name, dtype]`** → matched **by NAME**. Confirmed
  at the Rust level in `build_schema` (`schema_inference.rs:201-207`):

  > `// Apply schema_overwrite by column name only. Positional overrides are
  > handled separately via dtype_overwrite.`

  `schema_overwrite.get_full(field_name)` is a name lookup. Non-existent names in
  the dict are ignored (`test_schema_overrides_dict_with_nonexistent_columns`,
  issue #20903).
- **`schema_overrides` as `list[dtype]`** → matched **by POSITION**, applied to
  the first N columns. `test_partial_schema_overrides` (`test_csv.py:407-417`):
  `schema_overrides=[pl.String]` on `a,b,c` yields `[String, Int64, Int64]` —
  only column 0 is overridden.
- `dtypes=` is the **deprecated** spelling of `schema_overrides` (renamed in
  0.20.31, `functions.py:56`); a `@deprecate_renamed_parameter` shim still maps
  it.

Subtleties that bite:

- When `columns`/projection is combined with a **list** override, Python
  remaps the list so dtypes land on the selected columns, not the first N
  (`functions.py:399-422`). With **index-based** selection the un-overridden
  selected columns are forced to `String`, not inferred
  (`test_schema_overrides_with_column_idx_selection`, `test_csv.py:435-449`).
- Too many list entries → `InvalidOperationError: The number of schema overrides
  must be less than or equal to the number of fields` on the eager path;
  `TypeError: expected 'schema_overrides' dict, found 'list'` on the streaming
  path (`test_csv.py:2135-2155`).
- A non-list/non-dict (e.g. a `set`) → `TypeError: schema_overrides should be of
  type list or dict` (`functions.py:312-316`, `test_csv.py:2129-2132`).
- **Historical positional/named ambiguity**: a dict whose length equals the
  column count was once applied positionally; #20903 / `test_csv.py:2158-2185`
  pins that a name-keyed dict is always by name. If tallyman ever relied on a
  same-length dict, this is the trap.

### What error inference-mismatch raises

When inference (limited by `infer_schema_length`) picks `Int64` but a **later,
unsampled row** holds a non-numeric value, the parse — not inference — fails
with a `ComputeError`. Exact message (`test_error_message`, `test_csv.py:1535-1541`):

```
could not parse `1e5` as dtype `i64` at column 'energy' (column number 3)
```

(`1e5` is float-shaped; with `infer_schema_length=1` the column inferred `i64`
from row 1, then row 3 failed.) The documented remedy is to raise
`infer_schema_length`, set `schema_overrides`/`schema`, or use `infer_schema=False`
(`functions.py:164-189`, `260-265`).

### Lazy (`scan_csv`) differences

- Inference still uses `infer_schema_length` but happens at `.collect()` /
  `.collect_schema()` time. Predicate/projection pushdown can change which rows
  and columns are actually parsed.
- Only dict-form `schema_overrides` (plus the `new_columns` zip special-case).
- `with_column_names` lets you rewrite headers before the schema is finalized
  (`functions.py:1341-1361`), which `read_csv` cannot do.

## Error handling

Polars expects **RFC 4180**; malformed input is documented as undefined
behavior (`functions.py:101-102`). Behavior by failure class:

- **Type mismatch (value can't cast to the chosen/declared dtype)** — raises
  `ComputeError` (`could not parse ... as dtype ...`). With
  `ignore_errors=True`, the offending value becomes `null` and the read
  continues: `test_ignore_errors_casting_dtypes` (`test_csv.py:1937-1956`) reads
  `[10, None, None, 90]` for an `Int8` column where `400` overflows and a blank
  line appears; with `ignore_errors=False` it raises `ComputeError`.
- **Ragged line — too many fields**: raises `ComputeError: found more fields
  than defined` unless `truncate_ragged_lines=True`, which drops the extra
  fields (`test_csv_ragged_lines`, `test_csv.py:1969-1988`;
  `test_csv_ragged_lines_20062`, `2504-2533`). This applies when a header (or
  earlier rows) fixed the width.
- **Ragged line — too few fields (short row)**: the missing trailing fields are
  **null-filled**, no error. `test_provide_schema` (`test_csv.py:1992-2002`) and
  `test_csv_no_header_ragged_lines_1505` (`2619-2632`) show short rows padded
  with `None`.
- **Headerless width growth**: with `has_header=False`, the schema **grows** to
  the widest row seen; narrower rows are null-padded
  (`test_csv_no_header_ragged_lines_1505`). This is driven by
  `extend_header_with_unknown_column = header_line.is_none()`
  (`schema_inference.rs:29`, `112-120`).
- **Unparseable date with explicit `Date`/`Enum`**: `ComputeError`.
  `test_csv_enum_raise` (`test_csv.py:2607-2616`): value not in the enum →
  `ComputeError: could not parse `baz``.
- **Bad encoding / non-UTF8**: schema inference uses
  `String::from_utf8_lossy` so it tolerates invalid bytes, but execution can
  fail. `InvalidOperationError: file encoding is not UTF-8`
  (`test_csv.py:2363`); invalid escape →
  `ComputeError: Field ... is not properly escaped` (`test_csv.py:2204`).
- **Malformed quotes (odd quote count in a row)**: detection is deliberately
  basic. `test_csv_malformed_quote_in_unenclosed_field_22395`
  (`test_csv.py:2662-2684`) — raises `ComputeError`; with `ignore_errors=True`
  it instead **warns** `UserWarning: CSV malformed:` and continues. Its own
  comment notes the detector misses many malformations (e.g. `a,b"c,x"y` is not
  caught).
- **Empty input**: `NoDataError` (message contains `empty` / `empty CSV`).
  `raise_if_empty=False` → empty DataFrame / `next_batches` returns `None`
  (`test_csv.py:149-169`, `1620`).

There is **no quarantine/reject-table** mechanism. The model is binary per the
`ignore_errors` knob: either raise on the first bad value, or null-fill bad
values (with a warning only for the malformed-quote case). There is no
`on_bad_lines='skip'`-style row-drop and no error/rejected-rows side output like
pandas/DuckDB offer. The closest controls are:

- `ignore_errors` — null-fill failures, keep the row.
- `truncate_ragged_lines` — drop overflow fields on long rows.
- `null_values` / `missing_utf8_is_empty_string` — define what counts as null vs
  empty before parsing.
- `infer_schema=False` / `infer_schema_length=0` — read everything as `String`
  to inspect, which the docstrings explicitly recommend as a debugging step
  (`functions.py:164-170`).

## Test suite

- Location: `py-polars/tests/unit/io/test_csv.py` — the eager-path suite (3209
  lines, ~164 `test_*` functions). Lazy-specific coverage lives in
  `py-polars/tests/unit/io/test_lazy_csv.py`. Shared scan plumbing:
  `test_scan.py`, `test_scan_options.py`, `test_multiscan.py`,
  `test_scan_lines.py`.
- Fixtures: real CSV files under `py-polars/tests/unit/io/files/`
  (`foods1.csv`…`foods5.csv`, `small.csv`, `empty.csv`, `only_header.csv`,
  `gzipped.csv.gz`, `zstd_compressed.csv.zst`, `graph-data/*.csv`).
- **Chunk-boundary stress harness**: a `chunk_override` fixture
  (`test_csv.py:39-40`) is parametrized `["chunk-size-default", "chunk-size-7"]`
  and threaded into nearly every test, so each test runs once normally and once
  with a tiny 7-byte read chunk. This deliberately forces records to straddle
  buffer boundaries — directly relevant to any importer that does its own
  chunked reads.
- **Streaming/async cross-run**: many tests carry
  `@pytest.mark.may_fail_auto_streaming` (the `read_csv`→`scan_csv` dispatch
  path), and behavior is sometimes branched on
  `POLARS_AUTO_STREAMING`/`POLARS_FORCE_STREAMING` env vars
  (`test_csv.py:2146-2155`). The suite is run under multiple engine configs.
- Rust-side unit tests live inline in `schema_inference.rs` (`mod tests`,
  e.g. `test_infer_field_schema_i64_overflow`, `schema_inference.rs:341+`).

Organization is flat — one big file of focused `test_<topic>[_<issue#>]`
functions, frequently named after the GitHub issue they regression-guard
(`_20062`, `_22395`, `_10826`, `_1505`, `_8240`, `_19535`, etc.).

## Notable edge cases

Pathologies the suite explicitly stresses (function names verbatim):

- **Embedded newlines in quotes**: `test_skip_new_line_embedded_lines`
  (`:1297`), `test_csv_quoted_newlines_skip_rows_19535`, `test_csv_quoted_missing`.
- **Ragged lines**: `test_csv_ragged_lines` (`:1969`),
  `test_csv_ragged_lines_20062` (`:2504`, 22-column with short + long rows),
  `test_csv_no_header_ragged_lines_1505` (dynamic width growth),
  `test_trailing_separator_8240` (trailing sep → extra all-null column).
- **BOM**: `test_utf8_bom` (stripped), `test_invalid_utf8_bom` (kept as data),
  `test_write_csv_bom` (`b"\xef\xbb\xbf"`).
- **Encodings**: `test_read_csv_encoding` (Big5), `test_read_csv_encoding_lossy`
  (windows-1252 lossy → `�`), `test_invalid_utf8_in_schema` (lossy inference,
  failing execution).
- **Compression**: `test_compressed_csv` (gzip/zlib/zstd), `test_partial_decompression`.
- **Decimal comma / scientific**: `test_read_csv_float_type_decimal_comma`
  (`:2259`), `test_read_csv_decimal_type_decimal_comma_24414` (`:2266`),
  `test_csv_float_parsing` (`:343`, signs, leading/trailing dot, `inf`, `NaN`,
  `1e27`/`1E65`/`1e-28`).
- **Mixed-type / inference fallback**: `test_csv_no_date_dtype_because_string`
  (date mixed with non-date → String), `test_error_message` (i64 then float),
  `test_inferred_datetime_format_mixed` (`:1405`).
- **Integer widths / leading zeros**: `test_csv_int_types` (`:278`, u8…i128,
  leading zeros, negatives). Note: leading-zero values like ZIP codes infer as
  `Int64` and lose the zero unless forced to `String` via override.
- **All-null / quoted-null columns**: `test_csv_single_categorical_null`,
  `test_csv_null_values` (`:149`, `na`, `n/a`, `\N`, per-column dict),
  `test_empty_string_missing_round_trip`, `test_csv_missing_utf8_is_empty_string`.
- **Duplicate / empty / header-only**: `test_duplicated_columns` (dedup to
  `name_duplicated_0`, `schema_inference.rs:74-87`), `test_only_empty_quote_string`
  (`b'""'` → empty-named header), `test_only_header_with_newline`,
  `test_single_char_input_25908`, `test_header_only_column_selection_17173`.
- **Headerless**: `test_csv_no_header_ragged_lines_1505`,
  `test_csv_scan_new_columns_less_than_original_columns`.
- **CRLF / EOL**: `test_skip_crlf`, `test_different_eol_char` (custom `;` EOL),
  `test_csv_no_new_line_last`, `test_csv_double_newline`.
- **Datetime zoo**: `test_datetime_parsing_default_formats` (DMY variants),
  `test_tz_aware_try_parse_dates` (→ UTC), `test_fallback_chrono_parser`
  (`2021-1-1`), `test_csv_try_parse_dates_leading_zero_8_digits_22167`,
  `test_quoted_date`, `test_date_pattern_with_datetime_override_10826`.
- **Whitespace sensitivity**: `test_preserve_whitespace_at_line_start`,
  `test_csv_field_schema_inference_with_whitespace` (surrounding whitespace
  changes the inferred type — Polars does **not** strip before inference),
  `test_csv_whitespace_separator_at_start_do_not_skip`.
- **Comments**: `test_csv_multi_char_comment` (`##`),
  `test_csv_invalid_quoted_comment_line`, `test_csv_comment_after_header_25841`,
  `test_csv_skip_rows_with_interleaved_comments_25840`.
- **SIMD path**: `test_stop_split_fields_simd_23651`,
  `test_csv_malformed_quote_in_unenclosed_field_22395` (forces both the <64-byte
  scalar and >64-byte SIMD field-splitting code paths).

## Takeaways for tallyman

1. **Pin schema explicitly; don't trust the 100-row window.** Polars infers from
   only the first 100 rows by default. tallyman already mandates an explicit
   schema for `deferred_read_csv` (`src/tallyman_mcp/server.py:329-341`) — keep
   that. The failure mode is not at inference time but at parse time, deep into
   the file: `ComputeError: could not parse `<v>` as dtype `<t>` at column
   '<name>' (column number N)`. Surface that message structure to users
   verbatim; it names the column and the offending value.

2. **Issue #141 (name vs position):** route override channels by intent, not by
   convenience. In Polars, `schema` and **list**-form `schema_overrides` are
   **positional**; **dict**-form `schema_overrides` is **by NAME**
   (`schema_inference.rs:201-207`: "Apply schema_overwrite by column name
   only"). If tallyman's `tallyman_read_csv` accepts a column→dtype mapping,
   make it a dict so it binds by name and tolerates reordered/renamed columns;
   reserve list/positional form for headerless input. Beware the historical
   same-length-dict-treated-as-positional bug (#20903) — assert name-binding in
   a test.

3. **Issue #142 (ragged lines) is asymmetric.** Long rows (more fields than the
   schema) **raise** unless `truncate_ragged_lines=True`; short rows are
   silently **null-filled**; headerless input **grows** the schema to the widest
   row. If tallyman wants a single predictable policy, it must choose and enforce
   it — Polars' default differs by direction and by header presence. Decide
   whether over-wide rows should error (data-quality signal) or truncate
   (lenient ingest), and set `truncate_ragged_lines` accordingly per call.

4. **No reject/quarantine table exists.** Polars offers only raise-or-null-fill
   (`ignore_errors`) plus `truncate_ragged_lines`; there is no rejected-rows side
   output and only the malformed-quote case warns instead of failing. If
   tallyman needs per-row error capture, build it as a two-pass strategy: read
   all-`String` (`infer_schema=False`/`infer_schema_length=0`), then cast
   column-by-column capturing failures — the docstrings themselves recommend the
   all-String read as the debugging entry point (`functions.py:164-170`).

5. **Type-merge rules are simple and worth mirroring.** Per column: a single
   type wins; `{int, float}`→float; otherwise→`String`
   (`finish_infer_field_schema`, `schema_inference.rs:230-258`). `String` is the
   safe universal fallback. Floats are matched before ints; dates only when
   `try_parse_dates=True` and only after numeric, so date-shaped columns stay
   `String` unless asked. Null tokens are removed before inference, so
   `null_values` won't corrupt a column's inferred type.

6. **Leading-zero / ZIP-code columns silently lose zeros** — they infer `Int64`.
   Any column tallyman treats as an identifier (zip, FIPS, account number) must
   be forced to `String` in the schema, or the leading zero is gone before the
   user sees the grid.

7. **Borrow the chunk-boundary test harness.** Polars parametrizes essentially
   every CSV test over a 7-byte read chunk (`chunk_override`, `test_csv.py:39`)
   to catch records straddling buffer boundaries. If tallyman does any chunked
   reading or streaming over the importer, replicate this: run the fixture suite
   once normal, once with a pathologically small chunk.

8. **`scan_csv` is the right primitive for large files**, but its override API is
   stricter (dict-only `schema_overrides`, utf8/utf8-lossy only) and inference
   defers to collect time with pushdown. tallyman's xorq layer already prefers
   deferred/scan reads (`src/tallyman_xorq/source_cache.py`); a list-form
   override that works eagerly will raise `TypeError: expected 'schema_overrides'
   dict, found 'list'` on the lazy path — another reason to standardize on
   dict/name binding (#141).

9. **Encoding default is strict UTF-8.** Non-UTF8 files need an explicit
   `encoding=`, and anything beyond `utf8`/`utf8-lossy` is decoded in Python in
   memory first (losing streaming benefit). For robust ingest, detect-or-declare
   encoding up front rather than letting an `InvalidOperationError: file encoding
   is not UTF-8` surface mid-parse.
