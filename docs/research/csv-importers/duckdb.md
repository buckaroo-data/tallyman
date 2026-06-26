# DuckDB CSV Import — Research Note

Reference for tallyman's CSV-importer architecture decision. DuckDB is the single
most relevant prior art because it ships a real, production-grade **CSV sniffer**
plus a **rejects-table** mechanism that quarantines bad rows into queryable tables
— the exact "import, but tell me what broke" model tallyman wants.

Primary sources (DuckDB stable docs, current as of v1.x, 2026-06):

- Overview / full option list: <https://duckdb.org/docs/stable/data/csv/overview>
- Auto detection (the sniffer): <https://duckdb.org/docs/stable/data/csv/auto_detection>
- Reading faulty CSV files (rejects tables): <https://duckdb.org/docs/stable/data/csv/reading_faulty_csv_files>
- Tips: <https://duckdb.org/docs/stable/data/csv/tips>
- Pollock robustness benchmark write-up: <https://duckdb.org/2025/04/16/duckdb-csv-pollock-benchmark>
- Test suite: <https://github.com/duckdb/duckdb/tree/main/test/sql/copy/csv>

Two entry points matter: `read_csv(...)` (table function, returns a relation, used
in `FROM read_csv('f.csv', ...)`) and `COPY tbl FROM 'f.csv' (...)` (loads into an
existing table, takes the table's schema instead of sniffing). `read_csv_auto` is a
thin alias for `read_csv` with `auto_detect = true` — see the inference section.

---

## Config options

Every read-side option, grouped by category. Defaults quoted exactly from
`data/csv/overview`. "type" is the SQL type DuckDB expects for the option value.

### Dialect: delimiter / quote / escape / newline / comment

| Option | Type | Default | Purpose |
|---|---|---|---|
| `delim` / `sep` | VARCHAR | `,` | Column separator within each line. |
| `quote` | VARCHAR | `"` | String used to quote values. |
| `escape` | VARCHAR | `"` | String used to escape the quote char inside a quoted value (default = doubled-quote escaping, RFC-4180 style). |
| `new_line` | VARCHAR | (sniffed) | Newline terminator: `\r`, `\n`, or `\r\n`. |
| `comment` | VARCHAR | (empty) | Char that starts a comment; matching lines are skipped. |
| `multi_byte_search` / multi-char delim | VARCHAR | n/a | `delim` may be multi-character (e.g. `::`); the suite has a `multidelimiter/` dir for this. |

### Header

| Option | Type | Default | Purpose |
|---|---|---|---|
| `header` | BOOL | `false` (but auto-detected when `auto_detect=true`) | First line holds column names. |
| `names` / `column_names` | VARCHAR[] | (empty) | Supply column names as a list (use when the file is headerless). |
| `normalize_names` | BOOL | `false` | Strip non-alphanumeric chars from column names to make them SQL-safe. |
| `column_names` aliasing | — | — | `names`, `column_names` are aliases. |

### Types / schema

| Option | Type | Default | Purpose |
|---|---|---|---|
| `columns` | STRUCT | (empty) | Full `{name: type}` schema. **Setting this disables dialect+type+header auto-detection for those columns.** |
| `types` / `dtypes` / `column_types` | VARCHAR[] or STRUCT | (empty) | Override detected types, either positionally (`['INT','VARCHAR','DATE']`) or by name (`{'quarter':'INTEGER'}`). Type-detection still runs for unspecified columns. |
| `all_varchar` | BOOL | `false` | Skip type detection entirely; every column is VARCHAR. |
| `auto_detect` | BOOL | `true` | Master switch for the sniffer. |
| `auto_type_candidates` | TYPE[] | see Inference | The ordered candidate set the sniffer tries during type detection. |

### Type-inference window

| Option | Type | Default | Purpose |
|---|---|---|---|
| `sample_size` | BIGINT | `20480` | Number of rows the sniffer inspects for dialect+type detection. `-1` = read the **entire file** (slow but exhaustive). |
| `files_to_sniff` | BIGINT | `10` | When globbing multiple files, how many to sniff for schema. |

### Null / NA handling

| Option | Type | Default | Purpose |
|---|---|---|---|
| `nullstr` / `null` | VARCHAR or VARCHAR[] | (empty) | String(s) treated as NULL. Accepts a **list** of multiple null tokens (`['', 'NA', 'NULL']`). |
| `allow_quoted_nulls` | BOOL | `true` | Whether a quoted form of the null string (`"NA"`) also becomes NULL. |
| `force_not_null` | VARCHAR[] | `[]` | Columns whose values are never matched against the null string (so `""` stays empty-string, not NULL). |

### Date / datetime parsing

| Option | Type | Default | Purpose |
|---|---|---|---|
| `dateformat` / `date_format` | VARCHAR | (sniffed) | `strptime`-style format for DATE columns. |
| `timestampformat` / `timestamp_format` | VARCHAR | (sniffed) | `strptime`-style format for TIMESTAMP columns. |

ISO-8601 is always tried first; the sniffer then walks a fixed priority list of
non-ISO date/timestamp formats (below).

### Encoding / BOM / compression

| Option | Type | Default | Purpose |
|---|---|---|---|
| `encoding` | VARCHAR | `utf-8` | One of `utf-8`, `utf-16`, `latin-1`. (Built-in set is small; other encodings need pre-conversion.) |
| `compression` | VARCHAR | `auto` | `none`, `gzip`, `zstd` — auto-detected from extension by default. BGZF gzip is supported (`test_bgzf_read.test`). |

UTF-8 BOM is stripped automatically. Non-seekable compressed/stdin inputs change
sniffer sampling behavior (see Inference).

### Decimal / thousands

| Option | Type | Default | Purpose |
|---|---|---|---|
| `decimal_separator` | VARCHAR | `.` | Decimal point char; set to `,` for European numerics. |
| `thousands` | VARCHAR | (empty) | Thousands-grouping char to strip from numerics (e.g. `,` so `1,000` → `1000`). |

### Error / bad-line handling

| Option | Type | Default | Purpose |
|---|---|---|---|
| `ignore_errors` | BOOL | `false` | Skip rows that error instead of aborting the whole read. |
| `store_rejects` | BOOL | `false` | Don't abort; skip bad rows AND record them in the reject tables. |
| `rejects_table` | VARCHAR | `reject_errors` | Name of the temp table holding per-error rows. |
| `rejects_scan` | VARCHAR | `reject_scans` | Name of the temp table holding per-scan dialect/config. |
| `rejects_limit` | BIGINT | `0` | Cap on rejects recorded per file; `0` = unlimited. |
| `strict_mode` | BOOL | `true` | RFC-4180 strictness. `false` = best-effort permissive parsing (recover unescaped quotes, over-long rows, mixed newlines) at the cost of correctness guarantees. |
| `null_padding` | BOOL | `false` | If a row has **fewer** columns than the schema, pad the missing trailing columns with NULL instead of erroring. |
| `max_line_size` / `maximum_line_size` | BIGINT | `2000000` (docs also cite the limit as 2,097,152 bytes) | Reject any physical line longer than this. |

### Performance / streaming / parallel

| Option | Type | Default | Purpose |
|---|---|---|---|
| `parallel` | BOOL | `true` | Use the parallel CSV reader. |
| `buffer_size` | BIGINT | `16 * max_line_size` | Read-buffer size in bytes. |
| `max_line_size` | BIGINT | `2000000` | Also bounds buffer sizing and per-thread work units. |

### Column selection / skipping / multi-file

| Option | Type | Default | Purpose |
|---|---|---|---|
| `skip` | BIGINT | `0` | Skip N lines at the start of each file (before header detection). |
| `filename` | BOOL / VARCHAR | `false` | Add a column with the source file path of each row. |
| `hive_partitioning` | BOOL | (auto) | Parse `key=value` path segments into columns. |
| `union_by_name` | BOOL | `false` | Align columns across multiple files by name, not position; missing columns NULL-filled. |

### COPY vs read_csv

`COPY tbl FROM 'f.csv' (...)` accepts the same dialect/null/format options but:
- It does **not** sniff types — it uses the target table's schema (faster, more
  predictable; recommended in the tips page).
- It does not take `max_line_size`, `encoding`, or `parallel` the same way.
- Option spellings differ: `COPY` uses `delimiter`, `date_format`, `timestamp_format`
  as the canonical names where `read_csv` uses `delim`, `dateformat`, `timestampformat`
  (the underscore/no-underscore forms are mutual aliases).

---

## Inference

DuckDB's sniffer is a multi-stage state machine. Default sample is **20,480 rows**
(`sample_size`, overridable; `-1` reads the whole file). For seekable disk files the
sniffer **jumps to multiple offsets** and samples chunks from across the file; for
non-seekable inputs (gzip stream, stdin) it can only sample from the **start**, which
is the root cause of most "worked on a small file, failed on a big one" bugs.

### Stage order

1. **Dialect detection** — pick `delim`, `quote`, `escape`, `new_line`.
2. **Type detection** — pick a type per column.
3. **Header detection** — decide whether row 0 is a header.
4. **Type replacement** — apply any user `types`/`columns` overrides on top of the sniffed result.

(`auto_detection` doc, "Detection Steps".)

### Stage 1 — dialect detection

Tries a grid of candidates:

- Delimiter candidates: `,` `|` `;` `\t`
- Quote candidates: `"` `'` (none)
- Escape candidates: `"` `'` `\` (none)

It runs each combination over the sample and scores by **consistency of column count
across rows** and then **highest column count**. The dialect that yields the most rows
with a stable, maximal column count wins. This is why a file whose first rows happen
to be consistent under the wrong delimiter can mis-sniff.

### Stage 2 — type detection

For each column, DuckDB tries to cast the sampled values against an ordered candidate
list and keeps the **highest-priority type every sampled value satisfies**. Default
priority (highest → lowest):

```
SQLNULL → BOOLEAN → BIGINT → DOUBLE → TIME → DATE → TIMESTAMP → TIMESTAMPTZ → VARCHAR
```

The docs phrase it as: NULL, BOOLEAN, then numerics and temporals, with VARCHAR as the
universal fallback ("Everything can be cast to VARCHAR, therefore this type has the
lowest priority"). A single value that fails the current candidate **demotes the whole
column** to the next candidate. `auto_type_candidates` lets you add finer numeric types
(`TINYINT`, `SMALLINT`, `INTEGER`, `FLOAT`, `DECIMAL`) or restrict the set.

Date/timestamp format inference walks a fixed priority list, ISO-8601 first, then:

- Dates: `%y-%m-%d`, `%Y-%m-%d`, `%d-%m-%y`, `%d-%m-%Y`, `%m-%d-%y`, `%m-%d-%Y`
- Timestamps: ISO-8601, then `%Y-%m-%d %H:%M:%S` variants, `%d-%m-%Y %H:%M:%S`,
  `%m-%d-%Y %I:%M:%S %p`, `%Y-%m-%d %H:%M:%S.%f`, etc.

Ambiguity (e.g. `03-04-2020` — is it Mar-4 or Apr-3?) is resolved by scanning later
rows: a day value > 12 eliminates the `%m-%d` interpretations.

### Stage 3 — header detection

The sniffer compares row 0's types against the inferred column types. If row 0 is all
VARCHAR but the column type is numeric/date, row 0 is a header. **Known limitation:**
if every column is VARCHAR, the header can't be distinguished by type, so DuckDB
**assumes a header exists** by default — override with `header = false`. (`tips` page
explicitly calls this out.)

### Stage 4 — type replacement

User-supplied `types`/`columns`/`dtypes` are applied last. `columns` short-circuits the
whole sniffer for the named columns; `types` overrides only the type stage while still
letting dialect/header detection run.

### sniff_csv()

`FROM sniff_csv('my_file.csv')` (optionally `sample_size = N`) runs the sniffer alone
and returns one row describing what it found. Columns:

`Delimiter, Quote, Escape, NewLineDelimiter, Comment, SkipRows, HasHeader, Columns
(LIST of {name,type} structs), DateFormat, TimestampFormat, UserArguments, Prompt`.

The **`Prompt` column** is the killer feature for a tool like tallyman: it emits a
ready-to-paste `read_csv(...)` call with every detected parameter pinned and
`auto_detect=false`, e.g.

```sql
.mode line
SELECT Prompt FROM sniff_csv('my_file.csv');
-- Prompt = FROM read_csv('my_file.csv', auto_detect=false, delim=',', quote='"',
--          escape='"', new_line='\n', skip=0, header=true, columns={...});
```

This is exactly the "show me what you inferred, let me freeze/edit it" UX. tallyman
should produce an equivalent of `Prompt`.

### read_csv vs read_csv_auto

`read_csv_auto(...)` ≡ `read_csv(..., auto_detect=true)`. With plain `read_csv` you can
still get full detection (auto_detect defaults to true); `read_csv_auto` exists mostly
for explicitness and historical reasons. Either becomes "manual" the moment you pass
`columns=` or `auto_detect=false`.

---

## Error handling

Default behavior (`strict_mode=true`, no `ignore_errors`, no `store_rejects`): the
**first** structural or cast error **aborts the entire read** with a Conversion Error.

### Error categories (recorded by reject tables)

| `error_type` | Trigger |
|---|---|
| `CAST` | A value can't convert to the column's type (`"The 90s"` → DATE). |
| `MISSING COLUMNS` | Row has fewer fields than the schema. |
| `TOO MANY COLUMNS` | Row has more fields than the schema. |
| `UNQUOTED VALUE` | A quoted field has content after the closing quote / stray quotes. |
| `LINE SIZE OVER MAXIMUM` | Physical line exceeds `max_line_size`. |
| `INVALID ENCODING` / `INVALID UNICODE` | Bytes outside the declared encoding. |

### What a real error looks like

A default-mode cast failure prints a multi-part message — line number, the original
raw line, the offending column, a hint block, and a dump of the scanner config. Docs
example (`reading_faulty_csv_files`):

```
Conversion Error: CSV Error on Line: 5648
Original Line: Pedro,The 90s
Error when converting column 'birth_date'. ...
```

The reject-table form of the same per-error message (from
`test/sql/copy/csv/rejects/csv_rejects_read.test`):

```
Error when converting column "col1". Could not convert string "BBB" to 'INTEGER'
Error when converting column "a".   Could not convert string "oh no" to 'BIGINT'
Expected Number of Columns: 2 Found: 1          -- a MISSING COLUMNS error
```

A `columns`-mismatch error (from `csv_error_message.test`):

```
Columns are set as: "columns = { 'A':'VARCHAR','B':'VARCHAR','C':'VARCHAR','D':'VARCHAR'}",
and they contain: 4 columns. It does not match the number of columns found by the
sniffer: 3. Verify the columns parameter is correctly set.
```

A type-override mismatch:

```
Column at position: 0 Set type: INTEGER Sniffed type: VARCHAR
```

### The three knobs

- **`ignore_errors = true`** — silently drop any row that errors; return the good rows.
  Pitfall the docs flag explicitly: **projection pushdown can mask errors.** If you
  `SELECT name` and the bad value is in `age`, DuckDB never parses `age`, so the "bad"
  row is returned anyway. Row counts therefore depend on which columns you project.
  (See also issue #19880: `ignore_errors=true` can drop rows that strict mode would
  have parsed fine — error-tolerant mode is not a strict superset.)

- **`store_rejects = true`** — like `ignore_errors`, but every skipped row is logged
  into two temp tables you can query afterward. This is the quarantine model.

- **`strict_mode = false`** — permissive RFC-4180 relaxation. Recovers unescaped
  quotes, over-wide rows (drops extras), mixed `\n`/`\r\n`. The docs warn there is **no
  correctness guarantee** in this mode — "it's impossible to define what a correct
  result is" for a non-standard file. Treat it as best-effort salvage, not a parser you
  trust blind.

- **`null_padding = true`** — orthogonal to the above; fills short rows' trailing
  columns with NULL rather than raising MISSING COLUMNS.

### Reject tables (the tallyman-relevant part)

`store_rejects=true` populates two TEMP tables.

**`reject_scans`** — one row per scanner, the full dialect/config snapshot so you can
reproduce or audit how the file was read. Columns (per `reading_faulty_csv_files`):

```
scan_id, file_id, file_path, delimiter, quote, escape, newline_delimiter,
skip_rows, has_header, columns, date_format, timestamp_format, user_arguments
```

**`reject_errors`** — one row **per error** (a single CSV line with two bad columns
yields two rows). Columns:

```
scan_id, file_id, line, line_byte_position, byte_position,
column_idx, column_name, error_type, csv_line, error_message
```

Join `reject_errors` to `reject_scans` on `(scan_id, file_id)`. Example shape of a
recorded error row:

| scan_id | file_id | line | column_idx | column_name | error_type | csv_line | error_message |
|---|---|---|---|---|---|---|---|
| 5 | 0 | 2 | 2 | age | CAST | `Oogie Boogie, three` | `Error when converting column 'age'. Could not convert string ' three' to 'INTEGER'` |

`rejects_limit` caps rows per file; `rejects_table` / `rejects_scan` rename the tables
(useful to keep two reads' rejects separate — see `csv_rejects_two_tables.test`).

---

## Test suite

Tests live in **`test/sql/copy/csv/`** in the DuckDB repo, written in DuckDB's
`sqllogictest` `.test` format (input SQL + expected result blocks). `.test_slow`
variants are large/long-running and gated out of the fast CI lane. Source CSV fixtures
sit under `data/csv/` (referenced by relative path from the test files).

Subdirectories:

| Dir | Focus |
|---|---|
| `rejects/` | The reject-table machinery (most relevant to tallyman — see below). |
| `auto/` | Auto-detection / sniffer behavior. |
| `parallel/` | Parallel reader correctness vs the single-thread reader. |
| `multidelimiter/` | Multi-byte / multi-char delimiters. |
| `glob/` | Multi-file globbing, `union_by_name`. |
| `pollock/` | Fixtures from the Pollock robustness benchmark (pathological real-world CSVs). |
| `duck_fuzz/`, `afl/` | Fuzzer-generated inputs. |
| `code_cov/`, `batched_write/`, `overwrite/` | Coverage, write path, COPY-overwrite. |

`rejects/` contents (the quarantine tests):

```
csv_rejects_read.test                       core reject_errors/reject_scans columns + values
csv_rejects_auto.test                       rejects under auto-detect
csv_rejects_two_tables.test                 two reads into separately-named reject tables
csv_rejects_flush_cast.test                 cast-error flushing across buffers
csv_rejects_flush_message.test              message flushing
csv_rejects_maximum_line.test               LINE SIZE OVER MAXIMUM
csv_rejects_truncate_line_invalid_utf8.test invalid UTF-8 truncation
csv_incorrect_columns_amount_rejects.test   MISSING / TOO MANY COLUMNS
csv_unquoted_rejects.test                   UNQUOTED VALUE
test_invalid_utf_rejects.test               encoding errors
test_multiple_errors_same_line.test         multiple reject rows per CSV line
test_mixed.test                             mixed error types in one file
csv_buffer_size_rejects.test_slow           rejects across buffer boundaries
test_invalid_parameters.test                bad rejects_* parameter combos
```

Notable top-level test files (selection from the ~100 in the dir):

- Error messages: `csv_error_message.test`, `test_csv_error_message_type.test`,
  `inconsistent_cells_error.test`, `test_cast_error.test_slow`,
  `null_value_wrong_type.test`.
- Sniffer/types: `csv_dtypes.test`, `csv_copy_sniffer.test`, `test_auto_date.test`,
  `test_auto_detection_headers.test`, `leading_zeros_autodetect.test`,
  `plus_autodetect.test`, `integer_exponent.test`, `csv_decimal_separator.test`,
  `csv_enum.test`, `csv_mixed_casts.test`, `test_column_inconsistencies.test`.
- Quoting/escaping: `test_all_quotes.test`, `empty_string_quote.test`,
  `relaxed_quotes.test`, `csv_quoted_newline_incorrect.test`,
  `test_allow_quoted_nulls_option.test`, `unquoted_escape/`.
- Structural/edge: `csv_null_byte.test`, `null_terminator.test`,
  `csv_null_padding.test`, `null_padding_big.test`, `struct_padding.test`,
  `csv_line_too_long.test`, `maximum_line_size.test_slow`, `test_big_line.test_slow`,
  `test_big_header.test`, `empty_first_line.test`, `test_csv_no_trailing_newline.test`,
  `test_copy_many_empty_lines.test`, `csv_windows_mixed_separators.test`,
  `test_csv_duplicate_columns.test`, `test_csv_column_count_mismatch.test_slow`.
- Misc: `csv_hive.test`, `recursive_read_csv.test`, `union_by_name` tests,
  `csv_projection_pushdown.test` (the projection-masking-errors behavior).

---

## Notable edge cases

The funky cases the suite + Pollock benchmark stress, and how DuckDB handles each:

- **Embedded newlines inside quotes** — handled natively; `csv_quoted_newline_incorrect.test`
  covers the failure mode where a stray quote makes the parser swallow a real line break.
- **BOM** — UTF-8 BOM stripped automatically; UTF-16 via `encoding='utf-16'`.
- **Encodings** — only `utf-8`/`utf-16`/`latin-1` built in; anything else must be
  transcoded first. Invalid bytes raise `INVALID ENCODING`/`INVALID UNICODE`
  (`test_invalid_utf_rejects.test`).
- **Decimal-comma** — `decimal_separator=','`; `csv_decimal_separator.test`.
- **Thousands separator** — `thousands=','` strips grouping; numerics like `1,234.56`.
- **Mixed-type column** — demotes to the lowest common type (often VARCHAR). The trap:
  the bad value lies **past `sample_size`**, so the column sniffs as INT then a row 30k
  in fails the cast. Fix is `sample_size=-1` or an explicit `types=`. Covered by
  `csv_mixed_casts.test`, `test_cast_error.test_slow`.
- **All-null column** — sniffs as the lowest-priority candidate; risky to widen later.
- **Leading-zero / ZIP codes** — `leading_zeros_autodetect.test`: `01234` is kept as
  VARCHAR rather than promoted to INT (so leading zeros survive), a deliberate sniffer
  rule.
- **`+`-prefixed numbers** — `plus_autodetect.test`.
- **Scientific notation** — `integer_exponent.test` (`1e3` style values).
- **Duplicate / empty headers** — `test_csv_duplicate_columns.test`; DuckDB
  de-duplicates / synthesizes names.
- **Headerless files** — header auto-assumed only when types disambiguate; otherwise
  pass `header=false` + `names=[...]`.
- **Trailing delimiter / ragged rows** — `MISSING`/`TOO MANY COLUMNS`; recover with
  `null_padding=true` (short rows) or `strict_mode=false` (drops extras).
- **Huge/wide files & long lines** — `max_line_size` (default ~2 MB) bounds a line;
  `csv_line_too_long.test`, `maximum_line_size.test_slow`, `test_big_line.test_slow`.
- **Datetime format zoo** — ISO-first priority list with day>12 disambiguation;
  `test_auto_date.test`.
- **Quoted nulls** — `allow_quoted_nulls` decides whether `"NA"` is NULL;
  `test_allow_quoted_nulls_option.test`.
- **Null bytes / null terminators mid-field** — `csv_null_byte.test`,
  `null_terminator.test`.
- **Windows mixed separators** — files mixing `\n` and `\r\n`;
  `csv_windows_mixed_separators.test` (recoverable in permissive mode).
- **Multiple tables / multiple header blocks in one file** — the Pollock "multiple
  tables" category; only partially recoverable, no clean answer.
- **Pollock benchmark result** — DuckDB scored **9.961 / 10 ("simple score", ~99.6%)
  with full config**, top among tested systems, by combining the sniffer +
  `strict_mode=false` + `null_padding` + `ignore_errors`.

---

## Takeaways for tallyman

1. **Split the read into stages exactly like the sniffer does** — dialect → types →
   header → user-override. Each stage independently overridable. This is the
   architecture that lets a user freeze one decision (delimiter) while re-running the
   rest.

2. **Sampling window is THE foundational correctness knob.** DuckDB's 20,480-row
   default plus multi-offset jumping for seekable files is a deliberate
   speed/correctness trade. Copy both: (a) a bounded default, (b) sample from multiple
   file offsets not just the head, and (c) a `sample_size=-1`/"scan whole file" escape
   hatch. The single most common DuckDB CSV bug is "type failed past the sample window"
   — design so the user can see and bump the window.

3. **Ship a `sniff_csv().Prompt` equivalent.** Emitting the fully-pinned, re-runnable
   config (every option explicit, auto-detect off) is the cleanest "explain what you
   inferred and let me edit it" UX. Make tallyman's importer able to print/round-trip
   its own inferred config.

4. **Quarantine, don't just skip.** The `reject_scans` + `reject_errors` two-table model
   is the gold standard and is directly what tallyman wants: good rows flow through, bad
   rows land in a queryable side-channel with `line`, `byte_position`, `column_name`,
   `error_type`, the raw `csv_line`, and a human message. Adopt this schema almost
   verbatim. Keep the per-scan config snapshot separate from per-error rows, joined by
   `(scan_id, file_id)`.

5. **Have an explicit `error_type` enum** (`CAST`, `MISSING COLUMNS`, `TOO MANY
   COLUMNS`, `UNQUOTED VALUE`, `LINE SIZE OVER MAXIMUM`, `INVALID ENCODING`). Structured
   error categories beat free-text — they let the UI group and count failures.

6. **One CSV line can produce many errors** — model rejects as per-(line,column) rows,
   not per-line, so a row with two bad columns yields two records.

7. **Beware projection pushdown masking errors.** DuckDB's own footgun: if you only read
   the columns the user selected, errors in unselected columns vanish and row counts
   become projection-dependent. tallyman should decide deliberately whether validation
   runs over all columns or only projected ones, and surface that choice — don't let it
   be an accidental side effect of query planning.

8. **`ignore_errors` is not a strict superset of strict parsing** (DuckDB #19880): the
   permissive path can drop rows the strict path would have read. If tallyman offers a
   "tolerant" mode, test that it never silently loses rows the strict mode keeps.

9. **Leading-zero / ZIP / scientific-notation rules are deliberate, not bugs.** Keeping
   `01234` as text and recognizing `1e3` are explicit sniffer rules. tallyman needs the
   same opinionated set, surfaced and overridable.

10. **Permissive parsing has no correctness guarantee — say so.** DuckDB documents that
    `strict_mode=false` cannot define a "correct" result. If tallyman offers salvage
    mode, label it best-effort and always pair it with the rejects log so the user can
    audit what was guessed.

11. **Borrow the test-suite shape.** Organize tallyman's CSV tests by pathology, mirror
    DuckDB's `rejects/` subdir for the quarantine path, and seed fixtures from the
    Pollock corpus categories (inconsistent rows, mixed newlines, multiple headers,
    unescaped quotes, multibyte delimiters, multiple tables) for a real robustness bar.
