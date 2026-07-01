# CSV importers, side by side — DuckDB vs Polars vs Pandas

Cross-tool synthesis of the three deep-dive notes in this directory:

- [`duckdb.md`](./duckdb.md) — `read_csv` / `read_csv_auto` / `COPY FROM`, sniffer + reject tables.
- [`polars.md`](./polars.md) — `read_csv` / `scan_csv` / `read_csv_batched`.
- [`pandas.md`](./pandas.md) — `read_csv` across the c / python / pyarrow engines.

Goal: pick the model that best fits tallyman's target — a **deterministic parse**
with a **great error-and-suggestion UX** — and pin down the two semantic decisions
that are already open as issues: schema binding **by name vs by position** (#141)
and **ragged-row** strictness (#142).

The companion file [`edge-cases.md`](./edge-cases.md) is the pathology corpus to
port into tallyman's own test suite.

---

## Config-option matrix

Rows are option *concepts*; cells give each tool's spelling, default, and any
name-vs-position note. "—" means the tool has no first-class option for that
concept. Defaults are quoted from each tool's note.

| Concept | DuckDB (`read_csv`) | Polars (`read_csv`/`scan_csv`) | Pandas (`read_csv`) |
|---|---|---|---|
| **Delimiter** | `delim`/`sep`, default `,`; multi-char allowed; **sniffed** when `auto_detect=true` | `separator`, default `,`; exactly 1 byte; **never sniffed** | `sep`/`delimiter`, default `,`; regex/multi-char → python engine; `sep=None` → `csv.Sniffer` (python engine only) |
| **Quote char** | `quote`, default `"`; sniffed | `quote_char`, default `"`; `None` disables quoting | `quotechar`, default `"`; `quoting` mode + `doublequote`/`escapechar` |
| **Escape** | `escape`, default `"` (doubled-quote, RFC-4180); sniffed | derived from `quote_char` (doubled) / `eol`; no separate escape arg | `escapechar`, default `None`; `doublequote=True` |
| **Newline / EOL** | `new_line`, sniffed (`\r`,`\n`,`\r\n`) | `eol_char`, default `\n` (CRLF handled, `\r` stripped) | `lineterminator`, default `None` (C engine, single char) |
| **Header present** | `header`, default `false` but **auto-detected**; all-VARCHAR file → assumes header | `has_header`, default `True`; no header → `column_1..N` | `header`, default `'infer'` (row 0 unless `names`); not statistically sniffed |
| **Supply column names** | `names`/`column_names` (list, headerless) | `new_columns` (rename post-parse); `with_column_names` callable (`scan_csv`) | `names` (list; duplicates mangled `x`,`x.1`) |
| **Full explicit schema** | `columns` = `{name:type}` → **disables** dialect+type+header sniff for those cols; matched **by name** | `schema` → **no inference**; matched **by POSITION/order** | `dtype` dict → per-col; matched **by NAME**; `names`+`header=None` sets the labels |
| **Partial dtype override** | `types`/`dtypes`/`column_types`: list = **positional**, struct = **by name**; other cols still sniffed | `schema_overrides`: `dict` = **by NAME**, `list` = **by POSITION** (first N); `dtypes=` deprecated alias | `dtype`: scalar / `{col:dtype}` **by name** / `defaultdict` |
| **All-string escape hatch** | `all_varchar=true` | `infer_schema=False` / `infer_schema_length=0` | `dtype=str` |
| **Inference window** | `sample_size`, default **20480** rows; `-1` = whole file; multi-offset jumps for seekable files | `infer_schema_length`, default **100** (`N_INFER_DEFAULT`); `None` = whole file; `0` = all String | **whole column** by default; `low_memory=True` (default) → per-buffer (`largest pow2 < 2**20/ncols`) |
| **Null tokens** | `nullstr`/`null` (str or list); `allow_quoted_nulls` (default true); `force_not_null` per col | `null_values` (str / list / `dict[col,str]`); `missing_utf8_is_empty_string` | `na_values` (scalar/list/dict) ∪ 19-token default set; `keep_default_na`, `na_filter` |
| **Date parsing** | **auto** when sniffing; `dateformat`/`timestampformat`; ISO-first priority list | **opt-in** `try_parse_dates` (ISO-ish, tried last); else String | **opt-in** `parse_dates`; `date_format`, `dayfirst`; ISO fast-path |
| **Decimal separator** | `decimal_separator`, default `.` | `decimal_comma` (bool, switches FLOAT regex) | `decimal`, default `.` (length-1) |
| **Thousands separator** | `thousands`, default empty | — (no option) | `thousands`, default `None` |
| **Bad-line / ragged** | `null_padding` (short rows), `strict_mode=false` (drop extras), `ignore_errors`, `store_rejects` | `truncate_ragged_lines` (long rows), short rows **silently null-filled**, `ignore_errors` | `on_bad_lines` `error`/`warn`/`skip`/callable (long rows only); short rows padded NaN |
| **Reject / quarantine** | `store_rejects` → `reject_scans` + `reject_errors` temp tables; `rejects_limit` | **none** (raise or null-fill only) | **none** (raise / warn+drop / silent-drop) |
| **Encoding** | `encoding`: utf-8 / utf-16 / latin-1 (BOM auto-stripped) | `encoding`: utf8 / utf8-lossy / windows-1252(+lossy)…; non-utf8 decoded in Python first; `scan_csv` only utf8/utf8-lossy | `encoding` (BOM-aware utf-8-sig/16/32), `encoding_errors` policy |
| **Compression** | `compression`: none/gzip/zstd (auto by ext) | inferred from path ext; only when reading from a path | `compression`: infer/gzip/bz2/zip/xz/zstd/tar |
| **Skip rows** | `skip` (before header) | `skip_rows` (CSV records), `skip_lines` (raw), `skip_rows_after_header` | `skiprows` (int/list/callable), `skipfooter` (python only) |
| **Row limit** | (via SQL `LIMIT`) | `n_rows` (upper bound under threads) | `nrows` |
| **Comment char** | `comment` | `comment_prefix` (single → Single, multi → Multi) | `comment` (single char) |
| **Source-path column** | `filename` | `include_file_paths` (`scan_csv`) | — |
| **Multi-file union** | `union_by_name`, `hive_partitioning`, `files_to_sniff` | `glob` (default True) | — (loop yourself) |
| **Error tolerance master** | `ignore_errors` + `strict_mode` | `ignore_errors` | `on_bad_lines` + forced-`dtype` raises |
| **Reproducible config dump** | `sniff_csv().Prompt` (re-runnable `read_csv(...)` with every param pinned) | — | — |

### Reading the matrix

The three tools agree on the *vocabulary* of dialect options (delimiter, quote,
escape, newline, comment) and diverge sharply on three axes that matter to
tallyman:

1. **Does it sniff a dialect?** DuckDB yes (full grid-search). Pandas only via
   `csv.Sniffer` on the python engine when `sep=None`. Polars never — separator,
   quote, and EOL are fixed defaults you configure, full stop.
2. **How wide is the inference window?** Pandas whole-column (but silently
   windowed under `low_memory`); DuckDB a bounded 20480-row multi-offset sample;
   Polars a tiny 100-row head sample.
3. **What happens to bad rows?** DuckDB can quarantine them into queryable
   tables; Polars and Pandas can only raise, drop, or null-fill — the rejected
   rows are never handed back as data.

---

## Type inference: window vs whole-column vs sniffer

**DuckDB — staged sniffer over a bounded multi-offset sample.** A four-stage state
machine (`dialect → types → header → user-override`) over a 20480-row sample
(`sample_size`). For seekable disk files it samples chunks from *several offsets*
across the file, not just the head; non-seekable inputs (gzip stream, stdin) can
only see the start, which is the documented root cause of "small file worked, big
file failed." Type detection per column keeps the highest-priority type every
sampled value satisfies, demoting on the first failure, down a fixed ladder
`SQLNULL → BOOLEAN → BIGINT → DOUBLE → TIME → DATE → TIMESTAMP → TIMESTAMPTZ →
VARCHAR`. Dates auto-parse with an ISO-first priority list and `day>12`
disambiguation. `auto_type_candidates` tunes the ladder; `sample_size=-1` scans
the whole file. The killer feature is `sniff_csv().Prompt`: it emits a fully
pinned, re-runnable `read_csv(...)` call with `auto_detect=false` — exactly the
"here's what I inferred, edit it" round-trip tallyman wants.

**Polars — two-phase, tiny window, no dialect sniff.** A separate inference pass
(`schema_inference.rs`) scans `infer_schema_length` rows (default **100**) and
classifies each field in a fixed order: quoted→String, then `BOOLEAN_RE`,
`FLOAT_RE`, `INTEGER_RE` (i64→i128), then dates only if `try_parse_dates`, else
String. Floats are matched **before** ints. Per column the row-types are merged:
single type wins, `{Int,Float}→Float`, `{Int64,Int128}→Int128`, **any other mix →
String** (universal fallback). Null tokens are removed before inference so they
can't pollute a column's type. There is **no dialect sniffer at all** — if you
want delimiter detection you build it yourself. The failure mode is the same as
DuckDB's but with a much smaller window: inference picks `i64` from the first 100
rows, then the actual parse hits `1e5` 30k rows in and raises `could not parse
'1e5' as dtype 'i64' at column 'energy' (column number 3)`.

**Pandas — whole-column by default, silently windowed under `low_memory`.** The
tokenizer stores every field as a string and casts per column afterward in the
order `int64 → float64 → bool → object` (`uint64` as an overflow fallback;
QUOTE_NONNUMERIC drops int64). NaN/NA forces int→float. By default it infers from
the *entire* column — the most correct of the three — but the C engine ships
`low_memory=True`, which infers in independent internal buffers
(`buffer_lines = largest pow2 < 2**20/ncols`) and **concatenates without
reconciling**. A column that is int in one buffer and string in another comes back
as `object` with mixed Python `int`+`str` values, silently and with no warning.
`low_memory=False` (or the pyarrow engine) restores single-pass whole-column
typing. Dates are never auto-inferred; leading zeros are stripped to ints unless
`dtype=str`.

**For tallyman.** The three sit on a window spectrum: Polars 100 rows (fast,
fragile) → DuckDB 20480 multi-offset (the deliberate balance) → Pandas whole-column
(correct, memory-hungry, or silently windowed). tallyman's deterministic-parse
goal argues against *any* implicit window as the contract: tallyman already
**mandates an explicit schema** for its deferred read, which is the right call.
Where tallyman *does* infer (the import-wizard / suggestion path), copy DuckDB:
a bounded multi-offset sample, a visible window size, a "type changed past the
window" diagnostic, and a whole-file escape hatch. Never silently mix types the
way `low_memory` does.

---

## Error handling: raise vs null-fill vs skip vs reject-table

| Failure class | DuckDB | Polars | Pandas |
|---|---|---|---|
| First cast error (default) | **abort** whole read, multi-part Conversion Error | **abort** `ComputeError` naming column+value | inference path **coerces** (widens to object); forced-`dtype` **raises** |
| Tolerant value mode | `ignore_errors` (drop row) or `store_rejects` (drop + log) | `ignore_errors` → value becomes **null**, row kept | — (no value-level skip; only structural `on_bad_lines`) |
| Long row (extra fields) | `TOO MANY COLUMNS` error; `strict_mode=false` drops extras | `ComputeError: found more fields than defined` unless `truncate_ragged_lines` | `on_bad_lines`: error/warn/skip/callable |
| Short row (missing fields) | `MISSING COLUMNS` error; `null_padding=true` pads | **silently null-filled** (no error) | **silently NaN-padded** (not a bad line) |
| Rejected-rows side output | **yes** — `reject_scans` + `reject_errors`, joined on `(scan_id,file_id)`, one row **per error** | **no** | **no** |
| Permissive salvage | `strict_mode=false` (explicitly *no correctness guarantee*) | `truncate_ragged_lines` + `ignore_errors` only | engine-dependent; no salvage mode |
| Structured error categories | `error_type` enum (CAST, MISSING/TOO MANY COLUMNS, UNQUOTED VALUE, LINE SIZE OVER MAXIMUM, INVALID ENCODING) | free-text `ComputeError` strings | free-text, **engine-specific wording** for the same condition |

The shape of the three models:

- **DuckDB is the only one that quarantines.** `store_rejects=true` keeps the good
  rows flowing and lands each bad row in a queryable side-channel with `line`,
  `byte_position`, `column_name`, `error_type`, the raw `csv_line`, and a human
  message — one row *per error*, so a line with two bad columns yields two
  records. It also has the one documented footgun worth stealing-the-test-for:
  `ignore_errors` is **not** a strict superset of strict parsing (issue #19880),
  and projection pushdown can mask errors in unselected columns so row counts
  become projection-dependent.
- **Polars is binary**: raise on the first bad value, or null-fill with
  `ignore_errors`. No reject table, no row-drop mode, and only the malformed-quote
  case downgrades to a `UserWarning` instead of failing. The docstrings recommend
  an all-String read (`infer_schema=False`) as the debugging entry point.
- **Pandas is "fail-loud on structure, coerce-quietly on values"**: ragged long
  rows hit `on_bad_lines`, value mismatches widen the column silently — but bad
  rows are *raised on, warned+dropped, or silently dropped, never returned*. An
  `ignore_errors` mode that hands the rejected rows *back* is the genuine gap all
  three leave open.

---

## Best fit for tallyman

tallyman wants a **deterministic parse** plus a **great error-and-suggestion UX**.
Mapping that onto the three:

- **Determinism** rules out leaning on inference as the contract. Polars' 100-row
  window and Pandas' `low_memory` per-buffer typing both produce non-deterministic
  schemas as a function of where in the file the data sits. tallyman's existing
  decision to require an explicit schema is correct and should stay.
- **Great error UX** points squarely at **DuckDB's model**: the staged sniffer (so
  a user can freeze one decision and re-run the rest), the `sniff_csv().Prompt`
  re-runnable config dump (the "show me what you inferred, let me edit it"
  round-trip), and above all the **two-table reject model**. Adopt
  `reject_scans` (per-scan dialect/config snapshot) + `reject_errors` (per-error
  rows) nearly verbatim, joined on `(scan_id, file_id)`, with an explicit
  `error_type` enum so the UI can group and count instead of regex-ing free text.

So: **inference engine and reject model from DuckDB; the "always pin an explicit
schema" discipline that Polars and tallyman already enforce; Pandas' inspectable
19-token NA set and its structural-vs-value error split as the taxonomy.** The
one thing none of them ship — handing rejected rows back as data — is exactly the
feature tallyman should build, because the whole point is "import it, *and tell me
what broke*."

---

## The two open tallyman issues, per tool

### #141 — schema binding by NAME vs by POSITION

The bug: tallyman's reader moved from datafusion `deferred_read_csv` (which applied
the ibis schema **positionally**, renaming the CSV header to the schema's names) to
Polars `scan_csv(schema_overrides=...)` (which matches **by name**), so the
documented yfinance `Price→Date` rename now raises `ColumnNotFoundError`.

How each tool binds:

| Tool | By NAME | By POSITION |
|---|---|---|
| **DuckDB** | `columns={name:type}`, struct-form `types={'col':TYPE}` | list-form `types=['INT','VARCHAR','DATE']` |
| **Polars** | `schema_overrides` as **dict** (`schema_inference.rs:201-207`, "Apply schema_overwrite by column name only") | `schema` (full) and `schema_overrides` as **list** (first N cols) |
| **Pandas** | `dtype={col:dtype}`; `names`+`header=None` to set labels | `names` list assigns labels positionally to the file's columns |

Every tool offers *both* channels and routes by the argument's shape (dict→name,
list→position). The lesson for tallyman: **pick one channel as the contract and
make it explicit.** Standardize the type-override map on a **dict so it binds by
name** (tolerates reordered/renamed columns), and reserve a **positional list form
for headerless input** where there are no names to bind to. Guard against the
historical "same-length dict treated positionally" trap (Polars #20903) with a
test. Note the lazy-path stricter API: a list override that works eagerly raises
`TypeError: expected 'schema_overrides' dict, found 'list'` on Polars `scan_csv` —
another reason to standardize on dict/name. For the yfinance case specifically,
the rename-by-position behavior the old path provided is a *positional* contract;
if tallyman keeps name-binding it must instead detect the header mismatch and
raise a clear error listing the actual headers (or apply `new_columns` to rename
positionally before binding).

### #142 — ragged / short rows

The bug: Polars `scan_csv` **silently null-fills** rows with fewer fields than the
header, so a truncated CSV row bakes into the canonical snapshot as `(3, null)`
data with no signal. The old datafusion path raised
`incorrect number of fields for line 3, expected 2 got 1`.

How each tool treats ragged rows — note the asymmetry between short and long:

| Tool | Short row (too few) | Long row (too many) |
|---|---|---|
| **DuckDB** | `MISSING COLUMNS` **error**; `null_padding=true` to pad | `TOO MANY COLUMNS` **error**; `strict_mode=false` drops extras |
| **Polars** | **silently null-filled**, no error | `ComputeError: found more fields than defined` unless `truncate_ragged_lines=True` |
| **Pandas** | **silently NaN-padded** (not a "bad line") | `on_bad_lines`: error (default) / warn / skip / callable |

Polars and Pandas both default to *silent* on short rows and *loud* on long rows —
the direction-dependent default that bit tallyman. DuckDB is the only one that
errors on short rows by default (good for a data-quality signal) while still
offering `null_padding` opt-in. The lesson: **choose one explicit ragged policy
and enforce it in both directions.** For tallyman's "deterministic parse" goal the
right default is **fail loud on any field-count mismatch** (matching the old
datafusion behavior and DuckDB's strict mode), with an opt-in
`null_padding`/`truncate_ragged_lines` salvage that always routes the affected rows
into the reject log so the null-fill is never invisible.
