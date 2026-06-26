# ADR: Intelligent CSV import — a deterministic reader with a great error/suggestion contract

- **Status:** Proposed (2026-06-26). Lands on top of #137 (the polars
  order-stable reader swap), which is merged to `main` (`52afca7`). Resolves the
  CSV-ingest-contract cluster filed during the #137 review: **#141, #143, #144,
  #145**; **#142 is deferred** to the holistic edge-case pass (see below).
- **Affected code:** `src/tallyman_xorq/io.py` (`tallyman_read_csv`,
  `_polars_overrides`, `_IBIS_TO_POLARS`, a new schema-spec normaliser and a
  suggestion engine), `src/tallyman_mcp/server.py` (the `catalog_run` CSV
  guidance), `tests/test_tallyman_read_csv.py`.
- **Research:** `docs/research/csv-importers/` — deep dives on DuckDB / Polars /
  Pandas (config, inference, error handling, test suites), a cross-tool
  `comparison.md`, and an `edge-cases.md` pathology corpus.
- **Related ADR:** `plans/adr-result-digest-canonical-ordering.md` (the
  `original_row_order` / snapshot-worthiness invariant this design must preserve).

## Problem

The #137 swap from datafusion `deferred_read_csv` to polars `scan_csv` made
`original_row_order` reflect true file order (the point of #137) but changed the
ingest semantics in ways the review filed as issues:

- **#141** — polars `schema_overrides` binds **by column name**; the old
  datafusion path applied the ibis schema **positionally**, renaming the header.
  The documented yfinance `Price`→`Date` rename now raises `ColumnNotFoundError`.
- **#143** (umbrella) — without a schema, polars infers from only the first 100
  rows (`io.py` leaves `infer_schema_length` at the default), so a column that is
  numeric for 100 rows but has an `N/A` at row 30k aborts the whole ingest. The
  feature targets 100k+ row CSVs where messy tails are common.
- **#144** — `_polars_overrides` rejects `time`, `decimal`, and non-nullable
  ibis types (`_IBIS_TO_POLARS` has no entry), so a legitimate schema raises.
- **#145** — every `timestamp*` ibis type maps to a bare `pl.Datetime`, flattening
  tz-aware and sub-microsecond timestamps to naive microseconds.
- **#142** — polars silently null-fills ragged short rows (baking bad rows into
  the canonical snapshot); the old path raised.

## Philosophy (the spine of every decision below)

`tallyman_read_csv` is a **deterministic, config-driven parser**, not a
"do-what-I-mean" importer. Given the same file and config it always produces the
same result, or the same error. It does **no** higher-level semantic cleaning
(it will not decide `020108` "is really a ZIP" and coerce it to string).

The **intelligence lives at the edges**:

1. When called without enough config, a good-faith **deterministic** attempt
   (escalating inference), and the inferred schema is **echoed back** so the
   caller can pin it.
2. When an attempt **fails**, an **excellent, actionable error** carrying a
   probable-good schema/config, so the calling LLM **re-calls** `tallyman_read_csv`
   with corrected args. The error string is the feedback channel.

The **CSV engine is opaque**. It is polars today; the stable contract the caller
sees is `{schema spec} + {dialect vocabulary}`, which a future engine
(datafusion, duckdb, …) must satisfy without changing recipes.

## Invariants this design must preserve (from #137, verified on `main`)

- **INV-1** — output carries `original_row_order: int64`, values `0..N-1` in true
  source file order. Produced by `pl.scan_csv → with_row_index → sink_parquet`
  (`io.py:200-207`), baked as a literal column in the intermediate parquet.
- **INV-2** — the returned expression is snapshot-**worthy**. The trailing
  `.order_by("original_row_order")` (`io.py:253`) contributes a `Sort` op, which
  is in `result_cache._EXPENSIVE_OPS`; that is the *only* worthy op present
  (there is no `RowNumber`/`WindowFunction` — `with_row_index` bakes a plain
  column). Drop the `order_by` and the entry goes cheap and bakes nothing.
- **INV-3** — once the intermediate parquet is baked, reconstruction resolves it
  from `_ordered_csv_key(path, schema, scan_kwargs)` (`io.py:137`) **without
  stat-ing the live CSV**; a deleted/moved source still serves the intermediate
  (`io.py:183-194`). Any new config that feeds the parse must feed the key.
- **INV-4** — the engine is opaque. The schema spec and dialect options are
  engine-agnostic; their translation to polars is an implementation detail.

## Decisions

### D1 — Engine: polars stays; the opacity boundary is the config surface

Keep polars as the materialization engine (it is already chosen and gives INV-1
directly via `with_row_index`; duckdb is a deliberately-excluded dependency —
`pyproject.toml` and `build.py:233` enforce "no duckdb backend"). The stable,
engine-agnostic contract is **`{schema spec} + {dialect vocabulary}`**; a future
engine swap reimplements the translation, recipes are untouched.

duckdb *can* preserve order (it preserves insertion order by default, even
multi-threaded — duckdb#12882) and ships a richer sniffer + reject tables, but
adopting it reverses the enforced no-duckdb stance and reopens #137's byte
guarantees. Rejected. We borrow duckdb's *design* (a re-runnable suggested config,
a per-error taxonomy) without the dependency.

### D2 — No-schema: succeed, and echo the inferred schema

A schema-less call may succeed (honours #143 step 1: cheap infer, "if it parses,
done"). On success the inferred schema is echoed back in the result so the caller
can pin it; **no semantic cleaning** and **no active warnings** about
identifier-looking columns — that is out of scope at the parse layer. Because the
parse validates the inferred types against the whole file (a row that violates
aborts → escalation, D6) and the typed parquet is then durable, a no-schema
success is deterministic for that file.

### D3 — #141: a schema spec that is total, name-bound by default, position-bound on request

`schema` accepts three shapes, routed by Python type (the universal
disambiguation duckdb/polars/pandas all use — dict→name, sequence→position):

```
schema := None                       # infer all, echo (D2)
        | { name: dtype, … }         # bind BY NAME (reorder-safe); ibis schema also accepted
        | ( (name, dtype), … )       # bind BY POSITION (tuple i = column i, renamed to name)
dtype  := "int64" | "date" | … | "infer"
```

- **By name** is the default contract (an ibis schema, or a plain dict). A schema
  name not in the header **raises the great mismatch error** listing the actual
  headers. (polars silently ignores override names absent from the file, so we
  validate header-vs-spec ourselves.)
- **By position** (tuple-of-tuples) renames column *i* to `name` and types it —
  this is the yfinance answer (`(("Date","date"), ("&rest","infer"))` renames
  col 0 → Date positionally). It carries the inherent "reorder the file →
  mislabel" risk, but it is an explicit, count-validated opt-in, not a default.
- A spec must be **total**: it names every column, or it carries exactly one
  **wildcard** closure token. There is no partial/`schema_overrides`; the D2 echo
  hands back the full schema, so partial overrides are unnecessary.
- **Wildcards** (Lisp-lambda-list inspired):
  - `("&rest", dtype)` — last cell only; the remaining **tail** columns keep their
    header names and take `dtype` (or `"infer"`). Valid in the dict form too
    (`{"id":"string", "&rest":"infer"}` = pin `id` by name, infer the rest).
  - `("&begin", dtype)` — first cell only; the leading **head** columns keep their
    names and take `dtype`/`"infer"`; the explicit cells bind to the last *k*
    columns. Tuple-only (positional). **Specced now, implemented when a real CSV
    needs the right-anchor.**
  - At most one wildcard per spec.
- `"infer"` is a dtype token usable in any cell (pin name/position without pinning
  type).
- **No `new_columns` parameter.** Relabelling a faithfully-read column (e.g.
  `Price`→`Date` when you keep name-binding) is a downstream `expr.rename(...)`,
  not a reader concern.

### D4 — #10 / opacity: a curated dialect vocabulary, no passthrough (DEFERRED)

The dialect half of the config surface should be a **curated, engine-agnostic** set
of named parameters — `delimiter`, `quote`, `escape`, `has_header`, `skip_rows`,
`comment`, `encoding`, `null_values`, `decimal`, `thousands` (drawn from the
research config-matrix's agreed vocabulary) — each translated to the engine, with
**no raw `**kwargs` passthrough**: a CSV that needs an option the vocabulary lacks
is a **bug** (file it, add the agnostic param), never a passthrough that couples
recipes to polars.

**Status: deferred, not in this PR.** #137 already forwards raw `**kwargs → scan_csv`
(the #10 fix), which works and is the status quo. Replacing it with the curated
vocabulary is opacity *hygiene*, not a filed bug, so it is documented here as the
target and left for a follow-up (reword the #10 test to the agnostic `delimiter=`
at that time). Until then recipes can still pass polars-specific reader kwargs.

### D5 — Source identity: durable parquet, verify trusts it, drift is a flagged error

This formalises what #137 already implements (#6/#7/#8/#9) and records the contract:
the **durable artifact is the parquet**; reconstruction/verify **trusts** it and
never re-reads the CSV (INV-3). tallyman is a closed system — editing a parsed
CSV is not forbidden (you can edit a file) but is an **error condition tallyman
detects and flags**, never silently absorbs. **Decided enhancement, deferred:**
record the source CSV's `(mtime, size, hash)` (today only `mtime` is recorded,
`io.py:218`) and surface a drift error from `catalog_scan_staleness` when the
live file's hash diverges from what an entry parsed. This is closer to the #5 UI
work than to the filed issues, so it is documented here but not built in this PR.

### D6 — #143: infer mode escalates silently; explicit mode raises with a suggestion

The two modes behave **oppositely** on a value that breaks a type:

- **Infer mode** (no schema, or an `"infer"` / `&rest` / `&begin` cell): tallyman
  owns the type, so a breaking value is ours to fix → **escalate the infer window**
  and retry. Ladder `[10_000 → whole-file]` (default 10k, not the fragile 100;
  the optional cheapest-first 100 rung is a perf nicety, not required). Whole-file
  infer is the always-resolves terminal (a mixed column falls back to `string`).
  The window is a tunable; the ladder is purely a perf optimisation over
  "always whole-file". Then succeed and echo (D2).
- **Explicit mode** (a pinned type): tallyman is **faithful** — it does **not**
  escalate or downgrade `int64`→`string`. A breaking value **raises** with the
  suggested fix.

One **suggestion engine** serves both: a whole-file infer produces the
always-parses schema, rendered as the tuple-of-tuples DSL (ordered, paste-ready).
It is the terminal rung in infer mode and the fix-generator in explicit mode.
Escalation runs once at ingest and bakes into the intermediate; reconstruction
never re-escalates (INV-3).

### D7 — Error contract: string-only, structured-by-convention (v1)

A CSV failure rides `str(exc)` → `build.BuildError` → `{"error": str, "error_id"}`
(`server.py` `_run_and_record`). For v1, `tallyman_read_csv` raises messages
carrying (a) the failing column / row / value and the pinned-vs-inferred type, and
(b) a **full, paste-ready `schema=(("id","string"), …)` tuple-DSL suggestion** the
LLM pastes straight into its next call. `tallyman_read_csv` owns these messages
directly (not via the regex `_ibis_import_hint` path, which is for generic ibis
errors). **Alternative — a structured `csv_suggestion` field on the MCP return**
(machine-readable, UI-renderable) — is deferred to the #5 UI followon; it needs
`BuildError` to carry a structured attribute that `_run_and_record` surfaces.

### D8 — #144: cover `time`, `decimal`, and non-nullable ibis types

Extend `_IBIS_TO_POLARS` / `_polars_overrides` so a valid ibis schema never
spuriously raises: map `time` → `pl.Time`, `decimal(p, s)` → `pl.Decimal(p, s)`
(parameterised, not a fixed precision), and strip ibis nullability (`!`-suffixed
types like `int64` non-null) before lookup — polars CSV columns are nullable
regardless, and a non-nullable *intent* is not a CSV parse concern. An unmapped
type still raises the loud, listed error (the escape hatch stays honest).

### D9 — #145: preserve timestamp tz and precision

Map `timestamp` → `pl.Datetime("us")` (naive) but `timestamp(scale)` → the
matching `pl.Datetime` precision (`s`/`ms`→"ms" where representable, `us`, `ns`),
and `timestamp('UTC')` / tz-aware → `pl.Datetime(time_unit, time_zone=tz)`,
instead of flattening everything to naive `pl.Datetime`. Round-trips the tz and
sub-µs precision the ibis type declares.

## Alternatives considered

- **duckdb as the engine** (sniffer + reject tables for free). Rejected: reverses
  the enforced no-duckdb stance, heavy dep, reopens #137 byte guarantees. duckdb's
  order preservation is *not* the blocker (it preserves insertion order by
  default) — the dependency stance is.
- **Restore positional-rename-by-schema** (make old yfinance recipes "just work").
  Rejected: silent mislabel footgun; replaced by the explicit tuple-positional
  form (D3).
- **Partial `schema_overrides`** (pin some columns, infer the rest implicitly).
  Rejected: ambiguous "half a schema". The `&rest`/`&begin` closure tokens make a
  partial spec **total and explicit** instead.
- **Raw `**kwargs` passthrough** (literal #10). Rejected: couples recipes to polars
  (D4).
- **pyarrow.csv as the sole reader** (fails loud on ragged in both directions,
  Arrow-native, faster than polars). Tempting for #142, but reopens #137's
  byte-stable snapshot machinery; kept as the validation-gate option for the
  ragged followon (see `edge-cases.md`).
- **Structured `csv_suggestion` payload in v1.** Deferred to the #5 UI followon
  (D7).

## Consequences / open questions

- **#144/#145 defaults (D8/D9) were not grilled** — they surfaced only in the full
  review list. The mappings above are defensible defaults; revisit if a different
  tz/precision or decimal policy is wanted.
- **#142 ragged is deferred** to the holistic edge-case pass that ports the
  pandas/polars/duckdb pathology suites onto tallyman. The empirical detection
  finding (polars cannot raise on short rows; `pyarrow.csv` is the fail-loud,
  quote-aware detector at ~4 ms/200k rows) is recorded in
  `docs/research/csv-importers/edge-cases.md`.
- **The drift-detection enhancement (D5)** and the **structured error payload (D7)**
  are decided but deferred, both adjacent to the #5 UI error/debug feature.
- `&begin` is specced (D3) but not implemented until a real CSV needs it.

## Phasing (this PR's commits, TDD red→green)

1. **docs** — this ADR + `docs/research/csv-importers/`.
2. **test** — all new failing tests (#144, #145, #141, #143) in one commit (seen
   red on CI), per the project's TDD rule.
3. **fix #144 + #145** — type coverage + timestamp tz/precision (self-contained
   `_IBIS_TO_POLARS`/`_polars_overrides` extensions).
4. **fix #141** — the schema-spec normaliser (dict/tuple/None, `&rest`, `&begin`
   specced, totality + header-mismatch error).
5. **fix #143** — the escalation ladder + suggestion engine + the D7 error string.

D4 (curated dialect vocabulary) and the D5 hash/drift-detection enhancement are
decided but deferred to follow-ups (see those sections); #142 ragged is deferred to
the edge-case pass.

## Testing plan

- **#144/#145**: a schema using `time`/`decimal(10,2)`/non-nullable and a
  tz-aware / `ns` timestamp builds and reads back with the declared types (today:
  raises / flattens).
- **#141**: dict binds by name; a name absent from the header raises the listed
  mismatch error; tuple-of-tuples renames positionally; `&rest`/dict-`&rest` close
  a partial spec; a non-total spec raises; the yfinance `(("Date","date"),
  ("&rest","infer"))` case builds with `Date` as col 0.
- **#143**: a no-schema CSV whose column is numeric for the first 10k rows then
  `N/A` escalates and succeeds with that column as `string` (today aborts); a
  pinned `int64` on the same file raises with a paste-ready `string` suggestion;
  the success path echoes the inferred schema.
- All preserve INV-1..INV-4 (existing #137 tests stay green).
