from __future__ import annotations

import functools
import json
import logging
import os
import sys
import traceback
import uuid
from dataclasses import asdict

import httpx
from fastmcp import FastMCP

from tallyman_core import (
    AliasExists,
    AliasNotFound,
    CellNotFound,
    ChartSpecError,
    DisplayKlassError,
    PostProcessingRunError,
    PostProcessingSourceError,
    StatSourceError,
    alias_for_hash,
    auto_recalc_enabled,
    carry_forward_entry_config,
    entry_dir,
    entry_schema_path,
    export_notebook_path,
    get_alias,
    history_for,
    list_display_klasses,
    list_post_processings,
    list_projects,
    list_stats,
    notebook,
    record_error,
    remove_alias,
    remove_display_klass,
    remove_post_processing,
    remove_stat,
    rename_alias,
    resolve_project,
    run_post_processing,
    set_alias,
    set_chart,
    validate_alias_name,
    write_display_klass,
    write_post_processing,
    write_stat,
)
from tallyman_core.events import record_event
from tallyman_xorq import BuildError, build_and_persist, full_diff, list_entries, staleness
from tallyman_xorq.dependents import references_own_alias
from tallyman_xorq.recalc import classify_orphans, recalc

log = logging.getLogger("tallyman_mcp")
COMPANION_URL = os.environ.get("TALLYMAN_COMPANION_URL", "http://127.0.0.1:7860")

# Identifies this MCP process in the project activity log. Claude Code spawns one
# `tallyman mcp` per session, so one process == one session; multiple sessions
# writing to the same project are told apart by this id in events.jsonl.
SESSION_ID = uuid.uuid4().hex[:8]

mcp = FastMCP("tallyman")

# Tracks the active project name observed at the end of the previous tool
# invocation, so the next call can detect a mid-conversation switch and
# surface a ``warning`` field (design decision 14 of the project-switcher
# plan). ``None`` until the first tool call seeds it.
_last_project: str | None = None

# Session-local active project. Set in-process by project_switch/project_new
# so that switching once is sticky for all subsequent calls in this MCP
# process, regardless of what another Claude session or shell command writes to
# ~/.tallyman/active_project between calls. Falls back to the disk file if
# never explicitly set (first run, or direct-file manipulation).
_mcp_active_project: str | None = None


def _resolve_active_project() -> str:
    """Active project for this MCP process: in-process override beats disk."""
    return _mcp_active_project or resolve_project()


def _tag_project(fn):
    """Wrap a tool function so every response carries the active project.

    Behavior:

    - If the wrapped tool returns a ``dict``, ``result.setdefault("project",
      <current>)`` is applied so the LLM always knows which project the
      call landed in.
    - If the wrapped tool returns a list (e.g. ``catalog_list``), it is
      wrapped as ``{"project": <current>, "items": [...]}``. This is a
      documented breaking change to those tools' surface — the trade-off
      is uniformity: every MCP response is now a dict with a ``project``
      key.
    - When ``_last_project`` differs from the current resolution, a
      ``warning`` field is added with the wording from the design plan.
    - On the very first tool call, no warning is emitted; ``_last_project``
      is simply seeded.
    """

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        global _last_project, _mcp_active_project
        result = fn(*args, **kwargs)
        current = _resolve_active_project()
        # Seed in-process state on the very first call so future calls are
        # sticky even if the disk file changes after this point.
        if _mcp_active_project is None:
            _mcp_active_project = current
        if not isinstance(result, dict):
            result = {"items": result}
        result.setdefault("project", current)
        if _last_project is not None and _last_project != current:
            result["warning"] = (
                f"active project changed from {_last_project!r} to {current!r} "
                f"since the last call; proceeding with {current!r}"
            )
        _last_project = current
        return result

    return wrapped


# ---------------------------------------------------------------------------
# Dispatch-boundary checkpoint (reset-to-revision, plans/adr-reset-to-revision.md)
# ---------------------------------------------------------------------------

# Read-only and lifecycle tools that must NOT append a revision. Everything
# else checkpoints by default — opt-out, so a tool added next month is covered
# automatically and coverage cannot silently regress (pinned by
# tests/test_reset_to_revision.py).
_NO_CHECKPOINT = frozenset(
    {
        "catalog_list",
        "catalog_diff",
        "catalog_scan_staleness",  # read-only staleness scan
        "catalog_recalc",  # self-checkpoints (arg-aware: dry-run takes none, a real run one)
        "catalog_promote_diff",  # self-checkpoints one tx for the promote + cascade (no double-commit)
        "catalog_list_summary_stats",
        "catalog_list_post_processings",
        "catalog_run_post_processing",  # preview, persists nothing
        "catalog_export_marimo",  # export artifact, not state
        "project_list",
        "project_switch",
        "project_new",  # lifecycle; genesis happens inside the tool itself
    }
)

# Names of every tool that registered through the checkpointing decorator —
# introspection surface for the opt-out coverage test.
_CHECKPOINTED_TOOLS: set[str] = set()


def _with_checkpoint(fn):
    """Checkpoint the catalog once after a successful mutating tool call.

    Applied to every tool at registration (see ``_checkpointing_tool``). The
    checkpoint is taken at the dispatch boundary, after the operation returns
    without error — one revision per operation even when the tool calls several
    ``tallyman_core`` mutators (#33's multi-commit bug). A checkpoint failure is
    logged, never raised: it must not break the tool response.
    """

    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        result = fn(*args, **kwargs)
        if fn.__name__ in _NO_CHECKPOINT:
            return result
        if isinstance(result, dict) and result.get("error"):
            return result
        try:
            from tallyman_core.catalog_state import checkpoint_catalog  # noqa: PLC0415

            project = _resolve_active_project()
            if project:
                step = checkpoint_catalog(project, f"tallyman: {fn.__name__}")
                # catalog_revise's auto-path recalc sub-report is built by the
                # checkpoint-free walk *before* this per-op checkpoint exists, so its
                # checkpoint_step is None. This is the checkpoint that commits the head
                # advance + cascade as one revision; backfill the real landed step the
                # report couldn't yet know. (catalog_promote_diff is _NO_CHECKPOINT-
                # exempt and self-checkpoints, so it stamps its own step and never
                # reaches here.)
                recalc_report = result.get("recalc") if isinstance(result, dict) else None
                if isinstance(recalc_report, dict) and recalc_report.get("checkpoint_step") is None:
                    recalc_report["checkpoint_step"] = step
        except Exception as exc:
            log.warning("checkpoint after %s failed: %s", fn.__name__, exc)
        return result

    return wrapped


_original_tool = mcp.tool


def _checkpointing_tool(*dargs, **dkwargs):
    """``mcp.tool`` wrapped so registration applies the checkpoint hook."""
    register = _original_tool(*dargs, **dkwargs)

    def _register(fn):
        _CHECKPOINTED_TOOLS.add(fn.__name__)
        return register(_with_checkpoint(fn))

    return _register


mcp.tool = _checkpointing_tool


def _notify(kind: str, content_hash: str | None = None, **extra) -> None:
    """Best-effort POST to the companion's /internal/notify. Never raise."""
    try:
        with httpx.Client(timeout=2.0) as client:
            client.post(
                f"{COMPANION_URL}/internal/notify",
                json={"kind": kind, "hash": content_hash, "extra": extra or None},
            )
    except Exception as exc:
        # Companion may not be up; that's allowed. Log and move on.
        print(f"[tallyman_mcp] notify failed ({kind}): {exc}", file=sys.stderr)


def _auto_recalc_after_head_advance(project: str, name: str, *, tool: str) -> dict | None:
    """Cascade-recompute *name*'s stale followers right after an existing alias
    head advanced — shared by every MCP path that re-points an existing alias
    (``catalog_revise``, ``catalog_promote_diff``). ``tool`` names that surface so
    a persisted cascade failure is attributed to the tool that actually ran.

    The walk is **checkpoint-free**: it leaves the re-pointed dependents in the
    working tree for the surrounding op's single checkpoint (the dispatch
    decorator, or promote_diff's own checkpoint) to commit, so the head advance
    and the whole cascade land as ONE git revision. Returns the recalc sub-report
    (``catalog_recalc``'s shape plus ``orphan_stale``), or ``None`` when the
    per-project ``auto_recalc`` switch is off. Best-effort cross-process notify so
    the companion invalidates caches / reloads sessions / emits the SSE event.
    """
    if not auto_recalc_enabled(project):
        return None
    from tallyman_xorq.recalc import auto_recalc  # noqa: PLC0415

    report = auto_recalc(project, name, tool=tool)
    if report.get("remap"):
        # Flat kwargs: _notify packs **extra into the wire `extra`, and the
        # companion handler reads extra.remap / extra.step. A literal extra={...}
        # would nest as extra.extra and silently drop both (#129 follow-up).
        _notify("recalc", remap=report["remap"], step=report.get("checkpoint_step"))
    return report


@mcp.tool()
@_tag_project
def catalog_run(code: str, prompt: str = "") -> dict:
    """Execute a xorq expression script and persist it as a catalog entry.

    The provided code MUST bind a top-level variable named `expr` to a xorq/ibis
    expression. Imports allowed; all imports happen in a fresh module scope.

    PREFERRED pattern:

        from tallyman_xorq.io import read_project_file, tracked_expr_from_alias
        import xorq.vendor.ibis as ibis
        t = tracked_expr_from_alias("alias_name")      # named catalog entry by alias (records lineage)
        t = read_project_file("file.parquet")          # raw file under <project>/data/
        expr = t.filter(t.col > 0).group_by("region").aggregate(n=t.count())

    THREE NAMESPACES — mixing these up is the #1 build failure:

        import xorq.api as xo            # backends + deferred reads:
                                         # xo.memtable, xo.deferred_read_parquet, xo.connect
        import xorq.vendor.ibis as ibis  # the expression API: ibis._, ibis.cases,
                                         # ibis.window, ibis.literal, ibis.coalesce, ibis.desc
        from tallyman_xorq.io import read_project_file, tracked_expr_from_alias, tallyman_read_csv   # reading data

        t.mutate(pk=ibis._.a + "_" + ibis._.b)   # deferred string concat via ibis._
        # NEVER `import xorq` then `xorq.<fn>`: bare xorq.read_parquet / xorq.memtable /
        #   xorq._ all raise AttributeError. The api module is `xo` (xorq.api).
        # NEVER bare `import ibis` / `from ibis ...`: it builds but fails at save time
        #   with a vendored-Expr class error. Always `import xorq.vendor.ibis as ibis`.
        # Math is a COLUMN METHOD: col.sin(), col.log(), col.sqrt() — not ibis.sin(col).
        # Read data only via read_project_file / tracked_expr_from_alias (no xo.read_parquet / ibis.read_parquet).

    ONLY BACKEND — xorq's built-in datafusion; there is NO duckdb. Do not use
    `ibis.duckdb`, a duckdb connection, `.sql()`, or `con.register()`. Build
    everything with the ibis expression API over read_project_file/tracked_expr_from_alias.

    DATA SOURCING — four functions, each with a distinct role:
      - `tracked_expr_from_alias("name")` reads a catalog entry by alias and records it
        as a parent in the lineage DAG. Use this for normal recipe chaining — the
        standard way to build on top of another catalog entry.
      - `read_project_file("file.parquet")` reads raw files under `<project>/data/`.
        Use this only for raw files visible on disk (not catalog aliases).
      - `tallyman_read_csv("/abs/path/to/file.csv", schema=...)` reads a CSV and
        injects ``original_row_order`` so the snapshot is byte-stable across builds.
        Use this for ALL CSV ingests instead of ``xo.deferred_read_csv``.
      - `pinned_expr_from_alias(<hash or "name-vN">)` reads a catalog entry by
        content hash or explicit version reference (e.g. "shoe_sales-v2") and
        records a pinned (follow=False) parent edge. Use when you want to stay on
        a specific version — recalc will find the entry but won't advance it when
        the parent alias moves. A bare alias is rejected (#166): a pin must name
        the same entry forever, so say which version you mean.
      - When in doubt, call `catalog_list` first. A name that looks like a file
        is often actually an alias — `read_project_file` on an alias raises
        `ProjectDataNotFound`.

    COLUMN NAMES — do not guess. The source step returns a `schema`, and
    `catalog_list` shows each entry's columns as a compact `name:type, ...`
    summary. Reference the exact names you see there (`tripduration`, not
    `trip_duration`). When unsure, index with `t["col"]` rather than `t.col`:
    the bracket form's error lists the real columns, whereas attribute access
    on a missing column raises an opaque AttributeError.

    GOTCHAS — types and casts:

        # BAD: timestamp subtraction yields IntervalColumn — fails on .mean()
        #      AND on parquet write. Cast or use epoch_seconds() arithmetic.
        expr = t.group_by("k").aggregate(avg=(t.b - t.a).mean())     # AttributeError on .mean()
        expr = t.mutate(d=t.b - t.a)                                 # parquet write fails
        # CORRECT:
        expr = t.mutate(d=(t.b - t.a).cast("int64"))                 # microseconds
        expr = t.mutate(d=t.b.epoch_seconds() - t.a.epoch_seconds()) # seconds

        # BAD: ibis.memtable() with date/timestamp values — pyarrow type coercion fails.
        t = ibis.memtable({"d": [ibis.literal("2024-01-01").cast("date")]})  # FAILS
        # CORRECT: use string columns and cast in the expression.
        t = ibis.memtable({"d": ["2024-01-01"]})
        expr = t.mutate(d=t.d.cast("date"))

    LOADING RAW CSV FILES — always inspect first:

        Use ``tallyman_read_csv`` (not ``xo.deferred_read_csv``) for all CSV
        ingests. It adds an ``original_row_order`` column that makes the
        snapshot byte-stable across builds:

            from tallyman_xorq.io import tallyman_read_csv
            import xorq.vendor.ibis as ibis
            schema = ibis.schema({"Date": "date", "Close": "float64"})
            t = tallyman_read_csv("/abs/path/to/file.csv", schema=schema)

        Extra keyword arguments are forwarded to ``polars.scan_csv`` as reader
        options (``separator``, ``skip_rows``, ``null_values``, ``quote_char``,
        ``has_header``, ``encoding``, ...), e.g.
        ``tallyman_read_csv(path, schema=schema, separator=";", skip_rows=2)``.

        Before writing a schema, run `head -5 <file>` to verify the actual
        column names and order. Do NOT guess — yfinance and similar sources
        produce non-obvious layouts:

        - yfinance multi-download CSVs have two extra header rows (a "Price"
          label row and a ticker row) before the real headers. Detect this with
          head; use skip_rows=2 only when those rows are actually present.
        - yfinance column order is alphabetical: Close, High, Low, Open, Volume
          (NOT the conventional Open, High, Low, Close, Volume order).
        - The first column is often literally named "Price" in raw yfinance output,
          not "Date".

        ALWAYS use an explicit schema with tallyman_read_csv — without one, type
        inference silently misassigns types (e.g. date columns become timestamps).

        DATE-ONLY COLUMNS — use 'date', never 'timestamp':

        # BAD: 'timestamp' stores midnight UTC; Western-timezone display shifts
        #      the date back by one day (2026-05-19 → shows as 2026-05-18).
        import xorq.vendor.ibis as ibis
        schema = ibis.schema({'Date': 'timestamp', ...})   # off-by-one in viewer

        # CORRECT: 'date' has no time component, no timezone conversion.
        schema = ibis.schema({'Date': 'date', ...})        # import xorq.vendor.ibis as ibis

        This applies to any date-only column: OHLCV Date, trading calendars,
        event dates, anything that looks like YYYY-MM-DD in the CSV.

    GOTCHAS — pandas reflexes that silently fail:

        t.x.describe()        # No describe() — decompose into explicit aggregates.
        t.ts.dt.hour          # No .dt accessor — call .hour() directly on the column.
        t.select("*")         # No wildcard expansion — list columns or skip select().
        t.select(ibis._, x=…) # No ibis._ wildcard selector — prefer mutate() for additions.
        # CORRECT:
        t.aggregate(mean=t.x.mean(), std=t.x.std(), min=t.x.min(), max=t.x.max())
        t.mutate(h=t.ts.hour())
        t.select("col_a", "col_b")                                   # explicit columns
        t.mutate(new_col=t.x + 1)                                    # not select(ibis._, ...)

    GOTCHAS — group_by / aggregate patterns:

        # WRONG: .agg() is a pandas shorthand — ibis/xorq uses .aggregate()
        t.group_by("k").agg(n=t.x.count())           # AttributeError: 'agg' not found
        # CORRECT:
        t.group_by("k").aggregate(n=t.x.count())

        # WRONG: .sort() is pandas/polars — ibis uses .sort_by() or .order_by()
        agg.sort("n", descending=True)                # AttributeError: 'sort' not found
        # CORRECT:
        agg.sort_by("n", descending=True)
        agg.order_by(ibis.desc("n"))

        # WRONG: .alias() to rename a computed column expression
        distance_col.alias("distance_km")             # AttributeError: 'alias' not found
        # CORRECT:
        distance_col.name("distance_km")

        # WRONG: referencing the *source* table in post-aggregate operations
        agg = t.group_by(["a", "b"]).aggregate(n=t.x.count())
        agg.order_by(t.n.desc())                      # t has no column 'n'; fails or silently wrong
        agg.select(t.a, t.b, agg.n)                  # mixes references across different relations
        # CORRECT: always reference the *result* table; use bracket form for safety
        agg.order_by(ibis.desc("n"))
        agg.select(agg["a"], agg["b"], agg["n"])

        # CANONICAL multi-step pattern (mutate → group_by → aggregate → sort → select):
        with_dur = t.mutate(dur_s=(t.ended_at - t.started_at).cast("int64"))
        agg = (
            with_dur
            .group_by(["start_col", "end_col"])
            .aggregate(
                n=with_dur.dur_s.count(),
                avg_dur=with_dur.dur_s.mean(),
                lat=with_dur.start_lat.first(),
            )
        )
        result = agg.order_by(ibis.desc("n")).limit(500)
        expr = result.select(
            result["start_col"], result["end_col"],
            result["n"], result["avg_dur"], result["lat"],
        )

    GOTCHAS — referencing aggregate columns:

        # BAD: when an aggregate alias collides with an ibis method name
        # (count, min, max, mean, sum, std), attribute access returns the method.
        agg = t.group_by("k").aggregate(count=t.x.count())
        agg.filter(agg.count > 100)        # TypeError: '>' not supported between method and int
        agg.order_by(agg.count.desc())     # AttributeError: 'function' has no attribute 'desc'
        # CORRECT: reference by string, or pick a non-shadowing alias.
        agg.filter(agg["count"] > 100)
        agg.order_by(ibis.desc("count"))
        agg = t.group_by("k").aggregate(n=t.x.count())   # avoids shadowing entirely

    COLUMN ORDER — put new columns first:

        When the expression adds a computed column to an existing table,
        select the new column first so it appears at the left of the viewer.
        # BAD:
        expr = t.mutate(tripduration=(t.ended_at - t.started_at).cast("int64"))
        # CORRECT — new column leads:
        expr = t.select(
            tripduration=(t.ended_at - t.started_at).cast("int64"),
            *t.columns,
        )
        # Or equivalently with mutate + relocation:
        expr = t.mutate(tripduration=(t.ended_at - t.started_at).cast("int64"))
        expr = expr.select("tripduration", *[c for c in t.columns])

    GOTCHAS — APIs that DON'T exist (do not invent these):

        ibis.to_date(...) / ibis.re_replace(...)   # NOT real. Parse string dates with:
        t.mutate(d=t.date_str.as_timestamp("%b %d %Y"))            # strptime-style format
        ibis.struct(mean=..., std=...)             # struct() takes a dict, not kwargs.
        # For several per-group stats use separate aggregate kwargs — NOT a struct:
        t.group_by("k").aggregate(mean_x=t.x.mean(), std_x=t.x.std(), n=t.x.count())

    WINDOW FUNCTIONS (rolling / lag):

        w = ibis.window(group_by="symbol", order_by="d", preceding=2, following=0)
        t.mutate(ma3=t.price.mean().over(w), prev=t.price.lag(1).over(w))
        # kwargs are group_by/order_by/preceding/following — NOT partition_by / rows=(-2, 0).

    CASE / binning — prefer ibis.cases() (ibis.case().when() is deprecated):

        b = ibis.cases((t.x < 1, "lo"), (t.x < 2, "mid"), else_="hi")

    MODELING — fit sklearn models as catalog entries, NEVER inside a
    post-processing process() function (that sandbox blocks third-party
    imports). The fit becomes part of the expression and the trained model is
    serialized into the build — a content-hashed, diffable catalog entry:

        import xorq.expr.datatypes as dt
        from xorq.ml import deferred_fit_predict_sklearn, train_test_splits
        from sklearn.linear_model import LinearRegression
        t = tracked_expr_from_alias("diamond_features")
        train, test = train_test_splits(t, test_sizes=0.25, random_seed=42)
        deferred = deferred_fit_predict_sklearn(cls=LinearRegression, return_type=dt.float64)
        instance = deferred(train, target="price", features=["carat", "depth", "x", "y", "z"])
        expr = test.mutate(price_pred=instance.deferred_other.on_expr(test))   # rename the predict col
        # SCORE in pure ibis on the prediction column. deferred_sklearn_metric does NOT
        # survive build_and_persist (its MetricComputation op has no build serializer):
        #   acc  = preds.aggregate(accuracy=(preds.species == preds.species_pred).mean())
        #   rmse = preds.aggregate(rmse=((preds.price - preds.price_pred) ** 2).mean().sqrt())
        # deferred_fit_transform_sklearn (transforms) and Pipeline/Step also exist.

    DO NOT:
        - fabricate placeholder values that look like real output (e.g. a
          memtable of zeros) when you cannot compute the requested result —
          return an error or a clearly-labelled column instead.
        - pass a project= argument to read_project_file/tracked_expr_from_alias; the active
          project is implicit.

    Args:
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description (the user's intent).

    Returns:
        dict with keys: hash, row_count, execute_seconds, schema, entry_path.
    """
    project = _resolve_active_project()
    out = _run_and_record(project, code, prompt, tool="catalog_run")
    if "error" in out:
        return out
    out.pop("_build", None)
    record_event(project, "build_ok", session=SESSION_ID, tool="catalog_run", prompt=prompt or None, hash=out["hash"])
    _notify("new_entry", content_hash=out["hash"])
    return out


@mcp.tool()
@_tag_project
def catalog_load_parquet(rel_path: str, prompt: str = "", name: str = "") -> dict:
    """Register a parquet file from the project's data/ directory as a catalog entry.

    Use this for simple "load this file" steps. The agent does not need to write
    any xorq code — pass a path relative to the project's `data/` directory.

    Args:
        rel_path: Path relative to `<project>/data/`. Example: "orders.parquet".
        prompt: Optional human-readable description (the user's intent).
        name: Optional alias to register the entry under. If provided, the
            entry behaves like one created by `catalog_create` (named, appended
            to the notebook). Errors if the alias already exists.

    Returns:
        Same shape as catalog_run, plus `alias`/`version` when `name` is set.
    """
    project = _resolve_active_project()
    if name and get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists. Use catalog_revise to update it."}
    code = f"from tallyman_xorq.io import read_project_file\nexpr = read_project_file({rel_path!r})\n"
    out = _run_and_record(project, code, prompt, tool="catalog_load_parquet")
    if "error" in out:
        return out
    out.pop("_build", None)
    if name:
        info = set_alias(project, name, out["hash"], expect_exists=False)
        notebook.append(project, name, markdown=prompt or "")
        out["alias"] = info["name"]
        out["version"] = info["version"]
        _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
        _notify("notebook_changed")
    else:
        _notify("new_entry", content_hash=out["hash"])
    return out


def _entry_url(project: str, content_hash: str) -> str:
    return f"{COMPANION_URL}/{project}/catalog/{content_hash}"


def _run_and_record(project: str, code: str, prompt: str, *, tool: str = "catalog_run") -> dict:
    """Shared body for tools that compile-and-persist; returns the tool reply dict."""
    try:
        result = build_and_persist(project=project, code=code, prompt=prompt or None)
    except BuildError as exc:
        tb = traceback.format_exc()
        rec = record_error(project, code=code, message=str(exc), prompt=prompt or None, tool=tool, traceback=tb)
        record_event(
            project,
            "build_error",
            session=SESSION_ID,
            tool=tool,
            prompt=prompt or None,
            code=code,
            message=str(exc),
            traceback=tb,
            error_id=rec["id"],
        )
        _notify("build_failed", error_id=rec["id"], tool=tool)
        return {"error": str(exc), "error_id": rec["id"]}
    reply = {
        "_build": result,
        "hash": result.content_hash,
        "row_count": result.row_count,
        "execute_seconds": result.execute_seconds,
        "schema": result.schema,
        "entry_path": str(result.entry_path),
        "url": _entry_url(project, result.content_hash),
    }
    if result.lint_warnings:
        reply["lint_warnings"] = result.lint_warnings
    return reply


@mcp.tool()
@_tag_project
def catalog_create(name: str, code: str, prompt: str = "") -> dict:
    """Execute and persist a NAMED catalog entry (alias).

    Use this when the user gives a concept a name ("name this `shoe_sales`").
    Errors if the alias already exists — use `catalog_revise` to update an
    existing alias instead.

    Code conventions and gotchas are documented in `catalog_run`. The same
    rules apply here.

    Args:
        name: The alias for this entry. Must not already exist in the project.
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description.

    Returns:
        Same shape as catalog_run, plus `alias` and `version`.
    """
    project = _resolve_active_project()
    if get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists. Use catalog_revise to update it."}
    try:
        validate_alias_name(name)  # before the build — an unaliasable entry shouldn't be built
    except ValueError as exc:
        return {"error": str(exc)}
    out = _run_and_record(project, code, prompt, tool="catalog_create")
    if "error" in out:
        return out
    info = set_alias(project, name, out["hash"], expect_exists=False)
    record_event(
        project, "alias_set", session=SESSION_ID, tool="catalog_create",
        alias=name, version=info["version"], hash=out["hash"], prompt=prompt or None,
    )
    notebook.append(project, name, markdown=prompt or "")
    out.pop("_build", None)
    out["alias"] = info["name"]
    out["version"] = info["version"]
    _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
    _notify("notebook_changed")
    return out


@mcp.tool()
@_tag_project
def catalog_revise(name: str, code: str, prompt: str = "") -> dict:
    """Persist a NEW VERSION of an existing alias.

    The alias is repointed to the new content hash; the previous hash remains as
    a forensic artifact in alias_history.

    Write the revision as a SELF-CONTAINED recipe — start from this entry's
    current code and extend it, so the code tab reads as the full state of this
    version in one view. A revision that references its OWN alias by name
    (`tracked_expr_from_alias("<name>")` / `pinned_expr_from_alias("<name>")`) is
    rejected (#135) — it makes an opaque, follows-its-own-head entry. Two good
    shapes instead:
      - source-shaped entry -> inline the source: `read_project_file(...)` /
        `read_csv` plus the transforms, then your change.
      - expensive parent (Aggregate/Join/Sort/window/UDF) -> reference the
        previous version by its content HASH: `pinned_expr_from_alias("<hash>")`,
        which reads its baked snapshot instead of re-running the computation.

    Code conventions and gotchas are documented in `catalog_run`.

    Args:
        name: An existing alias.
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description of what changed.
    """
    project = _resolve_active_project()
    prev_hash = get_alias(project, name)
    if prev_hash is None:
        return {"error": f"alias {name!r} does not exist. Use catalog_create to create it."}
    out = _run_and_record(project, code, prompt, tool="catalog_revise")
    if "error" in out:
        return out
    if references_own_alias(project, out["hash"], name):
        msg = (
            f"this revision references its own alias {name!r}, which makes an opaque, "
            f"permanently-stale (follows-its-own-head) entry. Write a self-contained recipe: "
            f"inline the source (read_project_file / read_csv …) for a source-shaped entry, or "
            f"reference the previous version by hash with pinned_expr_from_alias({prev_hash!r}) "
            f"for an expensive one."
        )
        rec = record_error(project, code=code, message=msg, prompt=prompt or None, tool="catalog_revise")
        record_event(
            project,
            "build_error",
            session=SESSION_ID,
            tool="catalog_revise",
            prompt=prompt or None,
            code=code,
            message=msg,
            error_id=rec["id"],
        )
        return {"error": msg, "error_id": rec["id"]}
    info = set_alias(project, name, out["hash"], expect_exists=True)
    record_event(
        project, "alias_set", session=SESSION_ID, tool="catalog_revise",
        alias=name, version=info["version"], hash=out["hash"], prompt=prompt or None,
    )
    out.pop("_build", None)
    out["alias"] = info["name"]
    out["version"] = info["version"]
    # Chart + display config are keyed by content hash; seed the new version
    # from the previous one so they follow the alias across revisions.
    carried = carry_forward_entry_config(project, prev_hash, out["hash"])
    if carried:
        out["carried_over"] = carried
    _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
    # Auto-recalc: cascade-recompute name's stale followers in this op's single
    # checkpoint (the dispatch decorator commits the head advance + the cascade as
    # one revision). Off → the explicit scan→recalc path. See plans/auto-recalc-on-revise.md.
    recalc_report = _auto_recalc_after_head_advance(project, name, tool="catalog_revise")
    if recalc_report is not None:
        out["recalc"] = recalc_report
    return out


@mcp.tool()
@_tag_project
def catalog_alias(hash: str, name: str) -> dict:
    """Promote an existing scratch (unnamed) entry to a named entry.

    Use this when an entry was created via `catalog_run` and you decide
    post-hoc that it deserves a name. Errors if the alias already exists.
    """
    project = _resolve_active_project()
    if not entry_dir(project, hash).exists():
        return {"error": f"no catalog entry for hash {hash!r}"}
    if get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists"}
    info = set_alias(project, name, hash, expect_exists=False)
    notebook.append(project, name)
    _notify("alias_changed", content_hash=hash, alias=name, version=info["version"])
    _notify("notebook_changed")
    return {"hash": hash, "alias": name, "version": info["version"], "url": _entry_url(project, hash)}


@mcp.tool()
@_tag_project
def catalog_rename(old_name: str, new_name: str) -> dict:
    """Rename an existing alias. Preserves version history and notebook position."""
    project = _resolve_active_project()
    try:
        info = rename_alias(project, old_name, new_name)
    except AliasNotFound:
        return {"error": f"alias {old_name!r} does not exist"}
    except AliasExists:
        return {"error": f"alias {new_name!r} already exists"}
    except ValueError as exc:  # version-shaped name (#166)
        return {"error": str(exc)}
    notebook.rename_alias_in_cells(project, old_name, new_name)
    _notify("alias_renamed", old=old_name, new=new_name, content_hash=info["hash"])
    _notify("notebook_changed")
    return info


@mcp.tool()
@_tag_project
def catalog_unalias(name: str) -> dict:
    """Remove an alias (the named entries become scratch again).

    Catalog entries themselves remain; only the named handle is dropped. Any
    notebook cells anchored on this alias are removed.
    """
    project = _resolve_active_project()
    if get_alias(project, name) is None:
        return {"error": f"alias {name!r} does not exist"}
    remove_alias(project, name)
    n = notebook.remove_alias_from_cells(project, name)
    _notify("alias_changed", alias=name)
    if n:
        _notify("notebook_changed")
    return {"removed_alias": name, "notebook_cells_removed": n}


@mcp.tool()
@_tag_project
def notebook_reorder(cell_id: str, new_index: int) -> dict:
    """Move a notebook cell to a new position (0-based)."""
    project = _resolve_active_project()
    try:
        cell = notebook.reorder(project, cell_id, new_index)
    except CellNotFound:
        return {"error": f"no cell with id {cell_id!r}"}
    _notify("notebook_changed")
    return {"cell": cell, "new_index": new_index}


@mcp.tool()
@_tag_project
def notebook_remove(cell_id: str) -> dict:
    """Remove a cell from the notebook. The underlying catalog entry is unaffected."""
    project = _resolve_active_project()
    try:
        notebook.remove(project, cell_id)
    except CellNotFound:
        return {"error": f"no cell with id {cell_id!r}"}
    _notify("notebook_changed")
    return {"removed": cell_id}


@mcp.tool()
@_tag_project
def notebook_edit_markdown(cell_id: str, markdown: str) -> dict:
    """Replace the markdown text shown above a cell."""
    project = _resolve_active_project()
    try:
        cell = notebook.edit_markdown(project, cell_id, markdown)
    except CellNotFound:
        return {"error": f"no cell with id {cell_id!r}"}
    _notify("notebook_changed")
    return {"cell": cell}


@mcp.tool()
@_tag_project
def catalog_chart(hash_or_alias: str, vega_spec: dict | str) -> dict:
    """Attach a Vega-Lite spec to a catalog entry.

    The spec is rendered above the table on the entry's detail page via
    vega-embed in the browser. The spec is stored as-is — no server-side
    validation. Use the entry's columns by name; vega-embed will load the
    parquet's data automatically (companion serves it as JSON to the chart).

    Args:
        hash_or_alias: Target entry. Aliases resolve to their latest hash.
        vega_spec: A Vega-Lite v5 JSON spec (dict or JSON string).

    Example spec::

        {"mark": "bar",
         "encoding": {"x": {"field": "region", "type": "nominal"},
                      "y": {"field": "total", "type": "quantitative"}}}
    """
    project = _resolve_active_project()
    target_hash = hash_or_alias
    if not entry_dir(project, target_hash).exists():
        resolved = get_alias(project, hash_or_alias)
        if resolved is None:
            return {"error": f"no entry for {hash_or_alias!r}"}
        target_hash = resolved
    try:
        path = set_chart(project, target_hash, vega_spec)
    except ChartSpecError as exc:
        return {"error": str(exc)}
    _notify("chart_attached", content_hash=target_hash)
    return {"hash": target_hash, "spec_path": str(path)}


@mcp.tool()
@_tag_project
def catalog_diff(name: str, va: int = -2, vb: int = -1) -> dict:
    """Diff two versions of an alias.

    Default compares the previous version to the current one (V_{n-1} vs V_n).
    Returns code/schema/stats/head/keyed diff summaries.

    Args:
        name: An existing alias.
        va, vb: 1-based version indices, or negative for from-end (-1 = latest).
    """
    project = _resolve_active_project()
    hashes = history_for(project, name)
    if not hashes:
        return {"error": f"alias {name!r} has no history"}

    def resolve(v):
        if v < 0:
            v = len(hashes) + v + 1  # -1 -> last, -2 -> penultimate
        if v < 1 or v > len(hashes):
            return None
        return v, hashes[v - 1]

    a = resolve(va)
    b = resolve(vb)
    if a is None or b is None:
        return {"error": f"version out of range; alias has {len(hashes)} versions"}
    a_idx, a_hash = a
    b_idx, b_hash = b
    a_dir = entry_dir(project, a_hash)
    b_dir = entry_dir(project, b_hash)
    if not a_dir.exists() or not b_dir.exists():
        return {"error": "one or both entries are missing on disk"}

    # Diff the two entries' (cache-resolving) expressions — an expensive entry
    # reads its baked result cache, a cheap one recomputes (#73); neither
    # depends on a result.parquet existing.
    from tallyman_xorq.result_cache import cached_result_expr

    diff = full_diff(
        a_dir,
        b_dir,
        a_label=f"V{a_idx}",
        b_label=f"V{b_idx}",
        a_expr=cached_result_expr(project, a_hash),
        b_expr=cached_result_expr(project, b_hash),
    )
    return {
        "alias": name,
        "before": {"version": a_idx, "hash": a_hash},
        "after": {"version": b_idx, "hash": b_hash},
        "schema": diff["schema"],
        "stats": diff["stats"],
        "keyed_summary": ({k: v for k, v in diff["keyed"].items() if k != "table_html"} if diff["keyed"] else None),
    }


@mcp.tool()
@_tag_project
def catalog_promote_diff(name: str, va: int = -2, vb: int = -1, alias: str | None = None) -> dict:
    """Promote a diff between two versions to a first-class catalog entry.

    Creates a named catalog entry whose expression is the full outer-join
    comparison of the two versions, with Buckaroo coloring preserved. The
    entry can be post-processed, charted, and exported to a marimo notebook.

    Args:
        name: An existing alias.
        va, vb: 1-based version indices, or negative for from-end (-1 = latest).
        alias: Name for the new entry. Defaults to diff_{name}_v{va}_{vb}.
    """
    import textwrap

    from tallyman_companion.diff import compute_column_config_overrides
    from tallyman_core.aliases import AliasExists, set_alias
    from tallyman_core.catalog_state import checkpoint_catalog
    from tallyman_core.display_configs import set_display_config
    from tallyman_xorq import build_and_persist as _build_and_persist
    from tallyman_xorq.build import BuildError
    from tallyman_xorq.primary_key import diff_keys
    from tallyman_xorq.result_cache import cached_result_expr

    project = _resolve_active_project()
    hashes = history_for(project, name)
    if not hashes:
        return {"error": f"alias {name!r} has no history"}

    def _resolve(v):
        if v < 0:
            v = len(hashes) + v + 1
        if v < 1 or v > len(hashes):
            return None
        return v, hashes[v - 1]

    a = _resolve(va)
    b = _resolve(vb)
    if a is None or b is None:
        return {"error": f"version out of range; alias has {len(hashes)} versions"}
    a_idx, a_hash = a
    b_idx, b_hash = b

    keys = diff_keys(project, a_hash, b_hash) or []
    if not keys:
        return {"error": "no stable join key detected between the two versions; cannot build keyed diff"}

    a_expr = cached_result_expr(project, a_hash)
    b_expr = cached_result_expr(project, b_hash)
    column_config_overrides = compute_column_config_overrides(a_expr.schema(), b_expr.schema(), keys)

    target_alias = alias or f"diff_{name}_v{a_idx}_v{b_idx}"
    keys_repr = repr(keys)
    code = textwrap.dedent(f"""\
        # auto-generated — diff of {name} V{a_idx} → V{b_idx}
        # a_hash: {a_hash}
        # b_hash: {b_hash}
        from tallyman_companion.diff import build_diff_expr
        expr = build_diff_expr(
            a_hash={a_hash!r},
            b_hash={b_hash!r},
            keys={keys_repr},
        )
    """)

    try:
        result = _build_and_persist(project, code, prompt=f"promote diff {name} V{a_idx}→V{b_idx}")
    except BuildError as exc:
        return {"error": str(exc)}

    repointed = False
    try:
        set_alias(project, target_alias, result.content_hash, expect_exists=False)
    except AliasExists:
        set_alias(project, target_alias, result.content_hash)
        repointed = True  # re-pointed an existing alias → its followers may go stale

    set_display_config(
        project,
        result.content_hash,
        {
            "column_config_overrides": column_config_overrides,
            "diff_provenance": {
                "source_alias": name,
                "va": a_idx,
                "vb": b_idx,
                "a_hash": a_hash,
                "b_hash": b_hash,
                "keys": keys,
            },
        },
    )

    # D5: every existing-alias head-mover triggers auto-recalc via the shared
    # helper. A fresh target_alias (expect_exists=False) has no followers; only
    # the re-point branch can. The checkpoint-free walk runs BEFORE this op's
    # checkpoint so that one revision sweeps the promote + the cascade together.
    recalc_report = (
        _auto_recalc_after_head_advance(project, target_alias, tool="catalog_promote_diff") if repointed else None
    )

    # promote_diff is checkpoint-exempt (self-checkpoints): THIS is the single
    # revision the promote + cascade land in. Stamp its step onto the recalc
    # sub-report — the checkpoint-free walk built it before this commit existed.
    step = checkpoint_catalog(project, f"tallyman: catalog_promote_diff {name} V{a_idx}→V{b_idx}")
    if recalc_report is not None:
        recalc_report["checkpoint_step"] = step
    _notify("entry_added", content_hash=result.content_hash)
    out = {
        "alias": target_alias,
        "hash": result.content_hash,
        "source_alias": name,
        "va": a_idx,
        "vb": b_idx,
        "a_hash": a_hash,
        "b_hash": b_hash,
        "keys": keys,
        "row_count": result.row_count,
    }
    if recalc_report is not None:
        out["recalc"] = recalc_report
    return out


@mcp.tool()
@_tag_project
def catalog_scan_staleness(verify_results: bool = False) -> dict:
    """Scan every catalog entry for staleness against its recorded inputs.

    Read-only. An entry is *stale* when an input it was built against moved:
    a followed alias (built with ``tracked_expr_from_alias("name")``) advanced past the
    recorded head, or a recorded source file's content drifted on disk. A
    *directly* stale entry has its own input moved; a *transitively* stale entry
    is a clean descendant of a stale ancestor. Only current alias heads are
    directly stale (#154): a superseded historical version reports ``live=False``
    and is never listed, since recomputing it re-points no alias. Use
    ``catalog_recalc`` to act.

    ``verify_results=True`` additionally sweeps result faithfulness: every entry
    with a recorded ``result_digest`` has its baked snapshot re-hashed and
    compared. Adds a ``verify`` key — ``{results: {hash: bool|null}, unfaithful:
    [hashes], errors: {hash: message}}`` — where ``false`` means the snapshot's
    bytes are not what the build recorded (a nondeterministic recompute healed it
    to different bytes). Hashes every snapshot file, so opt-in.

    Returns:
        stale: hashes that are directly stale (the natural recalc roots).
        transitively_stale: clean descendants carried by a stale ancestor.
        entries: ``{hash: verdict}`` for every live entry, where a verdict is
            ``{stale, live, reasons:[{axis, ref, was, now}], unknown_axes,
            transitively_stale}``.
        verify: only when ``verify_results=True`` (see above).
    """
    project = _resolve_active_project()
    verdicts = staleness.scan(project)
    # D5 scan-on-load backstop: classify the directly-stale set as orphans so a
    # project load surfaces unexplained staleness even when no revise has fired
    # since (a bypass, a source edit). Gated on the switch: manual mode (flag off)
    # owns its own staleness and a directly-stale follower is expected there, not
    # a bug to flag. Same classifier auto-recalc uses, so the verdict is identical.
    orphan_stale = classify_orphans(project, verdicts) if auto_recalc_enabled(project) else []
    out = {
        "stale": sorted(h for h, v in verdicts.items() if v.stale),
        "transitively_stale": sorted(h for h, v in verdicts.items() if v.transitively_stale),
        "entries": {h: asdict(v) for h, v in verdicts.items()},
        "orphan_stale": orphan_stale,
    }
    if verify_results:
        out["verify"] = staleness.verify_sweep(project)
    return out


@mcp.tool()
@_tag_project
def catalog_recalc(roots: list[str] | None = None, dry_run: bool = True) -> dict:
    """Recompute stale entries and their dependents, in dependency order.

    ``dry_run=True`` (the default) previews the cone — what would rebuild, in
    order — without building anything. Run it first; call again with
    ``dry_run=False`` to commit. A real run replays each entry's recipe (so it
    re-resolves a followed parent to the advanced alias head), re-points the
    followed aliases, and takes ONE checkpoint for the whole walk, so a recalc
    is a single revision ``reset_to`` can undo atomically. A hash-pinned child
    re-pins the same parent and is left unchanged. On a build failure the walk
    stops, the already-rebuilt prefix stays committed, and the report flags it.

    Args:
        roots: content hashes to recompute together with their dependents. When
            omitted, defaults to every directly-stale entry (a scan-driven recalc).
        dry_run: preview only (default True). False commits the recompute.

    Returns the report: the ordered ``cone``, each entry's ``action``
    (dry-run: rebuild/cascade/unchanged; real: rebuilt/noop/failed/skipped),
    the old→new ``remap``, the ``status`` (ok/failed/cycle), and
    ``checkpoint_step`` for a real run that changed something.
    """
    project = _resolve_active_project()
    if roots is None:
        roots = sorted(h for h, v in staleness.scan(project).items() if v.stale)
    if not roots:
        return {
            "status": "ok",
            "roots": [],
            "cone": [],
            "entries": [],
            "dry_run": dry_run,
            "remap": {},
            "note": "nothing stale to recompute",
        }
    report = recalc(project, roots, dry_run=dry_run)
    if not dry_run and report.remap:
        # Forward remap + checkpoint step as FLAT kwargs so the companion can
        # republish the same normalized {kind, remap, step} SSE event the
        # in-process /api/recalc route emits (see app._recalc_sse_event). _notify
        # packs **extra into the wire `extra`; a literal extra={...} would nest as
        # extra.extra and the handler's extra.remap read would drop it.
        _notify("recalc", remap=report.remap, step=report.checkpoint_step)
    return asdict(report)


@mcp.tool()
@_tag_project
def catalog_add_summary_stat(name: str, source: str) -> dict:
    """Register a project-authored column summary stat.

    ``source`` must define a callable ``compute(col)`` that takes an ibis
    column expression and returns an ibis scalar expression. The function
    is validated by dry-running it against a 1-row in-memory table before
    the file lands on disk — syntax / type / shape errors surface in the
    tool response, not later in the buckaroo log.

    The stat is hot-reloaded into all open buckaroo sessions immediately
    after this call; no page refresh is needed.

    ``source`` example::

        def compute(col):
            return (col.cast("string").contains("@").sum() / col.count())

    The function name MUST be ``compute``; the *filename* is ``<name>.py``
    under ``<project>/stats/``, and the file's stem is the stat's key in
    every column's stats panel.

    PINNED-ROW NOTE: adding a stat makes it visible in the "summary" stats
    panel only.  To show it as a pinned row in the *main* table view, call
    ``catalog_add_display_klass`` immediately after with a class that
    subclasses ``DefaultMainStyling`` and extends ``pinned_rows``::

        class MainWith<Name>(DefaultMainStyling):
            df_display_name = "main"
            requires_summary = ["histogram", "is_numeric", "dtype", "_type", "<name>"]
            pinned_rows = [
                {'primary_key_val': 'dtype',     'displayer_args': {'displayer': 'obj'}},
                {'primary_key_val': 'histogram', 'displayer_args': {'displayer': 'histogram'}},
                {'primary_key_val': '<name>',    'displayer_args': {'displayer': 'inherit'}},
            ]

    Args:
        name: filename stem (no ``.py``). Must be a valid python identifier.
        source: the file body. Must define ``compute(col) -> ibis_expr``.
    """
    project = _resolve_active_project()
    try:
        path = write_stat(project, name, source)
    except StatSourceError as exc:
        return {"error": str(exc)}
    _notify("summary_stat_changed", extra={"action": "add", "name": name})
    return {"name": name, "path": str(path)}


@mcp.tool()
@_tag_project
def catalog_remove_summary_stat(name: str) -> dict:
    """Soft-delete a project-authored stat.

    Moves ``stats/<name>.py`` to ``stats/_disabled/<name>.py``. The
    ``_disabled/`` subdirectory is ignored by buckaroo's scanner (the
    ``_`` prefix is the convention) so the stat disappears from new
    sessions on the next load. To hard-delete, ``rm`` the file under
    ``stats/_disabled/`` manually.
    """
    project = _resolve_active_project()
    new_path = remove_stat(project, name)
    if new_path is None:
        return {"error": f"no stat named {name!r}"}
    _notify("summary_stat_changed", extra={"action": "remove", "name": name})
    return {"name": name, "moved_to": str(new_path)}


@mcp.tool()
@_tag_project
def catalog_list_summary_stats() -> list[dict]:
    """List every project-authored summary stat (active + disabled).

    Returns ``[{name, path, source, disabled}]`` sorted with active stats
    first, then disabled (parked) ones.
    """
    project = _resolve_active_project()
    return list_stats(project)


@mcp.tool()
@_tag_project
def catalog_add_display_klass(name: str, source: str) -> dict:
    """Register a project-authored display klass (ColAnalysis subclass).

    Display klasses control how columns are rendered and which rows are
    pinned at the top of a buckaroo table.  They are exec'd with
    ``ColAnalysis``, ``DefaultMainStyling``, ``DefaultSummaryStatsStyling``,
    and ``StylingAnalysis`` already in scope — no imports needed.

    The klass is hot-reloaded into all open buckaroo sessions immediately
    after this call; no page refresh is needed.

    PRIMARY USE CASE — adding a stat as a pinned row in the main view:
    after calling ``catalog_add_summary_stat`` for a new stat, call this
    tool to extend ``DefaultMainStyling`` so the stat appears as a frozen
    row at the top of every catalog entry's main table::

        class MainWithMostFreq(DefaultMainStyling):
            df_display_name = "main"
            requires_summary = ["histogram", "is_numeric", "dtype", "_type", "most_freq"]
            pinned_rows = [
                {'primary_key_val': 'dtype',      'displayer_args': {'displayer': 'obj'}},
                {'primary_key_val': 'histogram',  'displayer_args': {'displayer': 'histogram'}},
                {'primary_key_val': 'most_freq',  'displayer_args': {'displayer': 'inherit'}},
            ]

    Pinned-row displayer options:
      - ``'inherit'``  — use the column's natural type displayer (recommended)
      - ``'obj'``      — render as a raw Python object (fallback)
      - ``'float'``    — fixed decimal places (for numeric stats)
      - ``'histogram'``— histogram bar (only for the ``histogram`` stat key)

    The ``df_display_name`` must match an existing display slot:
      - ``"main"``     — the primary table view (override DefaultMainStyling)
      - ``"summary"``  — the all-stats panel (override DefaultSummaryStatsStyling)

    Args:
        name: filename stem (no ``.py``). Must be a valid Python identifier.
        source: class definition. Must produce at least one ColAnalysis
            subclass with a string ``df_display_name`` attribute.
    """
    project = _resolve_active_project()
    try:
        path = write_display_klass(project, name, source)
    except DisplayKlassError as exc:
        return {"error": str(exc)}
    _notify("display_changed", extra={"action": "add", "name": name})
    return {"name": name, "path": str(path)}


@mcp.tool()
@_tag_project
def catalog_list_display_klasses() -> list[dict]:
    """List every project-authored display klass (active + disabled).

    Returns ``[{name, path, source, disabled}]`` sorted with active klasses
    first, then disabled (parked) ones.
    """
    project = _resolve_active_project()
    return list_display_klasses(project)


@mcp.tool()
@_tag_project
def catalog_remove_display_klass(name: str) -> dict:
    """Soft-delete a project-authored display klass.

    Moves ``display/<name>.py`` to ``display/_disabled/<name>.py``.
    The ``_disabled/`` subdirectory is ignored by buckaroo's scanner so
    the klass disappears from new sessions on the next load.
    """
    project = _resolve_active_project()
    new_path = remove_display_klass(project, name)
    if new_path is None:
        return {"error": f"no display klass named {name!r}"}
    _notify("display_changed", extra={"action": "remove", "name": name})
    return {"name": name, "moved_to": str(new_path)}


@mcp.tool()
@_tag_project
def catalog_add_post_processing(name: str, source: str) -> dict:
    """Register a project-authored post-processing function.

    ``source`` must define a callable ``process(expr)`` that takes the
    table expression and returns either a transformed ibis expression or
    a pandas DataFrame. The function is validated by dry-running it
    against a small in-memory table before the file lands on disk —
    syntax / type / shape errors surface in the tool response, not later
    in the buckaroo log.

    SANDBOX: ``process`` runs in a restricted namespace. You MAY use ``expr``
    (an ibis table), the injected ``ibis`` module, and ``expr.execute()`` (a
    pandas DataFrame whose methods you may call). You MAY NOT ``import`` third
    party modules — ``import numpy/scipy/sklearn/pandas`` raises
    ``ImportError('__import__ not found')``. To FIT A MODEL, use the xorq.ml
    MODELING path on ``catalog_create`` instead (see its docstring); for stats
    not in ibis, compute them with ibis aggregates.

    VALIDATOR GOTCHA: the dry-run executes ``process`` against a tiny table with
    columns ``{a, b}`` only, so a function that references real column names is
    rejected at commit even when ``catalog_run_post_processing`` previewed it
    fine. Guard for the dry-run, e.g. ``if "wine_type" not in expr.columns: return expr``.

    The function shows up as a new option in the embed's "post
    processing" dropdown the next time a catalog entry's buckaroo
    session is loaded (clicking a different entry, revising an alias,
    or after a buckaroo restart). V1 does NOT hot-swap already-loaded
    sessions.

    ``source`` example — flag low-volume regions::

        def process(expr):
            return expr.filter(expr.n < 100)

    The function name MUST be ``process``; the *filename* is
    ``<name>.py`` under ``<project>/post_processing/``, and the file's
    stem is the option label in the dropdown.

    Args:
        name: filename stem (no ``.py``). Must be a valid python identifier.
        source: file body. Must define ``process(expr) -> ibis_expr | DataFrame``.
    """
    project = _resolve_active_project()
    try:
        path = write_post_processing(project, name, source)
    except PostProcessingSourceError as exc:
        return {"error": str(exc)}
    _notify("post_processing_changed", extra={"action": "add", "name": name})
    return {"name": name, "path": str(path)}


@mcp.tool()
@_tag_project
def catalog_remove_post_processing(name: str) -> dict:
    """Soft-delete a project-authored post-processing function.

    Moves ``post_processing/<name>.py`` to
    ``post_processing/_disabled/<name>.py``. The ``_disabled/``
    subdirectory is ignored by buckaroo's scanner so the option
    disappears from the dropdown on the next session load.
    """
    project = _resolve_active_project()
    new_path = remove_post_processing(project, name)
    if new_path is None:
        return {"error": f"no post-processing named {name!r}"}
    _notify("post_processing_changed", extra={"action": "remove", "name": name})
    return {"name": name, "moved_to": str(new_path)}


@mcp.tool()
@_tag_project
def catalog_list_post_processings() -> list[dict]:
    """List every project-authored post-processing function (active + disabled).

    Returns ``[{name, path, source, disabled}]`` sorted with active
    entries first, then disabled (parked) ones.
    """
    project = _resolve_active_project()
    return list_post_processings(project)


@mcp.tool()
@_tag_project
def catalog_run_post_processing(code: str, entry: str) -> dict:
    """Test a post-processing function against a real catalog entry's result.

    Use this to iterate on ``process(expr)`` code before committing it with
    ``catalog_add_post_processing``. The function is executed against the
    entry's result (read via ``cached_result_expr`` — a cheap entry recomputes,
    an expensive one reads its baked snapshot) — you see real output rows, not
    the small dry-run memtable that ``catalog_add_post_processing`` validates
    against.

    SANDBOX: same restricted namespace as ``catalog_add_post_processing`` —
    ``expr`` / ``ibis`` / ``expr.execute()`` are available, but ``import`` of
    numpy/scipy/sklearn/pandas fails. Fit models via the xorq.ml MODELING path
    on ``catalog_create``, not here.

    Typical workflow::

        # 1. Draft the function.
        catalog_run_post_processing(
            code="def process(expr):\\n    return expr.filter(expr.n > 10)\\n",
            entry="shoe_sales",
        )
        # 2. Iterate until the preview looks right.
        # 3. Commit it.
        catalog_add_post_processing("high_volume", code)

    ``entry`` can be an alias name (e.g. ``"shoe_sales"``) or a raw content
    hash. The current alias revision is used when both exist.

    Args:
        code: Python source defining ``process(expr) -> ibis_expr | DataFrame``.
        entry: Alias name or content hash of the catalog entry to test against.

    Returns:
        dict with keys: ``entry`` (resolved hash), ``row_count``, ``columns``,
        ``preview`` (first 20 rows as list-of-dicts). On error, ``{"error": ...}``.
    """
    project = _resolve_active_project()
    try:
        return run_post_processing(project, entry, code)
    except PostProcessingRunError as exc:
        return {"error": str(exc)}


@mcp.tool()
@_tag_project
def catalog_export_marimo() -> dict:
    """Export the project's notebook to a Marimo Python notebook file.

    Converts the default notebook to a ``.py`` file that can be opened and
    run directly with ``marimo edit notebook_marimo.py``. Each notebook cell
    becomes a pair of Marimo cells: a markdown narrative cell and a code cell
    that re-runs the xorq expression and displays the result DataFrame.

    The file is written to ``<project_dir>/notebook_marimo.py`` and is also
    downloadable via the companion's ``/export/marimo`` route.

    Returns:
        dict with keys: ``path`` (absolute path of the written file),
        ``cells`` (number of notebook cells exported).
    """
    project = _resolve_active_project()
    try:
        path = export_notebook_path(project)
    except Exception as exc:
        return {"error": str(exc)}
    from tallyman_core.notebook import load as _load  # noqa: PLC0415

    n = len(_load(project).get("cells", []))
    return {"path": str(path), "cells": n}


def _compact_columns(project: str, content_hash: str) -> str | None:
    """Compact ``"name:type, name:type"`` column summary for an entry.

    Read straight from the entry's ``schema.json`` so the model can see an
    entry's columns before writing an expression against it — no build, one
    short line to scan. Returns ``None`` when the schema isn't on disk.
    """
    schema_path = entry_schema_path(project, content_hash)
    if not schema_path.exists():
        return None
    try:
        fields = json.loads(schema_path.read_text()).get("fields", [])
    except Exception:
        return None
    return ", ".join(f"{f['name']}:{f['type']}" for f in fields) or None


@mcp.tool()
@_tag_project
def catalog_list() -> list[dict]:
    """List every entry in the current project's catalog (most recent first).

    Each entry is annotated with `alias` and `version` if it currently
    corresponds to a named version. Scratch entries (no alias) are also
    included.

    Each entry also carries `columns`: a compact `"name:type, name:type"`
    summary read from the entry's schema. Call this BEFORE writing an
    expression against an entry you did not just build — reference the exact
    column names shown here rather than guessing (`tripduration` is not
    `trip_duration`).
    """
    project = _resolve_active_project()
    entries = list_entries(project)
    for e in entries:
        a = alias_for_hash(project, e["content_hash"])
        if a is not None:
            from tallyman_core import version_of_hash

            info = version_of_hash(project, e["content_hash"])
            e["alias"] = a
            e["version"] = info[1] if info else 1
        else:
            e["alias"] = None
            e["version"] = None
        e["columns"] = _compact_columns(project, e["content_hash"])
    return entries


# ---------------------------------------------------------------------------
# Project lifecycle tools (T-45)
#
# Read-side (``project_list``) touches the file directly via tallyman_core,
# so it works when the companion isn't running. Write-side
# (``project_switch``, ``project_new``) POST to the companion's
# /api/projects/* routes — the companion is the sole writer of the
# active-project file so SSE event publishing stays honest. See
# plans/project_switcher.md (decisions 1, 6, 8) for the rationale.
# ---------------------------------------------------------------------------


def _companion_post(path: str, payload: dict) -> dict:
    """POST to the companion; normalise transport / HTTP errors into a
    ``{"error": ...}`` dict that the MCP caller can render. The companion
    URL is included so the LLM can tell the user which endpoint to bring
    up if the request failed."""
    url = f"{COMPANION_URL}{path}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(url, json=payload)
    except httpx.HTTPError as exc:
        return {
            "error": (
                f"companion not reachable at {COMPANION_URL}: {exc!s}. "
                f"Project lifecycle tools require 'tallyman run' to be active."
            )
        }
    if resp.status_code >= 400:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        return {"error": f"companion at {url} returned {resp.status_code}: {detail}"}
    try:
        return resp.json()
    except Exception as exc:
        return {"error": f"companion at {url} returned non-JSON: {exc!s}"}


@mcp.tool()
@_tag_project
def project_list() -> dict:
    """List every project on disk and report which one is active.

    Read-only. ``active`` reflects this process's sticky in-process
    project (consistent with every other tool), falling back to the
    active-project file only if it was never explicitly set; ``available``
    is read from the projects directory on disk. Works when the companion
    process isn't running, so this is the safe tool to call before
    deciding whether to switch.

    Returns:
        dict with keys: ``active`` (project name, or ``None`` if no file
        exists yet) and ``available`` (alphabetically-sorted list of
        every project name on disk).
    """
    return {"active": _resolve_active_project(), "available": list_projects()}


@mcp.tool()
@_tag_project
def project_switch(name: str) -> dict:
    """Switch the active project for every subsequent companion + MCP call.

    POSTs to the companion's ``/api/projects/switch`` — the companion
    rewrites ``~/.tallyman/active_project`` and broadcasts a
    ``project_switched`` SSE event so any open browser tabs reload into
    the new project.

    Args:
        name: An existing project (must already appear in
            ``project_list().available``). 404 if the project doesn't
            exist; 400 if the name is invalid.

    Returns:
        ``{"previous": "<old>", "active": "<name>"}`` on success.
        ``{"error": "..."}`` if the companion is down, the URL is wrong,
        or the request was rejected.

    Example workflow::

        project_list()                # see what's available
        project_switch("forecast-q3") # all subsequent catalog_* calls
                                       # now target forecast-q3
    """
    global _mcp_active_project
    result = _companion_post("/api/projects/switch", {"name": name})
    if "error" not in result:
        _mcp_active_project = name
    return result


@mcp.tool()
@_tag_project
def project_new(name: str, with_fixture: bool = False) -> dict:
    """Create a new project on disk and switch to it.

    POSTs to the companion's ``/api/projects/new`` — the companion calls
    ``ensure_project``, optionally writes the shoe-orders demo fixture,
    flips the active-project file, and broadcasts ``project_switched``.

    Args:
        name: New project name. Must match ``^[a-z0-9][a-z0-9_-]{0,31}$``
            and not collide with an existing project (409 on collision).
        with_fixture: When ``True``, copy the shoe-orders demo parquet
            into the new project's ``data/`` directory. Defaults to
            ``False`` — the demo fixture is opt-in for MCP-driven
            project creation; the CLI ``tallyman init`` keeps the old
            default-on behavior for demo workflows.

    Returns:
        ``{"name": "<name>", "active": "<name>"}`` on success.
        ``{"error": "..."}`` on transport failure, invalid name (400),
        or collision (409).
    """
    global _mcp_active_project
    result = _companion_post("/api/projects/new", {"name": name, "with_fixture": with_fixture})
    if "error" not in result:
        _mcp_active_project = name
    return result


@mcp.prompt()
def ds_modeling_workflow(dataset: str, target: str = "") -> str:
    """Scaffold a data-science modeling workflow with the tallyman catalog tools:
    load -> engineer features -> split -> fit (via xorq.ml) -> score -> chart.

    ``dataset`` is a parquet under the project's ``data/`` dir; ``target`` is
    the column to predict (leave empty for unsupervised/clustering).
    """
    tgt = target or "<target column>"
    return (
        f"Build a modeling workflow on {dataset!r} predicting {tgt!r} using the tallyman catalog tools.\n"
        "1. catalog_load_parquet the dataset under a name.\n"
        "2. catalog_create an engineered-feature entry — new computed columns first, "
        "ibis.cases() for encodings, and add a stable row id for splitting/diffing.\n"
        "3. Fit the model AS A CATALOG ENTRY with xorq.ml (deferred_fit_predict_sklearn + "
        "train_test_splits) — never inside a post-processing function (that sandbox blocks "
        "sklearn/scipy/numpy imports). Copy the exact fit/predict calls from the MODELING section "
        "of the catalog_run tool docstring (catalog_create inherits it); that docstring is the "
        "canonical source for the xorq.ml API, so follow it rather than guessing the signatures.\n"
        "4. Score in PURE IBIS on the prediction column (e.g. accuracy=(t.y==t.pred).mean(), or "
        "rmse=((t.y-t.pred)**2).mean().sqrt()) — deferred_sklearn_metric does NOT survive catalog "
        "build (see the same MODELING section). Attach a predicted-vs-actual chart.\n"
        "Reminders: import xorq.vendor.ibis as ibis (never bare import ibis); the api module is "
        "import xorq.api as xo (never bare xorq.<fn>); the only backend is xorq's built-in datafusion "
        "(no duckdb); check columns with catalog_list before referencing them; no project= kwargs; "
        "do not fabricate placeholder results."
    )


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("TALLYMAN_LOG_LEVEL", "INFO"),
        format="[%(name)s] %(message)s",
    )
    from tallyman_core.version import git_revision  # noqa: PLC0415

    log.info("tallyman_mcp revision %s", git_revision())
    mcp.run()


if __name__ == "__main__":
    main()
