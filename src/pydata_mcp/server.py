from __future__ import annotations

import functools
import logging
import os
import sys

import httpx
from fastmcp import FastMCP

from pydata_core import (
    AliasExists,
    AliasNotFound,
    CellNotFound,
    ChartSpecError,
    PostProcessingRunError,
    PostProcessingSourceError,
    StatSourceError,
    alias_for_hash,
    entry_dir,
    export_notebook_path,
    get_alias,
    history_for,
    list_post_processings,
    list_projects,
    list_stats,
    notebook,
    record_error,
    remove_alias,
    remove_post_processing,
    remove_stat,
    rename_alias,
    resolve_project,
    run_post_processing,
    set_alias,
    set_chart,
    write_post_processing,
    write_stat,
)
from pydata_xorq import BuildError, build_and_persist, full_diff, list_entries

log = logging.getLogger("pydata_mcp")
COMPANION_URL = os.environ.get("PYDATA_COMPANION_URL", "http://127.0.0.1:7860")

mcp = FastMCP("pydata")

# Tracks the active project name observed at the end of the previous tool
# invocation, so the next call can detect a mid-conversation switch and
# surface a ``warning`` field (design decision 14 of the project-switcher
# plan). ``None`` until the first tool call seeds it.
_last_project: str | None = None

# Session-local active project. Set in-process by project_switch/project_new
# so that switching once is sticky for all subsequent calls in this MCP
# process, regardless of what another Claude session or shell command writes to
# ~/.pydata-app/active_project between calls. Falls back to the disk file if
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
        print(f"[pydata_mcp] notify failed ({kind}): {exc}", file=sys.stderr)


@mcp.tool()
@_tag_project
def catalog_run(code: str, prompt: str = "") -> dict:
    """Execute a xorq expression script and persist it as a catalog entry.

    The provided code MUST bind a top-level variable named `expr` to a xorq/ibis
    expression. Imports allowed; all imports happen in a fresh module scope.

    PREFERRED pattern:

        from pydata_xorq.io import from_project, from_catalog
        import xorq.vendor.ibis as ibis
        t = from_catalog("alias_name")            # named catalog entry (alias or hash)
        t = from_project("file.parquet")          # raw file under <project>/data/
        expr = t.filter(t.col > 0).group_by("region").aggregate(n=t.count())

    DATA SOURCING — choose between `from_catalog` and `from_project`:
      - `from_catalog("name")` resolves catalog aliases AND bare content hashes.
        Use this when the source appears in `catalog_list` output.
      - `from_project("file.parquet")` reads raw files under `<project>/data/`.
        Use this only for raw files visible on disk (not catalog aliases).
      - When in doubt, call `catalog_list` first. A name that looks like a file
        is often actually an alias — `from_project` on an alias raises
        `ProjectDataNotFound`.

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

    GOTCHAS — xorq vs ibis deferred accessor:

        xo._.column_name       # AttributeError: module 'xorq' has no attribute '_'
        # xorq has no `_` deferred proxy. Use ibis._:
        ibis._.column_name     # correct — `ibis` from `import xorq.vendor.ibis as ibis`
        t.mutate(pk=ibis._.a + "_" + ibis._.b)   # deferred string concat

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

    GOTCHAS — imports:

        # FORBIDDEN: even one `import ibis` makes the expression unsavable.
        # The expression constructs fine but explodes at catalog-save time with
        # a TypeError about ibis vs xorq.vendor.ibis Expr classes. Always use:
        import xorq.vendor.ibis as ibis

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
        t = from_catalog("diamond_features")
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
        - pass a project= argument to from_project/from_catalog; the active
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
    code = f"from pydata_xorq.io import from_project\nexpr = from_project({rel_path!r})\n"
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


def _run_and_record(project: str, code: str, prompt: str, *, tool: str = "catalog_run") -> dict:
    """Shared body for tools that compile-and-persist; returns the tool reply dict."""
    try:
        result = build_and_persist(project=project, code=code, prompt=prompt or None)
    except BuildError as exc:
        rec = record_error(project, code=code, message=str(exc), prompt=prompt or None, tool=tool)
        _notify("build_failed", error_id=rec["id"], tool=tool)
        return {"error": str(exc), "error_id": rec["id"]}
    return {
        "_build": result,
        "hash": result.content_hash,
        "row_count": result.row_count,
        "execute_seconds": result.execute_seconds,
        "schema": result.schema,
        "entry_path": str(result.entry_path),
    }


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
    out = _run_and_record(project, code, prompt, tool="catalog_create")
    if "error" in out:
        return out
    info = set_alias(project, name, out["hash"], expect_exists=False)
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

    The alias is repointed to the new content hash. The previous hash remains
    in the catalog as a forensic artifact and is recorded in alias_history.

    Code conventions and gotchas are documented in `catalog_run`. The same
    rules apply here. Use `from_catalog(name)` to chain off the previous
    version of the alias (or any other named entry).

    Args:
        name: An existing alias.
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description of what changed.
    """
    project = _resolve_active_project()
    if get_alias(project, name) is None:
        return {"error": f"alias {name!r} does not exist. Use catalog_create to create it."}
    out = _run_and_record(project, code, prompt, tool="catalog_revise")
    if "error" in out:
        return out
    info = set_alias(project, name, out["hash"], expect_exists=True)
    out.pop("_build", None)
    out["alias"] = info["name"]
    out["version"] = info["version"]
    _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
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
    return {"hash": hash, "alias": name, "version": info["version"]}


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

    # Diff the two entries' (cache-resolving) expressions — streaming xorq
    # aggregates, and self-healing if a result.parquet was evicted.
    from pydata_xorq.result_cache import cached_result_expr
    diff = full_diff(
        a_dir, b_dir, a_label=f"V{a_idx}", b_label=f"V{b_idx}", backend="xorq",
        a_expr=cached_result_expr(project, a_hash), b_expr=cached_result_expr(project, b_hash))
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
def catalog_add_summary_stat(name: str, source: str) -> dict:
    """Register a project-authored column summary stat.

    ``source`` must define a callable ``compute(col)`` that takes an ibis
    column expression and returns an ibis expression (scalar). The
    function is validated by dry-running it against a 1-row in-memory
    table before the file lands on disk — syntax / type / shape errors
    surface in the tool response, not later in the buckaroo log.

    The stat shows up alongside the built-in xorq column stats the next
    time a catalog entry's buckaroo session is loaded (clicking a
    different entry, or after a buckaroo restart). V1 does NOT hot-swap
    already-loaded sessions.

    ``source`` example::

        def compute(col):
            return (col.cast("string").contains("@").sum() / col.count())

    The function name MUST be ``compute``; the *filename* is ``<name>.py``
    under ``<project>/stats/``, and the file's stem is the stat's key in
    every column's stats panel.

    Args:
        name: filename stem (no ``.py``). Must be a valid python identifier.
        source: the file body. Must define ``compute(col) -> ibis_expr``.
    """
    project = _resolve_active_project()
    try:
        path = write_stat(project, name, source)
    except StatSourceError as exc:
        return {"error": str(exc)}
    _notify("stats_changed", extra={"action": "add", "name": name})
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
    _notify("stats_changed", extra={"action": "remove", "name": name})
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
    entry's stored ``result.parquet`` — you see real output rows, not the
    small dry-run memtable that ``catalog_add_post_processing`` validates
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
    from pydata_core.notebook import load as _load  # noqa: PLC0415

    n = len(_load(project).get("cells", []))
    return {"path": str(path), "cells": n}


@mcp.tool()
@_tag_project
def catalog_list() -> list[dict]:
    """List every entry in the current project's catalog (most recent first).

    Each entry is annotated with `alias` and `version` if it currently
    corresponds to a named version. Scratch entries (no alias) are also
    included.
    """
    project = _resolve_active_project()
    entries = list_entries(project)
    for e in entries:
        a = alias_for_hash(project, e["content_hash"])
        if a is not None:
            from pydata_core import version_of_hash

            info = version_of_hash(project, e["content_hash"])
            e["alias"] = a
            e["version"] = info[1] if info else 1
        else:
            e["alias"] = None
            e["version"] = None
    return entries


# ---------------------------------------------------------------------------
# Project lifecycle tools (T-45)
#
# Read-side (``project_list``) touches the file directly via pydata_core,
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
                f"Project lifecycle tools require 'pydata run' to be active."
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

    Read-only; reads the active-project file and the projects directory
    directly. Works when the companion process isn't running, so this is
    the safe tool to call before deciding whether to switch.

    Returns:
        dict with keys: ``active`` (project name, or ``None`` if no file
        exists yet) and ``available`` (alphabetically-sorted list of
        every project name on disk).
    """
    return {"active": resolve_project(), "available": list_projects()}


@mcp.tool()
@_tag_project
def project_switch(name: str) -> dict:
    """Switch the active project for every subsequent companion + MCP call.

    POSTs to the companion's ``/api/projects/switch`` — the companion
    rewrites ``~/.pydata-app/active_project`` and broadcasts a
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
            project creation; the CLI ``pydata init`` keeps the old
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
    """Scaffold a data-science modeling workflow with the pydata catalog tools:
    load -> engineer features -> split -> fit (via xorq.ml) -> score -> chart.

    ``dataset`` is a parquet under the project's ``data/`` dir; ``target`` is
    the column to predict (leave empty for unsupervised/clustering).
    """
    tgt = target or "<target column>"
    return (
        f"Build a modeling workflow on {dataset!r} predicting {tgt!r} using the pydata catalog tools.\n"
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
        "Reminders: import xorq.vendor.ibis as ibis (never bare import ibis); no project= kwargs; "
        "do not fabricate placeholder results."
    )


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PYDATA_LOG_LEVEL", "INFO"),
        format="[%(name)s] %(message)s",
    )
    mcp.run()


if __name__ == "__main__":
    main()
