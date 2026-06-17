"""Diff expression builder — shared between the companion app and catalog tools.

Two entry points:

  build_compare_expr(a_expr, b_expr, keys)
      Pure ibis expression construction: returns (expr, column_config_overrides).
      The expr is the full outer-join with membership / per-column equality /
      pct-delta / abs-delta sentinel columns that drive Buckaroo coloring.

  build_diff_expr(a_hash, b_hash, keys)
      Convenience wrapper used as the internal expr.py rebuild path for
      promoted diff entries.  Resolves the active project, composes both
      entries' cache-resolving expressions, and delegates to build_compare_expr.
"""

from __future__ import annotations

from typing import Any


def _is_comparable(dt_a: Any, dt_b: Any) -> bool:
    """True if two dtypes can be compared with ibis ``==`` without a XorqTypeError.

    Identical dtypes always compare; two numeric dtypes are mutually castable
    (the numeric diff path already casts both to float64).  A name-shared column
    whose dtype changed across revisions to an incomparable type (e.g.
    string→date, #68) returns False and falls back to side-by-side display.
    """
    return dt_a == dt_b or (dt_a.is_numeric() and dt_b.is_numeric())


def _classify_shared(a_schema: Any, b_schema: Any, keys: list[str]) -> tuple[list, list, set, set]:
    """Partition non-key columns shared by name into numeric / comparable sets.

    Returns (a_non_keys, b_non_keys, numeric_shared, eq_shared):
      numeric_shared  name-shared and numeric on *both* sides → pct/abs deltas
      eq_shared       name-shared and dtype-comparable → emit a {col}_eq term

    numeric_shared ⊆ eq_shared.  A name-shared column in neither set changed to
    an incomparable dtype and renders side-by-side with no equality term.
    """
    a_non_keys = [c for c in a_schema if c not in keys]
    b_non_keys = [c for c in b_schema if c not in keys]
    shared = [c for c in a_non_keys if c in b_non_keys]
    numeric_shared = {c for c in shared if a_schema[c].is_numeric() and b_schema[c].is_numeric()}
    eq_shared = {c for c in shared if _is_comparable(a_schema[c], b_schema[c])}
    return a_non_keys, b_non_keys, numeric_shared, eq_shared


def compute_column_config_overrides(a_schema: Any, b_schema: Any, keys: list[str]) -> dict:
    """Compute Buckaroo column_config_overrides from two ibis schemas and join keys.

    Pure schema analysis — no expression building.  Called by both build_compare_expr
    and the promote paths so the expression is only constructed once (during build).
    """
    a_non_keys, b_non_keys, numeric_shared, eq_shared = _classify_shared(a_schema, b_schema, keys)

    eq_map = ["#e8b4b8", "#73ae80", "#90b2b3", "#6c83b5"]
    pk_color = "#6c5fc7"
    pk_map = [pk_color] * 4
    overrides: dict = {"membership": {"merge_rule": "hidden"}}
    for col in a_non_keys:
        if col not in b_non_keys:
            continue
        if col not in eq_shared:
            # #68: dtype changed to an incomparable type across revisions, so no
            # {col}_eq term is emitted.  Render side-by-side — a-side stays
            # visible, b-side shows the new value with the old in a tooltip and
            # no equality/magnitude coloring.
            overrides[f"{col}_v2"] = {
                "header_name": f"{col} (v2)",
                "tooltip_config": {"tooltip_type": "simple", "val_column": col},
            }
            continue
        overrides[col] = {"merge_rule": "hidden"}
        overrides[f"{col}_eq"] = {"merge_rule": "hidden"}
        if col in numeric_shared:
            # _pct_delta and _abs_delta are not hidden here so that
            # DiffDetailedPctStyling / DiffDetailedAbsoluteStyling can
            # display them.  DiffMainStyling filters them by name.
            #
            # This pct_delta coloring is what a *promoted* diff entry
            # renders with: it's persisted into display_configs and applied
            # without the diff display klasses (which only load on the live
            # /diff route).  The live diff route strips this numeric color
            # via strip_live_diff_color() so the per-view klass coloring
            # (pct_delta for main/detailed_pct, abs_delta for
            # detailed_absolute) isn't clobbered by merge_column_config.
            overrides[f"{col}_v2"] = {
                "header_name": col,
                "tooltip_config": {"tooltip_type": "simple", "val_column": col},
                "color_map_config": {
                    "color_rule": "color_map",
                    "map_name": "DIVERGING_RED_WHITE_BLUE",
                    "val_column": f"{col}_pct_delta",
                },
            }
        else:
            overrides[f"{col}_v2"] = {
                "header_name": col,
                "tooltip_config": {"tooltip_type": "simple", "val_column": col},
                "color_map_config": {
                    "color_rule": "color_categorical",
                    "map_name": eq_map,
                    "val_column": f"{col}_eq",
                },
            }
    for k in keys:
        overrides[k] = {
            "color_map_config": {
                "color_rule": "color_categorical",
                "map_name": pk_map,
                "val_column": "membership",
            }
        }
    return overrides


def strip_live_diff_color(overrides: dict) -> dict:
    """Return a copy of overrides with the numeric value-column coloring removed.

    The live /diff route loads the diff display klasses, which color each value
    column per view (pct_delta for main/detailed_pct, abs_delta for
    detailed_absolute). Because merge_column_config applies these overrides
    *after* the klasses run and ``row.update()`` would clobber the klass-set
    color, the shared 'color_map' (numeric pct_delta) entries must be dropped on
    the live path. The categorical key/equality colors are left intact, and the
    original overrides (with color) are kept for promoted entries, which render
    without the klasses.
    """
    out: dict = {}
    for col, cfg in overrides.items():
        cmc = cfg.get("color_map_config") if isinstance(cfg, dict) else None
        if cmc and cmc.get("color_rule") == "color_map":
            cfg = {k: v for k, v in cfg.items() if k != "color_map_config"}
        out[col] = cfg
    return out


def build_compare_expr(a_expr: Any, b_expr: Any, keys: list[str]) -> tuple[Any, dict]:
    """Build an outer-join comparison expression from two ibis expressions.

    Returns (joined_expr, column_config_overrides).  Column layout:
      - key columns: coalesced from both sides
      - for each non-key column:
          {col}           the "before" value (hidden)
          {col}_v2        the "after" value shown with color
          {col}_pct_delta (b-a)/|a|, numeric cols only; null when a=0
          {col}_abs_delta b-a, numeric cols only
      - membership (int8): 1=a_only, 2=b_only, 3=both
      - {col}_eq (int8): membership+4 if equal, membership+0 if different
    """
    import xorq.vendor.ibis as ibis
    from buckaroo.compare import _align_backends

    a_schema = a_expr.schema()
    b_schema = b_expr.schema()
    a_non_keys, b_non_keys, numeric_shared, eq_shared = _classify_shared(a_schema, b_schema, keys)

    # #68 (reopened after #69): a *key* column whose dtype changed across
    # revisions to an incomparable type (issue_date string→date) would make the
    # join predicate and the key coalesce compare incompatible dtypes and abort
    # the whole diff. Cast both sides of such keys to string — a universally
    # comparable type — for the join and coalesce, the same "never compare
    # incompatible dtypes" invariant #69 established for the non-key _eq path.
    recast_keys = {k for k in keys if not _is_comparable(a_schema[k], b_schema[k])}

    def _key(expr: Any, k: str) -> Any:
        return expr[k].cast("string") if k in recast_keys else expr[k]

    a_expr, b_expr = _align_backends(a_expr, b_expr)
    b_renamed = b_expr.rename({f"{c}_v2": c for c in b_non_keys})
    joined = a_expr.outer_join(b_renamed, [_key(a_expr, k) == _key(b_renamed, k) for k in keys])

    sel: list = [ibis.coalesce(_key(a_expr, k), _key(b_renamed, k)).name(k) for k in keys]
    for col in a_non_keys:
        sel.append(joined[col])
        if col in b_non_keys:
            sel.append(joined[f"{col}_v2"])
            if col in numeric_shared:
                a_val = joined[col].cast("float64")
                b_val = joined[f"{col}_v2"].cast("float64")
                pct = ibis.cases(
                    (a_val == ibis.literal(0.0), ibis.null().cast("float64")),
                    else_=(b_val - a_val) / a_val.abs(),
                ).name(f"{col}_pct_delta")
                sel.append(pct)
                sel.append((b_val - a_val).name(f"{col}_abs_delta"))

    a_sent = a_non_keys[0] if a_non_keys else keys[0]
    b_sent = f"{b_non_keys[0]}_v2" if b_non_keys else keys[0]
    membership = ibis.cases(
        (joined[b_sent].isnull(), ibis.literal(1).cast("int8")),
        (joined[a_sent].isnull(), ibis.literal(2).cast("int8")),
        else_=ibis.literal(3).cast("int8"),
    ).name("membership")
    sel.append(membership)

    for col in (c for c in a_non_keys if c in eq_shared):
        eq = (
            membership
            + ibis.cases(
                (joined[col] == joined[f"{col}_v2"], ibis.literal(4).cast("int8")),
                else_=ibis.literal(0).cast("int8"),
            )
        ).name(f"{col}_eq")
        sel.append(eq)

    expr = joined.select(*sel)
    overrides = compute_column_config_overrides(a_schema, b_schema, keys)
    return expr, overrides


def build_diff_expr(a_hash: str, b_hash: str, keys: list[str]) -> Any:
    """Build the comparison expression for a promoted diff entry.

    Composes both source entries' cache-resolving expressions (a cheap entry
    recomputes, an expensive one reads its baked snapshot) and delegates to
    build_compare_expr.  Both sides root on the default backend, so the join
    stays single-backend.  Project resolves via the active-project file (same
    mechanism as other catalog-aware code).
    """
    from tallyman_core.paths import resolve_project
    from tallyman_xorq.result_cache import cached_result_expr

    project = resolve_project()
    a_expr = cached_result_expr(project, a_hash)
    b_expr = cached_result_expr(project, b_hash)
    expr, _ = build_compare_expr(a_expr, b_expr, keys)
    return expr
