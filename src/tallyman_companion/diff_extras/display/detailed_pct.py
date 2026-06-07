def _diverging(n=101):
    """Blue -> white -> red diverging palette (positive/large delta = red).

    Generated inline because these display files are exec'd in a sandbox that
    forbids imports, so we can't pull in the curated palette from
    diff_color_maps. Color direction (positive = red) is what matters here.
    """

    def lerp(a, b, t):
        return tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))

    blue, white, red = (36, 127, 254), (255, 255, 255), (220, 40, 40)
    out = []
    for i in range(n):
        t = i / (n - 1)
        c = lerp(blue, white, t / 0.5) if t < 0.5 else lerp(white, red, (t - 0.5) / 0.5)
        out.append("#%02x%02x%02x" % c)
    return out


DIVERGING = _diverging()


# A 3-decimal float displayer renders any |value| < 5e-4 as "0.000", so a delta
# column whose min and max both fall under this displays as all-zero. Drop those
# (covers exact zeros and floating-point noise, e.g. 1e-6 lat/lng deltas).
_ZERO_TOL = 5e-4


def _all_zero(sd, c):
    stats = sd.get(c.get("col_name", ""), {})
    mn, mx = stats.get("min"), stats.get("max")
    if mn is None or mx is None:
        return False
    return abs(mn) < _ZERO_TOL and abs(mx) < _ZERO_TOL


def _color(alias):
    return {"color_rule": "color_map", "map_name": DIVERGING, "val_column": alias}


def _layout(result, sd, keep_suffix, drop_suffix):
    """Filter and color a detailed delta view, preserving SELECT order.

    _build_compare_expr already emits ``{col}_pct_delta`` / ``{col}_abs_delta``
    immediately after ``{col}_v2``, so no reordering is needed. We run before
    column_config_overrides are merged, so columns still carry raw headers
    (value columns ``{col}_v2``, deltas ``{col}<suffix>``). Drops the opposite
    delta and all-zero kept deltas, then color-maps both each value column and
    its kept delta by the delta's value (``col_name`` is the short alias the
    JS color map references).
    """
    result = [c for c in result if not c.get("header_name", "").endswith(drop_suffix)]
    result = [c for c in result if not (c.get("header_name", "").endswith(keep_suffix) and _all_zero(sd, c))]
    delta_alias = {
        c["header_name"][: -len(keep_suffix)]: c.get("col_name")
        for c in result
        if c.get("header_name", "").endswith(keep_suffix)
    }
    out = []
    for c in result:
        h = c.get("header_name", "")
        if h.endswith("_v2"):
            alias = delta_alias.get(h[: -len("_v2")])
            if alias:
                c = {**c, "color_map_config": _color(alias)}
        elif h.endswith(keep_suffix):
            c = {**c, "color_map_config": _color(c.get("col_name"))}
        out.append(c)
    return out


class DiffDetailedPctStyling(DefaultMainStyling):  # noqa: F821
    """'detailed_pct' display: each value column next to its ``_pct_delta``
    (all-zero deltas dropped), ``_abs_delta`` hidden, colored by pct_delta."""

    df_display_name = "detailed_pct"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        return _layout(result, sd, "_pct_delta", "_abs_delta")
