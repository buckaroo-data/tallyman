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


def _delta_alias_by_base(result, suffix):
    """Map base column name -> the short alias (col_name) of its delta column."""
    out = {}
    for c in result:
        h = c.get("header_name", "")
        if h.endswith(suffix):
            out[h[: -len(suffix)]] = c.get("col_name")
    return out


class DiffMainStyling(DefaultMainStyling):  # noqa: F821
    """'main' display for diff sessions.

    Shows only the value columns (the renamed ``_v2`` column), no delta
    columns. Each numeric value column is color-mapped by its ``_pct_delta``.
    Runs before column_config_overrides are merged, so columns still carry
    their raw headers here (``{col}_v2``, ``{col}_pct_delta``).
    """

    df_display_name = "main"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        pct_alias = _delta_alias_by_base(result, "_pct_delta")
        result = [
            c for c in result
            if not c.get("header_name", "").endswith(("_pct_delta", "_abs_delta"))
        ]
        for c in result:
            h = c.get("header_name", "")
            if h.endswith("_v2"):
                alias = pct_alias.get(h[: -len("_v2")])
                if alias:
                    c["color_map_config"] = {
                        "color_rule": "color_map",
                        "map_name": DIVERGING,
                        "val_column": alias,
                    }
        return result
