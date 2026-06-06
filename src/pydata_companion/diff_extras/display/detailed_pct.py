def _all_zero(df, col_name, suffix):
    if not col_name.endswith(suffix) or col_name not in df.columns:
        return False
    col = df[col_name].dropna()
    return col.empty or (col == 0).all()


class DiffDetailedPctStyling(DefaultMainStyling):  # noqa: F821
    """'detailed_pct' display for diff sessions: show *_pct_delta, hide *_abs_delta.

    Exposes raw percentage-change columns alongside the standard diff columns
    so the user can inspect relative magnitude of each change.
    """

    df_display_name = "detailed_pct"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        result = [c for c in result if not c.get("col_name", "").endswith("_abs_delta")]
        return [c for c in result if not _all_zero(df, c.get("col_name", ""), "_pct_delta")]
