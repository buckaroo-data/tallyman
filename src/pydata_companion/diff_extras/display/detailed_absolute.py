def _all_zero(df, col_name, suffix):
    if not col_name.endswith(suffix) or col_name not in df.columns:
        return False
    col = df[col_name].dropna()
    return col.empty or (col == 0).all()


class DiffDetailedAbsoluteStyling(DefaultMainStyling):  # noqa: F821
    """'detailed_absolute' display for diff sessions: show *_abs_delta, hide *_pct_delta.

    Exposes absolute-change columns (new − old, same units as the original column)
    alongside the standard diff columns so the user can inspect raw magnitude of change.
    """

    df_display_name = "detailed_absolute"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        result = [c for c in result if not c.get("col_name", "").endswith("_pct_delta")]
        return [c for c in result if not _all_zero(df, c.get("col_name", ""), "_abs_delta")]
