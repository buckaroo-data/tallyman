class DiffDetailedAbsoluteStyling(DefaultMainStyling):
    """'detailed_absolute' display for diff sessions: show *_abs_delta, hide *_pct_delta.

    Exposes absolute-change columns (new − old, same units as the original column)
    alongside the standard diff columns so the user can inspect raw magnitude of change.
    """

    df_display_name = "detailed_absolute"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        return [c for c in result if not c.get("col_name", "").endswith("_pct_delta")]
