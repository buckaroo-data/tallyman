class DiffDetailedPctStyling(DefaultMainStyling):  # noqa: F821
    """'detailed_pct' display for diff sessions: show *_pct_delta, hide *_abs_delta.

    Exposes raw percentage-change columns alongside the standard diff columns
    so the user can inspect relative magnitude of each change.
    """

    df_display_name = "detailed_pct"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        return [c for c in result if not c.get("col_name", "").endswith("_abs_delta")]
