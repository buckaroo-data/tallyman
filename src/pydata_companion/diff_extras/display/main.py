class DiffMainStyling(DefaultMainStyling):
    """Override the 'main' display for diff sessions: hide *_pct_delta columns.

    The pct_delta columns are available in the 'detailed_pct' display.
    Numeric diff columns use _v2 as the visible column (header renamed to the
    original name, color-mapped by pct_delta) — so pct_delta itself is
    redundant noise in the default view.
    """

    df_display_name = "main"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        return [c for c in result if not c.get("col_name", "").endswith("_pct_delta")]
