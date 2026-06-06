from buckaroo.customizations.styling import DefaultMainStyling


class DiffMainStyling(DefaultMainStyling):
    """Override the 'main' display for diff sessions: hide *_pct_delta and *_abs_delta columns.

    Both derived-change columns are available in the 'detailed_pct' and
    'detailed_absolute' displays respectively. The default view shows only
    the _v2 column (renamed to the original column header, color-mapped by
    pct_delta) to keep the diff readable without redundant columns.
    """

    df_display_name = "main"

    @classmethod
    def style_columns(cls, sd, df):
        result = super().style_columns(sd, df)
        return [c for c in result if not c.get("col_name", "").endswith(("_pct_delta", "_abs_delta"))]
