class DiffDetailedPctStyling(DefaultMainStyling):
    """'detailed_pct' display for diff sessions: show *_pct_delta columns.

    Inherits DefaultMainStyling without filtering — all columns including
    pct_delta are visible so the user can inspect raw percentage changes.
    """

    df_display_name = "detailed_pct"
