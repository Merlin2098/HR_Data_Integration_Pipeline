from __future__ import annotations

import polars as pl


MONTH_NAME_ENGLISH = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def add_month_name_column(
    df: pl.DataFrame,
    source_col: str = "MES",
    target_col: str = "NOMBRE_MES",
    *,
    insert_after: str | None = None,
    default_invalid: str | None = None,
) -> pl.DataFrame:
    """Adds a month-name column using explicit English month names."""
    anchor_col = insert_after or source_col

    if source_col not in df.columns:
        raise ValueError(f"Column '{source_col}' not found in DataFrame")
    if anchor_col not in df.columns:
        raise ValueError(f"Column '{anchor_col}' not found in DataFrame")

    df = df.with_columns(
        pl.col(source_col)
        .replace_strict(MONTH_NAME_ENGLISH, default=default_invalid)
        .alias(target_col)
    )

    ordered_columns = [column for column in df.columns if column != target_col]
    insert_position = ordered_columns.index(anchor_col) + 1
    ordered_columns.insert(insert_position, target_col)

    return df.select(ordered_columns)
