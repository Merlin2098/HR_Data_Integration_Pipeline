from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


BD_DOCUMENT_DATE_COLUMN = "FECHA_DOCUMENTO"
BD_FILENAME_REGEX = re.compile(
    r"(?i)^BD\.\s*(?P<fecha>\d{2}\.\d{2}\.\d{4})\.\.(xlsx|xlsm|xls)$"
)


def extract_bd_document_date(file_path: Path) -> str:
    """Extracts FECHA_DOCUMENTO from a BD source filename and normalizes it."""
    source = Path(file_path)
    match = BD_FILENAME_REGEX.search(source.name)
    if not match:
        raise ValueError(
            "Filename does not match expected BD pattern "
            "'BD. DD.MM.YYYY..xlsx|xlsm|xls'"
        )

    raw_date = match.group("fecha")

    try:
        parsed = datetime.strptime(raw_date, "%d.%m.%Y")
    except ValueError as exc:
        raise ValueError(f"Invalid document date in filename: {raw_date}") from exc

    return parsed.strftime("%Y-%m-%d")


def append_bd_document_date_column(
    df: "pl.DataFrame",
    file_path: Path,
    column_name: str = BD_DOCUMENT_DATE_COLUMN,
) -> "pl.DataFrame":
    """Appends FECHA_DOCUMENTO as a constant metadata column for the whole file."""
    import polars as pl

    document_date = extract_bd_document_date(file_path)
    return df.with_columns(pl.lit(document_date).alias(column_name))
