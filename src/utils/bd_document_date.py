from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


BD_DOCUMENT_DATE_COLUMN = "FECHA_DOCUMENTO"
DOCUMENT_DATE_FRAGMENT_REGEX = re.compile(r"\d{2}\.\d{2}\.\d{4}")
BD_FILENAME_REGEX = re.compile(
    r"(?i)^BD\.\s*(?P<fecha>\d{2}\.\d{2}\.\d{4})\.\.(xlsx|xlsm|xls)$"
)
CONTROL_PRACTICANTES_FILENAME_REGEX = re.compile(
    r"(?i)^BD Practicantes\s+(?P<fecha>\d{2}\.\d{2}\.\d{4})\.(xlsx|xlsm|xls)$"
)


def extract_document_date_from_filename(
    file_path: Path,
    filename_regex: re.Pattern[str],
    expected_pattern: str,
) -> str:
    """Extracts and normalizes FECHA_DOCUMENTO from a source filename."""
    source = Path(file_path)
    match = filename_regex.search(source.name)
    if not match:
        raise ValueError(
            "Filename does not match expected pattern "
            f"'{expected_pattern}'"
        )

    raw_date = match.group("fecha")

    try:
        parsed = datetime.strptime(raw_date, "%d.%m.%Y")
    except ValueError as exc:
        raise ValueError(f"Invalid document date in filename: {raw_date}") from exc

    return parsed.strftime("%Y-%m-%d")


def has_document_date_fragment(file_path: Path | str) -> bool:
    """Returns True when the filename contains a DD.MM.YYYY date fragment."""
    source_name = Path(file_path).name if not isinstance(file_path, Path) else file_path.name
    return bool(DOCUMENT_DATE_FRAGMENT_REGEX.search(source_name))


def extract_bd_document_date(file_path: Path) -> str:
    """Extracts FECHA_DOCUMENTO from a BD source filename and normalizes it."""
    return extract_document_date_from_filename(
        file_path=file_path,
        filename_regex=BD_FILENAME_REGEX,
        expected_pattern="BD. DD.MM.YYYY..xlsx|xlsm|xls",
    )


def extract_control_practicantes_document_date(file_path: Path) -> str:
    """Extracts FECHA_DOCUMENTO from a Control Practicantes source filename."""
    return extract_document_date_from_filename(
        file_path=file_path,
        filename_regex=CONTROL_PRACTICANTES_FILENAME_REGEX,
        expected_pattern="BD Practicantes DD.MM.YYYY.xlsx|xlsm|xls",
    )


def append_document_date_column(
    df: "pl.DataFrame",
    document_date: str,
    column_name: str = BD_DOCUMENT_DATE_COLUMN,
) -> "pl.DataFrame":
    """Appends FECHA_DOCUMENTO as a constant metadata column for the whole file."""
    import polars as pl

    return df.with_columns(pl.lit(document_date).alias(column_name))


def append_bd_document_date_column(
    df: "pl.DataFrame",
    file_path: Path,
    column_name: str = BD_DOCUMENT_DATE_COLUMN,
) -> "pl.DataFrame":
    """Backwards-compatible BD wrapper for appending FECHA_DOCUMENTO."""
    document_date = extract_bd_document_date(file_path)
    return append_document_date_column(df, document_date, column_name)


def append_control_practicantes_document_date_column(
    df: "pl.DataFrame",
    file_path: Path,
    column_name: str = BD_DOCUMENT_DATE_COLUMN,
) -> "pl.DataFrame":
    """Appends FECHA_DOCUMENTO for Control Practicantes source files."""
    document_date = extract_control_practicantes_document_date(file_path)
    return append_document_date_column(df, document_date, column_name)
