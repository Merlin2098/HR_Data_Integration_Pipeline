from __future__ import annotations

import polars as pl

from src.modules.nomina.steps.step1_consolidar_planillas import (
    SUBSIDIO_MATERNIDAD_COL,
    asegurar_columna_subsidio_maternidad,
)


def test_asegura_subsidio_maternidad_when_header_is_missing():
    df = pl.DataFrame({"DNI/CEX": ["1", "2"]})

    result = asegurar_columna_subsidio_maternidad(df)

    assert SUBSIDIO_MATERNIDAD_COL in result.columns
    assert result[SUBSIDIO_MATERNIDAD_COL].to_list() == [None, None]


def test_asegura_subsidio_maternidad_matches_header_case_insensitive():
    df = pl.DataFrame({"subsidio maternidad": [10.5, None]})

    result = asegurar_columna_subsidio_maternidad(df)

    assert "subsidio maternidad" not in result.columns
    assert result[SUBSIDIO_MATERNIDAD_COL].to_list() == [10.5, None]
