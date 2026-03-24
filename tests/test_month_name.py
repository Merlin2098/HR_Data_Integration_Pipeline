from __future__ import annotations

from datetime import date

import polars as pl

from src.modules.examen_retiro.steps.step2_gold import transformar_silver_a_gold
from src.modules.pdt.steps.step3_exportar_practicantes import agregar_columna_enriquecida
from src.utils.month_name import MONTH_NAME_ENGLISH, add_month_name_column


def test_add_month_name_column_maps_all_months_in_english_title_case():
    df = pl.DataFrame({"MES": list(range(1, 13))})

    result = add_month_name_column(df)

    assert result["NOMBRE_MES"].to_list() == list(MONTH_NAME_ENGLISH.values())


def test_add_month_name_column_keeps_target_after_mes_and_preserves_none_fallback():
    df = pl.DataFrame(
        {
            "ID": [1, 2, 3],
            "MES": [1, None, 13],
            "OTRA": ["a", "b", "c"],
        }
    )

    result = add_month_name_column(df, default_invalid=None)

    assert result.columns == ["ID", "MES", "NOMBRE_MES", "OTRA"]
    assert result["NOMBRE_MES"].to_list() == ["January", None, None]


def test_pdt_enrichment_generates_english_month_names_with_blank_fallback():
    df = pl.DataFrame(
        {
            "PERIODO": ["2026-01", "2026-02", "2026-13"],
            "MES": [1, None, 13],
            "OTRA": ["ok", "null", "invalid"],
        }
    )

    result = agregar_columna_enriquecida(df)

    assert result.columns == ["PERIODO", "MES", "NOMBRE_MES", "OTRA"]
    assert result["NOMBRE_MES"].to_list() == ["January", "", ""]


def test_examen_retiro_transform_generates_english_month_names_from_fecha_de_cese():
    df_silver = pl.DataFrame(
        {
            "NOMBRE": ["Ana", "Luis"],
            "DNI": ["1", "2"],
            "FECHA DE CESE": pl.Series(
                "FECHA DE CESE",
                [date(2026, 1, 15), date(2026, 12, 1)],
                dtype=pl.Date,
            ),
            "CAUSA DE SALIDA": ["Renuncia", "Fin contrato"],
            "CARGO": ["Analista", "Coordinador"],
            "NOMBRE DE CC": ["CC01", "CC02"],
        }
    )
    esquema = {
        "schema": {
            "NOMBRE": {"type": "string"},
            "DNI": {"type": "string"},
            "FECHA DE CESE": {"type": "date"},
            "CAUSA DE SALIDA": {"type": "string"},
            "CARGO": {"type": "string"},
            "NOMBRE DE CC": {"type": "string"},
            "AÑO": {"type": "integer", "derivado_de": "FECHA DE CESE"},
            "MES": {"type": "integer", "derivado_de": "FECHA DE CESE"},
            "NOMBRE_MES": {"type": "string", "derivado_de": "FECHA DE CESE"},
        }
    }

    result = transformar_silver_a_gold(df_silver, esquema)

    assert result["MES"].to_list() == [1, 12]
    assert result["NOMBRE_MES"].to_list() == ["January", "December"]
