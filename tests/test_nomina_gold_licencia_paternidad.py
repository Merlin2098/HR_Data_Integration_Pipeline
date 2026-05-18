from __future__ import annotations

import polars as pl

from src.modules.nomina.steps.step2_exportar import aplicar_transformaciones_gold


def test_gold_preserva_licencia_paternidad_cuando_esta_en_schema():
    df_silver = pl.DataFrame(
        {
            "PERIODO": ["2026-04"],
            "DNI/CEX": ["12345678"],
            "LICENCIA PATERNIDAD": [150.5],
        }
    )
    schema = {
        "schema": {
            "PERIODO": {"type": "string"},
            "DNI/CEX": {"type": "string"},
            "LICENCIA PATERNIDAD": {"type": "float"},
        }
    }

    result = aplicar_transformaciones_gold(df_silver, schema)

    assert "LICENCIA PATERNIDAD" in result.columns
    assert result["LICENCIA PATERNIDAD"].to_list() == [150.5]
