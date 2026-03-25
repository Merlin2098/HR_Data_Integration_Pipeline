from __future__ import annotations

from pathlib import Path

import polars as pl

from src.modules.bd.steps.step3_flags_empleados import guardar_resultados as guardar_flags_bd
from src.modules.licencias.steps.step2_enriquecer_nomina import (
    guardar_resultados as guardar_nomina_enriquecida,
)
from src.utils.gold_export import maybe_write_excel


def test_maybe_write_excel_skips_file_creation_when_disabled(tmp_path: Path):
    ruta_excel = tmp_path / "salida.xlsx"
    writer_called = False

    def writer(path: Path) -> None:
        nonlocal writer_called
        writer_called = True
        path.write_text("excel", encoding="utf-8")

    result = maybe_write_excel(ruta_excel, False, writer)

    assert result is None
    assert not writer_called
    assert not ruta_excel.exists()


def test_maybe_write_excel_creates_file_when_enabled(tmp_path: Path):
    ruta_excel = tmp_path / "salida.xlsx"

    result = maybe_write_excel(
        ruta_excel,
        True,
        lambda path: path.write_text("excel", encoding="utf-8"),
    )

    assert result == ruta_excel
    assert ruta_excel.exists()


def test_bd_flags_gold_skips_excel_outputs_when_disabled(tmp_path: Path):
    carpeta_gold = tmp_path / "gold"
    carpeta_gold.mkdir()
    df = pl.DataFrame(
        {
            "NUMERO DE DOC": ["1"],
            "tiempo_servicio_texto": ["1 año"],
            "cumple_65_esteaño": [False],
        }
    )

    ruta_p_act, ruta_e_act, ruta_p_hist, ruta_e_hist = guardar_flags_bd(
        df,
        carpeta_gold,
        export_excel=False,
    )

    assert ruta_p_act.exists()
    assert ruta_p_hist.exists()
    assert ruta_e_act is None
    assert ruta_e_hist is None
    assert not (carpeta_gold / "bd_empleados_flags_gold.xlsx").exists()


def test_nomina_enriquecida_writes_excel_only_when_requested(tmp_path: Path):
    carpeta_actual = tmp_path / "gold" / "actual"
    carpeta_actual.mkdir(parents=True)
    ruta_nomina = carpeta_actual / "Planilla_Metso_Consolidado.parquet"
    df = pl.DataFrame({"NUMERO DE DOC": ["1"], "MOTIVO_CON_GOCE": ["Vacaciones"]})

    rutas_sin_excel = guardar_nomina_enriquecida(
        df,
        ruta_nomina,
        export_excel=False,
    )
    rutas_con_excel = guardar_nomina_enriquecida(
        df,
        ruta_nomina,
        export_excel=True,
    )

    assert rutas_sin_excel["parquet_actual"].exists()
    assert rutas_sin_excel["excel"] is None
    assert rutas_con_excel["excel"] is not None
    assert rutas_con_excel["excel"].exists()
