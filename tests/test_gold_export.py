from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.modules.bd.steps.step3_flags_empleados import (
    guardar_resultados as guardar_flags_bd,
)
from src.modules.examen_retiro.steps.step3_join import ejecutar_join_sql
from src.modules.licencias.steps.step2_enriquecer_nomina import (
    guardar_resultados as guardar_nomina_enriquecida,
)
from src.modules.examen_retiro.ui.worker import ExamenRetiroWorker
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
    assert rutas_sin_excel["parquet_actual"].name == "Planilla_Metso_Consolidado.parquet"
    assert rutas_sin_excel["excel"] is None
    assert rutas_con_excel["excel"] is not None
    assert rutas_con_excel["excel"].exists()
    assert rutas_con_excel["excel"].name == "Planilla_Metso_Consolidado.xlsx"


def test_examen_retiro_join_sql_returns_polars_without_pandas_dependency():
    df_examenes = pl.DataFrame(
        {
            "NOMBRE": ["Ana"],
            "DNI": ["1"],
            "FECHA DE CESE": ["2026-01-01"],
            "CAUSA DE SALIDA": ["Renuncia"],
            "CARGO": ["Analista"],
            "NOMBRE DE CC": ["CC01"],
            "AÑO": [2026],
            "MES": [1],
            "NOMBRE_MES": ["January"],
        }
    )
    df_cc_actual = pl.DataFrame(
        {
            "CC": ["CC01"],
            "NOMBRE CC": ["Centro 1"],
            "CATEGORIA CC": ["SGA"],
            "GERENCIA": ["HR"],
        }
    )
    df_cc_old = pl.DataFrame(
        {
            "CC": ["CC99"],
            "NOMBRE CC": ["Centro viejo"],
            "CATEGORIA CC": ["OLD"],
            "GERENCIA": ["Legacy"],
        }
    )
    query = """
    SELECT
        e.NOMBRE,
        e.DNI,
        e."FECHA DE CESE",
        e."CAUSA DE SALIDA",
        e.CARGO,
        e."NOMBRE DE CC" AS codigo_cc_original,
        e.AÑO,
        e.MES,
        e.NOMBRE_MES,
        cc.CC AS codigo_cc,
        cc."NOMBRE CC" AS nombre_cc_completo,
        cc."CATEGORIA CC" AS categoria_cc,
        cc.GERENCIA AS gerencia,
        CASE
            WHEN cc.CC IS NULL THEN 'CODIGO_NO_ENCONTRADO'
            ELSE 'OK'
        END AS status_match
    FROM examenes e
    LEFT JOIN cc_actual cc
        ON e."NOMBRE DE CC" = cc.CC
    """

    result = ejecutar_join_sql(df_examenes, df_cc_actual, df_cc_old, query)

    assert isinstance(result, pl.DataFrame)
    assert result["status_match"].to_list() == ["OK"]
    assert result["nombre_cc_completo"].to_list() == ["Centro 1"]


def test_examen_retiro_worker_fails_when_step3_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    class DummyPreflight:
        def raise_if_failed(self) -> None:
            return None

    class Step1:
        @staticmethod
        def extraer_bronze_examenes_retiro(_path: Path) -> pl.DataFrame:
            return pl.DataFrame({"dummy": [1]})

        @staticmethod
        def limpiar_silver_examenes_retiro(df: pl.DataFrame) -> pl.DataFrame:
            return df

        @staticmethod
        def guardar_resultados(df: pl.DataFrame, output_dir: Path) -> Path:
            carpeta = output_dir / "silver"
            carpeta.mkdir(parents=True, exist_ok=True)
            path = carpeta / "examenes_retiro.parquet"
            df.write_parquet(path)
            return path

    class Step2:
        @staticmethod
        def transformar_silver_a_gold(_df: pl.DataFrame, _esquema: dict) -> pl.DataFrame:
            return pl.DataFrame(
                {
                    "NOMBRE": ["Ana"],
                    "DNI": ["1"],
                    "FECHA DE CESE": ["2026-01-01"],
                    "CAUSA DE SALIDA": ["Renuncia"],
                    "CARGO": ["Analista"],
                    "NOMBRE DE CC": ["CC01"],
                    "AÑO": [2026],
                    "MES": [1],
                    "NOMBRE_MES": ["January"],
                }
            )

        @staticmethod
        def guardar_resultados(df: pl.DataFrame, carpeta_silver: Path, export_excel: bool = False):
            carpeta_gold = carpeta_silver.parent / "gold"
            carpeta_hist = carpeta_gold / "historico"
            carpeta_hist.mkdir(parents=True, exist_ok=True)
            path = carpeta_gold / "examenes_retiro_gold.parquet"
            df.write_parquet(path)
            return path, None, carpeta_hist / "hist.parquet", None

    class Step3:
        @staticmethod
        def cargar_parquets(_gold: Path, _cc_actual: Path, _cc_old: Path):
            return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

        @staticmethod
        def ejecutar_join_sql(_a, _b, _c, _query: str):
            raise RuntimeError("fallo join")

        @staticmethod
        def analizar_resultados(_df: pl.DataFrame):
            return {}

        @staticmethod
        def guardar_resultados(_df: pl.DataFrame, _ruta_gold: Path, _stats: dict, export_excel: bool = False):
            raise AssertionError("no debería guardar si falla el join")

    worker = ExamenRetiroWorker(
        archivo_bronze=tmp_path / "bronze.xlsm",
        archivo_cc_actual=tmp_path / "CC_ACTUAL.parquet",
        archivo_cc_old=tmp_path / "CC_OLD.parquet",
        output_dir=tmp_path,
        export_excel_gold=False,
    )
    worker.loader.step1 = Step1()
    worker.loader.step2 = Step2()
    worker.loader.step3 = Step3()

    monkeypatch.setattr(
        "src.modules.examen_retiro.ui.worker.validate_all_sources_for_etl",
        lambda *_args, **_kwargs: DummyPreflight(),
    )
    monkeypatch.setattr(
        "src.modules.examen_retiro.ui.worker.resolve_structured_path",
        lambda _path: tmp_path / "esquema_examen_retiro.yaml",
    )
    monkeypatch.setattr(
        "src.modules.examen_retiro.ui.worker.load_structured_data",
        lambda _path, prefer_resource_path=False: {"metadata": {"version": "test"}, "schema": {}},
    )
    (tmp_path / "esquema_examen_retiro.yaml").write_text("schema: {}", encoding="utf-8")

    result = worker.execute_etl()

    assert result["success"] is False
    assert result["error"] == "No se pudo generar examenes_retiro_gold_enriquecido.parquet"
    assert result["error_details"]["stage_name"] == "Step 3: Gold → Gold Enriquecido"
