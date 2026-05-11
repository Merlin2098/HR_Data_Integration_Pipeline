"""
Ejecutor de Pipeline Régimen Minero con Licencias
Subclase de PipelineNominaExecutor que adapta los parámetros para régimen minero.

Diferencia clave respecto a nóminas: el archivo de licencias proviene de una
ruta externa (carpeta de nóminas), no de una subcarpeta local del output_dir.
"""

from pathlib import Path
from typing import Dict, Any, List

from src.orchestrators.pipeline_nomina_executor import PipelineNominaExecutor


class PipelineRegimenMineroExecutor(PipelineNominaExecutor):
    """
    Ejecutor del pipeline Régimen Minero + Licencias.
    Hereda toda la lógica de orquestación de PipelineNominaExecutor y
    sobreescribe únicamente la preparación de parámetros por stage y
    la validación de estructura.
    """

    def __init__(
        self,
        yaml_path: Path,
        archivos: List[Path],
        output_dir: Path,
        ruta_licencias: Path,
        export_excel_gold: bool = False,
    ):
        super().__init__(
            yaml_path=yaml_path,
            archivos=archivos,
            output_dir=output_dir,
            export_excel_gold=export_excel_gold,
        )
        self.ruta_licencias = ruta_licencias

    def validate_structure(self) -> bool:
        """
        Verifica que el archivo de licencias externo exista.
        No requiere subcarpeta local porque el archivo proviene de otra carpeta.
        """
        if not self.ruta_licencias.exists():
            self._log(
                "ERROR",
                f"No se encontró el archivo de licencias: {self.ruta_licencias}",
            )
            return False

        self._log("INFO", "✓ Estructura validada:")
        self._log("INFO", f"  • Archivo licencias: {self.ruta_licencias}")
        return True

    def _prepare_stage_params(
        self, stage_config: Dict, stage_index: int
    ) -> Dict[str, Any]:
        """
        Parámetros por stage para el pipeline de régimen minero:

        Stage 0 — Régimen Minero Bronze → Silver
            consolidar_archivos(archivos, carpeta_trabajo)

        Stage 1 — Licencias Bronze → Silver
            procesar_sin_gui(ruta_archivo, carpeta_salida)

        Stage 2 — Régimen Minero Silver → Gold
            exportar_a_gold(ruta_parquet_silver, carpeta_trabajo, export_excel_gold)

        Stage 3 — Enriquecimiento con Licencias
            procesar_sin_gui(ruta_nomina, ruta_licencias, export_excel_gold, nombre_output)
        """
        if stage_index == 0:
            return {
                "archivos": self.archivos,
                "carpeta_trabajo": self.output_dir,
            }

        elif stage_index == 1:
            return {
                "ruta_archivo": self.ruta_licencias,
                "carpeta_salida": self.output_dir / "silver",
            }

        elif stage_index == 2:
            ruta_parquet_silver = (
                self.output_dir
                / "silver"
                / "Planilla Metso Consolidado - Regimen Minero.parquet"
            )
            return {
                "ruta_parquet_silver": ruta_parquet_silver,
                "carpeta_trabajo": self.output_dir,
                "export_excel_gold": self.export_excel_gold,
            }

        elif stage_index == 3:
            ruta_nomina_gold = (
                self.output_dir
                / "gold"
                / "actual"
                / "Planilla Metso - Regimen Minero.parquet"
            )
            ruta_licencias_silver = (
                self.output_dir / "silver" / "licencias_consolidadas.parquet"
            )
            return {
                "ruta_nomina": ruta_nomina_gold,
                "ruta_licencias": ruta_licencias_silver,
                "export_excel_gold": self.export_excel_gold,
                "nombre_output": "Planillas Metso - Regimen Minero con licencias",
            }

        else:
            return {}
