# ui/etls/control_practicantes/worker.py
"""
Worker para ETL de Control de Practicantes
Ejecuta pipeline completo: Bronze → Silver → Gold con Flags
Llama a pipeline_control_practicantes_executor.py para orquestar los 2 stages
"""
from pathlib import Path
from typing import Dict
import sys
import time

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root))

from src.utils.ui.workers.base_worker import BaseETLWorker
from src.orchestrators.pipeline_control_practicantes_executor import PipelineControlPracticantesExecutor
from src.utils.paths import get_resource_path
from src.utils.validate_source import SourceValidationError, validate_all_sources_for_etl


class ControlPracticantesWorker(BaseETLWorker):
    """Worker para procesamiento de control de practicantes con pipeline completo"""
    
    def __init__(self, archivos, output_dir, export_excel_gold: bool = False):
        super().__init__(archivos, output_dir, export_excel_gold=export_excel_gold)
        self.pipeline_executor = None
        
        # Timers
        self.timers = {
            'total': 0
        }
    
    def get_pipeline_name(self) -> str:
        return "control_practicantes_pipeline"
    
    def execute_etl(self) -> Dict:
        """
        Ejecuta el pipeline completo de control de practicantes
        usando el executor basado en YAML
        
        Returns:
            dict con resultados del proceso
        """
        tiempo_inicio_total = time.time()
        
        try:
            # Obtener ruta del YAML del pipeline
            yaml_path = get_resource_path("src/orchestrators/pipelines/pipeline_control_practicantes.yaml")
            
            if not yaml_path.exists():
                self.logger.error(f"❌ No se encontró el archivo YAML del pipeline: {yaml_path}")
                return {
                    'success': False,
                    'error': f'Archivo pipeline YAML no encontrado: {yaml_path}',
                    'timers': self.timers
                }
            
            # Obtener archivo único (no lista)
            if not self.archivos or len(self.archivos) == 0:
                self.logger.error("❌ No se proporcionó archivo de entrada")
                return {
                    'success': False,
                    'error': 'No se seleccionó archivo',
                    'timers': self.timers
                }
            
            archivo = self.archivos[0]  # Solo 1 archivo

            # Preflight / Validate Source (antes de ejecutar stages)
            self.progress_updated.emit(2, "🔎 Preflight: validando archivo fuente...")
            self.logger.info("🔎 PRE-FLIGHT: validando contrato de fuente...")
            preflight = validate_all_sources_for_etl("control_practicantes", archivo)
            preflight.raise_if_failed()
            self.logger.info(f"✓ Preflight válido ({archivo.name})")
            self.progress_updated.emit(4, "✓ Preflight completado")
            
            # Determinar carpeta de trabajo (carpeta del archivo)
            carpeta_trabajo = archivo.parent
            
            self.logger.info("=" * 70)
            self.logger.info("PIPELINE CONTROL DE PRACTICANTES")
            self.logger.info("=" * 70)
            self.logger.info(f"YAML: {yaml_path.name}")
            self.logger.info(f"Archivo: {archivo.name}")
            self.logger.info(f"Carpeta de trabajo: {carpeta_trabajo}")
            self.logger.info("=" * 70)
            
            # Crear executor del pipeline
            self.pipeline_executor = PipelineControlPracticantesExecutor(
                yaml_path=yaml_path,
                archivo=archivo,
                output_dir=carpeta_trabajo,
                export_excel_gold=self.export_excel_gold,
            )
            
            # Conectar señales del executor con las del worker
            self.pipeline_executor.log_message.connect(self._on_executor_log)
            self.pipeline_executor.progress_update.connect(self._on_executor_progress)
            self.pipeline_executor.stage_started.connect(self._on_stage_started)
            self.pipeline_executor.stage_completed.connect(self._on_stage_completed)
            
            # Ejecutar pipeline
            self.logger.info("")
            self.logger.info("🚀 Iniciando ejecución del pipeline...")
            self.logger.info("")
            
            resultado = self.pipeline_executor.execute()
            
            # Calcular tiempo total
            self.timers['total'] = time.time() - tiempo_inicio_total
            resultado['timers'] = self.timers
            
            if resultado['success']:
                # Log resumen final
                self.logger.info("")
                self.logger.info("=" * 70)
                self.logger.info("RESUMEN FINAL")
                self.logger.info("=" * 70)
                
                stages_completados = resultado.get('completed_stages', 0)
                duracion = resultado.get('duracion_total', self.timers['total'])
                
                # Obtener estadísticas de flags si están disponibles
                stage_results = resultado.get('stages_results', {})
                flags_info = ""
                
                for stage_name, stage_data in stage_results.items():
                    if 'flags' in stage_data:
                        flags = stage_data['flags']
                        flags_info = (
                            f"\n\n📊 Flags generados:\n"
                            f"  • Por cumplir 1 año: {flags.get('por_cumplir_1', 0)}\n"
                            f"  • Cumplió 1 año: {flags.get('cumplio_1', 0)}\n"
                            f"  • Por cumplir 2 años: {flags.get('por_cumplir_2', 0)}"
                        )
                
                mensaje = (
                    f"✓ Pipeline completado exitosamente\n"
                    f"  • Stages ejecutados: {stages_completados}/2\n"
                    f"  • Archivo procesado: {archivo.name}\n"
                    f"  ⏱️  Tiempo total: {self.logger.format_duration(duracion)}\n"
                    f"{flags_info}\n"
                    f"\n📂 Outputs generados en: {carpeta_trabajo.name}/\n"
                    f"  • silver/control_practicantes_silver.parquet\n"
                    f"  • gold/control_practicantes_flagsgold.parquet"
                )
                if self.export_excel_gold:
                    mensaje += "\n  • gold/control_practicantes_flagsgold.xlsx"
                
                resultado['mensaje'] = mensaje
                self.logger.info(mensaje)
                self.logger.info("=" * 70)
                
                self.progress_updated.emit(100, "✓ Pipeline completado")
            else:
                error_msg = resultado.get('error', 'Error desconocido')
                self.logger.error(f"❌ Pipeline falló: {error_msg}")
                
                self.progress_updated.emit(0, f"❌ Error: {error_msg}")
            
            return resultado

        except SourceValidationError as e:
            self.logger.error(str(e))

            self.timers['total'] = time.time() - tiempo_inicio_total
            return self.build_error_result(
                stage_name="Preflight / Validate Source",
                error=str(e),
                timers=self.timers,
                module_path="src.utils.validate_source",
                function_name="validate_all_sources_for_etl",
            )
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en pipeline: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            
            self.timers['total'] = time.time() - tiempo_inicio_total
            
            return {
                'success': False,
                'error': str(e),
                'timers': self.timers
            }
    
    def _on_executor_log(self, nivel: str, mensaje: str):
        """
        Callback cuando el executor emite un log
        Reenvía al logger del worker
        """
        log_method = getattr(self.logger, nivel.lower(), self.logger.info)
        log_method(mensaje)
    
    def _on_executor_progress(self, porcentaje: int, mensaje: str):
        """
        Callback cuando el executor emite progreso
        Reenvía como señal del worker
        """
        self.progress_updated.emit(porcentaje, mensaje)
    
    def _on_stage_started(self, stage_name: str, descripcion: str):
        """
        Callback cuando inicia un stage
        """
        self.logger.info("")
        self.logger.info(f"🚀 Iniciando: {stage_name}")
        if descripcion:
            self.logger.info(f"   {descripcion}")
    
    def _on_stage_completed(self, stage_name: str, exito: bool, duracion: float):
        """
        Callback cuando termina un stage
        """
        if exito:
            self.logger.info(f"✓ {stage_name} completado")
            self.logger.info(f"  ⏱️  Duración: {self.logger.format_duration(duracion)}")
        else:
            self.logger.error(f"❌ {stage_name} falló")
