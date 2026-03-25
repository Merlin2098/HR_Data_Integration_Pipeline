# ui/workers/base_worker.py
"""
Worker base abstracto para todos los ETLs

MEJORADO con:
- Timer automático de ejecución
- Tracking de duración por fase
- Mejor manejo de errores
- Resumen automático con estadísticas
"""
from PySide6.QtCore import QThread, Signal
from pathlib import Path
from typing import List, Dict, Optional, Any, Union
from abc import abstractmethod
import sys
import time
import traceback

# Agregar path del proyecto
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.utils.logger_qt import UILogger


class BaseETLWorker(QThread):
    """
    Worker base para ejecutar ETLs en background.
    Todos los workers heredan de esta clase.
    
    Características:
    - Timer automático de ejecución
    - Logger integrado con señales Qt
    - Manejo robusto de errores
    - Resumen automático al finalizar
    """
    
    # Señales comunes
    progress_updated = Signal(int, str)  # (porcentaje, mensaje)
    finished = Signal(bool, str, dict)   # (éxito, mensaje, resultados)
    
    def __init__(self, archivos: List[Path], output_dir: Path, export_excel_gold: bool = False):
        super().__init__()
        self.archivos = archivos
        self.output_dir = output_dir
        self.export_excel_gold = export_excel_gold
        
        # Logger con señales
        self.logger = UILogger(pipeline_name=self.get_pipeline_name())
        self.logger.progress_update.connect(self._emit_progress)
        
        # Timers
        self._start_time = None
        self._end_time = None
        self.phase_timers = {}  # {phase_name: duration}
        
        self.resultado = {}
    
    @abstractmethod
    def get_pipeline_name(self) -> str:
        """Nombre del pipeline para logger"""
        pass
    
    @abstractmethod
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo.
        Debe retornar dict con resultados incluyendo:
        {
            'success': bool,
            'error': str (opcional),
            'timers': dict (opcional),
            ...otros datos...
        }
        """
        pass
    
    def run(self):
        """Ejecuta el ETL (llamado por QThread.start())"""
        self._start_time = time.time()
        
        try:
            self.logger.info("=" * 70)
            self.logger.info(f"🚀 Iniciando ETL: {self.get_pipeline_name()}")
            self.logger.info("=" * 70)
            self.logger.info(f"📂 Archivos de entrada: {len(self.archivos)}")
            self.logger.info(f"📁 Directorio de salida: {self.output_dir}")
            self.logger.info("")
            
            # Ejecutar ETL
            self.resultado = self.execute_etl()
            
            self._end_time = time.time()
            total_duration = self._end_time - self._start_time
            
            # Agregar duración total si no está
            if 'timers' not in self.resultado:
                self.resultado['timers'] = {}
            if 'total' not in self.resultado['timers']:
                self.resultado['timers']['total'] = total_duration
            
            # Procesar resultado
            if self.resultado.get('success', False):
                self._log_success_summary()
                
                mensaje = self.resultado.get('mensaje', '✅ ETL completado exitosamente')
                
                self.finished.emit(True, mensaje, self.resultado)
            else:
                self._log_error_summary()

                mensaje = self._build_user_error_message(self.resultado)
                self.finished.emit(False, mensaje, self.resultado)
                
        except Exception as e:
            self._end_time = time.time()
            
            self.logger.log_error_details(e, "ejecución del ETL")
            
            total_duration = self._end_time - self._start_time
            
            self.finished.emit(
                False,
                f"❌ Error crítico: {str(e)}",
                {
                    'success': False, 
                    'error': str(e),
                    'timers': {'total': total_duration}
                }
            )

    def _build_user_error_message(self, resultado: Dict[str, Any]) -> str:
        """
        Construye un mensaje de error entendible para el usuario
        con contexto técnico mínimo (stage/módulo) cuando existe.
        """
        error = str(resultado.get('error', 'Error desconocido'))
        lines = [f"❌ Error en ETL: {error}"]

        details = resultado.get('error_details', {})
        if isinstance(details, dict) and details:
            stage_name = details.get('stage_name')
            stage_index = details.get('stage_index')
            total_stages = details.get('total_stages')
            module_path = details.get('module_path')
            function_name = details.get('function_name')
            detail_error = details.get('error_message')
            tb_excerpt = details.get('traceback_excerpt', [])

            if stage_name:
                if stage_index and total_stages:
                    lines.append(f"Etapa: {stage_index}/{total_stages} - {stage_name}")
                else:
                    lines.append(f"Etapa: {stage_name}")

            if module_path and function_name:
                lines.append(f"Módulo: {module_path}.{function_name}()")

            if detail_error and detail_error != error:
                lines.append(f"Detalle: {detail_error}")

            if isinstance(tb_excerpt, list) and tb_excerpt:
                lines.append(f"Traceback: {tb_excerpt[0]}")

        log_path = self.logger.get_log_path()
        if log_path:
            lines.append(f"Log: {log_path}")

        return "\n".join(lines)

    @staticmethod
    def build_error_details(
        stage_name: str,
        error: Union[Exception, str],
        stage_index: Optional[int] = None,
        total_stages: Optional[int] = None,
        module_path: Optional[str] = None,
        function_name: Optional[str] = None,
        traceback_text: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Construye payload estándar de error para UI/log.
        """
        if traceback_text is None:
            traceback_text = traceback.format_exc()
            if traceback_text.strip() == "NoneType: None":
                traceback_text = ""

        error_message = str(error)
        details: Dict[str, Any] = {
            'stage_name': stage_name,
            'error_message': error_message
        }

        if stage_index is not None:
            details['stage_index'] = stage_index
        if total_stages is not None:
            details['total_stages'] = total_stages
        if module_path:
            details['module_path'] = module_path
        if function_name:
            details['function_name'] = function_name
        if isinstance(error, Exception):
            details['exception_type'] = type(error).__name__

        tb_lines = [line for line in traceback_text.splitlines() if line.strip()]
        if tb_lines:
            details['traceback_excerpt'] = tb_lines[:8]

        return details

    def build_error_result(
        self,
        stage_name: str,
        error: Union[Exception, str],
        timers: Optional[Dict[str, Any]] = None,
        stage_index: Optional[int] = None,
        total_stages: Optional[int] = None,
        module_path: Optional[str] = None,
        function_name: Optional[str] = None,
        traceback_text: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Construye resultado de error estándar (success=False + error_details).
        """
        error_message = str(error)
        return {
            'success': False,
            'error': error_message,
            'error_details': self.build_error_details(
                stage_name=stage_name,
                error=error,
                stage_index=stage_index,
                total_stages=total_stages,
                module_path=module_path,
                function_name=function_name,
                traceback_text=traceback_text
            ),
            'timers': timers if timers is not None else self.resultado.get('timers', {})
        }
    
    def _log_success_summary(self):
        """Registra resumen de éxito con estadísticas"""
        timers = self.resultado.get('timers', {})
        total_duration = timers.get('total', 0)
        
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.info("✅ ETL COMPLETADO EXITOSAMENTE".center(70))
        self.logger.info("=" * 70)
        
        # Resumen de tiempos
        self.logger.info("")
        self.logger.info("⏱️  Tiempos de ejecución:")
        self.logger.info(f"  • Total: {self.logger.format_duration(total_duration)}")
        
        # Tiempos por fase
        for phase_name, duration in timers.items():
            if phase_name != 'total':
                self.logger.info(f"  • {phase_name}: {self.logger.format_duration(duration)}")
        
        # Estadísticas de datos (si existen)
        if 'step1' in self.resultado:
            step1 = self.resultado['step1']
            self.logger.info("")
            self.logger.info("📊 Datos procesados:")
            if 'registros' in step1:
                self.logger.info(f"  • Silver: {step1['registros']:,} registros")
            if 'columnas' in step1:
                self.logger.info(f"  • Columnas: {step1['columnas']}")
        
        if 'step2' in self.resultado and 'registros' in self.resultado['step2']:
            step2 = self.resultado['step2']
            self.logger.info(f"  • Gold: {step2['registros']:,} registros")
            if 'columnas' in step2:
                self.logger.info(f"  • Columnas finales: {step2['columnas']}")
        
        # Estadísticas del logger
        stats = self.logger.get_stats()
        if stats['warnings'] > 0 or stats['errors'] > 0:
            self.logger.info("")
            self.logger.info("📋 Resumen de logging:")
            if stats['warnings'] > 0:
                self.logger.warning(f"Warnings: {stats['warnings']}")
            if stats['errors'] > 0:
                self.logger.error(f"Errores: {stats['errors']}")
        
        if stats['validations_passed'] > 0 or stats['validations_failed'] > 0:
            total_validations = stats['validations_passed'] + stats['validations_failed']
            self.logger.info(f"  • Validaciones: {stats['validations_passed']}/{total_validations} exitosas")
        
        self.logger.info("")
        self.logger.info("=" * 70)
        
        # Ruta del log
        log_path = self.logger.get_log_path()
        if log_path:
            self.logger.info(f"📄 Log completo guardado en: {log_path}")
    
    def _log_error_summary(self):
        """Registra resumen de error"""
        timers = self.resultado.get('timers', {})
        total_duration = timers.get('total', 0)
        
        self.logger.info("")
        self.logger.info("=" * 70)
        self.logger.error("ETL FINALIZADO CON ERRORES".center(70))
        self.logger.info("=" * 70)
        
        error_msg = self.resultado.get('error', 'Error desconocido')
        self.logger.error(f"Causa: {error_msg}")

        error_details = self.resultado.get('error_details', {})
        if isinstance(error_details, dict) and error_details:
            stage_name = error_details.get('stage_name')
            stage_index = error_details.get('stage_index')
            total_stages = error_details.get('total_stages')
            module_path = error_details.get('module_path')
            function_name = error_details.get('function_name')
            detail_error = error_details.get('error_message')

            if stage_name:
                if stage_index and total_stages:
                    self.logger.error(f"Etapa fallida: {stage_index}/{total_stages} - {stage_name}")
                else:
                    self.logger.error(f"Etapa fallida: {stage_name}")

            if module_path and function_name:
                self.logger.error(f"Origen: {module_path}.{function_name}()")

            if detail_error and detail_error != error_msg:
                self.logger.error(f"Detalle técnico: {detail_error}")
        
        self.logger.info("")
        self.logger.info(f"⏱️  Tiempo transcurrido: {self.logger.format_duration(total_duration)}")
        
        # Estadísticas del logger
        stats = self.logger.get_stats()
        if stats['errors'] > 0:
            self.logger.info(f"📋 Total de errores registrados: {stats['errors']}")
        
        self.logger.info("")
        self.logger.info("=" * 70)
        
        # Ruta del log
        log_path = self.logger.get_log_path()
        if log_path:
            self.logger.info(f"📄 Log de errores guardado en: {log_path}")
    
    def _emit_progress(self, percentage: int, message: str):
        """Callback del logger para progreso"""
        self.progress_updated.emit(percentage, message)
    
    def start_phase(self, phase_name: str) -> float:
        """
        Inicia timer para una fase específica
        
        Args:
            phase_name: Nombre de la fase
            
        Returns:
            Timestamp de inicio (para usar en end_phase)
        """
        start_time = time.time()
        self.logger.log_step_start(phase_name)
        return start_time
    
    def end_phase(self, phase_name: str, start_time: float, success: bool = True):
        """
        Finaliza timer para una fase y registra duración
        
        Args:
            phase_name: Nombre de la fase
            start_time: Timestamp del start_phase
            success: Si la fase fue exitosa
        """
        duration = time.time() - start_time
        self.phase_timers[phase_name] = duration
        self.logger.log_step_end(phase_name, success, duration)
    
    def get_total_duration(self) -> Optional[float]:
        """Retorna duración total de ejecución (si ya finalizó)"""
        if self._start_time and self._end_time:
            return self._end_time - self._start_time
        elif self._start_time:
            return time.time() - self._start_time
        return None
    
    def cleanup(self):
        """Limpieza al finalizar (sobrescribir si es necesario)"""
        self.logger.close()


# ============================================================================
# CLASE AUXILIAR PARA TRACKING DE FASES
# ============================================================================

class PhaseTimer:
    """
    Context manager para timing automático de fases
    
    Uso:
        with PhaseTimer(worker, "Step 1: Consolidación"):
            # código de la fase
            pass
        # Automáticamente registra duración al salir
    """
    
    def __init__(self, worker: BaseETLWorker, phase_name: str):
        self.worker = worker
        self.phase_name = phase_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = self.worker.start_phase(self.phase_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        self.worker.end_phase(self.phase_name, self.start_time, success)
        return False  # No suprimir excepciones
