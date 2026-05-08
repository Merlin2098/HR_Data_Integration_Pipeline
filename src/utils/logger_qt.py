# utils/logger_qt.py
"""
Logger adaptado para interfaces PySide6
Emite señales para actualizar QTextEdit en tiempo real

MEJORADO con:
- Formateo de tiempos (hh:mm:ss.ms)
- Separación de errores vs warnings
- Contadores de errores/warnings
- Registro de validaciones
"""

from pathlib import Path
from typing import Optional, Dict
from PySide6.QtCore import QObject, Signal
import logging


class UILogger(QObject):
    """Logger que emite señales Qt para integración con QTextEdit"""

    log_message = Signal(str, str)  # (nivel, mensaje)
    step_started = Signal(str, str)  # (nombre_paso, descripción)
    step_completed = Signal(str, bool)  # (nombre_paso, éxito)
    progress_update = Signal(int, str)  # (porcentaje, mensaje)
    validation_result = Signal(str, bool, str)  # (tipo_validacion, éxito, mensaje)

    def __init__(
        self, pipeline_name: str, log_to_file: bool = True, log_dir: Path = Path("logs")
    ):
        """
        Inicializa logger para UI

        Args:
            pipeline_name: Nombre del pipeline
            log_to_file: También escribir a archivo
            log_dir: Directorio de logs
        """
        super().__init__()
        self.pipeline_name = pipeline_name
        self.log_to_file = log_to_file

        # Contadores para resumen
        self.stats = {
            "errors": 0,
            "warnings": 0,
            "validations_passed": 0,
            "validations_failed": 0,
        }

        if log_to_file:
            self.log_dir = Path(log_dir)
            self.log_dir.mkdir(exist_ok=True, parents=True)

            # Nombre simple sin timestamp (se sobreescribe en cada ejecución)
            self.log_file = self.log_dir / f"{pipeline_name}.log"
            self._setup_file_logger()
        else:
            self.file_logger = None

    def _setup_file_logger(self):
        """Configura logger de archivo"""
        self.file_logger = logging.getLogger(f"ui_pipeline.{self.pipeline_name}")
        self.file_logger.setLevel(logging.DEBUG)

        if self.file_logger.handlers:
            self.file_logger.handlers.clear()

        handler = logging.FileHandler(self.log_file, encoding="utf-8", mode="w")
        handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        self.file_logger.addHandler(handler)

    def _emit_and_log(self, level: str, message: str):
        """Emite señal y escribe a archivo"""
        self.log_message.emit(level, message)

        # Actualizar contadores
        if level == "ERROR":
            self.stats["errors"] += 1
        elif level == "WARNING":
            self.stats["warnings"] += 1

        if self.file_logger:
            log_method = getattr(self.file_logger, level.lower(), self.file_logger.info)
            log_method(message)

    def debug(self, message: str):
        """Log nivel DEBUG"""
        self._emit_and_log("DEBUG", message)

    def info(self, message: str):
        """Log nivel INFO"""
        self._emit_and_log("INFO", message)

    def warning(self, message: str):
        """Log nivel WARNING"""
        self._emit_and_log("WARNING", f"⚠️  {message}")

    def error(self, message: str):
        """Log nivel ERROR"""
        self._emit_and_log("ERROR", f"❌ {message}")

    def critical(self, message: str):
        """Log nivel CRITICAL"""
        self._emit_and_log("CRITICAL", f"🔥 {message}")

    def success(self, message: str):
        """Log de éxito"""
        self._emit_and_log("INFO", f"✓ {message}")

    def log_step_start(self, step_name: str, description: str = ""):
        """Registra inicio de paso"""
        self.step_started.emit(step_name, description)
        separator = "=" * 70
        self._emit_and_log("INFO", "")
        self._emit_and_log("INFO", separator)
        self._emit_and_log("INFO", f"INICIANDO: {step_name}")
        if description:
            self._emit_and_log("INFO", f"Descripción: {description}")
        self._emit_and_log("INFO", separator)

    def log_step_end(
        self, step_name: str, success: bool = True, duration: Optional[float] = None
    ):
        """
        Registra fin de paso

        Args:
            step_name: Nombre del paso
            success: Si fue exitoso
            duration: Duración en segundos (opcional)
        """
        self.step_completed.emit(step_name, success)

        status = "✓ COMPLETADO" if success else "✗ FALLIDO"
        separator = "-" * 70

        self._emit_and_log("INFO", separator)

        if duration is not None:
            time_str = self.format_duration(duration)
            self._emit_and_log("INFO", f"{status}: {step_name} (⏱️  {time_str})")
        else:
            self._emit_and_log("INFO", f"{status}: {step_name}")

        self._emit_and_log("INFO", separator)
        self._emit_and_log("INFO", "")

    def log_progress(self, percentage: int, message: str):
        """Registra progreso"""
        self.progress_update.emit(percentage, message)
        self._emit_and_log("INFO", f"[{percentage}%] {message}")

    def log_file_processing(self, file_path: Path, operation: str = "Procesando"):
        """Registra procesamiento de archivo"""
        self._emit_and_log("INFO", f"{operation}: {file_path.name}")

    def log_dataframe_info(self, name: str, rows: int, cols: int):
        """Registra info de DataFrame"""
        self._emit_and_log(
            "INFO", f"DataFrame '{name}': {rows:,} filas × {cols} columnas"
        )

    def log_validation(self, validation_type: str, passed: bool, message: str):
        """
        Registra resultado de validación

        Args:
            validation_type: Tipo de validación (ej: 'primary_key', 'nulls', 'ranges')
            passed: Si pasó la validación
            message: Mensaje descriptivo
        """
        self.validation_result.emit(validation_type, passed, message)

        if passed:
            self.stats["validations_passed"] += 1
            self._emit_and_log("INFO", f"✓ {message}")
        else:
            self.stats["validations_failed"] += 1
            self._emit_and_log("WARNING", message)

    def log_performance(self, operation: str, duration_seconds: float):
        """Registra métricas de rendimiento"""
        time_str = self.format_duration(duration_seconds)
        self._emit_and_log("INFO", f"⏱️  {operation}: {time_str}")

    @staticmethod
    def format_duration(seconds: float) -> str:
        """
        Formatea duración en palabras legibles en español

        Args:
            seconds: Duración en segundos

        Returns:
            String formateado en palabras (ej: "2 minutos, 15 segundos y 430 ms")
        """
        if seconds < 0.001:
            microseconds = seconds * 1000000
            return f"{microseconds:.0f} microsegundos"

        elif seconds < 1:
            milliseconds = seconds * 1000
            return f"{milliseconds:.0f} ms"

        elif seconds < 60:
            # Solo segundos y milisegundos
            secs = int(seconds)
            ms = int((seconds - secs) * 1000)

            if ms > 0:
                return f"{secs} segundo{'s' if secs != 1 else ''} y {ms} ms"
            else:
                return f"{secs} segundo{'s' if secs != 1 else ''}"

        elif seconds < 3600:
            # Minutos, segundos y milisegundos
            minutes = int(seconds // 60)
            remaining = seconds % 60
            secs = int(remaining)
            ms = int((remaining - secs) * 1000)

            parts = [f"{minutes} minuto{'s' if minutes != 1 else ''}"]

            if secs > 0:
                parts.append(f"{secs} segundo{'s' if secs != 1 else ''}")

            if ms > 0:
                parts.append(f"{ms} ms")

            # Unir con comas y "y" antes del último elemento
            if len(parts) == 1:
                return parts[0]
            elif len(parts) == 2:
                return f"{parts[0]} y {parts[1]}"
            else:
                return f"{parts[0]}, {parts[1]} y {parts[2]}"

        else:
            # Horas, minutos, segundos y milisegundos
            hours = int(seconds // 3600)
            remaining = seconds % 3600
            minutes = int(remaining // 60)
            remaining = remaining % 60
            secs = int(remaining)
            ms = int((remaining - secs) * 1000)

            parts = [f"{hours} hora{'s' if hours != 1 else ''}"]

            if minutes > 0:
                parts.append(f"{minutes} minuto{'s' if minutes != 1 else ''}")

            if secs > 0:
                parts.append(f"{secs} segundo{'s' if secs != 1 else ''}")

            if ms > 0:
                parts.append(f"{ms} ms")

            # Unir con comas y "y" antes del último elemento
            if len(parts) == 1:
                return parts[0]
            elif len(parts) == 2:
                return f"{parts[0]} y {parts[1]}"
            else:
                # Todas las partes excepto la última con coma, última con "y"
                return ", ".join(parts[:-1]) + f" y {parts[-1]}"

    def log_summary(self, title: str, data: Dict[str, any]):
        """
        Registra resumen estructurado

        Args:
            title: Título del resumen
            data: Diccionario con datos a mostrar
        """
        separator = "=" * 70
        self._emit_and_log("INFO", "")
        self._emit_and_log("INFO", separator)
        self._emit_and_log("INFO", title.center(70))
        self._emit_and_log("INFO", separator)

        for key, value in data.items():
            if isinstance(value, (int, float)):
                if isinstance(value, int):
                    self._emit_and_log("INFO", f"  • {key}: {value:,}")
                else:
                    self._emit_and_log("INFO", f"  • {key}: {value:.2f}")
            else:
                self._emit_and_log("INFO", f"  • {key}: {value}")

        self._emit_and_log("INFO", separator)

    def log_error_details(self, error: Exception, context: str = ""):
        """
        Registra error con detalles completos

        Args:
            error: Excepción capturada
            context: Contexto donde ocurrió el error
        """
        import traceback

        if context:
            self.error(f"Error en {context}: {str(error)}")
        else:
            self.error(f"Error: {str(error)}")

        # Traceback completo al archivo, resumido en UI
        if self.file_logger:
            self.file_logger.error("Traceback completo:")
            self.file_logger.error(traceback.format_exc())

        # Solo primeras líneas en UI
        tb_lines = traceback.format_exc().split("\n")
        for line in tb_lines[:5]:  # Primeras 5 líneas
            if line.strip():
                self._emit_and_log("DEBUG", f"  {line}")

    def get_stats(self) -> Dict[str, int]:
        """Retorna estadísticas del logging"""
        return self.stats.copy()

    def reset_stats(self):
        """Reinicia contadores de estadísticas"""
        self.stats = {
            "errors": 0,
            "warnings": 0,
            "validations_passed": 0,
            "validations_failed": 0,
        }

    def get_log_path(self) -> Optional[Path]:
        """Retorna ruta del log de archivo"""
        return self.log_file if self.log_to_file else None

    def close(self):
        """Cierra handlers del logger"""
        if self.file_logger:
            for handler in self.file_logger.handlers:
                handler.close()


def get_ui_logger(
    pipeline_name: str, log_to_file: bool = True, log_dir: Path = Path("logs")
) -> UILogger:
    """Factory para obtener UILogger configurado"""
    return UILogger(
        pipeline_name=pipeline_name, log_to_file=log_to_file, log_dir=log_dir
    )
