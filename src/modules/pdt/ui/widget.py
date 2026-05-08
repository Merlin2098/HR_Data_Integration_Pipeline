# ui/etls/pdt/widget.py
"""
Widget específico para ETL de PDT - Relación de Ingresos
Selecciona UN ARCHIVO Excel (no carpeta) que contiene 2 hojas: EMPLEADOS y PRACTICANTES
"""

import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.utils.ui.widgets.base_etl_widget import BaseETLWidget
from .worker import PDTWorker
from src.utils.ui.file_selector_qt import quick_file_select_qt


class PDTWidget(BaseETLWidget):
    """Widget para procesamiento de Relación de Ingresos (PDT)"""

    def __init__(self):
        super().__init__(title="Procesamiento de Relación de Ingresos")

    def _get_worker_class(self):
        """Retorna la clase Worker de PDT"""
        return PDTWorker

    def _get_file_filter(self) -> str:
        """Filtro para archivo Excel de Relación de Ingresos"""
        return "Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)"

    def _get_select_button_text(self) -> str:
        """Texto personalizado para botón de selección"""
        return "📂 Seleccionar Archivo de Relación de Ingresos"

    def _get_no_files_message(self) -> str:
        """Mensaje cuando no hay archivo seleccionado"""
        return "No hay archivo seleccionado. Seleccione el archivo Excel de Relación de Ingresos (contiene hojas EMPLEADOS y PRACTICANTES)."

    def _select_files(self):
        """
        SOBRESCRITURA: Selecciona UN SOLO ARCHIVO (no múltiples)
        El archivo debe contener las hojas EMPLEADOS y PRACTICANTES
        """
        # Abrir selector de archivo (NO múltiple)
        files = quick_file_select_qt(
            parent=self,
            title="Seleccionar archivo de Relación de Ingresos",
            file_filter=self._get_file_filter(),
            multiple=False,  # CRÍTICO: Solo un archivo
            cache_key="pdt_archivo",
        )

        if not files:
            return

        # quick_file_select_qt devuelve lista, tomar primer elemento
        archivo = files[0] if isinstance(files, list) else files

        # Verificar que sea archivo Excel
        if archivo.suffix.lower() not in [".xlsx", ".xls"]:
            self._log("⚠️ El archivo debe ser Excel (.xlsx o .xls)")
            self.label_files.setText(
                f"⚠️ Archivo no válido: {archivo.name}\n"
                f"Debe ser un archivo Excel (.xlsx o .xls)"
            )
            return

        # Guardar como lista con un solo elemento (para compatibilidad con base_worker)
        self.archivos_seleccionados = [archivo]

        # Actualizar UI
        self.label_files.setText(
            f"✓ Archivo seleccionado:\n"
            f"  • {archivo.name}\n"
            f"\n💡 Se procesarán las hojas EMPLEADOS y PRACTICANTES"
        )

        self.btn_process.setEnabled(True)
        self.btn_clear.setEnabled(True)
        self._log(f"📂 Archivo seleccionado: {archivo.name}")
