# ui/etls/nomina_regimen_minero/widget.py
"""
Widget específico para ETL de Nómina Régimen Minero
Selecciona una CARPETA y busca archivos Excel dentro de ella
"""

import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.utils.ui.widgets.base_etl_widget import BaseETLWidget
from .worker import NominaRegimenMineroWorker
from src.utils.ui.file_selector_qt import quick_dir_select_qt


class NominaRegimenMineroWidget(BaseETLWidget):
    """Widget para procesamiento de nóminas - Régimen Minero"""

    def __init__(self):
        super().__init__(title="Procesamiento de Planillas - Régimen Minero")

    def _get_worker_class(self):
        """Retorna la clase Worker de Nómina Régimen Minero"""
        return NominaRegimenMineroWorker

    def _get_file_filter(self) -> str:
        """Filtro para archivos Excel de planillas (no usado en este widget)"""
        return "Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)"

    def _get_select_button_text(self) -> str:
        """Texto personalizado para botón de selección"""
        return "📂 Seleccionar Carpeta con Planillas (Régimen Minero)"

    def _get_no_files_message(self) -> str:
        """Mensaje cuando no hay carpeta seleccionada"""
        return "No hay carpeta seleccionada. Seleccione una carpeta con archivos de planilla - Régimen Minero."

    def _select_files(self):
        """
        SOBRESCRITURA: Selecciona CARPETA en lugar de archivos
        Busca archivos Excel dentro de la carpeta seleccionada
        """
        # Abrir selector de carpeta
        carpeta = quick_dir_select_qt(
            parent=self,
            title="Seleccionar carpeta con archivos de planilla - Régimen Minero",
            cache_key="nomina_regimen_minero_carpeta",
        )

        if not carpeta:
            return

        # Buscar archivos Excel en la carpeta
        archivos_excel = list(carpeta.glob("*.xlsx")) + list(carpeta.glob("*.xls"))

        # Filtrar archivos temporales y consolidados previos
        archivos_excel = [
            f
            for f in archivos_excel
            if not f.name.startswith("~$")
            and not f.name.startswith("Planilla Metso Consolidado")
        ]

        if not archivos_excel:
            self._log(f"⚠️ No se encontraron archivos Excel en: {carpeta.name}")
            self.label_files.setText(
                f"⚠️ Carpeta seleccionada: {carpeta.name}\n"
                f"No se encontraron archivos Excel (.xlsx, .xls)"
            )
            return

        # Guardar archivos encontrados
        self.archivos_seleccionados = archivos_excel
        count = len(archivos_excel)

        # Actualizar UI
        self.label_files.setText(
            f"✓ Carpeta: {carpeta.name}\n"
            f"✓ {count} archivo{'s' if count > 1 else ''} encontrado{'s' if count > 1 else ''}:\n"
            + "\n".join([f"  • {f.name}" for f in archivos_excel[:5]])
            + (f"\n  ... y {count - 5} más" if count > 5 else "")
        )

        self.btn_process.setEnabled(True)
        self.btn_clear.setEnabled(True)
        self._log(f"📂 Carpeta seleccionada: {carpeta.name}")
        self._log(f"📄 Encontrados {count} archivo(s) Excel")
