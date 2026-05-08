# ui/etls/nomina/widget.py
"""
Widget específico para ETL de Nómina con Licencias
Selecciona carpeta y valida estructura: planillas + /licencias/
"""

import sys
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from src.utils.ui.widgets.base_etl_widget import BaseETLWidget
from .worker import NominaWorker
from src.utils.ui.file_selector_qt import quick_dir_select_qt


class NominaWidget(BaseETLWidget):
    """Widget para procesamiento de nóminas con licencias"""

    def __init__(self):
        super().__init__(title="Procesamiento de Planillas con Licencias")

    def _get_worker_class(self):
        """Retorna la clase Worker de Nómina"""
        return NominaWorker

    def _get_file_filter(self) -> str:
        """Filtro para archivos Excel de planillas (no usado en este widget)"""
        return "Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)"

    def _get_select_button_text(self) -> str:
        """Texto personalizado para botón de selección"""
        return "📁 Seleccionar Carpeta con Planillas"

    def _get_no_files_message(self) -> str:
        """Mensaje cuando no hay carpeta seleccionada"""
        return "No hay carpeta seleccionada. Seleccione una carpeta con archivos de planilla."

    def _select_files(self):
        """
        SOBRESCRITURA: Selecciona CARPETA y valida estructura
        Busca archivos Excel en raíz + subcarpeta /licencias/
        """
        # Abrir selector de carpeta
        carpeta = quick_dir_select_qt(
            parent=self,
            title="Seleccionar carpeta con archivos de planilla y /licencias/",
            cache_key="nomina_carpeta",
        )

        if not carpeta:
            return

        # Buscar archivos Excel en la carpeta raíz
        archivos_excel = list(carpeta.glob("*.xlsx")) + list(carpeta.glob("*.xls"))

        # Filtrar archivos temporales y consolidados previos
        archivos_excel = [
            f
            for f in archivos_excel
            if not f.name.startswith("~$")
            and not f.name.startswith("Planilla Metso Consolidado")
            and not f.name.startswith("Planilla Metso BI_Gold")
        ]

        if not archivos_excel:
            self._log(f"⚠️  No se encontraron archivos Excel en: {carpeta.name}")
            self.label_files.setText(
                f"⚠️  Carpeta seleccionada: {carpeta.name}\n"
                f"No se encontraron archivos Excel (.xlsx, .xls)"
            )
            return

        # VALIDAR ESTRUCTURA: Debe existir subcarpeta /licencias/
        carpeta_licencias = carpeta / "licencias"
        archivo_licencias = carpeta_licencias / "CONTROL DE LICENCIAS.xlsx"

        estructura_valida = carpeta_licencias.exists() and archivo_licencias.exists()

        if not estructura_valida:
            self._log(f"❌ Estructura inválida en: {carpeta.name}")

            mensaje_error = f"⚠️  Carpeta: {carpeta.name}\n"
            mensaje_error += "❌ Estructura inválida. Se requiere:\n"
            mensaje_error += (
                f"  • Archivos Excel en raíz (✓ {len(archivos_excel)} encontrados)\n"
            )

            if not carpeta_licencias.exists():
                mensaje_error += "  • Subcarpeta /licencias/ (❌ no existe)\n"
            elif not archivo_licencias.exists():
                mensaje_error += "  • Subcarpeta /licencias/ (✓ existe)\n"
                mensaje_error += (
                    "  • Archivo CONTROL DE LICENCIAS.xlsx (❌ no existe)\n"
                )

            self.label_files.setText(mensaje_error)
            self.btn_process.setEnabled(False)
            return

        # Guardar archivos encontrados
        self.archivos_seleccionados = archivos_excel
        count = len(archivos_excel)

        # Actualizar UI con estructura validada
        self.label_files.setText(
            f"✓ Carpeta: {carpeta.name}\n"
            f"✓ {count} archivo{'s' if count > 1 else ''} de planilla encontrado{'s' if count > 1 else ''}:\n"
            + "\n".join([f"  • {f.name}" for f in archivos_excel[:5]])
            + (f"\n  ... y {count - 5} más" if count > 5 else "")
            + "\n\n✓ Subcarpeta /licencias/ encontrada\n"
            "✓ Archivo CONTROL DE LICENCIAS.xlsx encontrado\n"
            "\n📊 Pipeline completo: Nóminas + Licencias → Gold"
        )

        self.btn_process.setEnabled(True)
        self.btn_clear.setEnabled(True)

        self._log(f"📂 Carpeta seleccionada: {carpeta.name}")
        self._log(f"📄 Encontrados {count} archivo(s) Excel")
        self._log("✓ Estructura validada: /licencias/ presente")
