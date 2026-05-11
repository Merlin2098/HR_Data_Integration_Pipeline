# ui/etls/nomina_regimen_minero/widget.py
"""
Widget específico para ETL de Nómina Régimen Minero con Licencias
Selecciona una CARPETA con planillas y un ARCHIVO de licencias por separado.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from PySide6.QtWidgets import QGroupBox, QVBoxLayout, QHBoxLayout, QPushButton, QLabel

from src.utils.ui.widgets.base_etl_widget import BaseETLWidget
from .worker import NominaRegimenMineroWorker
from src.utils.ui.file_selector_qt import quick_dir_select_qt, quick_file_select_qt


class NominaRegimenMineroWidget(BaseETLWidget):
    """Widget para procesamiento de nóminas - Régimen Minero con licencias"""

    def __init__(self):
        self.ruta_licencias: Path | None = None
        super().__init__(title="Procesamiento de Planillas - Régimen Minero")

    def _get_worker_class(self):
        return NominaRegimenMineroWorker

    def _get_file_filter(self) -> str:
        return "Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)"

    def _get_select_button_text(self) -> str:
        return "📂 Seleccionar Carpeta con Planillas (Régimen Minero)"

    def _get_no_files_message(self) -> str:
        return "No hay carpeta seleccionada. Seleccione una carpeta con archivos de planilla - Régimen Minero."

    def _setup_ui(self):
        """Extiende la UI base agregando la sección de selección de licencias."""
        super()._setup_ui()

        # Insertar sección de licencias después del grupo de archivos (índice 1)
        main_layout = self.layout()

        group_licencias = QGroupBox("2. Archivo de Licencias")
        layout_licencias = QVBoxLayout()

        self.label_licencias = QLabel(
            "No hay archivo de licencias seleccionado.\n"
            "Seleccione el archivo 'CONTROL DE LICENCIAS.xlsx'."
        )
        self.label_licencias.setProperty("labelStyle", "secondary")
        self.label_licencias.setWordWrap(True)

        btn_licencias_layout = QHBoxLayout()
        self.btn_select_licencias = QPushButton("📋 Seleccionar Archivo de Licencias")
        self.btn_select_licencias.clicked.connect(self._select_licencias)
        self.btn_clear_licencias = QPushButton("🗑️ Limpiar")
        self.btn_clear_licencias.clicked.connect(self._clear_licencias)
        self.btn_clear_licencias.setEnabled(False)

        btn_licencias_layout.addWidget(self.btn_select_licencias)
        btn_licencias_layout.addWidget(self.btn_clear_licencias)

        layout_licencias.addWidget(self.label_licencias)
        layout_licencias.addLayout(btn_licencias_layout)
        group_licencias.setLayout(layout_licencias)

        # Insertar en índice 1 (después del grupo de planillas en índice 0)
        main_layout.insertWidget(1, group_licencias)

        # Renombrar la sección de procesamiento para que quede "3."
        # (el QGroupBox ya fue creado por _setup_ui del padre como "2. Procesamiento")
        for i in range(main_layout.count()):
            item = main_layout.itemAt(i)
            if item and item.widget():
                w = item.widget()
                if isinstance(w, QGroupBox) and w.title() == "2. Procesamiento":
                    w.setTitle("3. Procesamiento")
                elif isinstance(w, QGroupBox) and w.title() == "3. Log de Actividad":
                    w.setTitle("4. Log de Actividad")

    def _select_files(self):
        """Selecciona CARPETA con archivos de planilla Régimen Minero."""
        carpeta = quick_dir_select_qt(
            parent=self,
            title="Seleccionar carpeta con archivos de planilla - Régimen Minero",
            cache_key="nomina_regimen_minero_carpeta",
        )

        if not carpeta:
            return

        archivos_excel = list(carpeta.glob("*.xlsx")) + list(carpeta.glob("*.xls"))
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

        self.archivos_seleccionados = archivos_excel
        count = len(archivos_excel)

        self.label_files.setText(
            f"✓ Carpeta: {carpeta.name}\n"
            f"✓ {count} archivo{'s' if count > 1 else ''} encontrado{'s' if count > 1 else ''}:\n"
            + "\n".join([f"  • {f.name}" for f in archivos_excel[:5]])
            + (f"\n  ... y {count - 5} más" if count > 5 else "")
        )

        self.btn_clear.setEnabled(True)
        self._log(f"📂 Carpeta seleccionada: {carpeta.name}")
        self._log(f"📄 Encontrados {count} archivo(s) Excel")
        self._update_process_button()

    def _clear_files(self):
        """Limpia la selección de planillas."""
        self.archivos_seleccionados = []
        self.label_files.setText(self._get_no_files_message())
        self.btn_clear.setEnabled(False)
        self._log("🗑️ Selección de planillas limpiada")
        self._update_process_button()

    def _select_licencias(self):
        """Abre selector para elegir el archivo de licencias."""
        archivo = quick_file_select_qt(
            parent=self,
            title="Seleccionar archivo de licencias - CONTROL DE LICENCIAS.xlsx",
            file_filter="Archivos Excel (*.xlsx *.xls);;Todos los archivos (*.*)",
            multiple=False,
            cache_key="licencias_archivo",
        )

        if not archivo:
            return

        # quick_file_select_qt puede retornar lista o Path según multiple=
        if isinstance(archivo, list):
            archivo = archivo[0] if archivo else None

        if not archivo:
            return

        self.ruta_licencias = archivo
        self.label_licencias.setText(f"✓ Archivo: {archivo.name}\n   {archivo.parent}")
        self.btn_clear_licencias.setEnabled(True)
        self._log(f"📋 Licencias seleccionadas: {archivo.name}")
        self._update_process_button()

    def _clear_licencias(self):
        """Limpia la selección del archivo de licencias."""
        self.ruta_licencias = None
        self.label_licencias.setText(
            "No hay archivo de licencias seleccionado.\n"
            "Seleccione el archivo 'CONTROL DE LICENCIAS.xlsx'."
        )
        self.btn_clear_licencias.setEnabled(False)
        self._log("🗑️ Selección de licencias limpiada")
        self._update_process_button()

    def _update_process_button(self):
        """Habilita el botón de proceso solo cuando ambas selecciones están listas."""
        listo = bool(self.archivos_seleccionados) and self.ruta_licencias is not None
        self.btn_process.setEnabled(listo)

    def _start_processing(self):
        """Inicia el ETL pasando también la ruta de licencias al worker."""
        if not self.archivos_seleccionados or self.ruta_licencias is None:
            return

        self.btn_select.setEnabled(False)
        self.btn_select_licencias.setEnabled(False)
        self.btn_process.setEnabled(False)
        self.btn_clear.setEnabled(False)
        self.btn_clear_licencias.setEnabled(False)

        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)

        self._log("🚀 Iniciando pipeline Régimen Minero + Licencias...")

        output_dir = self.archivos_seleccionados[0].parent

        self.worker = NominaRegimenMineroWorker(
            self.archivos_seleccionados,
            output_dir,
            ruta_licencias=self.ruta_licencias,
            export_excel_gold=self.chk_export_excel_gold.isChecked(),
        )
        self.worker.progress_updated.connect(self._on_progress)
        self.worker.finished.connect(self._on_finished)
        self.worker.start()

    def _on_finished(self, success: bool, message: str, results: dict):
        """Rehabilita todos los botones al terminar."""
        super()._on_finished(success, message, results)
        self.btn_select_licencias.setEnabled(True)
        self.btn_clear_licencias.setEnabled(self.ruta_licencias is not None)
