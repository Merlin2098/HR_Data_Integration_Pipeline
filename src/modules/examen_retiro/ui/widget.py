# ui/etls/examen_retiro/widget.py
"""
Widget específico para ETL de Exámenes de Retiro
Requiere selección de 3 archivos:
1. Archivo Bronze (Excel)
2. CC_ACTUAL (Parquet)
3. CC_OLD (Parquet)
"""
import sys
from pathlib import Path
from typing import Optional

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QProgressBar, QLabel, QTextEdit, QGroupBox, QCheckBox
)
from PySide6.QtCore import Qt

from src.utils.ui.workers.base_worker import BaseETLWorker
from .worker import ExamenRetiroWorker
from src.utils.ui.file_selector_qt import quick_file_select_qt


class ExamenRetiroWidget(QWidget):
    """Widget para procesamiento de Exámenes de Retiro"""
    
    def __init__(self):
        super().__init__()
        self.title = "Procesamiento de Exámenes de Retiro"
        
        # Archivos seleccionados
        self.archivo_bronze: Optional[Path] = None
        self.archivo_cc_actual: Optional[Path] = None
        self.archivo_cc_old: Optional[Path] = None
        
        self.worker = None
        self._setup_ui()
    
    def _setup_ui(self):
        """Configura UI completa"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)
        
        # Header
        header = QLabel(self.title)
        header.setProperty("labelStyle", "title")
        header.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(header)
        
        # Card: Selección de archivos
        group_files = QGroupBox("1. Selección de Archivos")
        layout_files = QVBoxLayout()
        
        # --- ARCHIVO BRONZE ---
        row_bronze = QHBoxLayout()
        self.btn_bronze = QPushButton("📂 SELECCIONAR ARCHIVO EXCEL")
        self.btn_bronze.setMinimumWidth(300)
        self.btn_bronze.setMaximumWidth(500)
        self.btn_bronze.clicked.connect(self._select_bronze)
        row_bronze.addWidget(self.btn_bronze)
        
        self.label_bronze_status = QLabel("✗")
        self.label_bronze_status.setStyleSheet("color: red; font-size: 16px; font-weight: bold;")
        self.label_bronze_status.setFixedWidth(30)
        self.label_bronze_status.setAlignment(Qt.AlignCenter)
        row_bronze.addWidget(self.label_bronze_status)
        row_bronze.addStretch()
        
        layout_files.addLayout(row_bronze)
        layout_files.addSpacing(5)
        
        # --- CC_ACTUAL ---
        row_cc_actual = QHBoxLayout()
        self.btn_cc_actual = QPushButton("📦 SELECCIONAR CC_ACTUAL")
        self.btn_cc_actual.setMinimumWidth(300)
        self.btn_cc_actual.setMaximumWidth(500)
        self.btn_cc_actual.clicked.connect(self._select_cc_actual)
        row_cc_actual.addWidget(self.btn_cc_actual)
        
        self.label_cc_actual_status = QLabel("✗")
        self.label_cc_actual_status.setStyleSheet("color: red; font-size: 16px; font-weight: bold;")
        self.label_cc_actual_status.setFixedWidth(30)
        self.label_cc_actual_status.setAlignment(Qt.AlignCenter)
        row_cc_actual.addWidget(self.label_cc_actual_status)
        row_cc_actual.addStretch()
        
        layout_files.addLayout(row_cc_actual)
        layout_files.addSpacing(5)
        
        # --- CC_OLD ---
        row_cc_old = QHBoxLayout()
        self.btn_cc_old = QPushButton("📦 SELECCIONAR CC_OLD")
        self.btn_cc_old.setMinimumWidth(300)
        self.btn_cc_old.setMaximumWidth(500)
        self.btn_cc_old.clicked.connect(self._select_cc_old)
        row_cc_old.addWidget(self.btn_cc_old)
        
        self.label_cc_old_status = QLabel("✗")
        self.label_cc_old_status.setStyleSheet("color: red; font-size: 16px; font-weight: bold;")
        self.label_cc_old_status.setFixedWidth(30)
        self.label_cc_old_status.setAlignment(Qt.AlignCenter)
        row_cc_old.addWidget(self.label_cc_old_status)
        row_cc_old.addStretch()
        
        layout_files.addLayout(row_cc_old)
        layout_files.addSpacing(10)
        
        # --- BOTÓN LIMPIAR ---
        row_clear = QHBoxLayout()
        row_clear.addStretch()
        self.btn_clear = QPushButton("🗑️ LIMPIAR")
        self.btn_clear.setFixedWidth(120)
        self.btn_clear.clicked.connect(self._clear_files)
        self.btn_clear.setEnabled(False)
        row_clear.addWidget(self.btn_clear)
        
        layout_files.addLayout(row_clear)

        self.chk_export_excel_gold = QCheckBox("Generar Excel adicional en Gold")
        self.chk_export_excel_gold.setChecked(False)
        layout_files.addWidget(self.chk_export_excel_gold)
        
        group_files.setLayout(layout_files)
        main_layout.addWidget(group_files)
        
        # Card: Progreso
        group_progress = QGroupBox("2. Procesamiento")
        layout_progress = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        
        self.status_label = QLabel("")
        self.status_label.setProperty("labelStyle", "secondary")
        
        self.btn_process = QPushButton("▶️ Iniciar Procesamiento")
        self.btn_process.clicked.connect(self._start_processing)
        self.btn_process.setEnabled(False)
        
        layout_progress.addWidget(self.progress_bar)
        layout_progress.addWidget(self.status_label)
        layout_progress.addWidget(self.btn_process)
        group_progress.setLayout(layout_progress)
        main_layout.addWidget(group_progress)
        
        # Card: Log
        group_log = QGroupBox("3. Log de Actividad")
        layout_log = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(150)
        self.log_text.append(f"📋 Sistema {self.title} listo")
        
        layout_log.addWidget(self.log_text)
        group_log.setLayout(layout_log)
        main_layout.addWidget(group_log)
        
        main_layout.addStretch()
    
    def _select_bronze(self):
        """Selecciona archivo Bronze (Excel)"""
        files = quick_file_select_qt(
            parent=self,
            title="Seleccionar archivo Excel - Exámenes de Retiro",
            file_filter="Archivos Excel (*.xlsx *.xlsm *.xls);;Todos los archivos (*.*)",
            multiple=False,
            cache_key="examen_retiro_bronze"
        )
        
        if not files:
            return
        
        archivo = files[0] if isinstance(files, list) else files
        
        if archivo.suffix.lower() not in ['.xlsx', '.xlsm', '.xls']:
            self._log(f"⚠️ El archivo debe ser Excel (.xlsx, .xlsm o .xls)")
            return
        
        self.archivo_bronze = archivo
        self.label_bronze_status.setText("✓")
        self.label_bronze_status.setStyleSheet("color: green; font-size: 16px; font-weight: bold;")
        self._log(f"📂 Archivo Bronze seleccionado: {archivo.name}")
        self._check_all_files_selected()
    
    def _select_cc_actual(self):
        """Selecciona archivo CC_ACTUAL (Parquet)"""
        files = quick_file_select_qt(
            parent=self,
            title="Seleccionar CC_ACTUAL (Parquet)",
            file_filter="Archivos Parquet (*.parquet);;Todos los archivos (*.*)",
            multiple=False,
            cache_key="examen_retiro_cc_actual"
        )
        
        if not files:
            return
        
        archivo = files[0] if isinstance(files, list) else files
        
        if archivo.suffix.lower() != '.parquet':
            self._log(f"⚠️ El archivo debe ser Parquet (.parquet)")
            self.label_cc_actual.setText("⚠️ Archivo no válido")
            return
        
        self.archivo_cc_actual = archivo
        self.label_cc_actual_status.setText("✓")
        self.label_cc_actual_status.setStyleSheet("color: green; font-size: 16px; font-weight: bold;")
        self._log(f"📦 CC_ACTUAL seleccionado: {archivo.name}")
        self._check_all_files_selected()
    
    def _select_cc_old(self):
        """Selecciona archivo CC_OLD (Parquet)"""
        files = quick_file_select_qt(
            parent=self,
            title="Seleccionar CC_OLD (Parquet)",
            file_filter="Archivos Parquet (*.parquet);;Todos los archivos (*.*)",
            multiple=False,
            cache_key="examen_retiro_cc_old"
        )
        
        if not files:
            return
        
        archivo = files[0] if isinstance(files, list) else files
        
        if archivo.suffix.lower() != '.parquet':
            self._log(f"⚠️ El archivo debe ser Parquet (.parquet)")
            self.label_cc_old.setText("⚠️ Archivo no válido")
            return
        
        self.archivo_cc_old = archivo
        self.label_cc_old_status.setText("✓")
        self.label_cc_old_status.setStyleSheet("color: green; font-size: 16px; font-weight: bold;")
        self._log(f"📦 CC_OLD seleccionado: {archivo.name}")
        self._check_all_files_selected()
    
    def _check_all_files_selected(self):
        """Verifica si todos los archivos están seleccionados y habilita botones"""
        all_selected = (
            self.archivo_bronze is not None and
            self.archivo_cc_actual is not None and
            self.archivo_cc_old is not None
        )
        
        self.btn_process.setEnabled(all_selected)
        self.btn_clear.setEnabled(
            self.archivo_bronze is not None or
            self.archivo_cc_actual is not None or
            self.archivo_cc_old is not None
        )
        
        if all_selected:
            self._log("✓ Todos los archivos seleccionados - Listo para procesar")
        else:
            archivos_faltantes = []
            if self.archivo_bronze is None:
                archivos_faltantes.append("Archivo Bronze")
            if self.archivo_cc_actual is None:
                archivos_faltantes.append("CC_ACTUAL")
            if self.archivo_cc_old is None:
                archivos_faltantes.append("CC_OLD")
            
            if archivos_faltantes:
                self._log(f"⚠️ Faltan: {', '.join(archivos_faltantes)}")
    
    def _clear_files(self):
        """Limpia la selección de archivos"""
        self.archivo_bronze = None
        self.archivo_cc_actual = None
        self.archivo_cc_old = None
        
        self.label_bronze_status.setText("✗")
        self.label_bronze_status.setStyleSheet("color: red; font-size: 16px; font-weight: bold;")
        
        self.label_cc_actual_status.setText("✗")
        self.label_cc_actual_status.setStyleSheet("color: red; font-size: 16px; font-weight: bold;")
        
        self.label_cc_old_status.setText("✗")
        self.label_cc_old_status.setStyleSheet("color: red; font-size: 16px; font-weight: bold;")
        
        self.btn_process.setEnabled(False)
        self.btn_clear.setEnabled(False)
        
        self._log("🗑️ Selección limpiada")
    
    def _start_processing(self):
        """Inicia el procesamiento ETL"""
        if not (self.archivo_bronze and self.archivo_cc_actual and self.archivo_cc_old):
            self._log("❌ Error: Faltan archivos por seleccionar")
            return
        
        self.btn_bronze.setEnabled(False)
        self.btn_cc_actual.setEnabled(False)
        self.btn_cc_old.setEnabled(False)
        self.btn_process.setEnabled(False)
        self.btn_clear.setEnabled(False)
        
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        
        self._log("🚀 Iniciando procesamiento ETL...")
        
        # Obtener directorio de salida (padre del archivo bronze)
        output_dir = self.archivo_bronze.parent
        
        # Crear worker con los 3 archivos
        self.worker = ExamenRetiroWorker(
            archivo_bronze=self.archivo_bronze,
            archivo_cc_actual=self.archivo_cc_actual,
            archivo_cc_old=self.archivo_cc_old,
            output_dir=output_dir,
            export_excel_gold=self.chk_export_excel_gold.isChecked(),
        )
        
        self.worker.progress_updated.connect(self._on_progress)
        self.worker.finished.connect(self._on_finished)
        self.worker.start()
    
    def _on_progress(self, value: int, message: str):
        """Actualiza barra de progreso"""
        self.progress_bar.setValue(value)
        self.status_label.setText(message)
        self._log(f"[{value}%] {message}")
    
    def _on_finished(self, success: bool, message: str, results: dict):
        """Maneja finalización del proceso"""
        from PySide6.QtWidgets import QMessageBox
        
        self._log(message)
        
        self.btn_bronze.setEnabled(True)
        self.btn_cc_actual.setEnabled(True)
        self.btn_cc_old.setEnabled(True)
        self.btn_clear.setEnabled(True)
        self.btn_process.setEnabled(True)
        
        if success:
            QMessageBox.information(self, "Proceso Completado", message)
            self.progress_bar.setVisible(False)
            self.status_label.setText("")
        else:
            QMessageBox.critical(self, "Error en Procesamiento", message)
    
    def _log(self, message: str):
        """Agrega mensaje al log"""
        self.log_text.append(message)
