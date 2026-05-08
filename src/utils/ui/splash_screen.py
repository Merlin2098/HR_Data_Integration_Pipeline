"""
Splash screen de inicio para mostrar progreso de carga de la UI.
"""

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont, QPainter, QPixmap
from PySide6.QtWidgets import QApplication, QLabel, QProgressBar, QSplashScreen


class StartupSplashScreen(QSplashScreen):
    """Splash screen simple con estado de arranque."""

    def __init__(self, title: str = "ETL Manager"):
        super().__init__(self._build_pixmap(title))
        self.setWindowFlag(Qt.WindowStaysOnTopHint)
        self._status_label = QLabel(self)
        self._status_label.setGeometry(40, 320, 680, 24)
        self._status_label.setAlignment(Qt.AlignCenter)
        self._status_label.setStyleSheet("color: #f9fafb;")
        self._status_label.setFont(QFont("Segoe UI", 12))

        self._progress_bar = QProgressBar(self)
        self._progress_bar.setGeometry(40, 350, 680, 12)
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setTextVisible(False)
        self._progress_bar.setStyleSheet(
            """
            QProgressBar {
                border: 1px solid #4b5563;
                border-radius: 6px;
                background-color: #111827;
            }
            QProgressBar::chunk {
                border-radius: 6px;
                background-color: #60a5fa;
            }
            """
        )
        self.update_status(0, "Inicializando aplicacion")

    @staticmethod
    def _build_pixmap(title: str) -> QPixmap:
        width, height = 760, 420
        pixmap = QPixmap(width, height)
        pixmap.fill(QColor("#1f2937"))

        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)

        painter.setPen(QColor("#f9fafb"))
        painter.setFont(QFont("Segoe UI", 26, QFont.Bold))
        painter.drawText(40, 160, title)

        painter.setPen(QColor("#d1d5db"))
        painter.setFont(QFont("Segoe UI", 12))
        painter.drawText(40, 205, "Cargando modulos y preparando interfaz")

        painter.setPen(QColor("#60a5fa"))
        painter.setFont(QFont("Segoe UI", 10))
        painter.drawText(40, 380, "Metso")

        painter.end()
        return pixmap

    def update_status(self, progress: int, message: str):
        """Actualiza mensaje mostrado en splash."""
        clamped = max(0, min(100, int(progress)))
        self._status_label.setText(message)
        self._progress_bar.setValue(clamped)
        QApplication.processEvents()
