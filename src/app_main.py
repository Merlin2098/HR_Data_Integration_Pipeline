# src/app_main.py
"""
Ventana principal con tabs dinámicos.
Los tabs se cargan automáticamente desde el registry.
"""
from typing import Callable, Dict, List, Optional
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QMessageBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtGui import QFont, QIcon

# ✅ Imports absolutos (compatibles con PyInstaller)
from src.utils.ui.theme_loader import load_theme
from src.utils.ui.etl_registry import get_registry
from src.utils.paths import get_resource_path
from src.utils.structured_config import resolve_structured_path


class ETLManagerWindow(QMainWindow):
    """Ventana principal con tabs dinámicos para ETLs."""
    
    def __init__(self, startup_progress_callback: Optional[Callable[[int, str], None]] = None):
        super().__init__()
        self._startup_progress_callback = startup_progress_callback
        self._etl_tabs: List[Dict] = []

        self.setWindowTitle("ETL Manager - Data RRHH")
        self.setMinimumSize(900, 650)

        self._report_startup_progress(10, "Preparando ventana principal")
        self._setup_ui()
        self._report_startup_progress(75, "Aplicando tema")
        self._apply_theme()
        self._report_startup_progress(85, "Configurando icono")
        self._set_app_icon()
        self._report_startup_progress(95, "UI lista")

    def _report_startup_progress(self, progress: int, message: str):
        """Notifica avance de startup al callback (splash)."""
        if self._startup_progress_callback:
            self._startup_progress_callback(progress, message)
    
    def _setup_ui(self):
        """Configura la interfaz con tabs dinámicos."""
        # Tab widget principal
        self.tabs = QTabWidget()
        self.tabs.setTabPosition(QTabWidget.North)
        self.tabs.setMovable(False)
        
        # Cargar ETLs dinámicamente
        self._load_etl_tabs()
        self.tabs.currentChanged.connect(self._on_tab_changed)
        
        self.setCentralWidget(self.tabs)
    
    def _load_etl_tabs(self):
        """
        Descubre y carga automáticamente todos los ETLs disponibles.
        """
        self._report_startup_progress(20, "Descubriendo ETLs")

        print("\n" + "="*60)
        print(" DESCUBRIENDO ETLs DISPONIBLES ".center(60, "="))
        print("="*60 + "\n")
        
        registry = get_registry()
        etls = registry.discover_etls()
        
        if not etls:
            # Si no hay ETLs, mostrar mensaje
            print("⚠️ No se encontraron ETLs disponibles")
            QMessageBox.warning(
                self,
                "Sin ETLs",
                "No se encontraron ETLs disponibles.\n"
                "Verifica la carpeta src/modules/"
            )
            return
        
        print(f"\n📦 Total de ETLs encontrados: {len(etls)}\n")

        # Crear tabs en modo lazy: solo se instancian al abrir cada tab.
        total_etls = len(etls)
        for idx, etl_info in enumerate(etls, start=1):
            try:
                container, placeholder = self._create_lazy_tab_container(etl_info['name'])
                lazy_info = etl_info.copy()
                lazy_info.update({
                    'container': container,
                    'placeholder': placeholder,
                    'loaded': False,
                    'widget_instance': None
                })
                self._etl_tabs.append(lazy_info)
                
                # Agregar tab con ícono + nombre
                tab_label = f"{etl_info['icon']} {etl_info['name']}"
                self.tabs.addTab(container, tab_label)
                
                print(f"  ✅ Tab creado: {tab_label}")

                progress = 25 + int((idx / total_etls) * 40)
                self._report_startup_progress(progress, f"Registrando ETL: {etl_info['name']}")
                
            except Exception as e:
                print(f"  ❌ Error creando tab para {etl_info['name']}: {e}")
                import traceback
                traceback.print_exc()
        
        # Cargar solo el primer tab al inicio; el resto se carga bajo demanda.
        if self.tabs.count() > 0:
            self._report_startup_progress(70, "Cargando primer modulo")
            self._ensure_tab_loaded(0)

        print(f"\n{'='*60}\n")

    def _create_lazy_tab_container(self, etl_name: str):
        """Crea contenedor placeholder para carga diferida del widget."""
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(24, 24, 24, 24)

        placeholder = QLabel(
            f"Modulo listo para carga diferida:\n{etl_name}\n\n"
            "Abre este tab para inicializar su interfaz."
        )
        placeholder.setAlignment(Qt.AlignCenter)
        placeholder.setWordWrap(True)
        placeholder.setStyleSheet("color: #4b5563;")
        placeholder.setFont(QFont("Segoe UI", 14))

        layout.addStretch()
        layout.addWidget(placeholder)
        layout.addStretch()

        return container, placeholder

    def _set_lazy_message(self, tab_info: Dict, message: str, is_error: bool = False):
        """Actualiza texto de placeholder del tab lazy."""
        placeholder = tab_info.get('placeholder')
        if placeholder is None:
            return

        color = "#b91c1c" if is_error else "#4b5563"
        placeholder.setStyleSheet(f"color: {color};")
        placeholder.setText(message)

    def _on_tab_changed(self, index: int):
        """Carga el tab seleccionado si todavía no está inicializado."""
        self._ensure_tab_loaded(index)

    def _ensure_tab_loaded(self, index: int):
        """Instancia widget de tab on-demand."""
        if index < 0 or index >= len(self._etl_tabs):
            return

        tab_info = self._etl_tabs[index]
        if tab_info.get('loaded'):
            return

        self._set_lazy_message(tab_info, f"Cargando interfaz de {tab_info['name']}...")
        QApplication.processEvents()

        try:
            widget = tab_info['widget_class']()
            container = tab_info['container']
            layout = container.layout()
            placeholder = tab_info.get('placeholder')

            if placeholder is not None:
                layout.removeWidget(placeholder)
                placeholder.deleteLater()
                tab_info['placeholder'] = None

            layout.addWidget(widget)
            tab_info['widget_instance'] = widget
            tab_info['loaded'] = True

            print(f"  ✅ Widget inicializado on-demand: {tab_info['name']}")

        except Exception as e:
            import traceback
            self._set_lazy_message(
                tab_info,
                f"No se pudo cargar {tab_info['name']}.\n{e}",
                is_error=True
            )
            print(f"  ❌ Error en lazy load de {tab_info['name']}: {e}")
            traceback.print_exc()
    
    def _apply_theme(self):
        """Aplica tema desde configuración estructurada."""
        try:
            # Mantener JSON como formato canónico del tema.
            theme_path = get_resource_path("assets/config/theme_light.json")

            if not theme_path.exists():
                theme_path = resolve_structured_path("assets/config/theme_light")
            
            if not theme_path.exists():
                print(f"⚠️ Tema no encontrado en: {theme_path}")
                print("   Continuando sin tema...")
                return
            
            stylesheet = load_theme(str(theme_path))
            self.setStyleSheet(stylesheet)
            print("✅ Tema aplicado correctamente")
        except Exception as e:
            print(f"⚠️ Error cargando tema: {e}")
            print("   Continuando sin tema...")
    
    def _set_app_icon(self):
        """Aplica ícono de la aplicación desde config/app.ico"""
        try:
            # ✅ Usar helper de paths
            icon_path = get_resource_path("assets/config/app.ico")
            
            if not icon_path.exists():
                print(f"⚠️ Ícono no encontrado en: {icon_path}")
                print("   Continuando sin ícono...")
                return
            
            icon = QIcon(str(icon_path))
            self.setWindowIcon(icon)
            print("✅ Ícono aplicado correctamente")
        except Exception as e:
            print(f"⚠️ Error cargando ícono: {e}")
            print("   Continuando sin ícono...")
