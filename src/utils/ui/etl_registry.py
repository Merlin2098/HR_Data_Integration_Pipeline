# src/utils/ui/etl_registry.py
"""
Registro automático de ETLs disponibles.
Lee la carpeta src/modules/ y descubre ETLs con layout <module>/ui.
"""

from typing import List, Dict
from importlib import import_module

# ✅ Import para rutas compatibles con PyInstaller
from src.utils.paths import get_resource_path


class ETLRegistry:
    """
    Registro de ETLs disponibles.
    Descubre automáticamente los ETLs en la carpeta src/modules/
    """

    def __init__(self):
        self.etls: Dict[str, Dict] = {}
        # ✅ Usar helper de paths para desarrollo y producción
        self.etls_dir = get_resource_path("src/modules")

    def discover_etls(self) -> List[Dict]:
        """
        Descubre todos los ETLs disponibles en src/modules/

        Returns:
            Lista de diccionarios con metadata de cada ETL
        """
        print(f"\n🔍 Buscando ETLs en: {self.etls_dir}")

        if not self.etls_dir.exists():
            print(f"⚠️ No se encontró la carpeta de módulos en: {self.etls_dir}")
            return []

        print("✅ Carpeta de módulos encontrada")

        discovered_etls = []

        # Buscar subcarpetas en src/modules/
        subdirs = [
            d
            for d in self.etls_dir.iterdir()
            if d.is_dir() and not d.name.startswith("_")
        ]
        print(f"📁 Subcarpetas encontradas: {[d.name for d in subdirs]}")

        for etl_dir in subdirs:
            print(f"\n📦 Procesando: {etl_dir.name}")

            # Buscar package ui de cada módulo
            ui_dir = etl_dir / "ui"
            config_file = ui_dir / "config.py"
            widget_file = ui_dir / "widget.py"

            print(f"   - config.py existe: {config_file.exists()}")
            print(f"   - widget.py existe: {widget_file.exists()}")

            if not config_file.exists() or not widget_file.exists():
                print(f"   ⚠️ ETL incompleto: {etl_dir.name}")
                continue

            try:
                # Importar configuración
                module_path = f"src.modules.{etl_dir.name}.ui.config"
                print(f"   - Importando config: {module_path}")
                config_module = import_module(module_path)

                if not hasattr(config_module, "CONFIG"):
                    print(f"   ⚠️ {etl_dir.name}/config.py no tiene CONFIG")
                    continue

                config = config_module.CONFIG
                print(f"   ✅ Config cargado: {config.name}")

                # Verificar si está habilitado
                if not config.enabled:
                    print(f"   ⏸️ ETL deshabilitado: {config.name}")
                    continue

                # Importar widget
                widget_module_path = f"src.modules.{etl_dir.name}.ui.widget"
                print(f"   - Importando widget: {widget_module_path}")
                widget_module = import_module(widget_module_path)

                # Buscar clase que termine en "Widget"
                widget_class = None
                for item_name in dir(widget_module):
                    if item_name.endswith("Widget") and item_name != "BaseETLWidget":
                        widget_class = getattr(widget_module, item_name)
                        print(f"   ✅ Widget encontrado: {item_name}")
                        break

                if not widget_class:
                    print(f"   ⚠️ No se encontró clase Widget en {etl_dir.name}")
                    continue

                # Registrar ETL
                etl_info = {
                    "id": config.id,
                    "name": config.name,
                    "icon": config.icon,
                    "description": config.description,
                    "order": config.order,
                    "widget_class": widget_class,
                    "config": config,
                }

                discovered_etls.append(etl_info)
                print(f"   ✅ ETL registrado: {config.icon} {config.name}")

            except Exception as e:
                print(f"   ❌ Error cargando ETL {etl_dir.name}: {e}")
                import traceback

                print("   Traceback completo:")
                traceback.print_exc()
                continue

        # Ordenar por 'order'
        discovered_etls.sort(key=lambda x: x["order"])

        print(f"\n📊 Total ETLs descubiertos: {len(discovered_etls)}\n")

        return discovered_etls

    def get_etl(self, etl_id: str) -> Dict:
        """Obtiene información de un ETL específico"""
        return self.etls.get(etl_id)

    def list_etls(self) -> List[str]:
        """Lista IDs de todos los ETLs registrados"""
        return list(self.etls.keys())


# Instancia global
_registry = None


def get_registry() -> ETLRegistry:
    """Obtiene instancia singleton del registry"""
    global _registry
    if _registry is None:
        _registry = ETLRegistry()
    return _registry
