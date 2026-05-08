# src/utils/paths.py
import sys
from pathlib import Path


def get_project_root() -> Path:
    """
    Retorna la ruta base del proyecto.
    - En Dev: La raíz del proyecto (donde está etl_manager.py).
    - En Exe (Onedir): La carpeta donde está el .exe.
    """
    if getattr(sys, "frozen", False):
        # Ejecutable de PyInstaller
        # sys.executable apunta al .exe
        return Path(sys.executable).parent
    else:
        # Desarrollo (este archivo está en /src/core)
        return Path(__file__).resolve().parent.parent.parent


def get_resource_path(relative_path: str) -> Path:
    """
    Resuelve la ruta absoluta para recursos.
    Maneja tanto desarrollo como PyInstaller onedir.

    Args:
        relative_path: Ruta relativa desde la raíz (ej: "config/app.ico")

    Returns:
        Path absoluto al recurso
    """
    base_path = get_project_root()

    if getattr(sys, "frozen", False):
        # En ejecutable PyInstaller onedir
        # Los recursos están en _internal/ dentro de la carpeta del exe
        internal_path = base_path / "_internal" / relative_path

        # Si existe en _internal, usarlo
        if internal_path.exists():
            return internal_path

        # Sino, intentar en la raíz (por si hay archivos externos)
        root_path = base_path / relative_path
        if root_path.exists():
            return root_path

        # Si no existe en ninguno, retornar el path de _internal
        # (para que los logs muestren dónde debería estar)
        return internal_path
    else:
        # En desarrollo, ruta relativa normal
        return base_path / relative_path


def get_data_path(relative_path: str) -> Path:
    """
    Resuelve rutas para archivos de DATOS (lectura/escritura).
    Estos NO están empaquetados, siempre están junto al .exe.

    Args:
        relative_path: Ruta relativa (ej: "data/output.xlsx")

    Returns:
        Path absoluto para datos
    """
    base_path = get_project_root()
    return base_path / relative_path
