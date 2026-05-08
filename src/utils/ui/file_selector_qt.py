# utils/file_selector_qt.py
"""
Selector de archivos usando PySide6 QFileDialog
Reemplazo de file_selector.py para interfaces Qt
"""

from pathlib import Path
from typing import List, Optional
from PySide6.QtWidgets import QFileDialog, QWidget


class FileSelectorQt:
    """Selector de archivos con QFileDialog para interfaces PySide6"""

    @staticmethod
    def select_files(
        parent: Optional[QWidget] = None,
        title: str = "Seleccionar Archivos",
        directory: Optional[Path] = None,
        file_filter: str = "Todos los archivos (*.*)",
        multiple: bool = True,
    ) -> List[Path]:
        """
        Abre diálogo para seleccionar archivos

        Args:
            parent: Widget padre
            title: Título del diálogo
            directory: Directorio inicial
            file_filter: Filtro de tipos de archivo
            multiple: Permitir selección múltiple

        Returns:
            Lista de Path de archivos seleccionados
        """
        initial_dir = str(directory) if directory else ""

        if multiple:
            files, _ = QFileDialog.getOpenFileNames(
                parent, title, initial_dir, file_filter
            )
            return [Path(f) for f in files] if files else []
        else:
            file, _ = QFileDialog.getOpenFileName(
                parent, title, initial_dir, file_filter
            )
            return [Path(file)] if file else []

    @staticmethod
    def select_directory(
        parent: Optional[QWidget] = None,
        title: str = "Seleccionar Carpeta",
        directory: Optional[Path] = None,
    ) -> Optional[Path]:
        """
        Abre diálogo para seleccionar directorio

        Args:
            parent: Widget padre
            title: Título del diálogo
            directory: Directorio inicial

        Returns:
            Path del directorio seleccionado o None
        """
        initial_dir = str(directory) if directory else ""

        dir_path = QFileDialog.getExistingDirectory(
            parent,
            title,
            initial_dir,
            QFileDialog.ShowDirsOnly | QFileDialog.DontResolveSymlinks,
        )

        return Path(dir_path) if dir_path else None


def quick_file_select_qt(
    parent: Optional[QWidget] = None,
    title: str = "Seleccionar Archivos",
    file_filter: str = "Excel (*.xlsx *.xls);;Todos (*.*)",
    multiple: bool = True,
    cache_key: Optional[str] = None,
) -> List[Path]:
    """
    Selección rápida de archivos con cache opcional

    Args:
        parent: Widget padre
        title: Título del diálogo
        file_filter: Filtro de archivos
        multiple: Permitir múltiples
        cache_key: Key para PathCache

    Returns:
        Lista de archivos seleccionados
    """
    from ..path_cache import get_path_cache

    initial_dir = None
    if cache_key:
        cache = get_path_cache()
        initial_dir = cache.get_last_path(cache_key)
        if initial_dir and initial_dir.is_file():
            initial_dir = initial_dir.parent

    files = FileSelectorQt.select_files(
        parent=parent,
        title=title,
        directory=initial_dir,
        file_filter=file_filter,
        multiple=multiple,
    )

    if files and cache_key:
        cache = get_path_cache()
        cache.set_last_path(cache_key, files[0])
        cache.add_to_frequent(cache_key, files[0])

    return files


def quick_dir_select_qt(
    parent: Optional[QWidget] = None,
    title: str = "Seleccionar Carpeta",
    cache_key: Optional[str] = None,
) -> Optional[Path]:
    """
    Selección rápida de directorio con cache opcional

    Args:
        parent: Widget padre
        title: Título
        cache_key: Key para PathCache

    Returns:
        Directorio seleccionado o None
    """
    from ..path_cache import get_path_cache

    initial_dir = None
    if cache_key:
        cache = get_path_cache()
        initial_dir = cache.get_last_path(cache_key)

    directory = FileSelectorQt.select_directory(
        parent=parent, title=title, directory=initial_dir
    )

    if directory and cache_key:
        cache = get_path_cache()
        cache.set_last_path(cache_key, directory)

    return directory
