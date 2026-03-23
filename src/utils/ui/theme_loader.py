# src/utils/ui/theme_loader.py
from pathlib import Path

from src.utils.paths import get_resource_path
from src.utils.structured_config import load_structured_data, resolve_structured_path


def load_theme(theme_filename: str = "theme_light.json") -> str:
    """
    Carga stylesheet del tema, manteniendo JSON como formato canónico
    para `theme_light` y dejando compatibilidad estructurada como fallback.
    
    Args:
        theme_filename: Nombre del archivo (ej: 'theme_light.json')
                       o ruta relativa 'assets/config/theme_light.json'
        
    Returns:
        String con el stylesheet QSS
    """
    # Si viene solo el nombre, asumimos que está en assets/config/
    if "/" not in theme_filename and "\\" not in theme_filename:
        relative_path = Path("assets/config") / theme_filename
    else:
        relative_path = Path(theme_filename)

    # Si se pide un archivo exacto, respetarlo antes del fallback estructurado.
    exact_candidates: list[Path] = []
    if relative_path.is_absolute():
        exact_candidates.append(relative_path)
    else:
        exact_candidates.append(get_resource_path(relative_path.as_posix()))
        exact_candidates.append(relative_path)

    full_path = next((candidate for candidate in exact_candidates if candidate.exists()), None)

    if full_path is None:
        full_path = resolve_structured_path(relative_path)
    
    if not full_path.exists():
        # Fallback: intentar buscar en raíz si falló config/
        full_path_root = resolve_structured_path(theme_filename)
        if full_path_root.exists():
            full_path = full_path_root
        else:
            raise FileNotFoundError(f"Tema no encontrado en: {full_path}")
    
    try:
        theme_data = load_structured_data(full_path)
        return theme_data['pyqt5']['stylesheet']
    except Exception as e:
        print(f"Error cargando tema: {e}")
        return "" # Retorno seguro para no crashear la UI
