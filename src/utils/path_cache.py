"""
Sistema de cache de rutas para facilitar la navegación de archivos
Almacena rutas recientes y frecuentes por pipeline/contexto.
Adaptado para funcionar tanto en script Python como en EXE congelado.
"""

import json
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime
from src.utils.paths import get_project_root  # Importamos el helper de rutas


class PathCache:
    """
    Gestor de cache de rutas para pipelines ETL
    Almacena rutas recientes y frecuentes por contexto
    """

    def __init__(self, cache_file: Optional[Path] = None):
        """
        Inicializa el cache de rutas.

        Args:
            cache_file: Ruta opcional. Si es None, se calcula automáticamente
                       en base a la ubicación del ejecutable o proyecto.
        """
        if cache_file is None:
            # Usamos get_project_root para asegurar que se cree al lado del exe/script
            # y no en carpetas temporales de PyInstaller
            self.cache_file = (
                get_project_root() / "assets" / "config" / "path_cache.json"
            )
        else:
            self.cache_file = cache_file

        # Asegurar que el directorio config exista
        self.cache_file.parent.mkdir(exist_ok=True, parents=True)
        self.cache: Dict = self._load_cache()

    def _load_cache(self) -> Dict:
        """
        Carga el cache desde el archivo JSON
        """
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: No se pudo cargar cache, usando cache vacío: {e}")
                return self._get_empty_cache()
        return self._get_empty_cache()

    def _get_empty_cache(self) -> Dict:
        """
        Retorna estructura de cache vacía
        """
        return {
            "last_paths": {},
            "frequent_dirs": {},
            "metadata": {
                "created": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
                "version": "1.0",
            },
        }

    def save(self):
        """
        Guarda el cache en el archivo JSON
        """
        try:
            self.cache["metadata"]["last_updated"] = datetime.now().isoformat()
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"Error: No se pudo guardar cache en {self.cache_file}: {e}")

    def get_last_path(self, key: str) -> Optional[Path]:
        """
        Obtiene la última ruta usada para una key específica
        """
        path_str = self.cache["last_paths"].get(key)
        if path_str:
            path = Path(path_str)
            if path.exists():
                return path
            else:
                # Limpiar path inválido
                self._remove_last_path(key)
        return None

    def set_last_path(self, key: str, path: Path):
        """
        Guarda la última ruta usada para una key
        """
        self.cache["last_paths"][key] = str(path.resolve())
        self.save()

    def _remove_last_path(self, key: str):
        """
        Elimina una ruta del cache de últimas rutas
        """
        if key in self.cache["last_paths"]:
            del self.cache["last_paths"][key]
            self.save()

    def get_frequent_dirs(self, key: str, limit: int = 5) -> List[Path]:
        """
        Obtiene los directorios más frecuentes para una key
        """
        freq_list = self.cache["frequent_dirs"].get(key, [])

        # Filtrar solo directorios válidos
        valid_dirs = []
        for dir_str in freq_list[:limit]:
            dir_path = Path(dir_str)
            if dir_path.exists() and dir_path.is_dir():
                valid_dirs.append(dir_path)

        return valid_dirs

    def add_to_frequent(self, key: str, path: Path, max_items: int = 10):
        """
        Agrega un directorio a la lista de frecuentes
        """
        # Obtener el directorio padre si path es un archivo
        if path.is_file():
            dir_path = path.parent
        else:
            dir_path = path

        dir_str = str(dir_path.resolve())

        # Obtener lista actual de frecuentes
        freq_list = self.cache["frequent_dirs"].get(key, [])

        # Si ya existe, moverlo al inicio
        if dir_str in freq_list:
            freq_list.remove(dir_str)

        # Insertar al inicio
        freq_list.insert(0, dir_str)

        # Mantener solo los últimos N items
        self.cache["frequent_dirs"][key] = freq_list[:max_items]
        self.save()

    def get_all_last_paths(self) -> Dict[str, Path]:
        """
        Obtiene todas las últimas rutas almacenadas
        """
        result = {}
        for key, path_str in self.cache["last_paths"].items():
            path = Path(path_str)
            if path.exists():
                result[key] = path
        return result

    def clear_invalid_paths(self):
        """
        Limpia todas las rutas inválidas del cache
        """
        # Limpiar last_paths
        valid_last_paths = {}
        for key, path_str in self.cache["last_paths"].items():
            if Path(path_str).exists():
                valid_last_paths[key] = path_str
        self.cache["last_paths"] = valid_last_paths

        # Limpiar frequent_dirs
        for key in list(self.cache["frequent_dirs"].keys()):
            valid_dirs = [
                d for d in self.cache["frequent_dirs"][key] if Path(d).exists()
            ]
            if valid_dirs:
                self.cache["frequent_dirs"][key] = valid_dirs
            else:
                del self.cache["frequent_dirs"][key]

        self.save()

    def clear_all(self):
        """
        Limpia completamente el cache
        """
        self.cache = self._get_empty_cache()
        self.save()

    def clear_key(self, key: str):
        """
        Limpia todas las entradas relacionadas con una key específica
        """
        if key in self.cache["last_paths"]:
            del self.cache["last_paths"][key]

        if key in self.cache["frequent_dirs"]:
            del self.cache["frequent_dirs"][key]

        self.save()

    def get_statistics(self) -> Dict:
        """
        Obtiene estadísticas del cache
        """
        total_last_paths = len(self.cache["last_paths"])
        total_frequent_keys = len(self.cache["frequent_dirs"])

        # Contar rutas válidas
        valid_last_paths = sum(
            1 for p in self.cache["last_paths"].values() if Path(p).exists()
        )

        return {
            "total_last_paths": total_last_paths,
            "valid_last_paths": valid_last_paths,
            "total_frequent_keys": total_frequent_keys,
            "metadata": self.cache["metadata"],
        }

    def export_readable(self) -> str:
        """
        Exporta el cache en formato legible
        """
        lines = ["=== PATH CACHE ===\n"]

        lines.append("ÚLTIMAS RUTAS:")
        for key, path in self.cache["last_paths"].items():
            status = "✓" if Path(path).exists() else "✗"
            lines.append(f"  {status} {key}: {path}")

        lines.append("\nDIRECTORIOS FRECUENTES:")
        for key, dirs in self.cache["frequent_dirs"].items():
            lines.append(f"  {key}:")
            for i, dir_path in enumerate(dirs, 1):
                status = "✓" if Path(dir_path).exists() else "✗"
                lines.append(f"    {i}. {status} {dir_path}")

        lines.append("\nMETADATA:")
        for k, v in self.cache["metadata"].items():
            lines.append(f"  {k}: {v}")

        return "\n".join(lines)


# Instancia global del cache (singleton pattern)
_cache_instance: Optional[PathCache] = None


def get_path_cache(cache_file: Optional[Path] = None) -> PathCache:
    """
    Obtiene la instancia singleton del PathCache.
    Ahora no requiere argumentos obligatorios.
    """
    global _cache_instance
    if _cache_instance is None:
        # Se inicializará con la ruta por defecto definida en __init__
        _cache_instance = PathCache(cache_file)
    return _cache_instance
