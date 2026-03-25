from __future__ import annotations

from pathlib import Path
from typing import Callable


def maybe_write_excel(
    ruta_excel: Path,
    export_excel: bool,
    writer: Callable[[Path], None],
) -> Path | None:
    """
    Ejecuta la escritura de Excel solo cuando está habilitada.

    Args:
        ruta_excel: Ruta de salida propuesta para el archivo Excel.
        export_excel: Flag que habilita o deshabilita la exportación.
        writer: Función que recibe la ruta y materializa el archivo.

    Returns:
        La ruta del Excel generado o ``None`` si la exportación fue omitida.
    """
    if not export_excel:
        return None

    writer(ruta_excel)
    return ruta_excel
