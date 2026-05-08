# ui/etls/bd/config.py
"""
Configuración del ETL de Base de Datos Consolidada
"""

from dataclasses import dataclass


@dataclass
class ETLConfig:
    """Metadata del ETL"""

    id: str
    name: str
    icon: str
    description: str
    enabled: bool = True
    order: int = 0


# Configuración de este ETL
CONFIG = ETLConfig(
    id="bd",
    name="Base de Datos",
    icon="🗄️",
    description="Procesamiento completo: Bronze→Silver→Gold + Centros de Costo + Flags",
    enabled=True,
    order=4,
)
