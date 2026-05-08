# ui/etls/pdt/config.py
"""
Configuración del ETL de PDT - Relación de Ingresos
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
    id="pdt",
    name="PDT - Relación de Ingresos",
    icon="📋",
    description="Procesamiento de Relación de Ingresos (EMPLEADOS y PRACTICANTES)",
    enabled=True,
    order=3,
)
