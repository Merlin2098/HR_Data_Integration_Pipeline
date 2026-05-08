# ui/etls/nomina/config.py
"""
Configuración del ETL de Nómina con Licencias
Pipeline completo: Nóminas + Licencias → Silver → Gold Enriquecido
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
    id="nomina",
    name="Nómina",
    icon="📊",
    description="Pipeline completo: Nóminas + Licencias → Gold Enriquecido",
    enabled=True,
    order=1,
)
