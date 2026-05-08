# ui/etls/nomina_regimen_minero/config.py
"""
Configuración del ETL de Nómina Régimen Minero
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
    id="nomina_regimen_minero",
    name="Nómina Régimen Minero",
    icon="⛏️",
    description="Consolidación de planillas - Régimen Minero",
    enabled=True,
    order=2,
)
