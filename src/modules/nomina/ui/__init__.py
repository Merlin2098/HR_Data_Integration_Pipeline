# ui/etls/nomina/__init__.py
"""
ETL de Nómina - Consolidación de planillas Metso
"""

from .config import CONFIG
from .widget import NominaWidget
from .worker import NominaWorker

__all__ = ["CONFIG", "NominaWidget", "NominaWorker"]
