"""
Módulo de Orquestadores de Pipelines
Ejecuta pipelines ETL definidos en archivos YAML
"""

from .pipeline_nomina_executor import PipelineNominaExecutor
from .pipeline_control_practicantes_executor import PipelineControlPracticantesExecutor

__all__ = ["PipelineNominaExecutor", "PipelineControlPracticantesExecutor"]
