# ui/etls/examen_retiro/worker.py
"""
Worker para ETL de Exámenes de Retiro
Ejecuta: Bronze → Silver → Gold → Gold Enriquecido (3 steps)

Step 1: Extrae datos de Excel (Bronze → Silver)
Step 2: Transforma y filtra datos (Silver → Gold)
Step 3: Enriquece con Centros de Costo usando JOIN (Gold → Gold Enriquecido)

Implementa:
- Lazy loading de módulos
- Timer de ejecución por fase
- Manejo robusto de errores
- Logs detallados de validaciones
"""
from pathlib import Path
from typing import Dict, Any, Optional, Union
import sys
import time
import traceback

# Asegurar que el directorio raíz del proyecto esté en el path
project_root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(project_root))

from PySide6.QtCore import QThread, Signal
from src.utils.lazy_loader import create_etl_loader
from src.utils.logger_qt import UILogger
from src.utils.validate_source import SourceValidationError, validate_all_sources_for_etl
from src.utils.structured_config import load_structured_data, resolve_structured_path


class ExamenRetiroWorker(QThread):
    """Worker para procesamiento de Exámenes de Retiro con lazy loading"""
    
    # Señales comunes
    progress_updated = Signal(int, str)
    finished = Signal(bool, str, dict)
    
    def __init__(self, archivo_bronze: Path, archivo_cc_actual: Path, 
                 archivo_cc_old: Path, output_dir: Path):
        super().__init__()
        
        self.archivo_bronze = archivo_bronze
        self.archivo_cc_actual = archivo_cc_actual
        self.archivo_cc_old = archivo_cc_old
        self.output_dir = output_dir
        
        # Logger con señales
        self.logger = UILogger(pipeline_name="examen_retiro")
        self.logger.progress_update.connect(self._emit_progress)
        
        # Configurar lazy loader para este ETL
        self.loader = create_etl_loader('examen_retiro', {
            'step1': 'src.modules.examen_retiro.steps.step1_clean',
            'step2': 'src.modules.examen_retiro.steps.step2_gold',
            'step3': 'src.modules.examen_retiro.steps.step3_join'
        })
        
        # Timers
        self.timers = {
            'total': 0,
            'step1': 0,
            'step2': 0,
            'step3': 0
        }
        
        self.resultado = {}
    
    def run(self):
        """Ejecuta el ETL (llamado por QThread.start())"""
        tiempo_inicio_total = time.time()
        
        try:
            self.logger.info("="*70)
            self.logger.info("🚀 Iniciando ETL: Exámenes de Retiro")
            self.logger.info("="*70)
            self.logger.info(f"📂 Archivo Bronze: {self.archivo_bronze.name}")
            self.logger.info(f"📊 CC_ACTUAL: {self.archivo_cc_actual.name}")
            self.logger.info(f"📊 CC_OLD: {self.archivo_cc_old.name}")
            self.logger.info(f"📁 Directorio salida: {self.output_dir}")
            self.logger.info("")
            
            # Ejecutar ETL completo
            self.resultado = self.execute_etl()
            
            self.timers['total'] = time.time() - tiempo_inicio_total
            
            if 'timers' not in self.resultado:
                self.resultado['timers'] = {}
            if 'total' not in self.resultado['timers']:
                self.resultado['timers']['total'] = self.timers['total']
            
            # Procesar resultado
            if self.resultado.get('success', False):
                self._log_success_summary()
                mensaje = self.resultado.get('mensaje', '✅ ETL completado exitosamente')
                self.finished.emit(True, mensaje, self.resultado)
            else:
                self._log_error_summary()
                mensaje = self._build_user_error_message(self.resultado)
                self.finished.emit(False, mensaje, self.resultado)
                
        except Exception as e:
            self.timers['total'] = time.time() - tiempo_inicio_total
            
            self.logger.log_error_details(e, "ejecución del ETL")
            
            self.finished.emit(
                False,
                f"❌ Error crítico: {str(e)}",
                {
                    'success': False,
                    'error': str(e),
                    'error_details': self._build_error_details(
                        stage_name='Ejecución ETL Exámenes de Retiro',
                        error=e
                    ),
                    'timers': {'total': self.timers['total']}
                }
            )
    
    def execute_etl(self) -> Dict:
        """
        Ejecuta el ETL completo de Exámenes de Retiro:
        Step 1: Extracción y limpieza (Bronze → Silver)
        Step 2: Transformación y filtrado (Silver → Gold)
        Step 3: Enriquecimiento con CC (Gold → Gold Enriquecido)
        
        Returns:
            dict con resultados del proceso
        """
        resultado = {}
        
        try:
            # Preflight / Validate Source (antes de cualquier stage)
            self.progress_updated.emit(2, "🔎 Preflight: validando archivo fuente...")
            self.logger.info("🔎 PRE-FLIGHT: validando contrato de fuente...")
            preflight = validate_all_sources_for_etl("examen_retiro", self.archivo_bronze)
            preflight.raise_if_failed()
            self.logger.info(f"✓ Preflight válido ({self.archivo_bronze.name})")
            self.progress_updated.emit(4, "✓ Preflight completado")

            # ============ STEP 1: Bronze → Silver ============
            self.logger.info("="*70)
            self.logger.info("STEP 1: EXTRACCIÓN Y LIMPIEZA (Bronze → Silver)")
            self.logger.info("="*70)
            
            self.progress_updated.emit(5, "🔥 Iniciando extracción...")
            
            tiempo_inicio_step1 = time.time()
            
            try:
                self.logger.info(f"Archivo a procesar: {self.archivo_bronze.name}")
                
                self.progress_updated.emit(10, "🔥 Cargando módulo de extracción...")
                
                # LAZY LOADING: step1
                extraer_bronze_examenes_retiro = self.loader.step1.extraer_bronze_examenes_retiro
                limpiar_silver_examenes_retiro = self.loader.step1.limpiar_silver_examenes_retiro
                guardar_resultados_step1 = self.loader.step1.guardar_resultados
                
                self.logger.info("✓ Módulo step1 cargado exitosamente")
                
                self.progress_updated.emit(15, "📄 Extrayendo datos de Excel...")
                
                # Extraer Bronze
                df_bronze = extraer_bronze_examenes_retiro(self.archivo_bronze)
                
                if df_bronze.is_empty():
                    return {
                        'success': False,
                        'error': 'No se encontraron datos en el archivo Excel',
                        'error_details': self._build_error_details(
                            stage_name='Step 1: Bronze → Silver',
                            error='No se encontraron datos en el archivo Excel',
                            stage_index=1,
                            total_stages=3,
                            module_path='src.modules.examen_retiro.steps.step1_clean'
                        ),
                        'timers': self.timers
                    }
                
                self.progress_updated.emit(25, "🧹 Limpiando datos Silver...")
                
                # Limpiar Silver
                df_silver = limpiar_silver_examenes_retiro(df_bronze)
                
                self.progress_updated.emit(35, "💾 Guardando archivo Silver...")
                
                # Guardar Silver
                ruta_parquet_silver = guardar_resultados_step1(
                    df_silver, 
                    self.output_dir
                )
                
                self.timers['step1'] = time.time() - tiempo_inicio_step1
                
                resultado['step1'] = {
                    'dataframe': df_silver,
                    'parquet': ruta_parquet_silver,
                    'registros': len(df_silver),
                    'columnas': len(df_silver.columns),
                    'duracion': self.timers['step1']
                }
                
                self.logger.info("-"*70)
                self.logger.info(f"✓ Step 1 completado exitosamente")
                self.logger.info(f"  • Registros: {len(df_silver):,}")
                self.logger.info(f"  • Columnas: {len(df_silver.columns)}")
                self.logger.info(f"  • Parquet: {ruta_parquet_silver.name}")
                self.logger.info(f"  ⏱️ Duración: {self.logger.format_duration(self.timers['step1'])}")
                self.logger.info("-"*70)
                
                self.progress_updated.emit(40, f"✓ Silver generado: {len(df_silver):,} registros")
                
            except ImportError as e:
                self.logger.error(f"❌ No se pudo importar step1: {e}")
                return {
                    'success': False,
                    'error': f'No se encontró examen_retiro/step1_clean.py: {e}',
                    'error_details': self._build_error_details(
                        stage_name='Step 1: Bronze → Silver',
                        error=e,
                        stage_index=1,
                        total_stages=3,
                        module_path='src.modules.examen_retiro.steps.step1_clean'
                    ),
                    'timers': self.timers
                }
            except Exception as e:
                self.logger.error(f"❌ Error en Step 1: {e}")
                self.logger.error(traceback.format_exc())
                return {
                    'success': False,
                    'error': f'Error en extracción Bronze→Silver: {str(e)}',
                    'error_details': self._build_error_details(
                        stage_name='Step 1: Bronze → Silver',
                        error=e,
                        stage_index=1,
                        total_stages=3,
                        module_path='src.modules.examen_retiro.steps.step1_clean'
                    ),
                    'timers': self.timers
                }
            
            # ============ STEP 2: Silver → Gold ============
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info("STEP 2: TRANSFORMACIÓN (Silver → Gold)")
            self.logger.info("="*70)
            
            self.progress_updated.emit(45, "🔍 Buscando esquema...")
            
            tiempo_inicio_step2 = time.time()
            
            try:
                esquema_path = resolve_structured_path("assets/esquemas/esquema_examen_retiro")
                
                if not esquema_path.exists():
                    self.logger.warning("⚠️ Esquema no encontrado, saltando Step 2")
                    self.logger.warning(f"   Ruta esperada: {esquema_path}")
                    self.progress_updated.emit(100, "✓ Silver completado (sin Gold)")
                    resultado['step2'] = {'warning': 'Esquema no encontrado'}
                else:
                    self.logger.info(f"✓ Esquema encontrado: {esquema_path.name}")
                    
                    # Cargar esquema YAML
                    esquema = load_structured_data(esquema_path, prefer_resource_path=False)
                    
                    self.logger.info(f"✓ Esquema cargado: v{esquema['metadata']['version']}")
                    self.logger.info(f"  • Columnas esperadas: {len(esquema['schema'])}")
                    
                    self.progress_updated.emit(50, "📊 Cargando datos Silver...")
                    
                    # Leer datos silver
                    import polars as pl
                    df_silver = pl.read_parquet(ruta_parquet_silver)
                    
                    self.logger.info(f"✓ Datos silver cargados: {len(df_silver):,} registros")
                    
                    self.progress_updated.emit(55, "⚙️ Cargando módulo de transformación...")
                    
                    # LAZY LOADING: step2
                    transformar_silver_a_gold = self.loader.step2.transformar_silver_a_gold
                    guardar_resultados_step2 = self.loader.step2.guardar_resultados
                    
                    self.logger.info("✓ Módulo step2 cargado exitosamente")
                    
                    self.progress_updated.emit(60, "🔄 Aplicando transformaciones Gold...")
                    
                    # Transformar a gold (selecciona columnas, aplica tipos, genera derivadas, filtra)
                    df_gold = transformar_silver_a_gold(df_silver, esquema)
                    
                    self.logger.info(f"✓ Transformaciones aplicadas")
                    self.logger.info(f"  • Registros finales: {len(df_gold):,}")
                    self.logger.info(f"  • Columnas finales: {len(df_gold.columns)}")
                    
                    self.progress_updated.emit(65, "💾 Guardando archivos Gold...")
                    
                    # Guardar gold
                    carpeta_silver = ruta_parquet_silver.parent
                    ruta_parquet_gold_actual, ruta_excel_gold_actual, ruta_parquet_gold_historico, ruta_excel_gold_historico = guardar_resultados_step2(
                        df_gold,
                        carpeta_silver
                    )
                    
                    self.timers['step2'] = time.time() - tiempo_inicio_step2
                    
                    resultado['step2'] = {
                        'registros': len(df_gold),
                        'columnas': len(df_gold.columns),
                        'parquet_gold': ruta_parquet_gold_actual,
                        'excel_gold': ruta_excel_gold_actual,
                        'parquet_historico': ruta_parquet_gold_historico,
                        'excel_historico': ruta_excel_gold_historico,
                        'duracion': self.timers['step2']
                    }
                    
                    self.logger.info("-"*70)
                    self.logger.info(f"✓ Step 2 completado exitosamente")
                    self.logger.info(f"  • Registros Gold: {len(df_gold):,}")
                    self.logger.info(f"  • Columnas Gold: {len(df_gold.columns)}")
                    self.logger.info(f"  • Parquet: {ruta_parquet_gold_actual.name}")
                    self.logger.info(f"  ⏱️ Duración: {self.logger.format_duration(self.timers['step2'])}")
                    self.logger.info("-"*70)
                    
                    self.progress_updated.emit(70, f"✓ Gold generado: {len(df_gold):,} registros")
                
            except ImportError as e:
                self.logger.warning(f"⚠️ Step 2 no disponible: {e}")
                self.progress_updated.emit(100, "✓ Silver completado (Step 2 no disponible)")
                resultado['step2'] = {'warning': f'Step 2 no implementado: {e}'}
            except Exception as e:
                self.logger.error(f"❌ Error en Step 2: {e}")
                self.logger.error(traceback.format_exc())
                resultado['step2'] = {
                    'error': str(e),
                    'error_details': self._build_error_details(
                        stage_name='Step 2: Silver → Gold',
                        error=e,
                        stage_index=2,
                        total_stages=3,
                        module_path='src.modules.examen_retiro.steps.step2_gold'
                    )
                }
                # No retornar error aquí, silver ya fue generado
            
            # ============ STEP 3: Gold → Gold Enriquecido (JOIN con CC) ============
            self.logger.info("")
            self.logger.info("="*70)
            self.logger.info("STEP 3: ENRIQUECIMIENTO (Gold → Gold Enriquecido con CC)")
            self.logger.info("="*70)
            
            self.progress_updated.emit(75, "🔍 Preparando JOIN con Centros de Costo...")
            
            tiempo_inicio_step3 = time.time()
            
            try:
                # Verificar que tenemos el archivo gold del step2
                if 'step2' not in resultado or 'parquet_gold' not in resultado['step2']:
                    self.logger.warning("⚠️ No se puede ejecutar Step 3: falta output de Step 2")
                    resultado['step3'] = {'warning': 'Step 2 no completado'}
                else:
                    ruta_gold = resultado['step2']['parquet_gold']
                    
                    self.logger.info(f"✓ Archivo Gold: {ruta_gold.name}")
                    self.logger.info(f"✓ CC_ACTUAL: {self.archivo_cc_actual.name}")
                    self.logger.info(f"✓ CC_OLD: {self.archivo_cc_old.name}")
                    
                    self.progress_updated.emit(78, "⚙️ Cargando módulo de JOIN...")
                    
                    # LAZY LOADING: step3
                    cargar_parquets = self.loader.step3.cargar_parquets
                    ejecutar_join_sql = self.loader.step3.ejecutar_join_sql
                    analizar_resultados = self.loader.step3.analizar_resultados
                    guardar_resultados_step3 = self.loader.step3.guardar_resultados
                    
                    self.logger.info("✓ Módulo step3 cargado exitosamente")
                    
                    self.progress_updated.emit(80, "📄 Cargando query SQL...")
                    
                    # Buscar y cargar query SQL usando get_resource_path
                    try:
                        from src.utils.paths import get_resource_path
                        ruta_query = get_resource_path("assets/queries/query_cc_join.sql")
                        
                        if not ruta_query.exists():
                            raise FileNotFoundError(f"No se encontró query_cc_join.sql en {ruta_query.parent}")
                        
                        with open(ruta_query, 'r', encoding='utf-8') as f:
                            query = f.read()
                        
                        self.logger.info(f"✓ Query SQL cargada ({len(query)} caracteres)")
                        
                    except FileNotFoundError as e:
                        self.logger.error(f"❌ {str(e)}")
                        resultado['step3'] = {
                            'error': 'Query SQL no encontrada',
                            'error_details': self._build_error_details(
                                stage_name='Step 3: Gold → Gold Enriquecido',
                                error=e,
                                stage_index=3,
                                total_stages=3,
                                module_path='assets/queries/query_cc_join.sql'
                            )
                        }
                        raise
                    
                    self.progress_updated.emit(82, "📊 Cargando parquets...")
                    
                    # Cargar los 3 parquets
                    df_examenes, df_cc_actual, df_cc_old = cargar_parquets(
                        ruta_gold,
                        self.archivo_cc_actual,
                        self.archivo_cc_old
                    )
                    
                    self.logger.info(f"✓ Parquets cargados exitosamente")
                    
                    self.progress_updated.emit(85, "🔄 Ejecutando JOIN con DuckDB...")
                    
                    # Ejecutar JOIN
                    df_resultado = ejecutar_join_sql(df_examenes, df_cc_actual, df_cc_old, query)
                    
                    self.logger.info(f"✓ JOIN ejecutado: {len(df_resultado):,} registros")
                    
                    self.progress_updated.emit(90, "📈 Analizando resultados...")
                    
                    # Analizar resultados
                    stats = analizar_resultados(df_resultado)
                    
                    self.progress_updated.emit(93, "💾 Guardando Gold Enriquecido...")
                    
                    # Guardar resultados
                    ruta_parquet_enriq_actual, ruta_excel_enriq_actual, ruta_parquet_enriq_historico, ruta_excel_enriq_historico = guardar_resultados_step3(
                        df_resultado,
                        ruta_gold,
                        stats
                    )
                    
                    self.timers['step3'] = time.time() - tiempo_inicio_step3
                    
                    resultado['step3'] = {
                        'registros': len(df_resultado),
                        'columnas': len(df_resultado.columns),
                        'stats': stats,
                        'parquet_enriquecido': ruta_parquet_enriq_actual,
                        'excel_enriquecido': ruta_excel_enriq_actual,
                        'parquet_historico': ruta_parquet_enriq_historico,
                        'excel_historico': ruta_excel_enriq_historico,
                        'duracion': self.timers['step3']
                    }
                    
                    self.logger.info("-"*70)
                    self.logger.info(f"✓ Step 3 completado exitosamente")
                    self.logger.info(f"  • Registros enriquecidos: {len(df_resultado):,}")
                    self.logger.info(f"  • OK: {stats['ok']:,} ({stats['ok']/stats['total']*100:.1f}%)")
                    self.logger.info(f"  • Sin código: {stats['sin_codigo']:,}")
                    self.logger.info(f"  • No encontrado: {stats['no_encontrado']:,}")
                    self.logger.info(f"  • Parquet: {ruta_parquet_enriq_actual.name}")
                    self.logger.info(f"  ⏱️ Duración: {self.logger.format_duration(self.timers['step3'])}")
                    self.logger.info("-"*70)
                    
                    self.progress_updated.emit(100, f"✓ Gold Enriquecido: {len(df_resultado):,} registros")
                
            except ImportError as e:
                self.logger.warning(f"⚠️ Step 3 no disponible: {e}")
                self.progress_updated.emit(100, "✓ Gold completado (Step 3 no disponible)")
                resultado['step3'] = {'warning': f'Step 3 no implementado: {e}'}
            except Exception as e:
                self.logger.error(f"❌ Error en Step 3: {e}")
                self.logger.error(traceback.format_exc())
                resultado['step3'] = {
                    'error': str(e),
                    'error_details': self._build_error_details(
                        stage_name='Step 3: Gold → Gold Enriquecido',
                        error=e,
                        stage_index=3,
                        total_stages=3,
                        module_path='src.modules.examen_retiro.steps.step3_join'
                    )
                }
                # No retornar error aquí, gold ya fue generado
            
            # ============ RESULTADO FINAL ============
            self.timers['total'] = time.time() - tiempo_inicio_step1
            
            resultado['success'] = True
            resultado['timers'] = self.timers
            
            # Mensaje resumen
            if 'step3' in resultado and 'registros' in resultado['step3']:
                stats = resultado['step3']['stats']
                mensaje = (
                    f"ETL completado exitosamente:\n"
                    f"  • Silver: {resultado['step1']['registros']:,} registros\n"
                    f"  • Gold: {resultado['step2']['registros']:,} registros\n"
                    f"  • Gold Enriquecido: {resultado['step3']['registros']:,} registros\n"
                    f"    - Enriquecidos OK: {stats['ok']:,} ({stats['ok']/stats['total']*100:.1f}%)\n"
                    f"  ⏱️ Tiempo total: {self.logger.format_duration(self.timers['total'])}\n"
                    f"    - Step 1: {self.logger.format_duration(self.timers['step1'])}\n"
                    f"    - Step 2: {self.logger.format_duration(self.timers['step2'])}\n"
                    f"    - Step 3: {self.logger.format_duration(self.timers['step3'])}"
                )
            elif 'step2' in resultado and 'registros' in resultado['step2']:
                mensaje = (
                    f"ETL completado (sin Step 3):\n"
                    f"  • Silver: {resultado['step1']['registros']:,} registros\n"
                    f"  • Gold: {resultado['step2']['registros']:,} registros\n"
                    f"  ⏱️ Tiempo total: {self.logger.format_duration(self.timers['total'])}"
                )
            else:
                mensaje = (
                    f"Procesamiento Silver completado:\n"
                    f"  • Registros: {resultado['step1']['registros']:,}\n"
                    f"  ⏱️ Tiempo total: {self.logger.format_duration(self.timers['total'])}"
                )
            
            resultado['mensaje'] = mensaje
            self.logger.info("\n" + mensaje)
            
            # Módulos cargados
            modulos_cargados = self.loader.get_loaded_modules()
            self.logger.info(f"\n📦 Módulos cargados: {', '.join(modulos_cargados)}")
            
            return resultado

        except SourceValidationError as e:
            self.logger.error(str(e))
            return {
                'success': False,
                'error': str(e),
                'error_details': self._build_error_details(
                    stage_name='Preflight / Validate Source',
                    error=e,
                    stage_index=1,
                    total_stages=3,
                    module_path='src.utils.validate_source',
                ),
                'timers': self.timers
            }
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico en ETL: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            return {
                'success': False,
                'error': str(e),
                'error_details': self._build_error_details(
                    stage_name='ETL completo Exámenes de Retiro',
                    error=e
                ),
                'timers': self.timers
            }
    
    def _log_success_summary(self):
        """Registra resumen de éxito"""
        self.logger.info("\n" + "="*70)
        self.logger.info("✅ ETL COMPLETADO EXITOSAMENTE".center(70))
        self.logger.info("="*70)
    
    def _log_error_summary(self):
        """Registra resumen de error"""
        self.logger.info("\n" + "="*70)
        self.logger.error("ETL FINALIZADO CON ERRORES".center(70))
        self.logger.info("="*70)
        
        error_msg = self.resultado.get('error', 'Error desconocido')
        self.logger.error(f"Causa: {error_msg}")

        error_details = self.resultado.get('error_details', {})
        if isinstance(error_details, dict) and error_details:
            stage_name = error_details.get('stage_name')
            stage_index = error_details.get('stage_index')
            total_stages = error_details.get('total_stages')
            module_path = error_details.get('module_path')
            detail_error = error_details.get('error_message')

            if stage_name:
                if stage_index and total_stages:
                    self.logger.error(f"Etapa fallida: {stage_index}/{total_stages} - {stage_name}")
                else:
                    self.logger.error(f"Etapa fallida: {stage_name}")

            if module_path:
                self.logger.error(f"Origen: {module_path}")

            if detail_error and detail_error != error_msg:
                self.logger.error(f"Detalle técnico: {detail_error}")

    def _build_user_error_message(self, resultado: Dict[str, Any]) -> str:
        """Construye mensaje de error con contexto técnico."""
        error = str(resultado.get('error', 'Error desconocido'))
        lines = [f"❌ Error en ETL: {error}"]

        details = resultado.get('error_details', {})
        if isinstance(details, dict) and details:
            stage_name = details.get('stage_name')
            stage_index = details.get('stage_index')
            total_stages = details.get('total_stages')
            module_path = details.get('module_path')
            detail_error = details.get('error_message')
            tb_excerpt = details.get('traceback_excerpt', [])

            if stage_name:
                if stage_index and total_stages:
                    lines.append(f"Etapa: {stage_index}/{total_stages} - {stage_name}")
                else:
                    lines.append(f"Etapa: {stage_name}")

            if module_path:
                lines.append(f"Módulo: {module_path}")

            if detail_error and detail_error != error:
                lines.append(f"Detalle: {detail_error}")

            if isinstance(tb_excerpt, list) and tb_excerpt:
                lines.append(f"Traceback: {tb_excerpt[0]}")

        log_path = self.logger.get_log_path()
        if log_path:
            lines.append(f"Log: {log_path}")

        return "\n".join(lines)

    @staticmethod
    def _build_error_details(
        stage_name: str,
        error: Union[Exception, str],
        stage_index: Optional[int] = None,
        total_stages: Optional[int] = None,
        module_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Construye payload estándar de error."""
        traceback_text = traceback.format_exc()
        if traceback_text.strip() == "NoneType: None":
            traceback_text = ""
        tb_lines = [line for line in traceback_text.splitlines() if line.strip()]

        details: Dict[str, Any] = {
            'stage_name': stage_name,
            'error_message': str(error)
        }

        if isinstance(error, Exception):
            details['exception_type'] = type(error).__name__
        if stage_index is not None:
            details['stage_index'] = stage_index
        if total_stages is not None:
            details['total_stages'] = total_stages
        if module_path:
            details['module_path'] = module_path
        if tb_lines:
            details['traceback_excerpt'] = tb_lines[:8]

        return details
    
    def _emit_progress(self, percentage: int, message: str):
        """Callback del logger para progreso"""
        self.progress_updated.emit(percentage, message)
