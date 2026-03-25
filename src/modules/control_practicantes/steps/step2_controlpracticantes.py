"""
Script: step2_gold.py
Descripción: Genera capa Gold de control de practicantes con tiempo de servicio y flags
             Lee Silver y genera Gold con columnas enriquecidas
             
Arquitectura:
- Input: /silver/control_practicantes_silver.parquet
- Output: /gold/control_practicantes_gold.parquet|xlsx

Columnas agregadas:
- anios_servicio: Años completos de servicio
- meses_servicio: Meses adicionales de servicio
- dias_servicio: Días totales de servicio
- flag_por_cumplir_1_anio: Por cumplir 1 año (falta <= 1 mes)
- flag_cumplio_1_anio: Ya cumplió 1 año
- flag_por_cumplir_2_anios: Por cumplir 2 años (falta <= 3 meses)

Autor: Richi via Claude
Fecha: 27.01.2026
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime
import time
import sys
from tkinter import Tk, filedialog

from src.utils.gold_export import maybe_write_excel


def get_resource_path(relative_path: str) -> Path:
    """
    Obtiene la ruta absoluta de un recurso, manejando tanto
    ejecución standalone como PyInstaller.
    """
    try:
        # PyInstaller crea una carpeta temporal _MEIPASS
        base_path = Path(sys._MEIPASS)
    except AttributeError:
        # Ejecución normal desde el directorio del script
        base_path = Path(__file__).resolve().parents[4]

    return base_path / relative_path


def seleccionar_archivo_parquet() -> Path | None:
    """Abre diálogo para seleccionar archivo Parquet"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title="Seleccionar control_practicantes_silver.parquet",
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def generar_gold_con_flags(ruta_silver: Path) -> pl.DataFrame:
    """
    Genera capa Gold con tiempo de servicio y flags usando DuckDB.
    
    Args:
        ruta_silver: Path al parquet Silver
        
    Returns:
        DataFrame enriquecido con columnas de tiempo y flags
    """
    print("\n📊 Generando capa Gold con flags de evaluación...")
    
    # Cargar query SQL
    ruta_query = get_resource_path("assets/queries/query_control_practicantes_gold.sql")
    
    if not ruta_query.exists():
        raise FileNotFoundError(f"No se encontró el archivo de query SQL: {ruta_query}")
    
    with open(ruta_query, 'r', encoding='utf-8') as f:
        query_sql = f.read()
    
    print(f"   ✓ Query SQL cargada: {ruta_query.name}")
    
    # Cargar DataFrame Silver
    print(f"   → Cargando Silver...")
    df_silver = pl.read_parquet(ruta_silver)
    registros_silver = len(df_silver)
    print(f"   ✓ {registros_silver:,} registros cargados")
    
    # Ejecutar query con DuckDB
    print(f"   → Ejecutando cálculos y evaluación de flags...")
    
    con = duckdb.connect(':memory:')
    
    # Registrar DataFrame en DuckDB
    con.register('control_practicantes_silver', df_silver.to_arrow())
    
    # Ejecutar query
    resultado = con.execute(query_sql).fetch_arrow_table()
    df_gold = pl.from_arrow(resultado)
    
    con.close()
    
    # Estadísticas de flags
    flags_stats = {
        'por_cumplir_1': df_gold.filter(pl.col("por_cumplir_1") == "SI").height,
        'cumplio_1': df_gold.filter(pl.col("cumplio_1") == "SI").height,
        'por_cumplir_2': df_gold.filter(pl.col("por_cumplir_2") == "SI").height
    }

    print(f"   ✓ Capa Gold generada")
    print(f"\n📊 Estadísticas de flags (PRACTICANTE PROFESIONAL):")
    print(f"   - Por cumplir 1 año (falta ≤ 1 mes): {flags_stats['por_cumplir_1']:,}")
    print(f"   - Ya cumplió 1 año: {flags_stats['cumplio_1']:,}")
    print(f"   - Por cumplir 2 años (falta ≤ 3 meses): {flags_stats['por_cumplir_2']:,}")
    
    return df_gold


def guardar_resultados(
    df_gold: pl.DataFrame,
    ruta_silver: Path,
    export_excel: bool = False,
):
    """
    Guarda el DataFrame Gold en archivos Parquet y Excel.
    
    Args:
        df_gold: DataFrame a guardar
        ruta_silver: Path del archivo Silver (para obtener carpeta base)
    """
    # Obtener carpeta base desde el archivo Silver
    carpeta_silver = ruta_silver.parent  # .../silver
    carpeta_base = carpeta_silver.parent  # carpeta de trabajo
    carpeta_gold = carpeta_base / "gold"
    
    carpeta_gold.mkdir(parents=True, exist_ok=True)
    
    print(f"\n💾 Guardando resultados en Gold...")
    print(f"  📁 Carpeta: {carpeta_gold}")
    
    nombre_base = "control_practicantes_flagsgold"
    
    # Guardar Parquet
    print(f"\n  - Guardando parquet...", end='', flush=True)
    ruta_parquet = carpeta_gold / f"{nombre_base}.parquet"
    df_gold.write_parquet(ruta_parquet, compression="snappy")
    print(f" ✓")
    print(f"    Ubicación: {ruta_parquet.name}")
    
    # Guardar Excel
    ruta_excel = carpeta_gold / f"{nombre_base}.xlsx"
    ruta_excel = maybe_write_excel(
        ruta_excel,
        export_excel,
        lambda path: df_gold.write_excel(path),
    )
    if ruta_excel is not None:
        print(f"  - Guardando excel...", end='', flush=True)
        print(f" ✓")
        print(f"    Ubicación: {ruta_excel.name}")
    else:
        print("  - Excel omitido (exportación opcional desactivada)")
    
    print(f"\n📊 Total registros: {len(df_gold):,}")

    return ruta_parquet, ruta_excel


def main():
    """Función principal de procesamiento"""
    print("=" * 80)
    print(" CONTROL DE PRACTICANTES - CAPA GOLD ".center(80, "="))
    print("=" * 80)
    
    # 1. Seleccionar archivo Silver
    print("\n[PASO 1] Selecciona el archivo Silver (control_practicantes_silver.parquet)...")
    ruta_silver = seleccionar_archivo_parquet()
    
    if not ruta_silver:
        print("✗ No se seleccionó ningún archivo. Proceso cancelado.")
        return
    
    # Iniciar cronómetro después de la selección
    tiempo_inicio = time.time()
    
    print(f"✓ Archivo seleccionado: {ruta_silver.name}")
    
    # 2. Procesar datos
    print("\n" + "=" * 80)
    print(" PROCESAMIENTO ".center(80, "="))
    print("=" * 80)
    
    try:
        # Generar capa Gold con flags
        df_gold = generar_gold_con_flags(ruta_silver)
        
        # Guardar resultados
        ruta_parquet, ruta_excel = guardar_resultados(df_gold, ruta_silver)
        
        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio
        
        # 3. Resumen final
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Procesamiento completado exitosamente")
        
        print(f"\n📂 Archivos generados:")
        print(f"  - {ruta_parquet.name}")
        if ruta_excel is not None:
            print(f"  - {ruta_excel.name}")
        
        print(f"\n⏱️  Tiempo de ejecución: {tiempo_total:.2f}s")
        
        print("\n💡 Los archivos se sobreescriben en cada ejecución")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n✗ Error durante el procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n✗ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def procesar_sin_gui(
    ruta_silver: Path,
    carpeta_gold: Path,
    export_excel_gold: bool = False,
) -> dict:
    """
    Genera capa Gold sin interfaz gráfica (modo headless)
    Usado por el pipeline executor
    
    Args:
        ruta_silver: Path al parquet Silver
        carpeta_gold: Path a la carpeta /gold/ donde guardar el resultado
        
    Returns:
        dict con resultados del procesamiento
    """
    print(f"\n📊 Generando capa Gold (modo headless)...")
    print(f"   Silver: {ruta_silver.name}")
    print(f"   Salida: {carpeta_gold}")
    
    try:
        # Cargar query SQL
        ruta_query = get_resource_path("assets/queries/query_control_practicantes_gold.sql")
        
        if not ruta_query.exists():
            raise FileNotFoundError(f"No se encontró query SQL: {ruta_query}")
        
        with open(ruta_query, 'r', encoding='utf-8') as f:
            query_sql = f.read()
        
        # Cargar DataFrame Silver
        df_silver = pl.read_parquet(ruta_silver)
        registros_silver = len(df_silver)
        
        print(f"   ✓ Silver cargado: {registros_silver:,} registros")
        
        # Ejecutar query con DuckDB
        con = duckdb.connect(':memory:')
        
        con.register('control_practicantes_silver', df_silver.to_arrow())
        
        resultado = con.execute(query_sql).fetch_arrow_table()
        df_gold = pl.from_arrow(resultado)
        
        con.close()
        
        # Estadísticas de flags
        flags_stats = {
            'por_cumplir_1': df_gold.filter(pl.col("por_cumplir_1") == "SI").height,
            'cumplio_1': df_gold.filter(pl.col("cumplio_1") == "SI").height,
            'por_cumplir_2': df_gold.filter(pl.col("por_cumplir_2") == "SI").height
        }
        
        print(f"   ✓ Capa Gold generada: {len(df_gold):,} registros")
        print(f"   ✓ Flags - Por cumplir 1 año: {flags_stats['por_cumplir_1']:,}")
        print(f"   ✓ Flags - Cumplió 1 año: {flags_stats['cumplio_1']:,}")
        print(f"   ✓ Flags - Por cumplir 2 años: {flags_stats['por_cumplir_2']:,}")
        
        # Guardar resultados
        carpeta_gold.mkdir(parents=True, exist_ok=True)
        
        nombre_base = "control_practicantes_flagsgold"
        
        # Guardar Parquet
        ruta_parquet = carpeta_gold / f"{nombre_base}.parquet"
        df_gold.write_parquet(ruta_parquet, compression="snappy")
        
        ruta_excel = carpeta_gold / f"{nombre_base}.xlsx"
        ruta_excel = maybe_write_excel(
            ruta_excel,
            export_excel_gold,
            lambda path: df_gold.write_excel(path),
        )
        
        print(f"   ✓ Parquet guardado: {ruta_parquet.name}")
        if ruta_excel is not None:
            print(f"   ✓ Excel guardado: {ruta_excel.name}")
        else:
            print("   ℹ️ Excel omitido (exportación opcional desactivada)")
        
        return {
            'success': True,
            'parquet': ruta_parquet,
            'excel': ruta_excel,
            'registros': len(df_gold),
            'flags': flags_stats
        }
        
    except Exception as e:
        print(f"   ✗ Error: {e}")
        raise
