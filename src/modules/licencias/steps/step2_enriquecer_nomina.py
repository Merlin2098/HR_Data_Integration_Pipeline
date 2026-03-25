"""
Script: step2_enriquecer_nomina.py
Descripción: Enriquece la nómina Gold con datos de licencias
             Lee nómina Gold + licencias Silver y genera Gold enriquecida
             
Arquitectura:
- Input 1: /gold/nomina/actual/Planilla_Metso_Consolidado.parquet
- Input 2: /silver/licencias_consolidadas.parquet
- Output: /gold/nomina/actual/Planilla Metso BI_Gold_Con_Licencias.parquet|xlsx

Columnas agregadas:
- MOTIVO_CON_GOCE: Motivos de licencias con goce (concatenados con " | ")
- MOTIVO_SIN_GOCE: Motivos de licencias sin goce (concatenados con " | ")

Autor: Richi via Claude
Fecha: 26.01.2026
"""

import polars as pl
import duckdb
from pathlib import Path
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


def seleccionar_archivo_parquet(titulo: str) -> Path | None:
    """Abre diálogo para seleccionar archivo Parquet"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title=titulo,
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def enriquecer_nomina_con_licencias(
    ruta_nomina: Path,
    ruta_licencias: Path
) -> pl.DataFrame:
    """
    Enriquece la nómina Gold con datos de licencias usando DuckDB.
    
    Args:
        ruta_nomina: Path al parquet de nómina Gold
        ruta_licencias: Path al parquet de licencias Silver
        
    Returns:
        DataFrame enriquecido con columnas MOTIVO_CON_GOCE y MOTIVO_SIN_GOCE
    """
    print("\n🔄 Enriqueciendo nómina con datos de licencias...")
    
    # Cargar query SQL
    ruta_query = get_resource_path("assets/queries/query_licencias_agregadas.sql")
    
    if not ruta_query.exists():
        raise FileNotFoundError(f"No se encontró el archivo de query SQL: {ruta_query}")
    
    with open(ruta_query, 'r', encoding='utf-8') as f:
        query_sql = f.read()
    
    print(f"   ✓ Query SQL cargada: {ruta_query.name}")
    
    # Cargar DataFrames
    print(f"   → Cargando nómina Gold...")
    df_nomina = pl.read_parquet(ruta_nomina)
    registros_nomina = len(df_nomina)
    print(f"   ✓ {registros_nomina:,} registros de nómina")
    
    print(f"   → Cargando licencias Silver...")
    df_licencias = pl.read_parquet(ruta_licencias)
    registros_licencias = len(df_licencias)
    print(f"   ✓ {registros_licencias:,} registros de licencias")
    
    # Ejecutar query con DuckDB
    print(f"   → Ejecutando agregación y join...")
    
    con = duckdb.connect(':memory:')
    
    # Registrar DataFrames en DuckDB
    con.register('nomina', df_nomina.to_arrow())
    con.register('licencias', df_licencias.to_arrow())
    
    # Ejecutar query
    resultado = con.execute(query_sql).fetch_arrow_table()
    df_enriquecido = pl.from_arrow(resultado)
    
    con.close()
    
    # Estadísticas
    registros_con_goce = df_enriquecido.filter(
        pl.col("MOTIVO_CON_GOCE").is_not_null()
    ).height
    
    registros_sin_goce = df_enriquecido.filter(
        pl.col("MOTIVO_SIN_GOCE").is_not_null()
    ).height
    
    print(f"   ✓ Enriquecimiento completado")
    print(f"\n📊 Estadísticas de licencias:")
    print(f"   - Total registros nómina: {len(df_enriquecido):,}")
    print(f"   - Con licencias CON GOCE: {registros_con_goce:,}")
    print(f"   - Con licencias SIN GOCE: {registros_sin_goce:,}")
    
    return df_enriquecido


def guardar_resultados(
    df_enriquecido: pl.DataFrame,
    ruta_nomina: Path,
    export_excel: bool = False,
):
    """
    Guarda el DataFrame enriquecido en Gold solo en actual/.
    La estructura se crea en la misma ubicación del archivo de nómina.
    
    Args:
        df_enriquecido: DataFrame a guardar
        ruta_nomina: Path del archivo de nómina Gold (para obtener carpeta base)
    """
    # Obtener carpeta base desde el archivo de nómina
    # Asumiendo ruta: .../gold/nomina/actual/Planilla_Metso_Consolidado.parquet
    carpeta_actual = ruta_nomina.parent  # .../gold/nomina/actual
    carpeta_nomina = carpeta_actual.parent  # .../gold/nomina
    
    print(f"\n💾 Guardando resultados en Gold...")
    print(f"  📁 Carpeta: {carpeta_nomina}")
    
    nombre_base = "Planilla Metso BI_Gold_Con_Licencias"
    
    # Archivo actual (sin timestamp para Power BI)
    print(f"\n  - Guardando actual/parquet...", end='', flush=True)
    ruta_parquet_actual = carpeta_actual / f"{nombre_base}.parquet"
    df_enriquecido.write_parquet(ruta_parquet_actual, compression="snappy")
    print(f" ✓")
    print(f"    Ubicación: actual/{ruta_parquet_actual.name}")
    
    # Archivo actual Excel
    ruta_excel_actual = carpeta_actual / f"{nombre_base}.xlsx"
    ruta_excel_actual = maybe_write_excel(
        ruta_excel_actual,
        export_excel,
        lambda path: df_enriquecido.write_excel(path),
    )
    if ruta_excel_actual is not None:
        print(f"  - Guardando actual/excel...", end='', flush=True)
        print(f" ✓")
        print(f"    Ubicación: actual/{ruta_excel_actual.name}")
    else:
        print("  - Excel omitido (exportación opcional desactivada)")

    return {
        'parquet_actual': ruta_parquet_actual,
        'excel': ruta_excel_actual,
    }


def main():
    """Función principal de procesamiento"""
    print("=" * 80)
    print(" ENRIQUECIMIENTO NÓMINA CON LICENCIAS - GOLD ".center(80, "="))
    print("=" * 80)
    
    # 1. Seleccionar archivo de nómina Gold
    print("\n[PASO 1/2] Selecciona el archivo de Nómina Gold (Planilla_Metso_Consolidado.parquet)...")
    ruta_nomina = seleccionar_archivo_parquet(
        "Seleccionar Nómina Gold - Planilla_Metso_Consolidado.parquet"
    )
    
    if not ruta_nomina:
        print("✗ No se seleccionó archivo de nómina. Proceso cancelado.")
        return
    
    print(f"✓ Nómina seleccionada: {ruta_nomina.name}")
    
    # 2. Seleccionar archivo de licencias Silver
    print("\n[PASO 2/2] Selecciona el archivo de Licencias Silver (licencias_consolidadas.parquet)...")
    ruta_licencias = seleccionar_archivo_parquet(
        "Seleccionar Licencias Silver - licencias_consolidadas.parquet"
    )
    
    if not ruta_licencias:
        print("✗ No se seleccionó archivo de licencias. Proceso cancelado.")
        return
    
    # Iniciar cronómetro después de la selección
    tiempo_inicio = time.time()
    
    print(f"✓ Licencias seleccionadas: {ruta_licencias.name}")
    
    # 3. Procesar datos
    print("\n" + "=" * 80)
    print(" PROCESAMIENTO ".center(80, "="))
    print("=" * 80)
    
    try:
        # Enriquecer nómina con licencias
        df_enriquecido = enriquecer_nomina_con_licencias(
            ruta_nomina,
            ruta_licencias
        )
        
        # Guardar resultados
        rutas = guardar_resultados(df_enriquecido, ruta_nomina)
        
        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio
        
        # 4. Resumen final
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Procesamiento completado exitosamente")
        
        print(f"\n📂 Archivos generados:")
        print(f"  - actual/Planilla Metso BI_Gold_Con_Licencias.parquet")
        if rutas['excel'] is not None:
            print(f"  - actual/Planilla Metso BI_Gold_Con_Licencias.xlsx")
        
        print(f"\n⏱️  Tiempo de ejecución: {tiempo_total:.2f}s")
        
        print("\n💡 El archivo en actual/ se sobreescribe (para Power BI)")
        
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
    ruta_nomina: Path,
    ruta_licencias: Path,
    export_excel_gold: bool = False,
) -> dict:
    """
    Enriquece nómina Gold con licencias sin interfaz gráfica (modo headless)
    Usado por el pipeline executor
    
    Args:
        ruta_nomina: Path al parquet de nómina Gold
        ruta_licencias: Path al parquet de licencias Silver
        
    Returns:
        dict con resultados del procesamiento
    """
    print(f"\n🔄 Enriqueciendo nómina con licencias (modo headless)...")
    print(f"   Nómina: {ruta_nomina.name}")
    print(f"   Licencias: {ruta_licencias.name}")
    
    try:
        # Cargar query SQL
        ruta_query = get_resource_path("assets/queries/query_licencias_agregadas.sql")
        
        if not ruta_query.exists():
            raise FileNotFoundError(f"No se encontró query SQL: {ruta_query}")
        
        with open(ruta_query, 'r', encoding='utf-8') as f:
            query_sql = f.read()
        
        # Cargar DataFrames
        df_nomina = pl.read_parquet(ruta_nomina)
        df_licencias = pl.read_parquet(ruta_licencias)
        
        registros_nomina = len(df_nomina)
        registros_licencias = len(df_licencias)
        
        print(f"   ✓ Nómina cargada: {registros_nomina:,} registros")
        print(f"   ✓ Licencias cargadas: {registros_licencias:,} registros")
        
        # Ejecutar query con DuckDB
        con = duckdb.connect(':memory:')
        
        con.register('nomina', df_nomina.to_arrow())
        con.register('licencias', df_licencias.to_arrow())
        
        resultado = con.execute(query_sql).fetch_arrow_table()
        df_enriquecido = pl.from_arrow(resultado)
        
        con.close()
        
        # Estadísticas
        registros_con_goce = df_enriquecido.filter(
            pl.col("MOTIVO_CON_GOCE").is_not_null()
        ).height
        
        registros_sin_goce = df_enriquecido.filter(
            pl.col("MOTIVO_SIN_GOCE").is_not_null()
        ).height
        
        print(f"   ✓ Enriquecimiento completado: {len(df_enriquecido):,} registros")
        print(f"   ✓ Con licencias CON GOCE: {registros_con_goce:,}")
        print(f"   ✓ Con licencias SIN GOCE: {registros_sin_goce:,}")
        
        # Guardar resultados
        # Obtener carpeta base desde archivo de nómina
        carpeta_actual = ruta_nomina.parent
        
        nombre_base = "Planilla Metso BI_Gold_Con_Licencias"
        
        # Archivo actual (sin timestamp)
        ruta_parquet_actual = carpeta_actual / f"{nombre_base}.parquet"
        df_enriquecido.write_parquet(ruta_parquet_actual, compression="snappy")
        
        ruta_excel_actual = carpeta_actual / f"{nombre_base}.xlsx"
        ruta_excel_actual = maybe_write_excel(
            ruta_excel_actual,
            export_excel_gold,
            lambda path: df_enriquecido.write_excel(path),
        )
        
        print(f"   ✓ Parquet actual: {ruta_parquet_actual.name}")
        if ruta_excel_actual is not None:
            print(f"   ✓ Excel: {ruta_excel_actual.name}")
        else:
            print("   ℹ️ Excel omitido (exportación opcional desactivada)")
        
        return {
            'success': True,
            'parquet_actual': ruta_parquet_actual,
            'excel': ruta_excel_actual,
            'registros': len(df_enriquecido),
            'registros_con_goce': registros_con_goce,
            'registros_sin_goce': registros_sin_goce
        }
        
    except Exception as e:
        print(f"   ✗ Error: {e}")
        raise
