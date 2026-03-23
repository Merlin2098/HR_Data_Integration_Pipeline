"""
Script: step1.5_centrosdecosto.py
Descripción: Extrae tabla única de Centros de Costo desde BD Silver
             
Arquitectura:
- Input: Silver BD (bd_silver.parquet)
- Output: Sistema de versionado dual
  * /actual: Archivos SIN timestamp (para Power BI - paths estables)
  * /historico: Archivos CON timestamp (para auditoría)

Nota importante:
- SISTEMA DE VERSIONADO DUAL implementado
- /actual: CC_ACTUAL.parquet y .xlsx (sin timestamp, Power BI lo usa)
- /historico: CC_ACTUAL_YYYYMMDD_HHMMSS.parquet y .xlsx (con timestamp)
- Cada ejecución actualiza /actual y crea nueva versión en /historico

Autor: Richi
Fecha: 06.01.2025
Actualizado: 08.01.2026 - Sistema de versionado dual
"""

import polars as pl
from pathlib import Path
from datetime import datetime
from tkinter import Tk, filedialog

from src.utils.structured_config import (
    find_first_structured_path,
    load_structured_data,
    structured_filetypes,
)


def seleccionar_archivo(titulo: str, tipos: list) -> Path:
    """
    Abre un diálogo para seleccionar un archivo.
    
    Args:
        titulo: Título de la ventana
        tipos: Lista de tuplas (descripción, extensión)
    
    Returns:
        Path del archivo seleccionado
    """
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title=titulo,
        filetypes=tipos
    )
    
    root.destroy()
    
    if not archivo:
        raise ValueError("No se seleccionó ningún archivo")
    
    return Path(archivo)


def buscar_esquema() -> Path | None:
    """Busca el archivo de esquema YAML en ubicaciones comunes."""
    rutas_posibles = [
        Path("../assets/esquemas/esquema_cc"),
        Path("assets/esquemas/esquema_cc"),
        Path("../../assets/esquemas/esquema_cc"),
        Path("esquema_cc"),
    ]
    
    return find_first_structured_path(rutas_posibles, prefer_resource_path=False)


def cargar_esquema(ruta_esquema: Path) -> dict:
    """
    Carga el esquema YAML de centros de costo.
    
    Args:
        ruta_esquema: Path al archivo del esquema
    
    Returns:
        Diccionario con la configuración del esquema
    """
    print(f"\n📋 Cargando esquema: {ruta_esquema.name}")
    
    esquema = load_structured_data(ruta_esquema, prefer_resource_path=False)
    
    print(f"  ✓ Esquema cargado correctamente")
    print(f"  ✓ Columnas requeridas: {len(esquema['columnas_requeridas'])}")
    print(f"  ✓ Columna de deduplicación: {esquema['columna_deduplicacion']}")
    
    return esquema


def cargar_parquet_silver(ruta_parquet: Path) -> pl.DataFrame:
    """
    Carga el parquet silver de BD.
    
    Args:
        ruta_parquet: Path al archivo parquet silver
    
    Returns:
        DataFrame de Polars con los datos silver
    """
    print(f"\n📊 Cargando parquet silver: {ruta_parquet.name}")
    
    df = pl.read_parquet(ruta_parquet)
    
    print(f"  ✓ Parquet cargado correctamente")
    print(f"  ✓ Filas totales: {len(df):,}")
    print(f"  ✓ Columnas: {len(df.columns)}")
    
    return df


def extraer_centros_costo(df: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """
    Extrae y procesa la tabla de centros de costo.
    
    Args:
        df: DataFrame con los datos silver
        esquema: Diccionario con la configuración del esquema
    
    Returns:
        DataFrame con centros de costo únicos
    """
    print("\n🔄 Procesando centros de costo...")
    
    # Verificar que existan las columnas requeridas
    columnas_faltantes = set(esquema['columnas_requeridas']) - set(df.columns)
    if columnas_faltantes:
        raise ValueError(f"Columnas faltantes en el parquet: {columnas_faltantes}")
    
    # Seleccionar solo las columnas requeridas
    df_cc = df.select(esquema['columnas_requeridas'])
    
    print(f"  - Filas antes de deduplicación: {len(df_cc):,}")
    
    # Eliminar duplicados basándose en la columna CC
    columna_dedupe = esquema['columna_deduplicacion']
    df_cc = df_cc.unique(subset=[columna_dedupe], keep='first')
    
    print(f"  - Filas después de deduplicación: {len(df_cc):,}")
    print(f"  - Centros de costo únicos: {df_cc[columna_dedupe].n_unique()}")
    
    # Ordenar por CC
    df_cc = df_cc.sort(columna_dedupe)
    
    return df_cc


def guardar_centros_costo(df: pl.DataFrame, carpeta_trabajo: Path) -> dict:
    """
    Guarda el DataFrame de centros de costo con sistema de versionado dual.
    
    Sistema de archivos:
    - /actual: Archivos SIN timestamp (para Power BI - paths estables)
    - /historico: Archivos CON timestamp (para auditoría)
    
    Args:
        df: DataFrame con los centros de costo procesados
        carpeta_trabajo: Path de la carpeta de trabajo
    
    Returns:
        Dict con paths de archivos generados
    """
    print("\n💾 Guardando centros de costo...")
    
    # Crear estructura de carpetas
    carpeta_cc = carpeta_trabajo / "centros_costo"
    carpeta_actual = carpeta_cc / "actual"
    carpeta_historico = carpeta_cc / "historico"
    
    carpeta_actual.mkdir(parents=True, exist_ok=True)
    carpeta_historico.mkdir(parents=True, exist_ok=True)
    
    # Generar timestamp para archivos históricos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # ========== ARCHIVOS EN /actual (SIN timestamp) ==========
    print("\n  📂 Guardando en /actual (para Power BI)...")
    
    nombre_base_actual = "CC_ACTUAL"
    ruta_parquet_actual = carpeta_actual / f"{nombre_base_actual}.parquet"
    ruta_excel_actual = carpeta_actual / f"{nombre_base_actual}.xlsx"
    
    # Guardar parquet actual
    print(f"    - {nombre_base_actual}.parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_actual, compression="snappy")
    tamanio_kb = ruta_parquet_actual.stat().st_size / 1024
    print(f" ✓ ({tamanio_kb:.2f} KB)")
    
    # Guardar Excel actual
    print(f"    - {nombre_base_actual}.xlsx...", end='', flush=True)
    df.write_excel(ruta_excel_actual)
    tamanio_kb = ruta_excel_actual.stat().st_size / 1024
    print(f" ✓ ({tamanio_kb:.2f} KB)")
    
    # ========== ARCHIVOS EN /historico (CON timestamp) ==========
    print("\n  📂 Guardando en /historico (auditoría)...")
    
    nombre_base_historico = f"CC_ACTUAL_{timestamp}"
    ruta_parquet_historico = carpeta_historico / f"{nombre_base_historico}.parquet"
    ruta_excel_historico = carpeta_historico / f"{nombre_base_historico}.xlsx"
    
    # Guardar parquet histórico
    print(f"    - {nombre_base_historico}.parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_historico, compression="snappy")
    tamanio_kb = ruta_parquet_historico.stat().st_size / 1024
    print(f" ✓ ({tamanio_kb:.2f} KB)")
    
    # Guardar Excel histórico
    print(f"    - {nombre_base_historico}.xlsx...", end='', flush=True)
    df.write_excel(ruta_excel_historico)
    tamanio_kb = ruta_excel_historico.stat().st_size / 1024
    print(f" ✓ ({tamanio_kb:.2f} KB)")
    
    print(f"\n  ✓ Archivos guardados exitosamente")
    print(f"    📁 Actual: {carpeta_actual}")
    print(f"    📁 Histórico: {carpeta_historico}")
    
    return {
        'parquet_actual': ruta_parquet_actual,
        'excel_actual': ruta_excel_actual,
        'parquet_historico': ruta_parquet_historico,
        'excel_historico': ruta_excel_historico,
        'carpeta_actual': carpeta_actual,
        'carpeta_historico': carpeta_historico
    }


def main():
    """Función principal del script"""
    print("=" * 80)
    print(" EXTRACTOR DE CENTROS DE COSTO - SILVER → CC ".center(80, "="))
    print("=" * 80)
    
    inicio = datetime.now()
    
    try:
        # 1. Buscar esquema YAML
        print("\n[1/3] Carga de Esquema YAML")
        ruta_esquema = buscar_esquema()
        
        if not ruta_esquema:
            print("⚠️ No se encontró el esquema automáticamente.")
            print("   Buscando manualmente...")
            
            ruta_esquema = seleccionar_archivo(
                titulo="Seleccionar esquema - Centros de Costo (YAML)",
                tipos=structured_filetypes()
            )
        
        esquema = cargar_esquema(ruta_esquema)
        
        # 2. Seleccionar parquet silver
        print("\n[2/3] Selección de Parquet Silver")
        ruta_parquet = seleccionar_archivo(
            titulo="Seleccionar parquet Silver de BD",
            tipos=[("Parquet files", "*.parquet"), ("All files", "*.*")]
        )
        print(f"  ✓ Seleccionado: {ruta_parquet.name}")
        
        df_silver = cargar_parquet_silver(ruta_parquet)
        
        # 3. Procesar centros de costo
        print("\n[3/3] Procesamiento de Centros de Costo")
        df_cc = extraer_centros_costo(df_silver, esquema)
        
        # 4. Guardar resultado con versionado dual
        carpeta_trabajo = ruta_parquet.parent.parent  # Subir de silver/ a bd/
        rutas = guardar_centros_costo(df_cc, carpeta_trabajo)
        
        # Resumen final
        duracion = (datetime.now() - inicio).total_seconds()
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Proceso completado exitosamente")
        print(f"\n📊 Estadísticas:")
        print(f"  - Centros de costo procesados: {len(df_cc):,}")
        print(f"  - Tiempo de ejecución: {duracion:.2f}s")
        
        print(f"\n📁 Archivos generados:")
        print(f"\n  Carpeta /actual (Power BI):")
        print(f"    - {rutas['parquet_actual'].name}")
        print(f"    - {rutas['excel_actual'].name}")
        
        print(f"\n  Carpeta /historico (Auditoría):")
        print(f"    - {rutas['parquet_historico'].name}")
        print(f"    - {rutas['excel_historico'].name}")
        
        print(f"\n📂 Ubicación base: {rutas['carpeta_actual'].parent}")
        
        print("\n💡 Sistema de Versionado:")
        print("  ✓ /actual: Archivos SIN timestamp → Power BI siempre lee el mismo path")
        print("  ✓ /historico: Archivos CON timestamp → Auditoría completa")
        print("  ✓ Cada ejecución actualiza /actual y crea versión en /historico")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
