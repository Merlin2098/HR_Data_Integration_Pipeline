"""
Script: step2_exportar_ingresos.py
Descripción: Transforma datos de EMPLEADOS de la capa Silver a Gold
             aplicando selección de columnas, tipado y validaciones.
             
             NOTA: Solo procesa EMPLEADOS. PRACTICANTES se mantiene en Silver.
             
Proceso:
    1. Lee silver/Relacion Ingresos EMPLEADOS.parquet
    2. Aplica esquema YAML (selección de columnas y tipado)
    3. Genera métricas de calidad
    4. Guarda en gold/ sin timestamp

Autor: Richi
Fecha: 06.01.2025
"""

import polars as pl
from pathlib import Path
from datetime import datetime
from tkinter import Tk, filedialog
import sys
import time

from src.utils.structured_config import (
    find_first_structured_path,
    load_structured_data,
    structured_filetypes,
)
from src.utils.gold_export import maybe_write_excel
from src.utils.month_name import add_month_name_column

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

def buscar_esquema() -> Path | None:
    """Busca el archivo de esquema YAML en ubicaciones comunes."""
    rutas_posibles = [
        Path("../assets/esquemas/esquema_relacion_ingresos"),
        Path("assets/esquemas/esquema_relacion_ingresos"),
        Path("../../assets/esquemas/esquema_relacion_ingresos"),
        Path("esquema_relacion_ingresos"),
    ]
    
    return find_first_structured_path(rutas_posibles, prefer_resource_path=False)

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def seleccionar_archivo_parquet() -> Path | None:
    """Abre diálogo para seleccionar archivo Parquet Silver"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title="Seleccionar archivo Parquet Silver - EMPLEADOS",
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def cargar_esquema(ruta_esquema: Path) -> dict:
    """Carga el esquema YAML y extrae configuración de EMPLEADOS."""
    print(f"📋 Cargando esquema: {ruta_esquema.name}")
    
    esquema_completo = load_structured_data(ruta_esquema, prefer_resource_path=False)
    
    # Extraer solo la configuración de EMPLEADOS
    if 'hojas' not in esquema_completo or 'EMPLEADOS' not in esquema_completo['hojas']:
        raise ValueError("El esquema no contiene configuración para EMPLEADOS")
    
    esquema = esquema_completo['hojas']['EMPLEADOS']
    
    # Mostrar metadata si existe
    if 'metadata' in esquema_completo:
        metadata = esquema_completo['metadata']
        print(f"   ✓ Versión: {metadata.get('version', 'N/A')}")
        print(f"   ✓ Última modificación: {metadata.get('fecha_actualizacion', 'N/A')}")
    
    print(f"   ✓ Columnas definidas: {len(esquema['schema'])}")
    
    return esquema


def seleccionar_y_convertir_columnas(df: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """Selecciona columnas y convierte tipos de datos según el esquema"""
    print(f"\n[2/3] Procesando columnas según esquema...")
    
    schema_def = esquema['schema']
    print(f"  - Columnas a procesar: {len(schema_def)}")
    
    # Verificar que todas las columnas existen
    columnas_faltantes = [col for col in schema_def.keys() if col not in df.columns]
    if columnas_faltantes:
        print(f"  ❌ Columnas faltantes en Silver: {columnas_faltantes}")
        raise ValueError(f"Columnas faltantes en el DataFrame: {columnas_faltantes}")
    
    # Mapeo de tipos del esquema a Polars
    tipo_map = {
        'string': pl.Utf8,
        'integer': pl.Int64,
        'float': pl.Float64,
        'boolean': pl.Boolean,
        'date': pl.Date,
    }
    
    # Seleccionar y convertir en una sola operación
    expresiones = []
    conversiones_aplicadas = 0
    
    for columna, config in schema_def.items():
        tipo_str = config.get('type', 'string').lower()
        tipo_polars = tipo_map.get(tipo_str, pl.Utf8)
        
        # Manejo especial para fechas: convertir de string con timestamp a Date
        if tipo_str == 'date':
            # Convertir string a datetime primero, luego extraer solo la fecha
            expresion = (
                pl.col(columna)
                .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
                .cast(pl.Date, strict=False)
                .alias(columna)
            )
        else:
            # Conversión normal para otros tipos
            expresion = pl.col(columna).cast(tipo_polars, strict=False).alias(columna)
        
        expresiones.append(expresion)
        conversiones_aplicadas += 1
    
    df_resultado = df.select(expresiones)
    
    print(f"  ✓ Conversiones de tipo aplicadas: {conversiones_aplicadas}")
    print(f"  ✓ Resultado: {df_resultado.height:,} filas × {df_resultado.width} columnas")
    
    return df_resultado


def generar_metricas_basicas(df: pl.DataFrame):
    """Genera métricas básicas de calidad"""
    print(f"\n📊 MÉTRICAS DE CALIDAD")
    print("=" * 80)
    print(f"Total de registros: {df.height:,}")
    print(f"Total de columnas: {df.width}")
    
    # Periodos únicos
    if "PERIODO" in df.columns:
        periodos = df["PERIODO"].unique().sort().to_list()
        print(f"Periodos únicos: {len(periodos)}")
        if len(periodos) <= 12:
            print(f"  → {', '.join(periodos)}")
    
    # Nulos por columna
    print(f"\nValores nulos por columna:")
    tiene_nulos = False
    for col in df.columns:
        nulos = df[col].is_null().sum()
        if nulos > 0:
            pct = (nulos / df.height * 100) if df.height > 0 else 0
            print(f"   {col:30}: {nulos:4} ({pct:5.2f}%)")
            tiene_nulos = True
    
    if not tiene_nulos:
        print("   ✓ Sin valores nulos")
    
    print("=" * 80)


def guardar_resultados(df: pl.DataFrame, carpeta_silver: Path, export_excel: bool = False):
    """
    Guarda el DataFrame en carpeta gold/ con sistema de versionamiento:
    - Archivos actuales sin timestamp en gold/
    - Copia con timestamp en gold/historico/
    
    Args:
        df: DataFrame a guardar
        carpeta_silver: Path de la carpeta donde está el archivo Silver
        
    Returns:
        tuple: (ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico)
    """
    # Crear carpeta gold/ un nivel arriba de silver/
    carpeta_trabajo = carpeta_silver.parent
    carpeta_gold = carpeta_trabajo / "gold"
    carpeta_gold.mkdir(exist_ok=True)
    
    # Crear carpeta historico/ dentro de gold/
    carpeta_historico = carpeta_gold / "historico"
    carpeta_historico.mkdir(exist_ok=True)
    
    # Timestamp para archivo histórico
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    
    print(f"\n[3/3] Guardando resultados en capa Gold...")
    print(f"  📁 Carpeta Gold: {carpeta_gold}")
    print(f"  📁 Carpeta Histórico: {carpeta_historico}")
    
    # === ARCHIVOS ACTUALES (sin timestamp) ===
    nombre_actual = "empleados_gold"
    ruta_parquet_actual = carpeta_gold / f"{nombre_actual}.parquet"
    ruta_excel_actual = carpeta_gold / f"{nombre_actual}.xlsx"
    
    print(f"\n  📄 Archivos actuales (se sobreescriben):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_actual, compression="snappy")
    print(f" ✓")
    
    ruta_excel_actual = maybe_write_excel(
        ruta_excel_actual,
        export_excel,
        lambda path: df.write_excel(path),
    )
    if ruta_excel_actual is not None:
        print(f"    - Guardando Excel...", end='', flush=True)
        print(f" ✓")
    else:
        print("    - Excel actual omitido (exportación opcional desactivada)")
    
    # === ARCHIVOS HISTÓRICOS (con timestamp) ===
    nombre_historico = f"empleados_gold_{timestamp}"
    ruta_parquet_historico = carpeta_historico / f"{nombre_historico}.parquet"
    ruta_excel_historico = carpeta_historico / f"{nombre_historico}.xlsx"
    
    print(f"\n  📦 Archivos históricos (con timestamp):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_historico, compression="snappy")
    print(f" ✓")
    
    ruta_excel_historico = maybe_write_excel(
        ruta_excel_historico,
        export_excel,
        lambda path: df.write_excel(path),
    )
    if ruta_excel_historico is not None:
        print(f"    - Guardando Excel...", end='', flush=True)
        print(f" ✓")
    else:
        print("    - Excel histórico omitido (exportación opcional desactivada)")
    
    return ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal de transformación Silver → Gold"""
    print("=" * 80)
    print(" TRANSFORMACIÓN SILVER → GOLD - EMPLEADOS ".center(80, "="))
    print("=" * 80)
    print("\n💡 Nota: Solo EMPLEADOS se procesa a Gold")
    print("   PRACTICANTES permanece en Silver para consultas\n")
    
    # Iniciar cronómetro
    tiempo_inicio = time.time()
    
    # 1. Buscar esquema YAML
    ruta_esquema = buscar_esquema()
    
    if not ruta_esquema:
        print("⚠️  No se encontró el esquema automáticamente.")
        print("   Buscando manualmente...")
        
        root = Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        
        ruta_esquema = filedialog.askopenfilename(
            title="Seleccionar esquema (YAML)",
            filetypes=structured_filetypes()
        )
        
        root.destroy()
        
        if not ruta_esquema:
            print("❌ No se seleccionó esquema. Proceso cancelado.")
            return
        
        ruta_esquema = Path(ruta_esquema)
    
    # 2. Cargar esquema
    try:
        esquema = cargar_esquema(ruta_esquema)
    except Exception as e:
        print(f"❌ Error al cargar esquema: {e}")
        return
    
    # 3. Seleccionar archivo Parquet Silver
    print("\n[PASO 1] Selecciona el archivo Parquet Silver - EMPLEADOS...")
    archivo_silver = seleccionar_archivo_parquet()
    
    if not archivo_silver:
        print("❌ No se seleccionó archivo. Proceso cancelado.")
        return
    
    print(f"✓ Archivo seleccionado: {archivo_silver.name}")
    print(f"  Ubicación: {archivo_silver.parent}")
    
    # 4. Leer datos Silver
    print(f"\n[1/3] Cargando archivo Silver...")
    print(f"  📖 Archivo: {archivo_silver.name}")
    
    try:
        # 5. Leer datos Silver
        df_silver = pl.read_parquet(archivo_silver)
        print(f"  ✓ Datos cargados: {df_silver.height:,} filas × {df_silver.width} columnas")
        
        # 6. Seleccionar columnas y convertir tipos
        df_gold = seleccionar_y_convertir_columnas(df_silver, esquema)

        # 6.1 Imputar SERVICIO y REGIMEN: null/vacío → "Personal_Interno"
        df_gold = df_gold.with_columns([
            pl.col("SERVICIO").fill_null("Personal_Interno").replace("", "Personal_Interno"),
            pl.col("REGIMEN").fill_null("Personal_Interno").replace("", "Personal_Interno"),
        ])
        print(f"  ✓ Imputación aplicada: SERVICIO y REGIMEN (null/vacío → 'Personal_Interno')")

        # 6.2 Agregar columna enriquecida NOMBRE_MES
        df_gold = add_month_name_column(df_gold, default_invalid="")

        # 7. Generar métricas
        generar_metricas_basicas(df_gold)
        
        # 8. Guardar archivos (en la misma carpeta que el archivo Silver)
        carpeta_trabajo = archivo_silver.parent
        ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico = guardar_resultados(
            df_gold,
            carpeta_trabajo,
        )
        
        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio
        
        # 9. Resumen final
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Transformación completada exitosamente")
        print(f"\n📊 Estadísticas:")
        print(f"  - Total de registros: {df_gold.height:,}")
        print(f"  - Columnas en Gold: {df_gold.width}")
        
        print(f"\n📁 Archivos generados:")
        print(f"\n  Actuales (para Power BI):")
        print(f"    - {ruta_parquet_actual.name}")
        if ruta_excel_actual is not None:
            print(f"    - {ruta_excel_actual.name}")
        
        print(f"\n  Históricos (con timestamp):")
        print(f"    - {ruta_parquet_historico.name}")
        if ruta_excel_historico is not None:
            print(f"    - {ruta_excel_historico.name}")
        
        print(f"\n⏱️  Tiempo de ejecución: {tiempo_total:.2f}s")
        
        print("\n💡 Notas:")
        print("  - Archivos actuales: se sobreescriben en cada ejecución (rutas estables para Power BI)")
        print("  - Archivos históricos: se archivan con timestamp para auditoría")
        print(f"  - Conectar Power BI a: {ruta_parquet_actual}")
        
        print("\n📂 Estructura de carpetas:")
        print(f"  carpeta_trabajo/")
        print(f"  ├── silver/")
        print(f"  │   └── {archivo_silver.name}")
        print(f"  └── gold/")
        print(f"      ├── {ruta_parquet_actual.name}")
        print(f"      └── historico/")
        print(f"          ├── {ruta_parquet_historico.name}")
        if ruta_excel_actual is not None:
            print(f"      ├── {ruta_excel_actual.name}")
        if ruta_excel_historico is not None:
            print(f"          └── {ruta_excel_historico.name}")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error durante el procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
