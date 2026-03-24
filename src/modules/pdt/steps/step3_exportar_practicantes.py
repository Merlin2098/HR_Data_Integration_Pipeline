"""
Script: step3_exportar_practicantes.py
Descripción: Transforma datos de PRACTICANTES de la capa Silver a Gold
             aplicando selección de columnas, tipado y validaciones.
             
Proceso:
    1. Lee silver/Relacion Ingresos PRACTICANTES.parquet
    2. Aplica esquema YAML (selección de columnas y tipado)
    3. Agrega columna enriquecida NOMBRE_MES
    4. Aplica business rules (Universidad de Procedencia)
    5. Genera métricas de calidad
    6. Guarda en gold/ con dual versioning (actual + histórico)

Autor: Richi
Fecha: 12.01.2025
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
from src.utils.month_name import add_month_name_column

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

def buscar_esquema() -> Path | None:
    """Busca el archivo de esquema YAML en ubicaciones comunes."""
    rutas_posibles = [
        Path("../assets/esquemas/esquema_ingresos_practicantes"),
        Path("assets/esquemas/esquema_ingresos_practicantes"),
        Path("../../assets/esquemas/esquema_ingresos_practicantes"),
        Path("esquema_ingresos_practicantes"),
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
        title="Seleccionar archivo Parquet Silver - PRACTICANTES",
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def cargar_esquema(ruta_esquema: Path) -> dict:
    """Carga el esquema YAML y extrae configuración de PRACTICANTES."""
    print(f"📋 Cargando esquema: {ruta_esquema.name}")
    
    esquema_completo = load_structured_data(ruta_esquema, prefer_resource_path=False)
    
    # Extraer solo la configuración de PRACTICANTES
    if 'hojas' not in esquema_completo or 'PRACTICANTES' not in esquema_completo['hojas']:
        raise ValueError("El esquema no contiene configuración para PRACTICANTES")
    
    esquema = esquema_completo['hojas']['PRACTICANTES']
    
    # Mostrar metadata si existe
    if 'metadata' in esquema_completo:
        metadata = esquema_completo['metadata']
        print(f"   ✓ Versión: {metadata.get('version', 'N/A')}")
        print(f"   ✓ Última modificación: {metadata.get('fecha_actualizacion', 'N/A')}")
    
    print(f"   ✓ Columnas definidas: {len(esquema['schema'])}")
    
    return esquema


def seleccionar_y_convertir_columnas(df: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """Selecciona columnas y convierte tipos de datos según el esquema"""
    print(f"\n[2/4] Procesando columnas según esquema...")
    
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


def aplicar_business_rules(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica reglas de negocio específicas de PRACTICANTES:
    - Universidad de Procedencia: null → 'POR DEFINIR'
    - CODIGO SAP: '#N/D', '#N/A', 'Error' → null (ya viene desde Silver)
    """
    print(f"\n[3/4] Aplicando business rules...")
    
    # Contar valores nulos antes
    nulos_antes = df["Universidad de Procedencia"].is_null().sum()
    
    # Aplicar regla: Universidad de Procedencia nula → 'POR DEFINIR'
    df = df.with_columns([
        pl.when(pl.col("Universidad de Procedencia").is_null())
        .then(pl.lit("POR DEFINIR"))
        .otherwise(pl.col("Universidad de Procedencia"))
        .alias("Universidad de Procedencia")
    ])
    
    # Contar valores 'POR DEFINIR' después
    por_definir = (df["Universidad de Procedencia"] == "POR DEFINIR").sum()
    
    print(f"  ✓ Universidad de Procedencia:")
    print(f"    - Valores nulos transformados a 'POR DEFINIR': {nulos_antes}")
    print(f"    - Total 'POR DEFINIR' en Gold: {por_definir}")
    
    return df


def agregar_columna_enriquecida(df: pl.DataFrame) -> pl.DataFrame:
    """
    Agrega columna enriquecida NOMBRE_MES basada en la columna MES.
    Esta operación se realiza DESPUÉS de la validación de esquema.
    """
    print(f"\n[4/4] Agregando columnas enriquecidas...")
    
    df = add_month_name_column(df, default_invalid="")
    
    print(f"  ✓ Columna NOMBRE_MES agregada exitosamente")

    print(f"  ✓ Columnas reordenadas (NOMBRE_MES después de MES)")
    
    return df


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
    
    # Universidad de Procedencia
    if "Universidad de Procedencia" in df.columns:
        universidades = df["Universidad de Procedencia"].value_counts().sort("Universidad de Procedencia")
        print(f"\nUniversidades de Procedencia:")
        for row in universidades.iter_rows(named=True):
            print(f"   {row['Universidad de Procedencia']:40}: {row['count']:4} practicantes")
    
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


def guardar_resultados(df: pl.DataFrame, carpeta_silver: Path):
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
    
    print(f"\n[5/5] Guardando resultados en capa Gold...")
    print(f"  📁 Carpeta Gold: {carpeta_gold}")
    print(f"  📁 Carpeta Histórico: {carpeta_historico}")
    
    # === ARCHIVOS ACTUALES (sin timestamp) ===
    nombre_actual = "practicante_gold"
    ruta_parquet_actual = carpeta_gold / f"{nombre_actual}.parquet"
    ruta_excel_actual = carpeta_gold / f"{nombre_actual}.xlsx"
    
    print(f"\n  📄 Archivos actuales (se sobreescriben):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_actual, compression="snappy")
    print(f" ✓")
    
    print(f"    - Guardando Excel...", end='', flush=True)
    df.write_excel(ruta_excel_actual)
    print(f" ✓")
    
    # === ARCHIVOS HISTÓRICOS (con timestamp) ===
    nombre_historico = f"practicante_gold_{timestamp}"
    ruta_parquet_historico = carpeta_historico / f"{nombre_historico}.parquet"
    ruta_excel_historico = carpeta_historico / f"{nombre_historico}.xlsx"
    
    print(f"\n  📦 Archivos históricos (con timestamp):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df.write_parquet(ruta_parquet_historico, compression="snappy")
    print(f" ✓")
    
    print(f"    - Guardando Excel...", end='', flush=True)
    df.write_excel(ruta_excel_historico)
    print(f" ✓")
    
    return ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal de transformación Silver → Gold"""
    print("=" * 80)
    print(" TRANSFORMACIÓN SILVER → GOLD - PRACTICANTES ".center(80, "="))
    print("=" * 80)
    print("\n💡 Procesando datos de practicantes para análisis de procedencia\n")
    
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
            title="Seleccionar esquema - PRACTICANTES (YAML)",
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
    print("\n[PASO 1] Selecciona el archivo Parquet Silver - PRACTICANTES...")
    archivo_silver = seleccionar_archivo_parquet()
    
    if not archivo_silver:
        print("❌ No se seleccionó archivo. Proceso cancelado.")
        return
    
    print(f"✓ Archivo seleccionado: {archivo_silver.name}")
    print(f"  Ubicación: {archivo_silver.parent}")
    
    # 4. Leer datos Silver
    print(f"\n[1/4] Cargando archivo Silver...")
    print(f"  📖 Archivo: {archivo_silver.name}")
    
    try:
        # 5. Leer datos Silver
        df_silver = pl.read_parquet(archivo_silver)
        print(f"  ✓ Datos cargados: {df_silver.height:,} filas × {df_silver.width} columnas")
        
        # 6. Seleccionar columnas y convertir tipos
        df_gold = seleccionar_y_convertir_columnas(df_silver, esquema)
        
        # 7. Aplicar business rules
        df_gold = aplicar_business_rules(df_gold)
        
        # 8. Agregar columna enriquecida NOMBRE_MES
        df_gold = agregar_columna_enriquecida(df_gold)
        
        # 9. Generar métricas
        generar_metricas_basicas(df_gold)
        
        # 10. Guardar archivos (en la misma carpeta que el archivo Silver)
        carpeta_trabajo = archivo_silver.parent
        ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico = guardar_resultados(df_gold, carpeta_trabajo)
        
        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio
        
        # 11. Resumen final
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
        print(f"    - {ruta_excel_actual.name}")
        
        print(f"\n  Históricos (con timestamp):")
        print(f"    - {ruta_parquet_historico.name}")
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
        print(f"      ├── {ruta_excel_actual.name}")
        print(f"      └── historico/")
        print(f"          ├── {ruta_parquet_historico.name}")
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
