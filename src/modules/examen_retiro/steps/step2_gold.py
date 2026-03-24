"""
Script: step2_gold.py
Descripción: Transforma datos de Exámenes de Retiro de Silver a Gold
             - Filtra y selecciona columnas según esquema
             - Genera columnas derivadas: AÑO, MES, NOMBRE_MES
             
Arquitectura:
- Silver: Parquet con todas las columnas
- Gold: Parquet con columnas filtradas + derivadas

Salida: 
- Archivos actuales sin timestamp en gold/
- Copias históricas con timestamp en gold/historico/

Autor: Richi
Fecha: 06.01.2025
"""

import polars as pl
from pathlib import Path
from datetime import datetime
import time
import sys
from tkinter import Tk, filedialog

from src.utils.structured_config import (
    find_first_structured_path,
    load_structured_data,
    structured_filetypes,
)
from src.utils.month_name import add_month_name_column


def buscar_esquema() -> Path | None:
    """Busca el archivo de esquema YAML en ubicaciones comunes."""
    rutas_posibles = [
        Path("../assets/esquemas/esquema_examen_retiro"),
        Path("assets/esquemas/esquema_examen_retiro"),
        Path("../../assets/esquemas/esquema_examen_retiro"),
        Path("esquema_examen_retiro"),
    ]
    
    return find_first_structured_path(rutas_posibles, prefer_resource_path=False)


def seleccionar_archivo_parquet() -> Path | None:
    """Abre diálogo para seleccionar archivo Parquet Silver"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title="Seleccionar archivo Parquet Silver - Exámenes de Retiro",
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def cargar_esquema(ruta_esquema: Path) -> dict:
    """
    Carga el esquema YAML que define las columnas para la capa Gold
    
    Args:
        ruta_esquema: Path al archivo del esquema
        
    Returns:
        dict con el esquema completo
    """
    print(f"📋 Cargando esquema: {ruta_esquema.name}")
    
    esquema = load_structured_data(ruta_esquema, prefer_resource_path=False)
    
    # Mostrar metadata si existe
    if 'metadata' in esquema:
        metadata = esquema['metadata']
        print(f"   ✓ Versión: {metadata.get('version', 'N/A')}")
        print(f"   ✓ Última modificación: {metadata.get('fecha_actualizacion', 'N/A')}")
    
    print(f"   ✓ Columnas definidas: {len(esquema['schema'])}")
    
    return esquema


def transformar_silver_a_gold(df_silver: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """
    Transforma la capa Silver a Gold aplicando el esquema definido
    Genera columnas derivadas automáticamente
    
    Args:
        df_silver: DataFrame de Silver
        esquema: Diccionario con el esquema
        
    Returns:
        DataFrame transformado para Gold
    """
    print(f"\n[2/3] Transformando Silver → Gold...")
    
    # Extraer nombres de columnas base (no derivadas)
    schema_def = esquema['schema']
    columnas_base = [col for col, config in schema_def.items() 
                    if 'derivado_de' not in config]
    
    # Verificar que todas las columnas base existen en Silver
    columnas_faltantes = [col for col in columnas_base if col not in df_silver.columns]
    if columnas_faltantes:
        print(f"  ⚠️  Columnas no encontradas en Silver: {columnas_faltantes}")
    
    # Seleccionar solo las columnas base disponibles
    columnas_disponibles = [col for col in columnas_base if col in df_silver.columns]
    df_gold = df_silver.select(columnas_disponibles)
    
    # Mapeo de tipos del esquema a Polars
    tipo_map = {
        'string': pl.Utf8,
        'integer': pl.Int64,
        'float': pl.Float64,
        'boolean': pl.Boolean,
        'date': pl.Date,
    }
    
    # Aplicar tipos de datos según esquema
    expresiones = []
    for col_nombre in df_gold.columns:
        if col_nombre not in schema_def:
            continue
            
        col_config = schema_def[col_nombre]
        tipo_str = col_config.get('type', 'string').lower()
        tipo_polars = tipo_map.get(tipo_str, pl.Utf8)
        
        # Manejo especial para fechas
        if tipo_str == 'date':
            # Verificar el tipo actual de la columna
            tipo_actual = df_gold[col_nombre].dtype
            
            if tipo_actual == pl.Date:
                # Ya es Date, no hacer nada
                expresion = pl.col(col_nombre).alias(col_nombre)
            elif tipo_actual == pl.Utf8:
                # Es String, intentar convertir desde string con timestamp
                expresion = (
                    pl.col(col_nombre)
                    .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
                    .cast(pl.Date, strict=False)
                    .alias(col_nombre)
                )
            else:
                # Otros tipos, intentar cast directo
                expresion = pl.col(col_nombre).cast(pl.Date, strict=False).alias(col_nombre)
        else:
            # Conversión normal para otros tipos
            expresion = pl.col(col_nombre).cast(tipo_polars, strict=False).alias(col_nombre)
        
        expresiones.append(expresion)
    
    df_gold = df_gold.select(expresiones)
    
    print(f"  ✓ Tipos de datos aplicados")
    
    # Generar columnas derivadas
    if 'FECHA DE CESE' in df_gold.columns:
        # AÑO
        df_gold = df_gold.with_columns(
            pl.col('FECHA DE CESE').dt.year().alias('AÑO')
        )
        print(f"  ✓ Columna derivada generada: AÑO")
        
        # MES
        df_gold = df_gold.with_columns(
            pl.col('FECHA DE CESE').dt.month().alias('MES')
        )
        print(f"  ✓ Columna derivada generada: MES")
        
        df_gold = add_month_name_column(df_gold, default_invalid=None)
        print(f"  ✓ Columna derivada generada: NOMBRE_MES")
    
    # Aplicar filtros definidos en el esquema
    if 'filtros' in esquema:
        for filtro in esquema['filtros']:
            col_filtro = filtro['columna']
            condicion = filtro['condicion']
            valor = filtro['valor']
            descripcion = filtro.get('descripcion', '')
            
            if col_filtro not in df_gold.columns:
                print(f"  ⚠️  No se puede aplicar filtro: columna {col_filtro} no existe")
                continue
            
            registros_antes = df_gold.height
            
            # Aplicar condición
            if condicion == 'NOT_EQUAL':
                df_gold = df_gold.filter(pl.col(col_filtro) != valor)
            elif condicion == 'EQUAL':
                df_gold = df_gold.filter(pl.col(col_filtro) == valor)
            elif condicion == 'IS_NOT_NULL':
                df_gold = df_gold.filter(pl.col(col_filtro).is_not_null())
            elif condicion == 'IS_NULL':
                df_gold = df_gold.filter(pl.col(col_filtro).is_null())
            
            registros_filtrados = registros_antes - df_gold.height
            print(f"  ✓ Filtro aplicado: {descripcion}")
            print(f"    • {registros_filtrados} registros excluidos")
    
    print(f"  ✓ Resultado: {df_gold.height:,} filas × {df_gold.width} columnas")
    
    return df_gold


def guardar_resultados(df_gold: pl.DataFrame, carpeta_silver: Path):
    """
    Guarda el DataFrame en carpeta gold/ con sistema de versionamiento:
    - Archivos actuales sin timestamp en gold/
    - Copia con timestamp en gold/historico/
    
    Args:
        df_gold: DataFrame a guardar
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
    nombre_actual = "examenes_retiro_gold"
    ruta_parquet_actual = carpeta_gold / f"{nombre_actual}.parquet"
    ruta_excel_actual = carpeta_gold / f"{nombre_actual}.xlsx"
    
    print(f"\n  📄 Archivos actuales (se sobreescriben):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df_gold.write_parquet(ruta_parquet_actual, compression="snappy")
    print(f" ✓")
    
    print(f"    - Guardando Excel...", end='', flush=True)
    # Convertir fechas a string para Excel
    df_export = df_gold.clone()
    for col in df_export.columns:
        if df_export[col].dtype == pl.Date:
            df_export = df_export.with_columns(
                pl.col(col).cast(pl.Utf8, strict=False).alias(col)
            )
    df_export.write_excel(ruta_excel_actual)
    print(f" ✓")
    
    # === ARCHIVOS HISTÓRICOS (con timestamp) ===
    nombre_historico = f"examenes_retiro_gold_{timestamp}"
    ruta_parquet_historico = carpeta_historico / f"{nombre_historico}.parquet"
    ruta_excel_historico = carpeta_historico / f"{nombre_historico}.xlsx"
    
    print(f"\n  📦 Archivos históricos (con timestamp):")
    print(f"    - Guardando parquet...", end='', flush=True)
    df_gold.write_parquet(ruta_parquet_historico, compression="snappy")
    print(f" ✓")
    
    print(f"    - Guardando Excel...", end='', flush=True)
    df_export.write_excel(ruta_excel_historico)
    print(f" ✓")
    
    return ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico


def main():
    """Función principal de transformación Silver → Gold"""
    print("=" * 80)
    print(" TRANSFORMACIÓN SILVER → GOLD - EXÁMENES DE RETIRO ".center(80, "="))
    print("=" * 80)
    
    # Iniciar cronómetro
    tiempo_inicio = time.time()
    
    # 1. Buscar esquema YAML
    ruta_esquema = buscar_esquema()
    
    if not ruta_esquema:
        print("\n⚠️  No se encontró el esquema automáticamente.")
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
    print("\n[PASO 1] Selecciona el archivo Parquet Silver - Exámenes de Retiro...")
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
        df_silver = pl.read_parquet(archivo_silver)
        print(f"  ✓ Datos cargados: {df_silver.height:,} filas × {df_silver.width} columnas")
        
        # 5. Transformar a Gold
        df_gold = transformar_silver_a_gold(df_silver, esquema)
        
        # 6. Guardar archivos
        carpeta_trabajo = archivo_silver.parent
        ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico = guardar_resultados(df_gold, carpeta_trabajo)
        
        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio
        
        # 7. Resumen final
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
        print("  - Columnas derivadas generadas: AÑO, MES, NOMBRE_MES")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error durante el procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
