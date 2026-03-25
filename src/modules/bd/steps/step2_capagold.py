"""
Script: step2_capagold.py
Descripción: Transforma datos de BD de Silver a Gold
             - Valida columnas según esquema YAML
             - Divide en Empleados y Practicantes
             - Convierte tipos de datos
             
Arquitectura:
- Silver: Parquet con todas las columnas (35)
- Gold: Parquet con columnas filtradas (13) según esquema

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
from tkinter import Tk, filedialog

from src.utils.structured_config import (
    find_first_structured_path,
    load_structured_data,
    structured_filetypes,
)
from src.utils.gold_export import maybe_write_excel


def seleccionar_archivo_parquet() -> Path | None:
    """Abre diálogo para seleccionar archivo Parquet Silver"""
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    archivo = filedialog.askopenfilename(
        title="Seleccionar archivo Parquet Silver - BD",
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")]
    )
    
    root.destroy()
    
    return Path(archivo) if archivo else None


def buscar_esquema() -> Path | None:
    """Busca el archivo de esquema YAML en ubicaciones comunes."""
    rutas_posibles = [
        Path("../assets/esquemas/esquema_bd"),
        Path("assets/esquemas/esquema_bd"),
        Path("../../assets/esquemas/esquema_bd"),
        Path("esquema_bd"),
    ]
    
    return find_first_structured_path(rutas_posibles, prefer_resource_path=False)


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
    
    print(f"  ✓ Esquema: {esquema['schema_name']} v{esquema['version']}")
    print(f"  ✓ Columnas esperadas: {len(esquema['columns'])}")
    
    return esquema


def validar_y_filtrar_columnas(df: pl.DataFrame, esquema: dict) -> tuple[pl.DataFrame, dict]:
    """
    Valida que las columnas del esquema existan en el DataFrame y filtra solo esas columnas.
    PRESERVA EL ORDEN ORIGINAL del DataFrame Silver.
    
    Args:
        df: DataFrame de Polars
        esquema: Diccionario con el esquema
    
    Returns:
        Tupla con (DataFrame filtrado, estadísticas de validación)
    """
    required_columns = [col["name"] for col in esquema["columns"]]
    present_columns = df.columns
    
    # Verificar columnas presentes y faltantes
    present = [col for col in required_columns if col in present_columns]
    missing = [col for col in required_columns if col not in present_columns]
    
    # Filtrar columnas en el ORDEN ORIGINAL del DataFrame Silver
    columns_to_select = [col for col in df.columns if col in required_columns]
    
    stats = {
        "total_required": len(required_columns),
        "present": len(present),
        "missing": len(missing),
        "missing_list": missing,
        "present_list": present
    }
    
    if missing:
        print(f"\n  ⚠️  {len(missing)} columna(s) faltante(s):")
        for col in missing:
            print(f"     • {col}")
    
    # Filtrar solo las columnas presentes del esquema
    df_filtered = df.select(columns_to_select)
    
    print(f"  ✓ Columnas requeridas: {stats['total_required']}")
    print(f"  ✓ Columnas presentes: {stats['present']}")
    print(f"  ✓ DataFrame filtrado: {df_filtered.height:,} filas × {df_filtered.width} columnas")
    
    return df_filtered, stats


def convertir_columnas_fecha(df: pl.DataFrame, esquema: dict) -> pl.DataFrame:
    """
    Convierte columnas de tipo date según el esquema.
    Silver tiene fechas como String en formato YYYY-MM-DD HH:MM:SS
    
    Args:
        df: DataFrame de Polars
        esquema: Diccionario con el esquema
    
    Returns:
        DataFrame con columnas de fecha convertidas
    """
    date_columns = [col["name"] for col in esquema["columns"] if col["type"] == "date"]
    
    print(f"\n  🔄 Convirtiendo columnas de fecha...")
    
    for col_name in date_columns:
        if col_name in df.columns:
            try:
                # Convertir desde String con formato "YYYY-MM-DD HH:MM:SS" a Date
                df = df.with_columns(
                    pl.col(col_name)
                    .str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
                    .cast(pl.Date, strict=False)
                    .alias(col_name)
                )
                print(f"    ✓ {col_name} convertida a Date")
            except Exception as e:
                print(f"    ⚠️  No se pudo convertir '{col_name}': {e}")
    
    return df


def dividir_por_modalidad(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Divide el DataFrame en Empleados y Practicantes según Modalidad de Contrato.
    
    Args:
        df: DataFrame completo
    
    Returns:
        Tupla con (df_empleados, df_practicantes)
    """
    print(f"\n  🔀 Dividiendo datos según Modalidad de Contrato...")
    
    if "Modalidad de Contrato" not in df.columns:
        raise ValueError("La columna 'Modalidad de Contrato' no existe en el DataFrame")
    
    # Practicantes: TERMINO DE CONVENIO
    df_practicantes = df.filter(
        pl.col("Modalidad de Contrato").str.contains("TERMINO DE CONVENIO")
    )
    
    # Empleados: todos los demás
    df_empleados = df.filter(
        ~pl.col("Modalidad de Contrato").str.contains("TERMINO DE CONVENIO")
    )
    
    print(f"    ✓ Practicantes (TERMINO DE CONVENIO): {df_practicantes.height:,} registros")
    print(f"    ✓ Empleados (otros): {df_empleados.height:,} registros")
    print(f"    ✓ Total: {df.height:,} registros")
    
    return df_empleados, df_practicantes


def guardar_resultados(
    df_empleados: pl.DataFrame,
    df_practicantes: pl.DataFrame,
    carpeta_silver: Path,
    export_excel: bool = False,
):
    """
    Guarda ambos DataFrames en carpeta gold/ con sistema de versionamiento:
    - Archivos actuales sin timestamp en gold/
    - Copia con timestamp en gold/historico/
    
    Args:
        df_empleados: DataFrame de empleados
        df_practicantes: DataFrame de practicantes
        carpeta_silver: Path de la carpeta silver
        
    Returns:
        dict con todas las rutas de archivos generados
    """
    # Crear carpetas
    carpeta_trabajo = carpeta_silver.parent
    carpeta_gold = carpeta_trabajo / "gold"
    carpeta_gold.mkdir(exist_ok=True)
    
    carpeta_historico = carpeta_gold / "historico"
    carpeta_historico.mkdir(exist_ok=True)
    
    # Timestamp para archivos históricos
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    
    print(f"\n[3/3] Guardando resultados en capa Gold...")
    print(f"  📁 Carpeta Gold: {carpeta_gold}")
    print(f"  📁 Carpeta Histórico: {carpeta_historico}")
    
    rutas = {}
    
    # === EMPLEADOS ===
    if df_empleados.height > 0:
        print(f"\n  📄 EMPLEADOS:")
        
        # Actual
        ruta_emp_parquet_actual = carpeta_gold / "bd_empleados_gold.parquet"
        ruta_emp_excel_actual = carpeta_gold / "bd_empleados_gold.xlsx"
        
        print(f"    - Guardando actual (parquet)...", end='', flush=True)
        df_empleados.write_parquet(ruta_emp_parquet_actual, compression="snappy")
        print(f" ✓")
        
        ruta_emp_excel_actual = maybe_write_excel(
            ruta_emp_excel_actual,
            export_excel,
            lambda path: df_empleados.write_excel(path),
        )
        if ruta_emp_excel_actual is not None:
            print(f"    - Guardando actual (Excel)...", end='', flush=True)
            print(f" ✓")
        else:
            print("    - Excel actual omitido (exportación opcional desactivada)")
        
        # Histórico
        ruta_emp_parquet_hist = carpeta_historico / f"bd_empleados_gold_{timestamp}.parquet"
        ruta_emp_excel_hist = carpeta_historico / f"bd_empleados_gold_{timestamp}.xlsx"
        
        print(f"    - Guardando histórico (parquet)...", end='', flush=True)
        df_empleados.write_parquet(ruta_emp_parquet_hist, compression="snappy")
        print(f" ✓")
        
        ruta_emp_excel_hist = maybe_write_excel(
            ruta_emp_excel_hist,
            export_excel,
            lambda path: df_empleados.write_excel(path),
        )
        if ruta_emp_excel_hist is not None:
            print(f"    - Guardando histórico (Excel)...", end='', flush=True)
            print(f" ✓")
        else:
            print("    - Excel histórico omitido (exportación opcional desactivada)")
        
        rutas['empleados'] = {
            'actual_parquet': ruta_emp_parquet_actual,
            'actual_excel': ruta_emp_excel_actual,
            'historico_parquet': ruta_emp_parquet_hist,
            'historico_excel': ruta_emp_excel_hist
        }
    
    # === PRACTICANTES ===
    if df_practicantes.height > 0:
        print(f"\n  📄 PRACTICANTES:")
        
        # Actual
        ruta_prac_parquet_actual = carpeta_gold / "bd_practicantes_gold.parquet"
        ruta_prac_excel_actual = carpeta_gold / "bd_practicantes_gold.xlsx"
        
        print(f"    - Guardando actual (parquet)...", end='', flush=True)
        df_practicantes.write_parquet(ruta_prac_parquet_actual, compression="snappy")
        print(f" ✓")
        
        ruta_prac_excel_actual = maybe_write_excel(
            ruta_prac_excel_actual,
            export_excel,
            lambda path: df_practicantes.write_excel(path),
        )
        if ruta_prac_excel_actual is not None:
            print(f"    - Guardando actual (Excel)...", end='', flush=True)
            print(f" ✓")
        else:
            print("    - Excel actual omitido (exportación opcional desactivada)")
        
        # Histórico
        ruta_prac_parquet_hist = carpeta_historico / f"bd_practicantes_gold_{timestamp}.parquet"
        ruta_prac_excel_hist = carpeta_historico / f"bd_practicantes_gold_{timestamp}.xlsx"
        
        print(f"    - Guardando histórico (parquet)...", end='', flush=True)
        df_practicantes.write_parquet(ruta_prac_parquet_hist, compression="snappy")
        print(f" ✓")
        
        ruta_prac_excel_hist = maybe_write_excel(
            ruta_prac_excel_hist,
            export_excel,
            lambda path: df_practicantes.write_excel(path),
        )
        if ruta_prac_excel_hist is not None:
            print(f"    - Guardando histórico (Excel)...", end='', flush=True)
            print(f" ✓")
        else:
            print("    - Excel histórico omitido (exportación opcional desactivada)")
        
        rutas['practicantes'] = {
            'actual_parquet': ruta_prac_parquet_actual,
            'actual_excel': ruta_prac_excel_actual,
            'historico_parquet': ruta_prac_parquet_hist,
            'historico_excel': ruta_prac_excel_hist
        }
    
    return rutas


def main():
    """Función principal de transformación Silver → Gold"""
    print("=" * 80)
    print(" TRANSFORMACIÓN SILVER → GOLD - BD ".center(80, "="))
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
    print("\n[PASO 1] Selecciona el archivo Parquet Silver - BD...")
    archivo_silver = seleccionar_archivo_parquet()
    
    if not archivo_silver:
        print("❌ No se seleccionó archivo. Proceso cancelado.")
        return
    
    print(f"✓ Archivo seleccionado: {archivo_silver.name}")
    
    # 4. Procesamiento
    print(f"\n[1/3] Cargando y procesando datos...")
    
    try:
        # Leer Silver
        df_silver = pl.read_parquet(archivo_silver)
        print(f"  ✓ Silver cargado: {df_silver.height:,} filas × {df_silver.width} columnas")
        
        # Validar y filtrar columnas
        print(f"\n  🔍 Validando columnas según esquema...")
        df_gold, stats = validar_y_filtrar_columnas(df_silver, esquema)
        
        # Convertir fechas
        df_gold = convertir_columnas_fecha(df_gold, esquema)
        
        # Dividir por modalidad
        print(f"\n[2/3] Dividiendo datos...")
        df_empleados, df_practicantes = dividir_por_modalidad(df_gold)
        
        # Guardar resultados
        carpeta_trabajo = archivo_silver.parent
        rutas = guardar_resultados(df_empleados, df_practicantes, carpeta_trabajo)
        
        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio
        
        # Resumen final
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Transformación completada exitosamente")
        print(f"\n📊 Estadísticas:")
        print(f"  - Total registros procesados: {df_gold.height:,}")
        print(f"  - Empleados: {df_empleados.height:,}")
        print(f"  - Practicantes: {df_practicantes.height:,}")
        print(f"  - Columnas en Gold: {df_gold.width}")
        
        print(f"\n📁 Archivos generados:")
        if 'empleados' in rutas:
            print(f"\n  EMPLEADOS (actuales):")
            print(f"    - {rutas['empleados']['actual_parquet'].name}")
            if rutas['empleados']['actual_excel'] is not None:
                print(f"    - {rutas['empleados']['actual_excel'].name}")
        
        if 'practicantes' in rutas:
            print(f"\n  PRACTICANTES (actuales):")
            print(f"    - {rutas['practicantes']['actual_parquet'].name}")
            if rutas['practicantes']['actual_excel'] is not None:
                print(f"    - {rutas['practicantes']['actual_excel'].name}")
        
        print(f"\n⏱️  Tiempo de ejecución: {tiempo_total:.2f}s")
        
        print("\n💡 Notas:")
        print("  - Archivos actuales: se sobreescriben (rutas estables para Power BI)")
        print("  - Archivos históricos: se archivan con timestamp en gold/historico/")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error durante el procesamiento: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()
