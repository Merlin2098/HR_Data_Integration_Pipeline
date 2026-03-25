"""
Script: step3_join.py
Descripción: Enriquecimiento de Exámenes de Retiro con Centros de Costo
             Ejecuta JOIN entre examenes_retiro_gold y catálogos CC usando query SQL
             
Arquitectura: 
- Input: Gold + CC_ACTUAL + CC_OLD
- Output: Gold Enriquecido (con información de CC)

Salida:
- Archivos actuales sin timestamp en gold/
- Copias históricas con timestamp en gold/historico/

Autor: Richi
Fecha: 06.01.2025
"""

import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime
import time
from tkinter import Tk, filedialog

from src.utils.gold_export import maybe_write_excel


def find_queries_folder() -> Path:
    """
    Busca la carpeta 'queries' en el directorio actual y hasta 3 niveles arriba.
    
    Returns:
        Path de la carpeta queries
    
    Raises:
        FileNotFoundError: Si no se encuentra la carpeta queries
    """
    carpeta_actual = Path.cwd()
    
    # Buscar en el directorio actual y hasta 3 niveles arriba
    for _ in range(4):
        posible_queries = carpeta_actual / "queries"
        if posible_queries.exists() and posible_queries.is_dir():
            return posible_queries
        carpeta_actual = carpeta_actual.parent
    
    # También buscar en el directorio del script
    script_dir = Path(__file__).parent
    for _ in range(4):
        posible_queries = script_dir / "queries"
        if posible_queries.exists() and posible_queries.is_dir():
            return posible_queries
        script_dir = script_dir.parent
    
    raise FileNotFoundError(
        "No se encontró la carpeta 'queries' en el proyecto.\n"
        "Asegúrate de que exista la carpeta 'queries' en la raíz del proyecto."
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


def cargar_query_sql() -> str:
    """
    Busca la carpeta 'queries' y carga el archivo query_cc_join.sql
    
    Returns:
        String con el query SQL
    """
    print("\n📄 Buscando carpeta de queries...")
    
    try:
        carpeta_queries = find_queries_folder()
        print(f"  ✓ Carpeta encontrada: {carpeta_queries}")
    except FileNotFoundError as e:
        raise FileNotFoundError(str(e))
    
    # Buscar el archivo query_cc_join.sql
    ruta_query = carpeta_queries / "query_cc_join.sql"
    
    if not ruta_query.exists():
        # Listar archivos SQL disponibles
        queries_disponibles = list(carpeta_queries.glob("*.sql"))
        mensaje = f"No se encontró el archivo 'query_cc_join.sql' en {carpeta_queries}\n"
        
        if queries_disponibles:
            mensaje += "\nArchivos SQL disponibles:\n"
            for query_file in queries_disponibles:
                mensaje += f"  • {query_file.name}\n"
            mensaje += "\nAsegúrate de que el archivo se llame 'query_cc_join.sql'"
        else:
            mensaje += "No hay archivos SQL en la carpeta queries"
        
        raise FileNotFoundError(mensaje)
    
    print(f"  ✓ Query encontrada: {ruta_query.name}")
    
    with open(ruta_query, 'r', encoding='utf-8') as f:
        query = f.read()
    
    print(f"  ✓ Query cargada ({len(query)} caracteres)")
    
    return query


def cargar_parquets(ruta_gold: Path, ruta_cc_actual: Path, ruta_cc_old: Path) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Carga los tres parquets necesarios para el JOIN.
    
    Args:
        ruta_gold: Path al parquet examenes_retiro_gold
        ruta_cc_actual: Path al parquet CC_ACTUAL
        ruta_cc_old: Path al parquet CC_OLD
    
    Returns:
        Tupla con (df_examenes, df_cc_actual, df_cc_old)
    """
    print("\n📊 Cargando parquets...")
    
    # Cargar examenes_retiro_gold
    print(f"\n  [1/3] Examenes Retiro Gold")
    df_examenes = pl.read_parquet(ruta_gold)
    print(f"    ✓ {len(df_examenes):,} registros | {len(df_examenes.columns)} columnas")
    
    # Cargar CC_ACTUAL
    print(f"\n  [2/3] CC_ACTUAL")
    df_cc_actual = pl.read_parquet(ruta_cc_actual)
    print(f"    ✓ {len(df_cc_actual):,} códigos únicos | {len(df_cc_actual.columns)} columnas")
    
    # Cargar CC_OLD
    print(f"\n  [3/3] CC_OLD")
    df_cc_old = pl.read_parquet(ruta_cc_old)
    print(f"    ✓ {len(df_cc_old):,} códigos únicos | {len(df_cc_old.columns)} columnas")
    
    return df_examenes, df_cc_actual, df_cc_old


def ejecutar_join_sql(df_examenes: pl.DataFrame, df_cc_actual: pl.DataFrame, 
                      df_cc_old: pl.DataFrame, query: str) -> pl.DataFrame:
    """
    Ejecuta el JOIN usando DuckDB con el query SQL proporcionado.
    
    Args:
        df_examenes: DataFrame de examenes_retiro_gold
        df_cc_actual: DataFrame de CC_ACTUAL
        df_cc_old: DataFrame de CC_OLD
        query: String con el query SQL
    
    Returns:
        DataFrame con el resultado del JOIN
    """
    print("\n🔄 Ejecutando JOIN con DuckDB...")
    
    # Crear conexión DuckDB en memoria
    conn = duckdb.connect(':memory:')
    
    try:
        # Registrar DataFrames como tablas en DuckDB
        conn.register('examenes', df_examenes)
        conn.register('cc_actual', df_cc_actual)
        conn.register('cc_old', df_cc_old)
        
        print("  ✓ Tablas registradas en DuckDB")
        
        # Ejecutar query
        resultado = conn.execute(query).df()
        
        print(f"  ✓ JOIN ejecutado exitosamente")
        print(f"  ✓ Registros resultantes: {len(resultado):,}")
        
        # Convertir a Polars
        df_resultado = pl.from_pandas(resultado)
        
        return df_resultado
        
    finally:
        conn.close()


def analizar_resultados(df: pl.DataFrame) -> dict:
    """
    Analiza la calidad del JOIN mediante la columna status_match.
    
    Args:
        df: DataFrame con el resultado del JOIN
    
    Returns:
        Diccionario con estadísticas
    """
    print("\n📈 Analizando resultados del JOIN...")
    
    # Contar por status_match
    stats = df.group_by('status_match').agg([
        pl.len().alias('cantidad')
    ]).sort('cantidad', descending=True)
    
    total = len(df)
    
    print("\n  Distribución de matches:")
    for row in stats.iter_rows(named=True):
        status = row['status_match']
        cantidad = row['cantidad']
        porcentaje = (cantidad / total) * 100
        print(f"    {status:20} {cantidad:5,} registros ({porcentaje:5.2f}%)")
    
    # Crear diccionario de estadísticas
    stats_dict = {
        'total': total,
        'ok': stats.filter(pl.col('status_match') == 'OK')['cantidad'].sum() if 'OK' in stats['status_match'] else 0,
        'sin_codigo': stats.filter(pl.col('status_match') == 'SIN_CODIGO')['cantidad'].sum() if 'SIN_CODIGO' in stats['status_match'] else 0,
        'no_encontrado': stats.filter(pl.col('status_match') == 'CODIGO_NO_ENCONTRADO')['cantidad'].sum() if 'CODIGO_NO_ENCONTRADO' in stats['status_match'] else 0
    }
    
    return stats_dict


def guardar_resultados(
    df: pl.DataFrame,
    ruta_gold: Path,
    stats: dict,
    export_excel: bool = False,
):
    """
    Guarda el resultado del JOIN en formatos parquet y Excel con versionamiento:
    - Archivos actuales sin timestamp en gold/
    - Copia con timestamp en gold/historico/
    
    Args:
        df: DataFrame con el resultado del JOIN
        ruta_gold: Path del parquet gold original
        stats: Diccionario con estadísticas del JOIN
    
    Returns:
        tuple: (ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico)
    """
    print("\n💾 Guardando resultados...")
    
    # Carpeta de destino (misma que el parquet gold)
    carpeta_gold = ruta_gold.parent
    
    # Crear carpeta historico/ si no existe
    carpeta_historico = carpeta_gold / "historico"
    carpeta_historico.mkdir(exist_ok=True)
    
    # Timestamp para archivo histórico
    timestamp = datetime.now().strftime("%d.%m.%Y_%H.%M.%S")
    
    print(f"  📁 Carpeta Gold: {carpeta_gold}")
    print(f"  📁 Carpeta Histórico: {carpeta_historico}")
    
    # === ARCHIVOS ACTUALES (sin timestamp) ===
    nombre_actual = "examenes_retiro_gold_enriquecido"
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
    nombre_historico = f"examenes_retiro_gold_enriquecido_{timestamp}"
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


def main():
    """Función principal del script"""
    print("=" * 80)
    print(" ENRIQUECIMIENTO DE EXÁMENES DE RETIRO CON CENTROS DE COSTO ".center(80, "="))
    print("=" * 80)
    
    inicio = time.time()
    
    try:
        # 1. Cargar query SQL
        print("\n[1/5] Carga de Query SQL")
        query = cargar_query_sql()
        
        # 2. Seleccionar parquet Gold
        print("\n[2/5] Selección de Parquet Gold")
        ruta_gold = seleccionar_archivo(
            titulo="Seleccione el parquet Gold de Examenes de Retiro",
            tipos=[("Archivos Parquet", "*.parquet"), ("Todos los archivos", "*.*")]
        )
        print(f"  ✓ Seleccionado: {ruta_gold.name}")
        
        # 3. Seleccionar parquets de Centros de Costo
        print("\n[3/5] Selección de Parquets de Centros de Costo")
        
        print("\n  a) Seleccione CC_ACTUAL:")
        ruta_cc_actual = seleccionar_archivo(
            titulo="Seleccione el parquet CC_ACTUAL",
            tipos=[("Archivos Parquet", "*.parquet"), ("Todos los archivos", "*.*")]
        )
        print(f"    ✓ Seleccionado: {ruta_cc_actual.name}")
        
        print("\n  b) Seleccione CC_OLD:")
        ruta_cc_old = seleccionar_archivo(
            titulo="Seleccione el parquet CC_OLD",
            tipos=[("Archivos Parquet", "*.parquet"), ("Todos los archivos", "*.*")]
        )
        print(f"    ✓ Seleccionado: {ruta_cc_old.name}")
        
        # 4. Cargar parquets
        print("\n[4/5] Procesamiento del JOIN")
        df_examenes, df_cc_actual, df_cc_old = cargar_parquets(
            ruta_gold, ruta_cc_actual, ruta_cc_old
        )
        
        # 5. Ejecutar JOIN
        df_resultado = ejecutar_join_sql(df_examenes, df_cc_actual, df_cc_old, query)
        
        # 6. Analizar resultados
        stats = analizar_resultados(df_resultado)
        
        # 7. Guardar resultados
        print("\n[5/5] Guardado de Resultados")
        ruta_parquet_actual, ruta_excel_actual, ruta_parquet_historico, ruta_excel_historico = guardar_resultados(
            df_resultado, ruta_gold, stats
        )
        
        # Resumen final
        duracion = time.time() - inicio
        print("\n" + "=" * 80)
        print(" RESUMEN ".center(80, "="))
        print("=" * 80)
        
        print(f"\n✓ Proceso completado exitosamente")
        
        print(f"\n⏱️  Tiempo de ejecución: {duracion:.2f} segundos")
        
        print(f"\n📊 Estadísticas de Enriquecimiento:")
        print(f"  - Total registros procesados: {stats['total']:,}")
        print(f"  - Enriquecidos exitosamente:  {stats['ok']:,} ({stats['ok']/stats['total']*100:.1f}%)")
        print(f"  - Sin código CC:              {stats['sin_codigo']:,} ({stats['sin_codigo']/stats['total']*100:.1f}%)")
        print(f"  - Código no encontrado:       {stats['no_encontrado']:,} ({stats['no_encontrado']/stats['total']*100:.1f}%)")
        
        print(f"\n📁 Archivos generados:")
        print(f"\n  Actuales (para Power BI):")
        print(f"    - {ruta_parquet_actual.name}")
        if ruta_excel_actual is not None:
            print(f"    - {ruta_excel_actual.name}")
        
        print(f"\n  Históricos (con timestamp):")
        print(f"    - {ruta_parquet_historico.name}")
        if ruta_excel_historico is not None:
            print(f"    - {ruta_excel_historico.name}")
        
        print(f"\n📂 Ubicación: {ruta_gold.parent}")
        
        print("\n💡 Notas:")
        print("  - Archivos actuales: se sobreescriben en cada ejecución (rutas estables para Power BI)")
        print("  - Archivos históricos: se archivan con timestamp para auditoría")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
