"""
Script de consolidación de reportes de planilla - Régimen Minero
Consolida múltiples archivos Excel en un solo parquet en capa Silver
"""

import polars as pl
import re
from pathlib import Path
from tkinter import Tk, filedialog
import time


def seleccionar_carpeta():
    """Abre diálogo para seleccionar carpeta de trabajo"""
    root = Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    carpeta = filedialog.askdirectory(
        title="Selecciona la carpeta con los archivos de planilla - Régimen Minero"
    )
    root.destroy()
    return carpeta


def extraer_periodo(nombre_archivo):
    """
    Extrae el periodo del nombre del archivo

    Patrón esperado: YYYY-MM en cualquier parte del nombre

    Args:
        nombre_archivo: Nombre del archivo (str)

    Returns:
        str: Periodo en formato YYYY-MM o None si no se encuentra
    """
    # Patrón para capturar YYYY-MM
    patron = r"(\d{4}-\d{2})"
    match = re.search(patron, nombre_archivo)

    if match:
        return match.group(1)
    else:
        return None


def leer_archivo_planilla(archivo_path, periodo):
    """
    Lee un archivo de planilla y agrega la columna PERIODO

    Args:
        archivo_path: Path del archivo Excel
        periodo: Periodo a agregar (str)

    Returns:
        pl.DataFrame: DataFrame con los datos del archivo
    """
    try:
        from openpyxl import load_workbook

        # Leer con openpyxl para obtener encabezados correctos
        wb = load_workbook(archivo_path, data_only=True, read_only=True)

        # Leer específicamente la hoja "Planilla"
        if "Planilla" not in wb.sheetnames:
            raise Exception(
                f"La hoja 'Planilla' no existe en el archivo. Hojas disponibles: {', '.join(wb.sheetnames)}"
            )

        ws = wb["Planilla"]

        # Leer encabezados desde fila 6
        encabezados = []
        for col in range(1, 200):  # Leer hasta columna 200
            cell = ws.cell(row=6, column=col)
            valor = cell.value

            if valor is not None:
                encabezado = str(valor).strip()
                encabezados.append(encabezado)
            else:
                # Si encontramos 3 columnas vacías consecutivas, terminamos
                if col > len(encabezados) + 3:
                    break

        # Leer datos desde fila 7 en adelante
        datos = []
        for row in ws.iter_rows(min_row=7, values_only=True):
            if any(cell is not None for cell in row):
                # Filtrar filas que parecen ser subtítulos o totales
                primera_celda = row[0] if len(row) > 0 else None

                # Si la primera celda es texto largo o None, probablemente no es una fila de datos
                if primera_celda is None:
                    continue
                if isinstance(primera_celda, str) and len(primera_celda) > 20:
                    continue

                datos.append(list(row[: len(encabezados)]))

        wb.close()

        # Crear diccionario para DataFrame
        data_dict = {
            encabezados[i]: [fila[i] if i < len(fila) else None for fila in datos]
            for i in range(len(encabezados))
        }

        # Crear DataFrame con polars usando strict=False para manejar tipos mixtos
        try:
            df = pl.DataFrame(data_dict, strict=False)
        except Exception:
            # Si falla, convertir todo a string primero
            data_dict_str = {
                k: [str(v) if v is not None else None for v in valores]
                for k, valores in data_dict.items()
            }
            df = pl.DataFrame(data_dict_str, strict=False)

        # Agregar columna PERIODO al inicio
        df = df.with_columns(pl.lit(periodo).alias("PERIODO"))

        # Reorganizar para que PERIODO sea la primera columna
        columnas = ["PERIODO"] + [col for col in df.columns if col != "PERIODO"]
        df = df.select(columnas)

        return df

    except Exception as e:
        raise Exception(f"Error al leer {archivo_path.name}: {str(e)}")


def consolidar_archivos(archivos, carpeta_trabajo):
    """
    Consolida múltiples archivos de planilla en un solo DataFrame

    Args:
        archivos: Lista de Path de archivos Excel
        carpeta_trabajo: Path de la carpeta de trabajo

    Returns:
        pl.DataFrame: DataFrame consolidado
    """
    dataframes = []
    archivos_procesados = 0
    archivos_con_error = []

    print(f"\n[1/3] Procesando {len(archivos)} archivo(s)...")

    for idx, archivo in enumerate(archivos, 1):
        try:
            # Extraer periodo del nombre del archivo
            periodo = extraer_periodo(archivo.name)

            if periodo is None:
                print(
                    f"  [{idx}/{len(archivos)}] ⚠️  {archivo.name} - No se pudo extraer periodo, omitiendo..."
                )
                archivos_con_error.append((archivo.name, "No se pudo extraer periodo"))
                continue

            print(
                f"  [{idx}/{len(archivos)}] Procesando: {archivo.name} (Periodo: {periodo})",
                end="",
                flush=True,
            )

            # Leer archivo
            df = leer_archivo_planilla(archivo, periodo)
            dataframes.append(df)
            archivos_procesados += 1

            print(f" ✓ ({len(df)} registros)")

        except Exception as e:
            print(" ✗ ERROR")
            print(f"      Error: {str(e)}")
            archivos_con_error.append((archivo.name, str(e)))

    if not dataframes:
        raise Exception("No se pudo procesar ningún archivo correctamente")

    print(
        f"\n  ✓ Archivos procesados exitosamente: {archivos_procesados}/{len(archivos)}"
    )

    if archivos_con_error:
        print(f"  ⚠️  Archivos con error: {len(archivos_con_error)}")
        for nombre, error in archivos_con_error:
            print(f"      - {nombre}: {error}")

    # Consolidar todos los DataFrames
    print("\n[2/3] Consolidando datos...")

    # Convertir todas las columnas a string para evitar conflictos de tipos
    print("  - Normalizando tipos de datos...")
    dataframes_normalizados = []

    for df in dataframes:
        # Convertir todas las columnas (excepto PERIODO) a string
        columnas_a_convertir = [col for col in df.columns if col != "PERIODO"]
        df_normalizado = df.with_columns(
            [
                pl.col(col).cast(pl.Utf8, strict=False).alias(col)
                for col in columnas_a_convertir
            ]
        )
        dataframes_normalizados.append(df_normalizado)

    # Usar diagonal=True para manejar columnas diferentes entre archivos
    print("  - Concatenando archivos...")
    df_consolidado = pl.concat(dataframes_normalizados, how="diagonal")

    print(f"  ✓ Consolidación completa: {len(df_consolidado):,} registros totales")
    print(f"  ✓ Total de columnas: {len(df_consolidado.columns)}")

    # Limpieza: Eliminar filas con DNI/CEX nulo
    print("\n  - Limpiando datos...")
    registros_antes = len(df_consolidado)

    if "DNI/CEX" in df_consolidado.columns:
        df_consolidado = df_consolidado.filter(pl.col("DNI/CEX").is_not_null())
        registros_despues = len(df_consolidado)
        registros_eliminados = registros_antes - registros_despues

        if registros_eliminados > 0:
            print(f"  ✓ Eliminadas {registros_eliminados:,} filas con DNI/CEX nulo")
        else:
            print("  ✓ No se encontraron filas con DNI/CEX nulo")
    else:
        print("  ⚠️  Advertencia: No se encontró la columna 'DNI/CEX' para limpieza")

    # Agregar columnas MES y AÑO derivadas de PERIODO
    print("  - Generando columnas MES y AÑO desde PERIODO...")

    # Extraer AÑO (primeros 4 caracteres) y MES (últimos 2 caracteres después del guión)
    df_consolidado = df_consolidado.with_columns(
        [
            pl.col("PERIODO").str.slice(0, 4).alias("AÑO"),
            pl.col("PERIODO").str.slice(5, 2).cast(pl.Int32, strict=False).alias("MES"),
        ]
    )

    # Reorganizar columnas: PERIODO, AÑO, MES, resto
    columnas_ordenadas = ["PERIODO", "AÑO", "MES"] + [
        col for col in df_consolidado.columns if col not in ["PERIODO", "AÑO", "MES"]
    ]
    df_consolidado = df_consolidado.select(columnas_ordenadas)

    print("  ✓ Columnas MES y AÑO generadas exitosamente")

    # Guardar en Silver para que el pipeline pueda encadenar el siguiente stage
    print("\n[3/3] Guardando resultados en capa Silver...")
    try:
        ruta_parquet = guardar_resultados(df_consolidado, carpeta_trabajo)
        if not ruta_parquet.exists():
            raise FileNotFoundError(f"No se pudo crear el archivo Parquet: {ruta_parquet}")
    except Exception as e:
        print(f"  ✗ ERROR al guardar resultados: {e}")
        import traceback as _tb
        _tb.print_exc()
        raise

    return df_consolidado


def guardar_resultados(df, carpeta_trabajo):
    """
    Guarda el DataFrame consolidado como parquet en carpeta silver/
    Sin timestamp - se sobreescribe en cada ejecución

    Args:
        df: DataFrame consolidado
        carpeta_trabajo: Path de la carpeta de trabajo

    Returns:
        Path: ruta del parquet generado
    """
    # Crear carpeta silver si no existe
    carpeta_silver = Path(carpeta_trabajo) / "silver"
    carpeta_silver.mkdir(exist_ok=True)

    # Nombres fijos sin timestamp
    nombre_parquet = "Planilla Metso Consolidado - Regimen Minero.parquet"
    # Ruta de salida
    ruta_parquet = carpeta_silver / nombre_parquet

    print("\n[3/3] Guardando resultados en capa Silver...")
    print(f"  📁 Carpeta: {carpeta_silver}")

    # Guardar como Parquet
    print("  - Guardando parquet...", end="", flush=True)
    df.write_parquet(ruta_parquet)
    print(" ✓")
    print(f"    Ubicación: {ruta_parquet}")

    return ruta_parquet


def main():
    print("=" * 70)
    print(
        " CONSOLIDADOR DE PLANILLAS METSO - RÉGIMEN MINERO - CAPA SILVER ".center(
            70, "="
        )
    )
    print("=" * 70)

    # 1. Seleccionar carpeta
    print("\n[PASO 1] Selecciona la carpeta con los archivos de planilla...")
    carpeta = seleccionar_carpeta()

    if not carpeta:
        print("✗ No se seleccionó ninguna carpeta. Proceso cancelado.")
        return

    # Iniciar cronómetro después de la selección
    tiempo_inicio = time.time()

    print(f"✓ Carpeta seleccionada: {carpeta}")

    # 2. Buscar archivos Excel
    print("\n[PASO 2] Buscando archivos Excel...")
    carpeta_path = Path(carpeta)
    archivos_excel = list(carpeta_path.glob("*.xlsx")) + list(
        carpeta_path.glob("*.xls")
    )

    # Filtrar archivos temporales y archivos consolidados previos
    archivos_excel = [
        f
        for f in archivos_excel
        if not f.name.startswith("~$")
        and not f.name.startswith("Planilla Metso Consolidado")
    ]

    if not archivos_excel:
        print("✗ No se encontraron archivos Excel en la carpeta seleccionada")
        return

    print(f"✓ Se encontraron {len(archivos_excel)} archivo(s) Excel")

    # Mostrar archivos encontrados
    print("\nArchivos a procesar:")
    for idx, archivo in enumerate(archivos_excel, 1):
        periodo = extraer_periodo(archivo.name)
        if periodo:
            print(f"  {idx}. {archivo.name} → Periodo: {periodo}")
        else:
            print(f"  {idx}. {archivo.name} → ⚠️  No se detectó periodo")

    # 3. Consolidar archivos
    print("\n" + "=" * 70)
    print(" PROCESAMIENTO ".center(70, "="))
    print("=" * 70)

    try:
        df_consolidado = consolidar_archivos(archivos_excel, carpeta_path)

        # 4. Guardar resultados
        ruta_parquet = guardar_resultados(df_consolidado, carpeta_path)

        # Calcular tiempo total
        tiempo_total = time.time() - tiempo_inicio

        # Resumen final
        print("\n" + "=" * 70)
        print(" RESUMEN ".center(70, "="))
        print("=" * 70)

        print("\n✓ Consolidación completada exitosamente")
        print("\n📊 Estadísticas:")
        print(f"  - Total de registros (final): {len(df_consolidado):,}")
        print(f"  - Total de columnas: {len(df_consolidado.columns)}")
        print(f"  - Periodos únicos: {df_consolidado['PERIODO'].n_unique()}")

        # Mostrar periodos encontrados
        periodos = df_consolidado["PERIODO"].unique().sort().to_list()
        print(f"  - Periodos: {', '.join(periodos)}")

        print("\n📁 Archivos generados en carpeta silver/:")
        print(f"  - Parquet: {ruta_parquet.name}")

        print(f"\n⏱️  Tiempo de ejecución: {tiempo_total:.2f}s")

        print("\n💡 Los archivos se sobreescriben en cada ejecución (sin histórico)")

        print("\n" + "=" * 70)

    except Exception as e:
        print(f"\n✗ Error durante el procesamiento: {str(e)}")
        return


if __name__ == "__main__":
    main()
