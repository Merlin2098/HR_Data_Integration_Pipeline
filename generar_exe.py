"""
Build script for PyInstaller (onedir) aligned with current runtime requirements.
Includes mandatory preflight contracts under assets/validate_source.
"""

import os
import subprocess
import sys
import threading
import time
from pathlib import Path
import shutil

NOMBRE_EXE = "GestorETL.exe"
MAIN_SCRIPT = "etl_manager.py"
DIST_PATH = "dist"
BUILD_PATH = "build"
SPEC_PATH = "spec"

EXCLUSIONES = [
    "test",
    "unittest",
    "scipy",
    "matplotlib",
    "notebook",
    "jupyter",
    "numpy.testing",
    "pandas.tests",
]

ARCHIVOS_REQUERIDOS = [
    "etl_manager.py",
    "assets/config/theme_light.json",
    "assets/config/path_cache.json",
    "assets/queries/queries_flags_gold.sql",
    "src/utils/bd_document_date.py",
    "src/utils/gold_export.py",
    "src/utils/month_name.py",
    "src/utils/validate_source.py",
    "src/orchestrators/pipelines/pipeline_nomina_licencias.yaml",
    "src/orchestrators/pipelines/pipeline_control_practicantes.yaml",
]

CONTRATOS_PREVALIDACION = [
    "assets/validate_source/nomina.yaml",
    "assets/validate_source/licencias.yaml",
    "assets/validate_source/bd.yaml",
    "assets/validate_source/regimen_minero.yaml",
    "assets/validate_source/control_practicantes.yaml",
    "assets/validate_source/ingresos.yaml",
    "assets/validate_source/examen_retiro.yaml",
]


def validar_entorno_virtual():
    print("=" * 60)
    print("VALIDACION DE ENTORNO VIRTUAL")
    print("=" * 60)

    if sys.prefix == sys.base_prefix:
        print("ERROR: No estas dentro de un entorno virtual (venv).")
        print("Activa uno antes de continuar.")
        sys.exit(1)

    print(f"OK entorno virtual detectado: {sys.prefix}\n")


def verificar_estructura():
    print("Verificando estructura del proyecto...")
    base_dir = Path.cwd()

    carpetas_requeridas = [
        "src",
        "assets",
        "assets/validate_source",
        "src/modules",
        "src/orchestrators",
        "src/orchestrators/pipelines",
    ]

    missing = [carpeta for carpeta in carpetas_requeridas if not (base_dir / carpeta).exists()]

    if missing:
        print(f"ERROR: Faltan carpetas criticas: {missing}")
        sys.exit(1)

    archivos_faltantes = [ruta for ruta in ARCHIVOS_REQUERIDOS if not (base_dir / ruta).exists()]
    if archivos_faltantes:
        print(f"ERROR: Faltan archivos criticos: {archivos_faltantes}")
        sys.exit(1)

    contratos_faltantes = [ruta for ruta in CONTRATOS_PREVALIDACION if not (base_dir / ruta).exists()]
    if contratos_faltantes:
        print(f"ERROR: Faltan contratos de prevalidacion: {contratos_faltantes}")
        sys.exit(1)

    print("OK estructura validada.\n")


def limpiar_builds():
    print("Limpiando builds anteriores...")
    for carpeta in [DIST_PATH, BUILD_PATH, SPEC_PATH]:
        path = Path(carpeta)
        if path.exists():
            try:
                shutil.rmtree(path)
            except Exception as exc:
                print(f"No se pudo eliminar {carpeta}: {exc}")
    print("OK limpieza completada.\n")


def _module_name_from_path(base_dir: Path, py_file: Path) -> str:
    rel = py_file.relative_to(base_dir).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def discover_hidden_imports(base_dir: Path) -> list[str]:
    src_root = base_dir / "src"
    modules: set[str] = set()
    runtime_prefixes = ("src.app_main", "src.modules.", "src.orchestrators.", "src.utils.")

    for py_file in src_root.rglob("*.py"):
        if "__pycache__" in py_file.parts:
            continue
        module_name = _module_name_from_path(base_dir, py_file)
        if (
            module_name
            and (module_name == "src" or module_name.startswith(runtime_prefixes))
            and ".tests." not in module_name
            and not module_name.endswith("_test")
        ):
            modules.add(module_name)

    extras = {
        "PySide6.QtCore",
        "PySide6.QtGui",
        "PySide6.QtWidgets",
        "pandas",
        "openpyxl",
        "polars",
        "duckdb",
        "yaml",
    }

    return sorted(modules | extras)


def construir_comando() -> list[str]:
    base_dir = Path.cwd()

    comando = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--onedir",
        "--windowed",
        "--clean",
        "--noconfirm",
        "--log-level",
        "WARN",
        "--distpath",
        DIST_PATH,
        "--workpath",
        BUILD_PATH,
        "--specpath",
        SPEC_PATH,
        "--name",
        NOMBRE_EXE.replace(".exe", ""),
    ]

    # Python search paths
    comando += ["--paths", str(base_dir)]
    comando += ["--paths", str(base_dir / "src")]

    # Hidden imports
    hidden_imports = discover_hidden_imports(base_dir)
    for imp in hidden_imports:
        comando += ["--hidden-import", imp]

    print("Configurando recursos estaticos...")

    assets_path = base_dir / "assets"
    if assets_path.exists():
        comando += ["--add-data", f"{assets_path}{os.pathsep}assets"]
        print(f"  OK assets: {assets_path}")

    pipelines_path = base_dir / "src" / "orchestrators" / "pipelines"
    if pipelines_path.exists():
        comando += [
            "--add-data",
            f"{pipelines_path}{os.pathsep}src/orchestrators/pipelines",
        ]
        print(f"  OK pipelines: {pipelines_path}")

    # Required for runtime ETL discovery by filesystem in ETL registry.
    modules_path = base_dir / "src" / "modules"
    if modules_path.exists():
        comando += ["--add-data", f"{modules_path}{os.pathsep}src/modules"]
        print(f"  OK module tree: {modules_path}")

    ico_path = base_dir / "assets" / "config" / "app.ico"
    if ico_path.exists():
        comando += ["--icon", str(ico_path)]
        print(f"  OK icono: {ico_path}")

    for excl in EXCLUSIONES:
        comando += ["--exclude-module", excl]

    comando.append(str(base_dir / MAIN_SCRIPT))
    return comando


def generar_exe():
    limpiar_builds()
    cmd = construir_comando()

    print("\n" + "=" * 60)
    print("EJECUTANDO PYINSTALLER")
    print("=" * 60)
    print("Este proceso puede tardar varios minutos...\n")

    done = [False]

    def spinner():
        symbols = ["|", "/", "-", "\\"]
        idx = 0
        while not done[0]:
            print(f"\rGenerando... {symbols[idx]}", end="", flush=True)
            idx = (idx + 1) % len(symbols)
            time.sleep(0.1)

    thread = threading.Thread(target=spinner, daemon=True)
    thread.start()

    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        done[0] = True
        time.sleep(0.2)
        print("\r" + " " * 40 + "\r", end="")

        if result.returncode == 0:
            carpeta_final = Path(DIST_PATH) / NOMBRE_EXE.replace(".exe", "")
            exe_final = carpeta_final / NOMBRE_EXE
            print("EXITO: ejecutable generado correctamente.")
            print("=" * 60)
            print(f"Ubicacion: {carpeta_final.absolute()}")
            print(f"Ejecutable: {exe_final.name}")
            print("=" * 60)
            print("Notas:")
            print("1. Distribuye la carpeta completa, no solo el .exe")
            print("2. _internal incluye assets, pipelines y metadatos necesarios")
            print("3. Incluye contratos de prevalidacion en assets/validate_source")
            print("4. Prueba ejecutando el .exe desde la carpeta generada")
        else:
            print("ERROR EN LA COMPILACION")
            print("=" * 60)
            print(result.stderr)

    except Exception as exc:
        done[0] = True
        print(f"\nError de ejecucion: {exc}")


def main():
    try:
        validar_entorno_virtual()
        verificar_estructura()

        print("\n" + "=" * 60)
        print("CONFIGURACION DEL EJECUTABLE")
        print("=" * 60)
        print(f"Nombre: {NOMBRE_EXE}")
        print(f"Entry Point: {MAIN_SCRIPT}")
        print(f"Salida: {DIST_PATH}/")
        print("=" * 60 + "\n")

        confirm = input(f"Generar '{NOMBRE_EXE}' ahora? (S/N): ").strip().lower()
        if confirm in ["s", "si", "y", "yes"]:
            generar_exe()
        else:
            print("Cancelado por el usuario.")

    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    except Exception as exc:
        print(f"\nError inesperado: {exc}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
