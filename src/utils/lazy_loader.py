# utils/lazy_loader.py
"""
Lazy Loader - Carga perezosa de módulos
Permite importar módulos solo cuando se necesitan, optimizando memoria y tiempo de inicio
"""

import importlib
from typing import Any, Dict, Callable
from functools import wraps


class LazyModule:
    """
    Wrapper para carga perezosa de un módulo

    Ejemplo:
        lazy_step1 = LazyModule('src.modules.nomina.steps.step1_consolidar_planillas')
        # El módulo NO se carga hasta que se accede
        result = lazy_step1.consolidar_archivos(args)  # AHORA se carga
    """

    def __init__(self, module_path: str):
        """
        Args:
            module_path: Ruta del módulo a cargar (ej: 'src.modules.nomina.steps.step1_consolidar_planillas')
        """
        self._module_path = module_path
        self._module = None
        self._loaded = False

    def _load(self):
        """Carga el módulo si aún no está cargado"""
        if not self._loaded:
            try:
                self._module = importlib.import_module(self._module_path)
                self._loaded = True
            except ImportError as e:
                raise ImportError(
                    f"No se pudo cargar el módulo '{self._module_path}': {e}"
                )

    def __getattr__(self, name: str) -> Any:
        """
        Acceso a atributos del módulo (funciones, clases, variables)
        Carga el módulo automáticamente en el primer acceso
        """
        self._load()
        return getattr(self._module, name)

    def is_loaded(self) -> bool:
        """Retorna True si el módulo ya fue cargado"""
        return self._loaded

    def reload(self):
        """Recarga el módulo (útil en desarrollo)"""
        if self._loaded and self._module:
            self._module = importlib.reload(self._module)


class LazyLoader:
    """
    Gestor de carga perezosa de módulos para ETLs

    Ejemplo de uso:
        loader = LazyLoader()
        loader.register('step1', 'src.modules.nomina.steps.step1_consolidar_planillas')
        loader.register('step2', 'src.modules.nomina.steps.step2_exportar')

        # Los módulos NO se cargan hasta usarlos
        df = loader.step1.consolidar_archivos(files, output_dir)
        df_gold = loader.step2.seleccionar_y_convertir_columnas(df, schema)
    """

    def __init__(self):
        self._modules: Dict[str, LazyModule] = {}

    def register(self, name: str, module_path: str):
        """
        Registra un módulo para carga perezosa

        Args:
            name: Nombre corto para acceder al módulo
            module_path: Ruta completa del módulo
        """
        self._modules[name] = LazyModule(module_path)

    def __getattr__(self, name: str) -> LazyModule:
        """Acceso a módulos registrados"""
        if name in self._modules:
            return self._modules[name]
        raise AttributeError(
            f"Módulo '{name}' no registrado. "
            f"Módulos disponibles: {list(self._modules.keys())}"
        )

    def is_loaded(self, name: str) -> bool:
        """Verifica si un módulo ya fue cargado"""
        if name in self._modules:
            return self._modules[name].is_loaded()
        return False

    def get_loaded_modules(self) -> list:
        """Retorna lista de módulos que ya fueron cargados"""
        return [name for name, module in self._modules.items() if module.is_loaded()]

    def reload_all(self):
        """Recarga todos los módulos cargados (útil en desarrollo)"""
        for module in self._modules.values():
            if module.is_loaded():
                module.reload()


def lazy_import(module_path: str) -> Callable:
    """
    Decorador para importación perezosa de funciones

    Ejemplo:
        @lazy_import('src.modules.nomina.steps.step1_consolidar_planillas')
        def consolidar_wrapper(consolidar_archivos):
            return consolidar_archivos(files, output_dir)

        # La función se importa solo cuando se llama a consolidar_wrapper
        result = consolidar_wrapper()
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            module = importlib.import_module(module_path)
            # Inyectar funciones del módulo como primer argumento
            return func(module, *args, **kwargs)

        return wrapper

    return decorator


# ============================================================================
# UTILIDADES PARA ETL WORKERS
# ============================================================================


def create_etl_loader(etl_name: str, steps: Dict[str, str]) -> LazyLoader:
    """
    Crea un LazyLoader preconfigurado para un ETL específico

    Args:
        etl_name: Nombre del ETL (ej: 'nomina')
        steps: Diccionario de steps {nombre: ruta_modulo}

    Returns:
        LazyLoader configurado

    Ejemplo:
        loader = create_etl_loader('nomina', {
            'step1': 'src.modules.nomina.steps.step1_consolidar_planillas',
            'step2': 'src.modules.nomina.steps.step2_exportar'
        })

        # Uso en worker
        df = loader.step1.consolidar_archivos(files, output_dir)
    """
    loader = LazyLoader()
    for step_name, module_path in steps.items():
        loader.register(step_name, module_path)
    return loader


def get_function_lazy(module_path: str, function_name: str) -> Callable:
    """
    Obtiene una función específica de un módulo sin cargarlo completamente

    Args:
        module_path: Ruta del módulo
        function_name: Nombre de la función

    Returns:
        Función callable

    Ejemplo:
        consolidar = get_function_lazy(
            'src.modules.nomina.steps.step1_consolidar_planillas',
            'consolidar_archivos'
        )
        result = consolidar(files, output_dir)  # AHORA se carga el módulo
    """
    lazy_module = LazyModule(module_path)
    return getattr(lazy_module, function_name)


# ============================================================================
# MONITOREO DE CARGA
# ============================================================================


class LoadMonitor:
    """
    Monitor para medir tiempo de carga de módulos
    Útil para detectar módulos lentos
    """

    def __init__(self):
        self._timings: Dict[str, float] = {}

    def wrap_loader(self, loader: LazyLoader) -> LazyLoader:
        """Envuelve un LazyLoader para monitorear tiempos de carga"""
        original_getattr = loader.__getattr__

        def monitored_getattr(name: str):
            import time

            start = time.time()
            module = original_getattr(name)

            # Si no estaba cargado, registrar tiempo
            if name not in self._timings:
                self._timings[name] = time.time() - start

            return module

        loader.__getattr__ = monitored_getattr
        return loader

    def get_timings(self) -> Dict[str, float]:
        """Retorna tiempos de carga de cada módulo"""
        return self._timings.copy()

    def print_report(self):
        """Imprime reporte de tiempos de carga"""
        if not self._timings:
            print("No se han cargado módulos aún")
            return

        print("\n" + "=" * 50)
        print("REPORTE DE CARGA DE MÓDULOS")
        print("=" * 50)

        for module, duration in sorted(
            self._timings.items(), key=lambda x: x[1], reverse=True
        ):
            print(f"  {module:30s} : {duration * 1000:6.2f} ms")

        total = sum(self._timings.values())
        print("-" * 50)
        print(f"  {'TOTAL':30s} : {total * 1000:6.2f} ms")
        print("=" * 50)


# ============================================================================
# EJEMPLO DE USO
# ============================================================================

if __name__ == "__main__":
    print("=== EJEMPLO 1: LazyModule básico ===")

    # Simular carga perezosa
    lazy_json = LazyModule("json")
    print(f"Módulo cargado: {lazy_json.is_loaded()}")  # False

    # Al acceder a una función, se carga el módulo
    data = lazy_json.dumps({"test": 123})
    print(f"Módulo cargado: {lazy_json.is_loaded()}")  # True
    print(f"Resultado: {data}")

    print("\n=== EJEMPLO 2: LazyLoader para ETL ===")

    # Crear loader para ETL de nómina
    loader = create_etl_loader(
        "nomina",
        {
            "step1": "src.modules.nomina.steps.step1_consolidar_planillas",
            "step2": "src.modules.nomina.steps.step2_exportar",
        },
    )

    print(f"Módulos cargados: {loader.get_loaded_modules()}")  # []

    # Simular acceso (esto cargaría el módulo en producción)
    # df = loader.step1.consolidar_archivos(files, output)

    print("\n=== EJEMPLO 3: Monitor de carga ===")

    monitor = LoadMonitor()
    loader_monitored = monitor.wrap_loader(loader)

    # Uso normal del loader
    # loader_monitored.step1.consolidar_archivos(...)

    # Ver reporte de tiempos
    monitor.print_report()
