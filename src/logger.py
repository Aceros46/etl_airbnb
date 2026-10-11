"""
logger.py
---------
Módulo centralizado de logs para todo el proceso ETL.

- Genera UN archivo por ejecución: logs/log_YYYYMMDD_HHMM.txt
- Formato: fecha y hora | nivel (INFO / WARNING / ERROR) | módulo | mensaje
- También muestra los mensajes en consola.

Uso:
    from logger import obtener_logger
    log = obtener_logger("extraccion")
    log.info("Mensaje")
"""

import logging
from datetime import datetime
from pathlib import Path

from config import LOGS_DIR

_NOMBRE_RAIZ = "etl_airbnb"
_FORMATO = "%(asctime)s | %(levelname)-8s | %(name)-26s | %(message)s"
_FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"
_archivo_log_actual: Path | None = None


def configurar_logs(directorio: Path = LOGS_DIR, nivel: int = logging.INFO) -> Path:
    """Configura el logger raíz del ETL (solo una vez por ejecución).

    Crea el archivo ``log_YYYYMMDD_HHMM.txt`` dentro de ``directorio`` y añade
    un handler de consola. Si ya fue configurado, devuelve el archivo actual.

    Returns:
        Path: ruta del archivo de log de esta ejecución.
    """
    global _archivo_log_actual
    raiz = logging.getLogger(_NOMBRE_RAIZ)
    if _archivo_log_actual is not None and raiz.handlers:
        return _archivo_log_actual

    directorio = Path(directorio)
    directorio.mkdir(parents=True, exist_ok=True)
    _archivo_log_actual = directorio / f"log_{datetime.now():%Y%m%d_%H%M}.txt"

    formato = logging.Formatter(_FORMATO, datefmt=_FORMATO_FECHA)

    manejador_archivo = logging.FileHandler(_archivo_log_actual, mode="a", encoding="utf-8")
    manejador_archivo.setFormatter(formato)

    manejador_consola = logging.StreamHandler()
    manejador_consola.setFormatter(formato)

    raiz.setLevel(nivel)
    raiz.addHandler(manejador_archivo)
    raiz.addHandler(manejador_consola)
    raiz.propagate = False

    raiz.info("=" * 70)
    raiz.info("Nueva ejecución del ETL. Archivo de log: %s", _archivo_log_actual.name)
    return _archivo_log_actual


def obtener_logger(nombre: str) -> logging.Logger:
    """Devuelve un logger hijo (p. ej. ``etl_airbnb.extraccion``).

    Si los logs aún no se han configurado (p. ej. al usar una clase desde el
    notebook), los configura automáticamente.
    """
    configurar_logs()
    return logging.getLogger(f"{_NOMBRE_RAIZ}.{nombre}")
