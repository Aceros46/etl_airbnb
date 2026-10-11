"""
main.py
-------
Orquestador del proceso ETL completo: Extracción (MongoDB) -> Transformación
(pandas) -> Carga (SQLite + XLSX). Cada ejecución genera un log nuevo en
logs/log_YYYYMMDD_HHMM.txt.

Ejecución (desde la carpeta etl_airbnb/):
    python src/main.py
    python src/main.py --limite-calendar 500000     # prueba rápida
    python src/main.py --xlsx-calendar              # intenta exportar calendar a Excel
"""

import argparse
import sys
import time

from carga import Carga
from extraccion import Extraccion
from logger import configurar_logs, obtener_logger
from transformacion import Transformacion


def ejecutar_etl(limite_calendar: int | None = None, xlsx_calendar: bool = False) -> None:
    """Ejecuta las tres fases del ETL y registra el tiempo de cada una."""
    archivo_log = configurar_logs()
    log = obtener_logger("main")
    inicio = time.perf_counter()
    log.info("INICIO DEL PROCESO ETL - Airbnb Bogotá")

    try:
        # 1. EXTRACCIÓN ------------------------------------------------------
        log.info("---------- FASE 1: EXTRACCIÓN ----------")
        with Extraccion() as extractor:
            crudos = extractor.extraer_todo(limite_calendar=limite_calendar)

        # 2. TRANSFORMACIÓN ------------------------------------------------
        log.info("---------- FASE 2: TRANSFORMACIÓN ----------")
        transformador = Transformacion(crudos)
        limpios = transformador.ejecutar()
        del crudos

        # 3. CARGA ------------------------------------------------------------
        log.info("---------- FASE 3: CARGA ----------")
        excluir = [] if xlsx_calendar else ["calendar"]
        verificacion = Carga().ejecutar(limpios, excluir_xlsx=excluir)
        print("\nVerificación de la carga:\n", verificacion.to_string(index=False))

    except ConnectionError:
        log.error("Proceso ETL detenido: no hay conexión con MongoDB.")
        sys.exit(1)
    except Exception as error:  # noqa: BLE001
        log.error("Proceso ETL detenido por un error inesperado: %s", error, exc_info=True)
        sys.exit(1)

    log.info("FIN DEL PROCESO ETL en %.1f segundos. Log: %s", time.perf_counter() - inicio, archivo_log)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Airbnb Bogotá: MongoDB -> SQLite/XLSX")
    parser.add_argument("--limite-calendar", type=int, default=None,
                        help="Máximo de documentos de Calendar a extraer (pruebas)")
    parser.add_argument("--xlsx-calendar", action="store_true",
                        help="Exportar también el calendario diario a Excel (puede ser muy lento)")
    argumentos = parser.parse_args()
    ejecutar_etl(argumentos.limite_calendar, argumentos.xlsx_calendar)
