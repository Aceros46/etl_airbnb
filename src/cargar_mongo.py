"""
cargar_mongo.py
---------------
Paso PREVIO al ETL: crea la base de datos MongoDB local e importa los archivos
fuente de Inside Airbnb (listings.csv.gz, calendar.csv.gz, reviews.csv.gz)
como las colecciones Listings, Calendar y Reviews.

Al final valida que la cantidad de documentos en cada colección coincida con
la cantidad de filas del archivo fuente.

Ejecución (desde la carpeta etl_airbnb/):
    python src/cargar_mongo.py
    python src/cargar_mongo.py --reemplazar      # borra y vuelve a cargar

Alternativa con la herramienta oficial de MongoDB (equivalente):
    mongoimport --db airbnb_bogota --collection Listings --type csv --headerline \
                --file listings.csv   (descomprimido previamente)
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

from config import ARCHIVOS_FUENTE, COLECCIONES, MONGO_DB, MONGO_URI
from logger import configurar_logs, obtener_logger

TAMANO_LOTE = 100_000


def _df_a_documentos(df: pd.DataFrame) -> list[dict]:
    """Convierte un DataFrame en documentos BSON-compatibles (NaN -> None)."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def cargar_archivo(db, clave: str, ruta: Path, reemplazar: bool, log) -> int:
    """Importa un archivo .csv.gz a su colección, por lotes, y devuelve las filas leídas."""
    nombre_coleccion = COLECCIONES[clave]
    coleccion = db[nombre_coleccion]

    if not ruta.exists():
        log.error("No se encontró el archivo fuente %s", ruta)
        return 0

    if coleccion.estimated_document_count() > 0:
        if not reemplazar:
            log.warning("La colección %s ya tiene datos; se omite (use --reemplazar).", nombre_coleccion)
            return coleccion.count_documents({})
        coleccion.drop()
        log.info("Colección %s eliminada para recargarla.", nombre_coleccion)

    filas = 0
    for lote in pd.read_csv(ruta, compression="gzip", chunksize=TAMANO_LOTE, low_memory=False):
        coleccion.insert_many(_df_a_documentos(lote), ordered=False)
        filas += len(lote)
        log.info("  %s: %s filas insertadas...", nombre_coleccion, f"{filas:,}")

    # Validación: filas del archivo vs documentos en MongoDB
    documentos = coleccion.count_documents({})
    if documentos == filas:
        log.info("Validación OK -> %s: %s documentos (= filas del archivo).", nombre_coleccion, f"{documentos:,}")
    else:
        log.error("Validación FALLIDA -> %s: archivo=%s, MongoDB=%s", nombre_coleccion, filas, documentos)
    return documentos


def main() -> None:
    parser = argparse.ArgumentParser(description="Carga los CSV de Inside Airbnb en MongoDB")
    parser.add_argument("--reemplazar", action="store_true", help="Elimina y recarga las colecciones")
    args = parser.parse_args()

    configurar_logs()
    log = obtener_logger("cargar_mongo")
    log.info("Conectando a MongoDB en %s (base de datos: %s)", MONGO_URI, MONGO_DB)

    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.admin.command("ping")
    except Exception as error:  # noqa: BLE001
        log.error("No fue posible conectarse a MongoDB: %s", error)
        sys.exit(1)

    db = cliente[MONGO_DB]
    for clave, ruta in ARCHIVOS_FUENTE.items():
        log.info("Importando %s -> colección %s", ruta.name, COLECCIONES[clave])
        cargar_archivo(db, clave, ruta, args.reemplazar, log)

    # Índices útiles para las consultas posteriores
    db[COLECCIONES["listings"]].create_index("id")
    db[COLECCIONES["calendar"]].create_index("listing_id")
    db[COLECCIONES["reviews"]].create_index("listing_id")
    log.info("Índices creados. Colecciones disponibles: %s", db.list_collection_names())
    cliente.close()


if __name__ == "__main__":
    main()
