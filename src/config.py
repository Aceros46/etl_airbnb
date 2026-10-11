"""
config.py
---------
Configuración centralizada del proceso ETL.

Los valores pueden sobreescribirse con variables de entorno, de modo que cada
integrante del grupo pueda ejecutar el proyecto sin modificar el código:

    MONGO_URI      -> cadena de conexión (por defecto mongodb://localhost:27017)
    MONGO_DB       -> nombre de la base de datos (por defecto airbnb_bogota)
"""

import os
from pathlib import Path

# Rutas base del proyecto (etl_airbnb/)
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "airbnb_bogota")

# Nombres de las colecciones exigidas por el taller
COLECCIONES = {
    "listings": "Listings",
    "reviews": "Reviews",
    "calendar": "Calendar",
}

# Archivos fuente de Inside Airbnb (se usan solo para la carga inicial a MongoDB)
ARCHIVOS_FUENTE = {
    "listings": DATA_RAW_DIR / "listings.csv.gz",
    "reviews": DATA_RAW_DIR / "reviews.csv.gz",
    "calendar": DATA_RAW_DIR / "calendar.csv.gz",
}

# Destinos de la carga
SQLITE_PATH = OUTPUT_DIR / "airbnb_bogota.db"
XLSX_DIR = OUTPUT_DIR / "xlsx"

# Rangos de precio por noche en pesos colombianos (COP) para la categorización
RANGOS_PRECIO = [0, 100_000, 200_000, 400_000, 800_000, float("inf")]
ETIQUETAS_PRECIO = ["Económico", "Moderado", "Medio-alto", "Alto", "Lujo"]
