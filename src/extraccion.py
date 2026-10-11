"""
extraccion.py
-------------
Fase de EXTRACCIÓN del ETL.

Contiene la clase ``Extraccion``, que:
    1. establece la conexión con la base de datos MongoDB local,
    2. consulta cada colección (Listings, Reviews, Calendar),
    3. carga los documentos en DataFrames de pandas,
    4. registra en el log la conexión y la cantidad de registros extraídos.

Ejemplo:
    >>> from extraccion import Extraccion
    >>> with Extraccion() as ext:
    ...     datos = ext.extraer_todo()
    >>> datos["listings"].shape
"""

from __future__ import annotations

import time

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import COLECCIONES, MONGO_DB, MONGO_URI
from logger import obtener_logger


class Extraccion:
    """Extrae las colecciones de Airbnb Bogotá desde MongoDB hacia pandas.

    Args:
        uri: cadena de conexión de MongoDB.
        nombre_bd: nombre de la base de datos.
        colecciones: diccionario {clave_lógica: nombre_colección}.
        cliente: (opcional) un cliente ya creado, útil para pruebas.
    """

    def __init__(
        self,
        uri: str = MONGO_URI,
        nombre_bd: str = MONGO_DB,
        colecciones: dict[str, str] | None = None,
        cliente: MongoClient | None = None,
    ) -> None:
        self.uri = uri
        self.nombre_bd = nombre_bd
        self.colecciones = colecciones or dict(COLECCIONES)
        self.cliente = cliente
        self.db = None
        self.log = obtener_logger("extraccion")

    # ------------------------------------------------------------------ #
    # Conexión
    # ------------------------------------------------------------------ #
    def conectar(self) -> None:
        """Abre la conexión, la valida con un ``ping`` y verifica las colecciones.

        Raises:
            ConnectionError: si MongoDB no responde.
        """
        try:
            if self.cliente is None:
                self.cliente = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.cliente.admin.command("ping")
            self.db = self.cliente[self.nombre_bd]
            self.log.info("Conexión exitosa a MongoDB (%s) - base de datos '%s'", self.uri, self.nombre_bd)
        except PyMongoError as error:
            self.log.error("Error al conectar con MongoDB (%s): %s", self.uri, error)
            raise ConnectionError(f"No fue posible conectarse a MongoDB: {error}") from error

        existentes = set(self.db.list_collection_names())
        self.log.info("Colecciones encontradas: %s", sorted(existentes))
        faltantes = [c for c in self.colecciones.values() if c not in existentes]
        if faltantes:
            self.log.warning("Colecciones requeridas que NO existen en la BD: %s", faltantes)

    def cerrar(self) -> None:
        """Cierra la conexión con MongoDB."""
        if self.cliente is not None:
            self.cliente.close()
            self.log.info("Conexión a MongoDB cerrada.")

    def __enter__(self) -> "Extraccion":
        self.conectar()
        return self

    def __exit__(self, *exc) -> None:
        self.cerrar()

    # ------------------------------------------------------------------ #
    # Extracción
    # ------------------------------------------------------------------ #
    def extraer_coleccion(
        self,
        clave: str,
        filtro: dict | None = None,
        proyeccion: list[str] | None = None,
        limite: int | None = None,
        tamano_lote: int = 50_000,
    ) -> pd.DataFrame:
        """Consulta una colección y devuelve sus documentos como DataFrame.

        Args:
            clave: 'listings', 'reviews' o 'calendar' (o el nombre real de la colección).
            filtro: filtro de MongoDB, p. ej. ``{"room_type": "Entire home/apt"}``.
            proyeccion: lista de campos a traer (por defecto todos).
            limite: número máximo de documentos (útil para pruebas con Calendar,
                que tiene millones de registros).
            tamano_lote: tamaño del batch del cursor.

        Returns:
            DataFrame sin el campo interno ``_id`` de MongoDB.
        """
        if self.db is None:
            self.conectar()

        nombre = self.colecciones.get(clave, clave)
        inicio = time.perf_counter()
        try:
            campos = {c: 1 for c in proyeccion} if proyeccion else None
            cursor = self.db[nombre].find(filtro or {}, campos).batch_size(tamano_lote)
            if limite:
                cursor = cursor.limit(int(limite))
            df = pd.DataFrame(list(cursor))
        except PyMongoError as error:
            self.log.error("Error extrayendo la colección %s: %s", nombre, error)
            raise

        if "_id" in df.columns:
            df = df.drop(columns="_id")

        segundos = time.perf_counter() - inicio
        if df.empty:
            self.log.warning("La colección %s no devolvió registros.", nombre)
        else:
            self.log.info(
                "Colección %-9s -> %s registros y %d columnas extraídos (%.1f s)",
                nombre, f"{len(df):,}", df.shape[1], segundos,
            )
        return df

    def extraer_todo(self, limite_calendar: int | None = None) -> dict[str, pd.DataFrame]:
        """Extrae Listings, Reviews y Calendar.

        Args:
            limite_calendar: límite opcional de documentos para Calendar.

        Returns:
            Diccionario ``{"listings": df, "reviews": df, "calendar": df}``.
        """
        datos = {
            "listings": self.extraer_coleccion("listings"),
            "reviews": self.extraer_coleccion("reviews"),
            "calendar": self.extraer_coleccion("calendar", limite=limite_calendar),
        }
        total = sum(len(df) for df in datos.values())
        self.log.info("Extracción finalizada: %s registros en total.", f"{total:,}")
        return datos


if __name__ == "__main__":
    # Prueba rápida: python src/extraccion.py
    with Extraccion() as extractor:
        for nombre, tabla in extractor.extraer_todo().items():
            print(nombre, tabla.shape)
