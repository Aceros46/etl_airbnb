"""
transformacion.py
-----------------
Fase de TRANSFORMACIÓN del ETL.

Contiene la clase ``Transformacion``, que recibe los DataFrames crudos
extraídos de MongoDB y produce DataFrames limpios listos para la carga:

    listings          -> 1 fila por alojamiento (limpio y enriquecido)
    hosts             -> dimensión de anfitriones (desanidada de listings)
    listing_amenities -> amenities desanidadas (1 fila por alojamiento-amenity)
    calendar          -> calendario diario limpio con variables de fecha
    calendar_mensual  -> calendario resumido por alojamiento y mes
    reviews           -> reseñas limpias con variables de fecha
    reviews_mensual   -> cantidad de reseñas por mes

Todas las decisiones se justifican en los docstrings y comentarios, y cada
paso deja en el log la cantidad de registros antes y después.
"""

from __future__ import annotations

import ast
import json
import re

import numpy as np
import pandas as pd

from config import ETIQUETAS_PRECIO, RANGOS_PRECIO
from logger import obtener_logger

# Columnas de listings que se conservan para el análisis. El resto (URLs,
# descripciones largas, campos 100 % vacíos, ids técnicos del scraping) no
# aporta valor analítico y se descarta.
COLUMNAS_LISTINGS = [
    "id", "name", "host_id", "neighbourhood_cleansed", "latitude", "longitude",
    "property_type", "room_type", "accommodates", "bathrooms", "bathrooms_text",
    "bedrooms", "beds", "amenities", "price", "minimum_nights", "maximum_nights",
    "has_availability", "availability_30", "availability_60", "availability_90",
    "availability_365", "number_of_reviews", "number_of_reviews_ltm", "first_review",
    "last_review", "review_scores_rating", "review_scores_accuracy",
    "review_scores_cleanliness", "review_scores_checkin", "review_scores_communication",
    "review_scores_location", "review_scores_value", "instant_bookable",
    "calculated_host_listings_count", "reviews_per_month", "license", "last_scraped",
]

# Campos con información del anfitrión que vienen "anidados" en cada listing
COLUMNAS_HOST = [
    "host_id", "host_name", "host_since", "host_location", "host_response_time",
    "host_response_rate", "host_acceptance_rate", "host_is_superhost",
    "host_neighbourhood", "host_listings_count", "host_total_listings_count",
    "host_verifications", "host_has_profile_pic", "host_identity_verified",
]

# Amenities más relevantes que se convierten en variables binarias
AMENITIES_CLAVE = {
    "tiene_wifi": r"wifi",
    "tiene_cocina": r"kitchen|cocina",
    "tiene_aire_acondicionado": r"air conditioning|aire acondicionado",
    "tiene_parqueadero": r"parking|parqueadero|estacionamiento",
    "tiene_lavadora": r"washer|lavadora",
    "tiene_piscina": r"\bpool\b|piscina",
    "tiene_espacio_trabajo": r"workspace|espacio de trabajo",
    "tiene_tv": r"\btv\b|television",
}

MAX_NOCHES_AIRBNB = 1125  # máximo de noches que permite la plataforma


class Transformacion:
    """Limpia y transforma los datos de Airbnb Bogotá.

    Args:
        datos: diccionario con los DataFrames crudos ``listings``, ``reviews``
            y ``calendar`` (salida de ``Extraccion.extraer_todo``).

    Uso:
        >>> t = Transformacion(datos)
        >>> limpios = t.ejecutar()
        >>> limpios["listings"].head()
    """

    def __init__(self, datos: dict[str, pd.DataFrame]) -> None:
        self.crudos = {k: v.copy() for k, v in datos.items()}
        self.resultado: dict[str, pd.DataFrame] = {}
        self.resumen: list[dict] = []  # bitácora antes/después de cada tabla
        self.log = obtener_logger("transformacion")

    # ================================================================== #
    # Utilidades genéricas (estáticas, reutilizables desde el notebook)
    # ================================================================== #
    @staticmethod
    def limpiar_precio(serie: pd.Series) -> pd.Series:
        """Normaliza precios: elimina ``$``, ``,`` y espacios y convierte a float.

        Ejemplo: ``"$1,250,000.00"`` -> ``1250000.0``. Valores no convertibles
        quedan como NaN.
        """
        if pd.api.types.is_numeric_dtype(serie):
            return serie.astype(float)
        texto = serie.astype("string").str.replace(r"[\$,\s]", "", regex=True)
        return pd.to_numeric(texto, errors="coerce").astype(float)

    @staticmethod
    def limpiar_porcentaje(serie: pd.Series) -> pd.Series:
        """Convierte ``"95%"`` en ``0.95``."""
        texto = serie.astype("string").str.replace("%", "", regex=False).str.strip()
        return pd.to_numeric(texto, errors="coerce").astype(float) / 100

    @staticmethod
    def a_booleano(serie: pd.Series) -> pd.Series:
        """Convierte los indicadores ``'t'``/``'f'`` de Inside Airbnb en 1/0 (entero con nulos)."""
        mapa = {"t": 1, "f": 0, "true": 1, "false": 0, True: 1, False: 0}
        normalizada = serie.map(lambda v: v.strip().lower() if isinstance(v, str) else v)
        return normalizada.map(mapa).astype("Int64")

    @staticmethod
    def a_fecha(serie: pd.Series) -> pd.Series:
        """Convierte a datetime (errores -> NaT)."""
        return pd.to_datetime(serie, errors="coerce")

    @staticmethod
    def formato_fecha(serie: pd.Series) -> pd.Series:
        """Formatea un datetime al estándar ``YYYY-MM-DD`` (texto) para la carga."""
        return serie.dt.strftime("%Y-%m-%d")

    @staticmethod
    def estandarizar_texto(serie: pd.Series, titulo: bool = False) -> pd.Series:
        """Quita espacios sobrantes, saltos de línea y etiquetas HTML."""
        texto = (
            serie.astype("string")
            .str.replace(r"<[^>]+>", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        return texto.str.title() if titulo else texto

    @staticmethod
    def parsear_lista(valor) -> list[str]:
        """Convierte un texto tipo lista (``'["Wifi", "Kitchen"]'``) en lista de Python."""
        if isinstance(valor, list):
            return [str(v).strip() for v in valor]
        if not isinstance(valor, str) or not valor.strip():
            return []
        for parser in (json.loads, ast.literal_eval):
            try:
                resultado = parser(valor)
                if isinstance(resultado, (list, tuple)):
                    return [str(v).strip() for v in resultado if str(v).strip()]
            except (ValueError, SyntaxError, json.JSONDecodeError):
                continue
        return []

    @staticmethod
    def agregar_variables_fecha(df: pd.DataFrame, columna: str = "date") -> pd.DataFrame:
        """Deriva año, mes, día, trimestre, nombre del mes, día de la semana y fin de semana."""
        fechas = df[columna]
        df["anio"] = fechas.dt.year.astype("Int64")
        df["mes"] = fechas.dt.month.astype("Int64")
        df["dia"] = fechas.dt.day.astype("Int64")
        df["trimestre"] = fechas.dt.quarter.astype("Int64")
        df["anio_mes"] = fechas.dt.strftime("%Y-%m")
        df["dia_semana"] = fechas.dt.dayofweek.astype("Int64")  # 0 = lunes
        df["es_fin_de_semana"] = (fechas.dt.dayofweek >= 5).astype("Int64")
        return df

    def _registrar(self, tabla: str, paso: str, antes: int, despues: int) -> None:
        """Guarda y registra en el log el efecto de un paso sobre el número de filas."""
        self.resumen.append({"tabla": tabla, "paso": paso, "antes": antes, "despues": despues,
                             "eliminados": antes - despues})
        nivel = self.log.warning if antes - despues > 0 else self.log.info
        nivel("[%s] %s: %s -> %s registros (%s eliminados)",
              tabla, paso, f"{antes:,}", f"{despues:,}", f"{antes - despues:,}")

    # ================================================================== #
    # LISTINGS
    # ================================================================== #
    def transformar_listings(self) -> pd.DataFrame:
        """Limpia y enriquece la colección Listings.

        Pasos:
            1. Elimina registros sin ``id`` y duplicados por ``id``.
            2. Desanida la información del host -> tabla ``hosts``.
            3. Selecciona columnas analíticas.
            4. Normaliza ``price`` (quita $ y ,) y anula precios <= 0.
            5. Convierte fechas a datetime y luego a ``YYYY-MM-DD``.
            6. Convierte indicadores t/f a 1/0.
            7. Deriva ``bathrooms`` desde ``bathrooms_text`` cuando falta.
            8. Trata nulos con una estrategia por columna (ver comentarios).
            9. Marca outliers (no se eliminan) y corrige topes imposibles.
           10. Estandariza texto (localidad, tipo de propiedad, nombre).
           11. Categoriza el precio por rangos.
           12. Desanida ``amenities`` -> conteo, variables binarias y tabla larga.
        """
        df = self.crudos["listings"]
        tabla = "listings"
        self.log.info("Transformando LISTINGS (%s registros, %d columnas)", f"{len(df):,}", df.shape[1])

        # 1. Nulos en la llave y duplicados -----------------------------------
        antes = len(df)
        df = df.dropna(subset=["id"])
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["id"])
        self._registrar(tabla, "Eliminar registros sin id", antes, len(df))

        antes = len(df)
        df = df.drop_duplicates(subset="id", keep="last")
        self._registrar(tabla, "Eliminar duplicados por id", antes, len(df))

        # 2. Desanidar host -------------------------------------------------
        self.resultado["hosts"] = self._construir_hosts(df)

        # 3. Selección de columnas -------------------------------------------
        faltantes = [c for c in COLUMNAS_LISTINGS if c not in df.columns]
        if faltantes:
            self.log.warning("[listings] Columnas esperadas no encontradas (se omiten): %s", faltantes)
        df = df[[c for c in COLUMNAS_LISTINGS if c in df.columns]].copy()
        self.log.info("[listings] Columnas conservadas: %d", df.shape[1])

        # 4. Precio ----------------------------------------------------------
        df["price"] = self.limpiar_precio(df["price"])
        invalidos = int((df["price"] <= 0).sum())
        if invalidos:
            self.log.warning("[listings] %d precios <= 0 se reemplazan por NaN", invalidos)
            df.loc[df["price"] <= 0, "price"] = np.nan
        self.log.info("[listings] Precio normalizado a numérico. Nulos en price: %s (%.1f %%)",
                      f"{df['price'].isna().sum():,}", 100 * df["price"].isna().mean())

        # 5. Fechas -----------------------------------------------------------
        for col in ["first_review", "last_review", "last_scraped"]:
            if col in df:
                df[col] = self.formato_fecha(self.a_fecha(df[col]))
        self.log.info("[listings] Fechas convertidas a formato YYYY-MM-DD")

        # 6. Booleanos -------------------------------------------------------
        for col in ["has_availability", "instant_bookable"]:
            if col in df:
                df[col] = self.a_booleano(df[col])

        # 7. Baños -----------------------------------------------------------
        if "bathrooms_text" in df:
            texto = df["bathrooms_text"].astype("string").str.lower()
            numero = pd.to_numeric(texto.str.extract(r"(\d+(?:\.\d+)?)")[0], errors="coerce").astype(float)
            numero = numero.where(~texto.str.contains("half", na=False) | numero.notna(), 0.5)
            if "bathrooms" in df:
                df["bathrooms"] = pd.to_numeric(df["bathrooms"], errors="coerce").fillna(numero)
            else:
                df["bathrooms"] = numero
            df["bano_compartido"] = texto.str.contains("shared|compartido", na=False).astype("Int64")

        # 8. Nulos (estrategia por columna) --------------------------------
        numericas = ["accommodates", "bathrooms", "bedrooms", "beds", "minimum_nights",
                     "maximum_nights", "availability_30", "availability_60", "availability_90",
                     "availability_365", "number_of_reviews", "number_of_reviews_ltm",
                     "reviews_per_month", "calculated_host_listings_count", "latitude", "longitude"]
        numericas += [c for c in df.columns if c.startswith("review_scores_")]
        for col in [c for c in numericas if c in df]:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

        #  - bedrooms/beds/bathrooms: se imputan con la mediana de su room_type
        #    (un apartamento entero y una habitación compartida no son comparables).
        for col in ["bedrooms", "beds", "bathrooms"]:
            if col in df:
                nulos = int(df[col].isna().sum())
                mediana_tipo = df.groupby("room_type")[col].transform("median")
                df[col] = df[col].fillna(mediana_tipo).fillna(df[col].median())
                self.log.info("[listings] %s: %d nulos imputados con la mediana por room_type", col, nulos)

        #  - reviews_per_month es nulo cuando el alojamiento no tiene reseñas -> 0
        if "reviews_per_month" in df:
            df["reviews_per_month"] = df["reviews_per_month"].fillna(0)

        #  - review_scores_*: NO se imputan (un alojamiento sin reseñas no tiene
        #    calificación; imputar sesgaría el análisis). Se crea un indicador.
        df["tiene_reviews"] = (df.get("number_of_reviews", 0) > 0).astype("Int64")

        #  - texto: valores faltantes -> "Sin información"
        for col in ["name", "license", "neighbourhood_cleansed", "property_type", "room_type"]:
            if col in df:
                df[col] = df[col].astype("string").fillna("Sin información")

        #  - registros sin coordenadas no se pueden ubicar -> se eliminan
        antes = len(df)
        df = df.dropna(subset=[c for c in ["latitude", "longitude"] if c in df])
        self._registrar(tabla, "Eliminar registros sin coordenadas", antes, len(df))

        # 9. Outliers y valores imposibles ----------------------------------
        #  - maximum_nights a veces trae 2147483647 (valor centinela) -> tope de Airbnb
        if "maximum_nights" in df:
            excedidos = int((df["maximum_nights"] > MAX_NOCHES_AIRBNB).sum())
            df["maximum_nights"] = df["maximum_nights"].clip(upper=MAX_NOCHES_AIRBNB)
            if excedidos:
                self.log.warning("[listings] %d valores de maximum_nights recortados a %d", excedidos, MAX_NOCHES_AIRBNB)
        #  - minimum_nights > 365 no es un alquiler turístico: se marca como outlier
        df["outlier_minimum_nights"] = (df["minimum_nights"] > 365).astype("Int64")
        df["estancia_larga"] = (df["minimum_nights"] >= 30).astype("Int64")
        #  - availability_365 debe estar entre 0 y 365
        fuera_rango = int((~df["availability_365"].between(0, 365)).sum())
        if fuera_rango:
            self.log.warning("[listings] %d valores de availability_365 fuera de [0, 365] -> NaN", fuera_rango)
            df.loc[~df["availability_365"].between(0, 365), "availability_365"] = np.nan
        #  - precio: regla IQR sobre log(precio) (la distribución es muy asimétrica)
        log_precio = np.log(df["price"])
        q1, q3 = log_precio.quantile([0.25, 0.75])
        lim_inf, lim_sup = np.exp(q1 - 1.5 * (q3 - q1)), np.exp(q3 + 1.5 * (q3 - q1))
        df["outlier_precio"] = ((df["price"] < lim_inf) | (df["price"] > lim_sup)).astype("Int64")
        self.log.info("[listings] Outliers de precio (IQR sobre log): fuera de [%.0f, %.0f] COP -> %d marcados",
                      lim_inf, lim_sup, int(df["outlier_precio"].sum()))

        # 10. Texto ------------------------------------------------------------
        df["name"] = self.estandarizar_texto(df["name"])
        df["neighbourhood_cleansed"] = self.estandarizar_texto(df["neighbourhood_cleansed"], titulo=True)
        df["property_type"] = self.estandarizar_texto(df["property_type"])
        df = df.rename(columns={"neighbourhood_cleansed": "localidad"})

        # 11. Categorización del precio -----------------------------------
        df["categoria_precio"] = pd.cut(df["price"], bins=RANGOS_PRECIO, labels=ETIQUETAS_PRECIO,
                                        right=False).astype("string").fillna("Sin precio")
        df["precio_por_huesped"] = (df["price"] / df["accommodates"].replace(0, np.nan)).round(0)
        self.log.info("[listings] Distribución por categoría de precio: %s",
                      df["categoria_precio"].value_counts().to_dict())

        # 12. Amenities --------------------------------------------------------
        df = self._desanidar_amenities(df)

        self.resultado["listings"] = df.reset_index(drop=True)
        self.log.info("[listings] Resultado final: %s filas x %d columnas", f"{len(df):,}", df.shape[1])
        return self.resultado["listings"]

    def _construir_hosts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Desanida los campos ``host_*`` en una tabla dimensión (1 fila por host)."""
        cols = [c for c in COLUMNAS_HOST if c in df.columns]
        hosts = df[cols].copy()
        hosts["host_id"] = pd.to_numeric(hosts["host_id"], errors="coerce").astype("Int64")
        antes = len(hosts)
        hosts = hosts.dropna(subset=["host_id"]).drop_duplicates(subset="host_id", keep="last")
        self._registrar("hosts", "Desanidar host y eliminar duplicados por host_id", antes, len(hosts))

        if "host_since" in hosts:
            fecha = self.a_fecha(hosts["host_since"])
            hosts["host_since"] = self.formato_fecha(fecha)
            hosts["anios_como_host"] = ((pd.Timestamp.today() - fecha).dt.days / 365.25).round(1)
        for col in ["host_response_rate", "host_acceptance_rate"]:
            if col in hosts:
                hosts[col] = self.limpiar_porcentaje(hosts[col])
        for col in ["host_is_superhost", "host_has_profile_pic", "host_identity_verified"]:
            if col in hosts:
                hosts[col] = self.a_booleano(hosts[col])
        if "host_verifications" in hosts:
            listas = hosts["host_verifications"].map(self.parsear_lista)
            hosts["n_verificaciones"] = listas.map(len).astype("Int64")
            hosts["host_verifications"] = listas.map(", ".join)
        for col in ["host_name", "host_location", "host_neighbourhood", "host_response_time"]:
            if col in hosts:
                hosts[col] = self.estandarizar_texto(hosts[col]).fillna("Sin información")
        self.log.info("[hosts] Tabla de anfitriones creada: %s hosts únicos", f"{len(hosts):,}")
        return hosts.reset_index(drop=True)

    def _desanidar_amenities(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte el texto-lista ``amenities`` en: conteo, flags binarios y tabla larga."""
        if "amenities" not in df:
            self.log.warning("[listings] No existe la columna amenities; se omite el desanidado.")
            return df
        listas = df["amenities"].map(self.parsear_lista)
        df["n_amenities"] = listas.map(len).astype("Int64")
        texto = listas.map(lambda l: " | ".join(l).lower())
        for nueva, patron in AMENITIES_CLAVE.items():
            df[nueva] = texto.str.contains(patron, regex=True).astype("Int64")

        larga = pd.DataFrame({"listing_id": df["id"].values, "amenity": listas.values}).explode("amenity")
        larga = larga.dropna(subset=["amenity"])
        larga["amenity"] = self.estandarizar_texto(larga["amenity"])
        larga = larga.drop_duplicates().reset_index(drop=True)
        self.resultado["listing_amenities"] = larga
        self.log.info("[listing_amenities] Amenities desanidadas: %s filas, %s amenities distintas",
                      f"{len(larga):,}", f"{larga['amenity'].nunique():,}")
        return df.drop(columns="amenities")

    # ================================================================== #
    # CALENDAR
    # ================================================================== #
    def transformar_calendar(self) -> pd.DataFrame:
        """Limpia la colección Calendar y genera el resumen mensual.

        Pasos:
            1. Elimina filas sin listing_id o fecha y duplicados (listing_id, date).
            2. Elimina filas de alojamientos que no existen en listings (integridad).
            3. Convierte ``date`` a fecha y deriva año, mes, día, trimestre, etc.
            4. ``available`` t/f -> ``disponible`` 1/0.
            5. Normaliza ``price``/``adjusted_price``; si falta, usa el precio del
               listing (columna ``precio_imputado`` = 1 para auditarlo).
            6. Completa minimum/maximum_nights con los valores del listing.
            7. Agrega por alojamiento y mes (tabla ``calendar_mensual``).
        """
        df = self.crudos["calendar"]
        tabla = "calendar"
        self.log.info("Transformando CALENDAR (%s registros)", f"{len(df):,}")

        antes = len(df)
        df = df.dropna(subset=["listing_id", "date"])
        df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
        df["date"] = self.a_fecha(df["date"])
        df = df.dropna(subset=["listing_id", "date"])
        self._registrar(tabla, "Eliminar filas sin listing_id o fecha válida", antes, len(df))

        antes = len(df)
        df = df.drop_duplicates(subset=["listing_id", "date"], keep="last")
        self._registrar(tabla, "Eliminar duplicados (listing_id, date)", antes, len(df))

        listings = self.resultado.get("listings")
        if listings is not None:
            antes = len(df)
            df = df[df["listing_id"].isin(listings["id"])]
            self._registrar(tabla, "Integridad referencial con listings", antes, len(df))

        df = self.agregar_variables_fecha(df, "date")
        df["disponible"] = self.a_booleano(df["available"])
        df = df.drop(columns="available")

        for col in ["price", "adjusted_price"]:
            if col in df:
                df[col] = self.limpiar_precio(df[col])
            else:
                df[col] = np.nan
        df["precio_imputado"] = df["price"].isna().astype("Int64")
        if listings is not None:
            precio_listing = df["listing_id"].map(listings.set_index("id")["price"])
            df["price"] = df["price"].fillna(precio_listing)
            for col in ["minimum_nights", "maximum_nights"]:
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(
                        df["listing_id"].map(listings.set_index("id")[col]))
        pct = 100 * df["precio_imputado"].mean() if len(df) else 0
        nivel = self.log.warning if pct > 50 else self.log.info
        nivel("[calendar] %.1f %% de los precios del calendario venían vacíos y se completaron "
              "con el precio del listing", pct)

        df["date"] = self.formato_fecha(df["date"])
        df = df.reset_index(drop=True)
        self.resultado["calendar"] = df

        # Resumen mensual por alojamiento ----------------------------------
        mensual = (
            df.groupby(["listing_id", "anio", "mes", "anio_mes"], observed=True)
            .agg(dias=("date", "count"), dias_disponibles=("disponible", "sum"),
                 precio_promedio=("price", "mean"), precio_maximo=("price", "max"))
            .reset_index()
        )
        mensual["tasa_disponibilidad"] = (mensual["dias_disponibles"] / mensual["dias"]).round(4)
        mensual["tasa_ocupacion_estimada"] = (1 - mensual["tasa_disponibilidad"]).round(4)
        mensual["precio_promedio"] = mensual["precio_promedio"].round(0)
        self.resultado["calendar_mensual"] = mensual
        self.log.info("[calendar_mensual] Calendario agrupado por mes: %s filas", f"{len(mensual):,}")
        return df

    # ================================================================== #
    # REVIEWS
    # ================================================================== #
    def transformar_reviews(self) -> pd.DataFrame:
        """Limpia la colección Reviews y genera el resumen mensual.

        Pasos:
            1. Elimina filas sin id, listing_id o fecha y duplicados por ``id``.
            2. Integridad referencial con listings.
            3. Fecha a ``YYYY-MM-DD`` + variables derivadas.
            4. Limpia ``comments`` (HTML, espacios); vacíos -> "" y se calcula su longitud.
            5. Agrega reseñas por mes (tabla ``reviews_mensual``).
        """
        df = self.crudos["reviews"]
        tabla = "reviews"
        self.log.info("Transformando REVIEWS (%s registros)", f"{len(df):,}")

        antes = len(df)
        for col in ["id", "listing_id", "reviewer_id"]:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        df["date"] = self.a_fecha(df["date"])
        df = df.dropna(subset=["id", "listing_id", "date"])
        self._registrar(tabla, "Eliminar filas sin id, listing_id o fecha", antes, len(df))

        antes = len(df)
        df = df.drop_duplicates(subset="id", keep="last")
        self._registrar(tabla, "Eliminar duplicados por id", antes, len(df))

        listings = self.resultado.get("listings")
        if listings is not None:
            antes = len(df)
            df = df[df["listing_id"].isin(listings["id"])]
            self._registrar(tabla, "Integridad referencial con listings", antes, len(df))

        df = self.agregar_variables_fecha(df, "date")
        sin_comentario = int(df["comments"].isna().sum())
        if sin_comentario:
            self.log.warning("[reviews] %d reseñas sin comentario (se dejan como texto vacío)", sin_comentario)
        df["comments"] = self.estandarizar_texto(df["comments"]).fillna("")
        df["reviewer_name"] = self.estandarizar_texto(df["reviewer_name"]).fillna("Anónimo")
        df["longitud_comentario"] = df["comments"].str.len().astype("Int64")
        df["date"] = self.formato_fecha(df["date"])
        df = df.reset_index(drop=True)
        self.resultado["reviews"] = df

        mensual = (
            df.groupby(["anio", "mes", "anio_mes"], observed=True)
            .agg(n_reviews=("id", "count"), n_listings=("listing_id", "nunique"),
                 longitud_promedio=("longitud_comentario", "mean"))
            .reset_index()
        )
        mensual["longitud_promedio"] = mensual["longitud_promedio"].round(1)
        self.resultado["reviews_mensual"] = mensual
        self.log.info("[reviews_mensual] Reseñas agrupadas por mes: %s filas", f"{len(mensual):,}")
        return df

    # ================================================================== #
    # Orquestación
    # ================================================================== #
    def ejecutar(self) -> dict[str, pd.DataFrame]:
        """Ejecuta todas las transformaciones (listings primero, por integridad).

        Returns:
            Diccionario con los DataFrames limpios listos para la carga.
        """
        pasos = [("listings", self.transformar_listings),
                 ("calendar", self.transformar_calendar),
                 ("reviews", self.transformar_reviews)]
        for nombre, funcion in pasos:
            if nombre not in self.crudos or self.crudos[nombre].empty:
                self.log.warning("No hay datos de %s; se omite su transformación.", nombre)
                continue
            try:
                funcion()
            except Exception as error:  # noqa: BLE001
                self.log.error("Error transformando %s: %s", nombre, error, exc_info=True)
                raise
        self.log.info("Transformación finalizada. Tablas generadas: %s",
                      {k: len(v) for k, v in self.resultado.items()})
        return self.resultado

    def resumen_df(self) -> pd.DataFrame:
        """Devuelve la bitácora de registros antes/después como DataFrame."""
        return pd.DataFrame(self.resumen)
