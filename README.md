# ETL Airbnb Bogotá – MongoDB → Python → SQLite / XLSX

Proyecto del **Taller 2 de Inteligencia de Negocios**: proceso ETL (Extracción, Transformación y Carga)
sobre los datasets de Inside Airbnb para **Bogotá, Distrito Capital, Colombia**.

## Objetivo
Aplicar los conceptos de ETL sobre los datasets de Airbnb Bogotá almacenados en una base de datos
**MongoDB local**, mediante un proceso automatizado en Python con manejo de logs, análisis exploratorio
de datos (EDA) y documentación del flujo de trabajo.

## Flujo del proceso

```
 listings.csv.gz ┐                          ┌─────────────┐   ┌────────────────┐   ┌──────────────────────┐
 calendar.csv.gz ├─ cargar_mongo.py ──────► │   MongoDB   │──►│  Extraccion    │──►│  Transformacion      │
 reviews.csv.gz  ┘  (carga inicial)         │ Listings    │   │ (pandas)       │   │ limpieza, precios,   │
                                            │ Calendar    │   └────────────────┘   │ fechas, categorías,  │
                                            │ Reviews     │                        │ amenities, hosts     │
                                            └─────────────┘                        └──────────┬───────────┘
                                                                                              ▼
                                       logs/log_YYYYMMDD_HHMM.txt  ◄── todas las fases   ┌───────────┐
                                                                                         │  Carga    │──► output/airbnb_bogota.db (SQLite)
                                                                                         └───────────┘──► output/xlsx/*.xlsx
```

## Estructura del repositorio

```
etl_airbnb/
├── src/
│   ├── config.py           # Configuración (URI de Mongo, rutas, rangos de precio)
│   ├── logger.py           # Módulo centralizado de logs (1 archivo por ejecución)
│   ├── cargar_mongo.py     # Paso previo: importa los .csv.gz a MongoDB y valida
│   ├── extraccion.py       # Clase Extraccion
│   ├── transformacion.py   # Clase Transformacion
│   ├── carga.py            # Clase Carga
│   └── main.py             # Orquestador del ETL completo
├── notebooks/
│   └── exploracion_airbnb.ipynb   # EDA con visualizaciones e interpretación
├── data/raw/               # Aquí se ubican listings/calendar/reviews.csv.gz (no se suben a Git)
├── logs/                   # log_YYYYMMDD_HHMM.txt
├── output/                 # airbnb_bogota.db y carpeta xlsx/ (generados)
├── reportes/figuras/       # Gráficas exportadas desde el notebook para el informe
├── README.md
└── requirements.txt
```

## Instalación

Requisitos: **Python 3.10+** y **MongoDB Community Server** corriendo en `localhost:27017`
(opcional: MongoDB Compass para inspeccionar las colecciones).

### 1. Clonar y crear el entorno virtual

```bash
git clone https://github.com/<usuario>/etl_airbnb.git
cd etl_airbnb

# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Datos fuente
Descargar de http://insideairbnb.com/get-the-data (Colombia → Bogotá) los archivos
`listings.csv.gz`, `calendar.csv.gz` y `reviews.csv.gz` (versión *detailed*) y copiarlos en `data/raw/`.

## Ejecución

Todos los comandos se ejecutan desde la carpeta `etl_airbnb/`.

### Paso 0 – Cargar la base de datos MongoDB (una sola vez)

```bash
python src/cargar_mongo.py              # crea la BD airbnb_bogota con Listings, Calendar y Reviews
python src/cargar_mongo.py --reemplazar # para volver a cargarla desde cero
```

El script valida que el número de documentos de cada colección sea igual al número de filas del
archivo. Si MongoDB usa otra URI o base de datos:

```bash
# Windows (PowerShell)
$env:MONGO_URI="mongodb://localhost:27017"; $env:MONGO_DB="airbnb_bogota"
# macOS / Linux
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="airbnb_bogota"
```

### Paso 1 – Análisis exploratorio

```bash
jupyter notebook notebooks/exploracion_airbnb.ipynb
```

### Paso 2 – Proceso ETL completo

```bash
python src/main.py                           # ETL completo
python src/main.py --limite-calendar 500000  # prueba rápida con parte del calendario
python src/main.py --xlsx-calendar           # intenta exportar también el calendario diario a Excel
```

### Uso de las clases por separado

```python
from extraccion import Extraccion
from transformacion import Transformacion
from carga import Carga

with Extraccion() as ext:
    crudos = ext.extraer_todo()

limpios = Transformacion(crudos).ejecutar()
verificacion = Carga().ejecutar(limpios, excluir_xlsx=["calendar"])
```

## Ejemplo de ejecución (extracto del log)

```
2026-10-09 16:14:19 | INFO     | etl_airbnb.main            | ---------- FASE 1: EXTRACCIÓN ----------
2026-10-09 16:14:19 | INFO     | etl_airbnb.extraccion      | Conexión exitosa a MongoDB (mongodb://localhost:27017) - base de datos 'airbnb_bogota'
2026-10-09 16:14:19 | INFO     | etl_airbnb.extraccion      | Colección Listings  -> 805 registros y 60 columnas extraídos (0.0 s)
2026-10-09 16:17:14 | WARNING  | etl_airbnb.transformacion  | [listings] Eliminar duplicados por id: 805 -> 800 registros (5 eliminados)
2026-10-09 16:17:14 | INFO     | etl_airbnb.transformacion  | [listings] Precio normalizado a numérico. Nulos en price: 97 (12.1 %)
2026-10-09 16:17:14 | WARNING  | etl_airbnb.transformacion  | [listings] 42 valores de maximum_nights recortados a 1125
2026-10-09 16:17:14 | WARNING  | etl_airbnb.transformacion  | [reviews] Eliminar duplicados por id: 15,020 -> 15,000 registros (20 eliminados)
2026-10-09 16:17:15 | INFO     | etl_airbnb.carga           | Verificación SQLite OK  -> listings           800 registros
2026-10-09 16:17:15 | WARNING  | etl_airbnb.carga           | Tabla calendar excluida del XLSX por tamaño; disponible en SQLite.
2026-10-09 16:17:18 | INFO     | etl_airbnb.carga           | Carga finalizada correctamente: todos los conteos coinciden.
```
> Reemplazar este extracto por el log real de su ejecución (carpeta `logs/`).

## Tablas generadas (SQLite y XLSX)

| Tabla | Granularidad | Descripción |
|---|---|---|
| `listings` | 1 fila por alojamiento | Limpia, con precio numérico, `categoria_precio`, flags de outliers y amenities |
| `hosts` | 1 fila por anfitrión | Información del host desanidada de listings |
| `listing_amenities` | alojamiento × amenity | Amenities desanidadas |
| `calendar` | alojamiento × día | Disponibilidad diaria con año, mes, día, trimestre (solo SQLite por tamaño) |
| `calendar_mensual` | alojamiento × mes | Días disponibles, tasa de ocupación estimada y precio promedio |
| `reviews` | 1 fila por reseña | Reseñas limpias con variables de fecha |
| `reviews_mensual` | 1 fila por mes | Número de reseñas y alojamientos reseñados por mes |

## Integrantes y responsabilidades

| Integrante | Responsabilidad |
|---|---|
| _[Nombre 1]_ | Carga a MongoDB y clase `Extraccion` |
| _[Nombre 2]_ | Análisis exploratorio (notebook) |
| _[Nombre 3]_ | Clase `Transformacion` y logs |
| _[Nombre 4]_ | Clase `Carga`, README e informe final |

## Fuente de datos
Inside Airbnb – Bogotá, Distrito Capital, Colombia. http://insideairbnb.com/get-the-data (licencia CC BY 4.0).
