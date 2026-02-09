# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Directiva: siempre que modifiques algo del etl debes tener en cuenta si eso modifica el DDL. Por lo tanto en ese caso debes modificar el DDL.

## Project Overview

This is a **Medallion Architecture ETL** project that extracts data from Chess ERP API and loads it into a PostgreSQL data warehouse following the bronze/silver/gold pattern. All three layers are implemented and operational.

## Architecture

```
.env (credentials)
       |
orchestrator.py (entry point)
       |
src/config/settings.py -> src/database/engine.py
       |                         |
Settings (Pydantic)      SQLAlchemy engine
                                 |
       +-----------+-------------+-----------+
       |           |                         |
   bronze/     silver/                    gold/
   loaders/    transformers/          aggregators/
       |           |                         |
  API -> JSONB   JSONB -> typed tables   star schema
```

## Project Structure

```
medallion-etl/
├── orchestrator.py              # Single entry point
├── src/
│   ├── config/
│   │   ├── settings.py          # Pydantic settings from .env
│   │   └── logging_config.py    # Structured logging
│   ├── database/
│   │   ├── engine.py            # SQLAlchemy engine
│   │   └── models/
│   │       ├── bronze.py        # ORM models bronze
│   │       └── silver.py        # ORM models silver
│   └── layers/
│       ├── bronze/loaders/      # Raw data ingestion from API
│       ├── silver/transformers/ # JSONB to typed tables
│       └── gold/aggregators/    # Dimensional model (star schema)
├── sql/setup_medallion.sql      # Database DDL
└── install_db.sh                # DB installation script
```

## Key Dependencies

- `pydantic-settings`: Configuration management
- `sqlalchemy`: Database engine and ORM models
- `psycopg2`: PostgreSQL adapter + `execute_values` for bulk inserts
- `python-dateutil`: Date range calculations
- `chesserp`: Chess ERP API client (uses `EMPRESA1_` prefixed env vars)

## Data Loading Pattern

For bulk inserts, use `execute_values` (NOT ORM):

```python
from psycopg2.extras import execute_values
import json

data = [
    (json.dumps(record), 'API_CHESS_ERP', record['fechaComprobate'])
    for record in records
]

execute_values(
    cursor,
    "INSERT INTO bronze.raw_sales (data_raw, source_system, date_comprobante) VALUES %s",
    data,
    template="(%s::jsonb, %s, %s::date)"
)
```

## Running the Pipeline

```bash
# Bronze (extract from API)
python3 orchestrator.py sales 2025-01-01 2025-12-31
python3 orchestrator.py routes    # Loads FV1 and FV4

# Silver (transform)
python3 orchestrator.py silver
python3 orchestrator.py silver sales 2025-01-01 2025-12-31

# Gold (aggregate)
python3 orchestrator.py gold

# Full pipeline
python3 orchestrator.py all 2025-01-01 2025-12-31
```

## Database Setup

```bash
./install_db.sh  # Installs PostgreSQL and creates medallion_db with users
```

## Required Environment Variables

```
POSTGRES_USER=
POSTGRES_PASSWORD=
DATABASE=
IP_SERVER=

ETL_USER=
ETL_PASSWORD=
READONLY_USER=
READONLY_PASSWORD=

EMPRESA1_USERNAME=
EMPRESA1_PASSWORD=
EMPRESA1_API_URL=
```

## Important Notes

- Bronze layer processes data **month by month** to avoid API timeouts
- `date_comprobante` column is indexed for fast DELETE by date range
- JSONB is used (not JSON) for query capabilities and indexing
- Routes are loaded for both FV1 and FV4 (`get_routes(fuerza_venta=N)`)
- Only active routes are kept in silver (`fecha_hasta = '9999-12-31'`)
- No FKs in silver/gold (by design in medallion architecture)
- `silver.routes` constraint: `UNIQUE(id_ruta, id_sucursal, id_fuerza_ventas)`
- Composite keys: IDs are unique **per sucursal**, always JOIN with `id_sucursal`
- `facturacion_neta` = `cantidades_total * ABS(precio_unitario_bruto)` (calculated field in silver.fact_ventas)
