# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Directiva: siempre que modifiques algo del etl debes tener en cuenta si eso modifica el DDL. Por lo tanto en ese caso debes modificar el DDL.

## Project Overview

This is a **Medallion Architecture ETL** project that extracts data from Chess ERP API and loads it into a PostgreSQL data warehouse following the bronze/silver/gold pattern. All three layers are implemented and operational.

## Architecture

```
.env (credentials)
       |
orchestrator.py (entry point) + daily_load.py (crontab)
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
├── daily_load.py                # Automated daily load (crontab)
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
│       ├── bronze/loaders/      # Raw data ingestion from API/files
│       ├── silver/transformers/ # JSONB to typed tables
│       └── gold/aggregators/    # Dimensional model (star schema)
├── sql/
│   ├── setup_medallion.sql      # Full DDL
│   └── migrations/              # dbmate incremental migrations
├── data/
│   ├── deposits_b.csv           # Source CSV for deposits
│   └── hectolitros.xlsx         # Source Excel for hectolitros factors
├── tests/                       # pytest tests (144 tests)
├── install_db_arch.sh           # DB installation (Arch Linux)
└── install_db_debian.sh         # DB installation (Debian/Ubuntu)
```

## Key Dependencies

- `pydantic-settings`: Configuration management
- `sqlalchemy`: Database engine and ORM models
- `psycopg2`: PostgreSQL adapter + `execute_values` for bulk inserts
- `python-dateutil`: Date range calculations
- `chesserp`: Chess ERP API client (uses `EMPRESA1_` prefixed env vars)
- `openpyxl`: Excel file reading (hectolitros)

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
python3 orchestrator.py bronze sales 2025-01-01 2025-12-31
python3 orchestrator.py bronze routes
python3 orchestrator.py bronze masters

# Silver (transform)
python3 orchestrator.py silver masters
python3 orchestrator.py silver sales 2025-01-01 2025-12-31
python3 orchestrator.py silver hectolitros

# Gold (aggregate)
python3 orchestrator.py gold dimensions
python3 orchestrator.py gold fact_ventas
python3 orchestrator.py gold fact_stock
python3 orchestrator.py gold cobertura 2025-01
python3 orchestrator.py gold all

# Full pipeline
python3 orchestrator.py all sales 2025-01-01 2025-12-31

# Partial refresh
python3 orchestrator.py partial-refresh-sales 2025-01
```

## Database Setup

```bash
./install_db_arch.sh    # Arch Linux
./install_db_debian.sh  # Debian/Ubuntu
```

## Database Migrations

```bash
# Apply pending migrations
source .env && dbmate --migrations-dir ./sql/migrations migrate

# Rollback last migration
source .env && dbmate --migrations-dir ./sql/migrations rollback
```

dbmate reads `DATABASE_URL` from `.env`.

## Required Environment Variables

```
POSTGRES_USER=
POSTGRES_PASSWORD=
DATABASE=
IP_SERVER=

DATABASE_URL=postgres://user:pass@host:5432/medallion_db?sslmode=disable

ETL_USER=
ETL_PASSWORD=
READONLY_USER=
READONLY_PASSWORD=

EMPRESA1_USERNAME=
EMPRESA1_PASSWORD=
EMPRESA1_API_URL=
```

## Testing

```bash
python3 -m pytest tests/ -v        # All tests (144)
python3 -m pytest tests/test_silver/ -v
python3 -m pytest tests/test_gold/ -v
```

## Important Rules

### Composite Keys (CRITICAL)
IDs are unique **per sucursal**, NOT globally. Always JOIN with `id_sucursal`:

```sql
-- CORRECT
JOIN silver.staff s ON r.id_personal = s.id_personal AND r.id_sucursal = s.id_sucursal
JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor AND fv.id_sucursal = dv.id_sucursal

-- WRONG (will mix data across branches)
JOIN silver.staff s ON r.id_personal = s.id_personal
```

This applies to: `silver.staff`, `silver.routes`, `gold.dim_vendedor`, and any JOIN involving these tables.

### Key Constraints
- `silver.staff`: `UNIQUE(id_personal, id_sucursal)`
- `silver.routes`: `UNIQUE(id_ruta, id_sucursal, id_fuerza_ventas)`
- `silver.client_forces`: `UNIQUE(id_cliente, id_ruta, fecha_inicio)`
- `silver.hectolitros`: `UNIQUE(id_articulo)`
- No FKs in silver/gold (by design in medallion architecture)

### Hectolitros
- Factor lives in `silver.hectolitros` as a separate table
- Gold calculates via LEFT JOIN at INSERT time (not stored in silver facts)
- Column name in gold: `cantidad_total_htls`
- `fact_ventas`: `cantidades_total * factor_hectolitros`
- `fact_stock`: `cant_bultos * factor_hectolitros`
- `dim_articulo`: `factor_hectolitros` (dimensional attribute)

### Other
- Bronze layer processes data **month by month** to avoid API timeouts
- `date_comprobante` column is indexed for fast DELETE by date range
- JSONB is used (not JSON) for query capabilities and indexing
- Routes are loaded for both FV1 and FV4 (`get_routes(fuerza_venta=N)`)
- Only active routes are kept in silver (`fecha_hasta = '9999-12-31'`)
- `facturacion_neta` = `cantidades_total * ABS(precio_unitario_bruto)` (calculated field in silver.fact_ventas)
- Always update DDL (`sql/setup_medallion.sql`) alongside migrations
- Always verify being on `dev` branch before committing
- Commits should use the user's git config, not Claude's

### Daily Load (crontab)

`daily_load.py` runs 10 phases in order:

1. BRONZE MASTERS (all master tables from API)
2. BRONZE VENTAS (current month; also previous month if day <= 3)
3. BRONZE STOCK (reference date only)
4. SILVER MASTERS (all master transformers)
5. SILVER VENTAS (same ranges as phase 2)
6. SILVER STOCK (same range as phase 3)
7. GOLD DIMENSIONES (all 6 dimensions)
8. GOLD FACT_VENTAS (same ranges as phase 2)
9. GOLD FACT_STOCK (same range as phase 3)
10. GOLD COBERTURA (current month period)

**Note:** `gold all` command only runs dimensions + fact_ventas. fact_stock and cobertura must be run separately.

### client_forces

`silver.client_forces` does NOT have `id_sucursal`. The sucursal comes implicitly from the route (JOIN to `silver.routes` to get `id_sucursal`).

## Known Bugs

See `BUGS.md` for the full list of pending bugs organized by priority (P0-P2).
See `INCIDENTS.md` for the history of resolved bugs and incidents.
