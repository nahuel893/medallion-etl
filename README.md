# Medallion ETL

Pipeline ETL con arquitectura Medallion para extracción de datos desde Chess ERP hacia un Data Warehouse en PostgreSQL.

## Arquitectura Medallion

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  (Raw Data) │     │ (Cleaned)   │     │ (Business)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

| Capa | Descripcion | Estado |
|------|-------------|--------|
| **Bronze** | Datos crudos tal cual llegan de la API | Implementado |
| **Silver** | Datos limpios, validados y normalizados | Implementado |
| **Gold** | Datos agregados listos para consumo | Implementado |

## Estructura del Proyecto

```
medallion-etl/
├── orchestrator.py              # Punto de entrada unico
├── .env                         # Variables de entorno
├── install_db.sh                # Script de instalacion de BD
├── sql/
│   └── setup_medallion.sql      # DDL de schemas y tablas
└── src/
    ├── config/
    │   ├── __init__.py
    │   ├── settings.py          # Configuracion con Pydantic
    │   └── logging_config.py    # Logging estructurado
    ├── database/
    │   ├── __init__.py
    │   ├── engine.py            # Conexion SQLAlchemy
    │   └── models/
    │       ├── bronze.py        # Modelos ORM bronze
    │       └── silver.py        # Modelos ORM silver
    ├── layers/
    │   ├── bronze/loaders/
    │   │   ├── sales_loader.py
    │   │   ├── stock_loader.py
    │   │   ├── clientes_loader.py
    │   │   ├── staff_loader.py
    │   │   ├── routes_loader.py     # Carga FV1 y FV4
    │   │   ├── articles_loader.py
    │   │   ├── depositos_loader.py
    │   │   └── marketing_loader.py
    │   ├── silver/transformers/
    │   │   ├── sales_transformer.py
    │   │   ├── stock_transformer.py
    │   │   ├── clients_transformer.py
    │   │   ├── staff_transformer.py
    │   │   ├── routes_transformer.py
    │   │   ├── articles_transformer.py
    │   │   ├── article_groupings_transformer.py
    │   │   ├── branches_transformer.py
    │   │   ├── client_forces_transformer.py
    │   │   ├── sales_forces_transformer.py
    │   │   └── marketing_transformer.py
    │   └── gold/aggregators/
    │       ├── fact_ventas.py
    │       ├── fact_stock.py
    │       ├── dim_cliente.py
    │       ├── dim_vendedor.py
    │       ├── dim_articulo.py
    │       ├── dim_sucursal.py
    │       ├── dim_tiempo.py
    │       └── cobertura.py
    └── utils/
```

## Instalacion

### 1. Configurar variables de entorno

Crear archivo `.env` en la raiz:

```env
# PostgreSQL
POSTGRES_USER=tu_usuario
POSTGRES_PASSWORD=tu_password
IP_SERVER=localhost
DATABASE=medallion_db

# Usuarios de plataforma
ETL_USER=etl_user
ETL_PASSWORD=tu_password_etl
READONLY_USER=reporting_user
READONLY_PASSWORD=tu_password_reporting

# Chess ERP API
EMPRESA1_USERNAME=usuario_api
EMPRESA1_PASSWORD=password_api
EMPRESA1_API_URL=http://tu-servidor:puerto/
```

### 2. Instalar PostgreSQL y crear BD

```bash
./install_db.sh
```

### 3. Instalar dependencias Python

```bash
pip install sqlalchemy psycopg2-binary pydantic-settings python-dateutil chesserp
```

## Uso

### Orchestrator (punto de entrada unico)

```bash
# === BRONZE (extraccion desde API) ===
# Datos con rango de fechas
python3 orchestrator.py sales 2025-01-01 2025-12-31
python3 orchestrator.py stock 2025-12-01 2025-12-31

# Datos full refresh
python3 orchestrator.py clientes
python3 orchestrator.py staff
python3 orchestrator.py routes        # Carga FV1 y FV4
python3 orchestrator.py articles
python3 orchestrator.py depositos
python3 orchestrator.py marketing

# === SILVER (transformacion) ===
python3 orchestrator.py silver                    # Todas las transformaciones
python3 orchestrator.py silver sales 2025-01-01 2025-12-31  # Solo ventas con rango
python3 orchestrator.py silver routes             # Solo rutas

# === GOLD (agregaciones) ===
python3 orchestrator.py gold                      # Todas las agregaciones

# === PIPELINE COMPLETO ===
python3 orchestrator.py all 2025-01-01 2025-12-31
```

## Loaders Bronze

| Loader | Comando | Tipo de Carga | Fuente | Parametros |
|--------|---------|---------------|--------|------------|
| **sales** | `sales` | Incremental (mes a mes) | API Chess ERP | `<fecha_desde> <fecha_hasta>` |
| **stock** | `stock` | Append (dia a dia x deposito) | API Chess ERP | `<fecha_desde> <fecha_hasta>` |
| **clientes** | `clientes` | Full Refresh | API Chess ERP | - |
| **staff** | `staff` | Full Refresh | API Chess ERP | - |
| **routes** | `routes` | Full Refresh (FV1 + FV4) | API Chess ERP | - |
| **articles** | `articles` | Full Refresh | API Chess ERP | - |
| **marketing** | `marketing` | Full Refresh | API Chess ERP | - |
| **depositos** | `depositos` | Full Refresh | CSV Local | - |

### Tipos de Carga

- **Incremental**: Carga datos por rango de fechas, procesa mes a mes
- **Append**: Agrega datos sin borrar (mantiene historial), procesa dia a dia por deposito
- **Full Refresh**: DELETE + INSERT (reemplaza todos los datos)

## Transformers Silver

Convierten datos crudos JSONB de bronze a tablas tipadas y normalizadas en silver.

| Transformer | Tabla Silver | Tipo |
|-------------|-------------|------|
| sales_transformer | `fact_ventas` | Incremental por fechas |
| stock_transformer | `stock` | Full refresh |
| clients_transformer | `clients` | Full refresh |
| staff_transformer | `staff` | Full refresh |
| routes_transformer | `routes` | Full refresh (solo vigentes: fecha_hasta=9999-12-31) |
| articles_transformer | `articles` | Full refresh |
| article_groupings_transformer | `article_groupings` | Full refresh |
| branches_transformer | `branches` | Full refresh |
| client_forces_transformer | `client_forces` | Full refresh (solo FV1/FV4 vigentes) |
| sales_forces_transformer | `sales_forces` | Full refresh |
| marketing_transformer | `marketing_*` | Full refresh |

### Campos calculados en Silver

- `facturacion_neta`: `cantidades_total * ABS(precio_unitario_bruto)` (en fact_ventas)

## Aggregators Gold

Modelo dimensional (star schema) para consumo analitico.

| Aggregator | Tabla Gold | Descripcion |
|------------|-----------|-------------|
| dim_cliente | `dim_cliente` | Clientes con rutas FV1/FV4, sucursal, marketing |
| dim_vendedor | `dim_vendedor` | Vendedores con fuerza de venta |
| dim_articulo | `dim_articulo` | Articulos con marca y generico |
| dim_sucursal | `dim_sucursal` | Sucursales |
| dim_deposito | `dim_deposito` | Depositos con jerarquia a sucursal |
| dim_tiempo | `dim_tiempo` | Calendario |
| fact_ventas | `fact_ventas` | Ventas desnormalizadas |
| fact_stock | `fact_stock` | Stock por deposito |
| cobertura | `cob_preventista_marca`, `cob_sucursal_marca`, `cob_preventista_generico` | Cobertura comercial |

## Base de Datos

### Schemas

| Schema | Proposito | Acceso ETL | Acceso Reporting |
|--------|-----------|------------|------------------|
| `bronze` | Datos crudos JSONB | Full | Ninguno |
| `silver` | Datos tipados y normalizados | Full | Ninguno |
| `gold` | Modelo dimensional | Full | Solo lectura |

### Constraints importantes

- `silver.routes`: `UNIQUE(id_ruta, id_sucursal, id_fuerza_ventas)` - Una ruta por sucursal y fuerza de venta
- `silver.client_forces`: `UNIQUE(id_cliente, id_ruta, fecha_inicio)` - Asignacion unica cliente-ruta
- Sin FKs entre tablas (por diseño en arquitectura medallion, la integridad la garantiza el ETL)

## Fuerzas de Venta

| Fuerza | ID | Descripcion |
|--------|----|-------------|
| FV1 | 1 | Preventa (cervezas, aguas, vinos CCU, sidras) |
| FV4 | 4 | Autoventa (Fratelli B, vinos, jugos, vinos finos) |

- Las rutas se cargan para ambas FV desde la API (`get_routes(fuerza_venta=N)`)
- Solo se procesan rutas vigentes (`fecha_hasta = '9999-12-31'`)
- Un cliente puede tener rutas asignadas en FV1, FV4 o ambas

## Decisiones Tecnicas

### ORM vs Raw SQL

Los modelos ORM existen para documentacion y consultas SELECT. Para cargas masivas se usa `execute_values` de psycopg2 (~20x mas rapido).

### Procesamiento mensual (Bronze)

La API de Chess ERP puede tener timeouts con rangos grandes. El loader divide automaticamente en consultas mensuales.

### JSONB en Bronze

Se usa JSONB (no JSON) para permitir indexado y consultas sobre campos internos del JSON.

### Sin FKs en Silver/Gold

En arquitectura medallion las FKs no se declaran. La integridad referencial la garantiza el proceso ETL, no la BD. Esto permite cargas masivas sin validacion por fila y sin dependencia de orden.

## Roles y Permisos

| Rol | Proposito | Permisos |
|-----|-----------|----------|
| `etl_user` | Ejecutar pipelines ETL | Full en bronze/silver/gold |
| `reporting_user` | Consultas de negocio | SELECT en gold |
