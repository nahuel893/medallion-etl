# Medallion ETL

Pipeline ETL con arquitectura Medallion para extraccion de datos desde Chess ERP hacia un Data Warehouse en PostgreSQL.

## Arquitectura Medallion

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  (Raw Data) │     │ (Cleaned)   │     │ (Business)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

| Capa | Descripcion | Estado |
|------|-------------|--------|
| **Bronze** | Datos crudos tal cual llegan de la API/archivos | Implementado |
| **Silver** | Datos limpios, validados y normalizados | Implementado |
| **Gold** | Modelo dimensional (star schema) para consumo analitico | Implementado |

## Estructura del Proyecto

```
medallion-etl/
├── orchestrator.py              # Punto de entrada unico
├── daily_load.py                # Carga diaria automatizada (crontab)
├── .env                         # Variables de entorno (no versionado)
├── install_db_arch.sh           # Script de instalacion (Arch Linux)
├── install_db_debian.sh         # Script de instalacion (Debian/Ubuntu)
├── requirements.txt             # Dependencias Python
├── sql/
│   ├── setup_medallion.sql      # DDL completo de schemas y tablas
│   └── migrations/              # Migraciones incrementales (dbmate)
├── data/
│   ├── deposits_b.csv           # CSV fuente para depositos
│   └── hectolitros.xlsx         # Excel fuente para factores de conversion
├── src/
│   ├── config/
│   │   ├── settings.py          # Configuracion con Pydantic
│   │   └── logging_config.py    # Logging estructurado
│   ├── database/
│   │   ├── engine.py            # Conexion SQLAlchemy
│   │   └── models/
│   │       ├── bronze.py        # Modelos ORM bronze
│   │       └── silver.py        # Modelos ORM silver
│   └── layers/
│       ├── bronze/loaders/      # Ingesta de datos crudos
│       │   ├── sales_loader.py
│       │   ├── stock_loader.py
│       │   ├── clientes_loader.py
│       │   ├── staff_loader.py
│       │   ├── routes_loader.py
│       │   ├── articles_loader.py
│       │   ├── depositos_loader.py
│       │   ├── marketing_loader.py
│       │   └── hectolitros_loader.py
│       ├── silver/transformers/ # JSONB a tablas tipadas
│       │   ├── sales_transformer.py
│       │   ├── stock_transformer.py
│       │   ├── clients_transformer.py
│       │   ├── staff_transformer.py
│       │   ├── routes_transformer.py
│       │   ├── articles_transformer.py
│       │   ├── article_groupings_transformer.py
│       │   ├── branches_transformer.py
│       │   ├── client_forces_transformer.py
│       │   ├── sales_forces_transformer.py
│       │   ├── marketing_transformer.py
│       │   ├── deposits_transformer.py
│       │   └── hectolitros_transformer.py
│       └── gold/aggregators/    # Modelo dimensional
│           ├── dim_tiempo.py
│           ├── dim_sucursal.py
│           ├── dim_deposito.py
│           ├── dim_vendedor.py
│           ├── dim_articulo.py
│           ├── dim_cliente.py
│           ├── fact_ventas.py
│           ├── fact_stock.py
│           └── cobertura.py
├── tests/
│   ├── test_bronze/             # Tests bronze loaders
│   ├── test_silver/             # Tests silver transformers
│   ├── test_gold/               # Tests gold aggregators
│   ├── test_orchestrator.py
│   └── test_daily_load.py
└── docs/
    ├── plan_htls_gold.md
    └── bugfix_staff_unique_constraint.md
```

## Instalacion

### 1. Configurar variables de entorno

Crear archivo `.env` en la raiz (copiar de `.env.example` si existe):

```env
# PostgreSQL
POSTGRES_USER=tu_usuario
POSTGRES_PASSWORD=tu_password
IP_SERVER=localhost
DATABASE=medallion_db

# dbmate
DATABASE_URL=postgres://usuario:password@localhost:5432/medallion_db?sslmode=disable

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

Hay dos scripts segun la distribucion:

| Distribucion | Script | Comando |
|---|---|---|
| Arch Linux | `install_db_arch.sh` | `./install_db_arch.sh` |
| Debian/Ubuntu | `install_db_debian.sh` | `./install_db_debian.sh` |

Ambos scripts ejecutan los mismos 4 pasos automaticamente:

1. **Instalar PostgreSQL** (y dbmate en Arch)
2. **Crear BD y usuario admin** (via `sudo -u postgres psql`)
3. **Aplicar migraciones** con dbmate (crea schemas bronze/silver/gold y todas las tablas)
4. **Aplicar roles y permisos** (etl_user, readonly_user)

### 3. Crear entorno virtual e instalar dependencias

El proyecto usa un entorno virtual (venv) para aislar las dependencias.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Esto instala todas las dependencias incluyendo [chesserp-api](https://pypi.org/project/chesserp-api/) desde PyPI.

**Importante:** Siempre activar el venv antes de ejecutar el ETL:
```bash
source venv/bin/activate
```

Para crontab, usar la ruta absoluta al Python del venv:
```bash
0 6 * * * cd /ruta/proyecto && venv/bin/python3 daily_load.py
```

### 4. Verificar instalacion

```bash
# Verificar conexion a la BD
psql -h localhost -U $POSTGRES_USER -d medallion_db -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze','silver','gold');"

# Verificar migraciones aplicadas
source .env && dbmate --migrations-dir ./sql/migrations status
```

### Despliegue paso a paso (manual)

Si preferis no usar los scripts automaticos, estos son los pasos detallados:

#### Arch Linux

```bash
# 1. Instalar paquetes
sudo pacman -S postgresql python-pip

# 2. Inicializar cluster PostgreSQL (solo la primera vez)
sudo -u postgres initdb -D /var/lib/postgres/data --locale=en_US.UTF-8

# 3. Arrancar y habilitar servicio
sudo systemctl start postgresql
sudo systemctl enable postgresql

# 4. Crear usuario admin y BD
sudo -u postgres psql -c "CREATE USER tu_usuario WITH PASSWORD 'tu_password' CREATEDB CREATEROLE;"
sudo -u postgres psql -c "CREATE DATABASE medallion_db OWNER tu_usuario;"

# 5. Instalar dbmate (desde AUR o binario)
yay -S dbmate-bin
# O descargar binario: https://github.com/amacneil/dbmate/releases

# 6. Aplicar migraciones (crea schemas y tablas)
source .env && dbmate --migrations-dir ./sql/migrations migrate

# 7. Aplicar permisos
psql -h localhost -U tu_usuario -d medallion_db \
    -v db_name="medallion_db" \
    -v etl_user="etl_user" -v etl_password="password_etl" \
    -v readonly_user="reporting_user" -v readonly_password="password_reporting" \
    -f sql/permissions.sql

# 8. Crear venv e instalar dependencias
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Debian/Ubuntu

```bash
# 1. Instalar paquetes
sudo apt update && sudo apt install -y postgresql postgresql-contrib

# 2. Arrancar servicio
sudo systemctl start postgresql
sudo systemctl enable postgresql

# 3. Crear usuario admin y BD
sudo -u postgres psql -c "CREATE USER tu_usuario WITH PASSWORD 'tu_password' CREATEDB CREATEROLE;"
sudo -u postgres psql -c "CREATE DATABASE medallion_db OWNER tu_usuario;"

# 4. Instalar dbmate
# Ver: https://github.com/amacneil/dbmate#installation
# O via npx: npx dbmate --migrations-dir ./sql/migrations migrate

# 5. Aplicar migraciones (crea schemas y tablas)
source .env && dbmate --migrations-dir ./sql/migrations migrate

# 6. Aplicar permisos
psql -h localhost -U tu_usuario -d medallion_db \
    -v db_name="medallion_db" \
    -v etl_user="etl_user" -v etl_password="password_etl" \
    -v readonly_user="reporting_user" -v readonly_password="password_reporting" \
    -f sql/permissions.sql

# 7. Crear venv e instalar dependencias
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Uso

### Orchestrator (punto de entrada unico)

```bash
# === BRONZE (extraccion desde API/archivos) ===
python3 orchestrator.py bronze sales 2025-01-01 2025-12-31
python3 orchestrator.py bronze stock 2025-01-01 2025-12-31
python3 orchestrator.py bronze clientes
python3 orchestrator.py bronze staff
python3 orchestrator.py bronze routes
python3 orchestrator.py bronze articles
python3 orchestrator.py bronze depositos
python3 orchestrator.py bronze marketing
python3 orchestrator.py bronze hectolitros
python3 orchestrator.py bronze masters           # Todos los maestros

# === SILVER (transformacion) ===
python3 orchestrator.py silver masters           # Todos los maestros
python3 orchestrator.py silver sales 2025-01-01 2025-12-31
python3 orchestrator.py silver stock 2025-01-01 2025-12-31
python3 orchestrator.py silver hectolitros
python3 orchestrator.py silver deposits

# === GOLD (modelo dimensional) ===
python3 orchestrator.py gold dimensions          # Todas las dimensiones (6)
python3 orchestrator.py gold dim_articulo        # Dimension individual
python3 orchestrator.py gold dim_cliente
python3 orchestrator.py gold dim_vendedor
python3 orchestrator.py gold dim_deposito
python3 orchestrator.py gold fact_ventas
python3 orchestrator.py gold fact_stock
python3 orchestrator.py gold cobertura 2025-01   # Las 3 tablas de cobertura
python3 orchestrator.py gold cob_preventista_marca 2025-01
python3 orchestrator.py gold cob_sucursal_marca 2025-01
python3 orchestrator.py gold cob_preventista_generico 2025-01
python3 orchestrator.py gold all                 # Solo dimensiones + fact_ventas (no fact_stock ni cobertura)

# === PIPELINE COMPLETO ===
python3 orchestrator.py all sales 2025-01-01 2025-12-31

# === PARTIAL REFRESH ===
python3 orchestrator.py partial-refresh-sales           # Mes actual
python3 orchestrator.py partial-refresh-sales 2025-01   # Mes especifico
```

### Carga diaria (crontab)

```bash
python3 daily_load.py              # Usa fecha de hoy
python3 daily_load.py 2025-06-15   # Fecha especifica
```

## Loaders Bronze

| Loader | Comando | Tipo de Carga | Fuente |
|--------|---------|---------------|--------|
| **sales** | `bronze sales <desde> <hasta>` | Incremental (mes a mes) | API Chess ERP |
| **stock** | `bronze stock <desde> <hasta>` | Append (dia a dia x deposito) | API Chess ERP |
| **clientes** | `bronze clientes` | Full Refresh | API Chess ERP |
| **staff** | `bronze staff` | Full Refresh | API Chess ERP |
| **routes** | `bronze routes` | Full Refresh (FV1 + FV4) | API Chess ERP |
| **articles** | `bronze articles` | Full Refresh | API Chess ERP |
| **marketing** | `bronze marketing` | Full Refresh | API Chess ERP |
| **depositos** | `bronze depositos` | Full Refresh | CSV Local |
| **hectolitros** | `bronze hectolitros` | Full Refresh | Excel Local |

## Transformers Silver

| Transformer | Tabla Silver | Tipo |
|-------------|-------------|------|
| sales_transformer | `fact_ventas` | Incremental por fechas |
| stock_transformer | `fact_stock` | Full refresh |
| clients_transformer | `clients` | Full refresh |
| staff_transformer | `staff` | Full refresh |
| routes_transformer | `routes` | Full refresh (solo vigentes) |
| articles_transformer | `articles` | Full refresh |
| article_groupings_transformer | `article_groupings` | Full refresh |
| branches_transformer | `branches` | Full refresh |
| client_forces_transformer | `client_forces` | Full refresh |
| sales_forces_transformer | `sales_forces` | Full refresh |
| marketing_transformer | `marketing_*` | Full refresh |
| deposits_transformer | `deposits` | Full refresh |
| hectolitros_transformer | `hectolitros` | Full refresh |

### Campos calculados en Silver

- `facturacion_neta`: `cantidades_total * ABS(precio_unitario_bruto)` (en fact_ventas)

## Aggregators Gold

Modelo dimensional (star schema) para consumo analitico.

### Dimensiones

| Tabla | Descripcion | PK |
|-------|-------------|-----|
| `dim_tiempo` | Calendario | fecha |
| `dim_sucursal` | Sucursales | id_sucursal |
| `dim_deposito` | Depositos con jerarquia a sucursal | id_deposito |
| `dim_vendedor` | Vendedores con fuerza de venta | (id_vendedor, id_sucursal) |
| `dim_articulo` | Articulos con marca, generico, factor_hectolitros | id_articulo |
| `dim_cliente` | Clientes desnormalizados con rutas FV1/FV4, marketing, telefonos | id_cliente |

### Hechos

| Tabla | Descripcion | Campos clave |
|-------|-------------|--------------|
| `fact_ventas` | Lineas de venta | cantidades_total, subtotal_final, cantidad_total_htls |
| `fact_stock` | Stock por deposito/articulo/fecha | cant_bultos, cant_unidades, cantidad_total_htls |

### Cobertura (agregaciones mensuales)

| Tabla | Apertura |
|-------|----------|
| `cob_preventista_marca` | Vendedor/ruta/marca por FV |
| `cob_sucursal_marca` | Sucursal/marca por FV |
| `cob_preventista_generico` | Vendedor/ruta/generico por FV |

## Hectolitros

Factor de conversion de unidades de venta a hectolitros. Flujo:

1. **Bronze**: Excel → `bronze.raw_hectolitros` (JSONB)
2. **Silver**: `silver.hectolitros` (id_articulo, factor_hectolitros)
3. **Gold**: No tiene tabla propia. Se consume via LEFT JOIN:
   - `dim_articulo` → `factor_hectolitros` (atributo dimensional)
   - `fact_ventas` → `cantidad_total_htls = cantidades_total * factor_hectolitros`
   - `fact_stock` → `cantidad_total_htls = cant_bultos * factor_hectolitros`

## Base de Datos

### Schemas

| Schema | Proposito | Acceso ETL | Acceso Reporting |
|--------|-----------|------------|------------------|
| `bronze` | Datos crudos JSONB | Full | Ninguno |
| `silver` | Datos tipados y normalizados | Full | Ninguno |
| `gold` | Modelo dimensional | Full | Solo lectura |

### Constraints importantes

| Tabla | Constraint | Razon |
|-------|-----------|-------|
| `silver.staff` | `UNIQUE(id_personal, id_sucursal)` | IDs unicos por sucursal |
| `silver.routes` | `UNIQUE(id_ruta, id_sucursal, id_fuerza_ventas)` | Ruta por sucursal y FV |
| `silver.client_forces` | `UNIQUE(id_cliente, id_ruta, fecha_inicio)` | Asignacion unica por vigencia |
| `silver.article_groupings` | `UNIQUE(id_articulo, id_forma_agrupar)` | Una agrupacion por tipo |
| `silver.fact_stock` | `UNIQUE(date_stock, id_deposito, id_articulo)` | Un registro por dia/deposito/articulo |
| `silver.hectolitros` | `UNIQUE(id_articulo)` | Un factor por articulo |

Sin FKs entre tablas (por diseno medallion, la integridad la garantiza el ETL).

### Claves compuestas (importante)

Los IDs de vendedor, ruta y staff **no son unicos globalmente**. Son unicos **por sucursal**:

```sql
-- CORRECTO: JOIN con clave compuesta
JOIN gold.dim_vendedor dv
  ON fv.id_vendedor = dv.id_vendedor
  AND fv.id_sucursal = dv.id_sucursal

-- INCORRECTO: JOIN solo por ID (puede mezclar sucursales)
JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor
```

### Migraciones

Se usan migraciones con [dbmate](https://github.com/amacneil/dbmate):

```bash
# Aplicar migraciones pendientes
source .env && dbmate --migrations-dir ./sql/migrations migrate

# Revertir ultima migracion
source .env && dbmate --migrations-dir ./sql/migrations rollback
```

dbmate toma la variable `DATABASE_URL` del `.env`.

## Fuerzas de Venta

| Fuerza | ID | Descripcion |
|--------|----|-------------|
| FV1 | 1 | Preventa (cervezas, aguas, vinos CCU, sidras) |
| FV4 | 4 | Autoventa (Fratelli B, vinos, jugos, vinos finos) |

- Las rutas se cargan para ambas FV desde la API (`get_routes(fuerza_venta=N)`)
- Solo se procesan rutas vigentes (`fecha_hasta = '9999-12-31'`)
- Un cliente puede tener rutas asignadas en FV1, FV4 o ambas

## Testing

```bash
# Correr todos los tests
python3 -m pytest tests/ -v

# Por capa
python3 -m pytest tests/test_silver/ -v
python3 -m pytest tests/test_gold/ -v
```

144 tests cubriendo silver transformers y gold aggregators.

## Decisiones Tecnicas

- **ORM vs Raw SQL**: Los modelos ORM existen para documentacion. Para cargas masivas se usa `execute_values` de psycopg2 (~20x mas rapido).
- **Procesamiento mensual**: La API de Chess ERP puede tener timeouts con rangos grandes. El loader divide en consultas mensuales.
- **JSONB en Bronze**: Se usa JSONB (no JSON) para permitir indexado y consultas sobre campos internos.
- **Sin FKs en Silver/Gold**: Permite cargas masivas sin validacion por fila y sin dependencia de orden.
- **Hectolitros en Gold**: El factor vive en silver como tabla separada. El calculo se hace en gold via LEFT JOIN, asi si cambia el factor solo se re-ejecuta gold.

## Roles y Permisos

| Rol | Proposito | Permisos |
|-----|-----------|----------|
| `etl_user` | Ejecutar pipelines ETL | Full en bronze/silver/gold |
| `reporting_user` | Consultas de negocio | SELECT en gold |
