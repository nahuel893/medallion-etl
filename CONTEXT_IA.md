# Contexto del Data Warehouse - Medallion ETL

## Resumen del Proyecto

Este es un Data Warehouse construido con **arquitectura Medallion** (Bronze - Silver - Gold) que extrae datos de un ERP de distribucion comercial (Chess ERP) y los transforma para analisis.

## Arquitectura de Capas

```
Bronze (Raw)     ->     Silver (Clean)     ->     Gold (Analytics)
---------------------------------------------------------------------
Datos crudos JSON      Datos normalizados      Modelo dimensional
Sin transformacion     Tipados y limpios       Star Schema
```

## Ejecucion

```bash
# Carga diaria automatizada (crontab)
python3 daily_load.py              # Usa fecha de hoy
python3 daily_load.py 2025-06-15   # Fecha especifica

# Carga manual por capa/entidad
python3 orchestrator.py <capa> <entidad> [args...]

# Ejemplos
python3 orchestrator.py bronze sales 2025-01-01 2025-12-31
python3 orchestrator.py silver masters
python3 orchestrator.py gold dimensions
python3 orchestrator.py gold fact_ventas
python3 orchestrator.py gold cobertura 2025-01
python3 orchestrator.py partial-refresh-sales 2025-01
```

El `daily_load.py` ejecuta el pipeline completo Bronze -> Silver -> Gold en 10 fases.
Recarga el mes actual completo de ventas en cada ejecucion, y si es dia 1-3 tambien el mes anterior.

## Esquema Silver (Tablas normalizadas)

### Tablas de hechos
| Tabla | Descripcion |
|-------|-------------|
| `silver.fact_ventas` | Lineas de venta (con campo calculado `facturacion_neta`) |
| `silver.fact_stock` | Stock por deposito/articulo/fecha |

### Tablas de dimension/maestros
| Tabla | Descripcion | Constraint |
|-------|-------------|------------|
| `silver.branches` | Sucursales | UNIQUE(id_sucursal) |
| `silver.sales_forces` | Fuerzas de venta | UNIQUE(id_fuerza_ventas) |
| `silver.staff` | Personal (vendedores, supervisores, gerentes) | UNIQUE(id_personal, id_sucursal) |
| `silver.routes` | Rutas de venta (solo vigentes) | UNIQUE(id_ruta, id_sucursal, id_fuerza_ventas) |
| `silver.clients` | Clientes (con segmentacion mkt y geolocalizacion) | UNIQUE(id_cliente) |
| `silver.client_forces` | Asignaciones cliente-ruta | UNIQUE(id_cliente, id_ruta, fecha_inicio) |
| `silver.articles` | Articulos | UNIQUE(id_articulo) |
| `silver.article_groupings` | Agrupaciones (marca, generico, calibre, etc.) | UNIQUE(id_articulo, id_forma_agrupar) |
| `silver.marketing_segments` | Segmentos de marketing (nivel 1) | |
| `silver.marketing_channels` | Canales de marketing (nivel 2) | |
| `silver.marketing_subchannels` | Subcanales de marketing (nivel 3) | |
| `silver.deposits` | Depositos con id_sucursal parseado | |
| `silver.hectolitros` | Factores de conversion articulo → hectolitros | UNIQUE(id_articulo) |

## Esquema Gold (Usar para consultas)

La capa **Gold** es la recomendada para consultas analiticas. Contiene un modelo dimensional (star schema).

### Tablas de Dimensiones

| Tabla | Descripcion | Campos clave |
|-------|-------------|--------------|
| `gold.dim_tiempo` | Calendario | fecha, dia, dia_semana, nombre_dia, semana, mes, nombre_mes, trimestre, anio |
| `gold.dim_sucursal` | Sucursales | id_sucursal, descripcion |
| `gold.dim_deposito` | Depositos (jerarquia: deposito → sucursal) | id_deposito, descripcion, id_sucursal, des_sucursal |
| `gold.dim_vendedor` | Vendedores | id_vendedor, des_vendedor, id_fuerza_ventas, id_sucursal, des_sucursal |
| `gold.dim_articulo` | Articulos | id_articulo, des_articulo, marca, generico, calibre, proveedor, unidad_negocio, factor_hectolitros |
| `gold.dim_cliente` | Clientes desnormalizados | id_cliente, razon_social, fantasia, id_sucursal, des_sucursal, marketing (canal/segmento/subcanal), id_ruta_fv1, des_personal_fv1, id_ruta_fv4, des_personal_fv4, ramo, localidad, provincia, lat/long, id_lista_precio, des_lista_precio, telefono_fijo, telefono_movil, anulado |

### Tablas de Hechos

| Tabla | Descripcion | Granularidad |
|-------|-------------|--------------|
| `gold.fact_ventas` | Lineas de venta | Una fila por linea de comprobante |
| `gold.fact_stock` | Stock por deposito | Una fila por articulo/deposito/fecha (UNIQUE) |

### Tablas de Cobertura (Agregaciones mensuales)

| Tabla | Descripcion | Campos |
|-------|-------------|--------|
| `gold.cob_preventista_marca` | Cobertura por vendedor/ruta/marca | periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, ds_sucursal, marca, clientes_compradores, volumen_total |
| `gold.cob_sucursal_marca` | Cobertura por sucursal/marca | periodo, id_fuerza_ventas, id_sucursal, ds_sucursal, marca, clientes_compradores, volumen_total |
| `gold.cob_preventista_generico` | Cobertura por vendedor/ruta/generico | periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, ds_sucursal, generico, clientes_compradores, volumen_total |

## Campos Importantes

### fact_ventas (Silver)
```sql
- fecha_comprobante     -- Fecha de la venta
- id_sucursal           -- Sucursal
- id_vendedor           -- Vendedor
- id_cliente            -- Cliente
- id_articulo           -- Articulo
- id_fuerza_ventas      -- Fuerza de venta
- cantidades_total      -- Cantidad vendida (unidades)
- facturacion_neta      -- cantidades_total * abs(precio_unitario_bruto) (campo calculado)
- subtotal_final        -- Monto total de la linea
- anulado               -- Boolean
```

### fact_ventas (Gold)
```sql
- fecha_comprobante     -- FK a dim_tiempo
- id_sucursal           -- FK a dim_sucursal
- id_vendedor           -- FK a dim_vendedor
- id_cliente            -- FK a dim_cliente
- id_articulo           -- FK a dim_articulo
- id_documento, letra, serie, nro_doc  -- Identificacion documento
- cantidades_con_cargo, cantidades_sin_cargo, cantidades_total
- subtotal_neto, subtotal_final, bonificacion
- cantidad_total_htls   -- cantidades_total * factor_hectolitros (campo calculado via LEFT JOIN silver.hectolitros)
- anulado
```

### fact_stock (Gold)
```sql
- date_stock            -- Fecha del stock
- id_deposito           -- FK a dim_deposito
- id_articulo           -- FK a dim_articulo
- cant_bultos           -- Cantidad en bultos
- cant_unidades         -- Cantidad en unidades
- cantidad_total_htls   -- cant_bultos * factor_hectolitros (campo calculado via LEFT JOIN silver.hectolitros)
```

### dim_articulo (Gold)
```sql
- id_articulo           -- ID del articulo
- des_articulo          -- Descripcion completa
- marca                 -- Marca del producto
- generico              -- Categoria generica
- calibre               -- Calibre
- proveedor             -- Proveedor
- unidad_negocio        -- Unidad de negocio
- factor_hectolitros    -- Factor de conversion a hectolitros (de silver.hectolitros)
```

### dim_cliente (Gold)
```sql
- id_cliente            -- ID del cliente
- razon_social          -- Nombre legal
- fantasia              -- Nombre de fantasia
- id_sucursal           -- Sucursal que lo atiende
- des_sucursal          -- Descripcion sucursal
- id_canal_mkt, des_canal_mkt        -- Canal de marketing
- id_segmento_mkt, des_segmento_mkt  -- Segmento de marketing
- id_subcanal_mkt, des_subcanal_mkt  -- Subcanal de marketing
- id_ruta_fv1           -- Ruta asignada en Fuerza de Venta 1
- des_personal_fv1      -- Preventista FV1
- id_ruta_fv4           -- Ruta asignada en Fuerza de Venta 4
- des_personal_fv4      -- Preventista FV4
- id_ramo, des_ramo     -- Ramo comercial
- id_localidad, des_localidad   -- Localidad
- id_provincia, des_provincia   -- Provincia
- latitud, longitud     -- Geolocalizacion
- id_lista_precio, des_lista_precio  -- Lista de precios
- telefono_fijo         -- Telefono fijo
- telefono_movil        -- Telefono movil
- anulado               -- Estado del cliente (true = anulado)
```

### dim_vendedor (Gold)
```sql
- id_vendedor           -- ID del vendedor (unico por sucursal)
- des_vendedor          -- Nombre del vendedor
- id_fuerza_ventas      -- 1=FV1 (Preventa), 4=FV4 (Autoventa)
- id_sucursal           -- Sucursal a la que pertenece
- des_sucursal          -- Descripcion de la sucursal
```

## Hectolitros

Factor de conversion de unidades de venta a hectolitros:

1. **Bronze**: Excel local → `bronze.raw_hectolitros` (JSONB)
2. **Silver**: `silver.hectolitros` (id_articulo, descripcion, factor_hectolitros)
3. **Gold**: No tiene tabla propia. Se consume via LEFT JOIN:
   - `dim_articulo.factor_hectolitros` — atributo dimensional
   - `fact_ventas.cantidad_total_htls` = `cantidades_total * factor_hectolitros`
   - `fact_stock.cantidad_total_htls` = `cant_bultos * factor_hectolitros`

Articulos sin factor: `cantidad_total_htls = NULL` (SUM ignora NULLs).

## Claves Compuestas (Importante)

Los IDs de vendedor, ruta y staff **NO son unicos globalmente**. Son unicos **por sucursal**:

```sql
-- CORRECTO: JOIN con clave compuesta
JOIN gold.dim_vendedor dv
  ON fv.id_vendedor = dv.id_vendedor
  AND fv.id_sucursal = dv.id_sucursal

-- INCORRECTO: JOIN solo por ID (mezcla datos entre sucursales)
JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor
```

Esta regla aplica a: `silver.staff`, `silver.routes`, `gold.dim_vendedor` y cualquier JOIN que involucre estas tablas.

## Fuerzas de Venta

Las fuerzas de venta son **grupos de preventistas que venden distintos articulos**:

| Fuerza | id_fuerza_ventas | Genericos que vende |
|--------|------------------|---------------------|
| FV1 | 1 | CERVEZAS, AGUAS DANONE, VINOS CCU, SIDRAS Y LICORES |
| FV4 | 4 | FRATELLI B, VINOS, JUGOS, VINOS FINOS |

- Un mismo cliente puede ser atendido por diferentes fuerzas de venta
- Cada FV tiene sus propias rutas asignadas
- Las rutas se cargan desde la API con `get_routes(fuerza_venta=N)`
- Solo se procesan rutas vigentes (`fecha_hasta = '9999-12-31'`)

## Constraints Silver

| Tabla | Constraint | Razon |
|-------|-----------|-------|
| `silver.staff` | `UNIQUE(id_personal, id_sucursal)` | IDs unicos por sucursal |
| `silver.routes` | `UNIQUE(id_ruta, id_sucursal, id_fuerza_ventas)` | Ruta por sucursal y FV |
| `silver.client_forces` | `UNIQUE(id_cliente, id_ruta, fecha_inicio)` | Asignacion unica por vigencia |
| `silver.article_groupings` | `UNIQUE(id_articulo, id_forma_agrupar)` | Una agrupacion por tipo por articulo |
| `silver.fact_stock` | `UNIQUE(date_stock, id_deposito, id_articulo)` | Un registro por dia/deposito/articulo |
| `silver.hectolitros` | `UNIQUE(id_articulo)` | Un factor por articulo |

Sin FKs entre tablas (por diseno medallion, la integridad la garantiza el ETL).

## Consultas de Ejemplo

### Ventas por Sucursal/Mes/Generico
```sql
SELECT
    fv.id_sucursal,
    ds.descripcion AS sucursal,
    DATE_TRUNC('month', fv.fecha_comprobante) AS mes,
    da.generico,
    SUM(fv.cantidades_total) AS volumen_total,
    SUM(fv.cantidad_total_htls) AS volumen_htls
FROM gold.fact_ventas fv
LEFT JOIN gold.dim_articulo da ON fv.id_articulo = da.id_articulo
LEFT JOIN gold.dim_sucursal ds ON fv.id_sucursal = ds.id_sucursal
GROUP BY 1, 2, 3, 4
ORDER BY 1, 3, volumen_total DESC;
```

### Ventas por Vendedor/Marca (con clave compuesta)
```sql
SELECT
    fv.id_sucursal,
    ds.descripcion AS sucursal,
    fv.id_vendedor,
    dv.des_vendedor,
    DATE_TRUNC('month', fv.fecha_comprobante) AS mes,
    da.marca,
    SUM(fv.cantidades_total) AS volumen_total,
    SUM(fv.cantidad_total_htls) AS volumen_htls
FROM gold.fact_ventas fv
LEFT JOIN gold.dim_articulo da ON fv.id_articulo = da.id_articulo
LEFT JOIN gold.dim_sucursal ds ON fv.id_sucursal = ds.id_sucursal
LEFT JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor
    AND fv.id_sucursal = dv.id_sucursal
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 3, 5, volumen_total DESC;
```

### Cobertura pre-calculada
```sql
SELECT * FROM gold.cob_preventista_marca
WHERE periodo = '2025-01-01'
ORDER BY id_sucursal, id_vendedor, volumen_total DESC;
```

### Stock en hectolitros por deposito
```sql
SELECT
    fs.date_stock,
    dd.descripcion AS deposito,
    dd.des_sucursal AS sucursal,
    da.generico,
    SUM(fs.cant_bultos) AS bultos,
    SUM(fs.cantidad_total_htls) AS htls
FROM gold.fact_stock fs
LEFT JOIN gold.dim_deposito dd ON fs.id_deposito = dd.id_deposito
LEFT JOIN gold.dim_articulo da ON fs.id_articulo = da.id_articulo
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, htls DESC;
```

## Notas Importantes

1. **Usar claves compuestas**: Siempre incluir `id_sucursal` en JOINs con dim_vendedor y dim_cliente
2. **Cobertura no es sumable**: La cobertura de marca A + marca B no es la cobertura total (clientes pueden comprar ambas)
3. **Periodo en cobertura**: Es el primer dia del mes (ej: '2025-01-01' para enero 2025)
4. **Hectolitros NULL**: Articulos sin factor de conversion tienen `cantidad_total_htls = NULL` en facts
5. **facturacion_neta solo en Silver**: Gold fact_ventas no tiene este campo aun (ver BUGS.md BUG-012)
6. **Anulados**: El campo `anulado` existe en dim_cliente (estado del cliente) y en fact_ventas (estado de la venta). Ambos se cargan, no se filtran.
7. **gold all incompleto**: `python3 orchestrator.py gold all` solo ejecuta dimensiones + fact_ventas. Para fact_stock y cobertura correr por separado.
8. **client_forces sin id_sucursal**: La tabla `silver.client_forces` no tiene columna `id_sucursal`. La sucursal viene implicita de la ruta (JOIN a `silver.routes`).
9. **email en silver.clients**: El campo `email` existe en silver.clients pero NO se propaga a gold.dim_cliente.

## Bronze Tables

| Tabla | Fuente | Tipo de Carga |
|-------|--------|---------------|
| `bronze.raw_sales` | API Chess ERP | Incremental (mes a mes) |
| `bronze.raw_clients` | API Chess ERP | Full Refresh |
| `bronze.raw_staff` | API Chess ERP | Full Refresh |
| `bronze.raw_routes` | API Chess ERP | Full Refresh (FV1 + FV4) |
| `bronze.raw_articles` | API Chess ERP | Full Refresh |
| `bronze.raw_stock` | API Chess ERP | Append (dia a dia x deposito) |
| `bronze.raw_marketing` | API Chess ERP | Full Refresh |
| `bronze.raw_deposits` | CSV local (`data/deposits_b.csv`) | Full Refresh |
| `bronze.raw_hectolitros` | Excel local (`data/hectolitros.xlsx`) | Full Refresh |

Todas las tablas bronze almacenan datos como JSONB en columna `data_raw`.

## Daily Load (Fases)

`daily_load.py` ejecuta 10 fases en orden:

| Fase | Descripcion | Condicion |
|------|-------------|-----------|
| 1 | BRONZE MASTERS | Siempre |
| 2 | BRONZE VENTAS | Mes actual + mes anterior si dia <= 3 |
| 3 | BRONZE STOCK | Solo fecha de referencia |
| 4 | SILVER MASTERS | Siempre |
| 5 | SILVER VENTAS | Mismos rangos que fase 2 |
| 6 | SILVER STOCK | Mismo rango que fase 3 |
| 7 | GOLD DIMENSIONES | Siempre (6 dimensiones) |
| 8 | GOLD FACT_VENTAS | Mismos rangos que fase 2 |
| 9 | GOLD FACT_STOCK | Mismo rango que fase 3 |
| 10 | GOLD COBERTURA | Periodo del mes actual |
