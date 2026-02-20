# Bugs Pendientes - Medallion ETL

**Ultima actualizacion:** 2026-02-16

---

## P0 - CRITICO

### BUG-001: DELETE se commitea antes del INSERT en sales_loader
**Archivo:** `src/layers/bronze/loaders/sales_loader.py:71`
**Impacto:** Perdida de datos irrecuperable si el proceso falla despues del DELETE.
**Causa:** `raw_conn.commit()` se ejecuta inmediatamente despues del DELETE, antes de los INSERTs.
**Solucion:** Eliminar el commit intermedio. Un unico `raw_conn.commit()` al final, despues de todos los INSERTs.

### BUG-002: Sin try/except ni rollback en toda la codebase
**Archivos:** Todos los loaders, transformers y aggregators (~30 archivos)
**Impacto:** Si un INSERT falla despues de un DELETE, la transaccion queda inconsistente. Datos perdidos.
**Solucion:** Envolver en try/except con rollback. Idealmente crear un context manager `atomic_cursor()` reutilizable.

---

## P1 - IMPORTANTE

### ~~BUG-003: dim_vendedor tiene UNIQUE(id_vendedor) — mismo bug que staff~~ RESUELTO
**Archivo:** `src/layers/gold/aggregators/dim_vendedor.py:39`
**Resuelto:** 2026-02-18. PK cambiada a (id_vendedor, id_sucursal). Migracion: `20260218200000_fix_dim_vendedor_pk.sql`

### BUG-004: NULLs en UNIQUE constraints de cobertura
**Archivo:** `src/layers/gold/aggregators/cobertura.py`
**Impacto:** ON CONFLICT no matchea filas con NULLs en columnas del UNIQUE index (PostgreSQL: NULL != NULL). Se insertan filas duplicadas en vez de actualizarse.
**Columnas afectadas:** id_vendedor, id_ruta, marca/generico en los 3 UNIQUE constraints de cobertura.
**Solucion:** Usar `COALESCE` para reemplazar NULLs con valores centinela en el UNIQUE, o crear partial indexes.

### BUG-005: Import roto RawClientes
**Archivo:** `src/database/models/__init__.py`
**Impacto:** Error de importacion si alguien importa desde models.
**Causa:** La clase se llama `RawClients` pero se importa como `RawClientes`.
**Solucion:** Corregir el import. Fix de 5 minutos.

### BUG-006: openpyxl faltante en requirements.txt
**Archivo:** `requirements.txt`
**Impacto:** `hectolitros_loader.py` falla en instalacion limpia porque openpyxl no esta declarado como dependencia.
**Solucion:** Agregar `openpyxl>=3.1` a requirements.txt.

### BUG-007: Stock loader sin idempotencia
**Archivo:** `src/layers/bronze/loaders/stock_loader.py`
**Impacto:** Re-ejecutar el loader duplica datos en bronze porque no hace DELETE antes de INSERT.
**Solucion:** Agregar DELETE por rango de fechas antes del INSERT (mismo patron que sales_loader).

### BUG-008: facturacion_neta usa campo incorrecto
**Archivo:** `src/layers/silver/transformers/sales_transformer.py:190`
**Impacto:** El campo calculado `facturacion_neta` podria estar usando `precioventabr` en vez de `precioUnitarioBruto` como dice la documentacion.
**Solucion:** Verificar cual es el campo correcto de la API y ajustar el calculo.

### ~~BUG-009: Cobertura contaba clientes con neto negativo como compradores~~ RESUELTO
**Archivo:** `src/layers/gold/aggregators/cobertura.py`
**Resuelto:** 2026-02-20. Se reemplazo `WHERE fv.cantidades_total > 0` por CTE con `HAVING SUM(fv.cantidades_total) > 0` que agrupa por cliente primero. No se filtra por anulado (decision del usuario).

---

## P2 - MEDIO

### BUG-010: gold_all() no incluye fact_stock ni cobertura
**Archivo:** `orchestrator.py:429-434`
**Impacto:** `python3 orchestrator.py gold all` solo ejecuta dimensiones + fact_ventas, omite fact_stock y cobertura.
**Solucion:** Agregar `gold_fact_stock()` y `gold_cobertura()` a `gold_all()`.

### BUG-011: ColoredFormatter corrompe logs de archivo
**Archivo:** `src/config/logging_config.py`
**Impacto:** Los logs escritos a archivo contienen caracteres ANSI de colores, dificultando su lectura.
**Solucion:** No restaurar el levelname original despues de colorizar, o usar formatter separado para archivos.

### BUG-012: gold.fact_ventas no tiene facturacion_neta
**Impacto:** El campo calculado `facturacion_neta` solo existe en silver.fact_ventas, no se propaga a gold.
**Solucion:** Agregar `facturacion_neta` al INSERT de gold.fact_ventas.

### BUG-013: fact_ventas/stock borran todo sin parametros
**Archivo:** `src/layers/gold/aggregators/fact_ventas.py:44-48`
**Impacto:** Llamar `load_fact_ventas()` sin parametros ejecuta DELETE de toda la tabla. Semantica confusa (no es full_refresh explicito, pero borra todo).
**Solucion:** Requerir `full_refresh=True` explicitamente para borrar todo, o loggear warning claro.

### BUG-014: Modelos ORM desincronizados con DDL
**Archivos:** `src/database/models/bronze.py`, `silver.py`
**Impacto:** Los modelos ORM no reflejan el estado actual de las tablas (faltan columnas nuevas).
**Solucion:** Actualizar modelos o eliminarlos si no se usan.

---

## Resueltos

| Bug | Descripcion | Commit | Fecha |
|-----|-------------|--------|-------|
| ~~UNIQUE staff~~ | `silver.staff` UNIQUE(id_personal) → UNIQUE(id_personal, id_sucursal) | `987440b` | 2026-02-14 |
| ~~dim_cliente JOINs~~ | JOINs routes-staff sin id_sucursal en CTEs fv1/fv4 | `987440b` | 2026-02-14 |
| ~~BUG-003 dim_vendedor PK~~ | PK (id_vendedor) → PK (id_vendedor, id_sucursal) | pendiente | 2026-02-18 |
| ~~BUG-009 cobertura neto~~ | WHERE cantidades > 0 → CTE con HAVING SUM > 0 por cliente | pendiente | 2026-02-20 |
