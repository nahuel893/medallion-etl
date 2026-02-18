# Implementar hectolitros en Gold layer

## Contexto

`silver.hectolitros` contiene factores de conversión por artículo (bultos → hectolitros). Actualmente estos factores no se usan en ninguna tabla gold. Se necesita:
- `factor_hectolitros` como atributo dimensional en `dim_articulo`
- `cantidad_total_htls` como métrica precalculada en `fact_ventas`, `fact_stock`, y las 3 tablas `cob_*`

NULL para artículos sin factor (SUM ignora NULLs, semánticamente correcto).

**Un commit por cada mejora. Verificar rama dev antes de commitear. Usar usuario del sistema (no Co-Authored-By).**

---

## Commit 1: dim_articulo — agregar factor_hectolitros

### Migración
**Archivo nuevo**: `sql/migrations/<ts>_add_factor_hectolitros_dim_articulo.sql`
```sql
-- migrate:up
ALTER TABLE gold.dim_articulo ADD COLUMN factor_hectolitros NUMERIC(12,8);

-- migrate:down
ALTER TABLE gold.dim_articulo DROP COLUMN IF EXISTS factor_hectolitros;
```

### Aggregator
**Archivo**: `src/layers/gold/aggregators/dim_articulo.py`
- Agregar `LEFT JOIN silver.hectolitros h ON a.id_articulo = h.id_articulo`
- Agregar `h.factor_hectolitros` al SELECT y al INSERT/ON CONFLICT

### DDL
**Archivo**: `sql/setup_medallion.sql`
- Agregar `factor_hectolitros NUMERIC(12,8)` a `gold.dim_articulo`

---

## Commit 2: fact_ventas — agregar cantidad_total_htls

### Migración
**Archivo nuevo**: `sql/migrations/<ts>_add_cantidad_total_htls_fact_ventas.sql`
```sql
-- migrate:up
ALTER TABLE gold.fact_ventas ADD COLUMN cantidad_total_htls NUMERIC(15,4);

-- migrate:down
ALTER TABLE gold.fact_ventas DROP COLUMN IF EXISTS cantidad_total_htls;
```

### Aggregator
**Archivo**: `src/layers/gold/aggregators/fact_ventas.py`
- Cambiar `FROM silver.fact_ventas` → `FROM silver.fact_ventas fv LEFT JOIN silver.hectolitros h ON fv.id_articulo = h.id_articulo`
- Agregar `fv.cantidades_total * h.factor_hectolitros AS cantidad_total_htls` al SELECT
- Prefijar columnas existentes con `fv.`
- Agregar `cantidad_total_htls` al INSERT columns

### DDL
**Archivo**: `sql/setup_medallion.sql`
- Agregar `cantidad_total_htls NUMERIC(15,4)` a `gold.fact_ventas`

---

## Commit 3: fact_stock — agregar cantidad_total_htls

### Migración
**Archivo nuevo**: `sql/migrations/<ts>_add_cantidad_total_htls_fact_stock.sql`
```sql
-- migrate:up
ALTER TABLE gold.fact_stock ADD COLUMN cantidad_total_htls NUMERIC(15,4);

-- migrate:down
ALTER TABLE gold.fact_stock DROP COLUMN IF EXISTS cantidad_total_htls;
```

### Aggregator
**Archivo**: `src/layers/gold/aggregators/fact_stock.py`
- Cambiar `FROM silver.fact_stock` → `FROM silver.fact_stock fs LEFT JOIN silver.hectolitros h ON fs.id_articulo = h.id_articulo`
- Agregar `fs.cant_bultos * h.factor_hectolitros AS cantidad_total_htls` al SELECT
- Prefijar columnas existentes con `fs.`
- Agregar `cantidad_total_htls` al INSERT columns

### DDL
**Archivo**: `sql/setup_medallion.sql`
- Agregar `cantidad_total_htls NUMERIC(15,4)` a `gold.fact_stock`

---

## Archivos impactados (resumen)

| Archivo | Commits |
|---------|---------|
| `src/layers/gold/aggregators/dim_articulo.py` | 1 |
| `src/layers/gold/aggregators/fact_ventas.py` | 2 |
| `src/layers/gold/aggregators/fact_stock.py` | 3 |
| `sql/setup_medallion.sql` | 1, 2, 3 |
| `sql/migrations/` (3 nuevas) | 1, 2, 3 |

## Verificación

1. `npx dbmate up` → aplica las 3 migraciones
2. `python3 orchestrator.py gold dim_articulo` → verifica factor_hectolitros poblado
3. `python3 orchestrator.py gold fact_ventas 2025-01-01 2025-01-31` → verifica cantidad_total_htls
4. `python3 orchestrator.py gold fact_stock` → verifica cantidad_total_htls
5. `python3 -m pytest tests/ -v` → todos los tests pasan
6. SQL de verificación:
   ```sql
   SELECT COUNT(*), COUNT(factor_hectolitros) FROM gold.dim_articulo;
   SELECT COUNT(*), COUNT(cantidad_total_htls), SUM(cantidad_total_htls) FROM gold.fact_ventas;
   SELECT COUNT(*), COUNT(cantidad_total_htls) FROM gold.fact_stock;
   ```
