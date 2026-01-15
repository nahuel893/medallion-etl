# Guía de Uso - Orchestrator ETL Medallion

## Sintaxis
```bash
python orchestrator.py <capa> <entidad> [argumentos]
```

---

## BRONZE (Extracción desde API)

```bash
# Ventas (requiere rango de fechas)
python orchestrator.py bronze sales 2025-01-01 2025-12-31

# Maestros (full refresh, sin argumentos)
python3 orchestrator.py bronze clientes
python3  orchestrator.py bronze staff
python3 orchestrator.py bronze routes
python3 orchestrator.py bronze articles
python3 orchestrator.py bronze depositos
python3 orchestrator.py bronze marketing

# Stock (requiere rango de fechas)
python orchestrator.py bronze stock 2025-01-01 2025-12-31
```

---

## SILVER (Transformación)

```bash
# Orden recomendado (por dependencias FK)
python3 orchestrator.py bronze clientes
python3  orchestrator.py bronze staff
python3 orchestrator.py bronze routes
python3 orchestrator.py bronze articles
python3 orchestrator.py bronze depositos
python3 orchestrator.py bronze marketing        # 9. Segmentación

# Ventas (acepta fechas o --full-refresh)
python3 orchestrator.py silver sales 2025-01-01 2025-12-31
python3 orchestrator.py silver sales --full-refresh
```

---

## GOLD (Agregación - Star Schema)

```bash
# Dimensiones
python3 orchestrator.py gold dim_tiempo 2025-01-01 2025-12-31
python3 orchestrator.py gold dim_sucursal
python3 orchestrator.py gold dim_vendedor
python3 orchestrator.py gold dim_articulo
python3 orchestrator.py gold dim_cliente

# Fact table
python3 orchestrator.py gold fact_ventas
python3 orchestrator.py gold fact_ventas --full-refresh

# Todo Gold de una vez
python3 orchestrator.py gold all
```

---

## Flujos Comunes

### Carga Inicial Completa

```bash
# === 1. BRONZE: Extraer datos de API ===
python orchestrator.py bronze clientes
python orchestrator.py bronze staff
python orchestrator.py bronze routes
python orchestrator.py bronze articles
python orchestrator.py bronze depositos
python orchestrator.py bronze marketing
python orchestrator.py bronze sales 2024-01-01 2025-12-31

# === 2. SILVER: Transformar (en orden) ===
python orchestrator.py silver branches
python orchestrator.py silver sales_forces
python orchestrator.py silver staff
python orchestrator.py silver routes
python orchestrator.py silver clients
python orchestrator.py silver client_forces
python orchestrator.py silver articles
python orchestrator.py silver article_groupings
python orchestrator.py silver marketing
python orchestrator.py silver sales --full-refresh

# === 3. GOLD: Cargar Star Schema ===
python orchestrator.py gold all
```

### Carga Incremental Diaria

```bash
# Solo ventas del día anterior
AYER=$(date -d "yesterday" +%Y-%m-%d)
HOY=$(date +%Y-%m-%d)

python orchestrator.py bronze sales $AYER $HOY
python orchestrator.py silver sales $AYER $HOY
python orchestrator.py gold fact_ventas
```

### Actualizar Maestros

```bash
# Cuando cambian clientes, artículos, rutas, etc.
python orchestrator.py bronze clientes
python orchestrator.py bronze articles
python orchestrator.py silver clients
python orchestrator.py silver articles
python orchestrator.py gold dim_cliente
python orchestrator.py gold dim_articulo
```

---

## Partial Refresh

Recarga el mes completo (borra y vuelve a insertar) en Bronze → Silver → Gold.

```bash
# Mes actual (basado en la fecha de hoy)
python3 orchestrator.py partial-refresh-sales

# Mes específico
python3 orchestrator.py partial-refresh-sales 2025-01
python3 orchestrator.py partial-refresh-sales 2024-12
```

Ideal para un cron diario que mantiene el mes actual actualizado.
