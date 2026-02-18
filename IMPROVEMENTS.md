# Informe de Code Review - Medallion ETL (v2)

**Fecha:** 2026-02-11
**Revisor:** Claude Opus 4.6 (Senior Software Architect)
**Codebase:** ~7,628 LOC (Python + SQL)
**Branch:** dev
**Score anterior:** 6.5/10 (2026-02-10)

---

## Resumen Ejecutivo

**Puntuacion general: 6.8/10** (mejora +0.3 por tests y nuevas entidades, pero problemas estructurales P0/P1 persisten).

Desde el analisis anterior se agregaron: tests unitarios para silver/gold (~80 nuevos, total ~135), depositos/hectolitros como entidades nuevas en las 3 capas, migraciones dbmate, y daily_load.py. Sin embargo, los problemas criticos (transacciones no atomicas, duplicacion de codigo, falta de validacion) siguen abiertos.

---

## P0 - CRITICO

### 2.1 DELETE se commitea antes del INSERT en sales_loader
**Archivo:** `src/layers/bronze/loaders/sales_loader.py`, linea 71

Si el proceso se interrumpe despues del commit del DELETE pero antes de completar los INSERTs, se pierden datos irrecuperablemente.

**Solucion:** Eliminar el `raw_conn.commit()` de linea 71 y hacer un unico commit al final.

### 2.2 Sin try/except ni rollback en toda la codebase
**Afecta:** Todos los loaders, transformers y aggregators (~30 archivos)

Ningun archivo tiene try/except con rollback. Si un INSERT falla despues del DELETE, los datos quedan inconsistentes.

**Solucion:** Context manager centralizado `atomic_cursor()` con try/except/rollback.

---

## P1 - IMPORTANTE

| ID | Descripcion | Esfuerzo | Archivo |
|----|-------------|----------|---------|
| 9.1 | **BUG:** Import roto `RawClientes` (deberia ser `RawClients`) | 5min | `database/models/__init__.py` |
| 9.2 | `openpyxl` faltante en requirements.txt | 5min | `requirements.txt` |
| 3.5 | facturacion_neta usa `precioventabr` pero docs dicen `precioUnitarioBruto` | 2h | `silver/transformers/sales_transformer.py:190` |
| 2.3 | Stock loader sin idempotencia (duplica datos en bronze) | 2h | `bronze/loaders/stock_loader.py` |
| 3.1 | Duplicacion masiva bronze loaders (~400 lineas, 7 de 9 loaders) | 8h | `bronze/loaders/` |
| 4.2 | Stock loader: N*M llamadas API secuenciales (450 calls) | 4h | `bronze/loaders/stock_loader.py` |
| 5.2 | SQL injection potencial en install_db.sh | 2h | `install_db.sh:56-72` |
| 6.2 | Sin tests de integracion con DB real | 16h | `tests/` |
| 6.3 | Sin test de facturacion_neta | 2h | `tests/` |
| 8.1 | Sin CI/CD pipeline | 8h | -- |
| 8.2 | Sin alertas cuando falla daily_load | 4h | `daily_load.py` |

---

## P2 - MEDIO

| ID | Descripcion | Esfuerzo |
|----|-------------|----------|
| 9.3 | ColoredFormatter corrompe logs de archivo (no restaura levelname) | 1h |
| 9.4 | `gold_all()` no incluye fact_stock ni cobertura | 30min |
| 9.5 | Usuario "nahuel" hardcodeado en permisos SQL | 1h |
| 9.7 | gold.fact_ventas no tiene facturacion_neta | 2h |
| 9.9 | fact_ventas borra todo sin params (semantica confusa) | 1h |
| 1.2 | Modelos ORM desincronizados con DDL real | 4h |
| 1.3 | DDL dual: setup_medallion.sql vs migraciones dbmate | 2h |
| 3.2 | Duplicacion silver transformers (~200 lineas) | 6h |
| 3.3 | Duplicacion gold cobertura (~120 lineas) | 3h |
| 2.4 | daily_load continua tras errores criticos | 2h |
| 2.5 | Sin retry para API calls | 4h |
| 4.3 | SET work_mem sin restaurar (usar SET LOCAL) | 1h |
| 5.3 | Sin validacion de fechas CLI | 2h |
| 6.5 | Sin tests de flujos de error | 4h |
| 7.1 | Gestion conexiones bypasea SQLAlchemy (30+ archivos) | 6h |
| 7.2 | Paths fragiles con 5x .parent | 1h |
| 8.3 | Sin rotacion de logs | 1h |
| 8.4 | requirements.txt sin versiones exactas | 1h |

---

## P3 - NICE-TO-HAVE

| ID | Descripcion | Esfuerzo |
|----|-------------|----------|
| 1.4 | Orquestador monolitico 747 lineas | 6h |
| 3.4 | `--full-refresh or True` sin efecto | 30min |
| 4.4 | TRUNCATE en lugar de DELETE para full refresh | 2h |
| 4.5 | dim_tiempo: generate_series en lugar de loop Python | 2h |
| 5.5 | ChessClient re-creado en cada loader | 1h |
| 6.4 | Helper _make_mock_conn duplicado 7 veces en tests | 1h |
| 7.3 | Utils de fechas duplicadas en 3 archivos | 1h |
| 8.5 | Sin Dockerfile | 4h |
| 9.6 | Inconsistencia naming: date_stock vs fecha_comprobante | 1h |
| 9.8 | full_refresh ignorado en 4 transformers | 1h |

---

## Estado de Tests

| Area | Tests | Cobertura |
|------|-------|-----------|
| Bronze loaders | 17 | ~22% |
| Silver transformers | ~30 | ~23% |
| Gold aggregators | ~55 | ~56% |
| Orchestrator | 13 | ~75% |
| Daily load | 13 | ~85% |
| Utilidades | 7 | ~90% |
| **Total** | **~135** | **~35%** |

---

## Comparacion con v1 (2026-02-10)

**Resueltos:** Tests silver/gold (+80 tests nuevos)

**Sin resolver:** Transacciones atomicas (P0), duplicacion bronze (P1), facturacion_neta (P1), stock N+1 (P1), SQL injection install_db.sh (P1), CI/CD (P1), alertas (P1)

**Nuevos hallazgos:** Import roto RawClientes (P1), openpyxl faltante (P1), ColoredFormatter corrompe logs (P2), gold_all() incompleto (P2), gold sin facturacion_neta (P2), usuario hardcodeado en permisos (P2)

---

## Plan de Ejecucion Recomendado

**Sprint 1:** Bugs y fiabilidad critica (P0 + P1 bugs) ~20h
**Sprint 2:** Seguridad y performance (P1 restantes) ~18h
**Sprint 3:** Refactoring y DRY (P2 codigo) ~15h
**Sprint 4:** Testing y DevOps (P1-P2 testing/devops) ~28h

Con P0+P1 resueltos (~50h): estimado **8.0/10**
