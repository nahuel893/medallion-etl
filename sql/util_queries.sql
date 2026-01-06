-- ==========================================
-- CONSULTAS ÚTILES - MEDALLION ETL
-- ==========================================

-- ==========================================
-- STOCK - Consultas por intervalo de fechas
-- ==========================================

-- Snapshots de un día específico
SELECT * FROM bronze.raw_stock
WHERE ingestion_at >= '2025-12-29'
  AND ingestion_at < '2025-12-30';

-- Snapshots de una semana
SELECT * FROM bronze.raw_stock
WHERE ingestion_at >= '2025-12-23'
  AND ingestion_at < '2025-12-30';

-- Snapshots entre dos horas específicas
SELECT * FROM bronze.raw_stock
WHERE ingestion_at >= '2025-12-29 08:00:00'
  AND ingestion_at < '2025-12-29 14:00:00';

-- Últimos 7 días
SELECT * FROM bronze.raw_stock
WHERE ingestion_at >= CURRENT_DATE - INTERVAL '7 days';

-- Snapshot más reciente por artículo (útil para Silver)
SELECT DISTINCT ON (data_raw->>'idArticulo') *
FROM bronze.raw_stock
ORDER BY data_raw->>'idArticulo', ingestion_at DESC;

-- Último snapshot completo
SELECT * FROM bronze.raw_stock
WHERE ingestion_at = (SELECT MAX(ingestion_at) FROM bronze.raw_stock);

-- Borrar snapshots viejos (más de 30 días)
DELETE FROM bronze.raw_stock
WHERE ingestion_at < CURRENT_DATE - INTERVAL '30 days';


-- ==========================================
-- SALES - Consultas por fecha de comprobante
-- ==========================================

-- Ventas de un día específico
SELECT * FROM bronze.raw_sales
WHERE date_comprobante = '2025-12-01';

-- Ventas de un mes
SELECT * FROM bronze.raw_sales
WHERE date_comprobante >= '2025-12-01'
  AND date_comprobante < '2026-01-01';

-- Borrar ventas de un mes (para recarga)
DELETE FROM bronze.raw_sales
WHERE date_comprobante >= '2025-12-01'
  AND date_comprobante < '2026-01-01';


-- ==========================================
-- TAMAÑO DE TABLAS
-- ==========================================

-- Tamaño de una tabla específica
SELECT pg_size_pretty(pg_total_relation_size('bronze.raw_sales')) AS tamaño_total;

-- Desglose: tabla + índices
SELECT
    pg_size_pretty(pg_relation_size('bronze.raw_sales')) AS tabla,
    pg_size_pretty(pg_indexes_size('bronze.raw_sales')) AS indices,
    pg_size_pretty(pg_total_relation_size('bronze.raw_sales')) AS total;

-- Tamaño de todas las tablas en bronze
SELECT
    tablename AS tabla,
    pg_size_pretty(pg_total_relation_size('bronze.' || tablename)) AS tamaño
FROM pg_tables
WHERE schemaname = 'bronze'
ORDER BY pg_total_relation_size('bronze.' || tablename) DESC;

-- Peso estimado por columna
SELECT
    'data_raw' AS columna,
    pg_size_pretty(SUM(pg_column_size(data_raw))) AS tamaño
FROM bronze.raw_sales;


-- ==========================================
-- CONTEO DE REGISTROS
-- ==========================================

-- Conteo por tabla
SELECT 'raw_sales' AS tabla, COUNT(*) AS registros FROM bronze.raw_sales
UNION ALL
SELECT 'raw_clients', COUNT(*) FROM bronze.raw_clients
UNION ALL
SELECT 'raw_staff', COUNT(*) FROM bronze.raw_staff
UNION ALL
SELECT 'raw_routes', COUNT(*) FROM bronze.raw_routes
UNION ALL
SELECT 'raw_articles', COUNT(*) FROM bronze.raw_articles
UNION ALL
SELECT 'raw_stock', COUNT(*) FROM bronze.raw_stock;

-- Conteo de snapshots de stock por día
SELECT
    DATE(ingestion_at) AS fecha,
    COUNT(DISTINCT ingestion_at) AS snapshots,
    COUNT(*) AS registros_totales
FROM bronze.raw_stock
GROUP BY DATE(ingestion_at)
ORDER BY fecha DESC;
