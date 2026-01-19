-- Script para eliminar FKs entre capas (ejecutar una sola vez)
-- Esto permite que cada capa sea independiente para full refresh

-- Eliminar FK de silver.clients -> bronze.raw_clients
ALTER TABLE silver.clients DROP CONSTRAINT IF EXISTS clients_bronze_id_fkey;

-- Eliminar FK de silver.staff -> bronze.raw_staff
ALTER TABLE silver.staff DROP CONSTRAINT IF EXISTS staff_bronze_id_fkey;

-- Eliminar FK de silver.routes -> bronze.raw_routes
ALTER TABLE silver.routes DROP CONSTRAINT IF EXISTS routes_bronze_id_fkey;

-- Eliminar FK de silver.articles -> bronze.raw_articles
ALTER TABLE silver.articles DROP CONSTRAINT IF EXISTS articles_bronze_id_fkey;

-- Eliminar FK de silver.fact_ventas -> bronze.raw_sales
ALTER TABLE silver.fact_ventas DROP CONSTRAINT IF EXISTS fact_ventas_bronze_id_fkey;

-- Verificar que no quedan FKs entre capas
SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    ccu.table_schema AS foreign_schema,
    ccu.table_name AS foreign_table
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema IN ('bronze', 'silver', 'gold')
    AND ccu.table_schema IN ('bronze', 'silver', 'gold')
    AND tc.table_schema != ccu.table_schema;
