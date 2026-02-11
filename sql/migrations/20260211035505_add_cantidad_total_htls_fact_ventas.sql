-- migrate:up
ALTER TABLE gold.fact_ventas ADD COLUMN cantidad_total_htls NUMERIC(15,4);

-- migrate:down
ALTER TABLE gold.fact_ventas DROP COLUMN IF EXISTS cantidad_total_htls;
