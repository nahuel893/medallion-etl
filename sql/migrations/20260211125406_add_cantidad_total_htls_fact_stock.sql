-- migrate:up
ALTER TABLE gold.fact_stock ADD COLUMN cantidad_total_htls NUMERIC(15,4);

-- migrate:down
ALTER TABLE gold.fact_stock DROP COLUMN IF EXISTS cantidad_total_htls;
