-- migrate:up
ALTER TABLE gold.dim_articulo ADD COLUMN factor_hectolitros NUMERIC(12,8);

-- migrate:down
ALTER TABLE gold.dim_articulo DROP COLUMN IF EXISTS factor_hectolitros;
