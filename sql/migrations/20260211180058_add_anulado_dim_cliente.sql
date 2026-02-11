-- migrate:up
ALTER TABLE gold.dim_cliente ADD COLUMN anulado BOOLEAN DEFAULT FALSE;

-- migrate:down
ALTER TABLE gold.dim_cliente DROP COLUMN IF EXISTS anulado;
