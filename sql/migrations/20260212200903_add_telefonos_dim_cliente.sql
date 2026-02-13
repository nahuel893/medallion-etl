-- migrate:up
ALTER TABLE gold.dim_cliente ADD COLUMN telefono_fijo VARCHAR(50);
ALTER TABLE gold.dim_cliente ADD COLUMN telefono_movil VARCHAR(50);

-- migrate:down
ALTER TABLE gold.dim_cliente DROP COLUMN IF EXISTS telefono_fijo;
ALTER TABLE gold.dim_cliente DROP COLUMN IF EXISTS telefono_movil;
