-- migrate:up
-- BUG-003: dim_vendedor tiene PK solo en id_vendedor, pero los IDs son unicos por sucursal.
-- Cambiar PK de (id_vendedor) a (id_vendedor, id_sucursal).

ALTER TABLE gold.dim_vendedor DROP CONSTRAINT dim_vendedor_pkey;
ALTER TABLE gold.dim_vendedor ADD PRIMARY KEY (id_vendedor, id_sucursal);

-- migrate:down
ALTER TABLE gold.dim_vendedor DROP CONSTRAINT dim_vendedor_pkey;
ALTER TABLE gold.dim_vendedor ADD PRIMARY KEY (id_vendedor);
