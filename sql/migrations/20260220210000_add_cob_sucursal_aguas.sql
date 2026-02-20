-- migrate:up
CREATE TABLE IF NOT EXISTS gold.cob_sucursal_aguas (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    id_fuerza_ventas INTEGER NOT NULL,
    id_sucursal INTEGER,
    ds_sucursal VARCHAR(100),
    subdivision_aguas VARCHAR(150),
    clientes_compradores INTEGER,
    volumen_total NUMERIC(15,4)
);

CREATE INDEX IF NOT EXISTS idx_cob_suc_aguas_periodo ON gold.cob_sucursal_aguas(periodo);
CREATE INDEX IF NOT EXISTS idx_cob_suc_aguas_fuerza ON gold.cob_sucursal_aguas(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_cob_suc_aguas_sucursal ON gold.cob_sucursal_aguas(id_sucursal);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cob_suc_aguas_unique ON gold.cob_sucursal_aguas(periodo, id_fuerza_ventas, id_sucursal, subdivision_aguas);

-- migrate:down
DROP TABLE IF EXISTS gold.cob_sucursal_aguas;
