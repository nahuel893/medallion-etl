-- migrate:up
CREATE TABLE IF NOT EXISTS gold.cob_sucursal_generico (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    id_fuerza_ventas INTEGER NOT NULL,
    id_sucursal INTEGER,
    ds_sucursal VARCHAR(100),
    generico VARCHAR(150),
    clientes_compradores INTEGER,
    volumen_total NUMERIC(15,4)
);

CREATE INDEX IF NOT EXISTS idx_cob_suc_gen_periodo ON gold.cob_sucursal_generico(periodo);
CREATE INDEX IF NOT EXISTS idx_cob_suc_gen_fuerza ON gold.cob_sucursal_generico(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_cob_suc_gen_sucursal ON gold.cob_sucursal_generico(id_sucursal);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cob_suc_gen_unique ON gold.cob_sucursal_generico(periodo, id_fuerza_ventas, id_sucursal, generico);

-- migrate:down
DROP TABLE IF EXISTS gold.cob_sucursal_generico;
