-- migrate:up
CREATE TABLE IF NOT EXISTS gold.dim_deposito (
    id_deposito INTEGER PRIMARY KEY,
    descripcion VARCHAR(255),
    id_sucursal INTEGER,
    des_sucursal VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_dim_deposito_sucursal ON gold.dim_deposito(id_sucursal);

-- migrate:down
DROP TABLE IF EXISTS gold.dim_deposito;
