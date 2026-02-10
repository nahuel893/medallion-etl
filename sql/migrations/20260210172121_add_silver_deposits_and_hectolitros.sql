-- migrate:up
CREATE TABLE IF NOT EXISTS silver.deposits (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_deposito INTEGER NOT NULL,
    descripcion VARCHAR(255),
    id_sucursal INTEGER,
    des_sucursal VARCHAR(100),
    CONSTRAINT deposits_unique UNIQUE (id_deposito)
);

CREATE INDEX IF NOT EXISTS idx_silver_deposits_sucursal ON silver.deposits(id_sucursal);

CREATE TABLE IF NOT EXISTS silver.hectolitros (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_articulo INTEGER NOT NULL,
    descripcion VARCHAR(255),
    factor_hectolitros NUMERIC(12,8),
    CONSTRAINT hectolitros_unique UNIQUE (id_articulo)
);

CREATE INDEX IF NOT EXISTS idx_silver_hectolitros_articulo ON silver.hectolitros(id_articulo);

-- migrate:down
DROP TABLE IF EXISTS silver.hectolitros;
DROP TABLE IF EXISTS silver.deposits;
