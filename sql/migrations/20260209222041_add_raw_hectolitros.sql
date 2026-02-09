-- migrate:up
CREATE TABLE IF NOT EXISTS bronze.raw_hectolitros (
    id_articulo INTEGER NOT NULL,
    descripcion VARCHAR(255),
    factor_hectolitros NUMERIC(12,8),
    source_system VARCHAR(50) DEFAULT 'XLSX_HECTOLITROS',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT raw_hectolitros_pk PRIMARY KEY (id_articulo)
);

-- migrate:down
DROP TABLE IF EXISTS bronze.raw_hectolitros;
