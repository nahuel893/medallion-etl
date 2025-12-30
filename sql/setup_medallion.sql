-- Archivo: sql/setup_medallion.sql
-- Descripción: Configuración inicial de arquitectura Medallion (Schemas + Roles)

-- ==========================================
-- 1. CREACIÓN DE ESQUEMAS (CAPAS LÓGICAS)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==========================================
-- 2. GESTIÓN DE USUARIOS (IDEMPOTENTE CON \gexec)
-- ==========================================
-- Nota: Usamos \gexec para poder usar variables psql (:variable) dentro de la lógica condicional.

-- 2.1 Crear Usuario ETL si no existe
SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'etl_user', :'etl_password')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = :'etl_user')
\gexec

-- 2.2 Crear Usuario Reportes si no existe
SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'readonly_user', :'readonly_password')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = :'readonly_user')
\gexec

-- Permisos de conexión básicos
GRANT CONNECT ON DATABASE :db_name TO :etl_user;
GRANT CONNECT ON DATABASE :db_name TO :readonly_user;

-- ==========================================
-- 3. PERMISOS PARA EL USUARIO ETL (El Constructor)
-- ==========================================
-- Nota: Usamos :etl_user (sin comillas simples) para que psql lo pegue como identificador.

-- Capa BRONZE
GRANT USAGE, CREATE ON SCHEMA bronze TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL PRIVILEGES ON TABLES TO :etl_user;


CREATE TABLE IF NOT EXISTS bronze.raw_sales (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB,  -- <--- LA JOYA DE LA CORONA
    date_comprobante DATE
);

-- Creación del Índice (Vital para que el DELETE por fecha sea instantáneo)
CREATE INDEX IF NOT EXISTS idx_bronze_sales_date
ON bronze.raw_sales(date_comprobante);

CREATE TABLE IF NOT EXISTS bronze.raw_clientes (
      id SERIAL PRIMARY KEY,
      ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      source_system VARCHAR(50),
      data_raw JSONB,
      date_carga DATE
  );

CREATE TABLE IF NOT EXISTS bronze.raw_articles(
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB

);

CREATE TABLE IF NOT EXISTS bronze.raw_staff(
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB

);

CREATE TABLE IF NOT EXISTS bronze.raw_routes(
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB

);

CREATE TABLE IF NOT EXISTS bronze.raw_stock (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB,
    date_stock DATE,
    id_deposito INTEGER
);

CREATE INDEX idx_stock_date ON bronze.raw_stock(date_stock);
CREATE INDEX idx_stock_deposito ON bronze.raw_stock(id_deposito);
CREATE INDEX idx_stock_ingestion ON bronze.raw_stock(ingestion_at);

-- Capa SILVER
GRANT USAGE, CREATE ON SCHEMA silver TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL PRIVILEGES ON TABLES TO :etl_user;

-- ==========================================
-- TABLAS SILVER - Datos parseados y tipados
-- ==========================================

-- Tabla de líneas de venta (fact table desnormalizada)
CREATE TABLE IF NOT EXISTS silver.fact_ventas (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_id INTEGER REFERENCES bronze.raw_sales(id),

    -- Identificación documento
    id_empresa INTEGER NOT NULL,
    ds_empresa VARCHAR(100),
    id_documento VARCHAR(20) NOT NULL,
    ds_documento VARCHAR(100),
    letra CHAR(1),
    serie INTEGER,
    nro_doc INTEGER NOT NULL,
    anulado BOOLEAN DEFAULT FALSE,

    -- Fechas
    fecha_comprobante DATE NOT NULL,
    fecha_alta DATE,
    fecha_pedido DATE,
    fecha_entrega DATE,
    fecha_vencimiento DATE,
    fecha_caja DATE,

    -- Organización
    id_sucursal INTEGER,
    ds_sucursal VARCHAR(100),
    id_deposito INTEGER,
    ds_deposito VARCHAR(100),

    -- Personal
    id_vendedor INTEGER,
    ds_vendedor VARCHAR(100),
    id_supervisor INTEGER,
    ds_supervisor VARCHAR(100),
    id_gerente INTEGER,
    ds_gerente VARCHAR(100),

    -- Cliente
    id_cliente INTEGER NOT NULL,
    nombre_cliente VARCHAR(200),
    domicilio_cliente VARCHAR(300),
    codigo_postal VARCHAR(20),
    id_localidad INTEGER,
    ds_localidad VARCHAR(100),
    id_provincia VARCHAR(10),
    ds_provincia VARCHAR(100),

    -- Pago
    id_tipo_pago INTEGER,
    ds_tipo_pago VARCHAR(50),

    -- Segmentación comercial
    id_negocio INTEGER,
    ds_negocio VARCHAR(100),
    id_canal_mkt INTEGER,
    ds_canal_mkt VARCHAR(100),
    id_segmento_mkt INTEGER,
    ds_segmento_mkt VARCHAR(100),
    id_area INTEGER,
    ds_area VARCHAR(100),

    -- Línea de venta (detalle)
    id_linea INTEGER NOT NULL,
    id_articulo INTEGER NOT NULL,
    ds_articulo VARCHAR(200),
    id_concepto INTEGER,
    ds_concepto VARCHAR(100),
    es_combo BOOLEAN DEFAULT FALSE,
    id_combo INTEGER,

    -- Artículo estadístico
    id_articulo_estadistico INTEGER,
    ds_articulo_estadistico VARCHAR(200),
    presentacion_articulo VARCHAR(50),

    -- Cantidades
    cantidad_solicitada NUMERIC(15,4),
    unidades_solicitadas NUMERIC(15,4),
    cantidades_con_cargo NUMERIC(15,4),
    cantidades_sin_cargo NUMERIC(15,4),
    cantidades_total NUMERIC(15,4),
    cantidades_rechazo NUMERIC(15,4),
    peso NUMERIC(15,4),
    peso_total NUMERIC(15,4),

    -- Precios
    precio_unitario_bruto NUMERIC(15,4),
    bonificacion NUMERIC(8,4),
    precio_unitario_neto NUMERIC(15,4),

    -- Subtotales línea
    subtotal_bruto NUMERIC(15,4),
    subtotal_bonificado NUMERIC(15,4),
    subtotal_neto NUMERIC(15,4),
    subtotal_final NUMERIC(15,4),

    -- Impuestos
    iva21 NUMERIC(15,4),
    iva27 NUMERIC(15,4),
    iva105 NUMERIC(15,4),
    internos NUMERIC(15,4),
    per3337 NUMERIC(15,4),
    percepcion212 NUMERIC(15,4),
    percepcion_iibb NUMERIC(15,4),

    -- Trade spend
    totradspend NUMERIC(15,4),

    -- Metadata
    origen VARCHAR(50),
    id_rechazo INTEGER,
    ds_rechazo VARCHAR(100)
);

-- Índices para consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_silver_ventas_fecha ON silver.fact_ventas(fecha_comprobante);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_cliente ON silver.fact_ventas(id_cliente);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_articulo ON silver.fact_ventas(id_articulo);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_vendedor ON silver.fact_ventas(id_vendedor);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_documento ON silver.fact_ventas(id_documento, serie, nro_doc);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_bronze ON silver.fact_ventas(bronze_id);

-- Capa GOLD
GRANT USAGE, CREATE ON SCHEMA gold TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL PRIVILEGES ON TABLES TO :etl_user;

-- ==========================================
-- 4. PERMISOS PARA EL USUARIO REPORTING (El Consumidor)
-- ==========================================

-- Solo permiso de entrada (USAGE) a Gold
GRANT USAGE ON SCHEMA gold TO :readonly_user;

-- Solo lectura (SELECT) en las tablas actuales
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO :readonly_user;

-- Asegurar que pueda leer tablas futuras creadas por el ETL
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO :readonly_user;

-- Opcional: Revocar acceso a public por seguridad
REVOKE ALL ON SCHEMA public FROM :readonly_user;REVOKE ALL ON SCHEMA public FROM :readonly_user;

