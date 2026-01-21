-- Archivo: sql/setup_medallion.sql
-- Descripción: Configuración inicial de arquitectura Medallion (Schemas + Roles)

-- ==========================================
-- 1. CREACIÓN DE USUARIOS (ANTES DE TODO)
-- ==========================================
-- Crear usuarios si no existen (usando \gexec para manejar variables psql)

SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'etl_user', :'etl_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'etl_user')
\gexec

SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'readonly_user', :'readonly_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'readonly_user')
\gexec

-- ==========================================
-- 2. CREACIÓN DE ESQUEMAS (CAPAS LÓGICAS)
-- ==========================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==========================================
-- 3. PERMISOS DE CONEXIÓN
-- ==========================================
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

CREATE TABLE IF NOT EXISTS bronze.raw_clients (
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

CREATE INDEX IF NOT EXISTS idx_stock_date ON bronze.raw_stock(date_stock);
CREATE INDEX IF NOT EXISTS idx_stock_deposito ON bronze.raw_stock(id_deposito);
CREATE INDEX IF NOT EXISTS idx_stock_ingestion ON bronze.raw_stock(ingestion_at);

CREATE TABLE IF NOT EXISTS bronze.raw_deposits (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    id_deposito INTEGER NOT NULL,
    descripcion VARCHAR(255),
    sucursal VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_bronze_deposits_id
ON bronze.raw_deposits(id_deposito);

CREATE TABLE IF NOT EXISTS bronze.raw_marketing (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB
);

-- Capa SILVER
GRANT USAGE, CREATE ON SCHEMA silver TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL PRIVILEGES ON TABLES TO :etl_user;

-- ==========================================
-- TABLAS SILVER - Datos parseados y tipados
-- ==========================================

-- Tabla que guarda los datos de clientes
CREATE TABLE IF NOT EXISTS silver.clients (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_id INTEGER,  -- Referencia a bronze.raw_clients (sin FK para independencia de capas)

    -- === DATOS PRINCIPALES ===
    id_cliente INTEGER NOT NULL,
    razon_social VARCHAR(150),
    fantasia VARCHAR(150),
    id_ramo INTEGER,
    desc_ramo VARCHAR(100),
    anulado BOOLEAN DEFAULT FALSE,
    calle VARCHAR(150),
    id_localidad INTEGER,
    desc_localidad VARCHAR(100),
    id_provincia VARCHAR(10),
    desc_provincia VARCHAR(100),

    -- === FECHAS ===
    fecha_alta DATE,
    fecha_baja DATE,

    -- === ORGANIZACIÓN (FK a branches) ===
    id_sucursal INTEGER,  -- FK a silver.branches

    -- === DATOS FISCALES (desde eClialias) ===
    identificador VARCHAR(30),
    id_tipo_identificador INTEGER,
    desc_tipo_identificador VARCHAR(50),
    id_tipo_contribuyente VARCHAR(10),
    desc_tipo_contribuyente VARCHAR(100),
    es_inscripto_iibb BOOLEAN,

    -- === COMERCIAL ===
    id_lista_precio INTEGER,
    desc_lista_precio VARCHAR(100),
    id_canal_mkt INTEGER,
    desc_canal_mkt VARCHAR(100),
    id_segmento_mkt INTEGER,
    desc_segmento_mkt VARCHAR(100),
    id_subcanal_mkt INTEGER,
    desc_subcanal_mkt VARCHAR(100),

    -- === GEOLOCALIZACIÓN ===
    latitud NUMERIC(15, 6),
    longitud NUMERIC(15, 6),

    -- === CONTACTO ===
    telefono_fijo VARCHAR(50),
    telefono_movil VARCHAR(50),
    email VARCHAR(150),

    -- Constraint para evitar duplicados
    UNIQUE(id_cliente)
);

-- Índices para consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_silver_clients_id_cliente ON silver.clients(id_cliente);
CREATE INDEX IF NOT EXISTS idx_silver_clients_id_sucursal ON silver.clients(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_silver_clients_segmento ON silver.clients(id_segmento_mkt);
CREATE INDEX IF NOT EXISTS idx_silver_clients_subcanal ON silver.clients(id_subcanal_mkt);

-- ==========================================
-- DIMENSIONES NORMALIZADAS
-- ==========================================

-- Tabla de sucursales (carga manual de descripciones)
CREATE TABLE IF NOT EXISTS silver.branches (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_sucursal INTEGER NOT NULL UNIQUE,
    descripcion VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_branches_sucursal ON silver.branches(id_sucursal);

-- Tabla de fuerzas de venta (derivada de staff)
CREATE TABLE IF NOT EXISTS silver.sales_forces (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_fuerza_ventas INTEGER NOT NULL UNIQUE,
    des_fuerza_ventas VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_sales_forces_fuerza ON silver.sales_forces(id_fuerza_ventas);

-- Tabla de preventistas/personal
CREATE TABLE IF NOT EXISTS silver.staff (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_id INTEGER,  -- Referencia a bronze.raw_staff (sin FK para independencia de capas)

    id_personal INTEGER NOT NULL UNIQUE,
    des_personal VARCHAR(150),
    cargo VARCHAR(50),
    tipo_venta VARCHAR(10),
    usuario_sistema VARCHAR(50),
    telefono VARCHAR(50),
    domicilio VARCHAR(200),
    fecha_nacimiento DATE,

    -- FKs
    id_sucursal INTEGER,
    id_fuerza_ventas INTEGER,
    id_personal_superior INTEGER
);

CREATE INDEX IF NOT EXISTS idx_staff_personal ON silver.staff(id_personal);
CREATE INDEX IF NOT EXISTS idx_staff_sucursal ON silver.staff(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_staff_fuerza ON silver.staff(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_staff_superior ON silver.staff(id_personal_superior);

-- Tabla de rutas
CREATE TABLE IF NOT EXISTS silver.routes (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_id INTEGER,  -- Referencia a bronze.raw_routes (sin FK para independencia de capas)

    id_ruta INTEGER NOT NULL,
    des_ruta VARCHAR(150),
    dias_visita VARCHAR(30),
    semana_visita INTEGER,
    periodicidad_visita INTEGER,
    dias_entrega VARCHAR(30),
    semana_entrega INTEGER,
    periodicidad_entrega INTEGER,
    id_modo_atencion VARCHAR(10),
    des_modo_atencion VARCHAR(50),
    fecha_desde DATE,
    fecha_hasta DATE,
    anulado BOOLEAN DEFAULT FALSE,

    -- FKs
    id_sucursal INTEGER,
    id_fuerza_ventas INTEGER,
    id_personal INTEGER,

    -- Una ruta puede tener múltiples vigencias
    UNIQUE(id_ruta, fecha_desde)
);

CREATE INDEX IF NOT EXISTS idx_routes_ruta ON silver.routes(id_ruta);
CREATE INDEX IF NOT EXISTS idx_routes_sucursal ON silver.routes(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_routes_fuerza ON silver.routes(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_routes_personal ON silver.routes(id_personal);

-- ==========================================
-- TABLAS DE RELACIÓN (JUNCTION)
-- ==========================================

-- Tabla de asignaciones cliente-ruta
-- Un cliente pertenece a una fuerza de venta a través de su ruta asignada
CREATE TABLE IF NOT EXISTS silver.client_forces (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- FKs
    id_cliente INTEGER NOT NULL,
    id_ruta INTEGER NOT NULL,  -- FK a routes (la fuerza viene implícita de la ruta)

    -- Datos de la asignación
    dias_visita VARCHAR(30),
    semana_visita INTEGER,
    periodicidad_visita INTEGER,
    id_modo_atencion VARCHAR(10),
    fecha_inicio DATE,
    fecha_fin DATE,

    -- Constraint: un cliente puede tener una sola asignación por ruta y fecha
    UNIQUE(id_cliente, id_ruta, fecha_inicio)
);

-- Índices para client_forces
CREATE INDEX IF NOT EXISTS idx_client_forces_cliente ON silver.client_forces(id_cliente);
CREATE INDEX IF NOT EXISTS idx_client_forces_ruta ON silver.client_forces(id_ruta);
CREATE INDEX IF NOT EXISTS idx_client_forces_activas ON silver.client_forces(fecha_fin) WHERE fecha_fin = '9999-12-31';


-- Tabla de artículos (normalizada - sin agrupaciones encolumnadas)
CREATE TABLE IF NOT EXISTS silver.articles (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_id INTEGER,  -- Referencia a bronze.raw_articles (sin FK para independencia de capas)

    -- === DATOS PRINCIPALES ===
    id_articulo INTEGER NOT NULL UNIQUE,
    des_articulo VARCHAR(200),
    des_corta_articulo VARCHAR(100),
    anulado BOOLEAN DEFAULT FALSE,
    fecha_alta DATE,

    -- === CARACTERÍSTICAS ===
    es_combo BOOLEAN DEFAULT FALSE,
    es_alcoholico BOOLEAN DEFAULT FALSE,
    es_activo_fijo BOOLEAN DEFAULT FALSE,
    pesable BOOLEAN DEFAULT FALSE,
    visible_mobile BOOLEAN DEFAULT TRUE,
    tiene_retornables BOOLEAN DEFAULT FALSE,

    -- === UNIDADES Y PRESENTACIÓN ===
    id_unidad_medida INTEGER,
    des_unidad_medida VARCHAR(50),
    valor_unidad_medida NUMERIC(10,4),
    unidades_bulto INTEGER,
    id_presentacion_bulto VARCHAR(10),
    des_presentacion_bulto VARCHAR(50),
    id_presentacion_unidad VARCHAR(10),
    des_presentacion_unidad VARCHAR(50),

    -- === CÓDIGOS DE BARRA ===
    cod_barra_bulto VARCHAR(50),
    cod_barra_unidad VARCHAR(50),

    -- === IMPUESTOS ===
    tasa_iva NUMERIC(8,4),
    tasa_iibb NUMERIC(8,4),
    tasa_internos NUMERIC(8,4),
    internos_bulto NUMERIC(15,4),
    exento_iva BOOLEAN DEFAULT FALSE,
    iva_diferencial BOOLEAN DEFAULT FALSE,

    -- === LOGÍSTICA ===
    peso_bulto NUMERIC(10,4),
    bultos_pallet INTEGER,
    pisos_pallet INTEGER
);

CREATE INDEX IF NOT EXISTS idx_silver_articles_id_articulo ON silver.articles(id_articulo);

-- Tabla normalizada de agrupaciones de artículos
-- Una fila por cada agrupación (MARCA, GENERICO, CALIBRE, etc.)
CREATE TABLE IF NOT EXISTS silver.article_groupings (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- FK al artículo
    id_articulo INTEGER NOT NULL,

    -- Datos de la agrupación
    id_forma_agrupar VARCHAR(30) NOT NULL,  -- MARCA, GENERICO, CALIBRE, ESQUEMA, PROVEED, UNIDAD DE NEGOCIO
    id_agrupacion VARCHAR(50),
    des_agrupacion VARCHAR(150),

    -- Un artículo tiene una sola agrupación por tipo
    UNIQUE(id_articulo, id_forma_agrupar)
);

CREATE INDEX IF NOT EXISTS idx_article_groupings_articulo ON silver.article_groupings(id_articulo);
CREATE INDEX IF NOT EXISTS idx_article_groupings_forma ON silver.article_groupings(id_forma_agrupar);
CREATE INDEX IF NOT EXISTS idx_article_groupings_agrupacion ON silver.article_groupings(id_agrupacion);


-- ==========================================
-- MARKETING - Segmentación comercial (3 niveles)
-- ==========================================

-- Nivel 1: Segmentos
CREATE TABLE IF NOT EXISTS silver.marketing_segments (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    id_segmento_mkt INTEGER NOT NULL UNIQUE,
    des_segmento_mkt VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_mkt_segments_id ON silver.marketing_segments(id_segmento_mkt);

-- Nivel 2: Canales (FK a segmento)
CREATE TABLE IF NOT EXISTS silver.marketing_channels (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    id_canal_mkt INTEGER NOT NULL UNIQUE,
    des_canal_mkt VARCHAR(100),
    id_segmento_mkt INTEGER  -- FK a marketing_segments
);

CREATE INDEX IF NOT EXISTS idx_mkt_channels_id ON silver.marketing_channels(id_canal_mkt);
CREATE INDEX IF NOT EXISTS idx_mkt_channels_segmento ON silver.marketing_channels(id_segmento_mkt);

-- Nivel 3: Subcanales (FK a canal)
CREATE TABLE IF NOT EXISTS silver.marketing_subchannels (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    id_subcanal_mkt INTEGER NOT NULL UNIQUE,
    des_subcanal_mkt VARCHAR(100),
    id_canal_mkt INTEGER  -- FK a marketing_channels
);

CREATE INDEX IF NOT EXISTS idx_mkt_subchannels_id ON silver.marketing_subchannels(id_subcanal_mkt);
CREATE INDEX IF NOT EXISTS idx_mkt_subchannels_canal ON silver.marketing_subchannels(id_canal_mkt);


-- Tabla de líneas de venta (fact table NORMALIZADA)
-- Solo IDs de dimensiones + datos propios del hecho (sin descripciones redundantes)
CREATE TABLE IF NOT EXISTS silver.fact_ventas (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    bronze_id INTEGER,  -- Referencia a bronze.raw_sales (sin FK para independencia de capas)

    -- === IDENTIFICACIÓN DOCUMENTO ===
    id_empresa INTEGER NOT NULL,
    id_documento VARCHAR(20) NOT NULL,
    letra CHAR(1),
    serie INTEGER,
    nro_doc INTEGER NOT NULL,
    anulado BOOLEAN DEFAULT FALSE,

    -- === FECHAS ===
    fecha_comprobante DATE NOT NULL,
    fecha_alta DATE,
    fecha_pedido DATE,
    fecha_entrega DATE,
    fecha_vencimiento DATE,
    fecha_caja DATE,
    fecha_anulacion DATE,
    fecha_pago DATE,
    fecha_liquidacion DATE,
    fecha_asiento_contable DATE,

    -- === ORGANIZACIÓN (FKs a dimensiones) ===
    id_sucursal INTEGER,          -- FK a silver.branches
    id_deposito INTEGER,          -- FK a silver.deposits
    id_caja INTEGER,
    cajero VARCHAR(100),
    id_centro_costo INTEGER,

    -- === PERSONAL (FKs a dimensiones) ===
    id_vendedor INTEGER,          -- FK a silver.staff
    id_supervisor INTEGER,        -- FK a silver.staff
    id_gerente INTEGER,           -- FK a silver.staff
    id_fuerza_ventas INTEGER,     -- FK a silver.sales_forces
    usuario_alta VARCHAR(100),

    -- === CLIENTE (FK a dimensión) ===
    id_cliente INTEGER NOT NULL,  -- FK a silver.clients
    linea_credito VARCHAR(200),

    -- === SEGMENTACIÓN COMERCIAL (FKs a dimensiones) ===
    id_canal_mkt INTEGER,         -- FK a silver.marketing
    id_segmento_mkt INTEGER,      -- FK a silver.marketing
    id_subcanal_mkt INTEGER,      -- FK a silver.marketing

    -- === LOGÍSTICA ===
    id_fletero_carga INTEGER,     -- FK a silver.staff
    planilla_carga VARCHAR(50),

    -- === LÍNEA DE VENTA ===
    id_articulo INTEGER NOT NULL, -- FK a silver.articles
    es_combo BOOLEAN DEFAULT FALSE,
    id_combo INTEGER,
    id_pedido INTEGER,
    id_origen VARCHAR(150),
    origen VARCHAR(50),
    acciones VARCHAR(100),

    -- === CANTIDADES ===
    cantidades_con_cargo NUMERIC(15,4),
    cantidades_sin_cargo NUMERIC(15,4),
    cantidades_total NUMERIC(15,4),
    cantidades_rechazo NUMERIC(15,4),

    -- === PRECIOS ===
    precio_unitario_bruto NUMERIC(15,4),
    precio_unitario_neto NUMERIC(15,4),
    bonificacion NUMERIC(8,4),
    precio_compra_bruto NUMERIC(15,4),
    precio_compra_neto NUMERIC(15,4),

    -- === SUBTOTALES ===
    subtotal_bruto NUMERIC(15,4),
    subtotal_bonificado NUMERIC(15,4),
    subtotal_neto NUMERIC(15,4),
    subtotal_final NUMERIC(15,4),

    -- === IMPUESTOS ===
    iva21 NUMERIC(15,4),
    iva27 NUMERIC(15,4),
    iva105 NUMERIC(15,4),
    iva2 NUMERIC(15,4),
    internos NUMERIC(15,4),
    per3337 NUMERIC(15,4),
    percepcion212 NUMERIC(15,4),
    percepcion_iibb NUMERIC(15,4),
    pers_iibb_d NUMERIC(15,4),
    pers_iibb_r NUMERIC(15,4),
    cod_prov_iibb VARCHAR(10),

    -- === CONTABILIDAD ===
    cod_cuenta_contable VARCHAR(50),
    nro_asiento_contable INTEGER,
    nro_plan_contable INTEGER,
    id_liquidacion INTEGER,

    -- === PROVEEDOR ===
    proveedor VARCHAR(100),
    fvig_pcompra DATE,

    -- === METADATA / RECHAZO ===
    id_rechazo INTEGER,
    informado BOOLEAN,
    regimen_fiscal VARCHAR(50)
);

-- Índices para consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_silver_ventas_fecha ON silver.fact_ventas(fecha_comprobante);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_cliente ON silver.fact_ventas(id_cliente);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_articulo ON silver.fact_ventas(id_articulo);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_vendedor ON silver.fact_ventas(id_vendedor);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_sucursal ON silver.fact_ventas(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_fuerza ON silver.fact_ventas(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_documento ON silver.fact_ventas(id_documento, serie, nro_doc);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_bronze ON silver.fact_ventas(bronze_id);

-- Tabla de stock (fact table)
CREATE TABLE IF NOT EXISTS silver.fact_stock (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Dimensiones
    date_stock DATE NOT NULL,
    id_deposito INTEGER NOT NULL,
    id_almacen INTEGER,
    id_articulo INTEGER NOT NULL,

    -- Descripciones (desnormalizado para consultas rápidas)
    ds_articulo VARCHAR(200),

    -- Métricas
    cant_bultos NUMERIC(15,4),
    cant_unidades NUMERIC(15,4),

    -- Lote
    fec_vto_lote DATE
);

CREATE INDEX IF NOT EXISTS idx_silver_stock_fecha ON silver.fact_stock(date_stock);
CREATE INDEX IF NOT EXISTS idx_silver_stock_deposito ON silver.fact_stock(id_deposito);
CREATE INDEX IF NOT EXISTS idx_silver_stock_articulo ON silver.fact_stock(id_articulo);
CREATE INDEX IF NOT EXISTS idx_silver_stock_fecha_deposito ON silver.fact_stock(date_stock, id_deposito);
CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_stock_unique ON silver.fact_stock(date_stock, id_deposito, id_articulo);

-- Capa GOLD
GRANT USAGE, CREATE ON SCHEMA gold TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL PRIVILEGES ON TABLES TO :etl_user;

-- ==========================================
-- GOLD - ESQUEMA ESTRELLA (Star Schema)
-- ==========================================

-- Dimensión Tiempo
CREATE TABLE IF NOT EXISTS gold.dim_tiempo (
    fecha DATE PRIMARY KEY,
    dia INTEGER,
    dia_semana INTEGER,
    nombre_dia VARCHAR(15),
    semana INTEGER,
    mes INTEGER,
    nombre_mes VARCHAR(15),
    trimestre INTEGER,
    anio INTEGER
);

CREATE INDEX IF NOT EXISTS idx_dim_tiempo_anio_mes ON gold.dim_tiempo(anio, mes);

-- Dimensión Sucursal
CREATE TABLE IF NOT EXISTS gold.dim_sucursal (
    id_sucursal INTEGER PRIMARY KEY,
    descripcion VARCHAR(100)
);

-- Dimensión Vendedor
CREATE TABLE IF NOT EXISTS gold.dim_vendedor (
    id_vendedor INTEGER PRIMARY KEY,
    des_vendedor VARCHAR(150),
    id_fuerza_ventas INTEGER,
    id_sucursal INTEGER,
    des_sucursal VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_dim_vendedor_sucursal ON gold.dim_vendedor(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_dim_vendedor_fuerza ON gold.dim_vendedor(id_fuerza_ventas);

-- Dimensión Artículo
CREATE TABLE IF NOT EXISTS gold.dim_articulo (
    id_articulo INTEGER PRIMARY KEY,
    des_articulo VARCHAR(200),
    marca VARCHAR(150),
    generico VARCHAR(150),
    calibre VARCHAR(150),
    proveedor VARCHAR(150),
    unidad_negocio VARCHAR(150)
);

CREATE INDEX IF NOT EXISTS idx_dim_articulo_marca ON gold.dim_articulo(marca);
CREATE INDEX IF NOT EXISTS idx_dim_articulo_proveedor ON gold.dim_articulo(proveedor);

-- Dimensión Cliente
CREATE TABLE IF NOT EXISTS gold.dim_cliente (
    id_cliente INTEGER PRIMARY KEY,
    razon_social VARCHAR(150),
    fantasia VARCHAR(150),

    -- Sucursal
    id_sucursal INTEGER,
    des_sucursal VARCHAR(100),

    -- Marketing
    id_canal_mkt INTEGER,
    des_canal_mkt VARCHAR(100),
    id_segmento_mkt INTEGER,
    des_segmento_mkt VARCHAR(100),
    id_subcanal_mkt INTEGER,
    des_subcanal_mkt VARCHAR(100),

    -- Ruta/Preventista Fuerza Ventas 1
    id_ruta_fv1 INTEGER,
    des_personal_fv1 VARCHAR(150),

    -- Ruta/Preventista Fuerza Ventas 4
    id_ruta_fv4 INTEGER,
    des_personal_fv4 VARCHAR(150),

    -- Clasificación
    id_ramo INTEGER,
    des_ramo VARCHAR(100),
    id_localidad INTEGER,
    des_localidad VARCHAR(100),
    id_provincia VARCHAR(10),
    des_provincia VARCHAR(100),

    -- Geolocalización
    latitud NUMERIC(15, 6),
    longitud NUMERIC(15, 6),

    -- Lista de precio
    id_lista_precio INTEGER,
    des_lista_precio VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_dim_cliente_sucursal ON gold.dim_cliente(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_canal ON gold.dim_cliente(id_canal_mkt);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_segmento ON gold.dim_cliente(id_segmento_mkt);

-- Fact Table Ventas
CREATE TABLE IF NOT EXISTS gold.fact_ventas (
    id SERIAL PRIMARY KEY,

    -- Claves de dimensión
    id_cliente INTEGER,
    id_articulo INTEGER,
    id_vendedor INTEGER,
    id_sucursal INTEGER,
    fecha_comprobante DATE,

    -- Identificación documento
    id_documento VARCHAR(20),
    letra CHAR(1),
    serie INTEGER,
    nro_doc INTEGER,
    anulado BOOLEAN DEFAULT FALSE,

    -- Métricas
    cantidades_con_cargo NUMERIC(15,4),
    cantidades_sin_cargo NUMERIC(15,4),
    cantidades_total NUMERIC(15,4),
    subtotal_neto NUMERIC(15,4),
    subtotal_final NUMERIC(15,4),
    bonificacion NUMERIC(8,4)
);

CREATE INDEX IF NOT EXISTS idx_gold_fact_fecha ON gold.fact_ventas(fecha_comprobante);
CREATE INDEX IF NOT EXISTS idx_gold_fact_cliente ON gold.fact_ventas(id_cliente);
CREATE INDEX IF NOT EXISTS idx_gold_fact_articulo ON gold.fact_ventas(id_articulo);
CREATE INDEX IF NOT EXISTS idx_gold_fact_vendedor ON gold.fact_ventas(id_vendedor);
CREATE INDEX IF NOT EXISTS idx_gold_fact_sucursal ON gold.fact_ventas(id_sucursal);

-- Fact Table Stock
CREATE TABLE IF NOT EXISTS gold.fact_stock (
    id SERIAL PRIMARY KEY,

    -- Dimensiones (FK a dimensiones)
    date_stock DATE NOT NULL,
    id_deposito INTEGER NOT NULL,
    id_articulo INTEGER NOT NULL,

    -- Métricas
    cant_bultos NUMERIC(15,4),
    cant_unidades NUMERIC(15,4)
);

CREATE INDEX IF NOT EXISTS idx_gold_stock_fecha ON gold.fact_stock(date_stock);
CREATE INDEX IF NOT EXISTS idx_gold_stock_deposito ON gold.fact_stock(id_deposito);
CREATE INDEX IF NOT EXISTS idx_gold_stock_articulo ON gold.fact_stock(id_articulo);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_stock_unique ON gold.fact_stock(date_stock, id_deposito, id_articulo);

-- Tablas de Cobertura (agregaciones mensuales)

-- Cobertura por Preventista/Ruta/Sucursal/Marca
CREATE TABLE IF NOT EXISTS gold.cob_preventista_marca (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,  -- Primer día del mes
    id_fuerza_ventas INTEGER NOT NULL,
    id_vendedor INTEGER,
    id_ruta INTEGER,
    id_sucursal INTEGER,
    ds_sucursal VARCHAR(100),
    marca VARCHAR(150),
    clientes_compradores INTEGER,
    volumen_total NUMERIC(15,4)
);

CREATE INDEX IF NOT EXISTS idx_cob_prev_marca_periodo ON gold.cob_preventista_marca(periodo);
CREATE INDEX IF NOT EXISTS idx_cob_prev_marca_fuerza ON gold.cob_preventista_marca(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_cob_prev_marca_vendedor ON gold.cob_preventista_marca(id_vendedor);
CREATE INDEX IF NOT EXISTS idx_cob_prev_marca_sucursal ON gold.cob_preventista_marca(id_sucursal);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cob_prev_marca_unique ON gold.cob_preventista_marca(periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, marca);

-- Cobertura por Sucursal/Marca
CREATE TABLE IF NOT EXISTS gold.cob_sucursal_marca (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    id_fuerza_ventas INTEGER NOT NULL,
    id_sucursal INTEGER,
    ds_sucursal VARCHAR(100),
    marca VARCHAR(150),
    clientes_compradores INTEGER,
    volumen_total NUMERIC(15,4)
);

CREATE INDEX IF NOT EXISTS idx_cob_suc_marca_periodo ON gold.cob_sucursal_marca(periodo);
CREATE INDEX IF NOT EXISTS idx_cob_suc_marca_fuerza ON gold.cob_sucursal_marca(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_cob_suc_marca_sucursal ON gold.cob_sucursal_marca(id_sucursal);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cob_suc_marca_unique ON gold.cob_sucursal_marca(periodo, id_fuerza_ventas, id_sucursal, marca);

-- Cobertura por Preventista/Ruta/Sucursal/Genérico
CREATE TABLE IF NOT EXISTS gold.cob_preventista_generico (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
    id_fuerza_ventas INTEGER NOT NULL,
    id_vendedor INTEGER,
    id_ruta INTEGER,
    id_sucursal INTEGER,
    ds_sucursal VARCHAR(100),
    generico VARCHAR(150),
    clientes_compradores INTEGER,
    volumen_total NUMERIC(15,4)
);

CREATE INDEX IF NOT EXISTS idx_cob_prev_gen_periodo ON gold.cob_preventista_generico(periodo);
CREATE INDEX IF NOT EXISTS idx_cob_prev_gen_fuerza ON gold.cob_preventista_generico(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_cob_prev_gen_vendedor ON gold.cob_preventista_generico(id_vendedor);
CREATE INDEX IF NOT EXISTS idx_cob_prev_gen_sucursal ON gold.cob_preventista_generico(id_sucursal);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cob_prev_gen_unique ON gold.cob_preventista_generico(periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, generico);

-- ==========================================
-- 4. PERMISOS FINALES
-- ==========================================

-- ETL User: permisos completos sobre todas las tablas creadas
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO :etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO :etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO :etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold TO :etl_user;

-- Readonly User: solo lectura en Gold
GRANT USAGE ON SCHEMA gold TO :readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO :readonly_user;

-- Permisos para tablas futuras
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO :readonly_user;

-- Opcional: Revocar acceso a public por seguridad
REVOKE ALL ON SCHEMA public FROM :readonly_user;

