-- migrate:up
-- BASELINE: Estado inicial del esquema medallion_db
-- Este archivo documenta el estado del DDL al momento de adoptar dbmate.
-- NO ejecutar en BDs existentes - marcar como aplicado con: dbmate migrate

-- ==========================================
-- SCHEMAS
-- ==========================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==========================================
-- BRONZE
-- ==========================================
CREATE TABLE IF NOT EXISTS bronze.raw_sales (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB,
    date_comprobante DATE
);
CREATE INDEX IF NOT EXISTS idx_bronze_sales_date ON bronze.raw_sales(date_comprobante);

CREATE TABLE IF NOT EXISTS bronze.raw_clients (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB,
    date_carga DATE
);

CREATE TABLE IF NOT EXISTS bronze.raw_articles (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB
);

CREATE TABLE IF NOT EXISTS bronze.raw_staff (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB
);

CREATE TABLE IF NOT EXISTS bronze.raw_routes (
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
CREATE INDEX IF NOT EXISTS idx_bronze_deposits_id ON bronze.raw_deposits(id_deposito);

CREATE TABLE IF NOT EXISTS bronze.raw_marketing (
    id SERIAL PRIMARY KEY,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50),
    data_raw JSONB
);

-- ==========================================
-- SILVER
-- ==========================================
CREATE TABLE IF NOT EXISTS silver.clients (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    fecha_alta DATE,
    fecha_baja DATE,
    id_sucursal INTEGER,
    identificador VARCHAR(30),
    id_tipo_identificador INTEGER,
    desc_tipo_identificador VARCHAR(50),
    id_tipo_contribuyente VARCHAR(10),
    desc_tipo_contribuyente VARCHAR(100),
    es_inscripto_iibb BOOLEAN,
    id_lista_precio INTEGER,
    desc_lista_precio VARCHAR(100),
    id_canal_mkt INTEGER,
    desc_canal_mkt VARCHAR(100),
    id_segmento_mkt INTEGER,
    desc_segmento_mkt VARCHAR(100),
    id_subcanal_mkt INTEGER,
    desc_subcanal_mkt VARCHAR(100),
    latitud NUMERIC(15, 6),
    longitud NUMERIC(15, 6),
    telefono_fijo VARCHAR(50),
    telefono_movil VARCHAR(50),
    email VARCHAR(150),
    UNIQUE(id_cliente)
);
CREATE INDEX IF NOT EXISTS idx_silver_clients_id_cliente ON silver.clients(id_cliente);
CREATE INDEX IF NOT EXISTS idx_silver_clients_id_sucursal ON silver.clients(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_silver_clients_segmento ON silver.clients(id_segmento_mkt);
CREATE INDEX IF NOT EXISTS idx_silver_clients_subcanal ON silver.clients(id_subcanal_mkt);

CREATE TABLE IF NOT EXISTS silver.branches (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_sucursal INTEGER NOT NULL UNIQUE,
    descripcion VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_branches_sucursal ON silver.branches(id_sucursal);

CREATE TABLE IF NOT EXISTS silver.sales_forces (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_fuerza_ventas INTEGER NOT NULL UNIQUE,
    des_fuerza_ventas VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_sales_forces_fuerza ON silver.sales_forces(id_fuerza_ventas);

CREATE TABLE IF NOT EXISTS silver.staff (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_personal INTEGER NOT NULL UNIQUE,
    des_personal VARCHAR(150),
    cargo VARCHAR(50),
    tipo_venta VARCHAR(10),
    usuario_sistema VARCHAR(50),
    telefono VARCHAR(50),
    domicilio VARCHAR(200),
    fecha_nacimiento DATE,
    id_sucursal INTEGER,
    id_fuerza_ventas INTEGER,
    id_personal_superior INTEGER
);
CREATE INDEX IF NOT EXISTS idx_staff_personal ON silver.staff(id_personal);
CREATE INDEX IF NOT EXISTS idx_staff_sucursal ON silver.staff(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_staff_fuerza ON silver.staff(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_staff_superior ON silver.staff(id_personal_superior);

CREATE TABLE IF NOT EXISTS silver.routes (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
    id_sucursal INTEGER,
    id_fuerza_ventas INTEGER,
    id_personal INTEGER,
    UNIQUE(id_ruta, id_sucursal, id_fuerza_ventas)
);
CREATE INDEX IF NOT EXISTS idx_routes_ruta ON silver.routes(id_ruta);
CREATE INDEX IF NOT EXISTS idx_routes_sucursal ON silver.routes(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_routes_fuerza ON silver.routes(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_routes_personal ON silver.routes(id_personal);

CREATE TABLE IF NOT EXISTS silver.client_forces (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_cliente INTEGER NOT NULL,
    id_ruta INTEGER NOT NULL,
    dias_visita VARCHAR(30),
    semana_visita INTEGER,
    periodicidad_visita INTEGER,
    id_modo_atencion VARCHAR(10),
    fecha_inicio DATE,
    fecha_fin DATE,
    UNIQUE(id_cliente, id_ruta, fecha_inicio)
);
CREATE INDEX IF NOT EXISTS idx_client_forces_cliente ON silver.client_forces(id_cliente);
CREATE INDEX IF NOT EXISTS idx_client_forces_ruta ON silver.client_forces(id_ruta);
CREATE INDEX IF NOT EXISTS idx_client_forces_activas ON silver.client_forces(fecha_fin) WHERE fecha_fin = '9999-12-31';

CREATE TABLE IF NOT EXISTS silver.articles (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_articulo INTEGER NOT NULL UNIQUE,
    des_articulo VARCHAR(200),
    des_corta_articulo VARCHAR(100),
    anulado BOOLEAN DEFAULT FALSE,
    fecha_alta DATE,
    es_combo BOOLEAN DEFAULT FALSE,
    es_alcoholico BOOLEAN DEFAULT FALSE,
    es_activo_fijo BOOLEAN DEFAULT FALSE,
    pesable BOOLEAN DEFAULT FALSE,
    visible_mobile BOOLEAN DEFAULT TRUE,
    tiene_retornables BOOLEAN DEFAULT FALSE,
    id_unidad_medida INTEGER,
    des_unidad_medida VARCHAR(50),
    valor_unidad_medida NUMERIC(10,4),
    unidades_bulto INTEGER,
    id_presentacion_bulto VARCHAR(10),
    des_presentacion_bulto VARCHAR(50),
    id_presentacion_unidad VARCHAR(10),
    des_presentacion_unidad VARCHAR(50),
    cod_barra_bulto VARCHAR(50),
    cod_barra_unidad VARCHAR(50),
    tasa_iva NUMERIC(8,4),
    tasa_iibb NUMERIC(8,4),
    tasa_internos NUMERIC(8,4),
    internos_bulto NUMERIC(15,4),
    exento_iva BOOLEAN DEFAULT FALSE,
    iva_diferencial BOOLEAN DEFAULT FALSE,
    peso_bulto NUMERIC(10,4),
    bultos_pallet INTEGER,
    pisos_pallet INTEGER
);
CREATE INDEX IF NOT EXISTS idx_silver_articles_id_articulo ON silver.articles(id_articulo);

CREATE TABLE IF NOT EXISTS silver.article_groupings (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_articulo INTEGER NOT NULL,
    id_forma_agrupar VARCHAR(30) NOT NULL,
    id_agrupacion VARCHAR(50),
    des_agrupacion VARCHAR(150),
    UNIQUE(id_articulo, id_forma_agrupar)
);
CREATE INDEX IF NOT EXISTS idx_article_groupings_articulo ON silver.article_groupings(id_articulo);
CREATE INDEX IF NOT EXISTS idx_article_groupings_forma ON silver.article_groupings(id_forma_agrupar);
CREATE INDEX IF NOT EXISTS idx_article_groupings_agrupacion ON silver.article_groupings(id_agrupacion);

CREATE TABLE IF NOT EXISTS silver.marketing_segments (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_segmento_mkt INTEGER NOT NULL UNIQUE,
    des_segmento_mkt VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_mkt_segments_id ON silver.marketing_segments(id_segmento_mkt);

CREATE TABLE IF NOT EXISTS silver.marketing_channels (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_canal_mkt INTEGER NOT NULL UNIQUE,
    des_canal_mkt VARCHAR(100),
    id_segmento_mkt INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mkt_channels_id ON silver.marketing_channels(id_canal_mkt);
CREATE INDEX IF NOT EXISTS idx_mkt_channels_segmento ON silver.marketing_channels(id_segmento_mkt);

CREATE TABLE IF NOT EXISTS silver.marketing_subchannels (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_subcanal_mkt INTEGER NOT NULL UNIQUE,
    des_subcanal_mkt VARCHAR(100),
    id_canal_mkt INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mkt_subchannels_id ON silver.marketing_subchannels(id_subcanal_mkt);
CREATE INDEX IF NOT EXISTS idx_mkt_subchannels_canal ON silver.marketing_subchannels(id_canal_mkt);

CREATE TABLE IF NOT EXISTS silver.fact_ventas (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    id_empresa INTEGER NOT NULL,
    id_documento VARCHAR(20) NOT NULL,
    letra CHAR(1),
    serie INTEGER,
    nro_doc INTEGER NOT NULL,
    anulado BOOLEAN DEFAULT FALSE,
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
    id_sucursal INTEGER,
    id_deposito INTEGER,
    id_caja INTEGER,
    cajero VARCHAR(100),
    id_centro_costo INTEGER,
    id_vendedor INTEGER,
    id_supervisor INTEGER,
    id_gerente INTEGER,
    id_fuerza_ventas INTEGER,
    usuario_alta VARCHAR(100),
    id_cliente INTEGER NOT NULL,
    linea_credito VARCHAR(200),
    id_canal_mkt INTEGER,
    id_segmento_mkt INTEGER,
    id_subcanal_mkt INTEGER,
    id_fletero_carga INTEGER,
    planilla_carga VARCHAR(50),
    id_articulo INTEGER NOT NULL,
    es_combo BOOLEAN DEFAULT FALSE,
    id_combo INTEGER,
    id_pedido INTEGER,
    id_origen VARCHAR(150),
    origen VARCHAR(50),
    acciones VARCHAR(100),
    cantidades_con_cargo NUMERIC(15,4),
    cantidades_sin_cargo NUMERIC(15,4),
    cantidades_total NUMERIC(15,4),
    cantidades_rechazo NUMERIC(15,4),
    precio_unitario_bruto NUMERIC(15,4),
    precio_unitario_neto NUMERIC(15,4),
    bonificacion NUMERIC(8,4),
    precio_compra_bruto NUMERIC(15,4),
    precio_compra_neto NUMERIC(15,4),
    subtotal_bruto NUMERIC(15,4),
    subtotal_bonificado NUMERIC(15,4),
    subtotal_neto NUMERIC(15,4),
    subtotal_final NUMERIC(15,4),
    facturacion_neta NUMERIC(15,4),
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
    cod_cuenta_contable VARCHAR(50),
    nro_asiento_contable INTEGER,
    nro_plan_contable INTEGER,
    id_liquidacion INTEGER,
    proveedor VARCHAR(100),
    fvig_pcompra DATE,
    id_rechazo INTEGER,
    informado BOOLEAN,
    regimen_fiscal VARCHAR(50)
);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_fecha ON silver.fact_ventas(fecha_comprobante);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_cliente ON silver.fact_ventas(id_cliente);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_articulo ON silver.fact_ventas(id_articulo);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_vendedor ON silver.fact_ventas(id_vendedor);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_sucursal ON silver.fact_ventas(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_fuerza ON silver.fact_ventas(id_fuerza_ventas);
CREATE INDEX IF NOT EXISTS idx_silver_ventas_documento ON silver.fact_ventas(id_documento, serie, nro_doc);

CREATE TABLE IF NOT EXISTS silver.fact_stock (
    id SERIAL PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_stock DATE NOT NULL,
    id_deposito INTEGER NOT NULL,
    id_almacen INTEGER,
    id_articulo INTEGER NOT NULL,
    ds_articulo VARCHAR(200),
    cant_bultos NUMERIC(15,4),
    cant_unidades NUMERIC(15,4),
    fec_vto_lote DATE
);
CREATE INDEX IF NOT EXISTS idx_silver_stock_fecha ON silver.fact_stock(date_stock);
CREATE INDEX IF NOT EXISTS idx_silver_stock_deposito ON silver.fact_stock(id_deposito);
CREATE INDEX IF NOT EXISTS idx_silver_stock_articulo ON silver.fact_stock(id_articulo);
CREATE INDEX IF NOT EXISTS idx_silver_stock_fecha_deposito ON silver.fact_stock(date_stock, id_deposito);
CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_stock_unique ON silver.fact_stock(date_stock, id_deposito, id_articulo);

-- ==========================================
-- GOLD
-- ==========================================
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

CREATE TABLE IF NOT EXISTS gold.dim_sucursal (
    id_sucursal INTEGER PRIMARY KEY,
    descripcion VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS gold.dim_vendedor (
    id_vendedor INTEGER PRIMARY KEY,
    des_vendedor VARCHAR(150),
    id_fuerza_ventas INTEGER,
    id_sucursal INTEGER,
    des_sucursal VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_dim_vendedor_sucursal ON gold.dim_vendedor(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_dim_vendedor_fuerza ON gold.dim_vendedor(id_fuerza_ventas);

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

CREATE TABLE IF NOT EXISTS gold.dim_cliente (
    id_cliente INTEGER PRIMARY KEY,
    razon_social VARCHAR(150),
    fantasia VARCHAR(150),
    id_sucursal INTEGER,
    des_sucursal VARCHAR(100),
    id_canal_mkt INTEGER,
    des_canal_mkt VARCHAR(100),
    id_segmento_mkt INTEGER,
    des_segmento_mkt VARCHAR(100),
    id_subcanal_mkt INTEGER,
    des_subcanal_mkt VARCHAR(100),
    id_ruta_fv1 INTEGER,
    des_personal_fv1 VARCHAR(150),
    id_ruta_fv4 INTEGER,
    des_personal_fv4 VARCHAR(150),
    id_ramo INTEGER,
    des_ramo VARCHAR(100),
    id_localidad INTEGER,
    des_localidad VARCHAR(100),
    id_provincia VARCHAR(10),
    des_provincia VARCHAR(100),
    latitud NUMERIC(15, 6),
    longitud NUMERIC(15, 6),
    id_lista_precio INTEGER,
    des_lista_precio VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_sucursal ON gold.dim_cliente(id_sucursal);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_canal ON gold.dim_cliente(id_canal_mkt);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_segmento ON gold.dim_cliente(id_segmento_mkt);

CREATE TABLE IF NOT EXISTS gold.fact_ventas (
    id SERIAL PRIMARY KEY,
    id_cliente INTEGER,
    id_articulo INTEGER,
    id_vendedor INTEGER,
    id_sucursal INTEGER,
    fecha_comprobante DATE,
    id_documento VARCHAR(20),
    letra CHAR(1),
    serie INTEGER,
    nro_doc INTEGER,
    anulado BOOLEAN DEFAULT FALSE,
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

CREATE TABLE IF NOT EXISTS gold.fact_stock (
    id SERIAL PRIMARY KEY,
    date_stock DATE NOT NULL,
    id_deposito INTEGER NOT NULL,
    id_articulo INTEGER NOT NULL,
    cant_bultos NUMERIC(15,4),
    cant_unidades NUMERIC(15,4)
);
CREATE INDEX IF NOT EXISTS idx_gold_stock_fecha ON gold.fact_stock(date_stock);
CREATE INDEX IF NOT EXISTS idx_gold_stock_deposito ON gold.fact_stock(id_deposito);
CREATE INDEX IF NOT EXISTS idx_gold_stock_articulo ON gold.fact_stock(id_articulo);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gold_stock_unique ON gold.fact_stock(date_stock, id_deposito, id_articulo);

CREATE TABLE IF NOT EXISTS gold.cob_preventista_marca (
    id SERIAL PRIMARY KEY,
    periodo DATE NOT NULL,
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

-- migrate:down
-- ADVERTENCIA: Esto destruye TODA la base de datos
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS bronze CASCADE;
