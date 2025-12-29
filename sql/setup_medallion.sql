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
    data_raw JSONB  -- <--- LA JOYA DE LA CORONA
);


-- Capa SILVER
GRANT USAGE, CREATE ON SCHEMA silver TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL PRIVILEGES ON TABLES TO :etl_user;

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

