-- Archivo: sql/permissions.sql
-- Crea roles y asigna permisos para la arquitectura Medallion.
--
-- Requiere que los schemas (bronze, silver, gold) y tablas ya existan (creados por dbmate).
--
-- Ejecutar con:
--   psql -U $USER -d $DB -v db_name=X -v etl_user=X -v etl_password=X -v readonly_user=X -v readonly_password=X -f sql/permissions.sql

-- ==========================================
-- 1. CREACION DE ROLES
-- ==========================================

SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'etl_user', :'etl_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'etl_user')
\gexec

SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'readonly_user', :'readonly_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'readonly_user')
\gexec

-- ==========================================
-- 2. PERMISOS DE CONEXION
-- ==========================================

GRANT CONNECT ON DATABASE :db_name TO :etl_user;
GRANT CONNECT ON DATABASE :db_name TO :readonly_user;

-- ==========================================
-- 3. ETL USER - Acceso completo a bronze/silver/gold
-- ==========================================

-- Bronze
GRANT USAGE, CREATE ON SCHEMA bronze TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL PRIVILEGES ON TABLES TO :etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO :etl_user;

-- Silver
GRANT USAGE, CREATE ON SCHEMA silver TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL PRIVILEGES ON TABLES TO :etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO :etl_user;

-- Gold
GRANT USAGE, CREATE ON SCHEMA gold TO :etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO :etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL PRIVILEGES ON TABLES TO :etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold TO :etl_user;

-- ==========================================
-- 4. READONLY USER - Solo lectura en gold
-- ==========================================

GRANT USAGE ON SCHEMA gold TO :readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO :readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO :readonly_user;
REVOKE ALL ON SCHEMA public FROM :readonly_user;

-- ==========================================
-- 5. USUARIO NAHUEL (admin/owner de la BD)
-- ==========================================

GRANT USAGE, CREATE ON SCHEMA bronze TO nahuel;
GRANT USAGE, CREATE ON SCHEMA silver TO nahuel;
GRANT USAGE, CREATE ON SCHEMA gold TO nahuel;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO nahuel;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO nahuel;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO nahuel;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO nahuel;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO nahuel;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold TO nahuel;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL PRIVILEGES ON TABLES TO nahuel;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL PRIVILEGES ON TABLES TO nahuel;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL PRIVILEGES ON TABLES TO nahuel;
