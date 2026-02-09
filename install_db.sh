#!/bin/bash
set -e
# ==========================================
# CONFIGURACIÓN (Carga desde .env)
# ==========================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: No se encontró el archivo .env en $ENV_FILE"
    exit 1
fi

# Cargar variables del .env directamente
set -a
source "$ENV_FILE"
set +a

# Alias para claridad
DB_NAME="$DATABASE"
DB_USER="$POSTGRES_USER"
DB_PASSWORD="$POSTGRES_PASSWORD"

# Validar variables requeridas
for var in DB_NAME DB_USER DB_PASSWORD ETL_USER ETL_PASSWORD READONLY_USER READONLY_PASSWORD; do
    if [ -z "${!var}" ]; then
        echo "Error: Variable $var no está definida en .env"
        exit 1
    fi
done

echo "Variables cargadas correctamente:"
echo "  DATABASE: $DB_NAME"
echo "  ETL_USER: $ETL_USER"
echo "  READONLY_USER: $READONLY_USER"

# Ruta al archivo de permisos
SQL_PERMISSIONS="$SCRIPT_DIR/sql/permissions.sql"

# ==========================================
# 1. INSTALACIÓN DE SISTEMA
# ==========================================
echo "=== 1. Instalando PostgreSQL ==="
sudo apt update
sudo apt install -y postgresql postgresql-contrib

echo "=== Iniciando servicio ==="
sudo systemctl start postgresql
sudo systemctl enable postgresql

# ==========================================
# 2. BOOTSTRAP (Creación de BD y Admin)
# ==========================================
# Esto se mantiene en bash porque necesitamos ser 'postgres' para crear la BD inicial
echo "=== 2. Creando Base de Datos y Admin ==="
sudo -u postgres psql <<EOF
-- Crea el usuario admin si no existe (con permisos para crear roles y DBs)
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$DB_USER') THEN
    CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD' CREATEDB CREATEROLE;
  ELSE
    -- Si ya existe, asegurarse de que tenga CREATEROLE
    ALTER USER $DB_USER WITH CREATEROLE;
  END IF;
END
\$\$;

-- Crea la BD si no existe (truco para evitar error en bash)
SELECT 'CREATE DATABASE $DB_NAME OWNER $DB_USER'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')\gexec
EOF

# ==========================================
# 3. ESTRUCTURA DE LA BD (dbmate)
# ==========================================
echo "=== 3. Creando estructura de la BD (dbmate) ==="
cd "$SCRIPT_DIR"
PGPASSWORD="$DB_PASSWORD" DATABASE_URL="postgres://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME?sslmode=disable" \
    npx dbmate --migrations-dir "./sql/migrations" --no-dump-schema up

# ==========================================
# 4. ROLES Y PERMISOS
# ==========================================
echo "=== 4. Aplicando roles y permisos ==="
echo "  etl_user=$ETL_USER"
echo "  readonly_user=$READONLY_USER"

PGPASSWORD="$DB_PASSWORD" psql -h localhost -U "$DB_USER" -d "$DB_NAME" \
    -v db_name="$DB_NAME" \
    -v etl_user="$ETL_USER" \
    -v etl_password="$ETL_PASSWORD" \
    -v readonly_user="$READONLY_USER" \
    -v readonly_password="$READONLY_PASSWORD" \
    -f "$SQL_PERMISSIONS"

echo ""
echo "Instalacion completada."
echo "Arquitectura Medallion (Bronze, Silver, Gold) desplegada con dbmate + permisos."
