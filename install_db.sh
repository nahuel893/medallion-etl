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

# Función para leer variables del .env (maneja espacios y comillas)
get_env_var() {
    local var_name=$1
    grep "^${var_name}" "$ENV_FILE" | sed "s/^${var_name}[[:space:]]*=[[:space:]]*//" | tr -d "\"'"
}

# Credenciales de PostgreSQL
DB_NAME=$(get_env_var "DATABASE")
DB_USER=$(get_env_var "POSTGRES_USER")
DB_PASSWORD=$(get_env_var "POSTGRES_PASSWORD")

# Usuarios de la Plataforma
ETL_USER=$(get_env_var "ETL_USER")
ETL_PASSWORD=$(get_env_var "ETL_PASSWORD")
READONLY_USER=$(get_env_var "READONLY_USER")
READONLY_PASSWORD=$(get_env_var "READONLY_PASSWORD")

# Ruta al archivo SQL
SQL_SCRIPT="$SCRIPT_DIR/sql/setup_medallion.sql"

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
-- Crea el usuario admin si no existe
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$DB_USER') THEN
    CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD' CREATEDB;
  END IF;
END
\$\$;

-- Crea la BD si no existe (truco para evitar error en bash)
SELECT 'CREATE DATABASE $DB_NAME OWNER $DB_USER'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$DB_NAME')\gexec
EOF

# ==========================================
# 3. EJECUCIÓN DEL MODELO MEDALLION (SQL)
# ==========================================
echo "=== 3. Aplicando Arquitectura Medallion y Permisos ==="

# Aquí ocurre la magia: Inyectamos las variables de bash hacia el archivo SQL
PGPASSWORD=$DB_PASSWORD psql -h localhost -U $DB_USER -d $DB_NAME \
    -v db_name="$DB_NAME" \
    -v etl_user="$ETL_USER" \
    -v etl_password="$ETL_PASSWORD" \
    -v readonly_user="$READONLY_USER" \
    -v readonly_password="$READONLY_PASSWORD" \
    -f $SQL_SCRIPT

echo ""
echo "✅ Instalación Exitosa."
echo "La arquitectura Medallion (Bronze, Silver, Gold) ha sido desplegada."
