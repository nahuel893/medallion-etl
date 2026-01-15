#!/bin/bash
# Script para cambiar las contraseñas de todos los roles de PostgreSQL
# Lee las contraseñas desde .env y las aplica a los roles existentes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: No se encontró el archivo .env en $ENV_FILE"
    exit 1
fi

# Cargar variables del .env
set -a
source "$ENV_FILE"
set +a

# Alias para claridad
DB_USER="$POSTGRES_USER"
DB_PASSWORD="$POSTGRES_PASSWORD"
DB_NAME="$DATABASE"

# Validar variables requeridas
for var in DB_USER DB_PASSWORD DB_NAME ETL_USER ETL_PASSWORD READONLY_USER READONLY_PASSWORD; do
    if [ -z "${!var}" ]; then
        echo "Error: Variable $var no está definida en .env"
        exit 1
    fi
done

echo "=== Cambiando contraseñas de roles PostgreSQL ==="
echo "  Base de datos: $DB_NAME"
echo "  Roles a actualizar:"
echo "    - $DB_USER (admin)"
echo "    - $ETL_USER (etl)"
echo "    - $READONLY_USER (readonly)"
echo ""

# Cambiar contraseñas usando el usuario postgres (superusuario)
sudo -u postgres psql <<EOF
-- Cambiar contraseña del usuario admin
ALTER ROLE $DB_USER WITH PASSWORD '$DB_PASSWORD';
\echo '  ✓ Contraseña de $DB_USER actualizada'

-- Cambiar contraseña del usuario ETL
ALTER ROLE $ETL_USER WITH PASSWORD '$ETL_PASSWORD';
\echo '  ✓ Contraseña de $ETL_USER actualizada'

-- Cambiar contraseña del usuario readonly
ALTER ROLE $READONLY_USER WITH PASSWORD '$READONLY_PASSWORD';
\echo '  ✓ Contraseña de $READONLY_USER actualizada'
EOF

echo ""
echo "=== Contraseñas actualizadas exitosamente ==="
