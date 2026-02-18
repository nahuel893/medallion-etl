#!/bin/bash
set -e

# ==========================================
# Setup de entorno de desarrollo - Medallion ETL
# ==========================================
# Clona chesserp si no existe, crea venv e instala dependencias.
# Uso: ./setup_dev.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHESSERP_DIR="$SCRIPT_DIR/../chesserp-py-sdk"

# 1. Clonar chesserp si no existe
if [ ! -d "$CHESSERP_DIR" ]; then
    echo "=== Clonando chesserp-py-sdk ==="
    git clone git@github.com:nahuel893/chesserp-py-sdk.git "$CHESSERP_DIR"
else
    echo "=== chesserp-py-sdk ya existe en $CHESSERP_DIR ==="
fi

# 2. Crear venv si no existe
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "=== Creando entorno virtual ==="
    python3 -m venv "$SCRIPT_DIR/venv"
else
    echo "=== Entorno virtual ya existe ==="
fi

# 3. Instalar dependencias
echo "=== Instalando dependencias ==="
source "$SCRIPT_DIR/venv/bin/activate"
pip install --upgrade pip --quiet
pip install -r "$SCRIPT_DIR/requirements.txt" --quiet
pip install -e "$CHESSERP_DIR" --quiet

echo ""
echo "Entorno listo. Activar con: source venv/bin/activate"
