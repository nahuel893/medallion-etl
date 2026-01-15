"""
Configuración y fixtures compartidos para tests.
"""
import sys
from pathlib import Path
import pytest

# Agregar src/ al path para imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def sample_sales_data():
    """Datos de ejemplo para tests de ventas."""
    return [
        {
            "idEmpresa": "1",
            "idDocumento": "FAC",
            "letra": "A",
            "serie": "1",
            "nrodoc": "12345",
            "fechaComprobate": "2025-01-15",
            "idCliente": "100",
            "idArticulo": "500",
            "cantidadesTotal": "10.00",
            "subtotalFinal": "1500.00"
        },
        {
            "idEmpresa": "1",
            "idDocumento": "FAC",
            "letra": "A",
            "serie": "1",
            "nrodoc": "12346",
            "fechaComprobate": "2025-01-16",
            "idCliente": "101",
            "idArticulo": "501",
            "cantidadesTotal": "5.00",
            "subtotalFinal": "750.00"
        }
    ]


@pytest.fixture
def sample_client_data():
    """Datos de ejemplo para tests de clientes."""
    return {
        "idCliente": "100",
        "razonSocial": "Cliente Test SA",
        "fantasia": "Test",
        "idSucursal": "1",
        "latitud": "-34.603722",
        "longitud": "-58.381592"
    }
