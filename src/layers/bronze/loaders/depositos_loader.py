import csv
from pathlib import Path

from psycopg2.extras import execute_values
from database import engine
from config import get_logger

logger = get_logger(__name__)


# Ruta al archivo de depósitos
DEPOSITS_FILE = Path(__file__).parent.parent.parent.parent.parent / 'data' / 'deposits_b.csv'


def load_depositos():
    """Carga datos de depósitos desde CSV (full refresh: DELETE + INSERT)."""

    logger.info(f"Leyendo depósitos desde: {DEPOSITS_FILE}")

    depositos = []
    with open(DEPOSITS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            depositos.append((
                int(row['idDeposito']),
                row['descripcion_deposito'],
                row['Sucursal'],
                'CSV_DEPOSITS'
            ))

    if not depositos:
        logger.warning("Sin datos de depósitos")
        return

    logger.info(f"Obtenidos {len(depositos)} depósitos")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        logger.debug("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_deposits")

        logger.debug("Insertando datos nuevos...")
        query = """
            INSERT INTO bronze.raw_deposits (id_deposito, descripcion, sucursal, source_system)
            VALUES %s
        """

        execute_values(
            cursor,
            query,
            depositos,
            template="(%s, %s, %s, %s)"
        )

        raw_conn.commit()
        cursor.close()

    logger.info(f"Insertados {len(depositos)} depósitos en bronze.raw_deposits")


if __name__ == '__main__':
    load_depositos()
