import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine
from config import get_logger

logger = get_logger(__name__)

def load_clientes():
    """Carga datos de clientes (full refresh: DELETE + INSERT)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    logger.info("Consultando clientes desde API...")

    clientes = client.get_customers(raw=True)

    if not clientes:
        logger.warning("Sin datos de clientes")
        return

    logger.info(f"Obtenidos {len(clientes)} clientes")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        logger.debug("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_clients")

        logger.debug("Insertando datos nuevos...")
        data = [
            (json.dumps(cliente), 'API_CHESS_ERP')
            for cliente in clientes
        ]

        query = """
            INSERT INTO bronze.raw_clients (data_raw, source_system)
            VALUES %s
        """

        execute_values(
            cursor,
            query,
            data,
            template="(%s::jsonb, %s)"
        )

        raw_conn.commit()
        cursor.close()

    logger.info(f"Insertados {len(clientes)} clientes en bronze.raw_clients")

if __name__ == '__main__':
    load_clientes()
