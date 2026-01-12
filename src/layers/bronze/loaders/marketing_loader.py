import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine
from config import get_logger

logger = get_logger(__name__)


def load_marketing():
    """Carga datos de marketing - segmentos, canales y subcanales (full refresh: DELETE + INSERT)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    logger.info("Consultando marketing desde API...")

    marketing = client.get_marketing(raw=True)

    if not marketing:
        logger.warning("Sin datos de marketing")
        return

    logger.info(f"Obtenidos {len(marketing)} registros de marketing")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        logger.debug("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_marketing")

        logger.debug("Insertando datos nuevos...")
        data = [
            (json.dumps(record), 'API_CHESS_ERP')
            for record in marketing
        ]

        query = """
            INSERT INTO bronze.raw_marketing (data_raw, source_system)
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

    logger.info(f"Insertados {len(marketing)} registros en bronze.raw_marketing")


if __name__ == '__main__':
    load_marketing()
