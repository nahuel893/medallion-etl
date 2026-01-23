import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine
from config import get_logger

logger = get_logger(__name__)


def load_articles():
    """Carga datos de artículos (full refresh: DELETE + INSERT)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    logger.info("Consultando artículos desde API...")

    articles = client.get_articles(raw=True, anulado=True)

    if not articles:
        logger.warning("Sin datos de artículos")
        return

    logger.info(f"Obtenidos {len(articles)} artículos")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        logger.debug("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_articles")

        logger.debug("Insertando datos nuevos...")
        data = [
            (json.dumps(article), 'API_CHESS_ERP')
            for article in articles
        ]

        query = """
            INSERT INTO bronze.raw_articles (data_raw, source_system)
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

    logger.info(f"Insertados {len(articles)} artículos en bronze.raw_articles")


if __name__ == '__main__':
    load_articles()
