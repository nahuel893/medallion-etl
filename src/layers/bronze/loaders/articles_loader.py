import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine


def load_articles():
    """Carga datos de artículos (full refresh: DELETE + INSERT)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    print("Consultando artículos desde API...")

    articles = client.get_articles(raw=True)

    if not articles:
        print("Sin datos de artículos")
        return

    print(f"Obtenidos {len(articles)} artículos")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        print("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_articles")

        print("Insertando datos nuevos...")
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

    print(f"Insertados {len(articles)} artículos en bronze.raw_articles")


if __name__ == '__main__':
    load_articles()
