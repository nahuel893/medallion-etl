import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine


def load_marketing():
    """Carga datos de marketing - segmentos, canales y subcanales (full refresh: DELETE + INSERT)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    print("Consultando marketing desde API...")

    marketing = client.get_marketing(raw=True)

    if not marketing:
        print("Sin datos de marketing")
        return

    print(f"Obtenidos {len(marketing)} registros de marketing")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        print("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_marketing")

        print("Insertando datos nuevos...")
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

    print(f"Insertados {len(marketing)} registros en bronze.raw_marketing")


if __name__ == '__main__':
    load_marketing()
