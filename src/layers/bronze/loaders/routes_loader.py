import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine


def load_routes():
    """Carga datos de rutas (full refresh: DELETE + INSERT)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    print("Consultando rutas desde API...")

    routes = client.get_routes(raw=True)

    if not routes:
        print("Sin datos de rutas")
        return

    print(f"Obtenidos {len(routes)} rutas")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        print("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_routes")

        print("Insertando datos nuevos...")
        data = [
            (json.dumps(route), 'API_CHESS_ERP')
            for route in routes
        ]

        query = """
            INSERT INTO bronze.raw_routes (data_raw, source_system)
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

    print(f"Insertados {len(routes)} rutas en bronze.raw_routes")


if __name__ == '__main__':
    load_routes()
