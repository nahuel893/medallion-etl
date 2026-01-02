import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine

def load_staff():
    """Carga datos de staff (full refresh: DELETE + INSERT)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    print("Consultando staff desde API...")

    staff = client.get_staff(raw=True)

    if not staff:
        print("Sin datos de staff")
        return

    print(f"Obtenidos {len(staff)} staffs")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        print("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_staff")

        print("Insertando datos nuevos...")
        data = [
            (json.dumps(record), 'API_CHESS_ERP')
            for record in staff
        ]

        query = """
            INSERT INTO bronze.raw_staff(data_raw, source_system)
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

    print(f"Insertados {len(staff)} staff en bronze.raw_staff")

if __name__ == '__main__':
    load_staff()

