"""
Transformer para dim_sucursal en Gold layer.
Copia datos desde silver.branches.
"""
from database import engine
from datetime import datetime


def load_dim_sucursal():
    """
    Carga dim_sucursal desde silver.branches.
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Cargando dim_sucursal...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh
        cursor.execute("DELETE FROM gold.dim_sucursal")

        insert_query = """
            INSERT INTO gold.dim_sucursal (id_sucursal, descripcion)
            SELECT id_sucursal, descripcion
            FROM silver.branches
            WHERE id_sucursal IS NOT NULL
            ON CONFLICT (id_sucursal) DO UPDATE SET
                descripcion = EXCLUDED.descripcion
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        print(f"    ✓ dim_sucursal completado: {inserted:,} registros en {total_time:.2f}s")


if __name__ == '__main__':
    load_dim_sucursal()
