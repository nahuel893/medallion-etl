"""
Transformer para dim_vendedor en Gold layer.
Desnormaliza staff con sucursal.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def load_dim_vendedor():
    """
    Carga dim_vendedor desde silver.staff con sucursal desnormalizada.
    """
    start_time = datetime.now()
    logger.info("Cargando dim_vendedor...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh
        cursor.execute("DELETE FROM gold.dim_vendedor")

        insert_query = """
            INSERT INTO gold.dim_vendedor (
                id_vendedor, des_vendedor, id_fuerza_ventas, id_sucursal, des_sucursal
            )
            SELECT
                s.id_personal,
                s.des_personal,
                s.id_fuerza_ventas,
                s.id_sucursal,
                b.descripcion
            FROM silver.staff s
            LEFT JOIN silver.branches b ON s.id_sucursal = b.id_sucursal
            WHERE s.id_personal IS NOT NULL
            ON CONFLICT (id_vendedor) DO UPDATE SET
                des_vendedor = EXCLUDED.des_vendedor,
                id_fuerza_ventas = EXCLUDED.id_fuerza_ventas,
                id_sucursal = EXCLUDED.id_sucursal,
                des_sucursal = EXCLUDED.des_sucursal
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"dim_vendedor completado: {inserted:,} registros en {total_time:.2f}s")


if __name__ == '__main__':
    load_dim_vendedor()
