"""
Aggregator para dim_deposito en Gold layer.
Carga datos desde silver.deposits.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def load_dim_deposito():
    """
    Carga dim_deposito desde silver.deposits.
    """
    start_time = datetime.now()
    logger.info("Cargando dim_deposito...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh
        cursor.execute("DELETE FROM gold.dim_deposito")

        insert_query = """
            INSERT INTO gold.dim_deposito (id_deposito, descripcion, id_sucursal, des_sucursal)
            SELECT
                d.id_deposito,
                d.descripcion,
                d.id_sucursal,
                d.des_sucursal
            FROM silver.deposits d
            WHERE d.id_deposito IS NOT NULL
            ON CONFLICT (id_deposito) DO UPDATE SET
                descripcion = EXCLUDED.descripcion,
                id_sucursal = EXCLUDED.id_sucursal,
                des_sucursal = EXCLUDED.des_sucursal
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"dim_deposito completado: {inserted:,} registros en {total_time:.2f}s")


if __name__ == '__main__':
    load_dim_deposito()
