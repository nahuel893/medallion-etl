"""
Transformer para depósitos.
Convierte bronze.raw_deposits a silver.deposits, parseando id_sucursal del campo sucursal.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_deposits(full_refresh: bool = True):
    """
    Transforma bronze.raw_deposits a silver.deposits.
    Parsea el campo sucursal ("1 - CASA CENTRAL") en id_sucursal + des_sucursal.
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de deposits...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '256MB'")

        if full_refresh:
            logger.debug("Full refresh: eliminando datos de silver.deposits...")
            cursor.execute("DELETE FROM silver.deposits")

        cursor.execute("SELECT COUNT(*) FROM bronze.raw_deposits")
        total = cursor.fetchone()[0]

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_deposits")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros")

        insert_query = """
            INSERT INTO silver.deposits (id_deposito, descripcion, id_sucursal, des_sucursal)
            SELECT DISTINCT
                rd.id_deposito,
                rd.descripcion,
                SPLIT_PART(rd.sucursal, ' - ', 1)::integer AS id_sucursal,
                SPLIT_PART(rd.sucursal, ' - ', 2) AS des_sucursal
            FROM bronze.raw_deposits rd
            WHERE rd.id_deposito IS NOT NULL
            ON CONFLICT (id_deposito) DO UPDATE SET
                descripcion = EXCLUDED.descripcion,
                id_sucursal = EXCLUDED.id_sucursal,
                des_sucursal = EXCLUDED.des_sucursal,
                processed_at = CURRENT_TIMESTAMP
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Transformación completada: {inserted:,} deposits en {total_time:.2f}s")


if __name__ == '__main__':
    transform_deposits()
