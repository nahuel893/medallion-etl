"""
Transformer para factores de conversión a hectolitros.
Convierte bronze.raw_hectolitros a silver.hectolitros.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_hectolitros(full_refresh: bool = True):
    """
    Transforma bronze.raw_hectolitros a silver.hectolitros.
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de hectolitros...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '256MB'")

        if full_refresh:
            logger.debug("Full refresh: eliminando datos de silver.hectolitros...")
            cursor.execute("DELETE FROM silver.hectolitros")

        cursor.execute("SELECT COUNT(*) FROM bronze.raw_hectolitros")
        total = cursor.fetchone()[0]

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_hectolitros")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros")

        insert_query = """
            INSERT INTO silver.hectolitros (id_articulo, descripcion, factor_hectolitros)
            SELECT
                rh.id_articulo,
                rh.descripcion,
                rh.factor_hectolitros
            FROM bronze.raw_hectolitros rh
            WHERE rh.id_articulo IS NOT NULL
            ON CONFLICT (id_articulo) DO UPDATE SET
                descripcion = EXCLUDED.descripcion,
                factor_hectolitros = EXCLUDED.factor_hectolitros,
                processed_at = CURRENT_TIMESTAMP
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Transformación completada: {inserted:,} hectolitros en {total_time:.2f}s")


if __name__ == '__main__':
    transform_hectolitros()
