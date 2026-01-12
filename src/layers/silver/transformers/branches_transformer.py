"""
Transformer para sucursales (branches).
Extrae IDs únicos de sucursales desde bronze.raw_staff.
Las descripciones se cargan manualmente después.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_branches(full_refresh: bool = True):
    """
    Extrae sucursales únicas desde bronze.raw_staff a silver.branches.
    Solo carga los IDs - las descripciones se cargan manualmente.

    Args:
        full_refresh: Si True (default), elimina todos los datos antes de insertar
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de branches...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '256MB'")

        if full_refresh:
            delete_start = datetime.now()
            logger.debug("Full refresh: eliminando datos de silver.branches...")
            cursor.execute("DELETE FROM silver.branches")
            delete_time = (datetime.now() - delete_start).total_seconds()
            logger.debug(f"DELETE completado en {delete_time:.2f}s")

        logger.debug("Ejecutando INSERT INTO SELECT...")

        # Extraer sucursales únicas desde staff
        insert_query = """
            INSERT INTO silver.branches (id_sucursal, descripcion)
            SELECT DISTINCT
                NULLIF(data_raw->>'idSucursal', '')::integer,
                data_raw->>'desSucursal'
            FROM bronze.raw_staff
            WHERE NULLIF(data_raw->>'idSucursal', '') IS NOT NULL
            ON CONFLICT (id_sucursal) DO NOTHING
        """

        insert_start = datetime.now()
        cursor.execute(insert_query)
        inserted = cursor.rowcount
        insert_time = (datetime.now() - insert_start).total_seconds()

        logger.debug(f"INSERT completado en {insert_time:.2f}s ({inserted:,} registros)")

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Transformación completada: {inserted:,} branches en {total_time:.2f}s")


if __name__ == '__main__':
    transform_branches()
