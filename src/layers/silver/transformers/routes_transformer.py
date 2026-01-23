"""
Transformer para rutas (routes).
Convierte bronze.raw_routes a silver.routes.
Ignora el array eClientesRutas (esa relación está en client_forces).
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_routes(full_refresh: bool = True):
    """
    Transforma bronze.raw_routes a silver.routes.

    Args:
        full_refresh: Si True (default), elimina todos los datos antes de insertar
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de routes...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '512MB'")

        if full_refresh:
            delete_start = datetime.now()
            logger.debug("Full refresh: eliminando datos de silver.routes...")
            cursor.execute("DELETE FROM silver.routes")
            delete_time = (datetime.now() - delete_start).total_seconds()
            logger.debug(f"DELETE completado en {delete_time:.2f}s")

        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM bronze.raw_routes")
        total = cursor.fetchone()[0]

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_routes")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros")
        logger.debug("Ejecutando INSERT INTO SELECT...")

        insert_query = """
            INSERT INTO silver.routes (
                id_ruta,
                des_ruta,
                dias_visita,
                semana_visita,
                periodicidad_visita,
                dias_entrega,
                semana_entrega,
                periodicidad_entrega,
                id_modo_atencion,
                des_modo_atencion,
                fecha_desde,
                fecha_hasta,
                anulado,
                id_sucursal,
                id_fuerza_ventas,
                id_personal
            )
            SELECT
                NULLIF(data_raw->>'idRuta', '')::integer,
                data_raw->>'desRuta',
                data_raw->>'diasVisita',
                NULLIF(data_raw->>'semanaVisita', '')::integer,
                NULLIF(data_raw->>'periodicidadVisita', '')::integer,
                data_raw->>'diasEntrega',
                NULLIF(data_raw->>'semanaEntrega', '')::integer,
                NULLIF(data_raw->>'periodicidadEntrega', '')::integer,
                data_raw->>'idModoAtencion',
                data_raw->>'desModoAtencion',
                NULLIF(data_raw->>'fechaDesde', '')::date,
                NULLIF(data_raw->>'fechaHasta', '')::date,
                COALESCE((data_raw->>'anulado')::boolean, false),
                NULLIF(data_raw->>'idSucursal', '')::integer,
                NULLIF(data_raw->>'idFuerzaVentas', '')::integer,
                NULLIF(data_raw->>'idPersonal', '')::integer
            FROM bronze.raw_routes
            WHERE NULLIF(data_raw->>'idRuta', '') IS NOT NULL
            ON CONFLICT (id_ruta, fecha_desde) DO UPDATE SET
                des_ruta = EXCLUDED.des_ruta,
                dias_visita = EXCLUDED.dias_visita,
                semana_visita = EXCLUDED.semana_visita,
                periodicidad_visita = EXCLUDED.periodicidad_visita,
                dias_entrega = EXCLUDED.dias_entrega,
                semana_entrega = EXCLUDED.semana_entrega,
                periodicidad_entrega = EXCLUDED.periodicidad_entrega,
                id_modo_atencion = EXCLUDED.id_modo_atencion,
                des_modo_atencion = EXCLUDED.des_modo_atencion,
                fecha_hasta = EXCLUDED.fecha_hasta,
                anulado = EXCLUDED.anulado,
                id_sucursal = EXCLUDED.id_sucursal,
                id_fuerza_ventas = EXCLUDED.id_fuerza_ventas,
                id_personal = EXCLUDED.id_personal,
                processed_at = CURRENT_TIMESTAMP
        """

        insert_start = datetime.now()
        cursor.execute(insert_query)
        inserted = cursor.rowcount
        insert_time = (datetime.now() - insert_start).total_seconds()

        logger.debug(f"INSERT completado en {insert_time:.2f}s ({inserted:,} registros)")

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        throughput = inserted / total_time if total_time > 0 else 0
        logger.info(f"Transformación completada: {inserted:,} routes en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    transform_routes()
