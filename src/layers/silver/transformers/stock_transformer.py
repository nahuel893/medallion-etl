"""
Transformer para convertir datos crudos de stock (bronze) a formato estructurado (silver).
Utiliza INSERT INTO SELECT para máxima eficiencia (todo ejecutado en PostgreSQL).
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_stock(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """
    Transforma datos de bronze.raw_stock a silver.fact_stock.

    Args:
        fecha_desde: Fecha inicial para filtrar (opcional)
        fecha_hasta: Fecha final para filtrar (opcional)
        full_refresh: Si True, elimina todos los datos de silver antes de insertar
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de stock...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Optimizaciones de PostgreSQL
        cursor.execute("SET work_mem = '512MB'")

        # Construir cláusula WHERE
        where_conditions = []
        params = []

        if fecha_desde:
            where_conditions.append("date_stock >= %s")
            params.append(fecha_desde)
        if fecha_hasta:
            where_conditions.append("date_stock <= %s")
            params.append(fecha_hasta)

        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

        # DELETE según el modo (fechas tienen prioridad sobre full_refresh)
        delete_start = datetime.now()
        if fecha_desde and fecha_hasta:
            logger.debug(f"Eliminando datos existentes en silver para el rango {fecha_desde} - {fecha_hasta}...")
            cursor.execute(
                "DELETE FROM silver.fact_stock WHERE date_stock >= %s AND date_stock <= %s",
                (fecha_desde, fecha_hasta)
            )
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos de silver.fact_stock...")
            cursor.execute("DELETE FROM silver.fact_stock")

        if full_refresh or (fecha_desde and fecha_hasta):
            deleted = cursor.rowcount
            delete_time = (datetime.now() - delete_start).total_seconds()
            logger.debug(f"DELETE completado en {delete_time:.2f}s ({deleted:,} registros)")

        # Contar registros a procesar
        count_start = datetime.now()
        count_query = f"SELECT COUNT(*) FROM bronze.raw_stock {where_clause}"
        cursor.execute(count_query, params if params else None)
        total = cursor.fetchone()[0]
        count_time = (datetime.now() - count_start).total_seconds()

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_stock")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros (COUNT en {count_time:.2f}s)")
        logger.debug("Ejecutando INSERT INTO SELECT...")

        # INSERT INTO SELECT
        insert_query = f"""
            INSERT INTO silver.fact_stock (
                date_stock,
                id_deposito,
                id_almacen,
                id_articulo,
                ds_articulo,
                cant_bultos,
                cant_unidades,
                fec_vto_lote
            )
            SELECT
                date_stock,
                id_deposito,
                NULLIF(data_raw->>'idAlmacen', '')::integer,
                NULLIF(data_raw->>'idArticulo', '')::integer,
                data_raw->>'dsArticulo',
                NULLIF(data_raw->>'cantBultos', '')::numeric(15,4),
                NULLIF(data_raw->>'cantUnidades', '')::numeric(15,4),
                NULLIF(NULLIF(data_raw->>'fecVtoLote', ''), '0001-01-01')::date
            FROM bronze.raw_stock
            {where_clause}
            ON CONFLICT (date_stock, id_deposito, id_articulo)
            DO UPDATE SET
                id_almacen = EXCLUDED.id_almacen,
                ds_articulo = EXCLUDED.ds_articulo,
                cant_bultos = EXCLUDED.cant_bultos,
                cant_unidades = EXCLUDED.cant_unidades,
                fec_vto_lote = EXCLUDED.fec_vto_lote,
                processed_at = CURRENT_TIMESTAMP
        """

        insert_start = datetime.now()
        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount
        insert_time = (datetime.now() - insert_start).total_seconds()

        logger.debug(f"INSERT completado en {insert_time:.2f}s ({inserted:,} registros)")

        commit_start = datetime.now()
        raw_conn.commit()
        commit_time = (datetime.now() - commit_start).total_seconds()
        logger.debug(f"COMMIT completado en {commit_time:.2f}s")

        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        throughput = inserted / total_time if total_time > 0 else 0
        logger.info(f"Transformación completada: {inserted:,} registros de stock en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    transform_stock()
