"""
Aggregator para fact_stock en Gold layer.
Copia datos esenciales desde silver.fact_stock.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def load_fact_stock(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """
    Carga fact_stock en Gold desde Silver.

    Args:
        fecha_desde: Fecha inicio (YYYY-MM-DD)
        fecha_hasta: Fecha fin (YYYY-MM-DD)
        full_refresh: Si True, elimina todo y recarga
    """
    start_time = datetime.now()
    logger.info("Cargando gold.fact_stock...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Determinar modo de carga (fechas tienen prioridad sobre full_refresh)
        if fecha_desde and fecha_hasta:
            logger.debug(f"Carga incremental: {fecha_desde} a {fecha_hasta}")
            cursor.execute(
                "DELETE FROM gold.fact_stock WHERE date_stock BETWEEN %s AND %s",
                (fecha_desde, fecha_hasta)
            )
            where_clause = "WHERE date_stock BETWEEN %s AND %s"
            params = (fecha_desde, fecha_hasta)
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos...")
            cursor.execute("DELETE FROM gold.fact_stock")
            where_clause = ""
            params = None
        else:
            logger.debug("Carga completa (sin filtro de fecha)...")
            cursor.execute("DELETE FROM gold.fact_stock")
            where_clause = ""
            params = None

        insert_query = f"""
            INSERT INTO gold.fact_stock (
                date_stock,
                id_deposito,
                id_articulo,
                cant_bultos,
                cant_unidades
            )
            SELECT
                date_stock,
                id_deposito,
                id_articulo,
                cant_bultos,
                cant_unidades
            FROM silver.fact_stock
            {where_clause}
        """

        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        throughput = inserted / total_time if total_time > 0 else 0
        logger.info(f"gold.fact_stock completado: {inserted:,} registros en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    load_fact_stock(full_refresh=True)
