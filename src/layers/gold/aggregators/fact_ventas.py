"""
Transformer para fact_ventas en Gold layer.
Copia datos esenciales desde silver.fact_ventas.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def load_fact_ventas(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """
    Carga fact_ventas en Gold desde Silver.

    Args:
        fecha_desde: Fecha inicio (YYYY-MM-DD)
        fecha_hasta: Fecha fin (YYYY-MM-DD)
        full_refresh: Si True, elimina todo y recarga
    """
    start_time = datetime.now()
    logger.info("Cargando gold.fact_ventas...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '512MB'")

        # Determinar modo de carga (fechas tienen prioridad sobre full_refresh)
        if fecha_desde and fecha_hasta:
            logger.debug(f"Carga incremental: {fecha_desde} a {fecha_hasta}")
            cursor.execute(
                "DELETE FROM gold.fact_ventas WHERE fecha_comprobante BETWEEN %s AND %s",
                (fecha_desde, fecha_hasta)
            )
            where_clause = "WHERE fecha_comprobante BETWEEN %s AND %s"
            params = (fecha_desde, fecha_hasta)
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos...")
            cursor.execute("DELETE FROM gold.fact_ventas")
            where_clause = ""
            params = None
        else:
            logger.debug("Carga completa (sin filtro de fecha)...")
            cursor.execute("DELETE FROM gold.fact_ventas")
            where_clause = ""
            params = None

        insert_query = f"""
            INSERT INTO gold.fact_ventas (
                id_cliente, id_articulo, id_vendedor, id_sucursal, fecha_comprobante,
                id_documento, letra, serie, nro_doc, anulado,
                cantidades_con_cargo, cantidades_sin_cargo, cantidades_total,
                subtotal_neto, subtotal_final, bonificacion,
                cantidad_total_htls
            )
            SELECT
                fv.id_cliente,
                fv.id_articulo,
                fv.id_vendedor,
                fv.id_sucursal,
                fv.fecha_comprobante,
                fv.id_documento,
                fv.letra,
                fv.serie,
                fv.nro_doc,
                fv.anulado,
                fv.cantidades_con_cargo,
                fv.cantidades_sin_cargo,
                fv.cantidades_total,
                fv.subtotal_neto,
                fv.subtotal_final,
                fv.bonificacion,
                fv.cantidades_total * h.factor_hectolitros AS cantidad_total_htls
            FROM silver.fact_ventas fv
            LEFT JOIN silver.hectolitros h ON fv.id_articulo = h.id_articulo
            {where_clause}
        """

        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        throughput = inserted / total_time if total_time > 0 else 0
        logger.info(f"gold.fact_ventas completado: {inserted:,} registros en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    load_fact_ventas(full_refresh=True)
