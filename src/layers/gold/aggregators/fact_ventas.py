"""
Transformer para fact_ventas en Gold layer.
Copia datos esenciales desde silver.fact_ventas.
"""
from database import engine
from datetime import datetime


def load_fact_ventas(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """
    Carga fact_ventas en Gold desde Silver.

    Args:
        fecha_desde: Fecha inicio (YYYY-MM-DD)
        fecha_hasta: Fecha fin (YYYY-MM-DD)
        full_refresh: Si True, elimina todo y recarga
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Cargando gold.fact_ventas...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '512MB'")

        # Determinar modo de carga
        if full_refresh:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Full refresh: eliminando todos los datos...")
            cursor.execute("DELETE FROM gold.fact_ventas")
            where_clause = ""
            params = None
        elif fecha_desde and fecha_hasta:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga incremental: {fecha_desde} a {fecha_hasta}")
            cursor.execute(
                "DELETE FROM gold.fact_ventas WHERE fecha_comprobante BETWEEN %s AND %s",
                (fecha_desde, fecha_hasta)
            )
            where_clause = "WHERE fecha_comprobante BETWEEN %s AND %s"
            params = (fecha_desde, fecha_hasta)
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Carga completa (sin filtro de fecha)...")
            cursor.execute("DELETE FROM gold.fact_ventas")
            where_clause = ""
            params = None

        insert_query = f"""
            INSERT INTO gold.fact_ventas (
                id_cliente, id_articulo, id_vendedor, id_sucursal, fecha_comprobante,
                id_documento, letra, serie, nro_doc, anulado,
                cantidades_con_cargo, cantidades_sin_cargo, cantidades_total,
                subtotal_neto, subtotal_final, bonificacion
            )
            SELECT
                id_cliente,
                id_articulo,
                id_vendedor,
                id_sucursal,
                fecha_comprobante,
                id_documento,
                letra,
                serie,
                nro_doc,
                anulado,
                cantidades_con_cargo,
                cantidades_sin_cargo,
                cantidades_total,
                subtotal_neto,
                subtotal_final,
                bonificacion
            FROM silver.fact_ventas
            {where_clause}
        """

        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        throughput = inserted / total_time if total_time > 0 else 0
        print(f"    ✓ gold.fact_ventas completado: {inserted:,} registros en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    load_fact_ventas(full_refresh=True)
