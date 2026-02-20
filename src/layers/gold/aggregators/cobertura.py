"""
Aggregator para tablas de cobertura en Gold layer.
Calcula cobertura (clientes compradores) y volumen por distintas aperturas.
Separa por fuerza de venta para evitar mezclar datos.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def load_cob_preventista_marca(periodo: str = '', full_refresh: bool = False):
    """
    Carga cobertura por Fuerza de Venta/Preventista/Ruta/Marca.
    Usa la ruta correspondiente a cada fuerza de venta (id_ruta_fv1 para FV1, id_ruta_fv4 para FV4).

    Args:
        periodo: Mes en formato 'YYYY-MM' (ej: '2025-01'). Si vacío, procesa todo.
        full_refresh: Si True, elimina todo y recarga
    """
    start_time = datetime.now()
    logger.info("Cargando gold.cob_preventista_marca...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Determinar modo de carga
        if periodo:
            periodo_date = f"{periodo}-01"
            logger.debug(f"Carga incremental: periodo {periodo}")
            cursor.execute(
                "DELETE FROM gold.cob_preventista_marca WHERE periodo = %s::date",
                (periodo_date,)
            )
            where_clause = "WHERE DATE_TRUNC('month', fv.fecha_comprobante) = %s::date"
            params = (periodo_date,)
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos...")
            cursor.execute("DELETE FROM gold.cob_preventista_marca")
            where_clause = ""
            params = None
        else:
            logger.debug("Carga completa...")
            cursor.execute("DELETE FROM gold.cob_preventista_marca")
            where_clause = ""
            params = None

        insert_query = f"""
            WITH cliente_marca AS (
                SELECT
                    DATE_TRUNC('month', fv.fecha_comprobante)::date AS periodo,
                    dv.id_fuerza_ventas,
                    fv.id_vendedor,
                    CASE
                        WHEN dv.id_fuerza_ventas = 1 THEN dc.id_ruta_fv1
                        WHEN dv.id_fuerza_ventas = 4 THEN dc.id_ruta_fv4
                        ELSE NULL
                    END AS id_ruta,
                    fv.id_sucursal,
                    ds.descripcion AS ds_sucursal,
                    da.marca,
                    fv.id_cliente,
                    SUM(fv.cantidades_total) AS total_qty
                FROM gold.fact_ventas fv
                JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor
                    AND fv.id_sucursal = dv.id_sucursal
                LEFT JOIN gold.dim_sucursal ds ON fv.id_sucursal = ds.id_sucursal
                LEFT JOIN gold.dim_articulo da ON fv.id_articulo = da.id_articulo
                LEFT JOIN gold.dim_cliente dc ON fv.id_cliente = dc.id_cliente
                    AND fv.id_sucursal = dc.id_sucursal
                {where_clause}
                {"AND" if where_clause else "WHERE"} dv.id_fuerza_ventas IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5, 6, 7, fv.id_cliente
                HAVING SUM(fv.cantidades_total) > 0
            )
            INSERT INTO gold.cob_preventista_marca (
                periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, ds_sucursal, marca,
                clientes_compradores, volumen_total
            )
            SELECT
                periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, ds_sucursal, marca,
                COUNT(DISTINCT id_cliente) AS clientes_compradores,
                SUM(total_qty) AS volumen_total
            FROM cliente_marca
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            ON CONFLICT (periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, marca)
            DO UPDATE SET
                ds_sucursal = EXCLUDED.ds_sucursal,
                clientes_compradores = EXCLUDED.clientes_compradores,
                volumen_total = EXCLUDED.volumen_total
        """

        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"cob_preventista_marca completado: {inserted:,} registros en {total_time:.2f}s")


def load_cob_sucursal_marca(periodo: str = '', full_refresh: bool = False):
    """
    Carga cobertura por Fuerza de Venta/Sucursal/Marca.
    """
    start_time = datetime.now()
    logger.info("Cargando gold.cob_sucursal_marca...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        if periodo:
            periodo_date = f"{periodo}-01"
            logger.debug(f"Carga incremental: periodo {periodo}")
            cursor.execute(
                "DELETE FROM gold.cob_sucursal_marca WHERE periodo = %s::date",
                (periodo_date,)
            )
            where_clause = "WHERE DATE_TRUNC('month', fv.fecha_comprobante) = %s::date"
            params = (periodo_date,)
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos...")
            cursor.execute("DELETE FROM gold.cob_sucursal_marca")
            where_clause = ""
            params = None
        else:
            logger.debug("Carga completa...")
            cursor.execute("DELETE FROM gold.cob_sucursal_marca")
            where_clause = ""
            params = None

        insert_query = f"""
            WITH cliente_marca AS (
                SELECT
                    DATE_TRUNC('month', fv.fecha_comprobante)::date AS periodo,
                    dv.id_fuerza_ventas,
                    fv.id_sucursal,
                    ds.descripcion AS ds_sucursal,
                    da.marca,
                    fv.id_cliente,
                    SUM(fv.cantidades_total) AS total_qty
                FROM gold.fact_ventas fv
                JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor
                    AND fv.id_sucursal = dv.id_sucursal
                LEFT JOIN gold.dim_sucursal ds ON fv.id_sucursal = ds.id_sucursal
                LEFT JOIN gold.dim_articulo da ON fv.id_articulo = da.id_articulo
                {where_clause}
                {"AND" if where_clause else "WHERE"} dv.id_fuerza_ventas IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5, fv.id_cliente
                HAVING SUM(fv.cantidades_total) > 0
            )
            INSERT INTO gold.cob_sucursal_marca (
                periodo, id_fuerza_ventas, id_sucursal, ds_sucursal, marca,
                clientes_compradores, volumen_total
            )
            SELECT
                periodo, id_fuerza_ventas, id_sucursal, ds_sucursal, marca,
                COUNT(DISTINCT id_cliente) AS clientes_compradores,
                SUM(total_qty) AS volumen_total
            FROM cliente_marca
            GROUP BY 1, 2, 3, 4, 5
            ON CONFLICT (periodo, id_fuerza_ventas, id_sucursal, marca)
            DO UPDATE SET
                ds_sucursal = EXCLUDED.ds_sucursal,
                clientes_compradores = EXCLUDED.clientes_compradores,
                volumen_total = EXCLUDED.volumen_total
        """

        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"cob_sucursal_marca completado: {inserted:,} registros en {total_time:.2f}s")


def load_cob_preventista_generico(periodo: str = '', full_refresh: bool = False):
    """
    Carga cobertura por Fuerza de Venta/Preventista/Ruta/Genérico.
    Usa la ruta correspondiente a cada fuerza de venta.
    """
    start_time = datetime.now()
    logger.info("Cargando gold.cob_preventista_generico...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        if periodo:
            periodo_date = f"{periodo}-01"
            logger.debug(f"Carga incremental: periodo {periodo}")
            cursor.execute(
                "DELETE FROM gold.cob_preventista_generico WHERE periodo = %s::date",
                (periodo_date,)
            )
            where_clause = "WHERE DATE_TRUNC('month', fv.fecha_comprobante) = %s::date"
            params = (periodo_date,)
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos...")
            cursor.execute("DELETE FROM gold.cob_preventista_generico")
            where_clause = ""
            params = None
        else:
            logger.debug("Carga completa...")
            cursor.execute("DELETE FROM gold.cob_preventista_generico")
            where_clause = ""
            params = None

        insert_query = f"""
            WITH cliente_generico AS (
                SELECT
                    DATE_TRUNC('month', fv.fecha_comprobante)::date AS periodo,
                    dv.id_fuerza_ventas,
                    fv.id_vendedor,
                    CASE
                        WHEN dv.id_fuerza_ventas = 1 THEN dc.id_ruta_fv1
                        WHEN dv.id_fuerza_ventas = 4 THEN dc.id_ruta_fv4
                        ELSE NULL
                    END AS id_ruta,
                    fv.id_sucursal,
                    ds.descripcion AS ds_sucursal,
                    da.generico,
                    fv.id_cliente,
                    SUM(fv.cantidades_total) AS total_qty
                FROM gold.fact_ventas fv
                JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor
                    AND fv.id_sucursal = dv.id_sucursal
                LEFT JOIN gold.dim_sucursal ds ON fv.id_sucursal = ds.id_sucursal
                LEFT JOIN gold.dim_articulo da ON fv.id_articulo = da.id_articulo
                LEFT JOIN gold.dim_cliente dc ON fv.id_cliente = dc.id_cliente
                    AND fv.id_sucursal = dc.id_sucursal
                {where_clause}
                {"AND" if where_clause else "WHERE"} dv.id_fuerza_ventas IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5, 6, 7, fv.id_cliente
                HAVING SUM(fv.cantidades_total) > 0
            )
            INSERT INTO gold.cob_preventista_generico (
                periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, ds_sucursal, generico,
                clientes_compradores, volumen_total
            )
            SELECT
                periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, ds_sucursal, generico,
                COUNT(DISTINCT id_cliente) AS clientes_compradores,
                SUM(total_qty) AS volumen_total
            FROM cliente_generico
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            ON CONFLICT (periodo, id_fuerza_ventas, id_vendedor, id_ruta, id_sucursal, generico)
            DO UPDATE SET
                ds_sucursal = EXCLUDED.ds_sucursal,
                clientes_compradores = EXCLUDED.clientes_compradores,
                volumen_total = EXCLUDED.volumen_total
        """

        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"cob_preventista_generico completado: {inserted:,} registros en {total_time:.2f}s")


def load_cob_sucursal_generico(periodo: str = '', full_refresh: bool = False):
    """
    Carga cobertura por Fuerza de Venta/Sucursal/Genérico.
    """
    start_time = datetime.now()
    logger.info("Cargando gold.cob_sucursal_generico...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        if periodo:
            periodo_date = f"{periodo}-01"
            logger.debug(f"Carga incremental: periodo {periodo}")
            cursor.execute(
                "DELETE FROM gold.cob_sucursal_generico WHERE periodo = %s::date",
                (periodo_date,)
            )
            where_clause = "WHERE DATE_TRUNC('month', fv.fecha_comprobante) = %s::date"
            params = (periodo_date,)
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos...")
            cursor.execute("DELETE FROM gold.cob_sucursal_generico")
            where_clause = ""
            params = None
        else:
            logger.debug("Carga completa...")
            cursor.execute("DELETE FROM gold.cob_sucursal_generico")
            where_clause = ""
            params = None

        insert_query = f"""
            WITH cliente_generico AS (
                SELECT
                    DATE_TRUNC('month', fv.fecha_comprobante)::date AS periodo,
                    dv.id_fuerza_ventas,
                    fv.id_sucursal,
                    ds.descripcion AS ds_sucursal,
                    da.generico,
                    fv.id_cliente,
                    SUM(fv.cantidades_total) AS total_qty
                FROM gold.fact_ventas fv
                JOIN gold.dim_vendedor dv ON fv.id_vendedor = dv.id_vendedor
                    AND fv.id_sucursal = dv.id_sucursal
                LEFT JOIN gold.dim_sucursal ds ON fv.id_sucursal = ds.id_sucursal
                LEFT JOIN gold.dim_articulo da ON fv.id_articulo = da.id_articulo
                {where_clause}
                {"AND" if where_clause else "WHERE"} dv.id_fuerza_ventas IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5, fv.id_cliente
                HAVING SUM(fv.cantidades_total) > 0
            )
            INSERT INTO gold.cob_sucursal_generico (
                periodo, id_fuerza_ventas, id_sucursal, ds_sucursal, generico,
                clientes_compradores, volumen_total
            )
            SELECT
                periodo, id_fuerza_ventas, id_sucursal, ds_sucursal, generico,
                COUNT(DISTINCT id_cliente) AS clientes_compradores,
                SUM(total_qty) AS volumen_total
            FROM cliente_generico
            GROUP BY 1, 2, 3, 4, 5
            ON CONFLICT (periodo, id_fuerza_ventas, id_sucursal, generico)
            DO UPDATE SET
                ds_sucursal = EXCLUDED.ds_sucursal,
                clientes_compradores = EXCLUDED.clientes_compradores,
                volumen_total = EXCLUDED.volumen_total
        """

        cursor.execute(insert_query, params if params else None)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"cob_sucursal_generico completado: {inserted:,} registros en {total_time:.2f}s")


def load_cobertura(periodo: str = '', full_refresh: bool = False):
    """
    Carga todas las tablas de cobertura.
    """
    logger.info("COBERTURA: Iniciando carga de todas las tablas")
    load_cob_preventista_marca(periodo, full_refresh)
    load_cob_sucursal_marca(periodo, full_refresh)
    load_cob_preventista_generico(periodo, full_refresh)
    load_cob_sucursal_generico(periodo, full_refresh)
    logger.info("COBERTURA: Completado")


if __name__ == '__main__':
    load_cobertura(full_refresh=True)
