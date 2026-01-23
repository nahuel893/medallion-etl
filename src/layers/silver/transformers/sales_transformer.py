"""
Transformer para convertir datos crudos de ventas (bronze) a formato estructurado (silver).
Utiliza INSERT INTO SELECT para máxima eficiencia (todo ejecutado en PostgreSQL).

NORMALIZADO: Solo IDs de dimensiones, sin descripciones redundantes.
Las descripciones se obtienen via JOIN a las tablas de dimensiones.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_sales(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """
    Transforma datos de bronze.raw_sales a silver.fact_ventas.

    Args:
        fecha_desde: Fecha inicial para filtrar (opcional)
        fecha_hasta: Fecha final para filtrar (opcional)
        full_refresh: Si True, elimina todos los datos de silver antes de insertar
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de ventas...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Optimizaciones de PostgreSQL
        cursor.execute("SET work_mem = '1GB'")
        cursor.execute("SET maintenance_work_mem = '2GB'")

        # Construir cláusula WHERE
        where_conditions = []
        params = []

        if fecha_desde:
            where_conditions.append("date_comprobante >= %s")
            params.append(fecha_desde)
        if fecha_hasta:
            where_conditions.append("date_comprobante <= %s")
            params.append(fecha_hasta)

        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""

        # DELETE según el modo (fechas tienen prioridad sobre full_refresh)
        delete_start = datetime.now()
        if fecha_desde and fecha_hasta:
            logger.debug(f"Eliminando datos existentes en silver para el rango {fecha_desde} - {fecha_hasta}...")
            cursor.execute(
                "DELETE FROM silver.fact_ventas WHERE fecha_comprobante >= %s AND fecha_comprobante <= %s",
                (fecha_desde, fecha_hasta)
            )
        elif full_refresh:
            logger.debug("Full refresh: eliminando todos los datos de silver.fact_ventas...")
            cursor.execute("DELETE FROM silver.fact_ventas")

        if full_refresh or (fecha_desde and fecha_hasta):
            delete_time = (datetime.now() - delete_start).total_seconds()
            logger.debug(f"DELETE completado en {delete_time:.2f}s")

        # Contar registros a procesar
        count_start = datetime.now()
        count_query = f"SELECT COUNT(*) FROM bronze.raw_sales {where_clause}"
        cursor.execute(count_query, params if params else None)
        total = cursor.fetchone()[0]
        count_time = (datetime.now() - count_start).total_seconds()

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_sales")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros (COUNT en {count_time:.2f}s)")
        logger.debug("Ejecutando INSERT INTO SELECT...")

        # INSERT INTO SELECT - NORMALIZADO (solo IDs, sin descripciones)
        insert_query = f"""
            INSERT INTO silver.fact_ventas (
                -- Identificación documento
                id_empresa, id_documento, letra, serie, nro_doc, anulado,
                -- Fechas
                fecha_comprobante, fecha_alta, fecha_pedido, fecha_entrega, fecha_vencimiento, fecha_caja,
                fecha_anulacion, fecha_pago, fecha_liquidacion, fecha_asiento_contable,
                -- Organización (solo IDs)
                id_sucursal, id_deposito, id_caja, cajero, id_centro_costo,
                -- Personal (solo IDs)
                id_vendedor, id_supervisor, id_gerente, id_fuerza_ventas, usuario_alta,
                -- Cliente (solo ID)
                id_cliente, linea_credito,
                -- Segmentación comercial (solo IDs)
                id_canal_mkt, id_segmento_mkt, id_subcanal_mkt,
                -- Logística
                id_fletero_carga, planilla_carga,
                -- Línea de venta (solo ID artículo)
                id_articulo, es_combo, id_combo, id_pedido, id_origen, origen, acciones,
                -- Cantidades
                cantidades_con_cargo, cantidades_sin_cargo, cantidades_total, cantidades_rechazo,
                -- Precios
                precio_unitario_bruto, precio_unitario_neto, bonificacion, precio_compra_bruto, precio_compra_neto,
                -- Subtotales
                subtotal_bruto, subtotal_bonificado, subtotal_neto, subtotal_final,
                -- Impuestos
                iva21, iva27, iva105, iva2, internos, per3337, percepcion212, percepcion_iibb,
                pers_iibb_d, pers_iibb_r, cod_prov_iibb,
                -- Contabilidad
                cod_cuenta_contable, nro_asiento_contable, nro_plan_contable, id_liquidacion,
                -- Proveedor
                proveedor, fvig_pcompra,
                -- Metadata / Rechazo
                id_rechazo, informado, regimen_fiscal
            )
            SELECT
                -- === IDENTIFICACIÓN DOCUMENTO ===
                NULLIF(data_raw->>'idEmpresa', '')::integer,
                data_raw->>'idDocumento',
                data_raw->>'letra',
                NULLIF(data_raw->>'serie', '')::integer,
                NULLIF(data_raw->>'nrodoc', '')::integer,
                UPPER(data_raw->>'anulado') = 'SI',

                -- === FECHAS ===
                NULLIF(NULLIF(data_raw->>'fechaComprobate', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaAlta', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaPedido', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaEntrega', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaVencimiento', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaCaja', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaAnulacion', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaPago', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaLiquidacion', ''), '0001-01-01')::date,
                NULLIF(NULLIF(data_raw->>'fechaAsientoContable', ''), '0001-01-01')::date,

                -- === ORGANIZACIÓN (solo IDs) ===
                NULLIF(data_raw->>'idSucursal', '')::integer,
                NULLIF(data_raw->>'idDeposito', '')::integer,
                NULLIF(data_raw->>'idCaja', '')::integer,
                data_raw->>'cajero',
                NULLIF(data_raw->>'idCentroCosto', '')::integer,

                -- === PERSONAL (solo IDs) ===
                NULLIF(data_raw->>'idVendedor', '')::integer,
                NULLIF(data_raw->>'idSupervisor', '')::integer,
                NULLIF(data_raw->>'idGerente', '')::integer,
                NULLIF(data_raw->>'idFuerzaVentas', '')::integer,
                data_raw->>'usuarioAlta',

                -- === CLIENTE (solo ID) ===
                NULLIF(data_raw->>'idCliente', '')::integer,
                data_raw->>'lineaCredito',

                -- === SEGMENTACIÓN COMERCIAL (solo IDs) ===
                NULLIF(data_raw->>'idCanalMkt', '')::integer,
                NULLIF(data_raw->>'idSegmentoMkt', '')::integer,
                NULLIF(data_raw->>'idSubcanalMkt', '')::integer,

                -- === LOGÍSTICA ===
                NULLIF(data_raw->>'idFleteroCarga', '')::integer,
                data_raw->>'planillaCarga',

                -- === LÍNEA DE VENTA (solo ID artículo) ===
                NULLIF(data_raw->>'idArticulo', '')::integer,
                UPPER(data_raw->>'esCombo') = 'SI',
                NULLIF(data_raw->>'idCombo', '')::integer,
                NULLIF(data_raw->>'idPedido', '')::integer,
                NULLIF(data_raw->>'idorigen', ''),
                data_raw->>'origen',
                data_raw->>'acciones',

                -- === CANTIDADES ===
                NULLIF(data_raw->>'cantidadesCorCargo', '')::numeric(15,4),
                NULLIF(data_raw->>'cantidadesSinCargo', '')::numeric(15,4),
                NULLIF(data_raw->>'cantidadesTotal', '')::numeric(15,4),
                NULLIF(data_raw->>'cantidadesRechazo', '')::numeric(15,4),

                -- === PRECIOS ===
                NULLIF(data_raw->>'precioUnitarioBruto', '')::numeric(15,4),
                NULLIF(data_raw->>'precioUnitarioNeto', '')::numeric(15,4),
                NULLIF(data_raw->>'bonificacion', '')::numeric(8,4),
                NULLIF(data_raw->>'preciocomprabr', '')::numeric(15,4),
                NULLIF(data_raw->>'preciocomprant', '')::numeric(15,4),

                -- === SUBTOTALES ===
                NULLIF(data_raw->>'subtotalBruto', '')::numeric(15,4),
                NULLIF(data_raw->>'subtotalBonificado', '')::numeric(15,4),
                NULLIF(data_raw->>'subtotalNeto', '')::numeric(15,4),
                NULLIF(data_raw->>'subtotalFinal', '')::numeric(15,4),

                -- === IMPUESTOS ===
                NULLIF(data_raw->>'iva21', '')::numeric(15,4),
                NULLIF(data_raw->>'iva27', '')::numeric(15,4),
                NULLIF(data_raw->>'iva105', '')::numeric(15,4),
                NULLIF(data_raw->>'iva2', '')::numeric(15,4),
                NULLIF(data_raw->>'internos', '')::numeric(15,4),
                NULLIF(data_raw->>'per3337', '')::numeric(15,4),
                NULLIF(data_raw->>'percepcion212', '')::numeric(15,4),
                NULLIF(data_raw->>'percepcioniibb', '')::numeric(15,4),
                NULLIF(data_raw->>'persiibbd', '')::numeric(15,4),
                NULLIF(data_raw->>'persiibbr', '')::numeric(15,4),
                data_raw->>'codproviibb',

                -- === CONTABILIDAD ===
                data_raw->>'codCuentaContable',
                NULLIF(data_raw->>'nroAsientoContable', '')::integer,
                NULLIF(data_raw->>'nroPlanContable', '')::integer,
                NULLIF(data_raw->>'idLiquidacion', '')::integer,

                -- === PROVEEDOR (solo código, sin nombre) ===
                SPLIT_PART(data_raw->>'proveedor', ' - ', 1),
                NULLIF(NULLIF(data_raw->>'fvigpcompra', ''), '0001-01-01')::date,

                -- === METADATA / RECHAZO ===
                NULLIF(data_raw->>'idRechazo', '')::integer,
                UPPER(data_raw->>'informado') = 'SI',
                data_raw->>'regimenFiscal'

            FROM bronze.raw_sales
            {where_clause}
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
        logger.info(f"Transformación completada: {inserted:,} ventas en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    transform_sales()
