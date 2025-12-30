"""
Transformer para convertir datos crudos de ventas (bronze) a formato estructurado (silver).
"""
from typing import Optional
from psycopg2.extras import execute_values
from database import engine


def parse_date(value: str) -> Optional[str]:
    """Convierte fecha a formato válido o None si es inválida."""
    if not value or value in ('', '0001-01-01', None):
        return None
    return value


def parse_bool(value) -> bool:
    """Convierte valor a booleano."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.upper() in ('SI', 'YES', 'TRUE', '1', 'S')
    return bool(value)


def parse_numeric(value) -> Optional[float]:
    """Convierte valor a numérico o None."""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_int(value) -> Optional[int]:
    """Convierte valor a entero o None."""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def transform_sales(fecha_desde: str = None, fecha_hasta: str = None, full_refresh: bool = False):
    """
    Transforma datos de bronze.raw_sales a silver.fact_ventas.

    Args:
        fecha_desde: Fecha inicial para filtrar (opcional)
        fecha_hasta: Fecha final para filtrar (opcional)
        full_refresh: Si True, elimina todos los datos de silver antes de insertar
    """
    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Construir query de selección desde bronze
        where_clauses = []
        params = []

        if fecha_desde:
            where_clauses.append("date_comprobante >= %s")
            params.append(fecha_desde)
        if fecha_hasta:
            where_clauses.append("date_comprobante <= %s")
            params.append(fecha_hasta)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Si es full refresh, eliminar todo. Si no, eliminar solo el rango de fechas
        if full_refresh:
            print("Full refresh: eliminando todos los datos de silver.fact_ventas...")
            cursor.execute("DELETE FROM silver.fact_ventas")
        elif fecha_desde and fecha_hasta:
            print(f"Eliminando datos existentes en silver para el rango {fecha_desde} - {fecha_hasta}...")
            cursor.execute(
                "DELETE FROM silver.fact_ventas WHERE fecha_comprobante >= %s AND fecha_comprobante <= %s",
                (fecha_desde, fecha_hasta)
            )

        # Obtener datos de bronze
        query = f"""
            SELECT id, data_raw
            FROM bronze.raw_sales
            {where_sql}
            ORDER BY date_comprobante
        """
        cursor.execute(query, params if params else None)
        rows = cursor.fetchall()

        if not rows:
            print("Sin datos para procesar en bronze.raw_sales")
            return

        print(f"Procesando {len(rows)} registros de bronze...")

        # Parsear y preparar datos para silver
        silver_data = []
        for bronze_id, data in rows:
            silver_data.append((
                bronze_id,
                # Identificación documento
                data.get('idEmpresa'),
                data.get('dsEmpresa'),
                data.get('idDocumento'),
                data.get('dsDocumento'),
                data.get('letra'),
                parse_int(data.get('serie')),
                data.get('nrodoc'),
                parse_bool(data.get('anulado')),
                # Fechas
                parse_date(data.get('fechaComprobate')),
                parse_date(data.get('fechaAlta')),
                parse_date(data.get('fechaPedido')),
                parse_date(data.get('fechaEntrega')),
                parse_date(data.get('fechaVencimiento')),
                parse_date(data.get('fechaCaja')),
                # Organización
                parse_int(data.get('idSucursal')),
                data.get('dsSucursal'),
                parse_int(data.get('idDeposito')),
                data.get('dsDeposito'),
                # Personal
                parse_int(data.get('idVendedor')),
                data.get('dsVendedor'),
                parse_int(data.get('idSupervisor')),
                data.get('dsSupervisor'),
                parse_int(data.get('idGerente')),
                data.get('dsGerente'),
                # Cliente
                data.get('idCliente'),
                data.get('nombreCliente'),
                data.get('domicilioCliente'),
                str(data.get('codigoPostal', '')) if data.get('codigoPostal') else None,
                parse_int(data.get('idLocalidad')),
                data.get('dsLocalidad'),
                data.get('idProvincia'),
                data.get('dsProvincia'),
                # Pago
                parse_int(data.get('idTipoPago')),
                data.get('dsTipoPago'),
                # Segmentación comercial
                parse_int(data.get('idNegocio')),
                data.get('dsNegocio'),
                parse_int(data.get('idCanalMkt')),
                data.get('dsCanalMkt'),
                parse_int(data.get('idSegmentoMkt')),
                data.get('dsSegmentoMkt'),
                parse_int(data.get('idArea')),
                data.get('dsArea'),
                # Línea de venta
                data.get('idLinea'),
                data.get('idArticulo'),
                data.get('dsArticulo'),
                parse_int(data.get('idConcepto')),
                data.get('dsConcepto'),
                parse_bool(data.get('esCombo')),
                parse_int(data.get('idCombo')),
                # Artículo estadístico
                parse_int(data.get('idArticuloEstadistico')),
                data.get('dsArticuloEstadistico'),
                str(data.get('presentacionArticulo', '')) if data.get('presentacionArticulo') else None,
                # Cantidades
                parse_numeric(data.get('cantidadSolicitada')),
                parse_numeric(data.get('unidadesSolicitadas')),
                parse_numeric(data.get('cantidadesCorCargo')),
                parse_numeric(data.get('cantidadesSinCargo')),
                parse_numeric(data.get('cantidadesTotal')),
                parse_numeric(data.get('cantidadesRechazo')),
                parse_numeric(data.get('peso')),
                parse_numeric(data.get('pesoTotal')),
                # Precios
                parse_numeric(data.get('precioUnitarioBruto')),
                parse_numeric(data.get('bonificacion')),
                parse_numeric(data.get('precioUnitarioNeto')),
                # Subtotales
                parse_numeric(data.get('subtotalBruto')),
                parse_numeric(data.get('subtotalBonificado')),
                parse_numeric(data.get('subtotalNeto')),
                parse_numeric(data.get('subtotalFinal')),
                # Impuestos
                parse_numeric(data.get('iva21')),
                parse_numeric(data.get('iva27')),
                parse_numeric(data.get('iva105')),
                parse_numeric(data.get('internos')),
                parse_numeric(data.get('per3337')),
                parse_numeric(data.get('percepcion212')),
                parse_numeric(data.get('percepcioniibb')),
                # Trade spend
                parse_numeric(data.get('totradspend')),
                # Metadata
                data.get('origen'),
                parse_int(data.get('idRechazo')),
                data.get('dsRechazo'),
            ))

        # Insertar en silver
        insert_query = """
            INSERT INTO silver.fact_ventas (
                bronze_id,
                id_empresa, ds_empresa, id_documento, ds_documento, letra, serie, nro_doc, anulado,
                fecha_comprobante, fecha_alta, fecha_pedido, fecha_entrega, fecha_vencimiento, fecha_caja,
                id_sucursal, ds_sucursal, id_deposito, ds_deposito,
                id_vendedor, ds_vendedor, id_supervisor, ds_supervisor, id_gerente, ds_gerente,
                id_cliente, nombre_cliente, domicilio_cliente, codigo_postal, id_localidad, ds_localidad, id_provincia, ds_provincia,
                id_tipo_pago, ds_tipo_pago,
                id_negocio, ds_negocio, id_canal_mkt, ds_canal_mkt, id_segmento_mkt, ds_segmento_mkt, id_area, ds_area,
                id_linea, id_articulo, ds_articulo, id_concepto, ds_concepto, es_combo, id_combo,
                id_articulo_estadistico, ds_articulo_estadistico, presentacion_articulo,
                cantidad_solicitada, unidades_solicitadas, cantidades_con_cargo, cantidades_sin_cargo, cantidades_total, cantidades_rechazo, peso, peso_total,
                precio_unitario_bruto, bonificacion, precio_unitario_neto,
                subtotal_bruto, subtotal_bonificado, subtotal_neto, subtotal_final,
                iva21, iva27, iva105, internos, per3337, percepcion212, percepcion_iibb,
                totradspend,
                origen, id_rechazo, ds_rechazo
            ) VALUES %s
        """

        print("Insertando datos en silver.fact_ventas...")
        execute_values(cursor, insert_query, silver_data, page_size=1000)
        raw_conn.commit()
        cursor.close()

        print(f"Transformación completada: {len(silver_data)} registros insertados en silver.fact_ventas")


if __name__ == '__main__':
    transform_sales()
