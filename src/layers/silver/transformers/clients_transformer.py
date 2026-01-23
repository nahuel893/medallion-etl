"""
Transformer para convertir datos crudos de clientes (bronze) a formato estructurado (silver).
Utiliza INSERT INTO SELECT para máxima eficiencia (todo ejecutado en PostgreSQL).

NORMALIZADO: Las fuerzas de venta se almacenan en silver.client_forces (tabla separada).
Este transformer solo inserta los datos core del cliente.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_clients(full_refresh: bool = True):
    """
    Transforma datos de bronze.raw_clients a silver.clients.
    Solo datos core del cliente (sin fuerzas de venta).

    Args:
        full_refresh: Si True (default), elimina todos los datos de silver antes de insertar
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de clientes...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Optimizaciones de PostgreSQL
        cursor.execute("SET work_mem = '512MB'")
        cursor.execute("SET maintenance_work_mem = '1GB'")

        # DELETE - Full refresh
        delete_start = datetime.now()
        logger.debug("Full refresh: eliminando datos de silver.clients...")
        cursor.execute("DELETE FROM silver.clients")
        delete_time = (datetime.now() - delete_start).total_seconds()
        logger.debug(f"DELETE completado en {delete_time:.2f}s")

        # Contar registros a procesar
        count_start = datetime.now()
        cursor.execute("SELECT COUNT(*) FROM bronze.raw_clients")
        total = cursor.fetchone()[0]
        count_time = (datetime.now() - count_start).total_seconds()

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_clients")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros (COUNT en {count_time:.2f}s)")
        logger.debug("Ejecutando INSERT INTO SELECT...")

        # INSERT INTO SELECT - Solo datos core (sin fuerzas de venta)
        insert_query = """
            WITH alias_vigente AS (
                -- Extraer datos fiscales del alias vigente (primer elemento de eClialias)
                SELECT
                    data_raw,
                    (data_raw->'eClialias'->0) AS alias
                FROM bronze.raw_clients
            )
            INSERT INTO silver.clients (
                -- Datos principales
                id_cliente, razon_social, fantasia, id_ramo, desc_ramo, anulado,
                calle, id_localidad, desc_localidad, id_provincia, desc_provincia,
                -- Fechas
                fecha_alta, fecha_baja,
                -- Organización (FK a branches)
                id_sucursal,
                -- Datos fiscales
                identificador, id_tipo_identificador, desc_tipo_identificador,
                id_tipo_contribuyente, desc_tipo_contribuyente, es_inscripto_iibb,
                -- Comercial
                id_lista_precio, desc_lista_precio, id_canal_mkt, desc_canal_mkt,
                id_segmento_mkt, desc_segmento_mkt, id_subcanal_mkt, desc_subcanal_mkt,
                -- Geolocalización
                latitud, longitud,
                -- Contacto
                telefono_fijo, telefono_movil, email
            )
            SELECT
                -- === DATOS PRINCIPALES ===
                NULLIF(a.data_raw->>'idCliente', '')::integer,
                a.alias->>'razonSocial',
                a.alias->>'fantasiaSocial',
                NULLIF(a.data_raw->>'idRamo', '')::integer,
                a.data_raw->>'desRamo',
                COALESCE((a.data_raw->>'anulado')::boolean, false),
                a.data_raw->>'calle',
                NULLIF(a.data_raw->>'idLocalidad', '')::integer,
                a.data_raw->>'desLocalidad',
                a.data_raw->>'idProvincia',
                a.data_raw->>'desProvincia',

                -- === FECHAS ===
                NULLIF(NULLIF(a.data_raw->>'fechaAlta', ''), '0001-01-01')::date,
                NULLIF(NULLIF(a.data_raw->>'fechaBaja', ''), '9999-12-31')::date,

                -- === ORGANIZACIÓN (FK a branches) ===
                NULLIF(a.data_raw->>'idSucursal', '')::integer,

                -- === DATOS FISCALES (desde eClialias) ===
                a.alias->>'identificador',
                NULLIF(a.alias->>'idTipoIdentificador', '')::integer,
                a.alias->>'desTipoIdentificador',
                a.alias->>'idTipoContribuyente',
                a.alias->>'desTipoContribuyente',
                COALESCE((a.alias->>'esInscriptoIibb')::boolean, false),

                -- === COMERCIAL ===
                NULLIF(a.data_raw->>'idListaPrecio', '')::integer,
                a.data_raw->>'desListaPrecio',
                NULLIF(a.data_raw->>'idCanalMkt', '')::integer,
                a.data_raw->>'desCanalMkt',
                NULLIF(a.data_raw->>'idSegmentoMkt', '')::integer,
                a.data_raw->>'desSegmentoMkt',
                NULLIF(a.data_raw->>'idSubcanalMkt', '')::integer,
                a.data_raw->>'desSubcanalMkt',

                -- === GEOLOCALIZACIÓN ===
                NULLIF(a.data_raw->>'latitudGeo', '')::numeric(15,6),
                NULLIF(a.data_raw->>'longitudGeo', '')::numeric(15,6),

                -- === CONTACTO ===
                a.data_raw->>'telefonoFijo',
                a.data_raw->>'telefonoMovil',
                a.data_raw->>'email'

            FROM alias_vigente a
        """

        insert_start = datetime.now()
        cursor.execute(insert_query)
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
        logger.info(f"Transformación completada: {inserted:,} clientes en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    transform_clients()
