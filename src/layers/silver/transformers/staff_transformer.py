"""
Transformer para preventistas/personal (staff).
Convierte bronze.raw_staff a silver.staff.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_staff(full_refresh: bool = True):
    """
    Transforma bronze.raw_staff a silver.staff.

    Args:
        full_refresh: Si True (default), elimina todos los datos antes de insertar
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de staff...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '512MB'")

        if full_refresh:
            delete_start = datetime.now()
            logger.debug("Full refresh: eliminando datos de silver.staff...")
            cursor.execute("DELETE FROM silver.staff")
            delete_time = (datetime.now() - delete_start).total_seconds()
            logger.debug(f"DELETE completado en {delete_time:.2f}s")

        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM bronze.raw_staff")
        total = cursor.fetchone()[0]

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_staff")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros")
        logger.debug("Ejecutando INSERT INTO SELECT...")

        insert_query = """
            INSERT INTO silver.staff (
                id_personal,
                des_personal,
                cargo,
                tipo_venta,
                usuario_sistema,
                telefono,
                domicilio,
                fecha_nacimiento,
                id_sucursal,
                id_fuerza_ventas,
                id_personal_superior
            )
            SELECT DISTINCT ON (
                    NULLIF(data_raw->>'idPersonal', '')::integer,
                    NULLIF(data_raw->>'idSucursal', '')::integer
                )
                NULLIF(data_raw->>'idPersonal', '')::integer,
                data_raw->>'desPersonal',
                data_raw->>'cargo',
                data_raw->>'tipoVenta',
                data_raw->>'usuarioSistema',
                data_raw->>'telefono',
                data_raw->>'domicilio',
                NULLIF(data_raw->>'fechaNacimiento', '')::date,
                NULLIF(data_raw->>'idSucursal', '')::integer,
                NULLIF(data_raw->>'idFuerzaVentas', '')::integer,
                NULLIF(data_raw->>'idPersonalSuperior', '')::integer
            FROM bronze.raw_staff
            WHERE NULLIF(data_raw->>'idPersonal', '') IS NOT NULL
            ORDER BY
                NULLIF(data_raw->>'idPersonal', '')::integer,
                NULLIF(data_raw->>'idSucursal', '')::integer,
                id DESC
            ON CONFLICT (id_personal, id_sucursal) DO UPDATE SET
                des_personal = EXCLUDED.des_personal,
                cargo = EXCLUDED.cargo,
                tipo_venta = EXCLUDED.tipo_venta,
                usuario_sistema = EXCLUDED.usuario_sistema,
                telefono = EXCLUDED.telefono,
                domicilio = EXCLUDED.domicilio,
                fecha_nacimiento = EXCLUDED.fecha_nacimiento,
                id_sucursal = EXCLUDED.id_sucursal,
                id_fuerza_ventas = EXCLUDED.id_fuerza_ventas,
                id_personal_superior = EXCLUDED.id_personal_superior,
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
        logger.info(f"Transformación completada: {inserted:,} staff en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    transform_staff()
