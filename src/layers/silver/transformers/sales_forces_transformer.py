"""
Transformer para fuerzas de venta (sales_forces).
Extrae fuerzas de venta únicas desde bronze.raw_staff.
"""
from database import engine
from datetime import datetime


def transform_sales_forces(full_refresh: bool = True):
    """
    Extrae fuerzas de venta únicas desde bronze.raw_staff a silver.sales_forces.

    Args:
        full_refresh: Si True (default), elimina todos los datos antes de insertar
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Iniciando transformación de sales_forces...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        cursor.execute("SET work_mem = '256MB'")

        if full_refresh:
            delete_start = datetime.now()
            print(f"[{delete_start.strftime('%H:%M:%S')}] Full refresh: eliminando datos de silver.sales_forces...")
            cursor.execute("DELETE FROM silver.sales_forces")
            delete_time = (datetime.now() - delete_start).total_seconds()
            print(f"    ✓ DELETE completado en {delete_time:.2f}s")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ejecutando INSERT INTO SELECT...")

        # Extraer fuerzas de venta únicas desde staff
        insert_query = """
            INSERT INTO silver.sales_forces (id_fuerza_ventas, des_fuerza_ventas)
            SELECT DISTINCT
                NULLIF(data_raw->>'idFuerzaVentas', '')::integer,
                data_raw->>'desFuerzaVentas'
            FROM bronze.raw_staff
            WHERE NULLIF(data_raw->>'idFuerzaVentas', '') IS NOT NULL
            ON CONFLICT (id_fuerza_ventas) DO NOTHING
        """

        insert_start = datetime.now()
        cursor.execute(insert_query)
        inserted = cursor.rowcount
        insert_time = (datetime.now() - insert_start).total_seconds()

        print(f"    ✓ INSERT completado en {insert_time:.2f}s ({inserted:,} registros)")

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        print(f"\n{'='*60}")
        print(f"Transformación completada: {inserted:,} sales_forces insertados")
        print(f"Tiempo total: {total_time:.2f}s")
        print(f"{'='*60}")


if __name__ == '__main__':
    transform_sales_forces()
