"""
Transformer para asignaciones cliente-ruta (normalizado).
Expande el array eClifuerza de bronze.raw_clients a filas individuales en silver.client_forces.

Un cliente pertenece a una fuerza de venta a través de su ruta asignada.
Solo se insertan asignaciones vigentes (fechaFinFuerza = '9999-12-31').
"""
from database import engine
from datetime import datetime


def transform_client_forces(full_refresh: bool = True):
    """
    Transforma eClifuerza de bronze.raw_clients a silver.client_forces.
    Genera una fila por cada asignación cliente-ruta vigente.

    Args:
        full_refresh: Si True (default), elimina todos los datos antes de insertar
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Iniciando transformación de client_forces...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Optimizaciones de PostgreSQL
        cursor.execute("SET work_mem = '512MB'")
        cursor.execute("SET maintenance_work_mem = '1GB'")

        # DELETE - Full refresh
        delete_start = datetime.now()
        print(f"[{delete_start.strftime('%H:%M:%S')}] Full refresh: eliminando datos de silver.client_forces...")
        cursor.execute("DELETE FROM silver.client_forces")
        delete_time = (datetime.now() - delete_start).total_seconds()
        print(f"    ✓ DELETE completado en {delete_time:.2f}s")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ejecutando INSERT INTO SELECT...")

        # INSERT con LATERAL expansion
        # Una fila por cada ruta vigente por cliente
        # La fuerza de venta se obtiene via JOIN a routes
        insert_query = """
            INSERT INTO silver.client_forces (
                id_cliente,
                id_ruta,
                dias_visita,
                semana_visita,
                periodicidad_visita,
                id_modo_atencion,
                fecha_inicio,
                fecha_fin
            )
            SELECT DISTINCT ON (
                NULLIF(b.data_raw->>'idCliente', '')::integer,
                (fuerza->>'idRuta')::integer,
                NULLIF(fuerza->>'fechaInicioFuerza', '')::date
            )
                NULLIF(b.data_raw->>'idCliente', '')::integer AS id_cliente,
                (fuerza->>'idRuta')::integer,
                fuerza->>'diasVisita',
                (fuerza->>'semanaVisita')::integer,
                (fuerza->>'periodicidadVisita')::integer,
                fuerza->>'idModoAtencion',
                NULLIF(fuerza->>'fechaInicioFuerza', '')::date,
                NULLIF(fuerza->>'fechaFinFuerza', '')::date
            FROM bronze.raw_clients b,
                 LATERAL jsonb_array_elements(b.data_raw->'eClifuerza') AS fuerza
            WHERE fuerza->>'fechaFinFuerza' = '9999-12-31'
              AND (fuerza->>'idFuerzaVentas')::integer IN (1, 4)
            ORDER BY
                NULLIF(b.data_raw->>'idCliente', '')::integer,
                (fuerza->>'idRuta')::integer,
                NULLIF(fuerza->>'fechaInicioFuerza', '')::date
        """

        insert_start = datetime.now()
        cursor.execute(insert_query)
        inserted = cursor.rowcount
        insert_time = (datetime.now() - insert_start).total_seconds()

        print(f"    ✓ INSERT completado en {insert_time:.2f}s ({inserted:,} registros)")

        commit_start = datetime.now()
        raw_conn.commit()
        commit_time = (datetime.now() - commit_start).total_seconds()
        print(f"    ✓ COMMIT completado en {commit_time:.2f}s")

        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        throughput = inserted / total_time if total_time > 0 else 0
        print(f"\n{'='*60}")
        print(f"Transformación completada: {inserted:,} client_forces insertados")
        print(f"Tiempo total: {total_time:.2f}s ({throughput:,.0f} registros/segundo)")
        print(f"{'='*60}")


if __name__ == '__main__':
    transform_client_forces()
