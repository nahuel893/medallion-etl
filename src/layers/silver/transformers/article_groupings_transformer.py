"""
Transformer para agrupaciones de artículos (normalizado).
Expande el array eAgrupaciones de bronze.raw_articles a filas individuales en silver.article_groupings.

Una fila por cada agrupación (MARCA, GENERICO, CALIBRE, ESQUEMA, PROVEED, UNIDAD DE NEGOCIO).
"""
from database import engine
from datetime import datetime


def transform_article_groupings(full_refresh: bool = True):
    """
    Transforma eAgrupaciones de bronze.raw_articles a silver.article_groupings.
    Genera una fila por cada agrupación por artículo.

    Args:
        full_refresh: Si True (default), elimina todos los datos antes de insertar
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Iniciando transformación de article_groupings...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Optimizaciones de PostgreSQL
        cursor.execute("SET work_mem = '512MB'")
        cursor.execute("SET maintenance_work_mem = '1GB'")

        # DELETE - Full refresh
        delete_start = datetime.now()
        print(f"[{delete_start.strftime('%H:%M:%S')}] Full refresh: eliminando datos de silver.article_groupings...")
        cursor.execute("DELETE FROM silver.article_groupings")
        delete_time = (datetime.now() - delete_start).total_seconds()
        print(f"    ✓ DELETE completado en {delete_time:.2f}s")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ejecutando INSERT INTO SELECT...")

        # INSERT con LATERAL expansion (SIN PIVOT)
        # Una fila por cada agrupación por artículo
        insert_query = """
            INSERT INTO silver.article_groupings (
                id_articulo,
                id_forma_agrupar,
                id_agrupacion,
                des_agrupacion
            )
            SELECT DISTINCT ON (
                NULLIF(a.data_raw->>'idArticulo', '')::integer,
                agrup->>'idFormaAgrupar'
            )
                NULLIF(a.data_raw->>'idArticulo', '')::integer AS id_articulo,
                agrup->>'idFormaAgrupar',
                agrup->>'idAgrupacion',
                agrup->>'desAgrupacion'
            FROM bronze.raw_articles a,
                 LATERAL jsonb_array_elements(a.data_raw->'eAgrupaciones') AS agrup
            WHERE agrup->>'idFormaAgrupar' IN (
                'MARCA', 'GENERICO', 'CALIBRE', 'ESQUEMA', 'PROVEED', 'UNIDAD DE NEGOCIO'
            )
            ORDER BY
                NULLIF(a.data_raw->>'idArticulo', '')::integer,
                agrup->>'idFormaAgrupar'
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
        print(f"Transformación completada: {inserted:,} article_groupings insertados")
        print(f"Tiempo total: {total_time:.2f}s ({throughput:,.0f} registros/segundo)")
        print(f"{'='*60}")


if __name__ == '__main__':
    transform_article_groupings()
