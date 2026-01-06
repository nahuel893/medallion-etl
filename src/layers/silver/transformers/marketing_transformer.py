"""
Transformer para marketing (segmentos, canales, subcanales).
Extrae la jerarquía de marketing desde bronze.raw_marketing.
"""
from database import engine
from datetime import datetime


def transform_marketing_segments(full_refresh: bool = True):
    """
    Extrae segmentos de marketing desde bronze.raw_marketing a silver.marketing_segments.
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Iniciando transformación de marketing_segments...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        if full_refresh:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Full refresh: eliminando datos...")
            cursor.execute("DELETE FROM silver.marketing_segments")

        insert_query = """
            INSERT INTO silver.marketing_segments (id_segmento_mkt, des_segmento_mkt)
            SELECT DISTINCT
                NULLIF(data_raw->>'idSegmentoMkt', '')::integer,
                data_raw->>'desSegmentoMkt'
            FROM bronze.raw_marketing
            WHERE NULLIF(data_raw->>'idSegmentoMkt', '') IS NOT NULL
            ON CONFLICT (id_segmento_mkt) DO NOTHING
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount
        print(f"    ✓ INSERT completado ({inserted:,} segmentos)")

        raw_conn.commit()
        cursor.close()

        print(f"Transformación completada: {inserted:,} marketing_segments")


def transform_marketing_channels(full_refresh: bool = True):
    """
    Extrae canales de marketing desde bronze.raw_marketing a silver.marketing_channels.
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Iniciando transformación de marketing_channels...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        if full_refresh:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Full refresh: eliminando datos...")
            cursor.execute("DELETE FROM silver.marketing_channels")

        # Extraer canales desde el array CanalesMkt
        insert_query = """
            INSERT INTO silver.marketing_channels (id_canal_mkt, des_canal_mkt, id_segmento_mkt)
            SELECT DISTINCT
                (canal->>'idCanalMkt')::integer,
                canal->>'desCanalMkt',
                (canal->>'idSegmentoMkt')::integer
            FROM bronze.raw_marketing b,
                 LATERAL jsonb_array_elements(b.data_raw->'CanalesMkt') AS canal
            WHERE canal->>'idCanalMkt' IS NOT NULL
            ON CONFLICT (id_canal_mkt) DO NOTHING
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount
        print(f"    ✓ INSERT completado ({inserted:,} canales)")

        raw_conn.commit()
        cursor.close()

        print(f"Transformación completada: {inserted:,} marketing_channels")


def transform_marketing_subchannels(full_refresh: bool = True):
    """
    Extrae subcanales de marketing desde bronze.raw_marketing a silver.marketing_subchannels.
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Iniciando transformación de marketing_subchannels...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        if full_refresh:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Full refresh: eliminando datos...")
            cursor.execute("DELETE FROM silver.marketing_subchannels")

        # Extraer subcanales desde el array anidado CanalesMkt->SubCanalesMkt
        insert_query = """
            INSERT INTO silver.marketing_subchannels (id_subcanal_mkt, des_subcanal_mkt, id_canal_mkt)
            SELECT DISTINCT
                (subcanal->>'idSubcanalMkt')::integer,
                subcanal->>'desSubcanalMkt',
                (subcanal->>'idCanalMkt')::integer
            FROM bronze.raw_marketing b,
                 LATERAL jsonb_array_elements(b.data_raw->'CanalesMkt') AS canal,
                 LATERAL jsonb_array_elements(canal->'SubCanalesMkt') AS subcanal
            WHERE subcanal->>'idSubcanalMkt' IS NOT NULL
            ON CONFLICT (id_subcanal_mkt) DO NOTHING
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount
        print(f"    ✓ INSERT completado ({inserted:,} subcanales)")

        raw_conn.commit()
        cursor.close()

        print(f"Transformación completada: {inserted:,} marketing_subchannels")


def transform_marketing(full_refresh: bool = True):
    """
    Transforma las 3 tablas de marketing en orden jerárquico.
    """
    print("\n" + "="*60)
    print("TRANSFORMACIÓN DE MARKETING (3 niveles)")
    print("="*60 + "\n")

    start_time = datetime.now()

    # Orden jerárquico: segmentos -> canales -> subcanales
    transform_marketing_segments(full_refresh)
    print()
    transform_marketing_channels(full_refresh)
    print()
    transform_marketing_subchannels(full_refresh)

    total_time = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*60}")
    print(f"Marketing completo. Tiempo total: {total_time:.2f}s")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    transform_marketing()
