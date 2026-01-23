"""
Transformer para convertir datos crudos de artículos (bronze) a formato estructurado (silver).
Utiliza INSERT INTO SELECT para máxima eficiencia (todo ejecutado en PostgreSQL).

NORMALIZADO: Las agrupaciones se almacenan en silver.article_groupings (tabla separada).
Este transformer solo inserta los datos core del artículo.
"""
from database import engine
from datetime import datetime
from config import get_logger

logger = get_logger(__name__)


def transform_articles(full_refresh: bool = True):
    """
    Transforma datos de bronze.raw_articles a silver.articles.
    Solo datos core del artículo (sin agrupaciones).

    Args:
        full_refresh: Si True (default), elimina todos los datos de silver antes de insertar
    """
    start_time = datetime.now()
    logger.info("Iniciando transformación de artículos...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Optimizaciones de PostgreSQL
        cursor.execute("SET work_mem = '512MB'")
        cursor.execute("SET maintenance_work_mem = '1GB'")

        # DELETE - Full refresh
        delete_start = datetime.now()
        logger.debug("Full refresh: eliminando datos de silver.articles...")
        cursor.execute("DELETE FROM silver.articles")
        delete_time = (datetime.now() - delete_start).total_seconds()
        logger.debug(f"DELETE completado en {delete_time:.2f}s")

        # Contar registros a procesar
        count_start = datetime.now()
        cursor.execute("SELECT COUNT(*) FROM bronze.raw_articles")
        total = cursor.fetchone()[0]
        count_time = (datetime.now() - count_start).total_seconds()

        if total == 0:
            logger.warning("Sin datos para procesar en bronze.raw_articles")
            cursor.close()
            return

        logger.info(f"Encontrados {total:,} registros (COUNT en {count_time:.2f}s)")
        logger.debug("Ejecutando INSERT INTO SELECT...")

        # INSERT INTO SELECT - Solo datos core (sin agrupaciones)
        insert_query = """
            INSERT INTO silver.articles (
                -- Datos principales
                id_articulo, des_articulo, des_corta_articulo, anulado, fecha_alta,
                -- Características
                es_combo, es_alcoholico, es_activo_fijo, pesable, visible_mobile, tiene_retornables,
                -- Unidades y presentación
                id_unidad_medida, des_unidad_medida, valor_unidad_medida, unidades_bulto,
                id_presentacion_bulto, des_presentacion_bulto, id_presentacion_unidad, des_presentacion_unidad,
                -- Códigos de barra
                cod_barra_bulto, cod_barra_unidad,
                -- Impuestos
                tasa_iva, tasa_iibb, tasa_internos, internos_bulto, exento_iva, iva_diferencial,
                -- Logística
                peso_bulto, bultos_pallet, pisos_pallet
            )
            SELECT
                -- === DATOS PRINCIPALES ===
                NULLIF(a.data_raw->>'idArticulo', '')::integer,
                a.data_raw->>'desArticulo',
                a.data_raw->>'desCortaArticulo',
                COALESCE((a.data_raw->>'anulado')::boolean, false),
                NULLIF(a.data_raw->>'fechaAlta', '')::date,

                -- === CARACTERÍSTICAS ===
                COALESCE((a.data_raw->>'esCombo')::boolean, false),
                COALESCE((a.data_raw->>'esAlcoholico')::boolean, false),
                COALESCE((a.data_raw->>'esActivoFijo')::boolean, false),
                COALESCE((a.data_raw->>'pesable')::boolean, false),
                COALESCE((a.data_raw->>'visibleMobile')::boolean, true),
                COALESCE((a.data_raw->>'tieneRetornables')::boolean, false),

                -- === UNIDADES Y PRESENTACIÓN ===
                NULLIF(a.data_raw->>'idUnidadMedida', '')::integer,
                a.data_raw->>'desUnidadMedida',
                NULLIF(a.data_raw->>'valorUnidadMedida', '')::numeric(10,4),
                NULLIF(a.data_raw->>'unidadesBulto', '')::integer,
                a.data_raw->>'idPresentacionBulto',
                a.data_raw->>'desPresentacionBulto',
                a.data_raw->>'idPresentacionUnidad',
                a.data_raw->>'desPresentacionUnidad',

                -- === CÓDIGOS DE BARRA ===
                a.data_raw->>'codBarraBulto',
                a.data_raw->>'codBarraUnidad',

                -- === IMPUESTOS ===
                NULLIF(a.data_raw->>'tasaIva', '')::numeric(8,4),
                NULLIF(a.data_raw->>'tasaIibb', '')::numeric(8,4),
                NULLIF(a.data_raw->>'tasaInternos', '')::numeric(8,4),
                NULLIF(a.data_raw->>'internosBulto', '')::numeric(15,4),
                COALESCE((a.data_raw->>'exentoIva')::boolean, false),
                COALESCE((a.data_raw->>'ivaDiferencial')::boolean, false),

                -- === LOGÍSTICA ===
                NULLIF(a.data_raw->>'pesoBulto', '')::numeric(10,4),
                NULLIF(a.data_raw->>'bultosPallet', '')::integer,
                NULLIF(a.data_raw->>'pisosPallet', '')::integer

            FROM bronze.raw_articles a
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
        logger.info(f"Transformación completada: {inserted:,} artículos en {total_time:.2f}s ({throughput:,.0f} reg/s)")


if __name__ == '__main__':
    transform_articles()
