"""
Transformer para dim_articulo en Gold layer.
Desnormaliza artículos con sus agrupaciones (marca, genérico, calibre, proveedor, unidad de negocio).
"""
from database import engine
from datetime import datetime


def load_dim_articulo():
    """
    Carga dim_articulo desde silver.articles con agrupaciones desnormalizadas.
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Cargando dim_articulo...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh
        cursor.execute("DELETE FROM gold.dim_articulo")

        # Pivotar agrupaciones y unir con artículos
        insert_query = """
            INSERT INTO gold.dim_articulo (
                id_articulo, des_articulo, marca, generico, calibre, proveedor, unidad_negocio
            )
            SELECT
                a.id_articulo,
                a.des_articulo,
                MAX(CASE WHEN ag.id_forma_agrupar = 'MARCA' THEN ag.des_agrupacion END) AS marca,
                MAX(CASE WHEN ag.id_forma_agrupar = 'GENERICO' THEN ag.des_agrupacion END) AS generico,
                MAX(CASE WHEN ag.id_forma_agrupar = 'CALIBRE' THEN ag.des_agrupacion END) AS calibre,
                MAX(CASE WHEN ag.id_forma_agrupar = 'PROVEED' THEN ag.des_agrupacion END) AS proveedor,
                MAX(CASE WHEN ag.id_forma_agrupar = 'UNIDAD DE NEGOCIO' THEN ag.des_agrupacion END) AS unidad_negocio
            FROM silver.articles a
            LEFT JOIN silver.article_groupings ag ON a.id_articulo = ag.id_articulo
            WHERE a.id_articulo IS NOT NULL
            GROUP BY a.id_articulo, a.des_articulo
            ON CONFLICT (id_articulo) DO UPDATE SET
                des_articulo = EXCLUDED.des_articulo,
                marca = EXCLUDED.marca,
                generico = EXCLUDED.generico,
                calibre = EXCLUDED.calibre,
                proveedor = EXCLUDED.proveedor,
                unidad_negocio = EXCLUDED.unidad_negocio
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        print(f"    ✓ dim_articulo completado: {inserted:,} registros en {total_time:.2f}s")


if __name__ == '__main__':
    load_dim_articulo()
