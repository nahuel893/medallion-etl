"""
Transformer para dim_cliente en Gold layer.
Desnormaliza clientes con sucursal, marketing y rutas/preventistas por fuerza de venta.
"""
from database import engine
from datetime import datetime


def load_dim_cliente():
    """
    Carga dim_cliente desde silver.clients con todas las dimensiones desnormalizadas.
    """
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Cargando dim_cliente...")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh
        cursor.execute("DELETE FROM gold.dim_cliente")

        # Query compleja con todas las desnormalizaciones
        insert_query = """
            WITH rutas_fv1 AS (
                -- Ruta y preventista para Fuerza de Ventas 1
                SELECT DISTINCT ON (cf.id_cliente)
                    cf.id_cliente,
                    cf.id_ruta,
                    s.des_personal
                FROM silver.client_forces cf
                JOIN silver.routes r ON cf.id_ruta = r.id_ruta
                JOIN silver.staff s ON r.id_personal = s.id_personal
                WHERE r.id_fuerza_ventas = 1
                  AND cf.fecha_fin = '9999-12-31'
                ORDER BY cf.id_cliente, cf.fecha_inicio DESC
            ),
            rutas_fv4 AS (
                -- Ruta y preventista para Fuerza de Ventas 4
                SELECT DISTINCT ON (cf.id_cliente)
                    cf.id_cliente,
                    cf.id_ruta,
                    s.des_personal
                FROM silver.client_forces cf
                JOIN silver.routes r ON cf.id_ruta = r.id_ruta
                JOIN silver.staff s ON r.id_personal = s.id_personal
                WHERE r.id_fuerza_ventas = 4
                  AND cf.fecha_fin = '9999-12-31'
                ORDER BY cf.id_cliente, cf.fecha_inicio DESC
            )
            INSERT INTO gold.dim_cliente (
                id_cliente, razon_social, fantasia,
                id_sucursal, des_sucursal,
                id_canal_mkt, des_canal_mkt,
                id_segmento_mkt, des_segmento_mkt,
                id_subcanal_mkt, des_subcanal_mkt,
                id_ruta_fv1, des_personal_fv1,
                id_ruta_fv4, des_personal_fv4,
                id_ramo, des_ramo,
                id_localidad, des_localidad,
                id_provincia, des_provincia
            )
            SELECT
                c.id_cliente,
                c.razon_social,
                c.fantasia,

                -- Sucursal
                c.id_sucursal,
                b.descripcion AS des_sucursal,

                -- Marketing
                c.id_canal_mkt,
                mc.des_canal_mkt,
                c.id_segmento_mkt,
                ms.des_segmento_mkt,
                c.id_subcanal_mkt,
                msc.des_subcanal_mkt,

                -- Ruta/Preventista FV1
                fv1.id_ruta,
                fv1.des_personal,

                -- Ruta/Preventista FV4
                fv4.id_ruta,
                fv4.des_personal,

                -- Clasificación
                c.id_ramo,
                c.desc_ramo,
                c.id_localidad,
                c.desc_localidad,
                c.id_provincia,
                c.desc_provincia

            FROM silver.clients c
            LEFT JOIN silver.branches b ON c.id_sucursal = b.id_sucursal
            LEFT JOIN silver.marketing_channels mc ON c.id_canal_mkt = mc.id_canal_mkt
            LEFT JOIN silver.marketing_segments ms ON c.id_segmento_mkt = ms.id_segmento_mkt
            LEFT JOIN silver.marketing_subchannels msc ON c.id_subcanal_mkt = msc.id_subcanal_mkt
            LEFT JOIN rutas_fv1 fv1 ON c.id_cliente = fv1.id_cliente
            LEFT JOIN rutas_fv4 fv4 ON c.id_cliente = fv4.id_cliente
            WHERE c.id_cliente IS NOT NULL
            ON CONFLICT (id_cliente) DO UPDATE SET
                razon_social = EXCLUDED.razon_social,
                fantasia = EXCLUDED.fantasia,
                id_sucursal = EXCLUDED.id_sucursal,
                des_sucursal = EXCLUDED.des_sucursal,
                id_canal_mkt = EXCLUDED.id_canal_mkt,
                des_canal_mkt = EXCLUDED.des_canal_mkt,
                id_segmento_mkt = EXCLUDED.id_segmento_mkt,
                des_segmento_mkt = EXCLUDED.des_segmento_mkt,
                id_subcanal_mkt = EXCLUDED.id_subcanal_mkt,
                des_subcanal_mkt = EXCLUDED.des_subcanal_mkt,
                id_ruta_fv1 = EXCLUDED.id_ruta_fv1,
                des_personal_fv1 = EXCLUDED.des_personal_fv1,
                id_ruta_fv4 = EXCLUDED.id_ruta_fv4,
                des_personal_fv4 = EXCLUDED.des_personal_fv4,
                id_ramo = EXCLUDED.id_ramo,
                des_ramo = EXCLUDED.des_ramo,
                id_localidad = EXCLUDED.id_localidad,
                des_localidad = EXCLUDED.des_localidad,
                id_provincia = EXCLUDED.id_provincia,
                des_provincia = EXCLUDED.des_provincia
        """

        cursor.execute(insert_query)
        inserted = cursor.rowcount

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        print(f"    ✓ dim_cliente completado: {inserted:,} registros en {total_time:.2f}s")


if __name__ == '__main__':
    load_dim_cliente()
