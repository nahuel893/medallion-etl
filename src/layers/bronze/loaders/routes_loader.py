import json
from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine
from config import get_logger

logger = get_logger(__name__)


def load_routes():
    """Carga datos de rutas de FV1 y FV4 (full refresh: DELETE + INSERT)."""
    # Future refact, the function load N routes force_sales
    client = ChessClient.from_env(prefix="EMPRESA1_")

    fuerzas = [1, 4]
    all_routes = []

    for fv in fuerzas:
        logger.info(f"Consultando rutas FV{fv} desde API...")
        routes = client.get_routes(fuerza_venta=fv, raw=True)
        if routes:
            logger.info(f"Obtenidas {len(routes)} rutas FV{fv}")
            all_routes.extend(routes)
        else:
            logger.warning(f"Sin datos de rutas para FV{fv}")

    if not all_routes:
        logger.warning("Sin datos de rutas")
        return

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh: DELETE + INSERT
        logger.debug("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_routes")

        logger.debug("Insertando datos nuevos...")
        data = [
            (json.dumps(route), 'API_CHESS_ERP')
            for route in all_routes
        ]

        query = """
            INSERT INTO bronze.raw_routes (data_raw, source_system)
            VALUES %s
        """

        execute_values(
            cursor,
            query,
            data,
            template="(%s::jsonb, %s)"
        )

        raw_conn.commit()
        cursor.close()

    logger.info(f"Insertados {len(all_routes)} rutas en bronze.raw_routes")


if __name__ == '__main__':
    load_routes()
