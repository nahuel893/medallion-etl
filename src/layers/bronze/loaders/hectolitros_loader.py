"""
Loader de factores de conversión a hectolitros desde archivo Excel.
Carga data/hectolitros.xlsx en bronze.raw_hectolitros.
"""
from pathlib import Path

import openpyxl
from psycopg2.extras import execute_values
from database import engine
from config import get_logger

logger = get_logger(__name__)

HECTOLITROS_FILE = Path(__file__).parent.parent.parent.parent.parent / 'data' / 'hectolitros.xlsx'


def load_hectolitros():
    """Carga factores de conversión a hectolitros desde Excel (full refresh: DELETE + INSERT)."""

    logger.info(f"Leyendo hectolitros desde: {HECTOLITROS_FILE}")

    wb = openpyxl.load_workbook(HECTOLITROS_FILE, data_only=True)
    ws = wb.active

    registros_dict = {}
    skipped = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        id_articulo = row[0]
        descripcion = row[1]
        htls = row[2]

        if id_articulo is None or htls is None:
            continue

        if not isinstance(id_articulo, (int, float)) or not isinstance(htls, (int, float)):
            skipped += 1
            logger.debug(f"Fila omitida (dato no numérico): id={id_articulo}, htls={htls}")
            continue

        # Último valor gana en caso de duplicados
        registros_dict[int(id_articulo)] = (
            int(id_articulo),
            str(descripcion) if descripcion else None,
            float(htls),
            'XLSX_HECTOLITROS'
        )

    registros = list(registros_dict.values())

    if skipped:
        logger.warning(f"Omitidas {skipped} filas con datos no numéricos")

    wb.close()

    if not registros:
        logger.warning("Sin datos de hectolitros")
        return

    logger.info(f"Obtenidos {len(registros)} factores de conversión")

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        logger.debug("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM bronze.raw_hectolitros")

        logger.debug("Insertando datos nuevos...")
        query = """
            INSERT INTO bronze.raw_hectolitros (id_articulo, descripcion, factor_hectolitros, source_system)
            VALUES %s
        """

        execute_values(
            cursor,
            query,
            registros,
            template="(%s, %s, %s, %s)"
        )

        raw_conn.commit()
        cursor.close()

    logger.info(f"Insertados {len(registros)} factores en bronze.raw_hectolitros")


if __name__ == '__main__':
    load_hectolitros()
