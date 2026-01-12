import json
import csv
from datetime import datetime, timedelta
from pathlib import Path

from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine
from config import get_logger

logger = get_logger(__name__)


# Ruta al archivo de depósitos
DEPOSITS_FILE = Path(__file__).parent.parent.parent.parent.parent / 'data' / 'deposits_b.csv'


def cargar_depositos():
    """Carga la lista de depósitos desde el archivo CSV."""
    depositos = []
    with open(DEPOSITS_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            depositos.append({
                'id': int(row['idDeposito']),
                'nombre': row['descripcion_deposito']
            })
    return depositos


def generar_rangos_diarios(fecha_desde: str, fecha_hasta: str):
    """Genera lista de fechas día a día entre dos fechas."""
    inicio = datetime.strptime(fecha_desde, '%Y-%m-%d')
    fin = datetime.strptime(fecha_hasta, '%Y-%m-%d')

    fechas = []
    actual = inicio

    while actual <= fin:
        fechas.append(actual.strftime('%Y-%m-%d'))
        actual += timedelta(days=1)

    return fechas


def load_stock(fecha_desde: str, fecha_hasta: str):
    """Carga datos de stock día a día por depósito (append: mantiene historial)."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    depositos = cargar_depositos()
    fechas = generar_rangos_diarios(fecha_desde, fecha_hasta)

    total_consultas = len(fechas) * len(depositos)
    logger.info(f"Procesando {len(fechas)} día(s) x {len(depositos)} depósitos = {total_consultas} consultas")

    total_registros = 0
    consulta_actual = 0

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        for fecha in fechas:
            logger.info(f"--- Fecha: {fecha} ---")

            for deposito in depositos:
                consulta_actual += 1
                logger.debug(f"[{consulta_actual}/{total_consultas}] Depósito {deposito['id']}: {deposito['nombre']}")

                stock = client.get_stock(
                    fecha=fecha,
                    id_deposito=deposito['id'],
                    raw=True
                )

                if not stock:
                    logger.debug(f"Sin datos para depósito {deposito['id']}")
                    continue

                logger.debug(f"Obtenidos {len(stock)} registros")

                data = [
                    (json.dumps(item), 'API_CHESS_ERP', fecha, deposito['id'])
                    for item in stock
                ]

                query = """
                    INSERT INTO bronze.raw_stock (data_raw, source_system, date_stock, id_deposito)
                    VALUES %s
                """

                execute_values(
                    cursor,
                    query,
                    data,
                    template="(%s::jsonb, %s, %s::date, %s)"
                )

                raw_conn.commit()
                total_registros += len(data)

        cursor.close()

    logger.info(f"Total: {total_registros} registros insertados en bronze.raw_stock")


if __name__ == '__main__':
    load_stock('2025-12-01', '2025-12-01')
