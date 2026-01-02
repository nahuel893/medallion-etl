from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import json

from psycopg2.extras import execute_values
from chesserp.client import ChessClient
from database import engine


def generar_rangos_mensuales(fecha_desde: str, fecha_hasta: str):
    """
    Genera rangos mensuales entre dos fechas.

    Ejemplo: 2025-01-15 a 2025-03-20 genera:
        - (2025-01-15, 2025-01-31)
        - (2025-02-01, 2025-02-28)
        - (2025-03-01, 2025-03-20)
    """
    inicio = datetime.strptime(fecha_desde, '%Y-%m-%d')
    fin = datetime.strptime(fecha_hasta, '%Y-%m-%d')

    rangos = []
    actual = inicio

    while actual <= fin:
        if actual == inicio:
            mes_inicio = actual
        else:
            mes_inicio = actual.replace(day=1)

        ultimo_dia = monthrange(actual.year, actual.month)[1]
        mes_fin = actual.replace(day=ultimo_dia)

        if mes_fin > fin:
            mes_fin = fin

        rangos.append((
            mes_inicio.strftime('%Y-%m-%d'),
            mes_fin.strftime('%Y-%m-%d')
        ))

        actual = (actual.replace(day=1) + relativedelta(months=1))

    return rangos


def load_bronze(fecha_desde: str, fecha_hasta: str):
    """Carga datos de ventas mes a mes entre las fechas especificadas."""
    client = ChessClient.from_env(prefix="EMPRESA1_")

    rangos = generar_rangos_mensuales(fecha_desde, fecha_hasta)
    print(f"Procesando {len(rangos)} mes(es)...\n")

    total_registros = 0

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        for i, (mes_desde, mes_hasta) in enumerate(rangos, 1):
            print(f"[{i}/{len(rangos)}] Consultando: {mes_desde} - {mes_hasta}")

            sales = client.get_sales(
                fecha_desde=mes_desde,
                fecha_hasta=mes_hasta,
                detallado=True,
                empresas='1',
                raw=True
            )

            if not sales:
                print(f"Sin datos para este período\n")
                continue

            print(f"Obtenidos {len(sales)} registros")

            data = [
                (
                    json.dumps(sale),
                    'API_CHESS_ERP',
                    sale['fechaComprobate']
                )
                for sale in sales
            ]

            query = """
                INSERT INTO bronze.raw_sales (data_raw, source_system, date_comprobante)
                VALUES %s
            """

            execute_values(
                cursor,
                query,
                data,
                template="(%s::jsonb, %s, %s::date)"
            )
            raw_conn.commit()

            total_registros += len(data)
            print(f"         Insertados correctamente\n")

        cursor.close()

    print(f"Total: {total_registros} registros insertados en bronze.raw_sales")


if __name__ == '__main__':
    load_bronze('2025-01-01', '2025-12-31')
