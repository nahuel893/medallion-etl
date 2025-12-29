import json
from sqlalchemy import text

# Local imports
from chesserp.client import ChessClient
from data_base import engine

def load_bronze(fecha_desde: str, fecha_hasta: str):
    # Obtener el cliente Chess
    client = ChessClient.from_env(prefix="EMPRESA1_")

    # Obtener datos de ventas
    sales = client.get_sales(
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        detallado=True,
        empresas='1',
        raw=True
    )

    print(f"Obtenidos {len(sales)} registros")

    # Inserción batch - mucho más eficiente para grandes volúmenes

    insert_sql = text("""
        INSERT INTO bronze.raw_sales (data_raw, source_system)
        VALUES (CAST(:data_raw AS jsonb), :source_system)
    """)

    # Preparar todos los registros para inserción batch
    records = [
        {'data_raw': json.dumps(sale), 'source_system': 'API_CHESS_ERP'}
        for sale in sales
    ]

    with engine.begin() as conn:
        conn.execute(insert_sql, records)
        print(f"Insertados {len(sales)} registros en bronze.raw_sales")
