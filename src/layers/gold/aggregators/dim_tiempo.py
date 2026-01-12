"""
Generador de dimensión tiempo para Gold layer.
Genera fechas desde una fecha inicial hasta una fecha final.
"""
from database import engine
from datetime import datetime, timedelta
from config import get_logger

logger = get_logger(__name__)


def load_dim_tiempo(fecha_desde: str = '2020-01-01', fecha_hasta: str = '2030-12-31'):
    """
    Genera la dimensión tiempo con todas las fechas en el rango especificado.
    """
    start_time = datetime.now()
    logger.info(f"Generando dim_tiempo ({fecha_desde} a {fecha_hasta})...")

    # Nombres en español
    dias_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
             'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()

        # Full refresh
        logger.debug("Eliminando datos anteriores...")
        cursor.execute("DELETE FROM gold.dim_tiempo")

        # Generar fechas
        fecha_inicio = datetime.strptime(fecha_desde, '%Y-%m-%d')
        fecha_fin = datetime.strptime(fecha_hasta, '%Y-%m-%d')

        registros = []
        fecha_actual = fecha_inicio

        while fecha_actual <= fecha_fin:
            registro = (
                fecha_actual.date(),
                fecha_actual.day,
                fecha_actual.isoweekday(),  # 1=Lunes, 7=Domingo
                dias_semana[fecha_actual.weekday()],
                fecha_actual.isocalendar()[1],  # Semana ISO
                fecha_actual.month,
                meses[fecha_actual.month - 1],
                (fecha_actual.month - 1) // 3 + 1,  # Trimestre
                fecha_actual.year
            )
            registros.append(registro)
            fecha_actual += timedelta(days=1)

        logger.debug(f"Insertando {len(registros):,} fechas...")

        # Bulk insert
        from psycopg2.extras import execute_values
        insert_query = """
            INSERT INTO gold.dim_tiempo (
                fecha, dia, dia_semana, nombre_dia, semana, mes, nombre_mes, trimestre, anio
            ) VALUES %s
        """
        execute_values(cursor, insert_query, registros)

        raw_conn.commit()
        cursor.close()

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"dim_tiempo completado: {len(registros):,} fechas en {total_time:.2f}s")


if __name__ == '__main__':
    load_dim_tiempo()
