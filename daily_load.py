#!/usr/bin/env python3
"""
Daily Load - Carga diaria automatizada del pipeline Medallion ETL.

Ejecuta el pipeline completo Bronze -> Silver -> Gold para el dia actual.
Diseñado para correr via crontab en el servidor.

Uso:
    python3 daily_load.py                # Usa fecha de hoy
    python3 daily_load.py 2025-06-15     # Usa fecha especifica

Logica de ventas:
    - Siempre recarga el mes actual completo
    - Si estamos en dia 1, 2 o 3: tambien recarga el mes anterior

Ejemplo crontab:
    0 5 * * * cd /srv/app/medallion-etl && /usr/bin/python3 daily_load.py >> /var/log/medallion-etl/daily.log 2>&1
"""
import sys
import time
from pathlib import Path
from datetime import date, timedelta
from dotenv import load_dotenv

# Cargar .env antes de cualquier otro import del proyecto
load_dotenv(Path(__file__).parent / '.env')

# Agregar src/ al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config import get_logger
from orchestrator import (
    bronze_masters, bronze_sales, bronze_stock,
    silver_masters, silver_sales, silver_stock,
    gold_dimensions, gold_fact_ventas, gold_fact_stock, gold_cobertura,
    get_month_range,
)

logger = get_logger('daily_load')


def run_phase(name: str, fn, *args, **kwargs) -> bool:
    """Ejecuta una fase del pipeline con logging de tiempos y manejo de errores."""
    logger.info(f"--- {name}: Iniciando ---")
    start = time.time()
    try:
        fn(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"--- {name}: OK ({elapsed:.1f}s) ---")
        return True
    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"--- {name}: ERROR ({elapsed:.1f}s) - {e} ---")
        return False


def main():
    # Determinar fecha de referencia
    if len(sys.argv) > 1:
        ref_date = date.fromisoformat(sys.argv[1])
    else:
        ref_date = date.today()

    day = ref_date.day
    current_month = f"{ref_date.year}-{ref_date.month:02d}"
    include_prev_month = day <= 3

    # Calcular rangos
    mes_actual_desde, mes_actual_hasta = get_month_range(current_month)

    if include_prev_month:
        prev = ref_date.replace(day=1) - timedelta(days=1)
        prev_month = f"{prev.year}-{prev.month:02d}"
        mes_anterior_desde, mes_anterior_hasta = get_month_range(prev_month)

    # Rango de stock: solo el dia de referencia
    stock_fecha = ref_date.isoformat()

    # Periodo cobertura (YYYY-MM)
    periodo_cobertura = current_month

    logger.info("=" * 60)
    logger.info(f"DAILY LOAD: Inicio - Fecha referencia: {ref_date.isoformat()}")
    logger.info(f"  Mes actual: {mes_actual_desde} - {mes_actual_hasta}")
    if include_prev_month:
        logger.info(f"  Mes anterior (dia <= 3): {mes_anterior_desde} - {mes_anterior_hasta}")
    logger.info("=" * 60)

    total_start = time.time()
    errors = []

    # FASE 1: BRONZE MASTERS
    if not run_phase("FASE 1: BRONZE MASTERS", bronze_masters):
        errors.append("BRONZE MASTERS")

    # FASE 2: BRONZE VENTAS
    if include_prev_month:
        if not run_phase("FASE 2a: BRONZE VENTAS (mes anterior)", bronze_sales, mes_anterior_desde, mes_anterior_hasta):
            errors.append("BRONZE VENTAS (mes anterior)")
    if not run_phase("FASE 2b: BRONZE VENTAS (mes actual)", bronze_sales, mes_actual_desde, mes_actual_hasta):
        errors.append("BRONZE VENTAS (mes actual)")

    # FASE 3: BRONZE STOCK
    if not run_phase("FASE 3: BRONZE STOCK", bronze_stock, stock_fecha, stock_fecha):
        errors.append("BRONZE STOCK")

    # FASE 4: SILVER MASTERS
    if not run_phase("FASE 4: SILVER MASTERS", silver_masters):
        errors.append("SILVER MASTERS")

    # FASE 5: SILVER VENTAS
    if include_prev_month:
        if not run_phase("FASE 5a: SILVER VENTAS (mes anterior)", silver_sales, mes_anterior_desde, mes_anterior_hasta):
            errors.append("SILVER VENTAS (mes anterior)")
    if not run_phase("FASE 5b: SILVER VENTAS (mes actual)", silver_sales, mes_actual_desde, mes_actual_hasta):
        errors.append("SILVER VENTAS (mes actual)")

    # FASE 6: SILVER STOCK
    if not run_phase("FASE 6: SILVER STOCK", silver_stock, stock_fecha, stock_fecha):
        errors.append("SILVER STOCK")

    # FASE 7: GOLD DIMENSIONES
    if not run_phase("FASE 7: GOLD DIMENSIONES", gold_dimensions):
        errors.append("GOLD DIMENSIONES")

    # FASE 8: GOLD FACT_VENTAS
    if include_prev_month:
        if not run_phase("FASE 8a: GOLD FACT_VENTAS (mes anterior)", gold_fact_ventas, mes_anterior_desde, mes_anterior_hasta):
            errors.append("GOLD FACT_VENTAS (mes anterior)")
    if not run_phase("FASE 8b: GOLD FACT_VENTAS (mes actual)", gold_fact_ventas, mes_actual_desde, mes_actual_hasta):
        errors.append("GOLD FACT_VENTAS (mes actual)")

    # FASE 9: GOLD FACT_STOCK
    if not run_phase("FASE 9: GOLD FACT_STOCK", gold_fact_stock, stock_fecha, stock_fecha):
        errors.append("GOLD FACT_STOCK")

    # FASE 10: GOLD COBERTURA
    if not run_phase("FASE 10: GOLD COBERTURA", gold_cobertura, periodo_cobertura):
        errors.append("GOLD COBERTURA")

    # Resumen
    total_elapsed = time.time() - total_start
    logger.info("=" * 60)
    logger.info(f"DAILY LOAD: Fin - Duracion total: {total_elapsed:.1f}s")
    if errors:
        logger.error(f"  Fases con error ({len(errors)}): {', '.join(errors)}")
        logger.info("=" * 60)
        sys.exit(1)
    else:
        logger.info("  Todas las fases completadas exitosamente")
        logger.info("=" * 60)
        sys.exit(0)


if __name__ == '__main__':
    main()
