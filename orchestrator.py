#!/usr/bin/env python3

"""
Orchestrator - Punto de entrada único para el pipeline ETL Medallion.

Uso:
    python orchestrator.py <capa> <entidad> [args...]

Ejemplos:
    # BRONZE
    python orchestrator.py bronze sales 2025-01-01 2025-12-31
    python orchestrator.py bronze clientes
    python orchestrator.py bronze staff
    python orchestrator.py bronze routes
    python orchestrator.py bronze articles
    python orchestrator.py bronze stock 2025-01-01 2025-12-31
    python orchestrator.py bronze depositos

    # SILVER
    python orchestrator.py silver sales [fecha_desde] [fecha_hasta] [--full-refresh]
    python orchestrator.py silver clientes [--full-refresh]

    # GOLD
    python orchestrator.py gold <entidad>

    # ALL (pipeline completo)
    python orchestrator.py all sales 2025-01-01 2025-12-31
"""
import sys
from pathlib import Path

# Agregar src/ al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))


# ==========================================
# BRONZE LOADERS
# ==========================================

def bronze_sales(fecha_desde: str, fecha_hasta: str):
    """Ejecuta la carga de ventas en Bronze."""
    from layers.bronze import load_bronze
    print(f"=== BRONZE SALES: Cargando datos ({fecha_desde} - {fecha_hasta}) ===")
    load_bronze(fecha_desde, fecha_hasta)
    print("=== BRONZE SALES: Completado ===\n")


def bronze_clientes():
    """Ejecuta la carga de clientes en Bronze (full refresh)."""
    from layers.bronze import load_clientes
    print("=== BRONZE CLIENTES: Cargando datos (full refresh) ===")
    load_clientes()
    print("=== BRONZE CLIENTES: Completado ===\n")


def bronze_staff():
    """Ejecuta la carga de staff en Bronze (full refresh)."""
    from layers.bronze import load_staff
    print("=== BRONZE STAFF: Cargando datos (full refresh) ===")
    load_staff()
    print("=== BRONZE STAFF: Completado ===\n")


def bronze_routes():
    """Ejecuta la carga de rutas en Bronze (full refresh)."""
    from layers.bronze import load_routes
    print("=== BRONZE ROUTES: Cargando datos (full refresh) ===")
    load_routes()
    print("=== BRONZE ROUTES: Completado ===\n")


def bronze_articles():
    """Ejecuta la carga de artículos en Bronze (full refresh)."""
    from layers.bronze import load_articles
    print("=== BRONZE ARTICLES: Cargando datos (full refresh) ===")
    load_articles()
    print("=== BRONZE ARTICLES: Completado ===\n")


def bronze_stock(fecha_desde: str, fecha_hasta: str):
    """Ejecuta la carga de stock en Bronze (append)."""
    from layers.bronze import load_stock
    print(f"=== BRONZE STOCK: Cargando datos ({fecha_desde} - {fecha_hasta}) (append) ===")
    load_stock(fecha_desde, fecha_hasta)
    print("=== BRONZE STOCK: Completado ===\n")


def bronze_depositos():
    """Ejecuta la carga de depósitos en Bronze (full refresh)."""
    from layers.bronze import load_depositos
    print("=== BRONZE DEPOSITOS: Cargando datos desde CSV (full refresh) ===")
    load_depositos()
    print("=== BRONZE DEPOSITOS: Completado ===\n")


# ==========================================
# SILVER TRANSFORMERS
# ==========================================

def silver_sales(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """Ejecuta la transformación de ventas a Silver."""
    from layers.silver.transformers.sales_transformer import transform_sales

    print("=== SILVER SALES: Transformando datos ===")
    if full_refresh:
        print("    Modo: Full Refresh")
        transform_sales(full_refresh=True)
    elif fecha_desde and fecha_hasta:
        print(f"    Rango: {fecha_desde} - {fecha_hasta}")
        transform_sales(fecha_desde, fecha_hasta)
    else:
        print("    Transformando todos los datos disponibles")
        transform_sales()
    print("=== SILVER SALES: Completado ===\n")


def silver_clientes(full_refresh: bool = False):
    """Ejecuta la transformación de clientes a Silver."""
    print("=== SILVER CLIENTES: Transformando datos ===")
    print("    [No implementado aún]")
    print("=== SILVER CLIENTES: Completado ===\n")


# ==========================================
# GOLD AGGREGATORS
# ==========================================

def gold_placeholder(entidad: str):
    """Placeholder para agregaciones Gold."""
    print(f"=== GOLD {entidad.upper()}: Agregando datos ===")
    print("    [No implementado aún]")
    print(f"=== GOLD {entidad.upper()}: Completado ===\n")


# ==========================================
# PIPELINE COMPLETO
# ==========================================

def run_all_sales(fecha_desde: str, fecha_hasta: str):
    """Ejecuta el pipeline completo para ventas: Bronze -> Silver -> Gold."""
    print("========== PIPELINE MEDALLION: SALES ==========\n")
    bronze_sales(fecha_desde, fecha_hasta)
    silver_sales(fecha_desde, fecha_hasta)
    gold_placeholder('sales')
    print("========== PIPELINE COMPLETADO ==========")


# ==========================================
# UTILIDADES
# ==========================================

def print_usage():
    """Muestra el uso del script."""
    print(__doc__)
    sys.exit(1)


# ==========================================
# MAIN
# ==========================================

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print_usage()

    capa = sys.argv[1].lower()
    entidad = sys.argv[2].lower()

    # ==========================================
    # BRONZE
    # ==========================================
    if capa == 'bronze':
        if entidad == 'sales':
            if len(sys.argv) < 5:
                print("Error: bronze sales requiere <fecha_desde> <fecha_hasta>")
                print("Ejemplo: python orchestrator.py bronze sales 2025-12-01 2025-12-31")
                sys.exit(1)
            bronze_sales(sys.argv[3], sys.argv[4])

        elif entidad == 'clientes':
            bronze_clientes()

        elif entidad == 'staff':
            bronze_staff()

        elif entidad == 'routes':
            bronze_routes()

        elif entidad == 'articles':
            bronze_articles()

        elif entidad == 'stock':
            if len(sys.argv) < 5:
                print("Error: bronze stock requiere <fecha_desde> <fecha_hasta>")
                print("Ejemplo: python orchestrator.py bronze stock 2025-12-01 2025-12-31")
                sys.exit(1)
            bronze_stock(sys.argv[3], sys.argv[4])

        elif entidad == 'depositos':
            bronze_depositos()

        else:
            print(f"Error: Entidad '{entidad}' no reconocida para bronze")
            print("Entidades disponibles: sales, clientes, staff, routes, articles, stock, depositos")
            sys.exit(1)

    # ==========================================
    # SILVER
    # ==========================================
    elif capa == 'silver':
        if entidad == 'sales':
            fecha_desde = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else ''
            fecha_hasta = sys.argv[4] if len(sys.argv) > 4 and not sys.argv[4].startswith('--') else ''
            full_refresh = '--full-refresh' in sys.argv
            silver_sales(fecha_desde, fecha_hasta, full_refresh)

        elif entidad == 'clientes':
            full_refresh = '--full-refresh' in sys.argv
            silver_clientes(full_refresh)

        else:
            print(f"Error: Entidad '{entidad}' no tiene transformer en silver")
            print("Entidades disponibles: sales, clientes")
            sys.exit(1)

    # ==========================================
    # GOLD
    # ==========================================
    elif capa == 'gold':
        gold_placeholder(entidad)

    # ==========================================
    # ALL (Pipeline completo)
    # ==========================================
    elif capa == 'all':
        if entidad == 'sales':
            if len(sys.argv) < 5:
                print("Error: all sales requiere <fecha_desde> <fecha_hasta>")
                print("Ejemplo: python orchestrator.py all sales 2025-12-01 2025-12-31")
                sys.exit(1)
            run_all_sales(sys.argv[3], sys.argv[4])
        else:
            print(f"Error: Pipeline completo solo disponible para 'sales' por ahora")
            sys.exit(1)

    else:
        print(f"Error: Capa '{capa}' no reconocida")
        print("Capas disponibles: bronze, silver, gold, all")
        sys.exit(1)
