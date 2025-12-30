#!/usr/bin/env python3

"""
Orchestrator - Punto de entrada único para el pipeline ETL Medallion. Uso:
    python orchestrator.py sales <fecha_desde> <fecha_hasta>
    python orchestrator.py clientes
    python orchestrator.py silver
    python orchestrator.py gold
    python orchestrator.py all <fecha_desde> <fecha_hasta>
"""
import sys
from pathlib import Path

# Agregar src/ al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))


def run_sales(fecha_desde: str, fecha_hasta: str):
    """Ejecuta la carga de ventas en Bronze."""
    from layers.bronze import load_bronze
    print(f"=== BRONZE SALES: Cargando datos ({fecha_desde} - {fecha_hasta}) ===")
    load_bronze(fecha_desde, fecha_hasta)
    print("=== BRONZE SALES: Completado ===\n")


def run_clientes():
    """Ejecuta la carga de clientes en Bronze (full refresh)."""
    from layers.bronze import  load_clientes
    print("=== BRONZE CLIENTES: Cargando datos (full refresh) ===")
    load_clientes()
    print("=== BRONZE CLIENTES: Completado ===\n")

def run_staff():
    from layers.bronze import load_staff
    print("=== BRONZE STAFF: Cargando datos (full refresh) ===")
    load_staff()
    print("=== BRONZE STAFF: Completado ===\n")


def run_routes():
    from layers.bronze import load_routes
    print("=== BRONZE ROUTES: Cargando datos (full refresh) ===")
    load_routes()
    print("=== BRONZE ROUTES: Completado ===\n")


def run_articles():
    from layers.bronze import load_articles
    print("=== BRONZE ARTICLES: Cargando datos (full refresh) ===")
    load_articles()
    print("=== BRONZE ARTICLES: Completado ===\n")


def run_stock(fecha_desde: str, fecha_hasta: str):
    from layers.bronze import load_stock
    print(f"=== BRONZE STOCK: Cargando datos ({fecha_desde} - {fecha_hasta}) (append) ===")
    load_stock(fecha_desde, fecha_hasta)
    print("=== BRONZE STOCK: Completado ===\n")


def run_depositos():
    from layers.bronze import load_depositos
    print("=== BRONZE DEPOSITOS: Cargando datos desde CSV (full refresh) ===")
    load_depositos()
    print("=== BRONZE DEPOSITOS: Completado ===\n")


def run_silver():
    """Ejecuta las transformaciones de la capa Silver."""
    # TODO: Implementar cuando exista la capa silver
    print("=== SILVER: Transformando datos ===")
    print("    [No implementado aún]")
    print("=== SILVER: Completado ===\n")


def run_gold():
    """Ejecuta las agregaciones de la capa Gold."""
    # TODO: Implementar cuando exista la capa gold
    print("=== GOLD: Agregando datos ===")
    print("    [No implementado aún]")
    print("=== GOLD: Completado ===\n")


def run_all(fecha_desde: str, fecha_hasta: str):
    """Ejecuta el pipeline completo: Bronze -> Silver -> Gold."""
    print("========== PIPELINE MEDALLION ==========\n")
    run_sales(fecha_desde, fecha_hasta)
    run_clientes()
    run_silver()
    run_gold()
    print("========== PIPELINE COMPLETADO ==========")


def print_usage():
    """Muestra el uso del script."""
    print(__doc__)
    sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage()

    command = sys.argv[1].lower()

    if command == 'sales':
        if len(sys.argv) < 4:
            print("Error: sales requiere <fecha_desde> <fecha_hasta>")
            print("Ejemplo: python orchestrator.py sales 2025-12-01 2025-12-31")
            sys.exit(1)
        run_sales(sys.argv[2], sys.argv[3])

    elif command == 'staff':
        run_staff()

    elif command == 'routes':
        run_routes()

    elif command == 'articles':
        run_articles()

    elif command == 'stock':
        if len(sys.argv) < 4:
            print("Error: stock requiere <fecha_desde> <fecha_hasta>")
            print("Ejemplo: python orchestrator.py stock 2025-12-01 2025-12-31")
            sys.exit(1)
        run_stock(sys.argv[2], sys.argv[3])

    elif command == 'clientes':
        run_clientes()

    elif command == 'depositos':
        run_depositos()

    elif command == 'silver':
        run_silver()

    elif command == 'gold':
        run_gold()

    elif command == 'all':
        if len(sys.argv) < 4:
            print("Error: all requiere <fecha_desde> <fecha_hasta>")
            print("Ejemplo: python orchestrator.py all 2025-12-01 2025-12-31")
            sys.exit(1)
        run_all(sys.argv[2], sys.argv[3])

    else:
        print(f"Error: Comando '{command}' no reconocido")
        print_usage()
