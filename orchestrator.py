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
    python orchestrator.py bronze marketing

    # SILVER (orden recomendado por dependencias)
    python orchestrator.py silver branches           # 1. Sucursales
    python orchestrator.py silver sales_forces       # 2. Fuerzas de venta
    python orchestrator.py silver staff              # 3. Preventistas
    python orchestrator.py silver routes             # 4. Rutas
    python orchestrator.py silver clients            # 5. Clientes
    python orchestrator.py silver client_forces      # 6. Asignaciones cliente-ruta
    python orchestrator.py silver articles           # 7. Artículos
    python orchestrator.py silver article_groupings  # 8. Agrupaciones de artículos
    python orchestrator.py silver marketing          # 9. Marketing (segmentos, canales, subcanales)
    python orchestrator.py silver sales [fecha_desde] [fecha_hasta] [--full-refresh]

    # GOLD (orden recomendado)
    python orchestrator.py gold dim_tiempo [fecha_desde] [fecha_hasta]  # 1. Dimensión tiempo
    python orchestrator.py gold dim_sucursal                            # 2. Dimensión sucursal
    python orchestrator.py gold dim_vendedor                            # 3. Dimensión vendedor
    python orchestrator.py gold dim_articulo                            # 4. Dimensión artículo
    python orchestrator.py gold dim_cliente                             # 5. Dimensión cliente
    python orchestrator.py gold fact_ventas [--full-refresh]            # 6. Fact table ventas
    python orchestrator.py gold all                                     # Carga todo el esquema estrella

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


def bronze_marketing():
    """Ejecuta la carga de marketing en Bronze (full refresh)."""
    from layers.bronze import load_marketing
    print("=== BRONZE MARKETING: Cargando datos (full refresh) ===")
    load_marketing()
    print("=== BRONZE MARKETING: Completado ===\n")


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


def silver_clientes(full_refresh: bool = True):
    """Ejecuta la transformación de clientes a Silver (siempre full refresh)."""
    from layers.silver.transformers.clients_transformer import transform_clients

    print("=== SILVER CLIENTS: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_clients(full_refresh=full_refresh)
    print("=== SILVER CLIENTS: Completado ===\n")


def silver_articles(full_refresh: bool = True):
    """Ejecuta la transformación de artículos a Silver (siempre full refresh)."""
    from layers.silver.transformers.articles_transformer import transform_articles

    print("=== SILVER ARTICLES: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_articles(full_refresh=full_refresh)
    print("=== SILVER ARTICLES: Completado ===\n")


def silver_client_forces(full_refresh: bool = True):
    """Ejecuta la transformación de fuerzas de venta de clientes a Silver."""
    from layers.silver.transformers.client_forces_transformer import transform_client_forces

    print("=== SILVER CLIENT_FORCES: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_client_forces(full_refresh=full_refresh)
    print("=== SILVER CLIENT_FORCES: Completado ===\n")


def silver_branches(full_refresh: bool = True):
    """Ejecuta la transformación de sucursales a Silver."""
    from layers.silver.transformers.branches_transformer import transform_branches

    print("=== SILVER BRANCHES: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_branches(full_refresh=full_refresh)
    print("=== SILVER BRANCHES: Completado ===\n")


def silver_sales_forces(full_refresh: bool = True):
    """Ejecuta la transformación de fuerzas de venta a Silver."""
    from layers.silver.transformers.sales_forces_transformer import transform_sales_forces

    print("=== SILVER SALES_FORCES: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_sales_forces(full_refresh=full_refresh)
    print("=== SILVER SALES_FORCES: Completado ===\n")


def silver_staff(full_refresh: bool = True):
    """Ejecuta la transformación de personal/preventistas a Silver."""
    from layers.silver.transformers.staff_transformer import transform_staff

    print("=== SILVER STAFF: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_staff(full_refresh=full_refresh)
    print("=== SILVER STAFF: Completado ===\n")


def silver_routes(full_refresh: bool = True):
    """Ejecuta la transformación de rutas a Silver."""
    from layers.silver.transformers.routes_transformer import transform_routes

    print("=== SILVER ROUTES: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_routes(full_refresh=full_refresh)
    print("=== SILVER ROUTES: Completado ===\n")


def silver_article_groupings(full_refresh: bool = True):
    """Ejecuta la transformación de agrupaciones de artículos a Silver."""
    from layers.silver.transformers.article_groupings_transformer import transform_article_groupings

    print("=== SILVER ARTICLE_GROUPINGS: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_article_groupings(full_refresh=full_refresh)
    print("=== SILVER ARTICLE_GROUPINGS: Completado ===\n")


def silver_marketing(full_refresh: bool = True):
    """Ejecuta la transformación de marketing (segmentos, canales, subcanales) a Silver."""
    from layers.silver.transformers.marketing_transformer import transform_marketing

    print("=== SILVER MARKETING: Transformando datos ===")
    print("    Modo: Full Refresh")
    transform_marketing(full_refresh=full_refresh)
    print("=== SILVER MARKETING: Completado ===\n")


# ==========================================
# GOLD AGGREGATORS
# ==========================================

def gold_dim_tiempo(fecha_desde: str = '2020-01-01', fecha_hasta: str = '2030-12-31'):
    """Genera dimensión tiempo."""
    from layers.gold.aggregators import load_dim_tiempo
    print("=== GOLD DIM_TIEMPO: Generando dimensión ===")
    load_dim_tiempo(fecha_desde, fecha_hasta)
    print("=== GOLD DIM_TIEMPO: Completado ===\n")


def gold_dim_sucursal():
    """Carga dimensión sucursal."""
    from layers.gold.aggregators import load_dim_sucursal
    print("=== GOLD DIM_SUCURSAL: Cargando dimensión ===")
    load_dim_sucursal()
    print("=== GOLD DIM_SUCURSAL: Completado ===\n")


def gold_dim_vendedor():
    """Carga dimensión vendedor."""
    from layers.gold.aggregators import load_dim_vendedor
    print("=== GOLD DIM_VENDEDOR: Cargando dimensión ===")
    load_dim_vendedor()
    print("=== GOLD DIM_VENDEDOR: Completado ===\n")


def gold_dim_articulo():
    """Carga dimensión artículo."""
    from layers.gold.aggregators import load_dim_articulo
    print("=== GOLD DIM_ARTICULO: Cargando dimensión ===")
    load_dim_articulo()
    print("=== GOLD DIM_ARTICULO: Completado ===\n")


def gold_dim_cliente():
    """Carga dimensión cliente."""
    from layers.gold.aggregators import load_dim_cliente
    print("=== GOLD DIM_CLIENTE: Cargando dimensión ===")
    load_dim_cliente()
    print("=== GOLD DIM_CLIENTE: Completado ===\n")


def gold_fact_ventas(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """Carga fact table de ventas."""
    from layers.gold.aggregators import load_fact_ventas
    print("=== GOLD FACT_VENTAS: Cargando hechos ===")
    load_fact_ventas(fecha_desde, fecha_hasta, full_refresh)
    print("=== GOLD FACT_VENTAS: Completado ===\n")


def gold_all():
    """Carga todas las dimensiones y hechos en orden."""
    print("========== GOLD: Cargando esquema estrella completo ==========\n")
    gold_dim_tiempo()
    gold_dim_sucursal()
    gold_dim_vendedor()
    gold_dim_articulo()
    gold_dim_cliente()
    gold_fact_ventas(full_refresh=True)
    print("========== GOLD: Esquema estrella completado ==========\n")


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

        elif entidad == 'marketing':
            bronze_marketing()

        else:
            print(f"Error: Entidad '{entidad}' no reconocida para bronze")
            print("Entidades disponibles: sales, clientes, staff, routes, articles, stock, depositos, marketing")
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

        elif entidad in ('clientes', 'clients'):
            full_refresh = '--full-refresh' in sys.argv or True  # Siempre full refresh
            silver_clientes(full_refresh)

        elif entidad == 'articles':
            full_refresh = '--full-refresh' in sys.argv or True  # Siempre full refresh
            silver_articles(full_refresh)

        elif entidad == 'client_forces':
            full_refresh = '--full-refresh' in sys.argv or True  # Siempre full refresh
            silver_client_forces(full_refresh)

        elif entidad == 'branches':
            full_refresh = '--full-refresh' in sys.argv or True
            silver_branches(full_refresh)

        elif entidad == 'sales_forces':
            full_refresh = '--full-refresh' in sys.argv or True
            silver_sales_forces(full_refresh)

        elif entidad == 'staff':
            full_refresh = '--full-refresh' in sys.argv or True
            silver_staff(full_refresh)

        elif entidad == 'routes':
            full_refresh = '--full-refresh' in sys.argv or True
            silver_routes(full_refresh)

        elif entidad == 'article_groupings':
            full_refresh = '--full-refresh' in sys.argv or True
            silver_article_groupings(full_refresh)

        elif entidad == 'marketing':
            full_refresh = '--full-refresh' in sys.argv or True
            silver_marketing(full_refresh)

        else:
            print(f"Error: Entidad '{entidad}' no tiene transformer en silver")
            print("Entidades disponibles: sales, clients, articles, client_forces, branches, sales_forces, staff, routes, article_groupings, marketing")
            sys.exit(1)

    # ==========================================
    # GOLD
    # ==========================================
    elif capa == 'gold':
        if entidad == 'dim_tiempo':
            fecha_desde = sys.argv[3] if len(sys.argv) > 3 else '2020-01-01'
            fecha_hasta = sys.argv[4] if len(sys.argv) > 4 else '2030-12-31'
            gold_dim_tiempo(fecha_desde, fecha_hasta)

        elif entidad == 'dim_sucursal':
            gold_dim_sucursal()

        elif entidad == 'dim_vendedor':
            gold_dim_vendedor()

        elif entidad == 'dim_articulo':
            gold_dim_articulo()

        elif entidad == 'dim_cliente':
            gold_dim_cliente()

        elif entidad == 'fact_ventas':
            fecha_desde = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else ''
            fecha_hasta = sys.argv[4] if len(sys.argv) > 4 and not sys.argv[4].startswith('--') else ''
            full_refresh = '--full-refresh' in sys.argv
            gold_fact_ventas(fecha_desde, fecha_hasta, full_refresh)

        elif entidad == 'all':
            gold_all()

        else:
            print(f"Error: Entidad '{entidad}' no reconocida para gold")
            print("Entidades disponibles: dim_tiempo, dim_sucursal, dim_vendedor, dim_articulo, dim_cliente, fact_ventas, all")
            sys.exit(1)

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
