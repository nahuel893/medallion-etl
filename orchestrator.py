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

    # PARTIAL REFRESH (mes actual o específico)
    python orchestrator.py partial-refresh-sales           # Mes actual
    python orchestrator.py partial-refresh-sales 2025-01   # Mes específico
"""
import sys
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env antes de cualquier otro import del proyecto
load_dotenv(Path(__file__).parent / '.env')

# Agregar src/ al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config import get_logger

logger = get_logger('orchestrator')


# ==========================================
# BRONZE LOADERS
# ==========================================

def bronze_sales(fecha_desde: str, fecha_hasta: str):
    """Ejecuta la carga de ventas en Bronze."""
    from layers.bronze import load_bronze
    logger.info(f"BRONZE SALES: Iniciando carga ({fecha_desde} - {fecha_hasta})")
    load_bronze(fecha_desde, fecha_hasta)
    logger.info("BRONZE SALES: Completado")


def bronze_clientes():
    """Ejecuta la carga de clientes en Bronze (full refresh)."""
    from layers.bronze import load_clientes
    logger.info("BRONZE CLIENTES: Iniciando carga (full refresh)")
    load_clientes()
    logger.info("BRONZE CLIENTES: Completado")


def bronze_staff():
    """Ejecuta la carga de staff en Bronze (full refresh)."""
    from layers.bronze import load_staff
    logger.info("BRONZE STAFF: Iniciando carga (full refresh)")
    load_staff()
    logger.info("BRONZE STAFF: Completado")


def bronze_routes():
    """Ejecuta la carga de rutas en Bronze (full refresh)."""
    from layers.bronze import load_routes
    logger.info("BRONZE ROUTES: Iniciando carga (full refresh)")
    load_routes()
    logger.info("BRONZE ROUTES: Completado")


def bronze_articles():
    """Ejecuta la carga de artículos en Bronze (full refresh)."""
    from layers.bronze import load_articles
    logger.info("BRONZE ARTICLES: Iniciando carga (full refresh)")
    load_articles()
    logger.info("BRONZE ARTICLES: Completado")


def bronze_stock(fecha_desde: str, fecha_hasta: str):
    """Ejecuta la carga de stock en Bronze (append)."""
    from layers.bronze import load_stock
    logger.info(f"BRONZE STOCK: Iniciando carga ({fecha_desde} - {fecha_hasta})")
    load_stock(fecha_desde, fecha_hasta)
    logger.info("BRONZE STOCK: Completado")


def bronze_depositos():
    """Ejecuta la carga de depósitos en Bronze (full refresh)."""
    from layers.bronze import load_depositos
    logger.info("BRONZE DEPOSITOS: Iniciando carga desde CSV (full refresh)")
    load_depositos()
    logger.info("BRONZE DEPOSITOS: Completado")


def bronze_marketing():
    """Ejecuta la carga de marketing en Bronze (full refresh)."""
    from layers.bronze import load_marketing
    logger.info("BRONZE MARKETING: Iniciando carga (full refresh)")
    load_marketing()
    logger.info("BRONZE MARKETING: Completado")


# ==========================================
# SILVER TRANSFORMERS
# ==========================================

def silver_sales(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """Ejecuta la transformación de ventas a Silver."""
    from layers.silver.transformers.sales_transformer import transform_sales

    logger.info("SILVER SALES: Iniciando transformación")
    if full_refresh:
        logger.info("  Modo: Full Refresh")
        transform_sales(full_refresh=True)
    elif fecha_desde and fecha_hasta:
        logger.info(f"  Rango: {fecha_desde} - {fecha_hasta}")
        transform_sales(fecha_desde, fecha_hasta)
    else:
        logger.info("  Transformando todos los datos disponibles")
        transform_sales()
    logger.info("SILVER SALES: Completado")


def silver_clientes(full_refresh: bool = True):
    """Ejecuta la transformación de clientes a Silver (siempre full refresh)."""
    from layers.silver.transformers.clients_transformer import transform_clients

    logger.info("SILVER CLIENTS: Iniciando transformación (full refresh)")
    transform_clients(full_refresh=full_refresh)
    logger.info("SILVER CLIENTS: Completado")


def silver_articles(full_refresh: bool = True):
    """Ejecuta la transformación de artículos a Silver (siempre full refresh)."""
    from layers.silver.transformers.articles_transformer import transform_articles

    logger.info("SILVER ARTICLES: Iniciando transformación (full refresh)")
    transform_articles(full_refresh=full_refresh)
    logger.info("SILVER ARTICLES: Completado")


def silver_client_forces(full_refresh: bool = True):
    """Ejecuta la transformación de fuerzas de venta de clientes a Silver."""
    from layers.silver.transformers.client_forces_transformer import transform_client_forces

    logger.info("SILVER CLIENT_FORCES: Iniciando transformación (full refresh)")
    transform_client_forces(full_refresh=full_refresh)
    logger.info("SILVER CLIENT_FORCES: Completado")


def silver_branches(full_refresh: bool = True):
    """Ejecuta la transformación de sucursales a Silver."""
    from layers.silver.transformers.branches_transformer import transform_branches

    logger.info("SILVER BRANCHES: Iniciando transformación (full refresh)")
    transform_branches(full_refresh=full_refresh)
    logger.info("SILVER BRANCHES: Completado")


def silver_sales_forces(full_refresh: bool = True):
    """Ejecuta la transformación de fuerzas de venta a Silver."""
    from layers.silver.transformers.sales_forces_transformer import transform_sales_forces

    logger.info("SILVER SALES_FORCES: Iniciando transformación (full refresh)")
    transform_sales_forces(full_refresh=full_refresh)
    logger.info("SILVER SALES_FORCES: Completado")


def silver_staff(full_refresh: bool = True):
    """Ejecuta la transformación de personal/preventistas a Silver."""
    from layers.silver.transformers.staff_transformer import transform_staff

    logger.info("SILVER STAFF: Iniciando transformación (full refresh)")
    transform_staff(full_refresh=full_refresh)
    logger.info("SILVER STAFF: Completado")


def silver_routes(full_refresh: bool = True):
    """Ejecuta la transformación de rutas a Silver."""
    from layers.silver.transformers.routes_transformer import transform_routes

    logger.info("SILVER ROUTES: Iniciando transformación (full refresh)")
    transform_routes(full_refresh=full_refresh)
    logger.info("SILVER ROUTES: Completado")


def silver_article_groupings(full_refresh: bool = True):
    """Ejecuta la transformación de agrupaciones de artículos a Silver."""
    from layers.silver.transformers.article_groupings_transformer import transform_article_groupings

    logger.info("SILVER ARTICLE_GROUPINGS: Iniciando transformación (full refresh)")
    transform_article_groupings(full_refresh=full_refresh)
    logger.info("SILVER ARTICLE_GROUPINGS: Completado")


def silver_marketing(full_refresh: bool = True):
    """Ejecuta la transformación de marketing (segmentos, canales, subcanales) a Silver."""
    from layers.silver.transformers.marketing_transformer import transform_marketing

    logger.info("SILVER MARKETING: Iniciando transformación (full refresh)")
    transform_marketing(full_refresh=full_refresh)
    logger.info("SILVER MARKETING: Completado")


# ==========================================
# GOLD AGGREGATORS
# ==========================================

def gold_dim_tiempo(fecha_desde: str = '2020-01-01', fecha_hasta: str = '2030-12-31'):
    """Genera dimensión tiempo."""
    from layers.gold.aggregators import load_dim_tiempo
    logger.info(f"GOLD DIM_TIEMPO: Generando dimensión ({fecha_desde} - {fecha_hasta})")
    load_dim_tiempo(fecha_desde, fecha_hasta)
    logger.info("GOLD DIM_TIEMPO: Completado")


def gold_dim_sucursal():
    """Carga dimensión sucursal."""
    from layers.gold.aggregators import load_dim_sucursal
    logger.info("GOLD DIM_SUCURSAL: Cargando dimensión")
    load_dim_sucursal()
    logger.info("GOLD DIM_SUCURSAL: Completado")


def gold_dim_vendedor():
    """Carga dimensión vendedor."""
    from layers.gold.aggregators import load_dim_vendedor
    logger.info("GOLD DIM_VENDEDOR: Cargando dimensión")
    load_dim_vendedor()
    logger.info("GOLD DIM_VENDEDOR: Completado")


def gold_dim_articulo():
    """Carga dimensión artículo."""
    from layers.gold.aggregators import load_dim_articulo
    logger.info("GOLD DIM_ARTICULO: Cargando dimensión")
    load_dim_articulo()
    logger.info("GOLD DIM_ARTICULO: Completado")


def gold_dim_cliente():
    """Carga dimensión cliente."""
    from layers.gold.aggregators import load_dim_cliente
    logger.info("GOLD DIM_CLIENTE: Cargando dimensión")
    load_dim_cliente()
    logger.info("GOLD DIM_CLIENTE: Completado")


def gold_fact_ventas(fecha_desde: str = '', fecha_hasta: str = '', full_refresh: bool = False):
    """Carga fact table de ventas."""
    from layers.gold.aggregators import load_fact_ventas
    logger.info("GOLD FACT_VENTAS: Cargando hechos")
    load_fact_ventas(fecha_desde, fecha_hasta, full_refresh)
    logger.info("GOLD FACT_VENTAS: Completado")


def gold_all():
    """Carga todas las dimensiones y hechos en orden."""
    logger.info("GOLD: Iniciando carga de esquema estrella completo")
    gold_dim_tiempo()
    gold_dim_sucursal()
    gold_dim_vendedor()
    gold_dim_articulo()
    gold_dim_cliente()
    gold_fact_ventas(full_refresh=True)
    logger.info("GOLD: Esquema estrella completado")


# ==========================================
# PIPELINE COMPLETO
# ==========================================

def run_all_sales(fecha_desde: str, fecha_hasta: str):
    """Ejecuta el pipeline completo para ventas: Bronze -> Silver -> Gold."""
    logger.info(f"PIPELINE MEDALLION: Iniciando pipeline completo ({fecha_desde} - {fecha_hasta})")
    bronze_sales(fecha_desde, fecha_hasta)
    silver_sales(fecha_desde, fecha_hasta)
    gold_fact_ventas(fecha_desde, fecha_hasta)
    logger.info("PIPELINE MEDALLION: Pipeline completado (Bronze -> Silver -> Gold)")


# ==========================================
# PARTIAL REFRESH
# ==========================================

def partial_refresh_sales(mes: str = None):
    """
    Ejecuta partial refresh de ventas para el mes indicado.
    Borra y recarga el mes completo en Bronze -> Silver -> Gold.

    Args:
        mes: Formato 'YYYY-MM' o None para mes actual

    Ejemplos:
        partial_refresh_sales()           # Mes actual
        partial_refresh_sales('2025-01')  # Enero 2025
    """
    fecha_desde, fecha_hasta = get_month_range(mes)
    mes_display = mes or "actual"

    logger.info(f"PARTIAL REFRESH SALES: Iniciando para mes {mes_display} ({fecha_desde} - {fecha_hasta})")

    # Bronze: Borrar y recargar desde API
    logger.info("  [1/3] Bronze: Extrayendo desde API...")
    bronze_sales(fecha_desde, fecha_hasta)

    # Silver: Transformar el rango
    logger.info("  [2/3] Silver: Transformando datos...")
    silver_sales(fecha_desde, fecha_hasta)

    # Gold: Recargar fact_ventas para el rango
    logger.info("  [3/3] Gold: Cargando fact_ventas...")
    gold_fact_ventas(fecha_desde, fecha_hasta)

    logger.info(f"PARTIAL REFRESH SALES: Completado para mes {mes_display}")


# ==========================================
# UTILIDADES
# ==========================================

def get_month_range(mes: str = None) -> tuple[str, str]:
    """
    Retorna (primer_dia, ultimo_dia) del mes en formato ISO.

    Args:
        mes: Formato 'YYYY-MM' o None para mes actual

    Returns:
        Tupla (fecha_desde, fecha_hasta) en formato 'YYYY-MM-DD'

    Ejemplos:
        get_month_range()           → ('2025-01-01', '2025-01-31')
        get_month_range('2024-12')  → ('2024-12-01', '2024-12-31')
    """
    from datetime import date
    from calendar import monthrange

    if mes:
        year, month = map(int, mes.split('-'))
    else:
        today = date.today()
        year, month = today.year, today.month

    primer_dia = date(year, month, 1)
    ultimo_dia = date(year, month, monthrange(year, month)[1])

    return primer_dia.isoformat(), ultimo_dia.isoformat()


def print_usage():
    """Muestra el uso del script."""
    print(__doc__)
    sys.exit(1)


# ==========================================
# MAIN
# ==========================================

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage()

    capa = sys.argv[1].lower()

    # partial-refresh-sales no requiere entidad
    if capa == 'partial-refresh-sales':
        mes = sys.argv[2] if len(sys.argv) >= 3 and not sys.argv[2].startswith('--') else None
        partial_refresh_sales(mes)
        sys.exit(0)

    # Otros comandos requieren entidad
    if len(sys.argv) < 3:
        print_usage()

    entidad = sys.argv[2].lower()

    # ==========================================
    # BRONZE
    # ==========================================
    if capa == 'bronze':
        if entidad == 'sales':
            if len(sys.argv) < 5:
                logger.error("bronze sales requiere <fecha_desde> <fecha_hasta>")
                logger.error("Ejemplo: python orchestrator.py bronze sales 2025-12-01 2025-12-31")
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
                logger.error("bronze stock requiere <fecha_desde> <fecha_hasta>")
                logger.error("Ejemplo: python orchestrator.py bronze stock 2025-12-01 2025-12-31")
                sys.exit(1)
            bronze_stock(sys.argv[3], sys.argv[4])

        elif entidad == 'depositos':
            bronze_depositos()

        elif entidad == 'marketing':
            bronze_marketing()

        else:
            logger.error(f"Entidad '{entidad}' no reconocida para bronze")
            logger.error("Entidades disponibles: sales, clientes, staff, routes, articles, stock, depositos, marketing")
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
            logger.error(f"Entidad '{entidad}' no tiene transformer en silver")
            logger.error("Entidades disponibles: sales, clients, articles, client_forces, branches, sales_forces, staff, routes, article_groupings, marketing")
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
            logger.error(f"Entidad '{entidad}' no reconocida para gold")
            logger.error("Entidades disponibles: dim_tiempo, dim_sucursal, dim_vendedor, dim_articulo, dim_cliente, fact_ventas, all")
            sys.exit(1)

    # ==========================================
    # ALL (Pipeline completo)
    # ==========================================
    elif capa == 'all':
        if entidad == 'sales':
            if len(sys.argv) < 5:
                logger.error("all sales requiere <fecha_desde> <fecha_hasta>")
                logger.error("Ejemplo: python orchestrator.py all sales 2025-12-01 2025-12-31")
                sys.exit(1)
            run_all_sales(sys.argv[3], sys.argv[4])
        else:
            logger.error(f"Pipeline completo solo disponible para 'sales' por ahora")
            sys.exit(1)

    else:
        logger.error(f"Capa '{capa}' no reconocida")
        logger.error("Capas disponibles: bronze, silver, gold, all, partial-refresh-sales")
        sys.exit(1)
