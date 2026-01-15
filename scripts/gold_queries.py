#!/usr/bin/env python3
"""
Script de consultas de práctica para Gold Layer.
Ejecuta consultas analíticas sobre el esquema estrella.

Uso:
    python scripts/gold_queries.py [numero_query]
    python scripts/gold_queries.py         # Muestra menú
    python scripts/gold_queries.py 1       # Ejecuta query 1
    python scripts/gold_queries.py all     # Ejecuta todas
"""
import sys
from pathlib import Path

# Agregar src/ al path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import engine
from tabulate import tabulate
from decimal import Decimal

# ==========================================
# QUERIES DE PRÁCTICA
# ==========================================

QUERIES = {
    "1": {
        "nombre": "Ventas por mes",
        "descripcion": "Total de ventas agrupado por año y mes",
        "sql": """
            SELECT
                t.anio,
                t.nombre_mes,
                COUNT(*) as lineas,
                ROUND(SUM(f.cantidades_total)::numeric, 2) as total_ventas
            FROM gold.fact_ventas f
            JOIN gold.dim_tiempo t ON f.fecha_comprobante = t.fecha
            WHERE f.anulado = FALSE
            GROUP BY t.anio, t.mes, t.nombre_mes
            ORDER BY t.anio DESC, t.mes DESC
            LIMIT 12
        """
    },
    "2": {
        "nombre": "Ventas por sucursal",
        "descripcion": "Ranking de sucursales por facturación",
        "sql": """
            SELECT
                s.id_sucursal,
                COALESCE(s.descripcion, 'Sin descripción') as sucursal,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as subtotal_final,
                ROUND(SUM(f.cantidades_total)::numeric, 0) as bultos 
            FROM gold.fact_ventas f
            JOIN gold.dim_sucursal s ON f.id_sucursal = s.id_sucursal
            WHERE f.anulado = FALSE
            GROUP BY s.id_sucursal, s.descripcion
            ORDER BY subtotal_final DESC
        """
    },
    "3": {
        "nombre": "Top 10 productos",
        "descripcion": "Productos más vendidos en unidades",
        "sql": """
            SELECT
                a.id_articulo,
                LEFT(a.des_articulo, 40) as articulo,
                COALESCE(a.marca, '-') as marca,
                ROUND(SUM(f.cantidades_total)::numeric, 0) as unidades,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as facturacion
            FROM gold.fact_ventas f
            JOIN gold.dim_articulo a ON f.id_articulo = a.id_articulo
            WHERE f.anulado = FALSE
            GROUP BY a.id_articulo, a.des_articulo, a.marca
            ORDER BY unidades DESC
            LIMIT 10
        """
    },
    "4": {
        "nombre": "Top 10 clientes",
        "descripcion": "Clientes con mayor facturación",
        "sql": """
            SELECT
                c.id_cliente,
                LEFT(c.razon_social, 35) as cliente,
                COALESCE(c.des_subcanal_mkt, '-') as subcanal,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as total_compras
            FROM gold.fact_ventas f
            JOIN gold.dim_cliente c ON f.id_cliente = c.id_cliente
            WHERE f.anulado = FALSE
            GROUP BY c.id_cliente, c.razon_social, c.des_subcanal_mkt
            ORDER BY total_compras DESC
            LIMIT 10
        """
    },
    "5": {
        "nombre": "Ventas por canal/segmento",
        "descripcion": "Análisis por segmentación de marketing",
        "sql": """
            SELECT
                COALESCE(c.des_segmento_mkt, 'Sin segmento') as segmento,
                COALESCE(c.des_canal_mkt, 'Sin canal') as canal,
                COUNT(DISTINCT f.id_cliente) as clientes,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as total_ventas
            FROM gold.fact_ventas f
            JOIN gold.dim_cliente c ON f.id_cliente = c.id_cliente
            WHERE f.anulado = FALSE
            GROUP BY c.des_segmento_mkt, c.des_canal_mkt
            ORDER BY total_ventas DESC
        """
    },
    "6": {
        "nombre": "Ventas por marca",
        "descripcion": "Facturación por marca de producto",
        "sql": """
            SELECT
                COALESCE(a.marca, 'Sin marca') as marca,
                COUNT(DISTINCT a.id_articulo) as productos,
                ROUND(SUM(f.cantidades_total)::numeric, 0) as unidades,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as facturacion
            FROM gold.fact_ventas f
            JOIN gold.dim_articulo a ON f.id_articulo = a.id_articulo
            WHERE f.anulado = FALSE
            GROUP BY a.marca
            ORDER BY facturacion DESC
            LIMIT 15
        """
    },
    "7": {
        "nombre": "Comparativa mensual",
        "descripcion": "Variación porcentual mes a mes",
        "sql": """
            WITH ventas_mes AS (
                SELECT
                    t.anio,
                    t.mes,
                    t.nombre_mes,
                    SUM(f.subtotal_final) as total
                FROM gold.fact_ventas f
                JOIN gold.dim_tiempo t ON f.fecha_comprobante = t.fecha
                WHERE f.anulado = FALSE
                GROUP BY t.anio, t.mes, t.nombre_mes
            )
            SELECT
                anio,
                nombre_mes as mes,
                ROUND(total::numeric, 2) as total,
                ROUND(LAG(total) OVER (ORDER BY anio, mes)::numeric, 2) as mes_anterior,
                ROUND((total - LAG(total) OVER (ORDER BY anio, mes)) /
                      NULLIF(LAG(total) OVER (ORDER BY anio, mes), 0) * 100, 1) as var_pct
            FROM ventas_mes
            ORDER BY anio DESC, mes DESC
            LIMIT 12
        """
    },
    "8": {
        "nombre": "Ventas por día de semana",
        "descripcion": "Análisis de patrones semanales",
        "sql": """
            SELECT
                t.dia_semana,
                t.nombre_dia,
                COUNT(*) as transacciones,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as total_ventas,
                ROUND(AVG(f.subtotal_final)::numeric, 2) as ticket_promedio
            FROM gold.fact_ventas f
            JOIN gold.dim_tiempo t ON f.fecha_comprobante = t.fecha
            WHERE f.anulado = FALSE
            GROUP BY t.dia_semana, t.nombre_dia
            ORDER BY t.dia_semana
        """
    },
    "9": {
        "nombre": "Performance vendedores",
        "descripcion": "Ranking de vendedores por facturación",
        "sql": """
            SELECT
                v.id_vendedor,
                LEFT(v.des_vendedor, 31) as vendedor,
                COALESCE(v.des_sucursal, '-') as sucursal,
                COUNT(DISTINCT f.id_cliente) as clientes,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as total_ventas
            FROM gold.fact_ventas f
            JOIN gold.dim_vendedor v ON f.id_vendedor = v.id_vendedor
            WHERE f.anulado = FALSE
            GROUP BY v.id_vendedor, v.des_vendedor, v.des_sucursal
            ORDER BY total_ventas DESC
            LIMIT 15
        """
    },
    "10": {
        "nombre": "Vendedores por fuerza de venta",
        "descripcion": "Resumen por fuerza de venta",
        "sql": """
            SELECT
                v.id_fuerza_ventas as fv,
                COUNT(DISTINCT v.id_vendedor) as vendedores,
                COUNT(DISTINCT f.id_cliente) as clientes,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as total_ventas,
                ROUND(SUM(f.subtotal_final)::numeric / NULLIF(COUNT(DISTINCT v.id_vendedor), 0), 2) as prom_vendedor
            FROM gold.fact_ventas f
            JOIN gold.dim_vendedor v ON f.id_vendedor = v.id_vendedor
            WHERE f.anulado = FALSE
            GROUP BY v.id_fuerza_ventas
            ORDER BY total_ventas DESC
        """
    },
    "11": {
        "nombre": "KPIs Dashboard",
        "descripcion": "Métricas generales del negocio",
        "sql": """
            SELECT
                COUNT(DISTINCT f.id_cliente) as clientes_activos,
                COUNT(DISTINCT f.id_articulo) as productos_vendidos,
                COUNT(DISTINCT CONCAT(f.id_documento, f.serie, f.nro_doc)) as documentos,
                ROUND(SUM(f.cantidades_total)::numeric, 0) as unidades_totales,
                ROUND(SUM(f.subtotal_final)::numeric, 2) as facturacion_total,
                ROUND(AVG(f.subtotal_final)::numeric, 2) as ticket_promedio
            FROM gold.fact_ventas f
            WHERE f.anulado = FALSE
        """
    },
    "12": {
        "nombre": "Matriz Sucursal x Trimestre",
        "descripcion": "Ventas cruzadas por sucursal y trimestre",
        "sql": """
            SELECT
                COALESCE(s.descripcion, 'Suc ' || s.id_sucursal::text) as sucursal,
                ROUND(SUM(CASE WHEN t.trimestre = 1 THEN f.subtotal_final ELSE 0 END)::numeric, 0) as q1,
                ROUND(SUM(CASE WHEN t.trimestre = 2 THEN f.subtotal_final ELSE 0 END)::numeric, 0) as q2,
                ROUND(SUM(CASE WHEN t.trimestre = 3 THEN f.subtotal_final ELSE 0 END)::numeric, 0) as q3,
                ROUND(SUM(CASE WHEN t.trimestre = 4 THEN f.subtotal_final ELSE 0 END)::numeric, 0) as q4,
                ROUND(SUM(f.subtotal_final)::numeric, 0) as total
            FROM gold.fact_ventas f
            JOIN gold.dim_sucursal s ON f.id_sucursal = s.id_sucursal
            JOIN gold.dim_tiempo t ON f.fecha_comprobante = t.fecha
            WHERE f.anulado = FALSE AND t.anio = EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY s.id_sucursal, s.descripcion
            ORDER BY total DESC
        """
    },
}


def format_value(val):
    """Formatea valores para mostrar números grandes correctamente."""
    if val is None:
        return '-'
    if isinstance(val, (Decimal, float)):
        # Mostrar número completo sin notación científica
        num = float(val)
        if num == int(num):
            return str(int(num))
        else:
            return f"{num:.2f}"
    return val


def ejecutar_query(key: str):
    """Ejecuta una query y muestra los resultados."""
    if key not in QUERIES:
        print(f"Query '{key}' no encontrada")
        return

    q = QUERIES[key]
    print(f"\n{'='*60}")
    print(f"[{key}] {q['nombre']}")
    print(f"    {q['descripcion']}")
    print('='*60)

    with engine.connect() as conn:
        raw_conn = conn.connection.dbapi_connection
        cursor = raw_conn.cursor()
        cursor.execute(q['sql'])

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        if rows:
            # Formatear valores numéricos
            formatted_rows = [
                tuple(format_value(val) for val in row)
                for row in rows
            ]
            print(tabulate(formatted_rows, headers=columns, tablefmt='simple',
                          stralign='right', disable_numparse=True))
            print(f"\n({len(rows)} filas)")
        else:
            print("Sin resultados")


def mostrar_menu():
    """Muestra el menú de queries disponibles."""
    print("\n" + "="*60)
    print("QUERIES DE PRÁCTICA - GOLD LAYER")
    print("="*60)

    for key, q in QUERIES.items():
        print(f"  [{key:>2}] {q['nombre']:<30} - {q['descripcion']}")

    print(f"\n  [all] Ejecutar todas las queries")
    print("\nUso: python scripts/gold_queries.py <numero>")
    print("="*60)


def main():
    if len(sys.argv) < 2:
        mostrar_menu()
        return

    arg = sys.argv[1].lower()

    if arg == 'all':
        for key in QUERIES:
            ejecutar_query(key)
            print()
    elif arg in QUERIES:
        ejecutar_query(arg)
    else:
        print(f"Query '{arg}' no reconocida")
        mostrar_menu()


if __name__ == '__main__':
    main()
