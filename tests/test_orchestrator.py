"""
Tests para funciones del orchestrator.
Extiende test_utils.py con tests de funciones de orquestación.
"""
import pytest
from unittest.mock import patch, MagicMock, call


class TestGoldDimensions:
    """Tests para gold_dimensions() - orden de ejecución."""

    @patch('orchestrator.gold_dim_cliente')
    @patch('orchestrator.gold_dim_articulo')
    @patch('orchestrator.gold_dim_vendedor')
    @patch('orchestrator.gold_dim_deposito')
    @patch('orchestrator.gold_dim_sucursal')
    @patch('orchestrator.gold_dim_tiempo')
    def test_gold_dimensions_llama_todas(self, mock_tiempo, mock_sucursal,
                                         mock_deposito, mock_vendedor,
                                         mock_articulo, mock_cliente):
        """gold_dimensions debe llamar a todas las dimensiones."""
        from orchestrator import gold_dimensions
        gold_dimensions()

        mock_tiempo.assert_called_once()
        mock_sucursal.assert_called_once()
        mock_deposito.assert_called_once()
        mock_vendedor.assert_called_once()
        mock_articulo.assert_called_once()
        mock_cliente.assert_called_once()

    @patch('orchestrator.gold_dim_cliente')
    @patch('orchestrator.gold_dim_articulo')
    @patch('orchestrator.gold_dim_vendedor')
    @patch('orchestrator.gold_dim_deposito')
    @patch('orchestrator.gold_dim_sucursal')
    @patch('orchestrator.gold_dim_tiempo')
    def test_gold_dimensions_orden(self, mock_tiempo, mock_sucursal,
                                    mock_deposito, mock_vendedor,
                                    mock_articulo, mock_cliente):
        """gold_dimensions debe ejecutar en orden: tiempo, sucursal, deposito, vendedor, articulo, cliente."""
        from orchestrator import gold_dimensions

        call_order = []
        mock_tiempo.side_effect = lambda *a, **k: call_order.append('tiempo')
        mock_sucursal.side_effect = lambda *a, **k: call_order.append('sucursal')
        mock_deposito.side_effect = lambda *a, **k: call_order.append('deposito')
        mock_vendedor.side_effect = lambda *a, **k: call_order.append('vendedor')
        mock_articulo.side_effect = lambda *a, **k: call_order.append('articulo')
        mock_cliente.side_effect = lambda *a, **k: call_order.append('cliente')

        gold_dimensions()

        assert call_order == ['tiempo', 'sucursal', 'deposito', 'vendedor', 'articulo', 'cliente']


class TestBronzeMasters:
    """Tests para bronze_masters() - orden de ejecución."""

    @patch('orchestrator.bronze_marketing')
    @patch('orchestrator.bronze_depositos')
    @patch('orchestrator.bronze_articles')
    @patch('orchestrator.bronze_routes')
    @patch('orchestrator.bronze_staff')
    @patch('orchestrator.bronze_clientes')
    def test_bronze_masters_llama_todas(self, mock_clientes, mock_staff,
                                         mock_routes, mock_articles,
                                         mock_depositos, mock_marketing):
        """bronze_masters debe llamar a todos los loaders maestros."""
        from orchestrator import bronze_masters
        bronze_masters()

        mock_clientes.assert_called_once()
        mock_staff.assert_called_once()
        mock_routes.assert_called_once()
        mock_articles.assert_called_once()
        mock_depositos.assert_called_once()
        mock_marketing.assert_called_once()


class TestSilverMasters:
    """Tests para silver_masters() - orden de ejecución."""

    @patch('orchestrator.silver_marketing')
    @patch('orchestrator.silver_article_groupings')
    @patch('orchestrator.silver_articles')
    @patch('orchestrator.silver_client_forces')
    @patch('orchestrator.silver_clientes')
    @patch('orchestrator.silver_routes')
    @patch('orchestrator.silver_staff')
    @patch('orchestrator.silver_sales_forces')
    @patch('orchestrator.silver_branches')
    def test_silver_masters_orden(self, mock_branches, mock_sf, mock_staff,
                                   mock_routes, mock_clientes, mock_cf,
                                   mock_articles, mock_ag, mock_marketing):
        """silver_masters debe ejecutar en el orden correcto de dependencias."""
        from orchestrator import silver_masters

        call_order = []
        mock_branches.side_effect = lambda *a, **k: call_order.append('branches')
        mock_sf.side_effect = lambda *a, **k: call_order.append('sales_forces')
        mock_staff.side_effect = lambda *a, **k: call_order.append('staff')
        mock_routes.side_effect = lambda *a, **k: call_order.append('routes')
        mock_clientes.side_effect = lambda *a, **k: call_order.append('clientes')
        mock_cf.side_effect = lambda *a, **k: call_order.append('client_forces')
        mock_articles.side_effect = lambda *a, **k: call_order.append('articles')
        mock_ag.side_effect = lambda *a, **k: call_order.append('article_groupings')
        mock_marketing.side_effect = lambda *a, **k: call_order.append('marketing')

        silver_masters()

        assert call_order == [
            'branches', 'sales_forces', 'staff', 'routes',
            'clientes', 'client_forces', 'articles', 'article_groupings', 'marketing'
        ]


class TestPartialRefreshSales:
    """Tests para partial_refresh_sales()."""

    @patch('orchestrator.gold_fact_ventas')
    @patch('orchestrator.silver_sales')
    @patch('orchestrator.bronze_sales')
    def test_partial_refresh_usa_mes_correcto(self, mock_bronze, mock_silver, mock_gold):
        """partial_refresh_sales debe calcular el rango del mes indicado."""
        from orchestrator import partial_refresh_sales
        partial_refresh_sales('2025-06')

        mock_bronze.assert_called_once_with('2025-06-01', '2025-06-30')
        mock_silver.assert_called_once_with('2025-06-01', '2025-06-30')
        mock_gold.assert_called_once_with('2025-06-01', '2025-06-30')

    @patch('orchestrator.gold_fact_ventas')
    @patch('orchestrator.silver_sales')
    @patch('orchestrator.bronze_sales')
    def test_partial_refresh_orden_bronze_silver_gold(self, mock_bronze, mock_silver, mock_gold):
        """partial_refresh_sales debe ejecutar Bronze -> Silver -> Gold."""
        from orchestrator import partial_refresh_sales

        call_order = []
        mock_bronze.side_effect = lambda *a, **k: call_order.append('bronze')
        mock_silver.side_effect = lambda *a, **k: call_order.append('silver')
        mock_gold.side_effect = lambda *a, **k: call_order.append('gold')

        partial_refresh_sales('2025-01')

        assert call_order == ['bronze', 'silver', 'gold']
