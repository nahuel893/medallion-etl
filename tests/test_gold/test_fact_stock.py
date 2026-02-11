"""
Tests para el aggregator fact_stock (Gold).
Verifica modos de carga, JOIN hectolitros y campo cantidad_total_htls.
"""
import pytest
from unittest.mock import patch, MagicMock


def _make_mock_conn():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 500
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


class TestFactStockModes:
    """Tests para los modos de carga."""

    def test_incremental_por_fecha(self):
        mock_conn, mock_cursor = _make_mock_conn()

        with patch('layers.gold.aggregators.fact_stock.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_stock import load_fact_stock
            load_fact_stock('2025-01-01', '2025-01-31')

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.fact_stock' in c and 'BETWEEN' in c for c in calls_sql)

    def test_full_refresh(self):
        mock_conn, mock_cursor = _make_mock_conn()

        with patch('layers.gold.aggregators.fact_stock.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_stock import load_fact_stock
            load_fact_stock(full_refresh=True)

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.fact_stock' in c for c in calls_sql)

    def test_fechas_tienen_prioridad(self):
        mock_conn, mock_cursor = _make_mock_conn()

        with patch('layers.gold.aggregators.fact_stock.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_stock import load_fact_stock
            load_fact_stock('2025-01-01', '2025-01-31', full_refresh=True)

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('BETWEEN' in c for c in calls_sql)


class TestFactStockHTLS:
    """Tests para la integración de hectolitros."""

    def _capture_sql(self, **kwargs):
        mock_conn, mock_cursor = _make_mock_conn()
        with patch('layers.gold.aggregators.fact_stock.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_stock import load_fact_stock
            load_fact_stock(**kwargs)
        return [str(c) for c in mock_cursor.execute.call_args_list]

    def test_join_silver_hectolitros(self):
        """Debe hacer LEFT JOIN a silver.hectolitros."""
        calls = self._capture_sql()
        assert any('silver.hectolitros' in c for c in calls)

    def test_calcula_cantidad_total_htls(self):
        """Debe calcular cant_bultos * factor_hectolitros."""
        calls = self._capture_sql()
        insert_sql = [c for c in calls if 'INSERT INTO gold.fact_stock' in c]
        assert len(insert_sql) > 0
        assert 'cantidad_total_htls' in insert_sql[0]
        assert 'cant_bultos' in insert_sql[0]
        assert 'factor_hectolitros' in insert_sql[0]

    def test_lee_de_silver_fact_stock(self):
        calls = self._capture_sql()
        assert any('silver.fact_stock' in c for c in calls)
