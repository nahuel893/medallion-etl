"""
Tests para el aggregator fact_ventas (Gold).
Verifica modos de carga, JOIN hectolitros y campo cantidad_total_htls.
"""
import pytest
from unittest.mock import patch, MagicMock


def _make_mock_conn():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1000
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


class TestFactVentasModes:
    """Tests para los modos de carga."""

    def test_incremental_por_fecha(self):
        """Con fechas, debe DELETE por rango."""
        mock_conn, mock_cursor = _make_mock_conn()

        with patch('layers.gold.aggregators.fact_ventas.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_ventas import load_fact_ventas
            load_fact_ventas('2025-01-01', '2025-01-31')

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.fact_ventas' in c and 'BETWEEN' in c for c in calls_sql)

    def test_full_refresh(self):
        """Con full_refresh=True, debe DELETE todo."""
        mock_conn, mock_cursor = _make_mock_conn()

        with patch('layers.gold.aggregators.fact_ventas.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_ventas import load_fact_ventas
            load_fact_ventas(full_refresh=True)

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.fact_ventas' in c for c in calls_sql)

    def test_sin_parametros_delete_todo(self):
        """Sin parámetros, debe DELETE todo."""
        mock_conn, mock_cursor = _make_mock_conn()

        with patch('layers.gold.aggregators.fact_ventas.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_ventas import load_fact_ventas
            load_fact_ventas()

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.fact_ventas' in c for c in calls_sql)

    def test_fechas_tienen_prioridad_sobre_full_refresh(self):
        """Cuando se pasan fechas Y full_refresh, las fechas tienen prioridad."""
        mock_conn, mock_cursor = _make_mock_conn()

        with patch('layers.gold.aggregators.fact_ventas.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_ventas import load_fact_ventas
            load_fact_ventas('2025-01-01', '2025-01-31', full_refresh=True)

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('BETWEEN' in c for c in calls_sql)


class TestFactVentasHTLS:
    """Tests para la integración de hectolitros."""

    def _capture_sql(self, **kwargs):
        mock_conn, mock_cursor = _make_mock_conn()
        with patch('layers.gold.aggregators.fact_ventas.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.fact_ventas import load_fact_ventas
            load_fact_ventas(**kwargs)
        return [str(c) for c in mock_cursor.execute.call_args_list]

    def test_join_silver_hectolitros(self):
        """Debe hacer LEFT JOIN a silver.hectolitros."""
        calls = self._capture_sql()
        assert any('silver.hectolitros' in c for c in calls)

    def test_calcula_cantidad_total_htls(self):
        """Debe calcular cantidades_total * factor_hectolitros."""
        calls = self._capture_sql()
        insert_sql = [c for c in calls if 'INSERT INTO gold.fact_ventas' in c]
        assert len(insert_sql) > 0
        assert 'cantidad_total_htls' in insert_sql[0]
        assert 'cantidades_total' in insert_sql[0]
        assert 'factor_hectolitros' in insert_sql[0]

    def test_lee_de_silver_fact_ventas(self):
        calls = self._capture_sql()
        assert any('silver.fact_ventas' in c for c in calls)

    def test_set_work_mem(self):
        """Debe configurar work_mem."""
        calls = self._capture_sql()
        assert any('work_mem' in c for c in calls)
