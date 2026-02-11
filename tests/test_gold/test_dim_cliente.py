"""
Tests para el aggregator dim_cliente (Gold).
Verifica CTEs de rutas, JOINs y campo anulado.
"""
import pytest
from unittest.mock import patch, MagicMock


def _capture_sql():
    """Helper: ejecuta load_dim_cliente y captura SQL."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 50
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch('layers.gold.aggregators.dim_cliente.engine') as mock_engine:
        mock_engine.connect.return_value = mock_conn
        from layers.gold.aggregators.dim_cliente import load_dim_cliente
        load_dim_cliente()

    return [str(c) for c in mock_cursor.execute.call_args_list]


class TestDimClienteSQL:
    """Tests para la estructura SQL de load_dim_cliente()."""

    def test_delete_full_refresh(self):
        calls = _capture_sql()
        assert any('DELETE FROM gold.dim_cliente' in c for c in calls)

    def test_insert_en_gold_dim_cliente(self):
        calls = _capture_sql()
        assert any('INSERT INTO gold.dim_cliente' in c for c in calls)

    def test_lee_de_silver_clients(self):
        calls = _capture_sql()
        assert any('silver.clients' in c for c in calls)

    def test_cte_rutas_fv1(self):
        """Debe tener CTE para rutas FV1."""
        calls = _capture_sql()
        assert any('rutas_fv1' in c for c in calls)

    def test_cte_rutas_fv4(self):
        """Debe tener CTE para rutas FV4."""
        calls = _capture_sql()
        assert any('rutas_fv4' in c for c in calls)

    def test_join_branches(self):
        calls = _capture_sql()
        assert any('silver.branches' in c for c in calls)

    def test_join_marketing_channels(self):
        calls = _capture_sql()
        assert any('silver.marketing_channels' in c for c in calls)

    def test_join_marketing_segments(self):
        calls = _capture_sql()
        assert any('silver.marketing_segments' in c for c in calls)

    def test_incluye_campo_anulado(self):
        """El INSERT debe incluir el campo anulado."""
        calls = _capture_sql()
        insert_sql = [c for c in calls if 'INSERT INTO gold.dim_cliente' in c]
        assert len(insert_sql) > 0
        assert 'anulado' in insert_sql[0]

    def test_on_conflict_actualiza_anulado(self):
        """ON CONFLICT debe actualizar anulado."""
        calls = _capture_sql()
        insert_sql = [c for c in calls if 'ON CONFLICT' in c]
        assert len(insert_sql) > 0
        assert 'anulado = EXCLUDED.anulado' in insert_sql[0]

    def test_filtra_rutas_activas(self):
        """CTEs deben filtrar por fecha_fin = 9999-12-31."""
        calls = _capture_sql()
        assert any('9999-12-31' in c for c in calls)

    def test_distinct_on_en_ctes(self):
        """CTEs deben usar DISTINCT ON para seleccionar una ruta por cliente."""
        calls = _capture_sql()
        assert any('DISTINCT ON' in c for c in calls)
