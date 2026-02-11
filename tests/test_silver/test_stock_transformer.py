"""
Tests para el transformer de stock (Silver).
Verifica modos de carga y estructura SQL.
"""
import pytest
from unittest.mock import patch, MagicMock


def _make_mock_conn():
    """Helper: crea conexión mockeada con COUNT > 0 para evitar early return."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 50
    mock_cursor.fetchone.return_value = (25,)
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor, mock_raw_conn


def _capture_sql(**kwargs):
    """Helper: ejecuta transform_stock y retorna las queries SQL capturadas."""
    mock_conn, mock_cursor, _ = _make_mock_conn()
    with patch('layers.silver.transformers.stock_transformer.engine') as mock_engine:
        mock_engine.connect.return_value = mock_conn
        from layers.silver.transformers.stock_transformer import transform_stock
        transform_stock(**kwargs)
    return [str(c) for c in mock_cursor.execute.call_args_list]


class TestStockTransformerModes:
    """Tests para los modos de carga de transform_stock()."""

    def test_incremental_delete_por_rango(self):
        """Con fechas, debe DELETE por rango."""
        calls = _capture_sql(fecha_desde='2025-01-01', fecha_hasta='2025-01-31')
        assert any('DELETE FROM silver.fact_stock' in c and 'date_stock' in c for c in calls)

    def test_full_refresh_delete_todo(self):
        """Con full_refresh=True, debe DELETE todo."""
        calls = _capture_sql(full_refresh=True)
        assert any('DELETE FROM silver.fact_stock' in c for c in calls)

    def test_lee_de_bronze_raw_stock(self):
        """Debe leer de bronze.raw_stock."""
        calls = _capture_sql(full_refresh=True)
        assert any('bronze.raw_stock' in c for c in calls)

    def test_escribe_en_silver_fact_stock(self):
        """Debe escribir en silver.fact_stock."""
        calls = _capture_sql(full_refresh=True)
        assert any('INSERT INTO silver.fact_stock' in c for c in calls)

    def test_on_conflict_upsert(self):
        """Debe usar ON CONFLICT para upsert."""
        calls = _capture_sql(full_refresh=True)
        assert any('ON CONFLICT' in c for c in calls)

    def test_commit_se_ejecuta(self):
        """Debe hacer commit."""
        mock_conn, mock_cursor, mock_raw_conn = _make_mock_conn()
        with patch('layers.silver.transformers.stock_transformer.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.silver.transformers.stock_transformer import transform_stock
            transform_stock(full_refresh=True)

        mock_raw_conn.commit.assert_called()
