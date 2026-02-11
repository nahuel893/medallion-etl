"""
Tests para el transformer de ventas (Silver).
Verifica modos de carga y estructura SQL.
"""
import pytest
from unittest.mock import patch, MagicMock, call


def _make_mock_conn():
    """Helper: crea conexión mockeada con COUNT > 0 para evitar early return."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 100
    mock_cursor.fetchone.return_value = (50,)
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor, mock_raw_conn


def _capture_sql(**kwargs):
    """Helper: ejecuta transform_sales y retorna las queries SQL capturadas."""
    mock_conn, mock_cursor, _ = _make_mock_conn()
    with patch('layers.silver.transformers.sales_transformer.engine') as mock_engine:
        mock_engine.connect.return_value = mock_conn
        from layers.silver.transformers.sales_transformer import transform_sales
        transform_sales(**kwargs)
    return [str(c) for c in mock_cursor.execute.call_args_list]


class TestSalesTransformerModes:
    """Tests para los modos de carga de transform_sales()."""

    def test_incremental_delete_por_rango(self):
        """Con fecha_desde y fecha_hasta, debe DELETE por rango de fechas."""
        calls = _capture_sql(fecha_desde='2025-01-01', fecha_hasta='2025-01-31')
        assert any('DELETE FROM silver.fact_ventas' in c and 'fecha_comprobante' in c for c in calls)

    def test_incremental_insert(self):
        """Con fechas, debe ejecutar INSERT."""
        calls = _capture_sql(fecha_desde='2025-01-01', fecha_hasta='2025-01-31')
        assert any('INSERT INTO silver.fact_ventas' in c for c in calls)

    def test_full_refresh_delete_todo(self):
        """Con full_refresh=True, debe DELETE todo."""
        calls = _capture_sql(full_refresh=True)
        # Debe haber un DELETE sin condiciones de fecha
        assert any('DELETE FROM silver.fact_ventas' in c for c in calls)

    def test_parametros_fecha_se_pasan(self):
        """Los parámetros de fecha deben pasarse al cursor.execute."""
        mock_conn, mock_cursor, _ = _make_mock_conn()
        with patch('layers.silver.transformers.sales_transformer.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.silver.transformers.sales_transformer import transform_sales
            transform_sales('2025-03-01', '2025-03-31')

        all_args = [c.args for c in mock_cursor.execute.call_args_list if c.args]
        fecha_encontrada = any(
            '2025-03-01' in str(args) and '2025-03-31' in str(args)
            for args in all_args
        )
        assert fecha_encontrada

    def test_commit_se_ejecuta(self):
        """Debe hacer commit al finalizar."""
        mock_conn, mock_cursor, mock_raw_conn = _make_mock_conn()
        with patch('layers.silver.transformers.sales_transformer.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.silver.transformers.sales_transformer import transform_sales
            transform_sales(full_refresh=True)

        mock_raw_conn.commit.assert_called()


class TestSalesTransformerSQL:
    """Tests para la estructura SQL del transformer de ventas."""

    def test_lee_de_bronze_raw_sales(self):
        """Debe leer de bronze.raw_sales."""
        calls = _capture_sql(full_refresh=True)
        assert any('bronze.raw_sales' in c for c in calls)

    def test_escribe_en_silver_fact_ventas(self):
        """Debe escribir en silver.fact_ventas."""
        calls = _capture_sql(full_refresh=True)
        assert any('INSERT INTO silver.fact_ventas' in c for c in calls)

    def test_sql_contiene_facturacion_neta(self):
        """El INSERT debe calcular facturacion_neta."""
        calls = _capture_sql(full_refresh=True)
        assert any('facturacion_neta' in c.lower() for c in calls)

    def test_sql_contiene_anulado(self):
        """El INSERT debe incluir campo anulado."""
        calls = _capture_sql(full_refresh=True)
        assert any('anulado' in c for c in calls)
