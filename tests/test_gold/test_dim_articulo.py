"""
Tests para el aggregator dim_articulo (Gold).
Verifica estructura SQL, JOINs y campo factor_hectolitros.
"""
import pytest
from unittest.mock import patch, MagicMock


def _capture_sql():
    """Helper: ejecuta load_dim_articulo con DB mockeada y captura SQL."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 100
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    with patch('layers.gold.aggregators.dim_articulo.engine') as mock_engine:
        mock_engine.connect.return_value = mock_conn
        from layers.gold.aggregators.dim_articulo import load_dim_articulo
        load_dim_articulo()

    return [str(c) for c in mock_cursor.execute.call_args_list]


class TestDimArticuloSQL:
    """Tests para la estructura SQL de load_dim_articulo()."""

    def test_delete_full_refresh(self):
        calls = _capture_sql()
        assert any('DELETE FROM gold.dim_articulo' in c for c in calls)

    def test_insert_en_gold_dim_articulo(self):
        calls = _capture_sql()
        assert any('INSERT INTO gold.dim_articulo' in c for c in calls)

    def test_lee_de_silver_articles(self):
        calls = _capture_sql()
        assert any('silver.articles' in c for c in calls)

    def test_join_article_groupings(self):
        calls = _capture_sql()
        assert any('silver.article_groupings' in c for c in calls)

    def test_join_hectolitros(self):
        """Debe hacer LEFT JOIN a silver.hectolitros."""
        calls = _capture_sql()
        assert any('silver.hectolitros' in c for c in calls)

    def test_incluye_factor_hectolitros(self):
        """El INSERT debe incluir factor_hectolitros."""
        calls = _capture_sql()
        insert_sql = [c for c in calls if 'INSERT INTO gold.dim_articulo' in c]
        assert len(insert_sql) > 0
        assert 'factor_hectolitros' in insert_sql[0]

    def test_pivot_marca(self):
        """Debe pivotar MARCA como columna."""
        calls = _capture_sql()
        assert any("'MARCA'" in c for c in calls)

    def test_pivot_generico(self):
        """Debe pivotar GENERICO como columna."""
        calls = _capture_sql()
        assert any("'GENERICO'" in c for c in calls)

    def test_pivot_calibre(self):
        """Debe pivotar CALIBRE como columna."""
        calls = _capture_sql()
        assert any("'CALIBRE'" in c for c in calls)

    def test_on_conflict_actualiza_factor_htls(self):
        """ON CONFLICT debe actualizar factor_hectolitros."""
        calls = _capture_sql()
        insert_sql = [c for c in calls if 'ON CONFLICT' in c]
        assert len(insert_sql) > 0
        assert 'factor_hectolitros = EXCLUDED.factor_hectolitros' in insert_sql[0]

    def test_commit(self):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor
        mock_conn = MagicMock()
        mock_conn.connection.dbapi_connection = mock_raw_conn
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch('layers.gold.aggregators.dim_articulo.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.dim_articulo import load_dim_articulo
            load_dim_articulo()

        mock_raw_conn.commit.assert_called_once()
