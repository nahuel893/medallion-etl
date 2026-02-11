"""
Tests para los transformers master de Silver.
Verifica estructura SQL y patrones comunes (full_refresh, DELETE+INSERT, source/target).
"""
import pytest
from unittest.mock import patch, MagicMock


def _make_mock_conn():
    """Helper: crea conexión mockeada."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 10
    mock_cursor.fetchone.return_value = (5,)
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def _capture_sql(module_path, func_name, **kwargs):
    """Helper: ejecuta un transformer y captura las queries SQL."""
    mock_conn, mock_cursor = _make_mock_conn()

    with patch(f'{module_path}.engine') as mock_engine:
        mock_engine.connect.return_value = mock_conn
        import importlib
        mod = importlib.import_module(module_path.replace('layers.', 'layers.'))
        func = getattr(mod, func_name)
        func(**kwargs)

    return [str(c) for c in mock_cursor.execute.call_args_list]


class TestArticlesTransformer:
    """Tests para transform_articles()."""

    def test_lee_de_bronze_raw_articles(self):
        calls = _capture_sql('layers.silver.transformers.articles_transformer', 'transform_articles')
        assert any('bronze.raw_articles' in c for c in calls)

    def test_escribe_en_silver_articles(self):
        calls = _capture_sql('layers.silver.transformers.articles_transformer', 'transform_articles')
        assert any('INSERT INTO silver.articles' in c for c in calls)

    def test_delete_antes_de_insert(self):
        calls = _capture_sql('layers.silver.transformers.articles_transformer', 'transform_articles')
        delete_idx = next(i for i, c in enumerate(calls) if 'DELETE' in c)
        insert_idx = next(i for i, c in enumerate(calls) if 'INSERT INTO silver.articles' in c)
        assert delete_idx < insert_idx

    def test_incluye_campo_anulado(self):
        calls = _capture_sql('layers.silver.transformers.articles_transformer', 'transform_articles')
        assert any('anulado' in c for c in calls)


class TestClientsTransformer:
    """Tests para transform_clients()."""

    def test_lee_de_bronze_raw_clients(self):
        calls = _capture_sql('layers.silver.transformers.clients_transformer', 'transform_clients')
        assert any('bronze.raw_clients' in c for c in calls)

    def test_escribe_en_silver_clients(self):
        calls = _capture_sql('layers.silver.transformers.clients_transformer', 'transform_clients')
        assert any('INSERT INTO silver.clients' in c for c in calls)

    def test_incluye_campo_anulado(self):
        calls = _capture_sql('layers.silver.transformers.clients_transformer', 'transform_clients')
        assert any('anulado' in c for c in calls)

    def test_incluye_geolocalizacion(self):
        calls = _capture_sql('layers.silver.transformers.clients_transformer', 'transform_clients')
        assert any('latitud' in c for c in calls)
        assert any('longitud' in c for c in calls)


class TestRoutesTransformer:
    """Tests para transform_routes()."""

    def test_lee_de_bronze_raw_routes(self):
        calls = _capture_sql('layers.silver.transformers.routes_transformer', 'transform_routes')
        assert any('bronze.raw_routes' in c for c in calls)

    def test_escribe_en_silver_routes(self):
        calls = _capture_sql('layers.silver.transformers.routes_transformer', 'transform_routes')
        assert any('INSERT INTO silver.routes' in c for c in calls)

    def test_filtra_rutas_activas(self):
        """Solo carga rutas activas (fecha_hasta = 9999-12-31)."""
        calls = _capture_sql('layers.silver.transformers.routes_transformer', 'transform_routes')
        assert any('9999-12-31' in c for c in calls)


class TestStaffTransformer:
    """Tests para transform_staff()."""

    def test_lee_de_bronze_raw_staff(self):
        calls = _capture_sql('layers.silver.transformers.staff_transformer', 'transform_staff')
        assert any('bronze.raw_staff' in c for c in calls)

    def test_escribe_en_silver_staff(self):
        calls = _capture_sql('layers.silver.transformers.staff_transformer', 'transform_staff')
        assert any('INSERT INTO silver.staff' in c for c in calls)

    def test_usa_distinct_on(self):
        """Debe usar DISTINCT ON para deduplicar personal."""
        calls = _capture_sql('layers.silver.transformers.staff_transformer', 'transform_staff')
        assert any('DISTINCT ON' in c for c in calls)


class TestDepositsTransformer:
    """Tests para transform_deposits()."""

    def test_lee_de_bronze_raw_deposits(self):
        calls = _capture_sql('layers.silver.transformers.deposits_transformer', 'transform_deposits')
        assert any('bronze.raw_deposits' in c for c in calls)

    def test_escribe_en_silver_deposits(self):
        calls = _capture_sql('layers.silver.transformers.deposits_transformer', 'transform_deposits')
        assert any('INSERT INTO silver.deposits' in c for c in calls)

    def test_parsea_sucursal_con_split_part(self):
        """Debe usar SPLIT_PART para parsear el campo sucursal."""
        calls = _capture_sql('layers.silver.transformers.deposits_transformer', 'transform_deposits')
        assert any('SPLIT_PART' in c or 'split_part' in c for c in calls)


class TestHectolitrosTransformer:
    """Tests para transform_hectolitros()."""

    def test_lee_de_bronze_raw_hectolitros(self):
        calls = _capture_sql('layers.silver.transformers.hectolitros_transformer', 'transform_hectolitros')
        assert any('bronze.raw_hectolitros' in c for c in calls)

    def test_escribe_en_silver_hectolitros(self):
        calls = _capture_sql('layers.silver.transformers.hectolitros_transformer', 'transform_hectolitros')
        assert any('INSERT INTO silver.hectolitros' in c for c in calls)

    def test_incluye_factor_hectolitros(self):
        calls = _capture_sql('layers.silver.transformers.hectolitros_transformer', 'transform_hectolitros')
        assert any('factor_hectolitros' in c for c in calls)


class TestBranchesTransformer:
    """Tests para transform_branches()."""

    def test_lee_de_bronze_raw_staff(self):
        calls = _capture_sql('layers.silver.transformers.branches_transformer', 'transform_branches')
        assert any('bronze.raw_staff' in c for c in calls)

    def test_escribe_en_silver_branches(self):
        calls = _capture_sql('layers.silver.transformers.branches_transformer', 'transform_branches')
        assert any('INSERT INTO silver.branches' in c for c in calls)
