"""
Tests para el aggregator de cobertura (Gold).
Verifica estructura SQL, período y orquestación.
"""
import pytest
from unittest.mock import patch, MagicMock


def _make_mock_conn():
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 200
    mock_raw_conn = MagicMock()
    mock_raw_conn.cursor.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.connection.dbapi_connection = mock_raw_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


def _capture_sql(func_name, **kwargs):
    mock_conn, mock_cursor = _make_mock_conn()
    with patch('layers.gold.aggregators.cobertura.engine') as mock_engine:
        mock_engine.connect.return_value = mock_conn
        from layers.gold.aggregators import cobertura
        func = getattr(cobertura, func_name)
        func(**kwargs)
    return [str(c) for c in mock_cursor.execute.call_args_list]


class TestCobPreventistaSQL:
    """Tests para load_cob_preventista_marca()."""

    def test_insert_en_cob_preventista_marca(self):
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('INSERT INTO gold.cob_preventista_marca' in c for c in calls)

    def test_join_dim_vendedor(self):
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('gold.dim_vendedor' in c for c in calls)

    def test_join_dim_articulo(self):
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('gold.dim_articulo' in c for c in calls)

    def test_join_dim_cliente(self):
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('gold.dim_cliente' in c for c in calls)

    def test_case_ruta_por_fuerza_venta(self):
        """Debe usar CASE para seleccionar ruta según fuerza de venta."""
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('id_ruta_fv1' in c and 'id_ruta_fv4' in c for c in calls)

    def test_count_distinct_clientes(self):
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('COUNT(DISTINCT' in c for c in calls)

    def test_filtra_neto_positivo_por_cliente(self):
        """Debe usar CTE con HAVING SUM para filtrar clientes con neto > 0."""
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('HAVING SUM(fv.cantidades_total) > 0' in c for c in calls)

    def test_usa_cte_cliente_marca(self):
        """Debe usar CTE para agrupar por cliente antes de contar."""
        calls = _capture_sql('load_cob_preventista_marca')
        assert any('WITH cliente_marca AS' in c for c in calls)


class TestCobSucursalSQL:
    """Tests para load_cob_sucursal_marca()."""

    def test_insert_en_cob_sucursal_marca(self):
        calls = _capture_sql('load_cob_sucursal_marca')
        assert any('INSERT INTO gold.cob_sucursal_marca' in c for c in calls)

    def test_group_by_sucursal(self):
        calls = _capture_sql('load_cob_sucursal_marca')
        assert any('id_sucursal' in c for c in calls)

    def test_usa_cte_cliente_marca(self):
        """Debe usar CTE para agrupar por cliente antes de contar."""
        calls = _capture_sql('load_cob_sucursal_marca')
        assert any('WITH cliente_marca AS' in c for c in calls)

    def test_filtra_neto_positivo_por_cliente(self):
        """Debe usar HAVING SUM para filtrar clientes con neto > 0."""
        calls = _capture_sql('load_cob_sucursal_marca')
        assert any('HAVING SUM(fv.cantidades_total) > 0' in c for c in calls)


class TestCobGenericoSQL:
    """Tests para load_cob_preventista_generico()."""

    def test_insert_en_cob_preventista_generico(self):
        calls = _capture_sql('load_cob_preventista_generico')
        assert any('INSERT INTO gold.cob_preventista_generico' in c for c in calls)

    def test_agrupa_por_generico(self):
        calls = _capture_sql('load_cob_preventista_generico')
        assert any('generico' in c.lower() for c in calls)

    def test_usa_cte_cliente_generico(self):
        """Debe usar CTE para agrupar por cliente antes de contar."""
        calls = _capture_sql('load_cob_preventista_generico')
        assert any('WITH cliente_generico AS' in c for c in calls)

    def test_filtra_neto_positivo_por_cliente(self):
        """Debe usar HAVING SUM para filtrar clientes con neto > 0."""
        calls = _capture_sql('load_cob_preventista_generico')
        assert any('HAVING SUM(fv.cantidades_total) > 0' in c for c in calls)


class TestCobSucursalGenericoSQL:
    """Tests para load_cob_sucursal_generico()."""

    def test_insert_en_cob_sucursal_generico(self):
        calls = _capture_sql('load_cob_sucursal_generico')
        assert any('INSERT INTO gold.cob_sucursal_generico' in c for c in calls)

    def test_group_by_sucursal(self):
        calls = _capture_sql('load_cob_sucursal_generico')
        assert any('id_sucursal' in c for c in calls)

    def test_agrupa_por_generico(self):
        calls = _capture_sql('load_cob_sucursal_generico')
        assert any('generico' in c.lower() for c in calls)

    def test_usa_cte_cliente_generico(self):
        """Debe usar CTE para agrupar por cliente antes de contar."""
        calls = _capture_sql('load_cob_sucursal_generico')
        assert any('WITH cliente_generico AS' in c for c in calls)

    def test_filtra_neto_positivo_por_cliente(self):
        """Debe usar HAVING SUM para filtrar clientes con neto > 0."""
        calls = _capture_sql('load_cob_sucursal_generico')
        assert any('HAVING SUM(fv.cantidades_total) > 0' in c for c in calls)


class TestCobSucursalAguasSQL:
    """Tests para load_cob_sucursal_aguas()."""

    def test_insert_en_cob_sucursal_aguas(self):
        calls = _capture_sql('load_cob_sucursal_aguas')
        assert any('INSERT INTO gold.cob_sucursal_aguas' in c for c in calls)

    def test_filtra_solo_aguas_danone(self):
        """Solo debe procesar artículos con genérico AGUAS DANONE."""
        calls = _capture_sql('load_cob_sucursal_aguas')
        assert any("AGUAS DANONE" in c for c in calls)

    def test_case_subdivision_aguas(self):
        """Debe usar CASE para subdividir en AGUAS MINERAL y AGUAS SABORIZADAS."""
        calls = _capture_sql('load_cob_sucursal_aguas')
        assert any('AGUAS MINERAL' in c and 'AGUAS SABORIZADAS' in c for c in calls)

    def test_marcas_mineral(self):
        """VILLA DEL SUR y VILLAVICENCIO deben ser AGUAS MINERAL."""
        calls = _capture_sql('load_cob_sucursal_aguas')
        assert any('VILLA DEL SUR' in c and 'VILLAVICENCIO' in c for c in calls)

    def test_marcas_saborizadas(self):
        """BRIO y LEVITE deben ser AGUAS SABORIZADAS."""
        calls = _capture_sql('load_cob_sucursal_aguas')
        assert any('BRIO' in c and 'LEVITE' in c for c in calls)

    def test_usa_cte_cliente_aguas(self):
        """Debe usar CTE para agrupar por cliente antes de contar."""
        calls = _capture_sql('load_cob_sucursal_aguas')
        assert any('WITH cliente_aguas AS' in c for c in calls)

    def test_filtra_neto_positivo_por_cliente(self):
        """Debe usar HAVING SUM para filtrar clientes con neto > 0."""
        calls = _capture_sql('load_cob_sucursal_aguas')
        assert any('HAVING SUM(fv.cantidades_total) > 0' in c for c in calls)


class TestCobPeriodoHandling:
    """Tests para el manejo de período."""

    def test_periodo_incremental(self):
        """Con periodo, debe DELETE por periodo y filtrar INSERT."""
        mock_conn, mock_cursor = _make_mock_conn()
        with patch('layers.gold.aggregators.cobertura.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.cobertura import load_cob_preventista_marca
            load_cob_preventista_marca(periodo='2025-01')

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.cob_preventista_marca' in c for c in calls_sql)
        # Parámetro debe contener el primer día del mes
        all_args = [c.args for c in mock_cursor.execute.call_args_list if c.args]
        assert any('2025-01-01' in str(args) for args in all_args)

    def test_full_refresh(self):
        """Con full_refresh=True, debe DELETE todo."""
        mock_conn, mock_cursor = _make_mock_conn()
        with patch('layers.gold.aggregators.cobertura.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            from layers.gold.aggregators.cobertura import load_cob_preventista_marca
            load_cob_preventista_marca(full_refresh=True)

        calls_sql = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.cob_preventista_marca' in c for c in calls_sql)


class TestCoberturaOrchestration:
    """Tests para load_cobertura() (orquestador)."""

    def test_ejecuta_las_cinco_tablas(self):
        """load_cobertura debe llamar a las 5 funciones de cobertura."""
        with patch('layers.gold.aggregators.cobertura.load_cob_preventista_marca') as mock_pm, \
             patch('layers.gold.aggregators.cobertura.load_cob_sucursal_marca') as mock_sm, \
             patch('layers.gold.aggregators.cobertura.load_cob_preventista_generico') as mock_pg, \
             patch('layers.gold.aggregators.cobertura.load_cob_sucursal_generico') as mock_sg, \
             patch('layers.gold.aggregators.cobertura.load_cob_sucursal_aguas') as mock_sa:
            from layers.gold.aggregators.cobertura import load_cobertura
            load_cobertura(periodo='2025-01')

            mock_pm.assert_called_once_with('2025-01', False)
            mock_sm.assert_called_once_with('2025-01', False)
            mock_pg.assert_called_once_with('2025-01', False)
            mock_sg.assert_called_once_with('2025-01', False)
            mock_sa.assert_called_once_with('2025-01', False)

    def test_pasa_full_refresh(self):
        """load_cobertura debe pasar full_refresh a las 5 funciones."""
        with patch('layers.gold.aggregators.cobertura.load_cob_preventista_marca') as mock_pm, \
             patch('layers.gold.aggregators.cobertura.load_cob_sucursal_marca') as mock_sm, \
             patch('layers.gold.aggregators.cobertura.load_cob_preventista_generico') as mock_pg, \
             patch('layers.gold.aggregators.cobertura.load_cob_sucursal_generico') as mock_sg, \
             patch('layers.gold.aggregators.cobertura.load_cob_sucursal_aguas') as mock_sa:
            from layers.gold.aggregators.cobertura import load_cobertura
            load_cobertura(full_refresh=True)

            mock_pm.assert_called_once_with('', True)
            mock_sm.assert_called_once_with('', True)
            mock_pg.assert_called_once_with('', True)
            mock_sg.assert_called_once_with('', True)
            mock_sa.assert_called_once_with('', True)
