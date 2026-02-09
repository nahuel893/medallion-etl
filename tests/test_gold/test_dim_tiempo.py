"""
Tests para el aggregator dim_tiempo (Gold).
Verifica la lógica de generación de registros de fechas.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import date


class TestDimTiempoRecordGeneration:
    """Tests para la generación de registros en load_dim_tiempo()."""

    def _run_dim_tiempo(self, fecha_desde, fecha_hasta):
        """Helper: ejecuta load_dim_tiempo con DB mockeada y retorna los registros generados."""
        captured_records = []

        def fake_execute_values(cursor, query, records):
            captured_records.extend(records)

        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor
        mock_conn = MagicMock()
        mock_conn.connection.dbapi_connection = mock_raw_conn
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch('layers.gold.aggregators.dim_tiempo.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            with patch('psycopg2.extras.execute_values', fake_execute_values):
                from layers.gold.aggregators.dim_tiempo import load_dim_tiempo
                load_dim_tiempo(fecha_desde, fecha_hasta)

        return captured_records

    def test_cantidad_registros_un_mes(self):
        """Enero 2025 debe generar 31 registros."""
        records = self._run_dim_tiempo('2025-01-01', '2025-01-31')
        assert len(records) == 31

    def test_cantidad_registros_anio_completo(self):
        """Un año completo debe generar 365 registros (no bisiesto)."""
        records = self._run_dim_tiempo('2025-01-01', '2025-12-31')
        assert len(records) == 365

    def test_cantidad_registros_anio_bisiesto(self):
        """Un año bisiesto debe generar 366 registros."""
        records = self._run_dim_tiempo('2024-01-01', '2024-12-31')
        assert len(records) == 366

    def test_estructura_registro(self):
        """Cada registro debe tener 9 campos: fecha, dia, dia_semana, nombre_dia, semana, mes, nombre_mes, trimestre, anio."""
        records = self._run_dim_tiempo('2025-01-01', '2025-01-01')
        assert len(records) == 1
        assert len(records[0]) == 9

    def test_primer_dia_enero(self):
        """1 de enero 2025 (miércoles): valores correctos."""
        records = self._run_dim_tiempo('2025-01-01', '2025-01-01')
        rec = records[0]

        assert rec[0] == date(2025, 1, 1)       # fecha
        assert rec[1] == 1                        # dia
        assert rec[2] == 3                        # dia_semana (miércoles = 3 ISO)
        assert rec[3] == 'Miércoles'             # nombre_dia
        assert rec[4] == 1                        # semana ISO
        assert rec[5] == 1                        # mes
        assert rec[6] == 'Enero'                 # nombre_mes
        assert rec[7] == 1                        # trimestre
        assert rec[8] == 2025                     # anio

    def test_trimestres(self):
        """Debe asignar trimestres correctamente para cada mes."""
        records = self._run_dim_tiempo('2025-01-15', '2025-12-15')

        # Tomar un registro por mes
        meses_trimestre = {}
        for rec in records:
            mes = rec[5]
            if mes not in meses_trimestre:
                meses_trimestre[mes] = rec[7]

        assert meses_trimestre[1] == 1   # Enero -> Q1
        assert meses_trimestre[3] == 1   # Marzo -> Q1
        assert meses_trimestre[4] == 2   # Abril -> Q2
        assert meses_trimestre[6] == 2   # Junio -> Q2
        assert meses_trimestre[7] == 3   # Julio -> Q3
        assert meses_trimestre[9] == 3   # Septiembre -> Q3
        assert meses_trimestre[10] == 4  # Octubre -> Q4
        assert meses_trimestre[12] == 4  # Diciembre -> Q4

    def test_nombres_meses_espanol(self):
        """Los nombres de mes deben estar en español."""
        records = self._run_dim_tiempo('2025-01-01', '2025-12-31')

        nombres_esperados = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }

        nombres_encontrados = {}
        for rec in records:
            mes_num = rec[5]
            if mes_num not in nombres_encontrados:
                nombres_encontrados[mes_num] = rec[6]

        assert nombres_encontrados == nombres_esperados

    def test_nombres_dias_espanol(self):
        """Los nombres de día deben estar en español."""
        # Semana del 6 al 12 de enero 2025 (lunes a domingo)
        records = self._run_dim_tiempo('2025-01-06', '2025-01-12')

        nombres = [rec[3] for rec in records]
        assert nombres == ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

    def test_dia_semana_iso(self):
        """dia_semana debe seguir ISO: 1=Lunes, 7=Domingo."""
        records = self._run_dim_tiempo('2025-01-06', '2025-01-12')

        dias_semana = [rec[2] for rec in records]
        assert dias_semana == [1, 2, 3, 4, 5, 6, 7]

    def test_febrero_29_bisiesto(self):
        """29 de febrero debe existir en año bisiesto."""
        records = self._run_dim_tiempo('2024-02-28', '2024-03-01')

        fechas = [rec[0] for rec in records]
        assert date(2024, 2, 29) in fechas
        assert len(records) == 3

    def test_un_solo_dia(self):
        """Debe funcionar con un rango de un solo día."""
        records = self._run_dim_tiempo('2025-06-15', '2025-06-15')
        assert len(records) == 1
        assert records[0][0] == date(2025, 6, 15)

    def test_delete_antes_de_insert(self):
        """Debe ejecutar DELETE antes de INSERT (full refresh)."""
        mock_cursor = MagicMock()
        mock_raw_conn = MagicMock()
        mock_raw_conn.cursor.return_value = mock_cursor
        mock_conn = MagicMock()
        mock_conn.connection.dbapi_connection = mock_raw_conn
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch('layers.gold.aggregators.dim_tiempo.engine') as mock_engine:
            mock_engine.connect.return_value = mock_conn
            with patch('psycopg2.extras.execute_values'):
                from layers.gold.aggregators.dim_tiempo import load_dim_tiempo
                load_dim_tiempo('2025-01-01', '2025-01-01')

        # Verificar que se llamó DELETE
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any('DELETE FROM gold.dim_tiempo' in c for c in calls)
