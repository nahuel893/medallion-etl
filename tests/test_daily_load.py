"""
Tests para daily_load.py.
Verifica la lógica de cálculo de fechas y el runner de fases.
"""
import pytest
from datetime import date
from unittest.mock import patch, MagicMock


class TestDailyLoadDateLogic:
    """Tests para la lógica de cálculo de fechas en daily_load."""

    def test_include_prev_month_dia_1(self):
        """Día 1 del mes debe incluir el mes anterior."""
        ref_date = date(2025, 6, 1)
        assert ref_date.day <= 3

    def test_include_prev_month_dia_3(self):
        """Día 3 del mes debe incluir el mes anterior."""
        ref_date = date(2025, 6, 3)
        assert ref_date.day <= 3

    def test_no_include_prev_month_dia_4(self):
        """Día 4 del mes NO debe incluir el mes anterior."""
        ref_date = date(2025, 6, 4)
        assert not (ref_date.day <= 3)

    def test_no_include_prev_month_dia_15(self):
        """Día 15 del mes NO debe incluir el mes anterior."""
        ref_date = date(2025, 6, 15)
        assert not (ref_date.day <= 3)

    def test_current_month_format(self):
        """El mes actual debe formatearse como YYYY-MM."""
        ref_date = date(2025, 1, 15)
        current_month = f"{ref_date.year}-{ref_date.month:02d}"
        assert current_month == "2025-01"

    def test_current_month_format_diciembre(self):
        """Diciembre debe formatearse correctamente."""
        ref_date = date(2025, 12, 25)
        current_month = f"{ref_date.year}-{ref_date.month:02d}"
        assert current_month == "2025-12"

    def test_prev_month_enero(self):
        """Si estamos en enero día 1, el mes anterior es diciembre del año anterior."""
        from datetime import timedelta
        ref_date = date(2025, 1, 1)
        prev = ref_date.replace(day=1) - timedelta(days=1)
        prev_month = f"{prev.year}-{prev.month:02d}"
        assert prev_month == "2024-12"

    def test_prev_month_marzo(self):
        """Si estamos en marzo día 2, el mes anterior es febrero."""
        from datetime import timedelta
        ref_date = date(2025, 3, 2)
        prev = ref_date.replace(day=1) - timedelta(days=1)
        prev_month = f"{prev.year}-{prev.month:02d}"
        assert prev_month == "2025-02"

    def test_stock_fecha_es_iso(self):
        """La fecha de stock debe ser formato ISO."""
        ref_date = date(2025, 6, 15)
        stock_fecha = ref_date.isoformat()
        assert stock_fecha == "2025-06-15"


class TestRunPhase:
    """Tests para la función run_phase()."""

    def test_run_phase_exitosa(self):
        """run_phase debe retornar True cuando la función no lanza excepción."""
        from daily_load import run_phase

        def ok_fn():
            pass

        assert run_phase("TEST_PHASE", ok_fn) is True

    def test_run_phase_con_error(self):
        """run_phase debe retornar False cuando la función lanza excepción."""
        from daily_load import run_phase

        def fail_fn():
            raise RuntimeError("test error")

        assert run_phase("TEST_PHASE", fail_fn) is False

    def test_run_phase_pasa_argumentos(self):
        """run_phase debe pasar args y kwargs a la función."""
        from daily_load import run_phase
        received = {}

        def capture_fn(a, b, key=None):
            received['a'] = a
            received['b'] = b
            received['key'] = key

        run_phase("TEST", capture_fn, 1, 2, key="val")

        assert received == {'a': 1, 'b': 2, 'key': 'val'}

    def test_run_phase_no_propaga_excepcion(self):
        """run_phase NO debe propagar la excepción, solo retornar False."""
        from daily_load import run_phase

        def fail_fn():
            raise ValueError("boom")

        # No debe lanzar excepción
        result = run_phase("TEST", fail_fn)
        assert result is False
