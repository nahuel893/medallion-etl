"""
Tests para funciones utilitarias del orchestrator.
"""
import pytest
from datetime import date
from unittest.mock import patch


class TestGetMonthRange:
    """Tests para la función get_month_range()."""

    def test_mes_actual_automatico(self):
        """Debe retornar el rango del mes actual cuando no se pasa parámetro."""
        from orchestrator import get_month_range
        from datetime import date
        from calendar import monthrange

        # Llamar sin argumentos (mes actual)
        fecha_desde, fecha_hasta = get_month_range()

        # Verificar que retorna el mes actual
        today = date.today()
        expected_desde = f"{today.year}-{today.month:02d}-01"
        ultimo_dia = monthrange(today.year, today.month)[1]
        expected_hasta = f"{today.year}-{today.month:02d}-{ultimo_dia:02d}"

        assert fecha_desde == expected_desde
        assert fecha_hasta == expected_hasta

    def test_mes_enero(self):
        """Debe retornar el rango correcto para enero."""
        from orchestrator import get_month_range

        fecha_desde, fecha_hasta = get_month_range('2025-01')

        assert fecha_desde == '2025-01-01'
        assert fecha_hasta == '2025-01-31'

    def test_mes_febrero_no_bisiesto(self):
        """Debe retornar 28 días para febrero en año no bisiesto."""
        from orchestrator import get_month_range

        fecha_desde, fecha_hasta = get_month_range('2025-02')

        assert fecha_desde == '2025-02-01'
        assert fecha_hasta == '2025-02-28'

    def test_mes_febrero_bisiesto(self):
        """Debe retornar 29 días para febrero en año bisiesto."""
        from orchestrator import get_month_range

        fecha_desde, fecha_hasta = get_month_range('2024-02')

        assert fecha_desde == '2024-02-01'
        assert fecha_hasta == '2024-02-29'

    def test_mes_diciembre(self):
        """Debe retornar el rango correcto para diciembre."""
        from orchestrator import get_month_range

        fecha_desde, fecha_hasta = get_month_range('2025-12')

        assert fecha_desde == '2025-12-01'
        assert fecha_hasta == '2025-12-31'

    def test_mes_abril_30_dias(self):
        """Debe retornar 30 días para meses de 30 días."""
        from orchestrator import get_month_range

        fecha_desde, fecha_hasta = get_month_range('2025-04')

        assert fecha_desde == '2025-04-01'
        assert fecha_hasta == '2025-04-30'

    def test_formato_salida_iso(self):
        """Debe retornar fechas en formato ISO (YYYY-MM-DD)."""
        from orchestrator import get_month_range

        fecha_desde, fecha_hasta = get_month_range('2025-06')

        # Verificar formato
        assert len(fecha_desde) == 10
        assert len(fecha_hasta) == 10
        assert fecha_desde[4] == '-'
        assert fecha_desde[7] == '-'
