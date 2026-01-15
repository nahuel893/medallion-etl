"""
Tests para el loader de stock (Bronze).
"""
import pytest


class TestGenerarRangosDiarios:
    """Tests para la función generar_rangos_diarios()."""

    def test_un_solo_dia(self):
        """Debe retornar una sola fecha cuando inicio y fin son iguales."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2025-01-15', '2025-01-15')

        assert len(fechas) == 1
        assert fechas[0] == '2025-01-15'

    def test_tres_dias(self):
        """Debe retornar tres fechas para un rango de 3 días."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2025-01-01', '2025-01-03')

        assert len(fechas) == 3
        assert fechas == ['2025-01-01', '2025-01-02', '2025-01-03']

    def test_cambio_de_mes(self):
        """Debe manejar correctamente el cambio de mes."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2025-01-30', '2025-02-02')

        assert len(fechas) == 4
        assert fechas == ['2025-01-30', '2025-01-31', '2025-02-01', '2025-02-02']

    def test_cambio_de_anio(self):
        """Debe manejar correctamente el cambio de año."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2024-12-30', '2025-01-02')

        assert len(fechas) == 4
        assert fechas == ['2024-12-30', '2024-12-31', '2025-01-01', '2025-01-02']

    def test_febrero_bisiesto(self):
        """Debe incluir el 29 de febrero en año bisiesto."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2024-02-28', '2024-03-01')

        assert len(fechas) == 3
        assert fechas == ['2024-02-28', '2024-02-29', '2024-03-01']

    def test_febrero_no_bisiesto(self):
        """No debe incluir el 29 de febrero en año no bisiesto."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2025-02-27', '2025-03-01')

        assert len(fechas) == 3
        assert fechas == ['2025-02-27', '2025-02-28', '2025-03-01']

    def test_mes_completo(self):
        """Debe generar 31 fechas para enero completo."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2025-01-01', '2025-01-31')

        assert len(fechas) == 31
        assert fechas[0] == '2025-01-01'
        assert fechas[30] == '2025-01-31'

    def test_formato_iso(self):
        """Debe retornar fechas en formato ISO (YYYY-MM-DD)."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2025-06-15', '2025-06-17')

        for fecha in fechas:
            assert len(fecha) == 10
            assert fecha[4] == '-'
            assert fecha[7] == '-'

    def test_orden_cronologico(self):
        """Las fechas deben estar en orden cronológico."""
        from layers.bronze.loaders.stock_loader import generar_rangos_diarios

        fechas = generar_rangos_diarios('2025-01-01', '2025-01-10')

        for i in range(len(fechas) - 1):
            assert fechas[i] < fechas[i + 1]
