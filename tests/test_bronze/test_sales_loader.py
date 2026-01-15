"""
Tests para el loader de ventas (Bronze).
"""
import pytest


class TestGenerarRangosMensuales:
    """Tests para la función generar_rangos_mensuales()."""

    def test_mismo_mes(self):
        """Debe retornar un solo rango cuando las fechas están en el mismo mes."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2025-01-10', '2025-01-20')

        assert len(rangos) == 1
        assert rangos[0] == ('2025-01-10', '2025-01-20')

    def test_dos_meses_completos(self):
        """Debe generar rangos correctos para dos meses."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2025-01-01', '2025-02-28')

        assert len(rangos) == 2
        assert rangos[0] == ('2025-01-01', '2025-01-31')
        assert rangos[1] == ('2025-02-01', '2025-02-28')

    def test_tres_meses_parciales(self):
        """Debe manejar correctamente meses parciales al inicio y fin."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2025-01-15', '2025-03-20')

        assert len(rangos) == 3
        assert rangos[0] == ('2025-01-15', '2025-01-31')  # Parcial inicio
        assert rangos[1] == ('2025-02-01', '2025-02-28')  # Completo
        assert rangos[2] == ('2025-03-01', '2025-03-20')  # Parcial fin

    def test_un_solo_dia(self):
        """Debe funcionar con un solo día."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2025-01-15', '2025-01-15')

        assert len(rangos) == 1
        assert rangos[0] == ('2025-01-15', '2025-01-15')

    def test_cambio_de_anio(self):
        """Debe manejar correctamente el cambio de año."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2024-12-15', '2025-01-15')

        assert len(rangos) == 2
        assert rangos[0] == ('2024-12-15', '2024-12-31')
        assert rangos[1] == ('2025-01-01', '2025-01-15')

    def test_febrero_bisiesto(self):
        """Debe manejar correctamente febrero en año bisiesto."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2024-02-01', '2024-02-29')

        assert len(rangos) == 1
        assert rangos[0] == ('2024-02-01', '2024-02-29')

    def test_anio_completo(self):
        """Debe generar 12 rangos para un año completo."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2025-01-01', '2025-12-31')

        assert len(rangos) == 12
        assert rangos[0] == ('2025-01-01', '2025-01-31')
        assert rangos[11] == ('2025-12-01', '2025-12-31')

    def test_orden_cronologico(self):
        """Los rangos deben estar en orden cronológico."""
        from layers.bronze.loaders.sales_loader import generar_rangos_mensuales

        rangos = generar_rangos_mensuales('2025-01-01', '2025-06-30')

        for i in range(len(rangos) - 1):
            fecha_fin_actual = rangos[i][1]
            fecha_inicio_siguiente = rangos[i + 1][0]
            # El día siguiente al fin debe ser el inicio del siguiente rango
            assert fecha_fin_actual < fecha_inicio_siguiente
