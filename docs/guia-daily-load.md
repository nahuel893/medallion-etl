# Guía - Daily Load (Carga Diaria Automatizada)

## Descripción

`daily_load.py` ejecuta el pipeline completo Bronze → Silver → Gold de forma automatizada. Está diseñado para correr diariamente via crontab en el servidor.

---

## Uso

```bash
# Fecha de hoy (default)
python3 daily_load.py

# Fecha específica
python3 daily_load.py 2025-06-15
```

---

## Lógica de Ventas

El script maneja dos escenarios según el día del mes:

| Día del mes | Meses que recarga |
|-------------|-------------------|
| 1, 2 o 3   | Mes anterior + mes actual |
| 4 en adelante | Solo mes actual |

Esto garantiza que las ventas de fin de mes cargadas con retraso queden reflejadas.

---

## Fases del Pipeline

| Fase | Descripción | Frecuencia |
|------|-------------|------------|
| 1 | Bronze Masters (clientes, staff, rutas, artículos, depósitos, marketing) | Full refresh |
| 2 | Bronze Ventas | Mes actual (+ anterior si día <= 3) |
| 3 | Bronze Stock | Solo fecha del día |
| 4 | Silver Masters (branches, sales_forces, staff, routes, clients, client_forces, articles, article_groupings, marketing) | Full refresh |
| 5 | Silver Ventas | Mes actual (+ anterior si día <= 3) |
| 6 | Silver Stock | Solo fecha del día |
| 7 | Gold Dimensiones (tiempo, sucursal, vendedor, artículo, cliente) | Full refresh |
| 8 | Gold Fact Ventas | Mes actual (+ anterior si día <= 3) |
| 9 | Gold Fact Stock | Solo fecha del día |
| 10 | Gold Cobertura (preventista/marca, sucursal/marca, preventista/genérico) | Mes actual |

---

## Manejo de Errores

- Cada fase se ejecuta de forma independiente dentro de un `try/except`
- Si una fase falla, se loguea el error y se continúa con la siguiente
- Al final se muestra un resumen con las fases que fallaron
- **Exit code 0**: todas las fases completadas
- **Exit code 1**: una o más fases tuvieron errores

---

## Configuración con Crontab

```bash
# Editar crontab
crontab -e

# Todos los días a las 5:00 AM
0 5 * * * cd /srv/app/medallion-etl && /usr/bin/python3 daily_load.py >> /var/log/medallion-etl/daily.log 2>&1
```

### Prerequisitos en el servidor

```bash
# Crear directorio de logs
sudo mkdir -p /var/log/medallion-etl
sudo chown $USER:$USER /var/log/medallion-etl
```

### Rotación de logs (opcional)

Crear `/etc/logrotate.d/medallion-etl`:

```
/var/log/medallion-etl/*.log {
    weekly
    rotate 8
    compress
    missingok
    notifempty
}
```

---

## Ejemplo de Output

```
============================================================
DAILY LOAD: Inicio - Fecha referencia: 2025-06-02
  Mes actual: 2025-06-01 - 2025-06-30
  Mes anterior (dia <= 3): 2025-05-01 - 2025-05-31
============================================================
--- FASE 1: BRONZE MASTERS: Iniciando ---
--- FASE 1: BRONZE MASTERS: OK (45.2s) ---
--- FASE 2a: BRONZE VENTAS (mes anterior): Iniciando ---
--- FASE 2a: BRONZE VENTAS (mes anterior): OK (120.3s) ---
--- FASE 2b: BRONZE VENTAS (mes actual): Iniciando ---
--- FASE 2b: BRONZE VENTAS (mes actual): OK (8.1s) ---
...
============================================================
DAILY LOAD: Fin - Duracion total: 312.5s
  Todas las fases completadas exitosamente
============================================================
```

---

## Ejecución con Fecha Específica

Útil para recargar un día puntual o para testing:

```bash
# Simular ejecución del día 1 (recarga 2 meses)
python3 daily_load.py 2025-06-01

# Simular ejecución del día 15 (recarga solo mes actual)
python3 daily_load.py 2025-06-15
```
