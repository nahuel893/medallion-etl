# Bugfix: staff UNIQUE constraint y JOINs sin id_sucursal

## Fecha: 2026-02-14

## Descripcion del error

El preventista **SALVA ELIAS GRIMALDO** (id_personal=2, id_sucursal=5) aparecia asignado a clientes de **todas las sucursales** en `gold.dim_cliente`, cuando en realidad solo deberia aparecer en sucursal 5 (METAN).

## Causa raiz

Dos errores relacionados con la regla del proyecto: **"IDs son unicos por sucursal, no globalmente"**.

### Error 1: UNIQUE(id_personal) en silver.staff

La tabla `silver.staff` tenia un constraint `UNIQUE(id_personal)` que trataba el `id_personal` como globalmente unico. En realidad, el mismo `id_personal` puede existir en distintas sucursales (son personas diferentes).

**Consecuencia:** El `ON CONFLICT (id_personal)` del transformer descartaba registros de staff con el mismo `id_personal` en otras sucursales. Solo quedaba uno (GRIMALDO con id_personal=2 en sucursal 5), y las demas sucursales que tenian otro preventista con id_personal=2 perdian su registro.

**Fix:** Cambiar a `UNIQUE(id_personal, id_sucursal)`.

### Error 2: JOINs sin id_sucursal en gold.dim_cliente

Los CTEs `rutas_fv1` y `rutas_fv4` en `dim_cliente.py` hacian:

```sql
-- ANTES (incorrecto)
JOIN silver.routes r ON cf.id_ruta = r.id_ruta
JOIN silver.staff s ON r.id_personal = s.id_personal
```

Esto hacia cross-join entre sucursales: una ruta de sucursal 1 con id_personal=2 matcheaba con GRIMALDO (sucursal 5) porque era el unico con ese id en la tabla.

**Fix:** Agregar `id_sucursal` a todos los JOINs:

```sql
-- DESPUES (correcto)
JOIN silver.routes r ON cf.id_ruta = r.id_ruta
    AND cf.id_sucursal = r.id_sucursal
JOIN silver.staff s ON r.id_personal = s.id_personal
    AND r.id_sucursal = s.id_sucursal
```

## Archivos modificados

| Archivo | Cambio |
|---------|--------|
| `sql/migrations/20260214200903_fix_staff_unique_constraint.sql` | Migracion: DROP UNIQUE(id_personal), ADD UNIQUE(id_personal, id_sucursal) |
| `sql/setup_medallion.sql` | DDL actualizado con nuevo constraint |
| `src/layers/silver/transformers/staff_transformer.py` | DISTINCT ON y ON CONFLICT ahora usan (id_personal, id_sucursal) |
| `src/layers/gold/aggregators/dim_cliente.py` | JOINs con id_sucursal en ambos CTEs (rutas_fv1 y rutas_fv4) |

## Pasos para aplicar

1. Aplicar migracion (cambia constraint en silver.staff)
2. Re-ejecutar silver staff (recarga todos los preventistas por sucursal)
3. Re-ejecutar gold dim_cliente (recalcula asignaciones correctas)

## Leccion aprendida

En este proyecto, **siempre que se haga JOIN entre tablas que contienen id_personal, id_ruta, id_cliente u otros IDs de negocio, se debe incluir id_sucursal** como parte del JOIN. Los IDs no son globalmente unicos, son unicos por sucursal.
