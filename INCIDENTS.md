# Registro de Incidentes y Bugs Resueltos

Historial de bugs encontrados, su causa raiz y la solucion aplicada.

---

## INC-001: GRIMALDO aparecia en todas las sucursales
**Fecha:** 2026-02-14
**Severidad:** Critica
**Commit:** `987440b`

**Sintoma:** El vendedor SALVA ELIAS GRIMALDO (id_personal=2, id_sucursal=5) aparecia asignado a clientes de las 13 sucursales en `gold.dim_cliente`, cuando solo deberia estar en sucursal 5 (METAN).

**Causa raiz:** Dos errores encadenados:
1. `silver.staff` tenia constraint `UNIQUE(id_personal)` en vez de `UNIQUE(id_personal, id_sucursal)`. Esto descartaba preventistas con mismo `id_personal` en otras sucursales, dejando solo a GRIMALDO.
2. Los CTEs de `gold.dim_cliente` (rutas_fv1 y rutas_fv4) hacian `JOIN silver.staff s ON r.id_personal = s.id_personal` sin filtrar por `id_sucursal`, causando cross-join entre sucursales.

**Solucion:**
- Migracion `20260214200903`: DROP `UNIQUE(id_personal)`, ADD `UNIQUE(id_personal, id_sucursal)`
- `staff_transformer.py`: DISTINCT ON y ON CONFLICT ahora usan `(id_personal, id_sucursal)`
- `dim_cliente.py`: Agregado `AND r.id_sucursal = s.id_sucursal` a ambos JOINs routes-staff
- DDL actualizado

**Leccion:** En este proyecto, siempre que se haga JOIN con `id_personal`, `id_ruta` u otros IDs de negocio, incluir `id_sucursal` como parte del JOIN.

**Documentacion:** `docs/bugfix_staff_unique_constraint.md`

---

## INC-002: dim_cliente error cf.id_sucursal does not exist
**Fecha:** 2026-02-14
**Severidad:** Media (error en produccion, fix rapido)
**Commit:** `987440b` (correccion posterior)

**Sintoma:** Al ejecutar `gold dim_cliente` despues del fix INC-001:
```
psycopg2.errors.UndefinedColumn: column cf.id_sucursal does not exist
```

**Causa raiz:** Al corregir los JOINs en dim_cliente (INC-001), se agrego `cf.id_sucursal = r.id_sucursal` pero `silver.client_forces` no tiene columna `id_sucursal`. La sucursal viene implicita de la ruta.

**Solucion:** Se quito `AND cf.id_sucursal = r.id_sucursal` del JOIN, dejando solo el JOIN correcto `r.id_sucursal = s.id_sucursal` entre routes y staff.

**Leccion:** `client_forces` no tiene `id_sucursal`. Para obtener la sucursal de una asignacion cliente-ruta, hacer JOIN a `silver.routes`.

---

## INC-003: Transaccion idle bloquea dim_cliente
**Fecha:** 2026-02-14
**Severidad:** Media

**Sintoma:** `python3 orchestrator.py gold dim_cliente` se quedaba colgado indefinidamente.

**Causa raiz:** Una ejecucion anterior habia fallado (INC-002) y dejo una transaccion `idle in transaction` con un lock sobre `gold.dim_cliente`. Habia 5 procesos bloqueados esperando el lock:
- 1 SELECT (idle in transaction, 14 min)
- 1 ALTER TABLE (migracion de telefonos)
- 2 DELETE FROM gold.dim_cliente
- 1 SELECT DISTINCT

**Solucion:** Se terminaron los 5 procesos con `pg_terminate_backend(pid)`.

**Leccion:** Si un proceso ETL falla, verificar que no queden transacciones abiertas con:
```sql
SELECT pid, state, query, age(clock_timestamp(), query_start) AS duration
FROM pg_stat_activity
WHERE datname = 'medallion_db' AND state != 'idle';
```

---

## INC-004: Migracion duplicada de telefonos
**Fecha:** 2026-02-14
**Severidad:** Baja
**Commit:** `1783319`

**Sintoma:** Existian dos migraciones identicas para agregar telefonos a `gold.dim_cliente`:
- `20260212200903_add_telefonos_dim_cliente.sql`
- `20260214201500_add_telefonos_dim_cliente.sql`

**Causa raiz:** La primera migracion se creo en un commit anterior. Al hacer el fix de staff se creo una segunda por error sin verificar que ya existia.

**Solucion:** Se elimino la duplicada (`20260214201500`).

**Leccion:** Antes de crear una migracion, verificar las existentes con `ls sql/migrations/`.

---

## INC-005: Commits en main en vez de dev
**Fecha:** 2026-02-11
**Severidad:** Baja

**Sintoma:** Los tests unitarios se commitearon en `main` en vez de `dev`.

**Causa raiz:** Despues de un reinicio del PC, el branch activo era `main` y no se verifico antes de commitear.

**Solucion:** Cherry-pick del commit a dev (`git cherry-pick 8ae54c9`), luego reset de main (`git reset --hard origin/main`).

**Leccion:** Siempre verificar el branch activo antes de commitear (`git branch --show-current`). Regla agregada a CLAUDE.md.

---

## INC-006: Push no autorizado a production
**Fecha:** 2026-02-14
**Severidad:** Baja

**Sintoma:** `git push production main` rechazado por non-fast-forward. Production tenia un commit (`ec0d15f`) que fue revertido localmente con soft reset.

**Causa raiz:** Se habia pusheado `ec0d15f` a production antes de hacer el soft reset local para reorganizar los commits en dev.

**Solucion:** Force push autorizado: `git push production main --force`.

**Leccion:** Si se necesita reorganizar commits, hacerlo ANTES de pushear a remotes.

---

## INC-007: Tests fallidos por patron SQL incorrecto
**Fecha:** 2026-02-11
**Severidad:** Baja
**Commit:** `1c07a77`

**Sintoma:** 5 tests fallaban al crear la suite de tests:
- Tests buscaban `BETWEEN` en SQL pero el codigo usa `>= AND <=`
- `fetchone.return_value = (0,)` causaba early return ("Sin datos")
- Tests sin parametros no ejecutaban DELETE

**Causa raiz:** Los tests asumian patrones SQL que no coincidian con la implementacion real.

**Solucion:**
- Cambiar asserts para buscar `fecha_comprobante` en vez de `BETWEEN`
- Usar `fetchone.return_value = (50,)` para evitar early return
- Testear con `full_refresh=True` para forzar DELETE

**Leccion:** Al escribir tests, leer el codigo fuente primero para verificar los patrones SQL exactos.
