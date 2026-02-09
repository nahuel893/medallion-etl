 Tutorial dbmate

  El problema que resuelve

  Hoy tu DDL vive en sql/setup_medallion.sql. Si mañana necesitás agregar una columna, tenés
  que:
  1. Editar el archivo SQL
  2. Acordarte de ejecutar el ALTER TABLE en la BD local
  3. Acordarte de ejecutarlo en producción
  4. Rezar que no se te olvide ningún paso

  dbmate resuelve esto: cada cambio a la BD es un archivo SQL con fecha, y dbmate sabe cuáles
  ya se ejecutaron y cuáles faltan.

  ---
  Concepto central: migraciones

  Una migración = un archivo SQL con dos secciones:

  -- migrate:up
  ALTER TABLE silver.fact_ventas ADD COLUMN descuento NUMERIC(15,4);

  -- migrate:down
  ALTER TABLE silver.fact_ventas DROP COLUMN descuento;

  - up → el cambio que querés hacer
  - down → cómo deshacerlo (rollback)

  Los archivos se guardan en sql/migrations/ con un timestamp:

  sql/migrations/
  ├── 20260208035511_baseline.sql      ← DDL inicial completo
  ├── 20260210120000_add_descuento.sql ← futuro cambio
  └── 20260215090000_add_tabla_x.sql   ← otro cambio

  ---
  Cómo sabe dbmate qué ya se ejecutó

  dbmate creó una tabla schema_migrations en tu BD:

  SELECT * FROM schema_migrations;
  --     version
  -- ----------------
  --  20260208035511    ← baseline ya aplicada

  Cuando ejecutás dbmate up, compara los archivos en sql/migrations/ contra esta tabla. Si un
  timestamp no está en la tabla, lo ejecuta.

  ---
  Los 4 comandos que vas a usar

  1. status — Ver qué está aplicado y qué falta

  npx dbmate --migrations-dir "./sql/migrations" --no-dump-schema status

  [X] 20260208035511_baseline.sql        ← aplicada
  [ ] 20260210120000_add_descuento.sql   ← pendiente

  2. new — Crear una migración nueva

  npx dbmate --migrations-dir "./sql/migrations" new add_descuento

  Crea el archivo sql/migrations/20260210120000_add_descuento.sql con el esqueleto:

  -- migrate:up

  -- migrate:down

  Vos completás ambas secciones.

  3. up — Aplicar migraciones pendientes

  npx dbmate --migrations-dir "./sql/migrations" --no-dump-schema up

  Ejecuta todas las migraciones pendientes en orden cronológico.

  4. rollback — Deshacer la última migración

  npx dbmate --migrations-dir "./sql/migrations" --no-dump-schema rollback

  Ejecuta el migrate:down de la última migración aplicada.

  ---
  Ejemplo práctico completo

  Supongamos que necesitás agregar un campo descuento a silver.fact_ventas.

  Paso 1: Crear la migración

  npx dbmate --migrations-dir "./sql/migrations" new add_descuento_fact_ventas

  Paso 2: Editar el archivo generado

  -- migrate:up
  ALTER TABLE silver.fact_ventas ADD COLUMN descuento NUMERIC(15,4) DEFAULT 0;

  -- migrate:down
  ALTER TABLE silver.fact_ventas DROP COLUMN descuento;

  Paso 3: Verificar que está pendiente

  npx dbmate --migrations-dir "./sql/migrations" --no-dump-schema status
  # [X] 20260208035511_baseline.sql
  # [ ] 20260210120000_add_descuento_fact_ventas.sql  ← pendiente

  Paso 4: Aplicar

  npx dbmate --migrations-dir "./sql/migrations" --no-dump-schema up
  # Applying: 20260210120000_add_descuento_fact_ventas.sql
  # Applied

  Paso 5: Si algo salió mal, revertir

  npx dbmate --migrations-dir "./sql/migrations" --no-dump-schema rollback
  # Rolling back: 20260210120000_add_descuento_fact_ventas.sql

  ---
  Reglas importantes
  ┌────────────────────────────────────────┬────────────────────────────────────────────────┐
  │                 Regla                  │                    Por qué                     │
  ├────────────────────────────────────────┼────────────────────────────────────────────────┤
  │ Nunca editar una migración ya aplicada │ dbmate no la re-ejecuta, el cambio se pierde   │
  ├────────────────────────────────────────┼────────────────────────────────────────────────┤
  │ Siempre escribir el migrate:down       │ Si no, no podés hacer rollback                 │
  ├────────────────────────────────────────┼────────────────────────────────────────────────┤
  │ Un cambio = una migración              │ No mezclar cosas distintas en el mismo archivo │
  ├────────────────────────────────────────┼────────────────────────────────────────────────┤
  │ Commitear las migraciones a git        │ Así producción recibe los mismos cambios       │
  └────────────────────────────────────────┴────────────────────────────────────────────────┘
  ---
  Flujo con producción

  Local                           Producción
  ─────                           ──────────
  1. Crear migración
  2. Escribir SQL
  3. dbmate up (probar local)
  4. git commit + push
                                  5. git pull
                                  6. dbmate up (aplica lo pendiente)

  dbmate en producción solo ejecuta las migraciones que le faltan. Si local tiene 5 aplicadas
  y producción tiene 3, ejecuta las 2 que faltan.

  ---
  Tu baseline

  El archivo 20260208035511_baseline.sql es especial: contiene todo el DDL actual. Sirve como
  "foto" del punto de partida. A partir de ahora, cada cambio al esquema va en una migración
  nueva, nunca editando el baseline.
