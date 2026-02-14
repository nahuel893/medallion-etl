-- migrate:up
-- Fix: UNIQUE(id_personal) descartaba staff con mismo id_personal en distintas sucursales.
-- Los IDs son únicos por sucursal, no globalmente.
ALTER TABLE silver.staff DROP CONSTRAINT staff_id_personal_key;
ALTER TABLE silver.staff ADD CONSTRAINT staff_id_personal_id_sucursal_key UNIQUE (id_personal, id_sucursal);

-- migrate:down
ALTER TABLE silver.staff DROP CONSTRAINT staff_id_personal_id_sucursal_key;
ALTER TABLE silver.staff ADD CONSTRAINT staff_id_personal_key UNIQUE (id_personal);
