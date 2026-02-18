# Plan de Despliegue - PC de Desarrollo (Arch Linux)

**Fecha:** 2026-02-18
**Contexto:** Migracion del proyecto desde portatil Debian/Ubuntu a PC de escritorio con Arch Linux.

---

## Problema

El proyecto se desarrollo en un portatil con Debian/Ubuntu. Al migrarlo a Arch Linux (rolling release), hay diferencias que impactan el despliegue:

- **Python 3.14** viene por defecto en Arch, pero varias dependencias del proyecto requieren versiones minimas especificas para soportarlo (psycopg2-binary >= 2.9.11, SQLAlchemy >= 2.0.41, pydantic >= 2.12). Usar Python del sistema sin control de version introduce riesgo de rotura silenciosa.
- **PostgreSQL 18** viene en los repos de Arch (vs 15/16 en Debian). Bajo riesgo, pero distinto proceso de inicializacion.
- **dbmate** se usaba via `npx` en Debian. En Arch conviene instalarlo como binario nativo.

La solucion es usar un **entorno virtual (venv)** con una version controlada de Python, y adaptar los scripts de instalacion.

---

## Tareas

### 1. Agregar venv/ al .gitignore

**Por que:** El directorio `venv/` contiene binarios compilados, pesa cientos de MB, y es especifico del sistema operativo y arquitectura. Nunca debe commitearse. Si alguien clona el repo, recrea el venv en segundos con `pip install -r requirements.txt`.

**Que hacer:**
- Agregar `venv/` al archivo `.gitignore`

---

### 2. Crear el venv con Python 3.12 e instalar dependencias

**Por que:** Python 3.14 (el del sistema en Arch) es muy nuevo. Varias dependencias necesitan versiones minimas que pueden no resolverse correctamente con `pip install`. Usar Python 3.12 garantiza compatibilidad total con las versiones declaradas en `requirements.txt` sin necesidad de cambiar los rangos.

Un venv aisla las dependencias del proyecto del sistema, evitando conflictos con otros proyectos o con paquetes del sistema operativo.

**Que hacer:**
1. Instalar Python 3.12 (desde AUR: `python312` o similar)
2. Crear el venv: `python3.12 -m venv venv`
3. Activar: `source venv/bin/activate`
4. Instalar dependencias: `pip install -r requirements.txt && pip install chesserp`
5. Verificar imports criticos: `python -c "import psycopg2; import sqlalchemy; import pydantic_settings"`

**Depende de:** Tarea 1 (para no commitear el venv por accidente)

---

### 3. Actualizar README con instrucciones de venv

**Por que:** El README actual no menciona entornos virtuales. Cualquiera que clone el repo (incluyendo vos en el futuro) necesita saber que debe crear un venv y con que version de Python. Sin esta documentacion, alguien podria instalar las dependencias globalmente o con la version incorrecta de Python, causando errores dificiles de diagnosticar.

**Que hacer:**
- Agregar seccion de venv en la parte de instalacion del README
- Documentar la version de Python recomendada (3.12)
- Incluir los comandos para crear, activar e instalar

---

### 4. Actualizar daily_load.py y crontab para usar Python del venv

**Por que:** `daily_load.py` se ejecuta automaticamente via crontab. El crontab por defecto usa el Python del sistema (`/usr/bin/python3`, que en Arch es 3.14). Si el ETL corre con un Python distinto al del venv, las dependencias no estaran disponibles y el proceso falla silenciosamente.

**Que hacer:**
- Modificar la entrada del crontab para que use la ruta absoluta al Python del venv:
  ```
  # Antes (usa Python del sistema)
  0 6 * * * cd /ruta/proyecto && python3 daily_load.py

  # Despues (usa Python del venv)
  0 6 * * * cd /ruta/proyecto && venv/bin/python3 daily_load.py
  ```
- Alternativa: agregar `source venv/bin/activate &&` antes del comando
- Revisar si `orchestrator.py` tiene el mismo problema (shebangs, etc.)

**Depende de:** Tarea 2 (el venv debe existir primero)

---

### 5. Desplegar la BD en esta PC

**Por que:** Sin la base de datos, el ETL no puede ejecutarse. El script `install_db_arch.sh` (creado en esta sesion) automatiza todo el proceso para Arch Linux: instala PostgreSQL, inicializa el cluster, crea la BD, aplica migraciones con dbmate, y configura los roles de usuario.

**Que hacer:**
1. Verificar que `.env` tenga las credenciales correctas para esta PC
2. Ejecutar `./install_db_arch.sh`
3. Verificar que los 3 schemas existen: `psql -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze','silver','gold');"`

**Nota:** Esta tarea es independiente del venv. Se puede hacer en paralelo con las tareas 1-3.

---

### 6. Correr tests para verificar que todo funciona

**Por que:** Es la validacion final. Los 144 tests cubren silver transformers y gold aggregators. Si pasan, confirma que las dependencias estan correctas, el codigo funciona con la version de Python del venv, y la BD esta accesible.

**Que hacer:**
1. Activar el venv: `source venv/bin/activate`
2. Ejecutar: `python -m pytest tests/ -v`
3. Esperar 144 tests passing

**Depende de:** Tareas 2 y 5 (necesita venv con deps + BD desplegada)

---

## Orden de ejecucion

```
Paralelo:  [1. .gitignore] ──> [2. Crear venv] ──> [4. daily_load/crontab]
           [3. README]                                       |
           [5. Desplegar BD] ──────────────────────> [6. Tests]
```

Las tareas 1, 3 y 5 se pueden ejecutar en paralelo. La tarea 6 es la ultima porque necesita tanto el venv (tarea 2) como la BD (tarea 5) funcionando.

---

## Riesgos y mitigaciones

| Riesgo | Mitigacion |
|---|---|
| Python 3.12 no disponible en repos Arch | Instalar desde AUR (`python312`) o compilar desde source |
| psycopg2-binary no compila | Instalar `postgresql-libs` como dependencia del sistema antes del pip install |
| dbmate no esta en repos oficiales | Instalar desde AUR (`dbmate-bin`) o descargar binario de GitHub Releases |
| daily_load falla silenciosamente con Python incorrecto | Usar ruta absoluta al binario del venv en crontab |
