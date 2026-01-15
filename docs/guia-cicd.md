# Guía Completa de CI/CD

## 1. Conceptos Fundamentales

### ¿Qué es CI/CD?

**CI/CD** es un conjunto de prácticas que automatizan el ciclo de vida del software:

```
Código → Build → Test → Deploy → Monitoreo
   ↑________________________________________↓
              (ciclo continuo)
```

Se divide en tres conceptos relacionados:

| Término | Significado | Qué automatiza |
|---------|-------------|----------------|
| **CI** (Continuous Integration) | Integración Continua | Merge de código + tests automáticos |
| **CD** (Continuous Delivery) | Entrega Continua | Preparar releases automáticamente |
| **CD** (Continuous Deployment) | Despliegue Continuo | Deploy automático a producción |

---

## 2. Continuous Integration (CI)

### Problema que resuelve

Sin CI:
```
Desarrollador A trabaja 2 semanas en feature X
Desarrollador B trabaja 2 semanas en feature Y
                    ↓
    Intentan unir código → CONFLICTOS MASIVOS
                    ↓
         "Integration Hell" - días arreglando
```

Con CI:
```
Desarrollador A hace commits pequeños diarios
Desarrollador B hace commits pequeños diarios
                    ↓
    Cada commit se integra y testea automáticamente
                    ↓
         Conflictos pequeños, fáciles de resolver
```

### Principios de CI

1. **Repositorio único**: Todo el código en un solo lugar (Git)
2. **Commits frecuentes**: Mínimo una vez al día
3. **Build automatizado**: Un comando construye todo el proyecto
4. **Tests automatizados**: Cada build ejecuta tests
5. **Feedback rápido**: Saber en minutos si algo se rompió

### Pipeline de CI típico

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   COMMIT    │────▶│    BUILD    │────▶│    TEST     │────▶│   REPORT    │
│             │     │             │     │             │     │             │
│ git push    │     │ Instalar    │     │ Unit tests  │     │ ✓ Pass      │
│             │     │ dependencias│     │ Linting     │     │ ✗ Fail      │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### Tipos de Tests en CI

```
                    ▲
                   /│\      Tests E2E (End-to-End)
                  / │ \     - Flujo completo
                 /  │  \    - Lentos, pocos
                /   │   \
               /────┼────\  Tests de Integración
              /     │     \ - Componentes juntos
             /      │      \- Velocidad media
            /───────┼───────\
           /        │        \ Tests Unitarios
          /         │         \- Funciones aisladas
         /──────────┼──────────\- Rápidos, muchos
        ────────────────────────
              PIRÁMIDE DE TESTS
```

---

## 3. Continuous Delivery vs Continuous Deployment

### Continuous Delivery (Entrega Continua)

```
Commit → Build → Test → Staging → [BOTÓN MANUAL] → Producción
                                        ↑
                                  Humano decide
                                  cuándo deployar
```

- El código **siempre está listo** para producción
- Un humano **aprueba** el deploy final
- Útil cuando necesitas control (regulaciones, QA manual)

### Continuous Deployment (Despliegue Continuo)

```
Commit → Build → Test → Staging → Test → Producción
                                    ↑
                              Automático si
                              tests pasan
```

- **Todo commit** que pasa tests va a producción
- Requiere tests muy robustos
- Deploys muy frecuentes (varias veces al día)

---

## 4. Ambientes (Environments)

### Concepto de Ambientes

Un **ambiente** es un entorno aislado donde ejecutas tu aplicación:

```
┌─────────────────────────────────────────────────────────────────┐
│                         AMBIENTES                                │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────┤
│    LOCAL    │     DEV     │   STAGING   │     PROD    │   DR    │
├─────────────┼─────────────┼─────────────┼─────────────┼─────────┤
│ Tu PC       │ Servidor    │ Réplica de  │ Producción  │Disaster │
│             │ compartido  │ producción  │ real        │Recovery │
├─────────────┼─────────────┼─────────────┼─────────────┼─────────┤
│ Datos fake  │ Datos fake  │ Copia de    │ Datos       │ Backup  │
│ o sample    │ o sample    │ datos reales│ reales      │         │
├─────────────┼─────────────┼─────────────┼─────────────┼─────────┤
│ Desarrollo  │ Integración │ QA/Testing  │ Usuarios    │ Failover│
│ individual  │ equipo      │ final       │ finales     │         │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────┘
```

### Variables de Ambiente

Cada ambiente tiene su propia configuración:

```
LOCAL (.env.local)          PROD (.env.prod)
─────────────────           ─────────────────
DB_HOST=localhost           DB_HOST=prod-server.com
DB_NAME=medallion_dev       DB_NAME=medallion_prod
DB_USER=nahuel              DB_USER=etl_service
DEBUG=true                  DEBUG=false
```

**Principio clave**: El código es **idéntico**, solo cambian las variables de ambiente.

---

## 5. Pipeline de CD

### Etapas de un Pipeline Completo

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE CI/CD                                  │
└──────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────┐
│ 1. SOURCE     │  ← Trigger: push a rama, PR, tag, schedule
│    Checkout   │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 2. BUILD      │  ← Instalar dependencias, compilar si aplica
│    Install    │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 3. QUALITY    │  ← Linting, formateo, análisis estático
│    Lint/Fmt   │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 4. TEST       │  ← Unit tests, integration tests
│    Pytest     │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 5. SECURITY   │  ← Escaneo de vulnerabilidades (opcional)
│    Scan       │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 6. PACKAGE    │  ← Crear artefacto (Docker image, zip, etc)
│    Docker     │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 7. DEPLOY     │  ← Subir a servidor staging
│    Staging    │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 8. SMOKE TEST │  ← Verificar que staging funciona
│    Health     │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 9. DEPLOY     │  ← Subir a producción (manual o auto)
│    Production │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ 10. VERIFY    │  ← Monitoreo post-deploy
│    Monitor    │
└───────────────┘
```

---

## 6. Estrategias de Deployment

### 6.1 Big Bang (No recomendado)

```
Servidor PROD:  [Versión 1] ─────STOP────▶ [Versión 2]
                                  ↑
                            Downtime total
```

- Simple pero riesgoso
- Si falla, usuarios sin servicio

### 6.2 Rolling Deployment

```
Servidor 1:  [V1] → [V2]
Servidor 2:  [V1] ────→ [V2]
Servidor 3:  [V1] ───────→ [V2]
                ↑
          Gradual, sin downtime
```

- Actualiza servidores uno a uno
- Siempre hay servidores funcionando

### 6.3 Blue-Green Deployment

```
         Load Balancer
              │
     ┌────────┴────────┐
     ▼                 ▼
┌─────────┐      ┌─────────┐
│  BLUE   │      │  GREEN  │
│  (V1)   │      │  (V2)   │
│ ACTIVO  │      │ STANDBY │
└─────────┘      └─────────┘

Después del switch:

         Load Balancer
              │
     ┌────────┴────────┐
     ▼                 ▼
┌─────────┐      ┌─────────┐
│  BLUE   │      │  GREEN  │
│  (V1)   │      │  (V2)   │
│ STANDBY │      │ ACTIVO  │
└─────────┘      └─────────┘
```

- Dos ambientes idénticos
- Switch instantáneo
- Rollback fácil (volver al otro)

### 6.4 Canary Deployment

```
                    Usuarios
                       │
              ┌────────┴────────┐
              │ Load Balancer   │
              │   90% / 10%     │
              └────────┬────────┘
         ┌─────────────┴─────────────┐
         ▼                           ▼
    ┌─────────┐                ┌─────────┐
    │ STABLE  │                │ CANARY  │
    │  (V1)   │                │  (V2)   │
    │   90%   │                │   10%   │
    └─────────┘                └─────────┘
```

- Nueva versión a % pequeño de usuarios
- Monitorear errores
- Incrementar gradualmente si todo va bien

---

## 7. CI/CD para ETL (Caso Medallion)

### Diferencias con aplicaciones web

| Aspecto | App Web | ETL Pipeline |
|---------|---------|--------------|
| **Deploy** | Servidor web corriendo | Scripts + Scheduler |
| **Tests** | Endpoints, UI | Transformaciones de datos |
| **Rollback** | Volver a versión anterior | + Revertir datos procesados |
| **Scheduling** | Siempre corriendo | Ejecución periódica (cron) |

### Pipeline CI/CD para ETL

```
┌─────────────────────────────────────────────────────────────────┐
│                    CI/CD PARA MEDALLION ETL                      │
└─────────────────────────────────────────────────────────────────┘

CONTINUOUS INTEGRATION
──────────────────────
    │
    ├── 1. Lint (flake8, black)
    │       └── ¿Código bien formateado?
    │
    ├── 2. Unit Tests
    │       ├── Test loaders (Bronze)
    │       ├── Test transformers (Silver)
    │       └── Test aggregators (Gold)
    │
    ├── 3. Integration Tests (con BD de prueba)
    │       ├── Test conexión BD
    │       ├── Test pipeline completo con datos sample
    │       └── Test idempotencia (correr 2 veces = mismo resultado)
    │
    └── 4. Data Quality Tests
            ├── Schemas correctos
            ├── Tipos de datos
            └── Reglas de negocio


CONTINUOUS DELIVERY
───────────────────
    │
    ├── 5. Package
    │       └── Crear artefacto deployable
    │
    ├── 6. Deploy a Staging
    │       ├── Copiar scripts al servidor
    │       ├── Configurar variables de ambiente
    │       └── Setup BD staging
    │
    ├── 7. Test en Staging
    │       └── Correr ETL con datos de prueba
    │
    └── 8. Deploy a Producción
            ├── Copiar scripts al servidor prod
            ├── Configurar cron/scheduler
            └── Verificar ejecución
```

---

## 8. Caso: Local → Servidor

### Situación Actual

```
┌─────────────────────────────────────────┐
│              TU PC (LOCAL)              │
│                                         │
│   ┌─────────────┐    ┌──────────────┐  │
│   │   Scripts   │───▶│  PostgreSQL  │  │
│   │ Python ETL  │    │  medallion_db│  │
│   └─────────────┘    └──────────────┘  │
│                                         │
│   - Desarrollo manual                   │
│   - Ejecución manual                    │
│   - Sin versionado de deploys          │
└─────────────────────────────────────────┘
```

### Situación Objetivo

```
┌─────────────────────┐         ┌─────────────────────────────────┐
│    TU PC (LOCAL)    │         │         SERVIDOR PROD           │
│                     │         │                                 │
│  ┌───────────────┐  │         │  ┌─────────────┐  ┌──────────┐ │
│  │ Desarrollo    │  │         │  │   Scripts   │─▶│ PostgreSQL│ │
│  │ + Git         │  │         │  │  Python ETL │  │medallion │ │
│  └───────┬───────┘  │         │  └─────────────┘  └──────────┘ │
│          │          │         │         ▲                      │
└──────────┼──────────┘         │         │                      │
           │                    │  ┌──────┴──────┐               │
           │                    │  │    Cron     │               │
           ▼                    │  │  Scheduler  │               │
    ┌─────────────┐             │  └─────────────┘               │
    │   GitHub    │             └─────────────────────────────────┘
    │ Repository  │                        ▲
    └──────┬──────┘                        │
           │                               │
           ▼                               │
    ┌─────────────┐      Automático        │
    │  CI/CD      │────────────────────────┘
    │  Pipeline   │   (deploy cuando hay push)
    └─────────────┘
```

### Flujo Automatizado

```
1. Desarrollas en tu PC
         │
         ▼
2. git push origin main
         │
         ▼
3. GitHub detecta el push
         │
         ▼
4. CI Pipeline se ejecuta:
   ├── Instala dependencias
   ├── Corre linting
   ├── Corre tests
   └── Si todo pasa → continúa
         │
         ▼
5. CD Pipeline se ejecuta:
   ├── Conecta al servidor via SSH
   ├── Hace git pull en el servidor
   ├── Instala/actualiza dependencias
   ├── Reinicia servicios si necesario
   └── Notifica éxito/fallo
         │
         ▼
6. ETL corre en servidor según schedule
```

---

## 9. Componentes Necesarios

### Para implementar CI/CD necesitas:

```
┌─────────────────────────────────────────────────────────────────┐
│                    COMPONENTES NECESARIOS                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. REPOSITORIO GIT (GitHub, GitLab, Bitbucket)                 │
│     └── Donde vive tu código                                    │
│                                                                  │
│  2. SERVIDOR DE CI/CD                                           │
│     ├── GitHub Actions (gratis para repos públicos)             │
│     ├── GitLab CI (incluido en GitLab)                          │
│     ├── Jenkins (self-hosted)                                   │
│     └── Otros: CircleCI, Travis, etc.                           │
│                                                                  │
│  3. SERVIDOR DE PRODUCCIÓN                                      │
│     ├── VPS (DigitalOcean, Linode, AWS EC2)                    │
│     ├── Servidor propio                                         │
│     └── Acceso SSH configurado                                  │
│                                                                  │
│  4. SECRETOS/CREDENCIALES                                       │
│     ├── SSH keys para acceso al servidor                        │
│     ├── Credenciales de BD                                      │
│     └── API keys si aplica                                      │
│                                                                  │
│  5. TESTS                                                       │
│     └── Suite de tests que validen el ETL                       │
│                                                                  │
│  6. CONFIGURACIÓN POR AMBIENTE                                  │
│     ├── .env.local (tu PC)                                      │
│     ├── .env.staging (si tienes)                                │
│     └── .env.prod (servidor)                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 10. Resumen

```
CI/CD = Automatizar el camino del código desde tu PC hasta producción

CI (Integración Continua):
  - Cada push → tests automáticos
  - Detectar errores temprano
  - Código siempre integrado

CD (Entrega/Despliegue Continuo):
  - Código listo para producción siempre
  - Deploy automatizado (con o sin aprobación manual)
  - Ambientes consistentes

Para tu caso (ETL Local → Servidor):
  1. Código en GitHub
  2. GitHub Actions como CI/CD
  3. Deploy via SSH al servidor
  4. Scheduler (cron) ejecuta el ETL
```
