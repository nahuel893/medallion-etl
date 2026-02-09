 Guía de Comandos del Orchestrator

  BRONZE (Ingesta de datos crudos desde API)

  # Ventas (requiere fechas)
  python3 orchestrator.py bronze sales 2025-01-01 2025-12-31

  # Stock (requiere fechas)
  python3 orchestrator.py bronze stock 2025-01-01 2025-12-31

  # Masters (sin fechas)
  python3 orchestrator.py bronze clientes
  python3 orchestrator.py bronze staff
  python3 orchestrator.py bronze routes
  python3 orchestrator.py bronze articles
  python3 orchestrator.py bronze depositos
  python3 orchestrator.py bronze marketing

  # Todos los masters juntos
  python3 orchestrator.py bronze masters

  SILVER (Transformación y limpieza)

  # Ventas (con fechas opcional, --full-refresh opcional)
  python3 orchestrator.py silver sales 2025-01-01 2025-12-31
  python3 orchestrator.py silver sales --full-refresh

  # Stock (con fechas opcional)
  python3 orchestrator.py silver stock 2025-01-01 2025-12-31
  python3 orchestrator.py silver stock --full-refresh

  # Masters (siempre full refresh)
  python3 orchestrator.py silver clients
  python3 orchestrator.py silver articles
  python3 orchestrator.py silver staff
  python3 orchestrator.py silver routes
  python3 orchestrator.py silver client_forces
  python3 orchestrator.py silver branches
  python3 orchestrator.py silver sales_forces
  python3 orchestrator.py silver article_groupings
  python3 orchestrator.py silver marketing

  # Todos los masters juntos
  python3 orchestrator.py silver masters

  GOLD (Agregaciones y modelo dimensional)

  # Dimensiones
  python3 orchestrator.py gold dim_tiempo 2020-01-01 2030-12-31
  python3 orchestrator.py gold dim_sucursal
  python3 orchestrator.py gold dim_vendedor
  python3 orchestrator.py gold dim_articulo
  python3 orchestrator.py gold dim_cliente
  python3 orchestrator.py gold dimensions  # Todas las dimensiones

  # Hechos
  python3 orchestrator.py gold fact_ventas 2025-01-01 2025-12-31
  python3 orchestrator.py gold fact_ventas --full-refresh
  python3 orchestrator.py gold fact_stock 2025-01-01 2025-12-31

  # Cobertura (periodo = YYYY-MM)
  python3 orchestrator.py gold cobertura                    # Full refresh
  python3 orchestrator.py gold cobertura 2025-01            # Solo enero 2025
  python3 orchestrator.py gold cob_preventista_marca 2025-01
  python3 orchestrator.py gold cob_sucursal_marca 2025-01
  python3 orchestrator.py gold cob_preventista_generico 2025-01

  # Todo gold
  python3 orchestrator.py gold all

  ALL (Pipeline completo Bronze → Silver → Gold)

  # Pipeline completo de ventas
  python3 orchestrator.py all sales 2025-01-01 2025-12-31

  Carga incremental recomendada (orden)

  # 1. Bronze
  python3 orchestrator.py bronze masters
  python3 orchestrator.py bronze sales 2025-01-01 2025-01-31

  # 2. Silver
  python3 orchestrator.py silver masters
  python3 orchestrator.py silver sales 2025-01-01 2025-01-31

  # 3. Gold
  python3 orchestrator.py gold dimensions
  python3 orchestrator.py gold fact_ventas 2025-01-01 2025-01-31
  python3 orchestrator.py gold cobertura 2025-01
