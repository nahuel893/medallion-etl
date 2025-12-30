"""
Modelos ORM para la capa Silver
"""
from sqlalchemy import Column, Date, Integer, String, DateTime, Boolean, Numeric, ForeignKey, Index
from sqlalchemy.sql import func
from database.engine import Base


class FactVentas(Base):
    """
    Modelo para la tabla de hechos de ventas (líneas de venta parseadas).
    Cada registro representa una línea de un comprobante de venta.
    """
    __tablename__ = 'fact_ventas'
    __table_args__ = (
        Index('idx_silver_ventas_fecha', 'fecha_comprobante'),
        Index('idx_silver_ventas_cliente', 'id_cliente'),
        Index('idx_silver_ventas_articulo', 'id_articulo'),
        Index('idx_silver_ventas_vendedor', 'id_vendedor'),
        Index('idx_silver_ventas_documento', 'id_documento', 'serie', 'nro_doc'),
        Index('idx_silver_ventas_bronze', 'bronze_id'),
        {'schema': 'silver'}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    processed_at = Column(DateTime, server_default=func.now())
    bronze_id = Column(Integer, ForeignKey('bronze.raw_sales.id'))

    # Identificación documento
    id_empresa = Column(Integer, nullable=False)
    ds_empresa = Column(String(100))
    id_documento = Column(String(20), nullable=False)
    ds_documento = Column(String(100))
    letra = Column(String(1))
    serie = Column(Integer)
    nro_doc = Column(Integer, nullable=False)
    anulado = Column(Boolean, default=False)

    # Fechas
    fecha_comprobante = Column(Date, nullable=False)
    fecha_alta = Column(Date)
    fecha_pedido = Column(Date)
    fecha_entrega = Column(Date)
    fecha_vencimiento = Column(Date)
    fecha_caja = Column(Date)

    # Organización
    id_sucursal = Column(Integer)
    ds_sucursal = Column(String(100))
    id_deposito = Column(Integer)
    ds_deposito = Column(String(100))

    # Personal
    id_vendedor = Column(Integer)
    ds_vendedor = Column(String(100))
    id_supervisor = Column(Integer)
    ds_supervisor = Column(String(100))
    id_gerente = Column(Integer)
    ds_gerente = Column(String(100))

    # Cliente
    id_cliente = Column(Integer, nullable=False)
    nombre_cliente = Column(String(200))
    domicilio_cliente = Column(String(300))
    codigo_postal = Column(String(20))
    id_localidad = Column(Integer)
    ds_localidad = Column(String(100))
    id_provincia = Column(String(10))
    ds_provincia = Column(String(100))

    # Pago
    id_tipo_pago = Column(Integer)
    ds_tipo_pago = Column(String(50))

    # Segmentación comercial
    id_negocio = Column(Integer)
    ds_negocio = Column(String(100))
    id_canal_mkt = Column(Integer)
    ds_canal_mkt = Column(String(100))
    id_segmento_mkt = Column(Integer)
    ds_segmento_mkt = Column(String(100))
    id_area = Column(Integer)
    ds_area = Column(String(100))

    # Línea de venta
    id_linea = Column(Integer, nullable=False)
    id_articulo = Column(Integer, nullable=False)
    ds_articulo = Column(String(200))
    id_concepto = Column(Integer)
    ds_concepto = Column(String(100))
    es_combo = Column(Boolean, default=False)
    id_combo = Column(Integer)

    # Artículo estadístico
    id_articulo_estadistico = Column(Integer)
    ds_articulo_estadistico = Column(String(200))
    presentacion_articulo = Column(String(50))

    # Cantidades
    cantidad_solicitada = Column(Numeric(15, 4))
    unidades_solicitadas = Column(Numeric(15, 4))
    cantidades_con_cargo = Column(Numeric(15, 4))
    cantidades_sin_cargo = Column(Numeric(15, 4))
    cantidades_total = Column(Numeric(15, 4))
    cantidades_rechazo = Column(Numeric(15, 4))
    peso = Column(Numeric(15, 4))
    peso_total = Column(Numeric(15, 4))

    # Precios
    precio_unitario_bruto = Column(Numeric(15, 4))
    bonificacion = Column(Numeric(8, 4))
    precio_unitario_neto = Column(Numeric(15, 4))

    # Subtotales
    subtotal_bruto = Column(Numeric(15, 4))
    subtotal_bonificado = Column(Numeric(15, 4))
    subtotal_neto = Column(Numeric(15, 4))
    subtotal_final = Column(Numeric(15, 4))

    # Impuestos
    iva21 = Column(Numeric(15, 4))
    iva27 = Column(Numeric(15, 4))
    iva105 = Column(Numeric(15, 4))
    internos = Column(Numeric(15, 4))
    per3337 = Column(Numeric(15, 4))
    percepcion212 = Column(Numeric(15, 4))
    percepcion_iibb = Column(Numeric(15, 4))

    # Trade spend
    totradspend = Column(Numeric(15, 4))

    # Metadata
    origen = Column(String(50))
    id_rechazo = Column(Integer)
    ds_rechazo = Column(String(100))

    def __repr__(self):
        return f"<FactVentas(id={self.id}, doc={self.id_documento}-{self.serie}-{self.nro_doc}, linea={self.id_linea})>"
