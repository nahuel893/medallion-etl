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

    JSONB keys excluidas (por decisión de negocio):
    - dsEmpresa, dsDocumento, idMovComercial, dsMovComercial
    - domicilioCliente, codigoPostal, dsLocalidad, idProvincia, dsProvincia
    - tipoConstribuyente, dsTipoConstribuyente, idTipoPago, dsTipoPago
    - idAgrupacion, dsAgrupacion, idArea, dsArea
    - tradespend* (todos), totradspend
    - numerosserie, numerosactivo, cuentayorden, codprovcyo, nrorendcyo
    - idTipoCambio, dsTipoCambio, cfdiEmitido, numeracionFiscal
    - idConcepto, dsConcepto, idArticuloEstadistico, dsArticuloEstadistico
    - cantidadPorPallets, peso, pesoTotal, tipocambio, motivocambio, descmotcambio
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

    # === IDENTIFICACIÓN DOCUMENTO ===
    id_empresa = Column(Integer, nullable=False)              # idEmpresa
    id_documento = Column(String(20), nullable=False)         # idDocumento
    letra = Column(String(1))                                 # letra
    serie = Column(Integer)                                   # serie
    nro_doc = Column(Integer, nullable=False)                 # nrodoc
    anulado = Column(Boolean, default=False)                  # anulado

    # === FECHAS ===
    fecha_comprobante = Column(Date, nullable=False)          # fechaComprobate
    fecha_alta = Column(Date)                                 # fechaAlta
    fecha_pedido = Column(Date)                               # fechaPedido
    fecha_entrega = Column(Date)                              # fechaEntrega
    fecha_vencimiento = Column(Date)                          # fechaVencimiento
    fecha_caja = Column(Date)                                 # fechaCaja
    fecha_anulacion = Column(Date)                            # fechaAnulacion
    fecha_pago = Column(Date)                                 # fechaPago
    fecha_liquidacion = Column(Date)                          # fechaLiquidacion
    fecha_asiento_contable = Column(Date)                     # fechaAsientoContable

    # === ORGANIZACIÓN ===
    id_sucursal = Column(Integer)                             # idSucursal
    ds_sucursal = Column(String(100))                         # dsSucursal
    id_deposito = Column(Integer)                             # idDeposito
    ds_deposito = Column(String(100))                         # dsDeposito
    id_caja = Column(Integer)                                 # idCaja
    cajero = Column(String(100))                              # cajero
    id_centro_costo = Column(Integer)                         # idCentroCosto

    # === PERSONAL ===
    id_vendedor = Column(Integer)                             # idVendedor
    ds_vendedor = Column(String(100))                         # dsVendedor
    id_supervisor = Column(Integer)                           # idSupervisor
    ds_supervisor = Column(String(100))                       # dsSupervisor
    id_gerente = Column(Integer)                              # idGerente
    ds_gerente = Column(String(100))                          # dsGerente
    id_fuerza_ventas = Column(Integer)                        # idFuerzaVentas
    ds_fuerza_ventas = Column(String(100))                    # dsFuerzaVentas
    usuario_alta = Column(String(100))                        # usuarioAlta

    # === CLIENTE ===
    id_cliente = Column(Integer, nullable=False)              # idCliente
    nombre_cliente = Column(String(200))                      # nombreCliente
    linea_credito = Column(String(200))                       # lineaCredito

    # === SEGMENTACIÓN COMERCIAL ===
    id_canal_mkt = Column(Integer)                            # idCanalMkt
    ds_canal_mkt = Column(String(100))                        # dsCanalMkt
    id_segmento_mkt = Column(Integer)                         # idSegmentoMkt
    ds_segmento_mkt = Column(String(100))                     # dsSegmentoMkt
    id_subcanal_mkt = Column(Integer)                         # idSubcanalMkt
    ds_subcanal_mkt = Column(String(100))                     # dsSubcanalMKT

    # === LOGÍSTICA ===
    id_fletero_carga = Column(Integer)                        # idFleteroCarga
    ds_fletero_carga = Column(String(100))                    # dsFleteroCarga
    planilla_carga = Column(String(50))                       # planillaCarga

    # === LÍNEA DE VENTA ===
    id_articulo = Column(Integer, nullable=False)             # idArticulo
    ds_articulo = Column(String(200))                         # dsArticulo
    presentacion_articulo = Column(String(50))                # presentacionArticulo
    es_combo = Column(Boolean, default=False)                 # esCombo
    id_combo = Column(Integer)                                # idCombo
    id_pedido = Column(Integer)                               # idPedido
    id_origen = Column(String(150))                           # idorigen
    origen = Column(String(50))                               # origen
    acciones = Column(String(200))                            # acciones

    # === CANTIDADES ===
    cantidades_con_cargo = Column(Numeric(15, 4))             # cantidadesCorCargo
    cantidades_sin_cargo = Column(Numeric(15, 4))             # cantidadesSinCargo
    cantidades_total = Column(Numeric(15, 4))                 # cantidadesTotal
    cantidades_rechazo = Column(Numeric(15, 4))               # cantidadesRechazo

    # === PRECIOS ===
    precio_unitario_bruto = Column(Numeric(15, 4))            # precioUnitarioBruto
    precio_unitario_neto = Column(Numeric(15, 4))             # precioUnitarioNeto
    bonificacion = Column(Numeric(8, 4))                      # bonificacion
    precio_compra_bruto = Column(Numeric(15, 4))              # preciocomprabr
    precio_compra_neto = Column(Numeric(15, 4))               # preciocomprant

    # === SUBTOTALES ===
    subtotal_bruto = Column(Numeric(15, 4))                   # subtotalBruto
    subtotal_bonificado = Column(Numeric(15, 4))              # subtotalBonificado
    subtotal_neto = Column(Numeric(15, 4))                    # subtotalNeto
    subtotal_final = Column(Numeric(15, 4))                   # subtotalFinal

    # === IMPUESTOS ===
    iva21 = Column(Numeric(15, 4))                            # iva21
    iva27 = Column(Numeric(15, 4))                            # iva27
    iva105 = Column(Numeric(15, 4))                           # iva105
    iva2 = Column(Numeric(15, 4))                             # iva2
    internos = Column(Numeric(15, 4))                         # internos
    per3337 = Column(Numeric(15, 4))                          # per3337
    percepcion212 = Column(Numeric(15, 4))                    # percepcion212
    percepcion_iibb = Column(Numeric(15, 4))                  # percepcioniibb
    pers_iibb_d = Column(Numeric(15, 4))                      # persiibbd
    pers_iibb_r = Column(Numeric(15, 4))                      # persiibbr
    cod_prov_iibb = Column(String(10))                        # codproviibb

    # === CONTABILIDAD ===
    cod_cuenta_contable = Column(String(50))                  # codCuentaContable
    ds_cuenta_contable = Column(String(100))                  # dsCuentaContable
    nro_asiento_contable = Column(Integer)                    # nroAsientoContable
    nro_plan_contable = Column(Integer)                       # nroPlanContable
    id_liquidacion = Column(Integer)                          # idLiquidacion

    # === PROVEEDOR ===
    proveedor = Column(String(100))                           # proveedor
    fvig_pcompra = Column(Date)                               # fvigpcompra

    # === METADATA / RECHAZO ===
    id_rechazo = Column(Integer)                              # idRechazo
    ds_rechazo = Column(String(100))                          # dsRechazo
    informado = Column(Boolean)                               # informado
    regimen_fiscal = Column(String(50))                       # regimenFiscal

    def __repr__(self):
        return f"<FactVentas(id={self.id}, doc={self.id_documento}-{self.serie}-{self.nro_doc})>"
