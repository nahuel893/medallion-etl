"""
Modelos ORM para la capa Bronze
"""
from sqlalchemy import Column, Date, Integer, String, DateTime, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from database.engine import Base


class RawSales(Base):
    """Modelo para almacenar datos crudos de ventas desde la API."""
    __tablename__ = 'raw_sales'
    __table_args__ = (
        Index('idx_bronze_sales_date', 'date_comprobante'),
        {'schema': 'bronze'}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_at = Column(DateTime, server_default=func.now())
    source_system = Column(String(50))
    data_raw = Column(JSONB)
    date_comprobante = Column(Date)

    def __repr__(self):
        return f"<RawSales(id={self.id}, source_system='{self.source_system}')>"

class RawClientes(Base):
    """Modelo para almacenar datos crudos de clientes desde la API (full refresh)."""
    __tablename__ = 'raw_clientes'
    __table_args__ = {'schema': 'bronze'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    ingestion_at = Column(DateTime, server_default=func.now())
    source_system = Column(String(50))
    data_raw = Column(JSONB)

    def __repr__(self):
        return f"<RawClientes(id={self.id}, source_system='{self.source_system}')>"



