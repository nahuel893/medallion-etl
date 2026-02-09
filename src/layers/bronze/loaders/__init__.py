from layers.bronze.loaders.sales_loader import load_bronze
from layers.bronze.loaders.clientes_loader import load_clientes
from layers.bronze.loaders.staff_loader import load_staff
from layers.bronze.loaders.routes_loader import load_routes
from layers.bronze.loaders.articles_loader import load_articles
from layers.bronze.loaders.stock_loader import load_stock
from layers.bronze.loaders.depositos_loader import load_depositos
from layers.bronze.loaders.marketing_loader import load_marketing
from layers.bronze.loaders.hectolitros_loader import load_hectolitros

__all__ = ['load_bronze', 'load_clientes', 'load_staff', 'load_routes', 'load_articles', 'load_stock', 'load_depositos', 'load_marketing', 'load_hectolitros']
