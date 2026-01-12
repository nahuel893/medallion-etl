"""
Configuración centralizada de logging para Medallion ETL.

Uso:
    from config.logging_config import get_logger
    logger = get_logger(__name__)

    logger.info("Mensaje informativo")
    logger.error("Mensaje de error")
"""
import logging
import sys
from pathlib import Path
from datetime import datetime


# Directorio de logs
LOG_DIR = Path(__file__).parent.parent.parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Formato de logs
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Colores para consola (ANSI)
COLORS = {
    'DEBUG': '\033[36m',     # Cyan
    'INFO': '\033[32m',      # Verde
    'WARNING': '\033[33m',   # Amarillo
    'ERROR': '\033[31m',     # Rojo
    'CRITICAL': '\033[35m',  # Magenta
    'RESET': '\033[0m'       # Reset
}


class ColoredFormatter(logging.Formatter):
    """Formatter con colores para la consola."""

    def format(self, record):
        # Agregar color según nivel
        color = COLORS.get(record.levelname, COLORS['RESET'])
        reset = COLORS['RESET']

        # Colorear el levelname
        record.levelname = f"{color}{record.levelname}{reset}"

        return super().format(record)


def setup_logging(level: str = 'INFO', log_to_file: bool = True) -> None:
    """
    Configura el logging global.

    Args:
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Si True, también escribe a archivo
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Limpiar handlers existentes
    root_logger.handlers.clear()

    # Handler para consola (con colores)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(ColoredFormatter(LOG_FORMAT, LOG_DATE_FORMAT))
    root_logger.addHandler(console_handler)

    # Handler para archivo (sin colores)
    if log_to_file:
        log_file = LOG_DIR / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Obtiene un logger con el nombre especificado.

    Args:
        name: Nombre del logger (típicamente __name__)

    Returns:
        Logger configurado
    """
    return logging.getLogger(name)


# Configurar logging al importar el módulo
setup_logging()
