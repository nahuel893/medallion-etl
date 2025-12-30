"""
Módulo de Configuración

Carga y proporciona acceso a las configuraciones de la aplicación,
como las claves de API, desde un archivo .env.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class Settings(BaseSettings):
    """
    Configuración de la aplicación cargada desde variables de entorno.
    """
    model_config = SettingsConfigDict(
        env_file=os.path.join(PROJECT_ROOT, '.env'),
        env_file_encoding='utf-8',
        extra='ignore'
    )
    # Net config
    POSTGRES_USER: str = Field(..., description="Usuario de PostgreSQL")
    POSTGRES_PASSWORD: str = Field(..., description="Contraseña de PostgreSQL")
    DATABASE: str = Field(..., description="Nombre de la base de datos PostgreSQL")
    IP_SERVER: str = Field(..., description="Direccion ip del servidor")

# Crear una instancia de Settings para ser usada en toda la aplicación
settings = Settings()
