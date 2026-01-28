"""
Superset Configuration File
"""

import os


def get_bool_env(key: str, default: bool = False) -> bool:
    """Convert environment variable string to boolean."""
    value = os.getenv(key, str(default)).lower()
    return value in ("true", "1", "yes", "on")


def get_int_env(key: str, default: int) -> int:
    """Convert environment variable string to integer."""
    try:
        return int(os.getenv(key, default))
    except (ValueError, TypeError):
        return default


# Database Configuration for Superset metadata
# Options: sqlite or postgresql
SUPERSET_DB_TYPE = os.getenv("SUPERSET_DB_TYPE", "sqlite").lower()

if SUPERSET_DB_TYPE == "postgresql":
    # Use PostgreSQL for Superset metadata
    SUPERSET_DB_NAME = os.getenv("SUPERSET_DB_NAME", "superset")
    SUPERSET_DB_HOST = os.getenv("SUPERSET_DB_HOST", os.getenv("POSTGRES_HOST", "postgres"))
    SUPERSET_DB_PORT = os.getenv("SUPERSET_DB_PORT", os.getenv("POSTGRES_PORT", "5432"))
    SUPERSET_DB_USER = os.getenv("SUPERSET_DB_USER", os.getenv("POSTGRES_USER", "analytics_user"))
    SUPERSET_DB_PASSWORD = os.getenv(
        "SUPERSET_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "analytics_pass")
    )
    SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{SUPERSET_DB_USER}:{SUPERSET_DB_PASSWORD}@{SUPERSET_DB_HOST}:{SUPERSET_DB_PORT}/{SUPERSET_DB_NAME}"
else:
    # Use SQLite for Superset metadata (default - simpler setup)
    SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Redis Configuration for caching and Celery
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = get_int_env("REDIS_PORT", 6379)

# Cache Configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": get_int_env("SUPERSET_CACHE_DEFAULT_TIMEOUT", 300),
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": get_int_env("SUPERSET_CACHE_REDIS_DB", 1),
}

# Data Cache Configuration
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": get_int_env("SUPERSET_DATA_CACHE_TIMEOUT", 86400),
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": get_int_env("SUPERSET_DATA_CACHE_REDIS_DB", 2),
}

# Secret Key for session encryption
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "CHANGE_THIS_SECRET_KEY")

# Additional Security Settings
WTF_CSRF_ENABLED = get_bool_env("SUPERSET_WTF_CSRF_ENABLED", True)
WTF_CSRF_TIME_LIMIT = None

# Superset specific configuration
ROW_LIMIT = get_int_env("SUPERSET_ROW_LIMIT", 5000)
SUPERSET_WEBSERVER_PORT = get_int_env("SUPERSET_WEBSERVER_PORT", 8088)

# SQL Lab Configuration
SQLLAB_TIMEOUT = get_int_env("SUPERSET_SQLLAB_TIMEOUT", 300)
SUPERSET_WEBSERVER_TIMEOUT = get_int_env("SUPERSET_WEBSERVER_TIMEOUT", 300)

# Enable public role for initial setup (optional)
PUBLIC_ROLE_LIKE = os.getenv("SUPERSET_PUBLIC_ROLE_LIKE", "Gamma")
