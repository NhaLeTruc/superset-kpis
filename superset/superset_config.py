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
        value = os.getenv(key)
        return int(value) if value is not None else default
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
    SUPERSET_DB_PATH = os.getenv("SUPERSET_DB_PATH", "/app/superset_home/superset.db")
    SQLALCHEMY_DATABASE_URI = f"sqlite:///{SUPERSET_DB_PATH}"

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
# SECURITY WARNING: Generate a secure key for production:
#   python -c "import secrets; print(secrets.token_hex(32))"
# Then set SUPERSET_SECRET_KEY environment variable
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY")
if not SECRET_KEY:
    import secrets
    import warnings

    warnings.warn(
        "SUPERSET_SECRET_KEY not set! Generating temporary key. "
        "Set SUPERSET_SECRET_KEY environment variable for production.",
        UserWarning,
        stacklevel=2,
    )
    SECRET_KEY = secrets.token_hex(32)

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
