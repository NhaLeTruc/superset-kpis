"""
Superset Configuration File
"""
import os

# Database Configuration for Superset metadata
# Using SQLite for Superset metadata (simpler setup)
# PostgreSQL will still be available as a data source for dashboards
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Redis Configuration for caching and Celery
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)

# Cache Configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': 1,
}

# Data Cache Configuration
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 24 hours
    'CACHE_KEY_PREFIX': 'superset_data_',
    'CACHE_REDIS_HOST': REDIS_HOST,
    'CACHE_REDIS_PORT': REDIS_PORT,
    'CACHE_REDIS_DB': 2,
}

# Secret Key for session encryption
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'CHANGE_THIS_SECRET_KEY')

# Additional Security Settings
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Superset specific configuration
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# SQL Lab Configuration
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# Enable public role for initial setup (optional)
PUBLIC_ROLE_LIKE = 'Gamma'