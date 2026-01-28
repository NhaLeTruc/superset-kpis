"""
PostgreSQL Database Operations Package

Provides comprehensive PostgreSQL connectivity for Spark:
- Connection management and validation
- Optimized read operations with partitioning
- Optimized write operations with batching
- SQL query execution
- Utility functions

All public APIs are re-exported for convenience.
"""

from .connection import (
    create_connection_string,
    get_postgres_connection_props,
    validate_database_config,
)
from .reader import execute_sql, get_table_row_count, read_from_postgres
from .writer import write_to_postgres


__all__ = [
    "create_connection_string",
    "execute_sql",
    # Connection
    "get_postgres_connection_props",
    "get_table_row_count",
    # Reader
    "read_from_postgres",
    "validate_database_config",
    # Writer
    "write_to_postgres",
]
