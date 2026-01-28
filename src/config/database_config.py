"""
Database Configuration (Compatibility Shim)

This module maintains backward compatibility by re-exporting all functions
from the new postgres package structure.

New code should import directly from src.config.postgres instead.
"""

from .postgres import (
    create_connection_string,
    execute_sql,
    get_postgres_connection_props,
    get_table_row_count,
    read_from_postgres,
    validate_database_config,
    write_to_postgres,
)


__all__ = [
    "create_connection_string",
    "execute_sql",
    "get_postgres_connection_props",
    "get_table_row_count",
    "read_from_postgres",
    "validate_database_config",
    "write_to_postgres",
]
