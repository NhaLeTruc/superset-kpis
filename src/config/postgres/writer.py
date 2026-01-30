"""
PostgreSQL Write Operations

Handles writing Spark DataFrames to PostgreSQL with optimizations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from .connection import get_postgres_connection_props


def write_to_postgres(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    batch_size: int = 10000,
    num_partitions: int | None = None,
) -> None:
    """
    Write Spark DataFrame to PostgreSQL table.

    Args:
        df: Spark DataFrame to write
        table_name: Target table name in PostgreSQL
        mode: Write mode - 'append', 'overwrite', 'error', 'ignore'
        batch_size: Number of rows per batch for JDBC write (default: 10000)
        num_partitions: Number of partitions for parallel writes (default: auto)

    Modes:
        - 'append': Add data to existing table
        - 'overwrite': Replace table contents
        - 'error': Fail if table exists (default JDBC behavior)
        - 'ignore': Skip write if table exists

    Optimization Notes:
        - Uses batchsize for efficient bulk inserts
        - Can repartition for parallel writes to avoid bottlenecks
        - Consider using smaller batch_size for large rows
        - Consider using more partitions for very large datasets

    Example:
        >>> # Simple append
        >>> write_to_postgres(df, "daily_active_users", mode="append")

        >>> # Overwrite with optimization
        >>> write_to_postgres(
        ...     df,
        ...     "power_users",
        ...     mode="overwrite",
        ...     batch_size=5000,
        ...     num_partitions=4
        ... )

    Raises:
        ValueError: If POSTGRES_PASSWORD is not set
        Exception: If write fails (connection, permissions, schema mismatch)
    """
    jdbc_url, properties = get_postgres_connection_props()

    # Optimize partitioning if specified
    if num_partitions:
        df = df.repartition(num_partitions)

    # Write to PostgreSQL
    try:
        properties["batchsize"] = str(batch_size)
        properties["isolationLevel"] = "READ_COMMITTED"

        df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)

        print(f"✅ Successfully wrote to {table_name}")

    except Exception as e:
        print(f"❌ Failed to write to {table_name}: {e!s}")
        raise
