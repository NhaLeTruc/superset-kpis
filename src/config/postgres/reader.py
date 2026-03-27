"""
PostgreSQL Read Operations

Handles reading data from PostgreSQL into Spark DataFrames.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

from .connection import get_postgres_connection_props

_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def read_from_postgres(
    spark: SparkSession,
    table_name: str,
    predicate_pushdown: str | None = None,
    num_partitions: int | None = None,
    partition_column: str | None = None,
    lower_bound: int | None = None,
    upper_bound: int | None = None,
) -> DataFrame:
    """
    Read table from PostgreSQL into Spark DataFrame.

    Args:
        spark: Active SparkSession
        table_name: Source table name
        predicate_pushdown: Optional WHERE clause for filtering (e.g., "date >= '2023-01-01'")
        num_partitions: Number of partitions for parallel reads
        partition_column: Column to use for partitioning (must be numeric)
        lower_bound: Minimum value of partition_column
        upper_bound: Maximum value of partition_column

    Returns:
        Spark DataFrame with table contents

    Partitioned Read Example:
        >>> # Read large table with parallel partitions
        >>> df = read_from_postgres(
        ...     spark,
        ...     "user_interactions",
        ...     partition_column="interaction_id",
        ...     num_partitions=8,
        ...     lower_bound=0,
        ...     upper_bound=10000000
        ... )

    Filtered Read Example:
        >>> # Read with predicate pushdown
        >>> df = read_from_postgres(
        ...     spark,
        ...     "daily_active_users",
        ...     predicate_pushdown="date >= '2023-01-01' AND date < '2024-01-01'"
        ... )

    Raises:
        ValueError: If POSTGRES_PASSWORD is not set or invalid partition config
    """
    jdbc_url, properties = get_postgres_connection_props()

    # Validate partition config before any I/O
    if partition_column and any(v is None for v in [num_partitions, lower_bound, upper_bound]):
        raise ValueError(
            "For partitioned reads, must specify: "
            "partition_column, num_partitions, lower_bound, upper_bound"
        )

    # spark.read.jdbc() returns a DataFrame directly — .option()/.load() chaining is
    # only valid on DataFrameReader (spark.read.format("jdbc")...).  Use the correct
    # parameter-based overloads instead.
    if partition_column:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            column=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions,
            properties=properties,
        )
    elif predicate_pushdown:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            predicates=[predicate_pushdown],
            properties=properties,
        )
    else:
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

    print(f"✅ Successfully read data from {table_name}")

    return df


def execute_sql(spark: SparkSession, sql_query: str) -> DataFrame:
    """
    Execute arbitrary SQL query on PostgreSQL.

    Args:
        spark: Active SparkSession
        sql_query: SQL query to execute

    Returns:
        DataFrame with query results

    Example:
        >>> # Execute custom aggregation
        >>> df = execute_sql(
        ...     spark,
        ...     \"\"\"
        ...     SELECT device_type, AVG(bounce_rate) as avg_bounce
        ...     FROM bounce_rates
        ...     WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
        ...     GROUP BY device_type
        ...     \"\"\"
        ... )
    """
    jdbc_url, properties = get_postgres_connection_props()

    # spark.read.jdbc() already returns a DataFrame — .load() is not needed and
    # raises AttributeError because DataFrame has no such method.
    df = spark.read.jdbc(
        url=jdbc_url, table=f"({sql_query}) as query", properties=properties
    )

    return df


def get_table_row_count(spark: SparkSession, table_name: str) -> int:
    """
    Get row count for a PostgreSQL table.

    Args:
        spark: Active SparkSession
        table_name: Table name to count

    Returns:
        Number of rows in table

    Example:
        >>> count = get_table_row_count(spark, "daily_active_users")
        >>> print(f"Table has {count} rows")
    """
    if not _TABLE_NAME_RE.match(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")

    jdbc_url, properties = get_postgres_connection_props()

    count_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT COUNT(*) as count FROM {table_name}) as count_query",
        properties=properties,
    )

    row = count_df.first()
    if row is None:
        return 0
    return row["count"]
