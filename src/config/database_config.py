"""
Database Configuration and Helper Functions

Provides utilities for connecting to PostgreSQL and writing Spark DataFrames.
Supports both direct JDBC connections and optimized bulk writes.
"""
import os
from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession


def get_postgres_connection_props() -> Dict[str, str]:
    """
    Get PostgreSQL JDBC connection properties from environment variables.

    Environment Variables Required:
        - POSTGRES_HOST: Database host (default: localhost)
        - POSTGRES_PORT: Database port (default: 5432)
        - POSTGRES_DB: Database name (default: goodnote_analytics)
        - POSTGRES_USER: Database user (default: postgres)
        - POSTGRES_PASSWORD: Database password (required)

    Returns:
        Dictionary with JDBC connection properties

    Example:
        >>> props = get_postgres_connection_props()
        >>> spark.read.jdbc(url, table="users", properties=props)
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "goodnote_analytics")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise ValueError(
            "POSTGRES_PASSWORD environment variable is required. "
            "Please set it in your environment or .env file."
        )

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"  # Helps with VARCHAR compatibility
    }

    return jdbc_url, properties


def write_to_postgres(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
    batch_size: int = 10000,
    num_partitions: Optional[int] = None
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
        df.write \
            .jdbc(
                url=jdbc_url,
                table=table_name,
                mode=mode,
                properties=properties
            ) \
            .option("batchsize", batch_size) \
            .option("isolationLevel", "READ_COMMITTED") \
            .save()

        print(f"✅ Successfully wrote {df.count()} rows to {table_name}")

    except Exception as e:
        print(f"❌ Failed to write to {table_name}: {str(e)}")
        raise


def read_from_postgres(
    spark: SparkSession,
    table_name: str,
    predicate_pushdown: Optional[str] = None,
    num_partitions: Optional[int] = None,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None
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

    reader = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=properties
    )

    # Apply predicate pushdown if specified
    if predicate_pushdown:
        reader = reader.option("predicates", [predicate_pushdown])

    # Apply partitioning if specified
    if partition_column:
        if not all([num_partitions, lower_bound is not None, upper_bound is not None]):
            raise ValueError(
                "For partitioned reads, must specify: "
                "partition_column, num_partitions, lower_bound, upper_bound"
            )

        reader = reader.option("partitionColumn", partition_column) \
                       .option("numPartitions", num_partitions) \
                       .option("lowerBound", lower_bound) \
                       .option("upperBound", upper_bound)

    df = reader.load()
    print(f"✅ Successfully read {df.count()} rows from {table_name}")

    return df


def execute_sql(
    spark: SparkSession,
    sql_query: str
) -> DataFrame:
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

    df = spark.read.jdbc(
        url=jdbc_url,
        table=f"({sql_query}) as query",
        properties=properties
    ).load()

    return df


def create_connection_string() -> str:
    """
    Create PostgreSQL connection string for use with psycopg2 or SQLAlchemy.

    Returns:
        PostgreSQL connection string

    Example:
        >>> # For psycopg2
        >>> import psycopg2
        >>> conn = psycopg2.connect(create_connection_string())

        >>> # For SQLAlchemy
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(f"postgresql+psycopg2://{create_connection_string()}")
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "goodnote_analytics")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise ValueError("POSTGRES_PASSWORD environment variable is required")

    # Format: host=localhost port=5432 dbname=mydb user=myuser password=mypass
    conn_string = f"host={host} port={port} dbname={database} user={user} password={password}"

    return conn_string


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
    jdbc_url, properties = get_postgres_connection_props()

    count_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT COUNT(*) as count FROM {table_name}) as count_query",
        properties=properties
    ).load()

    return count_df.first()["count"]


# ============================================
# Configuration Validation
# ============================================

def validate_database_config() -> bool:
    """
    Validate that all required database configuration is present.

    Returns:
        True if configuration is valid

    Raises:
        ValueError: If required configuration is missing
    """
    try:
        jdbc_url, properties = get_postgres_connection_props()
        print("✅ Database configuration is valid")
        print(f"   JDBC URL: {jdbc_url}")
        print(f"   User: {properties['user']}")
        return True
    except Exception as e:
        print(f"❌ Database configuration is invalid: {str(e)}")
        raise


if __name__ == "__main__":
    # Validate configuration when run directly
    print("Validating database configuration...")
    validate_database_config()
