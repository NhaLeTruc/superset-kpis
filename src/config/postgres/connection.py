"""
PostgreSQL Connection Management

Handles connection configuration, connection string creation, and validation.
"""

import os


def get_postgres_connection_props() -> tuple[str, dict[str, str]]:
    """
    Get PostgreSQL JDBC connection properties from environment variables.

    Environment Variables Required:
        - POSTGRES_HOST: Database host (default: localhost)
        - POSTGRES_PORT: Database port (default: 5432)
        - POSTGRES_DB: Database name (default: goodnote_analytics)
        - POSTGRES_USER: Database user (default: postgres)
        - POSTGRES_PASSWORD: Database password (required)

    Returns:
        Tuple of (jdbc_url, properties_dict)

    Example:
        >>> url, props = get_postgres_connection_props()
        >>> spark.read.jdbc(url, table="users", properties=props)

    Raises:
        ValueError: If POSTGRES_PASSWORD is not set
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
        "stringtype": "unspecified",  # Helps with VARCHAR compatibility
    }

    return jdbc_url, properties


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

    Raises:
        ValueError: If POSTGRES_PASSWORD is not set
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
        print(f"❌ Database configuration is invalid: {e!s}")
        raise
