"""
Pytest configuration for integration tests.

This module provides shared fixtures for integration testing,
including Spark session setup, test data generation, and
database connection management.
"""
import pytest
import tempfile
import shutil
import os
from pathlib import Path
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for integration testing.

    This session is configured for local testing with minimal resources.
    """
    spark = (
        SparkSession.builder
        .appName("GoodNote_Integration_Tests")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    # Set log level to reduce output
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture(scope="function")
def temp_dir():
    """
    Create a temporary directory for test data.

    The directory is cleaned up after the test completes.
    """
    temp_path = tempfile.mkdtemp(prefix="goodnote_test_")
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="function")
def test_data_paths(temp_dir):
    """
    Create directory structure for test data.

    Returns:
        Dictionary with paths for input, processed, and output data
    """
    paths = {
        "raw_interactions": os.path.join(temp_dir, "raw", "interactions"),
        "raw_metadata": os.path.join(temp_dir, "raw", "metadata"),
        "processed": os.path.join(temp_dir, "processed"),
        "output": os.path.join(temp_dir, "output"),
    }

    # Create directories
    for path in paths.values():
        os.makedirs(path, exist_ok=True)

    return paths


@pytest.fixture(scope="function")
def sample_interactions_data(spark):
    """
    Generate sample interactions data for testing.

    Returns:
        DataFrame with realistic user interactions
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

    # Generate 1000 interactions over 30 days
    base_date = datetime(2024, 1, 1)
    interactions = []

    for i in range(1000):
        user_id = f"user_{i % 100}"  # 100 unique users
        action_type = ["open_note", "edit_note", "save_note", "close_note"][i % 4]
        timestamp = base_date + timedelta(days=i % 30, hours=i % 24, minutes=i % 60)
        duration_ms = 100 + (i % 5000)

        interactions.append((
            f"interaction_{i}",
            user_id,
            action_type,
            timestamp,
            duration_ms
        ))

    schema = StructType([
        StructField("interaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("action_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("duration_ms", IntegerType(), False),
    ])

    return spark.createDataFrame(interactions, schema)


@pytest.fixture(scope="function")
def sample_metadata_data(spark):
    """
    Generate sample metadata for testing.

    Returns:
        DataFrame with user metadata
    """
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType

    # Generate metadata for 100 users
    metadata = []
    base_date = datetime(2023, 1, 1)

    for i in range(100):
        user_id = f"user_{i}"
        country = ["US", "UK", "CA", "AU", "DE"][i % 5]
        device_type = ["iOS", "Android", "Web"][i % 3]
        subscription_type = ["free", "premium", "enterprise"][i % 3]
        registration_date = base_date + timedelta(days=i)

        metadata.append((
            user_id,
            country,
            device_type,
            subscription_type,
            registration_date,
            "1.0.0"
        ))

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("country", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("subscription_type", StringType(), False),
        StructField("registration_date", TimestampType(), False),
        StructField("app_version", StringType(), False),
    ])

    return spark.createDataFrame(metadata, schema)


@pytest.fixture(scope="function")
def sample_skewed_interactions(spark):
    """
    Generate skewed interactions data for testing join optimization.

    Returns:
        DataFrame with 90% of interactions from 10% of users (power law distribution)
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

    interactions = []
    base_date = datetime(2024, 1, 1)
    interaction_id = 0

    # Power users (10% of users, 90% of interactions)
    for user_idx in range(10):  # 10 power users
        for i in range(900):  # 900 interactions each
            interactions.append((
                f"interaction_{interaction_id}",
                f"power_user_{user_idx}",
                "edit_note",
                base_date + timedelta(hours=interaction_id % 720),
                200 + (interaction_id % 3000)
            ))
            interaction_id += 1

    # Normal users (90% of users, 10% of interactions)
    for user_idx in range(90):  # 90 normal users
        for i in range(10):  # 10 interactions each
            interactions.append((
                f"interaction_{interaction_id}",
                f"normal_user_{user_idx}",
                "open_note",
                base_date + timedelta(hours=interaction_id % 720),
                100 + (interaction_id % 1000)
            ))
            interaction_id += 1

    schema = StructType([
        StructField("interaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("action_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("duration_ms", IntegerType(), False),
    ])

    return spark.createDataFrame(interactions, schema)


@pytest.fixture(scope="function")
def db_config():
    """
    Database configuration for testing.

    In a real environment, this would connect to a test database.
    For unit/integration tests, we'll use in-memory DataFrames.
    """
    return {
        "host": "localhost",
        "port": 5432,
        "database": "goodnote_test",
        "user": "test_user",
        "password": "test_password"
    }
