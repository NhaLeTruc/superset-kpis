"""
Pytest configuration and fixtures for TDD tests.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create SparkSession for testing.

    Scope: session (created once, shared across all tests)
    """
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("goodnote-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture
def sample_interactions(spark):
    """Basic interactions test data."""
    data = [
        ("u001", "2023-01-01 10:00:00", "page_view", "p001", 5000, "1.0.0"),
        ("u001", "2023-01-01 10:05:00", "edit", "p001", 120000, "1.0.0"),
        ("u002", "2023-01-01 10:10:00", "page_view", "p002", 3000, "1.0.0"),
    ]
    return spark.createDataFrame(
        data, ["user_id", "timestamp", "action_type", "page_id", "duration_ms", "app_version"]
    )


@pytest.fixture
def sample_metadata(spark):
    """Basic metadata test data."""
    data = [
        ("u001", "2023-01-01", "US", "iPad", "premium"),
        ("u002", "2023-01-01", "UK", "iPhone", "free"),
    ]
    return spark.createDataFrame(
        data, ["user_id", "join_date", "country", "device_type", "subscription_type"]
    )
