"""
Unit tests for power user identification.

Tests identify_power_users() function from engagement transforms.
"""

from datetime import datetime

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from src.transforms.engagement import identify_power_users


class TestIdentifyPowerUsers:
    """Tests for identify_power_users() function."""

    def test_identify_power_users_top_1_percent(self, spark):
        """
        GIVEN:
            - 100 users with total_duration uniformly distributed
            - percentile=0.99
        WHEN: identify_power_users() is called
        THEN:
            - Returns 1 user (top 1%)
            - User has highest total_duration_ms
        """
        # Arrange - interactions
        interactions_data = []
        for user_num in range(1, 101):
            user_id = f"u{user_num:03d}"
            duration = user_num * 1000  # 1000, 2000, ..., 100000
            interactions_data.append((user_id, datetime(2023, 1, 1, 10, 0, 0), duration, "page1"))

        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
                StructField("page_id", StringType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Metadata
        metadata_data = [(f"u{i:03d}", "US", "iPad", "premium") for i in range(1, 101)]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
                StructField("device_type", StringType(), nullable=False),
                StructField("subscription_type", StringType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(interactions_df, metadata_df, percentile=0.99)

        # Assert
        assert result_df.count() == 1
        result = result_df.collect()[0]
        assert result["user_id"] == "u100"
        assert result["total_duration_ms"] == 100000

    def test_identify_power_users_filters_outliers(self, spark):
        """
        GIVEN:
            - u001: 3 interactions: [5000ms, 120000ms, 50000000ms (13.8 hours)]
            - max_duration_ms = 28800000 (8 hours)
        WHEN: identify_power_users() is called
        THEN:
            - Outlier (50000000ms) is excluded
            - u001 total_duration_ms = 5000 + 120000 = 125000ms
        """
        # Arrange
        interactions_data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), 5000, "page1"),
            ("u001", datetime(2023, 1, 1, 11, 0, 0), 120000, "page2"),
            ("u001", datetime(2023, 1, 1, 12, 0, 0), 50000000, "page3"),  # Outlier
        ]
        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
                StructField("page_id", StringType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        metadata_data = [("u001", "US", "iPad", "premium")]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
                StructField("device_type", StringType(), nullable=False),
                StructField("subscription_type", StringType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(
            interactions_df,
            metadata_df,
            percentile=0.0,  # Include all users
            max_duration_ms=28800000,
        )

        # Assert
        result = result_df.collect()[0]
        assert result["total_duration_ms"] == 125000

    def test_identify_power_users_joins_metadata(self, spark):
        """
        GIVEN:
            - u001 is power user
            - metadata: u001 -> country=US, device_type=iPad, subscription_type=premium
        WHEN: identify_power_users() is called
        THEN:
            - Result includes country=US, device_type=iPad, subscription_type=premium
        """
        # Arrange
        interactions_data = [("u001", datetime(2023, 1, 1, 10, 0, 0), 100000, "page1")]
        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
                StructField("page_id", StringType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        metadata_data = [("u001", "US", "iPad", "premium")]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
                StructField("device_type", StringType(), nullable=False),
                StructField("subscription_type", StringType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(interactions_df, metadata_df, percentile=0.0)

        # Assert
        result = result_df.collect()[0]
        assert result["country"] == "US"
        assert result["device_type"] == "iPad"
        assert result["subscription_type"] == "premium"

    def test_identify_power_users_calculates_all_metrics(self, spark):
        """
        GIVEN: u001 with known interaction pattern
        WHEN: identify_power_users() is called
        THEN: All metrics calculated correctly
        """
        # Arrange
        interactions_data = [
            ("u001", datetime(2023, 1, 1, 10, 0, 0), 10000, "page1"),
            ("u001", datetime(2023, 1, 1, 11, 0, 0), 20000, "page2"),
            ("u001", datetime(2023, 1, 2, 10, 0, 0), 30000, "page1"),  # Different day
        ]
        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("timestamp", TimestampType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
                StructField("page_id", StringType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        metadata_data = [("u001", "US", "iPad", "premium")]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("country", StringType(), nullable=False),
                StructField("device_type", StringType(), nullable=False),
                StructField("subscription_type", StringType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = identify_power_users(interactions_df, metadata_df, percentile=0.0)

        # Assert
        result = result_df.collect()[0]
        assert result["total_interactions"] == 3
        assert result["unique_pages"] == 2  # page1, page2
        assert result["days_active"] == 2  # Jan 1, Jan 2
        assert result["total_duration_ms"] == 60000
        assert abs(result["hours_spent"] - 60000 / 3600000) < 0.01
        assert result["avg_duration_per_interaction"] == 20000.0
