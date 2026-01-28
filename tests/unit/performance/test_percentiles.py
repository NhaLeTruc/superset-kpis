"""
Unit tests for performance metrics transforms.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 3 (Performance Metrics Specifications)
"""

from datetime import date

from pyspark.sql.types import DateType, IntegerType, LongType, StringType, StructField, StructType
from tests.unit.helpers.assertions import assert_percentile_accuracy

from src.transforms.performance import calculate_device_correlation, calculate_percentiles


class TestCalculatePercentiles:
    """Tests for calculate_percentiles() function."""

    def test_calculate_percentiles_basic(self, spark):
        """
        GIVEN:
            - app_version=1.0.0 with durations: [1000, 2000, 3000, 4000, 5000] ms
        WHEN: calculate_percentiles() with percentiles=[0.50, 0.95]
        THEN:
            - p50 ≈ 3000 (median)
            - p95 ≈ 4900 (95th percentile)
            - count = 5
        """
        # Arrange
        schema = StructType(
            [
                StructField("app_version", StringType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [("1.0.0", 1000), ("1.0.0", 2000), ("1.0.0", 3000), ("1.0.0", 4000), ("1.0.0", 5000)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df,
            value_column="duration_ms",
            group_by_columns=["app_version"],
            percentiles=[0.50, 0.95],
        )

        # Assert
        result = result_df.collect()[0]
        assert result["app_version"] == "1.0.0"
        assert abs(result["p50"] - 3000) < 100  # Within 100ms
        assert abs(result["p95"] - 4900) < 200  # Within 200ms
        assert result["count"] == 5
        assert "avg_duration_ms" in result_df.columns
        assert "stddev_duration_ms" in result_df.columns

    def test_calculate_percentiles_multiple_groups(self, spark):
        """
        GIVEN:
            - app_version=1.0.0, date=2023-01-01: durations [1000, 2000, 3000]
            - app_version=1.0.0, date=2023-01-02: durations [4000, 5000, 6000]
            - app_version=2.0.0, date=2023-01-01: durations [500, 1000, 1500]
        WHEN: calculate_percentiles() grouped by ["app_version", "date"]
        THEN:
            - Returns 3 rows (one per group)
            - Each row has correct percentiles for its group
        """
        # Arrange
        schema = StructType(
            [
                StructField("app_version", StringType(), nullable=False),
                StructField("date", DateType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        data = [
            # Group 1: v1.0.0, 2023-01-01
            ("1.0.0", date(2023, 1, 1), 1000),
            ("1.0.0", date(2023, 1, 1), 2000),
            ("1.0.0", date(2023, 1, 1), 3000),
            # Group 2: v1.0.0, 2023-01-02
            ("1.0.0", date(2023, 1, 2), 4000),
            ("1.0.0", date(2023, 1, 2), 5000),
            ("1.0.0", date(2023, 1, 2), 6000),
            # Group 3: v2.0.0, 2023-01-01
            ("2.0.0", date(2023, 1, 1), 500),
            ("2.0.0", date(2023, 1, 1), 1000),
            ("2.0.0", date(2023, 1, 1), 1500),
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df,
            value_column="duration_ms",
            group_by_columns=["app_version", "date"],
            percentiles=[0.50],
        )

        # Assert
        assert result_df.count() == 3

        # Check each group
        results = result_df.collect()
        for row in results:
            if row["app_version"] == "1.0.0" and row["date"] == date(2023, 1, 1):
                assert abs(row["p50"] - 2000) < 100
                assert row["count"] == 3
            elif row["app_version"] == "1.0.0" and row["date"] == date(2023, 1, 2):
                assert abs(row["p50"] - 5000) < 100
                assert row["count"] == 3
            elif row["app_version"] == "2.0.0" and row["date"] == date(2023, 1, 1):
                assert abs(row["p50"] - 1000) < 100
                assert row["count"] == 3

    def test_calculate_percentiles_accuracy(self, spark):
        """
        GIVEN: 10,000 values uniformly distributed from 1 to 10,000
        WHEN: calculate_percentiles() with p50, p95, p99
        THEN:
            - p50 ≈ 5000 (±1% error acceptable)
            - p95 ≈ 9500 (±1% error acceptable)
            - p99 ≈ 9900 (±1% error acceptable)
        """
        # Arrange
        schema = StructType(
            [
                StructField("group", StringType(), nullable=False),
                StructField("value", IntegerType(), nullable=False),
            ]
        )

        data = [("A", i) for i in range(1, 10001)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df, value_column="value", group_by_columns=["group"], percentiles=[0.50, 0.95, 0.99]
        )

        # Assert using custom helper
        result = result_df.collect()[0]

        # p50 ≈ 5000 (±1%)
        assert_percentile_accuracy(result["p50"], 5000, tolerance_pct=1.0)

        # p95 ≈ 9500 (±1%)
        assert_percentile_accuracy(result["p95"], 9500, tolerance_pct=1.0)

        # p99 ≈ 9900 (±1%)
        assert_percentile_accuracy(result["p99"], 9900, tolerance_pct=1.0)

    def test_calculate_percentiles_single_value(self, spark):
        """
        GIVEN: A single value in the group
        WHEN: calculate_percentiles() is called
        THEN: All percentiles equal the single value
        """
        # Arrange
        schema = StructType(
            [
                StructField("group", StringType(), nullable=False),
                StructField("value", IntegerType(), nullable=False),
            ]
        )

        data = [("A", 5000)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df, value_column="value", group_by_columns=["group"], percentiles=[0.50, 0.95, 0.99]
        )

        # Assert
        row = result_df.collect()[0]
        assert row["p50"] == 5000
        assert row["p95"] == 5000
        assert row["count"] == 1

    def test_calculate_percentiles_all_same_values(self, spark):
        """
        GIVEN: All values in the group are identical
        WHEN: calculate_percentiles() is called
        THEN: All percentiles equal the value, stddev is 0
        """
        # Arrange
        schema = StructType(
            [
                StructField("group", StringType(), nullable=False),
                StructField("value", IntegerType(), nullable=False),
            ]
        )

        data = [("A", 1000)] * 100
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df, value_column="value", group_by_columns=["group"], percentiles=[0.50, 0.95]
        )

        # Assert
        row = result_df.collect()[0]
        assert row["p50"] == 1000
        assert row["p95"] == 1000
        assert row["stddev_value"] == 0.0


class TestCalculateDeviceCorrelation:
    """Tests for calculate_device_correlation() function."""

    def test_calculate_device_correlation_basic(self, spark):
        """
        GIVEN:
            - iPad users: avg_duration = 2000ms, 1000 interactions
            - iPhone users: avg_duration = 3000ms, 500 interactions
            - Mac users: avg_duration = 1500ms, 200 interactions
        WHEN: calculate_device_correlation() is called
        THEN:
            - Returns 3 rows (one per device type)
            - Sorted by avg_duration_ms DESC: iPhone (3000), iPad (2000), Mac (1500)
            - All metrics calculated correctly
        """
        # Arrange - interactions
        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )

        interactions_data = []
        # iPad: 1000 interactions with avg=2000ms
        for i in range(1000):
            interactions_data.append((f"ipad_user_{i % 10}", 2000))
        # iPhone: 500 interactions with avg=3000ms
        for i in range(500):
            interactions_data.append((f"iphone_user_{i % 10}", 3000))
        # Mac: 200 interactions with avg=1500ms
        for i in range(200):
            interactions_data.append((f"mac_user_{i % 10}", 1500))

        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Metadata
        metadata_data = []
        for i in range(10):
            metadata_data.append((f"ipad_user_{i}", "iPad"))
            metadata_data.append((f"iphone_user_{i}", "iPhone"))
            metadata_data.append((f"mac_user_{i}", "Mac"))

        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("device_type", StringType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = calculate_device_correlation(interactions_df, metadata_df)

        # Assert
        results = result_df.collect()
        assert len(results) == 3

        # Verify sorted by avg_duration_ms DESC
        assert results[0]["device_type"] == "iPhone"
        assert results[0]["avg_duration_ms"] == 3000.0
        assert results[0]["total_interactions"] == 500

        assert results[1]["device_type"] == "iPad"
        assert results[1]["avg_duration_ms"] == 2000.0
        assert results[1]["total_interactions"] == 1000

        assert results[2]["device_type"] == "Mac"
        assert results[2]["avg_duration_ms"] == 1500.0
        assert results[2]["total_interactions"] == 200

    def test_calculate_device_correlation_multiple_users(self, spark):
        """
        GIVEN:
            - iPad: u001 (100 interactions), u002 (50 interactions), u003 (50 interactions)
        WHEN: calculate_device_correlation() is called
        THEN:
            - device_type=iPad: unique_users=3, total_interactions=200
            - interactions_per_user = 200 / 3 ≈ 66.67
        """
        # Arrange - interactions
        interactions_data = []
        for _i in range(100):
            interactions_data.append(("u001", 1000))
        for _i in range(50):
            interactions_data.append(("u002", 1500))
        for _i in range(50):
            interactions_data.append(("u003", 2000))

        interactions_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("duration_ms", LongType(), nullable=False),
            ]
        )
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Metadata
        metadata_data = [("u001", "iPad"), ("u002", "iPad"), ("u003", "iPad")]
        metadata_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("device_type", StringType(), nullable=False),
            ]
        )
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = calculate_device_correlation(interactions_df, metadata_df)

        # Assert
        result = result_df.collect()[0]
        assert result["device_type"] == "iPad"
        assert result["unique_users"] == 3
        assert result["total_interactions"] == 200
        assert abs(result["interactions_per_user"] - 66.67) < 0.1
