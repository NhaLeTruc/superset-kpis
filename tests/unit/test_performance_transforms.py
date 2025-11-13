"""
Unit tests for performance metrics transforms.

Following TDD approach - tests written before implementation.
Reference: docs/TDD_SPEC.md - Task 3 (Performance Metrics Specifications)
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    TimestampType, DateType, DoubleType
)
from datetime import datetime, date
from src.transforms.performance_transforms import (
    calculate_percentiles,
    calculate_device_correlation,
    detect_anomalies_statistical
)


class TestCalculatePercentiles:
    """Tests for calculate_percentiles() function."""

    def test_calculate_percentiles_basic(self, spark):
        """
        GIVEN:
            - app_version=1.0.0 with durations: [1000, 2000, 3000, 4000, 5000] ms
        WHEN: calculate_percentiles() with percentiles=[0.50, 0.95]
        THEN:
            - p50_duration_ms ≈ 3000 (median)
            - p95_duration_ms ≈ 4900 (95th percentile)
            - count = 5
        """
        # Arrange
        schema = StructType([
            StructField("app_version", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        data = [
            ("1.0.0", 1000),
            ("1.0.0", 2000),
            ("1.0.0", 3000),
            ("1.0.0", 4000),
            ("1.0.0", 5000)
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df,
            value_column="duration_ms",
            group_by_columns=["app_version"],
            percentiles=[0.50, 0.95]
        )

        # Assert
        result = result_df.collect()[0]
        assert result["app_version"] == "1.0.0"
        assert abs(result["p50_duration_ms"] - 3000) < 100  # Within 100ms
        assert abs(result["p95_duration_ms"] - 4900) < 200  # Within 200ms
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
        schema = StructType([
            StructField("app_version", StringType(), nullable=False),
            StructField("date", DateType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

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
            ("2.0.0", date(2023, 1, 1), 1500)
        ]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df,
            value_column="duration_ms",
            group_by_columns=["app_version", "date"],
            percentiles=[0.50]
        )

        # Assert
        assert result_df.count() == 3

        # Check each group
        results = result_df.collect()
        for row in results:
            if row["app_version"] == "1.0.0" and row["date"] == date(2023, 1, 1):
                assert abs(row["p50_duration_ms"] - 2000) < 100
                assert row["count"] == 3
            elif row["app_version"] == "1.0.0" and row["date"] == date(2023, 1, 2):
                assert abs(row["p50_duration_ms"] - 5000) < 100
                assert row["count"] == 3
            elif row["app_version"] == "2.0.0" and row["date"] == date(2023, 1, 1):
                assert abs(row["p50_duration_ms"] - 1000) < 100
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
        schema = StructType([
            StructField("group", StringType(), nullable=False),
            StructField("value", IntegerType(), nullable=False)
        ])

        data = [("A", i) for i in range(1, 10001)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = calculate_percentiles(
            df,
            value_column="value",
            group_by_columns=["group"],
            percentiles=[0.50, 0.95, 0.99]
        )

        # Assert
        result = result_df.collect()[0]

        # p50 ≈ 5000 (±1%)
        assert abs(result["p50_value"] - 5000) / 5000 < 0.01

        # p95 ≈ 9500 (±1%)
        assert abs(result["p95_value"] - 9500) / 9500 < 0.01

        # p99 ≈ 9900 (±1%)
        assert abs(result["p99_value"] - 9900) / 9900 < 0.01


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
        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])

        interactions_data = []
        # iPad: 1000 interactions with avg=2000ms
        for i in range(1000):
            interactions_data.append((f"ipad_user_{i%10}", 2000))
        # iPhone: 500 interactions with avg=3000ms
        for i in range(500):
            interactions_data.append((f"iphone_user_{i%10}", 3000))
        # Mac: 200 interactions with avg=1500ms
        for i in range(200):
            interactions_data.append((f"mac_user_{i%10}", 1500))

        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Metadata
        metadata_data = []
        for i in range(10):
            metadata_data.append((f"ipad_user_{i}", "iPad"))
            metadata_data.append((f"iphone_user_{i}", "iPhone"))
            metadata_data.append((f"mac_user_{i}", "Mac"))

        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False)
        ])
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
        for i in range(100):
            interactions_data.append(("u001", 1000))
        for i in range(50):
            interactions_data.append(("u002", 1500))
        for i in range(50):
            interactions_data.append(("u003", 2000))

        interactions_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("duration_ms", LongType(), nullable=False)
        ])
        interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)

        # Metadata
        metadata_data = [
            ("u001", "iPad"),
            ("u002", "iPad"),
            ("u003", "iPad")
        ]
        metadata_schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("device_type", StringType(), nullable=False)
        ])
        metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)

        # Act
        result_df = calculate_device_correlation(interactions_df, metadata_df)

        # Assert
        result = result_df.collect()[0]
        assert result["device_type"] == "iPad"
        assert result["unique_users"] == 3
        assert result["total_interactions"] == 200
        assert abs(result["interactions_per_user"] - 66.67) < 0.1


class TestDetectAnomaliesStatistical:
    """Tests for detect_anomalies_statistical() function."""

    def test_detect_anomalies_statistical_basic(self, spark):
        """
        GIVEN:
            - 95 values around mean=1000 (stddev≈100)
            - 5 outliers: [2000, 2500, 3000, -500, -1000]
            - z_threshold=3.0
        WHEN: detect_anomalies_statistical() is called
        THEN:
            - Returns ~5 rows (the outliers)
            - z_scores > 3.0 or < -3.0
            - anomaly_type correctly labeled ("high" or "low")
        """
        # Arrange
        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("value", IntegerType(), nullable=False)
        ])

        # 95 normal values around 1000
        import random
        random.seed(42)
        normal_data = [(i, 1000 + random.randint(-200, 200)) for i in range(95)]

        # 5 outliers
        outlier_data = [
            (95, 2000),
            (96, 2500),
            (97, 3000),
            (98, -500),
            (99, -1000)
        ]

        all_data = normal_data + outlier_data
        df = spark.createDataFrame(all_data, schema=schema)

        # Act
        result_df = detect_anomalies_statistical(
            df,
            value_column="value",
            z_threshold=3.0
        )

        # Assert
        assert result_df.count() >= 3  # At least some outliers detected

        # Check columns exist
        assert "z_score" in result_df.columns
        assert "baseline_mean" in result_df.columns
        assert "baseline_stddev" in result_df.columns
        assert "anomaly_type" in result_df.columns

        # Check z_scores are beyond threshold
        results = result_df.collect()
        for row in results:
            assert abs(row["z_score"]) > 3.0

            # Check anomaly_type
            if row["value"] > row["baseline_mean"]:
                assert row["anomaly_type"] == "high"
            else:
                assert row["anomaly_type"] == "low"

    def test_detect_anomalies_statistical_grouped(self, spark):
        """
        GIVEN:
            - User u001: avg=1000ms, stddev=100ms, one value=2000ms (10 sigma!)
            - User u002: avg=5000ms, stddev=500ms, one value=7000ms (4 sigma)
            - group_by_columns=["user_id"]
            - z_threshold=3.0
        WHEN: detect_anomalies_statistical() is called
        THEN:
            - u001 anomaly detected (10 > 3)
            - u002 anomaly detected (4 > 3)
            - Each user's baseline (mean, stddev) is per-user
        """
        # Arrange
        schema = StructType([
            StructField("user_id", StringType(), nullable=False),
            StructField("value", IntegerType(), nullable=False)
        ])

        data = []
        # u001: 10 normal values around 1000
        for i in range(10):
            data.append(("u001", 1000))
        # u001: 1 anomaly at 2000 (10 sigma)
        data.append(("u001", 2000))

        # u002: 10 normal values around 5000
        for i in range(10):
            data.append(("u002", 5000))
        # u002: 1 anomaly at 7000 (4 sigma)
        data.append(("u002", 7000))

        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = detect_anomalies_statistical(
            df,
            value_column="value",
            z_threshold=3.0,
            group_by_columns=["user_id"]
        )

        # Assert
        assert result_df.count() == 2  # Both anomalies detected

        # Check u001 anomaly
        u001_anomaly = result_df.filter(F.col("user_id") == "u001").collect()[0]
        assert u001_anomaly["value"] == 2000
        assert abs(u001_anomaly["baseline_mean"] - 1000) < 50
        assert u001_anomaly["anomaly_type"] == "high"

        # Check u002 anomaly
        u002_anomaly = result_df.filter(F.col("user_id") == "u002").collect()[0]
        assert u002_anomaly["value"] == 7000
        assert abs(u002_anomaly["baseline_mean"] - 5000) < 50
        assert u002_anomaly["anomaly_type"] == "high"

    def test_detect_anomalies_statistical_no_anomalies(self, spark):
        """
        GIVEN: All values within 2 sigma of mean
        WHEN: detect_anomalies_statistical() is called with z_threshold=3.0
        THEN: Returns empty DataFrame with correct schema
        """
        # Arrange
        schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("value", IntegerType(), nullable=False)
        ])

        # All values close to mean (within 2 sigma)
        data = [(i, 1000 + (i % 20) * 5) for i in range(100)]
        df = spark.createDataFrame(data, schema=schema)

        # Act
        result_df = detect_anomalies_statistical(
            df,
            value_column="value",
            z_threshold=3.0
        )

        # Assert
        assert result_df.count() == 0

        # Verify schema
        assert "z_score" in result_df.columns
        assert "baseline_mean" in result_df.columns
        assert "baseline_stddev" in result_df.columns
        assert "anomaly_type" in result_df.columns
